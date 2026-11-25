/*
 * Teleport
 * Copyright (C) 2025  Gravitational, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// Package recordingindex implements an index of session recordings. The index
// stores only searchable metadata derived from session end events; it never
// stores recording bytes or decryption keys.
package recordingindex

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/gravitational/trace"

	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/session"
)

const (
	// ProtocolUnknown is used when the protocol of a session cannot be
	// determined from the session end event. The original event type is
	// preserved on the entry for diagnostics.
	ProtocolUnknown = "unknown"

	// maxLabelCount is the maximum number of labels on an entry.
	maxLabelCount = 64
	// maxLabelKeyLength is the maximum byte length of a label key.
	maxLabelKeyLength = 256
	// maxLabelValueLength is the maximum byte length of a label value.
	maxLabelValueLength = 256
	// maxEncodedEntrySize is the maximum encoded size of a single entry.
	maxEncodedEntrySize = 8 * 1024
)

// MetadataState describes how far along the recording metadata (thumbnail,
// enhanced metadata) for a session is.
type MetadataState string

const (
	// MetadataStatePending means metadata generation has not completed yet.
	MetadataStatePending MetadataState = "pending"
	// MetadataStateReady means metadata generation completed successfully.
	MetadataStateReady MetadataState = "ready"
	// MetadataStateFailed means metadata generation failed and may be retried.
	MetadataStateFailed MetadataState = "failed"
	// MetadataStateMissing means the index entry exists but the underlying
	// recording is missing. A rebuild may move it back to pending.
	MetadataStateMissing MetadataState = "missing"
)

// metadataStateOrder defines the total ordering used when merging metadata
// states from two write paths. Later states win.
var metadataStateOrder = map[MetadataState]int{
	MetadataStatePending: 0,
	MetadataStateMissing: 1,
	MetadataStateFailed:  2,
	MetadataStateReady:   3,
}

// maxMetadataRetries is the maximum number of metadata generation retries
// before the entry stays failed.
const maxMetadataRetries = 5

// Entry is an indexed session recording. It contains only index fields, never
// recording bytes or decryption keys.
type Entry struct {
	// SessionID is the UUID of the session.
	SessionID session.ID `json:"session_id"`
	// ClusterName is the name of the cluster where the session ran.
	ClusterName string `json:"cluster_name"`
	// Protocol is one of the apievents.EventProtocol* values or
	// ProtocolUnknown.
	Protocol string `json:"protocol"`
	// OriginalEventType retains the audit event type when Protocol could not
	// be determined.
	OriginalEventType string `json:"original_event_type,omitempty"`
	// User is the user that started the session.
	User string `json:"user"`
	// Participants is the deduplicated, sorted list of participants.
	Participants []string `json:"participants"`
	// ResourceKind is the kind of resource the session targeted.
	ResourceKind string `json:"resource_kind"`
	// ResourceName is the name of the resource the session targeted.
	ResourceName string `json:"resource_name"`
	// Labels are resource labels.
	Labels map[string]string `json:"labels,omitempty"`
	// StartTime is when the session started.
	StartTime time.Time `json:"start_time"`
	// EndTime is when the session ended.
	EndTime time.Time `json:"end_time"`
	// RecordingType is the session recording mode (e.g. node/proxy).
	RecordingType string `json:"recording_type,omitempty"`
	// MetadataState is the state of metadata generation.
	MetadataState MetadataState `json:"metadata_state"`
	// MetadataRetries counts failed metadata generation attempts.
	MetadataRetries int `json:"metadata_retries,omitempty"`
	// LastError is the last metadata generation error.
	LastError string `json:"last_error,omitempty"`
	// Revision is a monotonically increasing version of the entry. It is
	// assigned by the index and must never move backwards.
	Revision uint64 `json:"revision"`
	// Delete is an internal, unexported-to-wire marker used by the retry queue
	// to represent a failed deletion. It is never persisted.
	Delete bool `json:"-"`
}

// Clone returns a deep copy of the entry. Entries returned by the index are
// clones, so callers may mutate them without corrupting index state.
func (e Entry) Clone() Entry {
	clone := e
	if e.Participants != nil {
		clone.Participants = append([]string(nil), e.Participants...)
	}
	if e.Labels != nil {
		clone.Labels = make(map[string]string, len(e.Labels))
		for k, v := range e.Labels {
			clone.Labels[k] = v
		}
	}
	return clone
}

// Check validates the index fields of the entry. It normalizes Participants
// (deduplicated and sorted) and Labels in place.
func (e *Entry) Check() error {
	switch {
	case e == nil:
		return trace.BadParameter("entry is required")
	case e.SessionID == "":
		return trace.BadParameter("session ID is required")
	case e.ClusterName == "":
		return trace.BadParameter("cluster name is required")
	}
	if _, err := session.ParseID(string(e.SessionID)); err != nil {
		return trace.BadParameter("invalid session ID: %v", err)
	}
	// Normalize to the canonical UUID representation produced by ParseID.
	parsed, err := session.ParseID(string(e.SessionID))
	if err != nil {
		return trace.BadParameter("invalid session ID: %v", err)
	}
	e.SessionID = *parsed

	if e.StartTime.IsZero() {
		return trace.BadParameter("start time must be set")
	}
	if !e.EndTime.IsZero() && e.StartTime.After(e.EndTime) {
		return trace.BadParameter("start time %s must not be later than end time %s", e.StartTime, e.EndTime)
	}

	if e.Protocol == "" {
		e.Protocol = ProtocolUnknown
	}
	if e.MetadataState == "" {
		e.MetadataState = MetadataStatePending
	}
	if _, ok := metadataStateOrder[e.MetadataState]; !ok {
		return trace.BadParameter("unsupported metadata state %q", e.MetadataState)
	}
	if e.MetadataRetries < 0 {
		return trace.BadParameter("metadata retries must not be negative")
	}
	if e.MetadataRetries > maxMetadataRetries {
		e.MetadataRetries = maxMetadataRetries
	}

	e.Participants = normalizeParticipants(e.Participants)

	if len(e.Labels) > maxLabelCount {
		return trace.BadParameter("too many labels: %d exceeds limit of %d", len(e.Labels), maxLabelCount)
	}
	for key, value := range e.Labels {
		if len(key) > maxLabelKeyLength {
			return trace.BadParameter("label key %q exceeds maximum length of %d bytes", key, maxLabelKeyLength)
		}
		if len(value) > maxLabelValueLength {
			return trace.BadParameter("label value for key %q exceeds maximum length of %d bytes", key, maxLabelValueLength)
		}
	}
	return nil
}

// normalizeParticipants deduplicates and sorts participants. Empty values are
// dropped.
func normalizeParticipants(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, participant := range in {
		if participant == "" {
			continue
		}
		if _, ok := seen[participant]; ok {
			continue
		}
		seen[participant] = struct{}{}
		out = append(out, participant)
	}
	slices.Sort(out)
	return out
}

// mergeEntries performs a field-level merge of two entries for the same
// session. StartTime takes the earlier value, EndTime the later value,
// Participants are unioned (and sorted), and MetadataState takes the later
// state. Scalar fields from the more complete entry fill empty values. The
// returned entry keeps the higher revision and is never mutated through either
// argument.
func mergeEntries(existing, incoming Entry) Entry {
	merged := existing.Clone()
	in := incoming.Clone()

	merged.SessionID = existing.SessionID
	merged.ClusterName = cmp.Or(merged.ClusterName, in.ClusterName)
	merged.Protocol = cmp.Or(merged.Protocol, in.Protocol)
	if merged.Protocol == ProtocolUnknown && in.Protocol != ProtocolUnknown {
		merged.Protocol = in.Protocol
	}
	merged.OriginalEventType = cmp.Or(merged.OriginalEventType, in.OriginalEventType)
	merged.User = cmp.Or(merged.User, in.User)
	merged.ResourceKind = cmp.Or(merged.ResourceKind, in.ResourceKind)
	merged.ResourceName = cmp.Or(merged.ResourceName, in.ResourceName)
	merged.RecordingType = cmp.Or(merged.RecordingType, in.RecordingType)

	if !in.StartTime.IsZero() && (merged.StartTime.IsZero() || in.StartTime.Before(merged.StartTime)) {
		merged.StartTime = in.StartTime
	}
	if in.EndTime.After(merged.EndTime) {
		merged.EndTime = in.EndTime
	}

	merged.Participants = normalizeParticipants(append(append([]string(nil), merged.Participants...), in.Participants...))

	if len(in.Labels) > 0 {
		if merged.Labels == nil {
			merged.Labels = make(map[string]string, len(in.Labels))
		}
		for k, v := range in.Labels {
			if _, ok := merged.Labels[k]; !ok {
				merged.Labels[k] = v
			}
		}
	}

	if metadataStateOrder[in.MetadataState] >= metadataStateOrder[merged.MetadataState] {
		merged.MetadataState = in.MetadataState
	}
	// Presive failure details from the path that actually attempted metadata
	// generation.
	if in.MetadataRetries > merged.MetadataRetries {
		merged.MetadataRetries = in.MetadataRetries
	}
	if in.LastError != "" {
		merged.LastError = in.LastError
	}

	merged.Revision = max(existing.Revision, incoming.Revision)
	return merged
}

// EntryFromSessionEndEvent builds an Entry from a session end audit event. The
// returned entry still needs to pass [Entry.Check] before being written.
func EntryFromSessionEndEvent(event apievents.AuditEvent, clusterName string) (Entry, error) {
	if event == nil {
		return Entry{}, trace.BadParameter("session end event is required")
	}
	entry := Entry{
		ClusterName:       clusterName,
		MetadataState:     MetadataStatePending,
		OriginalEventType: event.GetType(),
	}
	if entry.ClusterName == "" {
		entry.ClusterName = event.GetClusterName()
	}

	switch e := event.(type) {
	case *apievents.SessionEnd:
		entry.SessionID = session.ID(e.SessionMetadata.SessionID)
		entry.Protocol = e.Protocol
		entry.User = e.UserMetadata.User
		entry.Participants = e.Participants
		entry.StartTime = e.StartTime
		entry.EndTime = e.EndTime
		entry.RecordingType = e.SessionRecording
		switch e.Protocol {
		case apievents.EventProtocolKube:
			entry.ResourceKind = "kube_cluster"
			entry.ResourceName = e.KubernetesClusterMetadata.KubernetesCluster
			entry.Labels = e.KubernetesClusterMetadata.KubernetesLabels
		default:
			entry.ResourceKind = "node"
			entry.ResourceName = e.ServerMetadata.ServerID
			entry.Labels = e.ServerMetadata.ServerLabels
		}
		if entry.StartTime.IsZero() {
			entry.StartTime = e.GetTime()
		}
		if entry.EndTime.IsZero() {
			entry.EndTime = e.GetTime()
		}
	case *apievents.WindowsDesktopSessionEnd:
		entry.SessionID = session.ID(e.SessionMetadata.SessionID)
		entry.Protocol = apievents.EventProtocolTDP
		entry.User = e.UserMetadata.User
		entry.Participants = e.Participants
		entry.ResourceKind = "windows_desktop"
		entry.ResourceName = e.DesktopName
		entry.Labels = e.DesktopLabels
		entry.StartTime = e.StartTime
		entry.EndTime = e.EndTime
		entry.RecordingType = recordingTypeFromBool(e.Recorded)
	case *apievents.DatabaseSessionEnd:
		entry.SessionID = session.ID(e.SessionMetadata.SessionID)
		entry.Protocol = apievents.EventProtocolDB
		entry.User = e.UserMetadata.User
		entry.Participants = e.Participants
		entry.ResourceKind = "db"
		entry.ResourceName = e.DatabaseMetadata.DatabaseService
		entry.Labels = e.DatabaseMetadata.DatabaseLabels
		entry.StartTime = e.StartTime
		entry.EndTime = e.EndTime
	case *apievents.AppSessionEnd:
		entry.SessionID = session.ID(e.SessionMetadata.SessionID)
		entry.Protocol = apievents.EventProtocolApp
		entry.User = e.UserMetadata.User
		entry.ResourceKind = "app"
		entry.ResourceName = e.AppMetadata.AppName
		entry.Labels = e.AppMetadata.AppLabels
		entry.StartTime = e.GetTime()
		entry.EndTime = e.GetTime()
	case *apievents.MCPSessionEnd:
		entry.SessionID = session.ID(e.SessionMetadata.SessionID)
		entry.Protocol = ProtocolUnknown
		entry.User = e.UserMetadata.User
		entry.ResourceKind = "mcp"
		entry.ResourceName = e.AppMetadata.AppName
		entry.Labels = e.AppMetadata.AppLabels
		entry.StartTime = e.GetTime()
		entry.EndTime = e.GetTime()
	default:
		return Entry{}, trace.BadParameter("unsupported session end event type %T", event)
	}

	if entry.Protocol == "" {
		entry.Protocol = ProtocolUnknown
	}
	if err := entry.Check(); err != nil {
		return Entry{}, trace.Wrap(err)
	}
	return entry, nil
}

func recordingTypeFromBool(recorded bool) string {
	if recorded {
		return "recorded"
	}
	return ""
}

// String implements fmt.Stringer without exposing more than is needed for
// diagnostics. It intentionally never prints participant or label contents.
func (e Entry) String() string {
	return fmt.Sprintf("Entry(session=%s, cluster=%s, protocol=%s, state=%s, revision=%d)",
		e.SessionID, e.ClusterName, e.Protocol, e.MetadataState, e.Revision)
}
