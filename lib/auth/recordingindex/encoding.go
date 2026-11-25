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

package recordingindex

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/session"
)

const (
	// keyPrefix is the backend prefix for recording index entries.
	keyPrefix = "recordingindex"
	// tombstonePrefix is the backend prefix for deletion tombstones.
	tombstonePrefix = "recordingindex_tombstones"
	// watermarkPrefix is the backend prefix for rebuild watermarks.
	watermarkPrefix = "recordingindex_watermarks"
	// startNanoWidth zero-pads StartUnixNano so that lexicographic order on
	// the key matches chronological order. Unix nanos are at most 19 digits
	// until year 2262.
	startNanoWidth = 19
)

// entryKey returns the backend key for an entry:
// /recordingindex/<cluster>/<19-digit zero-padded StartUnixNano>/<sessionID>.
// Because the timestamp component has fixed width and zero padding, key
// lexicographic order matches start-time order within a cluster.
func entryKey(clusterName string, startNano int64, sid session.ID) backend.Key {
	return backend.NewKey(keyPrefix, clusterName,
		fmt.Sprintf("%0*d", startNanoWidth, startNano), string(sid))
}

// clusterPrefix returns the range prefix covering every entry of a cluster.
func clusterPrefix(clusterName string) backend.Key {
	return backend.NewKey(keyPrefix, clusterName)
}

// tombstoneKey returns the backend key for a session's deletion tombstone.
func tombstoneKey(sid session.ID) backend.Key {
	return backend.NewKey(tombstonePrefix, string(sid))
}

// watermarkKey returns the backend key for a cluster's rebuild watermark.
func watermarkKey(clusterName string) backend.Key {
	return backend.NewKey(watermarkPrefix, clusterName)
}

// encodeEntry serializes an entry and enforces the maximum encoded size.
func encodeEntry(entry Entry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if len(data) > maxEncodedEntrySize {
		return nil, trace.BadParameter("encoded entry size %d exceeds maximum of %d bytes",
			len(data), maxEncodedEntrySize)
	}
	return data, nil
}

// decodeEntry parses a serialized entry.
func decodeEntry(data []byte) (Entry, error) {
	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return Entry{}, trace.Wrap(err)
	}
	if err := entry.Check(); err != nil {
		return Entry{}, trace.Wrap(err)
	}
	return entry, nil
}

// tombstone is the payload of a deletion record. Tombstones participate in Get
// (driving NotFound) but never appear in List results.
type tombstone struct {
	SessionID  string `json:"session_id"`
	DeletedAt  int64  `json:"deleted_at"`
	ExpiresAt  int64  `json:"expires_at"`
	Generation uint64 `json:"generation"`
}

func encodeTombstone(t tombstone) ([]byte, error) {
	data, err := json.Marshal(t)
	return data, trace.Wrap(err)
}

func decodeTombstone(data []byte) (tombstone, error) {
	var t tombstone
	err := json.Unmarshal(data, &t)
	return t, trace.Wrap(err)
}

// watermark is the persisted rebuild position for a cluster.
type watermark struct {
	ClusterName string `json:"cluster_name"`
	// PositionNano is the highest processed event timestamp (Unix nano).
	PositionNano int64 `json:"position_nano"`
	// UpdatedAtNano is when the watermark was last advanced.
	UpdatedAtNano int64 `json:"updated_at_nano"`
}

func encodeWatermark(w watermark) ([]byte, error) {
	data, err := json.Marshal(w)
	return data, trace.Wrap(err)
}

func decodeWatermark(data []byte) (watermark, error) {
	var w watermark
	err := json.Unmarshal(data, &w)
	return w, trace.Wrap(err)
}

// parseEntryKey extracts the timestamp and session ID from an entry key. It
// is used when loading entries from the backend.
func parseEntryKey(key backend.Key) (startNano int64, sid session.ID, err error) {
	components := key.Components()
	if len(components) < 4 || components[0] != keyPrefix {
		return 0, "", trace.BadParameter("invalid recording index key %q", key.String())
	}
	sid = session.ID(components[len(components)-1])
	tsPart := components[len(components)-2]
	startNano, parseErr := strconv.ParseInt(tsPart, 10, 64)
	if parseErr != nil {
		return 0, "", trace.BadParameter("invalid timestamp component %q in key %q", tsPart, key.String())
	}
	if _, parseErr := session.ParseID(string(sid)); parseErr != nil {
		return 0, "", trace.Wrap(parseErr)
	}
	return startNano, sid, nil
}

// encodePageToken / decodePageToken live in pagetoken.go.
