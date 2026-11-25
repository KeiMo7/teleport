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
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/gravitational/trace"
)

// Order controls the ordering of List results.
type Order int

const (
	// OrderAscending returns entries from earliest StartTime to latest.
	OrderAscending Order = iota
	// OrderDescending returns entries from latest StartTime to earliest.
	OrderDescending
)

// DefaultPageSize is used when a query does not specify one.
const DefaultPageSize = 100

// MaxPageSize bounds a single List response.
const MaxPageSize = 1000

// Query describes a recording index search. From is left-closed and To is
// right-open. A zero From means no lower bound; a zero To means no upper
// bound. Participants matches entries containing any of the requested
// participants.
type Query struct {
	// From is the inclusive lower bound on StartTime. Zero means no lower
	// bound.
	From time.Time
	// To is the exclusive upper bound on StartTime. Zero means no upper
	// bound.
	To time.Time
	// Participants matches entries that contain at least one listed
	// participant. Empty means no participant filter.
	Participants []string
	// Protocol filters on Entry.Protocol. Empty means any protocol.
	Protocol string
	// ResourceKind filters on Entry.ResourceKind. Empty means any kind.
	ResourceKind string
	// MetadataState filters on Entry.MetadataState. Empty means any state.
	MetadataState MetadataState
	// Order is the requested ordering.
	Order Order
	// PageSize is the maximum number of *visible* entries returned. Filtered
	// entries do not count toward it.
	PageSize int
}

// Check validates and normalizes the query.
func (q *Query) Check() error {
	if q == nil {
		return trace.BadParameter("query is required")
	}
	if !q.From.IsZero() && !q.To.IsZero() && q.From.After(q.To) {
		return trace.BadParameter("query From %s must not be later than To %s", q.From, q.To)
	}
	if q.PageSize < 0 {
		return trace.BadParameter("page size must not be negative")
	}
	if q.PageSize == 0 {
		q.PageSize = DefaultPageSize
	}
	if q.PageSize > MaxPageSize {
		q.PageSize = MaxPageSize
	}
	if q.MetadataState != "" {
		if _, ok := metadataStateOrder[q.MetadataState]; !ok {
			return trace.BadParameter("unsupported metadata state %q", q.MetadataState)
		}
	}
	return nil
}

// Fingerprint returns a stable hash identifying the filter portion of the
// query (everything except ordering and paging). It is embedded in page
// tokens so a token can only be resumed against the query that created it.
func (q Query) Fingerprint() string {
	h := sha256.New()
	writeField := func(name, value string) {
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write([]byte(value))
		h.Write([]byte{0})
	}
	writeField("from", nanosToString(q.From))
	writeField("to", nanosToString(q.To))
	for _, p := range q.Participants {
		writeField("participant", p)
	}
	writeField("protocol", q.Protocol)
	writeField("resource_kind", q.ResourceKind)
	writeField("metadata_state", string(q.MetadataState))
	return hex.EncodeToString(h.Sum(nil))
}

func nanosToString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconvFormatInt(t.UnixNano())
}

// matches reports whether an entry satisfies all query filters. It never
// mutates the entry.
func (q Query) matches(entry Entry) bool {
	if !q.From.IsZero() && entry.StartTime.Before(q.From) {
		return false
	}
	// To is right-open: an entry starting exactly at To is excluded.
	if !q.To.IsZero() && !entry.StartTime.Before(q.To) {
		return false
	}
	if q.Protocol != "" && entry.Protocol != q.Protocol {
		return false
	}
	if q.ResourceKind != "" && entry.ResourceKind != q.ResourceKind {
		return false
	}
	if q.MetadataState != "" && entry.MetadataState != q.MetadataState {
		return false
	}
	if len(q.Participants) > 0 && !containsAny(entry.Participants, q.Participants) {
		return false
	}
	return true
}

func containsAny(haystack, needles []string) bool {
	if len(haystack) == 0 || len(needles) == 0 {
		return false
	}
	for _, needle := range needles {
		for _, candidate := range haystack {
			if candidate == needle {
				return true
			}
		}
	}
	return false
}

// Predicate is an authorization decision over a concrete index entry. List
// applies predicates before counting entries toward PageSize, so callers
// cannot infer the existence of recordings they cannot read from page sizes.
type Predicate func(Entry) bool

// allowAll is the predicate used when authorization places no per-entry
// restriction.
func allowAll(Entry) bool {
	return true
}
