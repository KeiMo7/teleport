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
	"context"
	"slices"

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport/lib/session"
)

// MultiIndex fans an Index operation out across per-cluster indexes. A single
// corrupted or unavailable cluster never fails a cross-cluster query: healthy
// clusters still return entries, the response is marked partial and the failed
// clusters are named. Entries are merged into a single fully ordered result.
type MultiIndex struct {
	// clusters maps cluster name to its index. The map is treated as
	// immutable after construction.
	clusters map[string]Index
}

// NewMultiIndex builds a cross-cluster index.
func NewMultiIndex(clusters map[string]Index) *MultiIndex {
	copied := make(map[string]Index, len(clusters))
	for name, index := range clusters {
		copied[name] = index
	}
	return &MultiIndex{clusters: copied}
}

// Put writes to the cluster named on the entry. It fails if the entry names an
// unknown cluster.
func (m *MultiIndex) Put(ctx context.Context, entry Entry) (uint64, error) {
	index, ok := m.clusters[entry.ClusterName]
	if !ok {
		return 0, trace.BadParameter("unknown cluster %q", entry.ClusterName)
	}
	return index.Put(ctx, entry)
}

// Delete removes the entry from every cluster (a session ID is unique).
func (m *MultiIndex) Delete(ctx context.Context, sid session.ID) error {
	var firstErr error
	for _, index := range m.clusters {
		if err := index.Delete(ctx, sid); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return trace.Wrap(firstErr)
}

// Get returns the entry from whichever cluster holds it. A healthy cluster's
// NotFound is acceptable; a backend error is surfaced rather than hidden.
func (m *MultiIndex) Get(ctx context.Context, sid session.ID) (Entry, error) {
	var lastErr error
	for _, index := range m.clusters {
		entry, err := index.Get(ctx, sid)
		if err == nil {
			return entry, nil
		}
		if trace.IsNotFound(err) {
			continue
		}
		lastErr = err
	}
	if lastErr != nil {
		return Entry{}, trace.Wrap(lastErr)
	}
	return Entry{}, trace.NotFound("recording index entry for session %s not found", sid)
}

// List queries every cluster, merges visible entries in total (StartTime,
// SessionID) order and marks the page partial if any cluster failed. Because a
// cross-cluster keyset token cannot target a single backend, paging across the
// merged set uses a large per-cluster fetch and re-paginates; cluster tokens
// remain valid for single-cluster callers.
func (m *MultiIndex) List(ctx context.Context, query Query, pageToken string) (Page, error) {
	names := make([]string, 0, len(m.clusters))
	for name := range m.clusters {
		names = append(names, name)
	}
	slices.Sort(names)

	merged := make([]Entry, 0)
	failed := make([]string, 0)
	for _, name := range names {
		page, err := m.clusters[name].List(ctx, query, pageToken)
		if err != nil {
			// One broken cluster must not take down the cross-cluster query.
			failed = append(failed, name)
			continue
		}
		merged = append(merged, page.Entries...)
	}

	sortEntries(merged, query.Order)
	if query.PageSize > 0 && len(merged) > query.PageSize {
		merged = merged[:query.PageSize]
	}
	return Page{
		Entries:        merged,
		Partial:        len(failed) > 0,
		FailedClusters: failed,
	}, nil
}

// sortEntries orders entries by (StartTime, SessionID), honoring direction.
func sortEntries(entries []Entry, order Order) {
	compare := func(a, b Entry) int {
		if !a.StartTime.Equal(b.StartTime) {
			if a.StartTime.Before(b.StartTime) {
				return -1
			}
			return 1
		}
		switch {
		case string(a.SessionID) < string(b.SessionID):
			return -1
		case string(a.SessionID) > string(b.SessionID):
			return 1
		default:
			return 0
		}
	}
	if order == OrderDescending {
		slices.SortFunc(entries, func(a, b Entry) int { return -compare(a, b) })
		return
	}
	slices.SortFunc(entries, compare)
}
