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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gravitational/teleport/api/types"
	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/session"
)

// TestMultiIndexPartialResponse verifies a broken cluster yields a partial
// response naming the failed cluster while healthy clusters still return,
// with the merged entries fully ordered.
func TestMultiIndexPartialResponse(t *testing.T) {
	ctx := context.Background()
	idxA, clockA := newTestIndex(t)
	idxB, _ := newTestIndex(t)

	base := clockA.Now()
	entryA := sampleEntry(t, base, "a")
	entryA.ClusterName = "cluster-a"
	entryB := sampleEntry(t, base.Add(time.Second), "b")
	entryB.ClusterName = "cluster-b"
	_, err := idxA.Put(ctx, entryA)
	require.NoError(t, err)
	_, err = idxB.Put(ctx, entryB)
	require.NoError(t, err)

	// Healthy multi-index returns ordered entries.
	multi := NewMultiIndex(map[string]Index{
		"cluster-a": idxA,
		"cluster-b": idxB,
	})
	page, err := multi.List(ctx, Query{Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.False(t, page.Partial)
	require.Len(t, page.Entries, 2)
	require.True(t, page.Entries[0].StartTime.Before(page.Entries[1].StartTime))

	// Replace one cluster with a failing index. The SafeIndex wrapper turns
	// panics into classified errors so partial behavior can be observed
	// without crashing the test.
	failing := NewSafeIndex(&panickingIndex{})
	multi = NewMultiIndex(map[string]Index{
		"cluster-a": idxA,
		"cluster-b": failing,
	})
	page, err = multi.List(ctx, Query{Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.True(t, page.Partial)
	require.Equal(t, []string{"cluster-b"}, page.FailedClusters)
	require.Len(t, page.Entries, 1)
	require.Equal(t, entryA.SessionID, page.Entries[0].SessionID)
}

// TestPredicateFiltersBeforePaging asserts filtered entries do not count
// toward PageSize and cannot be inferred from the total.
func TestPredicateFiltersBeforePaging(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	base := clock.Now()

	allowed := make(map[session.ID]bool)
	for i := 0; i < 10; i++ {
		entry := sampleEntry(t, base.Add(time.Duration(i)*time.Second), "p")
		_, err := idx.Put(ctx, entry)
		require.NoError(t, err)
		// Only even entries are readable.
		if i%2 == 0 {
			allowed[entry.SessionID] = true
		}
	}
	predicate := func(entry Entry) bool { return allowed[entry.SessionID] }
	page, err := idx.List(WithPredicate(ctx, predicate),
		Query{Order: OrderAscending, PageSize: 3}, "")
	require.NoError(t, err)
	require.Len(t, page.Entries, 3)
	for _, entry := range page.Entries {
		require.True(t, predicate(entry))
	}

	// Page through all visible entries; there must be exactly 5.
	seen := 0
	token := page.NextToken
	require.NotEmpty(t, token)
	for token != "" {
		next, listErr := idx.List(WithPredicate(ctx, predicate),
			Query{Order: OrderAscending, PageSize: 3}, token)
		require.NoError(t, listErr)
		seen += len(next.Entries)
		token = next.NextToken
	}
	require.Equal(t, 5, seen+3)
}

// TestSessionEndMemoReusesFetch proves repeated decisions in one request
// trigger a single session end event fetch.
func TestSessionEndMemoReusesFetch(t *testing.T) {
	ctx := WithSessionEndMemo(context.Background())
	sid := session.NewID()

	fetches := 0
	fetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		fetches++
		return sessionEndEvent(sid), nil
	}
	checker := &fakeChecker{
		roles:       []types.Role{newTestRole(t, "r", "1")},
		simpleErr:   denyError("need context"),
		extendedErr: nil,
	}
	adapter := newAdapter(t, checker, fetch, nil)

	require.NoError(t, adapter.AuthorizeSession(ctx, sid))
	require.NoError(t, adapter.AuthorizeSession(ctx, sid))
	require.Equal(t, 1, fetches)
}
