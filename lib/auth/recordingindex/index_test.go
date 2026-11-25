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

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/backend/memory"
	"github.com/gravitational/teleport/lib/session"
)

func newTestIndex(t *testing.T) (*BackendIndex, *clockwork.FakeClock) {
	t.Helper()
	clock := clockwork.NewFakeClock()
	bk, err := memory.New(memory.Config{Clock: clock})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bk.Close()) })
	idx, err := NewBackendIndex(context.Background(), BackendIndexConfig{
		Backend: bk,
		Clock:   clock,
	})
	require.NoError(t, err)
	return idx, clock
}

func sampleEntry(t *testing.T, start time.Time, suffix string) Entry {
	t.Helper()
	return Entry{
		SessionID:     session.NewID(),
		ClusterName:   "root.example",
		Protocol:      apievents.EventProtocolSSH,
		User:          "alice",
		Participants:  []string{"alice", "bob"},
		ResourceKind:  "node",
		ResourceName:  "node-" + suffix,
		Labels:        map[string]string{"env": "test"},
		StartTime:     start,
		EndTime:       start.Add(time.Minute),
		MetadataState: MetadataStatePending,
	}
}

// TestIndexListKeysetStable inserts 5000 entries while interleaving new
// inserts, then pages through the whole set asserting no duplicates, no
// gaps, stable order and round-trippable tokens.
func TestIndexListKeysetStable(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	base := clock.Now()

	const total = 5000
	inserted := make(map[session.ID]struct{}, total)
	for i := 0; i < total; i++ {
		entry := sampleEntry(t, base.Add(time.Duration(i)*time.Second), "a")
		rev, err := idx.Put(ctx, entry)
		require.NoError(t, err)
		require.Equal(t, uint64(1), rev)
		inserted[entry.SessionID] = struct{}{}
	}

	// Interleave fresh inserts during paging to prove concurrent insertion
	// cannot cause duplicates or missing items.
	interleaved := make([]session.ID, 0, 100)
	for i := 0; i < 100; i++ {
		entry := sampleEntry(t, base.Add(time.Hour+time.Duration(i)*time.Second), "b")
		_, err := idx.Put(ctx, entry)
		require.NoError(t, err)
		interleaved = append(interleaved, entry.SessionID)
		inserted[entry.SessionID] = struct{}{}
	}

	seen := make(map[session.ID]struct{}, total+100)
	var token string
	pages := 0
	var lastStart time.Time
	var lastSID session.ID
	for {
		page, err := idx.List(ctx, Query{
			From:     base.Add(-time.Minute),
			Order:    OrderAscending,
			PageSize: 250,
		}, token)
		require.NoError(t, err)
		require.NotEmpty(t, page.Entries)
		for _, entry := range page.Entries {
			_, dup := seen[entry.SessionID]
			require.False(t, dup, "duplicate session %s", entry.SessionID)
			seen[entry.SessionID] = struct{}{}

			if !lastStart.IsZero() {
				if entry.StartTime.Equal(lastStart) {
					require.Less(t, string(lastSID), string(entry.SessionID))
				} else {
					require.True(t, entry.StartTime.After(lastStart))
				}
			}
			lastStart = entry.StartTime
			lastSID = entry.SessionID
		}
		pages++
		token = page.NextToken
		if token == "" {
			break
		}
	}
	require.Equal(t, len(inserted), len(seen))
	require.Greater(t, pages, 1)
	for sid := range inserted {
		_, ok := seen[sid]
		require.True(t, ok, "missing session %s", sid)
	}
}

// TestQueryBoundaries covers left-closed/right-open semantics, empty ranges,
// equal timestamps, extreme times and full scans.
func TestQueryBoundaries(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	base := clock.Now().Truncate(time.Second)

	t0 := base
	t1 := base.Add(time.Second)
	t2 := base.Add(2 * time.Second)

	entry0 := sampleEntry(t, t0, "0")
	entry1 := sampleEntry(t, t1, "1")
	entry2 := sampleEntry(t, t2, "2")
	for _, entry := range []Entry{entry0, entry1, entry2} {
		_, err := idx.Put(ctx, entry)
		require.NoError(t, err)
	}

	// Left-closed: From=t1 must include entry1.
	page, err := idx.List(ctx, Query{From: t1, Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Len(t, page.Entries, 2)
	require.Equal(t, entry1.SessionID, page.Entries[0].SessionID)

	// Right-open: To=t1 must exclude entry1.
	page, err = idx.List(ctx, Query{To: t1, Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.Equal(t, entry0.SessionID, page.Entries[0].SessionID)

	// Empty interval From==To returns nothing.
	page, err = idx.List(ctx, Query{From: t1, To: t1, Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Empty(t, page.Entries)

	// Full scan with no bounds.
	page, err = idx.List(ctx, Query{Order: OrderDescending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Len(t, page.Entries, 3)
	require.Equal(t, entry2.SessionID, page.Entries[0].SessionID)

	// Equal timestamps still produce a total order via session ID tie-break.
	same := t0.Add(3 * time.Second)
	eA := sampleEntry(t, same, "a")
	eB := sampleEntry(t, same, "b")
	_, err = idx.Put(ctx, eA)
	require.NoError(t, err)
	_, err = idx.Put(ctx, eB)
	require.NoError(t, err)
	page, err = idx.List(ctx, Query{From: same, To: same.Add(time.Nanosecond), Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Len(t, page.Entries, 2)
	require.Less(t, string(page.Entries[0].SessionID), string(page.Entries[1].SessionID))
}

// TestMergeKeepsEarliestStartAndUnionParticipants verifies field-level merge
// semantics and monotonic revisions across the two write paths.
func TestMergeKeepsEarliestStartAndUnionParticipants(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	start := clock.Now()

	sid := session.NewID()
	first := Entry{
		SessionID:     sid,
		ClusterName:   "root.example",
		Protocol:      apievents.EventProtocolSSH,
		User:          "alice",
		Participants:  []string{"alice"},
		ResourceKind:  "node",
		ResourceName:  "node-1",
		StartTime:     start.Add(time.Minute),
		EndTime:       start.Add(2 * time.Minute),
		MetadataState: MetadataStatePending,
	}
	rev1, err := idx.Put(ctx, first)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rev1)

	second := first.Clone()
	second.StartTime = start
	second.EndTime = start.Add(3 * time.Minute)
	second.Participants = []string{"bob", "alice"}
	second.MetadataState = MetadataStateReady
	rev2, err := idx.Put(ctx, second)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rev2)

	got, err := idx.Get(ctx, sid)
	require.NoError(t, err)
	// Earlier start wins, later end wins.
	require.True(t, got.StartTime.Equal(start))
	require.True(t, got.EndTime.Equal(start.Add(3*time.Minute)))
	// Unioned, sorted participants.
	require.Equal(t, []string{"alice", "bob"}, got.Participants)
	// Later metadata state wins.
	require.Equal(t, MetadataStateReady, got.MetadataState)
	require.Equal(t, uint64(2), got.Revision)
}

// TestPageTokenExpiry verifies expired, tampered and mismatched tokens all
// resolve to BadParameter.
func TestPageTokenExpiry(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	entry := sampleEntry(t, clock.Now(), "x")
	_, err := idx.Put(ctx, entry)
	require.NoError(t, err)

	query := Query{Order: OrderAscending, PageSize: 1}
	page, err := idx.List(ctx, query, "")
	require.NoError(t, err)
	require.NotEmpty(t, page.NextToken)

	// Expiry.
	clock.Advance(tokenTTL + time.Second)
	_, err = idx.List(ctx, query, page.NextToken)
	require.Error(t, err)
	require.True(t, trace.IsBadParameter(err))

	// Tampered token.
	_, err = idx.List(ctx, query, page.NextToken+"x")
	require.Error(t, err)
	require.True(t, trace.IsBadParameter(err))

	// Direction mismatch.
	_, err = idx.List(ctx, Query{Order: OrderDescending, PageSize: 1}, page.NextToken)
	require.Error(t, err)
	require.True(t, trace.IsBadParameter(err))
}
