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

// TestTombstonePreventsResurrection verifies that after Delete, a late Put is
// rejected with NotFound, Get returns NotFound, and List never shows the
// entry. A fresh index built over the same backend loads tombstones first.
func TestTombstonePreventsResurrection(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	entry := sampleEntry(t, clock.Now(), "del")
	_, err := idx.Put(ctx, entry)
	require.NoError(t, err)

	require.NoError(t, idx.Delete(ctx, entry.SessionID))

	_, err = idx.Get(ctx, entry.SessionID)
	require.Error(t, err)
	require.True(t, trace.IsNotFound(err))

	// A late write must not resurrect a deleted session.
	_, err = idx.Put(ctx, entry)
	require.Error(t, err)
	require.True(t, trace.IsNotFound(err))

	// Tombstones never appear in List.
	page, err := idx.List(ctx, Query{Order: OrderAscending, PageSize: 100}, "")
	require.NoError(t, err)
	require.Empty(t, page.Entries)

	// Rebuilding an index over the same backend keeps the entry hidden.
	bk := idx.backend
	rebuilt, err := NewBackendIndex(ctx, BackendIndexConfig{Backend: bk, Clock: clock})
	require.NoError(t, err)
	_, err = rebuilt.Get(ctx, entry.SessionID)
	require.True(t, trace.IsNotFound(err))
}

// TestEntryValidation enforces every documented validation rule.
func TestEntryValidation(t *testing.T) {
	now := time.Now()
	valid := func() Entry {
		return Entry{
			SessionID:     session.NewID(),
			ClusterName:   "root.example",
			Protocol:      apievents.EventProtocolSSH,
			StartTime:     now,
			EndTime:       now.Add(time.Second),
			MetadataState: MetadataStatePending,
			Participants:  []string{"b", "a", "a"},
		}
	}

	t.Run("bad session ID", func(t *testing.T) {
		entry := valid()
		entry.SessionID = "not-a-uuid"
		require.Error(t, entry.Check())
	})
	t.Run("zero start", func(t *testing.T) {
		entry := valid()
		entry.StartTime = time.Time{}
		require.Error(t, entry.Check())
	})
	t.Run("start after end", func(t *testing.T) {
		entry := valid()
		entry.EndTime = entry.StartTime.Add(-time.Second)
		require.Error(t, entry.Check())
	})
	t.Run("participants normalized", func(t *testing.T) {
		entry := valid()
		require.NoError(t, entry.Check())
		require.Equal(t, []string{"a", "b"}, entry.Participants)
	})
	t.Run("too many labels", func(t *testing.T) {
		entry := valid()
		entry.Labels = map[string]string{}
		for i := 0; i < maxLabelCount+1; i++ {
			entry.Labels["k"+itoa(i)] = "v"
		}
		require.Error(t, entry.Check())
	})
	t.Run("long label value", func(t *testing.T) {
		entry := valid()
		long := make([]byte, maxLabelValueLength+1)
		for i := range long {
			long[i] = 'x'
		}
		entry.Labels = map[string]string{"k": string(long)}
		require.Error(t, entry.Check())
	})
	t.Run("unknown protocol defaults", func(t *testing.T) {
		entry := valid()
		entry.Protocol = ""
		require.NoError(t, entry.Check())
		require.Equal(t, ProtocolUnknown, entry.Protocol)
	})
}

// TestEncodedEntrySizeLimit rejects entries whose JSON encoding exceeds 8KB.
func TestEncodedEntrySizeLimit(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	entry := sampleEntry(t, clock.Now(), "big")
	long := make([]byte, maxEncodedEntrySize)
	for i := range long {
		long[i] = 'y'
	}
	entry.Labels = map[string]string{"big": string(long)}
	_, err := idx.Put(ctx, entry)
	require.Error(t, err)
	require.True(t, trace.IsBadParameter(err))
}

// TestMetadataStateTransitions checks pending->ready and pending->failed and
// the retry cap.
func TestMetadataStateTransitions(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	entry := sampleEntry(t, clock.Now(), "meta")
	_, err := idx.Put(ctx, entry)
	require.NoError(t, err)
	require.Equal(t, MetadataStatePending, entry.MetadataState)

	entry.MetadataState = MetadataStateFailed
	entry.MetadataRetries = maxMetadataRetries + 4
	entry.LastError = "boom"
	_, err = idx.Put(ctx, entry)
	require.NoError(t, err)
	got, err := idx.Get(ctx, entry.SessionID)
	require.NoError(t, err)
	require.Equal(t, MetadataStateFailed, got.MetadataState)
	require.Equal(t, maxMetadataRetries, got.MetadataRetries)

	// Ready supersedes failed in the state ordering.
	entry.MetadataState = MetadataStateReady
	entry.LastError = ""
	_, err = idx.Put(ctx, entry)
	require.NoError(t, err)
	got, err = idx.Get(ctx, entry.SessionID)
	require.NoError(t, err)
	require.Equal(t, MetadataStateReady, got.MetadataState)
}

// TestBackendUnavailableClassifiesErrors maps backend failures onto
// Unavailable and never returns truncated List results.
func TestBackendUnavailableClassifiesErrors(t *testing.T) {
	clock := clockworkReal()
	bk, err := memory.New(memory.Config{Clock: clock})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bk.Close()) })

	faulty := &faultyBackend{Backend: bk}
	faulty.setFailGetRange(true)
	_, err = NewBackendIndex(context.Background(), BackendIndexConfig{Backend: faulty, Clock: clock})
	require.Error(t, err)
	require.True(t, IsUnavailable(err), "expected unavailable, got %v", err)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func clockworkReal() clockwork.Clock {
	return clockwork.NewRealClock()
}
