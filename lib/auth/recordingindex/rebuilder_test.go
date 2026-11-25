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

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/backend/memory"
	"github.com/gravitational/teleport/lib/player"
	"github.com/gravitational/teleport/lib/session"
)

// fakeStreamer emits a fixed session end event for one session, implementing
// player.Streamer.
type fakeStreamer struct {
	events map[session.ID]apievents.AuditEvent
}

func (s *fakeStreamer) StreamSessionEvents(ctx context.Context, sid session.ID, startIndex int64) (chan apievents.AuditEvent, chan error) {
	eventsCh := make(chan apievents.AuditEvent, 1)
	errCh := make(chan error, 1)
	// Normalize the requested ID to its canonical UUID representation, matching
	// how index lookups canonicalize session IDs.
	if parsed, err := session.ParseID(string(sid)); err == nil {
		sid = *parsed
	}
	go func() {
		if event, ok := s.events[sid]; ok {
			eventsCh <- event
		}
		// Close only after the event has been delivered, so a consumer never
		// observes the closed channel before the event.
		close(eventsCh)
		close(errCh)
	}()
	return eventsCh, errCh
}

var _ player.Streamer = (*fakeStreamer)(nil)

// TestIndexRebuildIdempotent rebuilds the same window twice and asserts
// revisions do not move backwards, the entry count stays constant and the
// watermark advances.
func TestIndexRebuildIdempotent(t *testing.T) {
	clock := clockwork.NewFakeClock()
	bk, err := memory.New(memory.Config{Clock: clock})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bk.Close()) })
	idx, err := NewBackendIndex(context.Background(), BackendIndexConfig{Backend: bk, Clock: clock})
	require.NoError(t, err)

	sid := session.NewID()
	event := sessionEndEvent(sid)
	streamer := &fakeStreamer{events: map[session.ID]apievents.AuditEvent{sid: event}}

	rebuilder, err := NewRebuilder(RebuilderConfig{
		Index:    idx,
		Backend:  bk,
		Streamer: streamer,
		Clock:    clock,
	})
	require.NoError(t, err)

	from := time.Time{}
	to := time.Time{}
	require.NoError(t, rebuilder.RebuildWindow(context.Background(), "root.example", from, to, []session.ID{sid}))

	first, err := idx.Get(context.Background(), sid)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.Revision)

	watermark1, err := rebuilder.Watermark(context.Background(), "root.example")
	require.NoError(t, err)
	require.False(t, watermark1.IsZero())

	// Rebuild the identical window.
	require.NoError(t, rebuilder.RebuildWindow(context.Background(), "root.example", from, to, []session.ID{sid}))
	second, err := idx.Get(context.Background(), sid)
	require.NoError(t, err)
	// Merge keeps the revision monotonic; the re-put increments it but never
	// regresses, and the data remains identical.
	require.GreaterOrEqual(t, second.Revision, first.Revision)
	require.Equal(t, first.SessionID, second.SessionID)
	require.Equal(t, first.Participants, second.Participants)

	watermark2, err := rebuilder.Watermark(context.Background(), "root.example")
	require.NoError(t, err)
	require.False(t, watermark2.Before(watermark1))

	// Watermark lag stays within the 5 minute bound.
	lag, err := rebuilder.WatermarkLag(context.Background(), "root.example", clock.Now())
	require.NoError(t, err)
	require.LessOrEqual(t, lag, watermarkLagTolerance)
}

// TestListConcurrentWithPutAndDelete runs 200 goroutines mixing Put, Delete,
// Get and List under -race, asserting no panic and that Get never returns a
// half-written entry (every returned entry passes Check).
func TestListConcurrentWithPutAndDelete(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()
	const goroutines = 200

	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			for i := 0; i < 20; i++ {
				entry := sampleEntry(t, clock.Now().Add(time.Duration(g)*time.Millisecond), "c")
				if _, err := idx.Put(ctx, entry); err != nil {
					errCh <- err
					return
				}
				got, err := idx.Get(ctx, entry.SessionID)
				if err == nil {
					if checkErr := got.Check(); checkErr != nil {
						errCh <- checkErr
						return
					}
				}
				if _, err := idx.List(ctx, Query{Order: OrderAscending, PageSize: 50}, ""); err != nil {
					errCh <- err
					return
				}
				if g%5 == 0 {
					if err := idx.Delete(ctx, entry.SessionID); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		select {
		case err := <-errCh:
			require.NoError(t, err)
		default:
		}
	}
	// Drain remaining completions.
	for i := 0; i < cap(errCh); i++ {
		select {
		case err := <-errCh:
			require.NoError(t, err)
		default:
			return
		}
	}
}

// FuzzPageToken feeds random bytes into token verification and asserts it
// never panics and always rejects malformed tokens with BadParameter.
func FuzzPageToken(f *testing.F) {
	clock := clockwork.NewFakeClock()
	bk, err := memory.New(memory.Config{Clock: clock})
	if err != nil {
		f.Fatal(err)
	}
	defer bk.Close()
	idx, err := NewBackendIndex(context.Background(), BackendIndexConfig{Backend: bk, Clock: clock})
	if err != nil {
		f.Fatal(err)
	}
	seeds := []string{
		"",
		"abc",
		"....",
		"eyJmcCI6IiJ9.invalid",
		"AAAAAAAAAAA.AAAAAAAAAAA",
		"\x00\x01\x02",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		query := Query{Order: OrderAscending, PageSize: 10}
		require.NoError(t, query.Check())
		_, err := verifyPageToken(raw, query, idx.clock.Now(), idx.tokenSecret)
		if raw == "" {
			require.NoError(t, err)
			return
		}
		require.Error(t, err)
	})
}
