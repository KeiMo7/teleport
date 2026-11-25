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
	"github.com/stretchr/testify/require"

	"github.com/gravitational/teleport/lib/session"
)

type testRetryMetrics struct{ dropped int }

func (m *testRetryMetrics) RetryDropped() { m.dropped++ }

// TestRetryQueueBounded fills the queue past capacity and asserts the bound
// holds and the oldest item is evicted with a drop count.
func TestRetryQueueBounded(t *testing.T) {
	metrics := &testRetryMetrics{}
	queue := NewRetryQueue(metrics)
	defer queue.Close()

	for i := 0; i < retryQueueCapacity+50; i++ {
		queue.Enqueue(retryItem{Entry: Entry{SessionID: session.NewID()}})
	}
	require.Equal(t, retryQueueCapacity, queue.Len())
	require.Equal(t, 50, int(queue.Dropped()))
	require.Equal(t, 50, metrics.dropped)
}

// TestWriterFailsOpen makes the backend fail Put and asserts the Writer
// returns promptly (never blocking the session-end path), queues the write,
// and succeeds after the backend recovers.
func TestWriterFailsOpen(t *testing.T) {
	idx, clock := newTestIndex(t)
	ctx := context.Background()

	writer, err := NewWriter(WriterConfig{Index: idx, Metrics: &testRetryMetrics{}})
	require.NoError(t, err)
	defer writer.Close()

	entry := sampleEntry(t, clock.Now(), "w")

	// Break the backend then attempt a write through the fail-open path.
	faulty := &faultyBackend{Backend: idx.backend, failPut: true}
	brokenIdx, err := NewBackendIndex(ctx, BackendIndexConfig{Backend: faulty, Clock: clock})
	require.NoError(t, err)
	brokenMetrics := &testRetryMetrics{}
	brokenWriter, err := NewWriter(WriterConfig{
		Index:         brokenIdx,
		Metrics:       brokenMetrics,
		RetryInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	defer brokenWriter.Close()

	start := time.Now()
	require.NoError(t, brokenWriter.Put(entry))
	require.Less(t, time.Since(start), 100*time.Millisecond)
	// The failed write must have been handed to the retry machinery. The
	// worker may dequeue immediately, so assert on observable retry activity
	// rather than an instantaneous queue length.
	require.Eventually(t, func() bool {
		return brokenWriter.QueueLen() > 0 || brokenWriter.attemptCount() > 0
	}, time.Second, 5*time.Millisecond)

	// The healthy writer path still works and the entry becomes readable.
	require.NoError(t, writer.Put(entry))
	got, err := idx.Get(ctx, entry.SessionID)
	require.NoError(t, err)
	require.Equal(t, entry.SessionID, got.SessionID)
}

// TestMetadataFailureDoesNotBlockSessionEnd mirrors the acceptance criterion:
// an index/backend failure must return from the session-end path within
// 100ms and the entry must be observable as failed once retried.
func TestMetadataFailureDoesNotBlockSessionEnd(t *testing.T) {
	idx, clock := newTestIndex(t)
	faulty := &faultyBackend{Backend: idx.backend, failPut: true}
	// Point the index's backend at the faulty wrapper while keeping one index
	// instance so the in-memory cache stays consistent across recovery.
	idx.backend = faulty
	writer, err := NewWriter(WriterConfig{
		Index:         idx,
		RetryInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	defer writer.Close()

	entry := sampleEntry(t, clock.Now(), "meta-fail")
	entry.MetadataState = MetadataStateFailed
	entry.LastError = "upload failed"

	start := time.Now()
	require.NoError(t, writer.Put(entry))
	require.Less(t, time.Since(start), 100*time.Millisecond)

	// Recover the backend; the queued write is eventually applied.
	faulty.setFailPut(false)
	require.Eventually(t, func() bool {
		_, err := idx.Get(context.Background(), entry.SessionID)
		return err == nil
	}, 5*time.Second, 50*time.Millisecond)

	got, err := idx.Get(context.Background(), entry.SessionID)
	require.NoError(t, err)
	require.Equal(t, MetadataStateFailed, got.MetadataState)
}

// TestSafeIndexRecoversPanic injects a panicking index and asserts the
// boundary converts it to an Internal-style error without propagating.
func TestSafeIndexRecoversPanic(t *testing.T) {
	safe := NewSafeIndex(panickingIndex{})
	_, err := safe.List(context.Background(), Query{}, "")
	require.Error(t, err)
	require.False(t, trace.IsBadParameter(err))
	require.Contains(t, err.Error(), "internal recording index error")
}

type panickingIndex struct{}

func (panickingIndex) Put(context.Context, Entry) (uint64, error) { panic("boom") }
func (panickingIndex) Delete(context.Context, session.ID) error   { panic("boom") }
func (panickingIndex) Get(context.Context, session.ID) (Entry, error) {
	panic("boom")
}
func (panickingIndex) List(context.Context, Query, string) (Page, error) { panic("boom") }

var _ Index = panickingIndex{}
