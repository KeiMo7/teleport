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
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/session"
)

const (
	// maxRetryAttempts bounds how many times a failed write is retried before
	// it is discarded from the queue.
	maxRetryAttempts = 10
	// defaultRetryInterval spaces out retry attempts.
	defaultRetryInterval = 5 * time.Second
)

// Writer wraps an Index so index write failures never block session end,
// recording upload or playback. Failures go to a bounded retry queue handled
// by a background worker.
type Writer struct {
	index  Index
	queue  *RetryQueue
	logger *slog.Logger

	retryInterval time.Duration
	cancel        context.CancelFunc
	done          chan struct{}

	attempts atomic.Uint64
}

// WriterConfig configures a Writer.
type WriterConfig struct {
	Index   Index
	Metrics RetryQueueMetrics
	Logger  *slog.Logger
	// RetryInterval spaces out retry attempts. Defaults to retryInterval.
	RetryInterval time.Duration
}

// NewWriter creates a fail-open index writer and starts its retry worker.
func NewWriter(cfg WriterConfig) (*Writer, error) {
	if cfg.Index == nil {
		return nil, trace.BadParameter("index is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default().With(teleport.ComponentKey, "recordingindex")
	}
	retryInterval := cfg.RetryInterval
	if retryInterval <= 0 {
		retryInterval = defaultRetryInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		index:         cfg.Index,
		queue:         NewRetryQueue(cfg.Metrics),
		logger:        logger,
		cancel:        cancel,
		done:          make(chan struct{}),
		retryInterval: retryInterval,
	}
	go w.run(ctx)
	return w, nil
}

// Put indexes an entry without ever blocking the caller on backend failures.
// Validation errors are returned directly (they are not retryable); backend
// failures are queued.
func (w *Writer) Put(entry Entry) error {
	if err := entry.Check(); err != nil {
		return trace.Wrap(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := w.index.Put(ctx, entry); err != nil {
		if trace.IsBadParameter(err) || IsAborted(err) {
			// Aborted means the conflict budget was spent; a fresh attempt is
			// still worthwhile, so queue it. Bad parameter is not retryable.
			if trace.IsBadParameter(err) {
				w.logger.Warn("dropping invalid recording index entry", "error", err)
				return trace.Wrap(err)
			}
		}
		w.logger.Warn("queuing failed recording index write", "error", err)
		w.queue.Enqueue(retryItem{Entry: entry.Clone()})
		// Fail open: the caller (session end path) must not be blocked.
		return nil
	}
	return nil
}

// Delete removes an entry, queuing a best-effort retry on failure.
func (w *Writer) Delete(sid session.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.index.Delete(ctx, sid); err != nil {
		w.logger.Warn("queuing failed recording index delete", "error", err)
		// Deletes are represented as a zero entry carrying only the session
		// ID; the worker recognizes them by the empty StartTime.
		w.queue.Enqueue(retryItem{Entry: Entry{SessionID: sid, Delete: true}})
	}
}

// Close stops the retry worker.
func (w *Writer) Close() {
	w.cancel()
	w.queue.Close()
	<-w.done
}

// QueueLen exposes the current queue depth for tests and metrics.
func (w *Writer) QueueLen() int {
	return w.queue.Len()
}

// attemptCount returns the total number of retry attempts performed.
func (w *Writer) attemptCount() int {
	return int(w.attempts.Load())
}

func (w *Writer) run(ctx context.Context) {
	defer close(w.done)
	for {
		item, ok := w.queue.Dequeue()
		if !ok {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if w.retry(ctx, item) {
			// The write still fails; wait for the retry interval before
			// re-enqueueing so a recovered backend is picked up promptly.
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.retryInterval):
			}
		}
	}
}

// retry attempts one write. It returns true when the item must be retried.
func (w *Writer) retry(ctx context.Context, item retryItem) bool {
	if item.Attempts >= maxRetryAttempts {
		w.logger.Error("discarding recording index write after exhausting retries",
			"attempts", item.Attempts)
		return false
	}
	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var err error
	if item.Entry.Delete {
		err = w.index.Delete(retryCtx, item.Entry.SessionID)
	} else {
		_, err = w.index.Put(retryCtx, item.Entry)
	}
	w.attempts.Add(1)
	if err == nil {
		return false
	}
	w.logger.Warn("retrying failed recording index write", "error", err, "attempt", item.Attempts+1)
	item.Attempts++
	// Re-enqueue. The bounded queue evicts the oldest entry if necessary.
	w.queue.Enqueue(item)
	return true
}
