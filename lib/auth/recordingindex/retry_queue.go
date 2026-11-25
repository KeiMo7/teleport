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
	"container/list"
	"sync"
	"sync/atomic"
)

const (
	// retryQueueCapacity bounds the number of failed writes held for retry.
	// When full, the oldest item is dropped and a failure counter is
	// incremented; the queue never grows without bound.
	retryQueueCapacity = 4096
)

// retryItem is one queued index write.
type retryItem struct {
	// Entry is the entry that previously failed to write.
	Entry Entry
	// Attempts is the number of retries already performed.
	Attempts int
}

// RetryQueueMetrics observes queue behavior.
type RetryQueueMetrics interface {
	// RetryDropped is called when the oldest item is evicted because the
	// queue is full.
	RetryDropped()
}

// RetryQueue is a bounded FIFO of failed index writes. Pops block while empty
// only through the Cond's broadcast model used by the worker.
type RetryQueue struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	items    *list.List
	capacity int
	closed   bool

	dropped atomic.Uint64
	metrics RetryQueueMetrics
}

// NewRetryQueue creates a queue with the fixed capacity.
func NewRetryQueue(metrics RetryQueueMetrics) *RetryQueue {
	q := &RetryQueue{
		items:    list.New(),
		capacity: retryQueueCapacity,
		metrics:  metrics,
	}
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// Enqueue adds an item, evicting the oldest item when the capacity is reached.
// It never blocks and never lets the queue exceed its bound.
func (q *RetryQueue) Enqueue(item retryItem) {
	q.mu.Lock()
	if q.items.Len() >= q.capacity {
		if oldest := q.items.Front(); oldest != nil {
			q.items.Remove(oldest)
			q.dropped.Add(1)
			if q.metrics != nil {
				q.metrics.RetryDropped()
			}
		}
	}
	q.items.PushBack(item)
	q.mu.Unlock()
	// Wake a blocked Dequeue so the retry worker picks up new work promptly.
	q.notFull.Signal()
}

// Dequeue removes and returns the oldest item. It blocks while the queue is
// empty until Shutdown or Enqueue wakes a waiter.
func (q *RetryQueue) Dequeue() (retryItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.items.Len() == 0 {
		if q.isClosed() {
			return retryItem{}, false
		}
		q.notFull.Wait()
	}
	front := q.items.Front()
	item := front.Value.(retryItem)
	q.items.Remove(front)
	return item, true
}

// Len returns the current queue length.
func (q *RetryQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items.Len()
}

// Dropped returns the cumulative number of evicted items.
func (q *RetryQueue) Dropped() uint64 {
	return q.dropped.Load()
}

// isClosed must be called with mu held.
func (q *RetryQueue) isClosed() bool {
	return q.closed
}

// Close wakes all blocked Dequeue callers so workers can exit.
func (q *RetryQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.notFull.Broadcast()
	q.mu.Unlock()
}
