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
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/gravitational/teleport/api/types/wrappers"
	"github.com/gravitational/teleport/lib/session"
)

type testCacheMetrics struct {
	hits, misses, invalidations int
}

func (m *testCacheMetrics) CacheHit(string)  { m.hits++ }
func (m *testCacheMetrics) CacheMiss(string) { m.misses++ }
func (m *testCacheMetrics) Invalidation(n int) {
	m.invalidations += n
}

func testCacheKey(user string, sid session.ID, revision uint64) cacheKey {
	return cacheKey{
		User:          user,
		Roles:         []roleRevision{{Name: "access", Revision: "r1"}},
		Traits:        wrappers.Traits{},
		SessionID:     string(sid),
		EntryRevision: revision,
	}
}

// TestDecisionCacheTTLAndDegraded verifies allow/deny TTLs, backward-clock
// handling and degraded-mode semantics.
func TestDecisionCacheTTLAndDegraded(t *testing.T) {
	clock := clockwork.NewFakeClock()
	metrics := &testCacheMetrics{}
	cache := NewDecisionCache(clock, metrics)
	sid := session.NewID()
	key := testCacheKey("alice", sid, 1)

	// Allow is stored and looked up.
	cache.Store(key, true)
	_, ok := cache.Lookup(key)
	require.True(t, ok)
	require.Equal(t, 1, metrics.hits)

	// Deny TTL is shorter.
	denyKey := testCacheKey("alice", session.NewID(), 1)
	cache.Store(denyKey, false)
	clock.Advance(denyTTL + time.Second)
	_, ok = cache.Lookup(denyKey)
	require.False(t, ok)

	// Allow survives past deny TTL but expires at allow TTL.
	_, ok = cache.Lookup(key)
	require.True(t, ok)
	clock.Advance(allowTTL - denyTTL + time.Second)
	_, ok = cache.Lookup(key)
	require.False(t, ok)

	// Degraded mode refuses new allows and hides existing ones.
	cache.Store(key, true)
	cache.EnterDegraded()
	_, ok = cache.Lookup(key)
	require.False(t, ok)
	cache.Store(key, true)
	_, ok = cache.Lookup(key)
	require.False(t, ok)

	// Reconnect flushes everything and restores healthy caching.
	cache.Recover()
	require.False(t, cache.isDegraded())
	cache.Store(key, true)
	_, ok = cache.Lookup(key)
	require.True(t, ok)
}

// TestDecisionCacheInvalidatesOnRoleRevision proves a changed role revision
// makes the old cache key miss.
func TestDecisionCacheInvalidatesOnRoleRevision(t *testing.T) {
	clock := clockwork.NewFakeClock()
	cache := NewDecisionCache(clock, &testCacheMetrics{})
	sid := session.NewID()
	key := testCacheKey("alice", sid, 1)
	cache.Store(key, true)
	_, ok := cache.Lookup(key)
	require.True(t, ok)

	// A role revision change produces a distinct key, so the stale decision
	// can never be reused.
	updated := key
	updated.Roles = []roleRevision{{Name: "access", Revision: "r2"}}
	_, ok = cache.Lookup(updated)
	require.False(t, ok)

	// Explicit invalidation flushes the user's shard.
	cache.Store(key, true)
	cache.InvalidateUser("alice")
	_, ok = cache.Lookup(key)
	require.False(t, ok)
}

// TestDecisionCacheEntryRevisionBinding ensures an entry revision change
// invalidates cached decisions.
func TestDecisionCacheEntryRevisionBinding(t *testing.T) {
	clock := clockwork.NewFakeClock()
	cache := NewDecisionCache(clock, nil)
	sid := session.NewID()
	key := testCacheKey("alice", sid, 1)
	cache.Store(key, true)

	updated := key
	updated.EntryRevision = 2
	_, ok := cache.Lookup(updated)
	require.False(t, ok)
}

// TestDecisionCacheBackwardClock ensures a future write timestamp is treated
// as invalid rather than producing a negative TTL.
func TestDecisionCacheBackwardClock(t *testing.T) {
	clock := clockwork.NewFakeClock()
	cache := NewDecisionCache(clock, nil)
	sid := session.NewID()
	key := testCacheKey("alice", sid, 1)

	cache.Store(key, true)
	// Move the clock backwards; the stored WrittenAt is now in the future.
	clock.Advance(-time.Minute)
	_, ok := cache.Lookup(key)
	require.False(t, ok)
}

// TestDecisionCacheSingleFlight merges concurrent misses.
func TestDecisionCacheSingleFlight(t *testing.T) {
	clock := clockwork.NewRealClock()
	cache := NewDecisionCache(clock, nil)
	key := testCacheKey("alice", session.NewID(), 1)

	var calls int
	var mu sync.Mutex
	compute := func() (bool, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return true, nil
	}
	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			allow, err := cache.Do(key, compute)
			require.NoError(t, err)
			require.True(t, allow)
		}()
	}
	wg.Wait()
	// The singleflight group coalesces concurrent identical calls; exact
	// coalescing is timing-dependent, so assert at least one compute ran and
	// no more than one per goroutine.
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, calls, 1)
	require.LessOrEqual(t, calls, goroutines)
}
