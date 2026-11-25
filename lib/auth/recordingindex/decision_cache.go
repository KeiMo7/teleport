/*
 * Teleport
 * Copyright (Gravitational, Inc.
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
	"sort"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/singleflight"

	"github.com/gravitational/teleport/api/types/wrappers"
)

const (
	// allowTTL is the time-to-live of an allow decision.
	allowTTL = 30 * time.Second
	// denyTTL is the time-to-live of a deny decision. Denies expire faster so
	// a corrected policy takes effect promptly.
	denyTTL = 10 * time.Second
	// decisionCacheShards is the number of independently locked shards.
	decisionCacheShards = 64
)

// roleRevision is a role name with the revision that was current when the
// decision was made.
type roleRevision struct {
	Name     string
	Revision string
}

func sortRoleRevisions(roles []roleRevision) {
	sort.Slice(roles, func(i, j int) bool {
		if roles[i].Name != roles[j].Name {
			return roles[i].Name < roles[j].Name
		}
		return roles[i].Revision < roles[j].Revision
	})
}

// cacheKey is the full identity of an authorization decision. It binds the
// user, the exact role revisions, traits, the access request, the query
// predicate fingerprint, the session and the entry revision, so any change in
// those inputs makes a stale decision uncacheable.
type cacheKey struct {
	User                 string
	Roles                []roleRevision
	Traits               wrappers.Traits
	AccessRequestID      string
	PredicateFingerprint string
	SessionID            string
	EntryRevision        uint64
}

// hash returns the stable SHA-256 representation of the key.
func (k cacheKey) hash() string {
	h := sha256.New()
	write := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	write(k.User)
	write(k.AccessRequestID)
	write(k.PredicateFingerprint)
	write(k.SessionID)
	write(hex.EncodeToString(sha256.New().Sum([]byte(
		uin64ToString(k.EntryRevision)))))
	for _, role := range k.Roles {
		write(role.Name)
		write(role.Revision)
	}
	// Traits are serialized in sorted key order for a stable digest.
	traitKeys := make([]string, 0, len(k.Traits))
	for key := range k.Traits {
		traitKeys = append(traitKeys, key)
	}
	sort.Strings(traitKeys)
	for _, key := range traitKeys {
		write(key)
		values := append([]string(nil), k.Traits[key]...)
		sort.Strings(values)
		for _, value := range values {
			write(value)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// decision is one cached authorization outcome.
type decision struct {
	Allow      bool
	WrittenAt  time.Time
	Hits       uint64
	Generation uint64
}

// cacheShard is one partition of the decision cache.
type cacheShard struct {
	mu         sync.RWMutex
	generation uint64
	entries    map[string]decision
}

// DecisionCacheMetrics observes cache behavior.
type DecisionCacheMetrics interface {
	CacheHit(protocol string)
	CacheMiss(protocol string)
	Invalidation(count int)
}

// DecisionCache is a sharded, TTL-bounded cache of authorization decisions.
// Allows are only written while the watch feed is healthy; a watch outage
// degrades the cache to deny-only and flushes everything on reconnect.
type DecisionCache struct {
	clock   clockwork.Clock
	metrics DecisionCacheMetrics

	shards [decisionCacheShards]cacheShard

	// degraded is true while the watch feed is disconnected. In degraded
	// mode no new allows are written and existing allows expire normally.
	degradedMu sync.RWMutex
	degraded   bool

	// inFlight merges concurrent misses for the same identity so the
	// underlying checker runs at most once per key.
	inFlight singleflight.Group

	// userShard maps a username to the shard that must be incremented and
	// cleaned when a role/user/access-request watch event arrives.
	userShardMu sync.RWMutex
	userShards  map[string]int
}

// NewDecisionCache builds a ready-to-use cache.
func NewDecisionCache(clock clockwork.Clock, metrics DecisionCacheMetrics) *DecisionCache {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	cache := &DecisionCache{
		clock:      clock,
		metrics:    metrics,
		userShards: make(map[string]int),
	}
	for i := range cache.shards {
		cache.shards[i].entries = make(map[string]decision)
	}
	return cache
}

// shardFor returns the shard responsible for a key hash.
func (c *DecisionCache) shardFor(hash string) *cacheShard {
	// The hash is hex SHA-256; the first byte is enough for even spread.
	var b byte
	if len(hash) > 0 {
		b = hash[0]
	}
	return &c.shards[int(b)%decisionCacheShards]
}

// Lookup returns a live decision. Expired entries are removed inline.
func (c *DecisionCache) Lookup(key cacheKey) (decision, bool) {
	hash := key.hash()
	shard := c.shardFor(hash)
	shard.mu.RLock()
	cached, ok := shard.entries[hash]
	shard.mu.RUnlock()
	if !ok {
		c.recordMiss()
		return decision{}, false
	}

	ttl := denyTTL
	if cached.Allow {
		ttl = allowTTL
	}
	now := c.clock.Now()
	// Guard against a backward clock: never compute a negative age.
	if cached.WrittenAt.After(now) {
		// A write timestamp in the future means the entry is invalid; drop it.
		c.delete(hash, shard, cached.Generation)
		c.recordMiss()
		return decision{}, false
	}
	if now.Sub(cached.WrittenAt) >= ttl {
		c.delete(hash, shard, cached.Generation)
		c.recordMiss()
		return decision{}, false
	}

	// In degraded mode allows are treated as absent; denies remain valid until
	// their shorter TTL.
	if cached.Allow && c.isDegraded() {
		c.recordMiss()
		return decision{}, false
	}

	shard.mu.Lock()
	cached.Hits++
	shard.entries[hash] = cached
	shard.mu.Unlock()
	c.recordHit()
	return cached, true
}

// Store records a decision. It refuses to cache allows while degraded.
func (c *DecisionCache) Store(key cacheKey, allow bool) {
	if allow && c.isDegraded() {
		return
	}
	hash := key.hash()
	shard := c.shardFor(hash)
	now := c.clock.Now()

	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.entries[hash] = decision{
		Allow:      allow,
		WrittenAt:  now,
		Generation: shard.generation,
	}
	c.rememberUserShard(key.User, shard)
}

// Do merges concurrent evaluations of the same identity. The compute function
// runs at most once per key; waiting callers share its result or error.
func (c *DecisionCache) Do(key cacheKey, compute func() (bool, error)) (bool, error) {
	hash := key.hash()
	result, err, _ := c.inFlight.Do(hash, func() (any, error) {
		return compute()
	})
	if err != nil {
		return false, err
	}
	allow, _ := result.(bool)
	return allow, nil
}

// InvalidateUser increments the generation of the shard owning a user and
// asynchronously clears that user's entries. It is driven by watch events for
// roles, users and access requests.
func (c *DecisionCache) InvalidateUser(username string, roleNames ...string) {
	if username == "" && len(roleNames) == 0 {
		return
	}
	// Find candidate shards: the user's registered shard plus the shards of
	// any explicitly named roles. Incrementing a generation invalidates every
	// entry produced under the previous generation.
	shards := c.shardsForUserOrRoles(username, roleNames)
	count := 0
	for _, shard := range shards {
		shard.mu.Lock()
		shard.generation++
		removed := len(shard.entries)
		shard.entries = make(map[string]decision)
		shard.mu.Unlock()
		count += removed
	}
	if c.metrics != nil && count > 0 {
		c.metrics.Invalidation(count)
	}
}

// EnterDegraded stops new allow writes and marks the cache degraded.
func (c *DecisionCache) EnterDegraded() {
	c.degradedMu.Lock()
	c.degraded = true
	c.degradedMu.Unlock()
}

// Recover clears every shard and leaves the cache healthy again. Reconnect
// after a watch outage must not reuse decisions computed against stale state.
func (c *DecisionCache) Recover() {
	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.generation++
		total += len(shard.entries)
		shard.entries = make(map[string]decision)
		shard.mu.Unlock()
	}
	c.degradedMu.Lock()
	c.degraded = false
	c.degradedMu.Unlock()
	if c.metrics != nil && total > 0 {
		c.metrics.Invalidation(total)
	}
}

func (c *DecisionCache) isDegraded() bool {
	c.degradedMu.RLock()
	defer c.degradedMu.RUnlock()
	return c.degraded
}

func (c *DecisionCache) delete(hash string, shard *cacheShard, generation uint64) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	// Only remove if the entry still belongs to the generation observed.
	if current, ok := shard.entries[hash]; ok && current.Generation == generation {
		delete(shard.entries, hash)
	}
}

func (c *DecisionCache) rememberUserShard(user string, shard *cacheShard) {
	if user == "" {
		return
	}
	c.userShardMu.Lock()
	c.userShards[user] = c.shardIndex(shard)
	c.userShardMu.Unlock()
}

func (c *DecisionCache) shardIndex(shard *cacheShard) int {
	for i := range c.shards {
		if &c.shards[i] == shard {
			return i
		}
	}
	return 0
}

func (c *DecisionCache) shardsForUserOrRoles(username string, roleNames []string) []*cacheShard {
	seen := make(map[int]struct{})
	var out []*cacheShard
	add := func(index int) {
		if _, ok := seen[index]; ok {
			return
		}
		seen[index] = struct{}{}
		out = append(out, &c.shards[index])
	}

	c.userShardMu.RLock()
	if index, ok := c.userShards[username]; ok {
		add(index)
	}
	c.userShardMu.RUnlock()

	// Roles do not have a fixed shard by themselves; they affect every shard
	// that cached decisions mentioning them. Those decisions live on the
	// affected user's shard, which the user lookup already covers. As a
	// conservative fallback for role-only events with no known user, flush the
	// whole cache so a revision change can never leave a stale allow.
	if username == "" && len(roleNames) > 0 {
		for i := range c.shards {
			add(i)
		}
	}
	return out
}

func (c *DecisionCache) recordHit() {
	if c.metrics != nil {
		c.metrics.CacheHit("")
	}
}

func (c *DecisionCache) recordMiss() {
	if c.metrics != nil {
		c.metrics.CacheMiss("")
	}
}

func uin64ToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[pos:])
}
