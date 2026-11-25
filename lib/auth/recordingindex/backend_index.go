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
	"crypto/rand"
	"iter"
	"sync"
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"

	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/session"
	"github.com/gravitational/teleport/lib/utils/sortcache"
)

const (
	// tombstoneTTL is how long deletion tombstones are retained.
	tombstoneTTL = 24 * time.Hour
	// maxCASRetries bounds CompareAndSwap retries before giving up.
	maxCASRetries = 3
	// lockShardCount is the number of shards keyed by session ID used to
	// serialize writes to the same session without taking a global lock.
	lockShardCount = 64
)

// cacheIndex names the sortcache indexes used by BackendIndex.
type cacheIndex int

const (
	// indexSession maps session ID -> entry and is the unique index.
	indexSession cacheIndex = iota
	// indexStart maps (start time, session ID) -> entry and provides the
	// total ordering used by List.
	indexStart
)

// Metrics receives counters and timers from the index. Implementations must
// be safe for concurrent use.
type Metrics interface {
	// EntryGauge reports the current number of live entries.
	EntryGauge(count int)
	// ObserveList records the duration of a List call.
	ObserveList(protocol string, duration time.Duration)
	// ObserveBackendError records a backend failure, classified by operation.
	ObserveBackendError(operation string)
}

// BackendIndexConfig configures a BackendIndex.
type BackendIndexConfig struct {
	// Backend is the durable store. Required.
	Backend backend.Backend
	// Clock is used for TTLs and page-token expiries. Defaults to a real
	// clock.
	Clock clockwork.Clock
	// Metrics receives index telemetry. Optional.
	Metrics Metrics
}

// Check validates the configuration.
func (c *BackendIndexConfig) Check() error {
	if c.Backend == nil {
		return trace.BadParameter("backend is required")
	}
	if c.Clock == nil {
		c.Clock = clockwork.NewRealClock()
	}
	return nil
}

// BackendIndex is an Index implementation backed by a sortcache for in-memory
// ordering and a lib/backend.Backend for durability. It never holds index or
// cache locks while performing backend calls.
type BackendIndex struct {
	backend backend.Backend
	clock   clockwork.Clock
	metrics Metrics

	cache *sortcache.SortCache[Entry, cacheIndex]

	// writeLocks shard per-session write serialization.
	writeLocks [lockShardCount]sync.Mutex

	// tombstones guards the in-memory tombstone set. Tombstones are loaded
	// before entries on startup so deleted entries never briefly resurrect.
	tombstoneMu sync.RWMutex
	tombstones  map[session.ID]tombstone
	// monotonic is a strictly increasing local counter used to judge tombstone
	// expiry even if the wall clock moves backwards.
	monotonicMu      sync.Mutex
	monotonicCounter uint64

	// tokenSecret authenticates page tokens.
	tokenSecret []byte
}

// NewBackendIndex creates an index and loads tombstones and entries from the
// backend. Tombstones are loaded first, satisfying the restart ordering
// requirement.
func NewBackendIndex(ctx context.Context, cfg BackendIndexConfig) (*BackendIndex, error) {
	if err := cfg.Check(); err != nil {
		return nil, trace.Wrap(err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, trace.Wrap(err)
	}

	idx := &BackendIndex{
		backend:     cfg.Backend,
		clock:       cfg.Clock,
		metrics:     cfg.Metrics,
		tombstones:  make(map[session.ID]tombstone),
		tokenSecret: secret,
		cache: sortcache.New(sortcache.Config[Entry, cacheIndex]{
			Indexes: map[cacheIndex]func(Entry) string{
				indexSession: sessionCacheKey,
				indexStart:   startCacheKey,
			},
		}),
	}

	if err := idx.load(ctx); err != nil {
		return nil, trace.Wrap(err)
	}
	return idx, nil
}

func sessionCacheKey(e Entry) string {
	return string(e.SessionID)
}

// startCacheKey builds a fixed-width composite key so lexicographic order on
// the sortcache index equals (StartTime, SessionID) order.
func startCacheKey(e Entry) string {
	return fixedWidthNano(e.StartTime.UnixNano()) + "/" + string(e.SessionID)
}

func fixedWidthNano(nanos int64) string {
	// Offset by a constant so pre-1970 timestamps sort correctly, then
	// zero-pad to a fixed width. Max int64 has 19 digits.
	const offset = int64(1) << 62
	return padUint(uint64(nanos+offset), startNanoWidth)
}

func padUint(v uint64, width int) string {
	digits := []byte("0000000000000000000")
	tmp := v
	pos := len(digits) - 1
	for pos >= 0 {
		digits[pos] = byte('0' + tmp%10)
		tmp /= 10
		pos--
		if tmp == 0 {
			break
		}
	}
	return string(digits[len(digits)-width:])
}

// load reads tombstones first and then entries from the backend.
func (i *BackendIndex) load(ctx context.Context) error {
	// Phase 1: load tombstones before any entry can be cached.
	tombResult, err := i.backend.GetRange(ctx,
		backend.NewKey(tombstonePrefix),
		backend.RangeEnd(backend.NewKey(tombstonePrefix)),
		backend.NoLimit)
	if err != nil {
		i.recordBackendError("load_tombstones")
		return classifyBackendError(err)
	}
	now := i.clock.Now()
	i.tombstoneMu.Lock()
	for _, item := range tombResult.Items {
		t, decodeErr := decodeTombstone(item.Value)
		if decodeErr != nil {
			continue
		}
		sid := session.ID(t.SessionID)
		if !item.Expires.IsZero() && now.After(item.Expires) {
			// Expired tombstone: drop it and clean it up best-effort.
			_ = i.backend.Delete(ctx, item.Key)
			continue
		}
		i.tombstones[sid] = t
	}
	i.tombstoneMu.Unlock()

	// Phase 2: load entries, skipping any session covered by a tombstone.
	entryResult, err := i.backend.GetRange(ctx,
		backend.NewKey(keyPrefix),
		backend.RangeEnd(backend.NewKey(keyPrefix)),
		backend.NoLimit)
	if err != nil {
		i.recordBackendError("load_entries")
		return classifyBackendError(err)
	}
	count := 0
	for _, item := range entryResult.Items {
		_, sid, keyErr := parseEntryKey(item.Key)
		if keyErr != nil {
			continue
		}
		if i.isTombstoned(sid) {
			continue
		}
		entry, decodeErr := decodeEntry(item.Value)
		if decodeErr != nil {
			continue
		}
		i.cache.Put(entry)
		count++
	}
	i.recordEntryGauge(count)
	return nil
}

// Put implements Index. Writes to the same session are serialized through a
// sharded lock; backend calls happen outside the cache lock.
func (i *BackendIndex) Put(ctx context.Context, incoming Entry) (uint64, error) {
	if err := incoming.Check(); err != nil {
		return 0, trace.Wrap(err)
	}
	incoming = incoming.Clone()
	lock := i.lockFor(incoming.SessionID)
	lock.Lock()
	defer lock.Unlock()

	if i.isTombstoned(incoming.SessionID) {
		// A deleted session must not be resurrected by a late write. The
		// rebuilder can clear the tombstone by re-creating the entry through
		// RebuildPut, which checks the recorded generation.
		return 0, trace.NotFound("session %s has been deleted", incoming.SessionID)
	}

	encoded, err := encodeEntry(incoming)
	if err != nil {
		return 0, trace.Wrap(err)
	}
	// Re-check size after normalization (Check normalized participants and
	// labels).
	if len(encoded) > maxEncodedEntrySize {
		return 0, trace.BadParameter("encoded entry size %d exceeds maximum of %d bytes",
			len(encoded), maxEncodedEntrySize)
	}

	var finalRevision uint64
	var merged Entry

	// Read-modify-write with CompareAndSwap. On conflict, re-read and retry,
	// up to maxCASRetries. A missing key is created with Put; both paths
	// guarantee the revision is monotonic.
	for attempt := 0; attempt <= maxCASRetries; attempt++ {
		existing, getErr := i.readCurrent(ctx, incoming.SessionID)
		switch {
		case getErr == nil:
			merged = mergeEntries(existing, incoming)
			// Guard against revision regression: never write a revision lower
			// than the one already stored.
			if merged.Revision < existing.Revision {
				merged.Revision = existing.Revision
			}
			merged.Revision = existing.Revision + 1
		case trace.IsNotFound(getErr):
			merged = incoming.Clone()
			if merged.Revision == 0 {
				merged.Revision = 1
			}
		default:
			i.recordBackendError("put_get")
			return 0, classifyBackendError(getErr)
		}

		newValue, encodeErr := encodeEntry(merged)
		if encodeErr != nil {
			return 0, trace.Wrap(encodeErr)
		}
		newKey := entryKey(merged.ClusterName, merged.StartTime.UnixNano(), merged.SessionID)

		if getErr != nil && trace.IsNotFound(getErr) {
			lease, putErr := i.backend.Put(ctx, backend.Item{
				Key:   newKey,
				Value: newValue,
			})
			if putErr == nil {
				finalRevision = merged.Revision
				_ = lease
				break
			}
			if !trace.IsAlreadyExists(putErr) {
				i.recordBackendError("put_create")
				return 0, classifyBackendError(putErr)
			}
			// Another writer created the key between our Get and Put; retry.
			if attempt == maxCASRetries {
				return 0, NewAborted("too many conflicts writing recording index entry")
			}
			continue
		}

		expectedKey := entryKey(existing.ClusterName, existing.StartTime.UnixNano(), existing.SessionID)
		expectedValue, encodeErr := encodeEntry(existing)
		if encodeErr != nil {
			return 0, trace.Wrap(encodeErr)
		}

		if expectedKey.Compare(newKey) == 0 {
			// Same sort key: a CompareAndSwap on the old value is enough.
			_, casErr := i.backend.CompareAndSwap(ctx,
				backend.Item{Key: expectedKey, Value: expectedValue},
				backend.Item{Key: newKey, Value: newValue})
			if casErr == nil {
				finalRevision = merged.Revision
				break
			}
			if !trace.IsCompareFailed(casErr) {
				i.recordBackendError("put_cas")
				return 0, classifyBackendError(casErr)
			}
			if attempt == maxCASRetries {
				return 0, NewAborted("too many conflicts writing recording index entry")
			}
			continue
		}

		// The start time changed, so the sort key changes. CAS the old key to
		// a tombstone marker is not possible, so write the new key first and
		// only delete the old key once the new value is durable. A crash in
		// between leaves an orphaned old key that the sweeper reconciles.
		if _, putErr := i.backend.Put(ctx, backend.Item{
			Key:   newKey,
			Value: newValue,
		}); putErr != nil {
			i.recordBackendError("put_rekey")
			return 0, classifyBackendError(putErr)
		}
		if delErr := i.backend.Delete(ctx, expectedKey); delErr != nil &&
			!trace.IsNotFound(delErr) {
			// The new value is durable; stale-key cleanup retries via the
			// sweeper. Report but do not fail the write.
			i.recordBackendError("put_cleanup_old_key")
		}
		finalRevision = merged.Revision
		break
	}

	// Update the in-memory cache from the durable merged value. The cache is
	// an index of last resort for ordering, so refresh it authoritatively.
	if durable, err := i.readCurrent(ctx, incoming.SessionID); err == nil {
		merged = durable
		finalRevision = durable.Revision
	}
	i.cache.Put(merged)
	i.refreshEntryGauge()
	return finalRevision, nil
}

// readCurrent fetches an entry by session ID, combining the cache (for the
// current key) and the backend (source of truth). Tombstones take precedence
// and resolve to NotFound.
func (i *BackendIndex) readCurrent(ctx context.Context, sid session.ID) (Entry, error) {
	if i.isTombstoned(sid) {
		return Entry{}, trace.NotFound("session %s has been deleted", sid)
	}
	if cached, ok := i.cache.Get(indexSession, string(sid)); ok {
		item, err := i.backend.Get(ctx,
			entryKey(cached.ClusterName, cached.StartTime.UnixNano(), sid))
		if err == nil {
			return decodeEntry(item.Value)
		}
		if !trace.IsNotFound(err) {
			return Entry{}, trace.Wrap(err)
		}
	}
	// Fall back to a range scan over the session ID suffix when the cache key
	// is unknown or stale.
	result, err := i.backend.GetRange(ctx,
		backend.NewKey(keyPrefix),
		backend.RangeEnd(backend.NewKey(keyPrefix)),
		backend.NoLimit)
	if err != nil {
		return Entry{}, trace.Wrap(err)
	}
	for _, item := range result.Items {
		_, keySID, keyErr := parseEntryKey(item.Key)
		if keyErr != nil || keySID != sid {
			continue
		}
		return decodeEntry(item.Value)
	}
	return Entry{}, trace.NotFound("recording index entry for session %s not found", sid)
}

// Delete implements Index. It writes a 24h tombstone and removes the entry.
func (i *BackendIndex) Delete(ctx context.Context, sid session.ID) error {
	if _, err := session.ParseID(string(sid)); err != nil {
		return trace.BadParameter("invalid session ID: %v", err)
	}
	lock := i.lockFor(sid)
	lock.Lock()
	defer lock.Unlock()

	now := i.clock.Now()
	t := tombstone{
		SessionID: string(sid),
		DeletedAt: now.UnixNano(),
		ExpiresAt: now.Add(tombstoneTTL).UnixNano(),
	}
	i.monotonicMu.Lock()
	i.monotonicCounter++
	t.Generation = i.monotonicCounter
	i.monotonicMu.Unlock()

	tombValue, err := encodeTombstone(t)
	if err != nil {
		return trace.Wrap(err)
	}
	if _, err := i.backend.Put(ctx, backend.Item{
		Key:     tombstoneKey(sid),
		Value:   tombValue,
		Expires: now.Add(tombstoneTTL),
	}); err != nil {
		i.recordBackendError("delete_tombstone")
		return classifyBackendError(err)
	}

	// Remove the entry wherever it currently lives.
	if existing, ok := i.cache.Get(indexSession, string(sid)); ok {
		key := entryKey(existing.ClusterName, existing.StartTime.UnixNano(), sid)
		if err := i.backend.Delete(ctx, key); err != nil && !trace.IsNotFound(err) {
			i.recordBackendError("delete_entry")
			// Keep the tombstone; report the backend failure to the caller.
			return classifyBackendError(err)
		}
	} else {
		// Cache miss: scan the backend to find any orphaned key.
		if entry, err := i.scanForSession(ctx, sid); err == nil {
			key := entryKey(entry.ClusterName, entry.StartTime.UnixNano(), sid)
			if err := i.backend.Delete(ctx, key); err != nil && !trace.IsNotFound(err) {
				i.recordBackendError("delete_entry_scan")
				return classifyBackendError(err)
			}
		} else if !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
	}

	i.tombstoneMu.Lock()
	i.tombstones[sid] = t
	i.tombstoneMu.Unlock()
	i.cache.Delete(indexSession, string(sid))
	i.refreshEntryGauge()
	return nil
}

// Get implements Index. Tombstones resolve to NotFound and take precedence
// over backend errors only when the backend itself is healthy: a real backend
// failure is returned before consulting the tombstone set.
func (i *BackendIndex) Get(ctx context.Context, sid session.ID) (Entry, error) {
	if _, err := session.ParseID(string(sid)); err != nil {
		return Entry{}, trace.BadParameter("invalid session ID: %v", err)
	}
	entry, err := i.readCurrent(ctx, sid)
	if err != nil {
		if trace.IsNotFound(err) && i.isTombstoned(sid) {
			return Entry{}, trace.NotFound("session %s has been deleted", sid)
		}
		if trace.IsNotFound(err) {
			return Entry{}, trace.Wrap(err)
		}
		// Backend errors win over tombstone state.
		i.recordBackendError("get")
		return Entry{}, classifyBackendError(err)
	}
	return entry, nil
}

// scanForSession does a full prefix scan looking for one session ID.
func (i *BackendIndex) scanForSession(ctx context.Context, sid session.ID) (Entry, error) {
	result, err := i.backend.GetRange(ctx,
		backend.NewKey(keyPrefix),
		backend.RangeEnd(backend.NewKey(keyPrefix)),
		backend.NoLimit)
	if err != nil {
		return Entry{}, classifyBackendError(err)
	}
	for _, item := range result.Items {
		_, keySID, keyErr := parseEntryKey(item.Key)
		if keyErr != nil || keySID != sid {
			continue
		}
		return decodeEntry(item.Value)
	}
	return Entry{}, trace.NotFound("recording index entry for session %s not found", sid)
}

// List implements keyset pagination over the in-memory ordered cache.
func (i *BackendIndex) List(ctx context.Context, query Query, rawToken string) (page Page, err error) {
	start := i.clock.Now()
	defer func() {
		if i.metrics != nil {
			i.metrics.ObserveList(query.Protocol, i.clock.Since(start))
		}
	}()

	if err := query.Check(); err != nil {
		return Page{}, trace.Wrap(err)
	}
	token, err := verifyPageToken(rawToken, query, i.clock.Now(), i.tokenSecret)
	if err != nil {
		return Page{}, trace.Wrap(err)
	}

	entries := make([]Entry, 0, query.PageSize)
	predicate := PredicateFromContext(ctx)
	seq := i.iterate(query, token)
	for entry := range seq {
		// Authorization filtering happens before counting toward PageSize, so
		// invisible entries cannot be inferred from totals. The predicate is
		// evaluated outside all index and cache locks.
		if predicate != nil && !predicate(entry) {
			continue
		}
		entries = append(entries, entry)
		if len(entries) >= query.PageSize {
			break
		}
	}

	nextToken := ""
	if len(entries) == query.PageSize {
		last := entries[len(entries)-1]
		next := pageToken{
			Fingerprint:   query.Fingerprint(),
			LastStartNano: last.StartTime.UnixNano(),
			LastSessionID: string(last.SessionID),
			Order:         query.Order,
			ExpiresAtNano: i.clock.Now().Add(tokenTTL).UnixNano(),
		}
		signed, signErr := signPageToken(next, i.tokenSecret)
		if signErr != nil {
			return Page{}, trace.Wrap(signErr)
		}
		nextToken = signed
	}
	return Page{Entries: entries, NextToken: nextToken}, nil
}

// iterate yields entries matching the query in fully deterministic order,
// starting after the page token position.
func (i *BackendIndex) iterate(query Query, token pageToken) iter.Seq[Entry] {
	startKey, stopKey := i.iterationBounds(query, token)
	var seq iter.Seq[Entry]
	if query.Order == OrderDescending {
		seq = i.cache.Descend(indexStart, startKey, stopKey)
	} else {
		seq = i.cache.Ascend(indexStart, startKey, stopKey)
	}
	return func(yield func(Entry) bool) {
		for entry := range seq {
			entry = entry.Clone()
			if i.isTombstoned(entry.SessionID) {
				continue
			}
			if !query.matches(entry) {
				continue
			}
			if !yield(entry) {
				return
			}
		}
	}
} // iterationBounds computes the sortcache start/stop keys for a query and a
// resume position. Ascend treats start as inclusive and stop as exclusive;
// Descend inverts the same pair.
func (i *BackendIndex) iterationBounds(query Query, token pageToken) (start, stop string) {
	fromNano := int64(0)
	toNano := int64(0)
	if !query.From.IsZero() {
		fromNano = query.From.UnixNano()
	}
	if !query.To.IsZero() {
		toNano = query.To.UnixNano()
	}

	if token.LastSessionID != "" {
		// Resume strictly past the last returned keyset position in the
		// traversal direction.
		lastKey := fixedWidthNano(token.LastStartNano) + "/" + token.LastSessionID
		if query.Order == OrderAscending {
			return sortcache.NextKey(lastKey), upperBound(toNano)
		}
		return prevKey(lastKey), lowerBound(fromNano)
	}

	if query.Order == OrderAscending {
		return lowerBound(fromNano), upperBound(toNano)
	}
	// Descending: start just below the exclusive To boundary and stop at the
	// inclusive From boundary. The From-side exclusion of the boundary is
	// handled by the query.matches time check.
	return descStartBound(toNano), lowerBound(fromNano)
}

// lowerBound returns the smallest sortcache key at or after nanos.
func lowerBound(nanos int64) string {
	if nanos == 0 {
		return ""
	}
	return fixedWidthNano(nanos) + "/"
}

// upperBound returns an exclusive sortcache boundary for nanos. Keys whose
// timestamp component equals nanos and whose session suffix is non-empty sort
// strictly below the returned key.
func upperBound(nanos int64) string {
	if nanos == 0 {
		return ""
	}
	return fixedWidthNano(nanos) + "0"
}

// descStartBound returns the greatest key strictly below the exclusive To
// boundary.
func descStartBound(nanos int64) string {
	if nanos == 0 {
		return ""
	}
	return prevKey(fixedWidthNano(nanos) + "0")
}

// prevKey returns the lexicographic key immediately preceding key for keys of
// the same length.
func prevKey(key string) string {
	end := []byte(key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] > 0 {
			end[i]--
			return string(end)
		}
	}
	return ""
}

func (i *BackendIndex) isTombstoned(sid session.ID) bool {
	i.tombstoneMu.RLock()
	_, ok := i.tombstones[sid]
	i.tombstoneMu.RUnlock()
	return ok
}

// forgetTombstone removes an in-memory tombstone after it expires.
func (i *BackendIndex) forgetTombstone(sid session.ID) {
	i.tombstoneMu.Lock()
	delete(i.tombstones, sid)
	i.tombstoneMu.Unlock()
}

func (i *BackendIndex) lockFor(sid session.ID) *sync.Mutex {
	h := fnvHash(string(sid))
	return &i.writeLocks[h%lockShardCount]
}

func fnvHash(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for _, b := range []byte(s) {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}

func (i *BackendIndex) refreshEntryGauge() {
	i.recordEntryGauge(i.cache.Len())
}

func (i *BackendIndex) recordEntryGauge(count int) {
	if i.metrics != nil {
		i.metrics.EntryGauge(count)
	}
}

func (i *BackendIndex) recordBackendError(op string) {
	if i.metrics != nil {
		i.metrics.ObserveBackendError(op)
	}
}

// classifyBackendError maps backend failures onto the fixed error taxonomy.
// trace errors keep their type; unknown failures and context deadlines become
// Unavailable.
func classifyBackendError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case trace.IsNotFound(err), trace.IsAlreadyExists(err), trace.IsCompareFailed(err),
		trace.IsBadParameter(err), trace.IsAccessDenied(err):
		return trace.Wrap(err)
	default:
		return NewUnavailable("recording index backend unavailable: %v", err)
	}
}
