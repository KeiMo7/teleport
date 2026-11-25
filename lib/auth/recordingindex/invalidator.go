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

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport/api/types"
	"github.com/gravitational/teleport/lib/backend"
)

// WatchInvalidator consumes the existing lib/cache watch events for roles,
// users and access requests and drives precise decision-cache invalidation.
// It does not introduce any new watch kind, so older auth servers that do not
// know about the index keep working unchanged.
type WatchInvalidator struct {
	backend backend.Backend
	cache   *DecisionCache
	logger  *slog.Logger
}

// NewWatchInvalidator builds an invalidator.
func NewWatchInvalidator(b backend.Backend, cache *DecisionCache, logger *slog.Logger) *WatchInvalidator {
	if logger == nil {
		logger = slog.Default()
	}
	return &WatchInvalidator{backend: b, cache: cache, logger: logger}
}

// Run blocks consuming watch events until the watcher closes or ctx is
// canceled. A watch outage degrades the cache; a successful reconnect flushes
// every shard.
func (w *WatchInvalidator) Run(ctx context.Context) error {
	watcher, err := w.backend.NewWatcher(ctx, backend.Watch{
		Name: "recordingindex_authz_invalidator",
		Prefixes: []backend.Key{
			backend.NewKey(types.KindRole),
			backend.NewKey(types.KindUser),
			backend.NewKey(types.KindAccessRequest),
		},
		QueueSize: 1024,
	})
	if err != nil {
		w.cache.EnterDegraded()
		return trace.Wrap(err)
	}
	defer watcher.Close()

	for {
		select {
		case <-ctx.Done():
			return trace.Wrap(ctx.Err())
		case <-watcher.Done():
			// Watch outage: stop accepting allows and let existing allows
			// expire, then surface the failure.
			w.cache.EnterDegraded()
			return trace.ConnectionProblem(nil, "recording index watcher closed")
		case event, ok := <-watcher.Events():
			if !ok {
				w.cache.EnterDegraded()
				return trace.ConnectionProblem(nil, "recording index watcher event channel closed")
			}
			w.handleEvent(event)
		}
	}
}

// handleEvent maps a backend watch event onto a precise cache invalidation.
// Unknown kinds are ignored so mixed-version clusters never fail on them.
func (w *WatchInvalidator) handleEvent(event backend.Event) {
	switch event.Item.Key.Components()[0] {
	case types.KindRole:
		// A role revision change can affect many users; without a user mapping
		// for every cached decision, flush conservatively. The cache treats
		// this as an invalidation and rebuilds on demand.
		w.cache.InvalidateUser("", roleNameFromKey(event.Item.Key))
	case types.KindUser:
		w.cache.InvalidateUser(roleNameFromKey(event.Item.Key))
	case types.KindAccessRequest:
		w.cache.InvalidateUser(roleNameFromKey(event.Item.Key))
	default:
		// Ignore unknown kinds for forward/backward compatibility.
	}
}

func roleNameFromKey(key backend.Key) string {
	components := key.Components()
	if len(components) == 0 {
		return ""
	}
	return components[len(components)-1]
}

// Recover is invoked once a watcher has been re-established. It flushes the
// whole cache so decisions computed against potentially stale state cannot be
// reused.
func (w *WatchInvalidator) Recover() {
	w.cache.Recover()
}
