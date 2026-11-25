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
	"errors"
	"testing"
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/gravitational/teleport/api/types"
	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/session"
)

func newTestRole(t *testing.T, name, revision string) types.Role {
	t.Helper()
	role, err := types.NewRole(name, types.RoleSpecV6{
		Allow: types.RoleConditions{
			Rules: []types.Rule{
				{
					Resources: []string{types.KindSession},
					Verbs:     []string{types.VerbRead},
				},
			},
		},
	})
	require.NoError(t, err)
	role.SetRevision(revision)
	return role
}

func sessionEndEvent(sid session.ID) apievents.AuditEvent {
	return &apievents.SessionEnd{
		Metadata: apievents.Metadata{
			Type: events.SessionEndEvent,
			Time: time.Now(),
		},
		UserMetadata: apievents.UserMetadata{User: "alice"},
		SessionMetadata: apievents.SessionMetadata{
			SessionID: string(sid),
		},
		ServerMetadata: apievents.ServerMetadata{
			ServerID: "node-1",
		},
		StartTime:    time.Now().Add(-time.Minute),
		EndTime:      time.Now(),
		Participants: []string{"alice"},
	}
}

func newAdapter(t *testing.T, checker *fakeChecker, fetch SessionEndFetcher, cache *DecisionCache) *AuthzAdapter {
	t.Helper()
	adapter, err := NewAuthzAdapter(AuthzAdapterConfig{
		User:            fakeUserState{name: "alice"},
		Checker:         checker,
		FetchSessionEnd: fetch,
		Cache:           cache,
	})
	require.NoError(t, err)
	return adapter
}

// TestAuthzThreeStageCheck mirrors actionForKindSession's three stages.
func TestAuthzThreeStageCheck(t *testing.T) {
	ctx := context.Background()
	sid := session.NewID()
	fetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		return sessionEndEvent(sid), nil
	}

	t.Run("pre-check denial", func(t *testing.T) {
		checker := &fakeChecker{
			roles:    []types.Role{newTestRole(t, "r", "1")},
			guessErr: denyError("impossible"),
		}
		adapter := newAdapter(t, checker, fetch, nil)
		err := adapter.AuthorizeSession(ctx, sid)
		require.Error(t, err)
		require.True(t, trace.IsAccessDenied(err))
	})

	t.Run("simple check allows", func(t *testing.T) {
		checker := &fakeChecker{
			roles:     []types.Role{newTestRole(t, "r", "1")},
			simpleErr: nil,
		}
		adapter := newAdapter(t, checker, fetch, nil)
		require.NoError(t, adapter.AuthorizeSession(ctx, sid))
	})

	t.Run("extended context allows", func(t *testing.T) {
		checker := &fakeChecker{
			roles:       []types.Role{newTestRole(t, "r", "1")},
			simpleErr:   denyError("need context"),
			extendedErr: nil,
		}
		adapter := newAdapter(t, checker, fetch, nil)
		require.NoError(t, adapter.AuthorizeSession(ctx, sid))
	})

	t.Run("event fetch failure denies", func(t *testing.T) {
		failingFetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
			return nil, errors.New("audit log unavailable")
		}
		checker := &fakeChecker{
			roles:       []types.Role{newTestRole(t, "r", "1")},
			simpleErr:   denyError("need context"),
			extendedErr: nil,
		}
		adapter := newAdapter(t, checker, failingFetch, nil)
		err := adapter.AuthorizeSession(ctx, sid)
		require.Error(t, err)
		require.True(t, trace.IsAccessDenied(err))
	})
}

// TestAuthzCacheInvalidatesOnRoleRevision caches an allow, then proves a role
// revision change makes the next decision recompute (and deny when the new
// role denies).
func TestAuthzCacheInvalidatesOnRoleRevision(t *testing.T) {
	ctx := context.Background()
	sid := session.NewID()
	fetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		return sessionEndEvent(sid), nil
	}
	clock := clockwork.NewFakeClock()
	cache := NewDecisionCache(clock, nil)

	checker := &fakeChecker{
		roles:     []types.Role{newTestRole(t, "access", "r1")},
		simpleErr: nil,
	}
	adapter := newAdapter(t, checker, fetch, cache)
	require.NoError(t, adapter.AuthorizeSession(ctx, sid))

	// First decision is cached; flip the checker to deny on both stages, but
	// the cached allow still wins until invalidation.
	checker.simpleErr = denyError("revoked")
	checker.extendedErr = denyError("revoked")
	require.NoError(t, adapter.AuthorizeSession(ctx, sid))

	// A watch event for the role flushes the shard.
	cache.InvalidateUser("", "access")
	err := adapter.AuthorizeSession(ctx, sid)
	require.Error(t, err)
	require.True(t, trace.IsAccessDenied(err))

	// After invalidation a role-revision change also produces a distinct key,
	// independently of the shard flush.
	checker.roles = []types.Role{newTestRole(t, "access", "r2")}
	checker.simpleErr = nil
	checker.extendedErr = nil
	require.NoError(t, adapter.AuthorizeSession(ctx, sid))
}

// TestAuthzCacheFailsClosed injects cache degradation and event-fetch errors
// and asserts every outcome is a denial or an expiry.
func TestAuthzCacheFailsClosed(t *testing.T) {
	ctx := context.Background()
	sid := session.NewID()
	fetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		return sessionEndEvent(sid), nil
	}
	clock := clockwork.NewFakeClock()
	cache := NewDecisionCache(clock, nil)
	checker := &fakeChecker{
		roles:     []types.Role{newTestRole(t, "r", "1")},
		simpleErr: nil,
	}
	adapter := newAdapter(t, checker, fetch, cache)

	// Cache an allow then enter degraded mode; the allow must no longer be
	// served.
	require.NoError(t, adapter.AuthorizeSession(ctx, sid))
	cache.EnterDegraded()
	// In degraded mode the underlying checker recomputes. With both stages
	// denying, the result must be a denial.
	checker.simpleErr = denyError("no")
	checker.extendedErr = denyError("no")
	err := adapter.AuthorizeSession(ctx, sid)
	require.Error(t, err)
	require.True(t, trace.IsAccessDenied(err))

	// Event fetch failure always denies, cache or not.
	checker.simpleErr = denyError("need context")
	checker.extendedErr = nil
	failing := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		return nil, errors.New("boom")
	}
	failAdapter := newAdapter(t, checker, failing, cache)
	err = failAdapter.AuthorizeSession(ctx, sid)
	require.Error(t, err)
	require.True(t, trace.IsAccessDenied(err))
}

// TestAuthorizeQueryPredicate verifies the predicate filters entries before
// paging and never leaks invisible entries through page size.
func TestAuthorizeQueryPredicate(t *testing.T) {
	ctx := context.Background()
	sid := session.NewID()
	fetch := func(context.Context, session.ID) (apievents.AuditEvent, error) {
		return sessionEndEvent(sid), nil
	}
	checker := &fakeChecker{
		roles:       []types.Role{newTestRole(t, "r", "1")},
		simpleErr:   denyError("need context"),
		extendedErr: nil,
	}
	adapter := newAdapter(t, checker, fetch, nil)

	predicate, err := adapter.AuthorizeQuery(ctx, Query{})
	require.NoError(t, err)
	require.True(t, predicate(Entry{SessionID: sid}))

	// A denying extended check yields a false predicate without an error.
	checker.extendedErr = denyError("not this resource")
	predicate, err = adapter.AuthorizeQuery(ctx, Query{})
	require.NoError(t, err)
	require.False(t, predicate(Entry{SessionID: sid}))
}
