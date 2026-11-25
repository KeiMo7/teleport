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

	"github.com/gravitational/trace"

	apidefaults "github.com/gravitational/teleport/api/defaults"
	"github.com/gravitational/teleport/api/types"
	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/session"
)

// SessionEndFetcher returns the session end event for a session. The same
// request must reuse a single fetch even when several authorizations run.
type SessionEndFetcher func(ctx context.Context, sid session.ID) (apievents.AuditEvent, error)

// AccessEvaluator bundles the RBAC primitives the adapter needs. It is a
// narrow slice of services.AccessChecker so the adapter stays testable without
// a full auth server.
type AccessEvaluator interface {
	services.AccessChecker
}

// AuthzAdapterConfig configures an AuthzAdapter.
type AuthzAdapterConfig struct {
	// User is the authenticated user state used to build rule contexts.
	User services.UserState
	// Checker performs the RBAC checks.
	Checker AccessEvaluator
	// FetchSessionEnd loads the session end event for the extended-context
	// check. Failures are treated as denials, never skipped.
	FetchSessionEnd SessionEndFetcher
	// Cache is the decision cache. Optional; when nil every decision is
	// computed directly.
	Cache *DecisionCache
}

// Check validates the configuration.
func (c *AuthzAdapterConfig) Check() error {
	switch {
	case c.Checker == nil:
		return trace.BadParameter("checker is required")
	case c.FetchSessionEnd == nil:
		return trace.BadParameter("session end fetcher is required")
	}
	return nil
}

// AuthzAdapter implements Authorizer using the same three-stage check as
// actionForKindSession: a GuessIfAccessIsPossible pre-check, a simple rule
// check, and an extended-context check built from the session end event.
type AuthzAdapter struct {
	user            services.UserState
	checker         AccessEvaluator
	fetchSessionEnd SessionEndFetcher
	cache           *DecisionCache
}

// NewAuthzAdapter builds an AuthzAdapter.
func NewAuthzAdapter(cfg AuthzAdapterConfig) (*AuthzAdapter, error) {
	if err := cfg.Check(); err != nil {
		return nil, trace.Wrap(err)
	}
	return &AuthzAdapter{
		user:            cfg.User,
		checker:         cfg.Checker,
		fetchSessionEnd: cfg.FetchSessionEnd,
		cache:           cfg.Cache,
	}, nil
}

// AuthorizeSession implements Authorizer. It never reveals whether a
// recording exists on denial: every failure comes back as AccessDenied.
func (a *AuthzAdapter) AuthorizeSession(ctx context.Context, sid session.ID) error {
	if _, err := session.ParseID(string(sid)); err != nil {
		return trace.AccessDenied("invalid session ID")
	}

	if a.cache != nil {
		if decision, ok := a.cache.Lookup(a.cacheKey(sid, 0, "")); ok {
			if decision.Allow {
				return nil
			}
			return trace.AccessDenied("access denied to session recording")
		}
	}

	err := a.evaluateSession(ctx, sid)
	allow := err == nil
	if a.cache != nil {
		a.cache.Store(a.cacheKey(sid, 0, ""), allow)
	}
	if !allow {
		// Normalize every failure (including backend and event-fetch errors)
		// to AccessDenied so existence cannot be probed through error types.
		if trace.IsAccessDenied(err) {
			return trace.Wrap(err)
		}
		return trace.AccessDenied("access denied to session recording")
	}
	return nil
}

// cacheKey builds the decision-cache identity for a session check. The entry
// revision is folded in so an updated index entry invalidates stale decisions.
func (a *AuthzAdapter) cacheKey(sid session.ID, entryRevision uint64, predicateFingerprint string) cacheKey {
	return cacheKey{
		User:                 a.userName(),
		Roles:                a.roleRevisions(),
		Traits:               a.checker.Traits(),
		AccessRequestID:      a.accessRequestID(),
		SessionID:            string(sid),
		EntryRevision:        entryRevision,
		PredicateFingerprint: predicateFingerprint,
	}
}

// evaluateSession runs the unchanged three-stage check from
// actionForKindSession: pre-check, simple check, then extended-context check.
func (a *AuthzAdapter) evaluateSession(ctx context.Context, sid session.ID) error {
	// Stage 1: fast pre-check.
	if err := a.checker.GuessIfAccessIsPossible(
		&services.Context{User: a.user},
		apidefaults.Namespace, types.KindSession, types.VerbRead,
	); err != nil {
		return trace.Wrap(err)
	}

	// Stage 2: simple check without extended context.
	simpleErr := a.checker.CheckAccessToRule(
		&services.Context{User: a.user},
		apidefaults.Namespace, types.KindSession, types.VerbRead,
	)
	if simpleErr == nil {
		return nil
	}
	if !trace.IsAccessDenied(simpleErr) {
		return trace.Wrap(simpleErr)
	}

	// Stage 3: extended-context check. A failure to read the session end
	// event is a denial, never a skip. The fetch is memoized per request.
	sessionEnd, err := a.fetchSessionEndMemoized(ctx, sid)
	if err != nil {
		return trace.AccessDenied("cannot verify access to session: %v", err)
	}
	ruleCtx := &services.Context{User: a.user}
	ruleCtx.ExtendWithSessionEnd(sessionEnd, a.checker)
	return trace.Wrap(a.checker.CheckAccessToRule(
		ruleCtx, apidefaults.Namespace, types.KindSession, types.VerbRead))
}

// fetchSessionEndMemoized returns the session end event, reusing a fetch
// already performed within the same request.
func (a *AuthzAdapter) fetchSessionEndMemoized(ctx context.Context, sid session.ID) (apievents.AuditEvent, error) {
	if memo := memoFromContext(ctx); memo != nil {
		if cached, ok := memo.events[string(sid)]; ok {
			return cached, nil
		}
		event, err := a.fetchSessionEnd(ctx, sid)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		memo.events[string(sid)] = event
		return event, nil
	}
	return a.fetchSessionEnd(ctx, sid)
}

// AuthorizeQuery implements Authorizer. It returns a Predicate that evaluates
// the same RBAC rule per entry and is safe to apply during List before
// counting.
func (a *AuthzAdapter) AuthorizeQuery(ctx context.Context, query Query) (Predicate, error) {
	// Pre-check: if session read is impossible in principle, deny the whole
	// query without leaking per-entry information.
	if err := a.checker.GuessIfAccessIsPossible(
		&services.Context{User: a.user},
		apidefaults.Namespace, types.KindSession, types.VerbRead,
	); err != nil {
		return nil, trace.AccessDenied("access denied to session recordings")
	}

	// If the simple rule already grants access without an extended context,
	// every entry is readable.
	if err := a.checker.CheckAccessToRule(
		&services.Context{User: a.user},
		apidefaults.Namespace, types.KindSession, types.VerbRead,
	); err == nil {
		return allowAll, nil
	} else if !trace.IsAccessDenied(err) {
		return nil, trace.Wrap(err)
	}

	// Otherwise build a per-entry predicate performing the extended-context
	// check against each entry's reconstructed resource.
	fingerprint := query.Fingerprint()
	predicate := a.buildEntryPredicate(ctx, fingerprint)
	return predicate, nil
}

// buildEntryPredicate returns a Predicate that re-runs the extended-context
// rule for each entry. Decisions are memoized through the decision cache keyed
// on the entry revision, so stale allows cannot survive an entry update.
func (a *AuthzAdapter) buildEntryPredicate(ctx context.Context, predicateFingerprint string) Predicate {
	return func(entry Entry) bool {
		sid := entry.SessionID
		if a.cache != nil {
			if decision, ok := a.cache.Lookup(a.cacheKey(sid, entry.Revision, predicateFingerprint)); ok {
				return decision.Allow
			}
		}

		allow := a.evaluateEntry(ctx, entry)
		if a.cache != nil {
			a.cache.Store(a.cacheKey(sid, entry.Revision, predicateFingerprint), allow)
		}
		return allow
	}
}

// evaluateEntry performs the extended-context check using the session end
// event (memoized per request), so List never performs redundant network I/O
// while evaluating a predicate. The predicate itself runs outside index and
// cache locks.
func (a *AuthzAdapter) evaluateEntry(ctx context.Context, entry Entry) bool {
	sessionEnd, err := a.fetchSessionEndMemoized(ctx, entry.SessionID)
	if err != nil {
		// Event fetch failure fails closed.
		return false
	}
	ruleCtx := &services.Context{User: a.user}
	ruleCtx.ExtendWithSessionEnd(sessionEnd, a.checker)
	return a.checker.CheckAccessToRule(
		ruleCtx, apidefaults.Namespace, types.KindSession, types.VerbRead) == nil
}

func (a *AuthzAdapter) userName() string {
	if a.user != nil {
		return a.user.GetName()
	}
	return ""
}

func (a *AuthzAdapter) accessRequestID() string {
	// Access request identity, when present, is exposed through the user
	// state's roles; callers may also wrap the adapter per request. The
	// decision cache additionally binds traits and role revisions, so a
	// privilege change always invalidates a stale entry.
	return ""
}

// roleRevisions returns sorted (role name, revision) pairs so the cache key
// is stable regardless of role ordering.
func (a *AuthzAdapter) roleRevisions() []roleRevision {
	roles := a.checker.Roles()
	out := make([]roleRevision, 0, len(roles))
	for _, role := range roles {
		out = append(out, roleRevision{
			Name:     role.GetName(),
			Revision: role.GetRevision(),
		})
	}
	sortRoleRevisions(out)
	return out
}
