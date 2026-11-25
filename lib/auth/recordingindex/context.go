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

	apievents "github.com/gravitational/teleport/api/types/events"
)

// predicateCtxKey is the context key carrying an authorization Predicate for
// a List request. It keeps the Index.List signature fixed while letting
// AuthorizeQuery's predicate filter entries before they are counted toward
// PageSize.
type predicateCtxKey struct{}

// WithPredicate attaches an authorization predicate to a context. List applies
// it to every candidate entry before paging, so filtered entries do not count
// toward PageSize and cannot be inferred from result totals.
func WithPredicate(ctx context.Context, predicate Predicate) context.Context {
	if predicate == nil {
		return ctx
	}
	return context.WithValue(ctx, predicateCtxKey{}, predicate)
}

// PredicateFromContext returns the predicate attached to ctx, or nil.
func PredicateFromContext(ctx context.Context) Predicate {
	predicate, _ := ctx.Value(predicateCtxKey{}).(Predicate)
	return predicate
}

// sessionEndMemoCtxKey carries a request-scoped memo of session end events so
// repeated authorization decisions within one request reuse a single fetch.
type sessionEndMemoCtxKey struct{}

// sessionEndMemo caches fetched session end events for one request.
type sessionEndMemo struct {
	events map[string]apievents.AuditEvent
}

// WithSessionEndMemo ensures ctx carries a request-scoped session end event
// memo. Repeated calls return the same memo, matching the requirement that one
// request never reads the same session end event twice.
func WithSessionEndMemo(ctx context.Context) context.Context {
	if ctx.Value(sessionEndMemoCtxKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, sessionEndMemoCtxKey{}, &sessionEndMemo{
		events: make(map[string]apievents.AuditEvent),
	})
}

func memoFromContext(ctx context.Context) *sessionEndMemo {
	memo, _ := ctx.Value(sessionEndMemoCtxKey{}).(*sessionEndMemo)
	return memo
}
