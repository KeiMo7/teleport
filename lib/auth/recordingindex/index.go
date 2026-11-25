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
	"time"

	"github.com/gravitational/teleport/lib/session"
)

// Page is one page of List results.
type Page struct {
	// Entries are visible entries in fully deterministic order.
	Entries []Entry `json:"entries"`
	// NextToken is an opaque keyset pagination token. It is empty when there
	// are no more results.
	NextToken string `json:"next_token,omitempty"`
	// Partial is true when one or more clusters could not be read during a
	// cross-cluster query. Failed clusters are listed in FailedClusters.
	Partial bool `json:"partial,omitempty"`
	// FailedClusters lists clusters that could not be queried.
	FailedClusters []string `json:"failed_clusters,omitempty"`
}

// Index is the storage interface for recording index entries.
type Index interface {
	// Put writes an entry, merging it field-by-field with any existing entry
	// for the same session, and returns the resulting revision.
	Put(ctx context.Context, entry Entry) (uint64, error)
	// Delete removes an entry and writes a tombstone so a concurrent
	// re-index cannot resurrect it.
	Delete(ctx context.Context, sid session.ID) error
	// Get returns a single entry. A tombstone or a missing entry both resolve
	// to NotFound.
	Get(ctx context.Context, sid session.ID) (Entry, error)
	// List returns a page of entries matching the query, filtered through the
	// predicate before paging.
	List(ctx context.Context, query Query, pageToken string) (Page, error)
}

// Authorizer authorizes access to individual recordings and to index queries.
type Authorizer interface {
	// AuthorizeSession decides whether the caller may read one recording.
	// It never reveals whether the recording exists on denial.
	AuthorizeSession(ctx context.Context, sessionID session.ID) error
	// AuthorizeQuery returns a Predicate expressing which entries the caller
	// may read, to be applied during List.
	AuthorizeQuery(ctx context.Context, query Query) (Predicate, error)
}

// tokenTTL bounds how long a page token remains valid.
const tokenTTL = 10 * time.Minute
