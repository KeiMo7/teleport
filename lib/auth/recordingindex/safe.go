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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime/debug"

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport/lib/session"
)

// SafeIndex wraps an Index and recovers from unexpected panics in token
// parsing, paging and filtering, converting them into Internal errors carrying
// a request fingerprint for troubleshooting. Recording identifiers are never
// included in the returned error.
type SafeIndex struct {
	inner Index
}

// NewSafeIndex wraps an index with boundary-level panic recovery.
func NewSafeIndex(inner Index) *SafeIndex {
	return &SafeIndex{inner: inner}
}

// Put implements Index.
func (s *SafeIndex) Put(ctx context.Context, entry Entry) (revision uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = s.internalError("put", r)
		}
	}()
	return s.inner.Put(ctx, entry)
}

// Delete implements Index.
func (s *SafeIndex) Delete(ctx context.Context, sid session.ID) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = s.internalError("delete", r)
		}
	}()
	return s.inner.Delete(ctx, sid)
}

// Get implements Index.
func (s *SafeIndex) Get(ctx context.Context, sid session.ID) (entry Entry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = s.internalError("get", r)
		}
	}()
	return s.inner.Get(ctx, sid)
}

// List implements Index.
func (s *SafeIndex) List(ctx context.Context, query Query, pageToken string) (page Page, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = s.internalError("list", r)
		}
	}()
	return s.inner.List(ctx, query, pageToken)
}

func (s *SafeIndex) internalError(operation string, recovered any) error {
	// The fingerprint is derived from the stack and operation, never from
	// session IDs or other recording identifiers.
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%v:%s", operation, recovered, debug.Stack())))
	fingerprint := hex.EncodeToString(h[:8])
	return trace.Errorf("internal recording index error in %s (fingerprint %s)", operation, fingerprint)
}
