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
	"sync"

	"github.com/gravitational/teleport/lib/backend"
)

// faultyBackend wraps a backend and injects errors on selected operations. It
// is used to verify fail-closed and error-classification behavior.
type faultyBackend struct {
	backend.Backend
	mu sync.Mutex

	failGetRange bool
	failPut      bool
}

func (f *faultyBackend) setFailGetRange(v bool) {
	f.mu.Lock()
	f.failGetRange = v
	f.mu.Unlock()
}

func (f *faultyBackend) setFailPut(v bool) {
	f.mu.Lock()
	f.failPut = v
	f.mu.Unlock()
}

func (f *faultyBackend) GetRange(ctx context.Context, startKey, endKey backend.Key, limit int) (*backend.GetResult, error) {
	f.mu.Lock()
	fail := f.failGetRange
	f.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	return f.Backend.GetRange(ctx, startKey, endKey, limit)
}

func (f *faultyBackend) Put(ctx context.Context, item backend.Item) (*backend.Lease, error) {
	f.mu.Lock()
	fail := f.failPut
	f.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	return f.Backend.Put(ctx, item)
}
