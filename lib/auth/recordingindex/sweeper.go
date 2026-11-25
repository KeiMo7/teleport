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
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"

	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/session"
)

const (
	// sweepInterval is how often the tombstone sweeper runs.
	sweepInterval = time.Hour
)

// Sweeper removes expired tombstones. Backend tombstones carry an Expires time
// enforced by the backend itself; the sweeper also drops them from the
// in-memory set so restarts do not keep paying the lookup cost.
type Sweeper struct {
	index   *BackendIndex
	backend backend.Backend
	clock   clockwork.Clock
	logger  *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// NewSweeper starts a background tombstone sweeper.
func NewSweeper(index *BackendIndex, b backend.Backend, clock clockwork.Clock, logger *slog.Logger) *Sweeper {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sweeper{
		index:   index,
		backend: b,
		clock:   clock,
		logger:  logger,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go s.run(ctx)
	return s
}

// Close stops the sweeper.
func (s *Sweeper) Close() {
	s.cancel()
	<-s.done
}

func (s *Sweeper) run(ctx context.Context) {
	defer close(s.done)
	ticker := s.clock.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if err := s.Sweep(ctx); err != nil {
				s.logger.WarnContext(ctx, "recording index tombstone sweep failed", "error", err)
			}
		}
	}
}

// Sweep deletes expired tombstones from both the backend and the in-memory
// set.
func (s *Sweeper) Sweep(ctx context.Context) error {
	result, err := s.backend.GetRange(ctx,
		backend.NewKey(tombstonePrefix),
		backend.RangeEnd(backend.NewKey(tombstonePrefix)),
		backend.NoLimit)
	if err != nil {
		return classifyBackendError(err)
	}
	now := s.clock.Now()
	for _, item := range result.Items {
		if item.Expires.IsZero() || now.Before(item.Expires) {
			continue
		}
		if err := s.backend.Delete(ctx, item.Key); err != nil && !trace.IsNotFound(err) {
			s.logger.WarnContext(ctx, "failed to delete expired tombstone", "error", err)
		}
		components := item.Key.Components()
		if len(components) >= 2 {
			sid := session.ID(components[len(components)-1])
			s.index.forgetTombstone(sid)
		}
	}
	return nil
}
