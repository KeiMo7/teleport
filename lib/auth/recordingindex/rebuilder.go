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

	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/player"
	"github.com/gravitational/teleport/lib/session"
)

const (
	// watermarkLagTolerance bounds how far a rebuild watermark may lag behind
	// the newest processed event before the next run is expected to catch up.
	watermarkLagTolerance = 5 * time.Minute
)

// SessionEndExtractor finds the session end event for one session. It defaults
// to events.FindSessionEndEvent and can be replaced in tests.
type SessionEndExtractor func(ctx context.Context, streamer player.Streamer, sid session.ID) (apievents.AuditEvent, error)

// Rebuilder reconstructs index entries from recorded sessions. It is safe to
// run repeatedly: a rebuild never moves a revision backwards because Put
// merges field-by-field and only increments the revision.
type Rebuilder struct {
	index    *BackendIndex
	backend  backend.Backend
	streamer player.Streamer
	clock    clockwork.Clock
	logger   *slog.Logger

	extractSessionEnd SessionEndExtractor
}

// RebuilderConfig configures a Rebuilder.
type RebuilderConfig struct {
	Index    *BackendIndex
	Backend  backend.Backend
	Streamer player.Streamer
	Clock    clockwork.Clock
	Logger   *slog.Logger
}

// NewRebuilder builds a Rebuilder.
func NewRebuilder(cfg RebuilderConfig) (*Rebuilder, error) {
	switch {
	case cfg.Index == nil:
		return nil, trace.BadParameter("index is required")
	case cfg.Backend == nil:
		return nil, trace.BadParameter("backend is required")
	case cfg.Streamer == nil:
		return nil, trace.BadParameter("streamer is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Rebuilder{
		index:             cfg.Index,
		backend:           cfg.Backend,
		streamer:          cfg.Streamer,
		clock:             cfg.Clock,
		logger:            cfg.Logger,
		extractSessionEnd: defaultExtractSessionEnd,
	}, nil
}

func defaultExtractSessionEnd(ctx context.Context, streamer player.Streamer, sid session.ID) (apievents.AuditEvent, error) {
	return events.FindSessionEndEvent(ctx, streamer, sid)
}

// RebuildWindow re-indexes the supplied sessions for one cluster. Only
// sessions whose StartTime falls in [from, to) are written. Each Put is
// idempotent and revisions never move backwards.
func (r *Rebuilder) RebuildWindow(ctx context.Context, clusterName string, from, to time.Time, sessionIDs []session.ID) error {
	if clusterName == "" {
		return trace.BadParameter("cluster name is required")
	}
	if !to.IsZero() && from.After(to) {
		return trace.BadParameter("rebuild window From %s is after To %s", from, to)
	}

	var highestNano int64
	for _, sid := range sessionIDs {
		if ctx.Err() != nil {
			return trace.Wrap(ctx.Err())
		}
		event, err := r.extractSessionEnd(ctx, r.streamer, sid)
		if err != nil {
			// One unreadable session must not fail the whole window; record
			// and continue. Missing recordings move the entry to Missing.
			r.logger.WarnContext(ctx, "failed to read session during rebuild",
				"cluster", clusterName, "error", err)
			r.markMissing(ctx, clusterName, sid)
			continue
		}
		entry, err := EntryFromSessionEndEvent(event, clusterName)
		if err != nil {
			r.logger.WarnContext(ctx, "failed to build entry during rebuild", "error", err)
			continue
		}
		if !r.withinWindow(entry, from, to) {
			continue
		}
		if _, err := r.index.Put(ctx, entry); err != nil {
			return trace.Wrap(err)
		}
		if entry.EndTime.UnixNano() > highestNano {
			highestNano = entry.EndTime.UnixNano()
		}
	}

	if highestNano > 0 {
		if err := r.advanceWatermark(ctx, clusterName, highestNano); err != nil {
			return trace.Wrap(err)
		}
	}
	return nil
}

// Watermark returns the persisted rebuild position for a cluster.
func (r *Rebuilder) Watermark(ctx context.Context, clusterName string) (time.Time, error) {
	item, err := r.backend.Get(ctx, watermarkKey(clusterName))
	if err != nil {
		if trace.IsNotFound(err) {
			return time.Time{}, nil
		}
		return time.Time{}, classifyBackendError(err)
	}
	w, err := decodeWatermark(item.Value)
	if err != nil {
		return time.Time{}, trace.Wrap(err)
	}
	return time.Unix(0, w.PositionNano), nil
}

// WatermarkLag reports how far the persisted watermark sits behind the given
// reference time. It is used to verify the 5-minute lag bound.
func (r *Rebuilder) WatermarkLag(ctx context.Context, clusterName string, reference time.Time) (time.Duration, error) {
	position, err := r.Watermark(ctx, clusterName)
	if err != nil {
		return 0, trace.Wrap(err)
	}
	if position.IsZero() {
		return 0, nil
	}
	lag := reference.Sub(position)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

func (r *Rebuilder) advanceWatermark(ctx context.Context, clusterName string, positionNano int64) error {
	key := watermarkKey(clusterName)

	// Never move the watermark backwards.
	if existing, err := r.backend.Get(ctx, key); err == nil {
		previous, decodeErr := decodeWatermark(existing.Value)
		if decodeErr == nil && previous.PositionNano >= positionNano {
			return nil
		}
	} else if !trace.IsNotFound(err) {
		return classifyBackendError(err)
	}

	value, err := encodeWatermark(watermark{
		ClusterName:   clusterName,
		PositionNano:  positionNano,
		UpdatedAtNano: r.clock.Now().UnixNano(),
	})
	if err != nil {
		return trace.Wrap(err)
	}
	if _, err := r.backend.Put(ctx, backend.Item{Key: key, Value: value}); err != nil {
		return classifyBackendError(err)
	}
	return nil
}

func (r *Rebuilder) withinWindow(entry Entry, from, to time.Time) bool {
	if !from.IsZero() && entry.StartTime.Before(from) {
		return false
	}
	if !to.IsZero() && !entry.StartTime.Before(to) {
		return false
	}
	return true
}

// markMissing moves an entry to the Missing metadata state when its recording
// cannot be read. A later successful rebuild can move it back to Pending.
func (r *Rebuilder) markMissing(ctx context.Context, clusterName string, sid session.ID) {
	entry := Entry{
		SessionID:     sid,
		ClusterName:   clusterName,
		Protocol:      ProtocolUnknown,
		MetadataState: MetadataStateMissing,
		StartTime:     r.clock.Now(),
	}
	if _, err := r.index.Put(ctx, entry); err != nil {
		r.logger.WarnContext(ctx, "failed to mark entry missing", "error", err)
	}
}
