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
	"testing"
	"time"

	"github.com/gravitational/trace"
	"github.com/stretchr/testify/require"

	apievents "github.com/gravitational/teleport/api/types/events"
	"github.com/gravitational/teleport/lib/events"
	"github.com/gravitational/teleport/lib/session"
)

// TestEntryFromSessionEndEvent covers every supported session end event type
// plus the unknown-protocol fallback.
func TestEntryFromSessionEndEvent(t *testing.T) {
	now := time.Now()
	sid := session.NewID()

	t.Run("ssh", func(t *testing.T) {
		event := &apievents.SessionEnd{
			Metadata:        apievents.Metadata{Type: events.SessionEndEvent, Time: now},
			UserMetadata:    apievents.UserMetadata{User: "alice"},
			SessionMetadata: apievents.SessionMetadata{SessionID: string(sid)},
			ServerMetadata: apievents.ServerMetadata{
				ServerID:     "node-1",
				ServerLabels: map[string]string{"env": "prod"},
			},
			Participants: []string{"alice", "carol", "alice"},
			StartTime:    now.Add(-time.Minute),
			EndTime:      now,
		}
		event.Protocol = apievents.EventProtocolSSH
		entry, err := EntryFromSessionEndEvent(event, "root.example")
		require.NoError(t, err)
		require.Equal(t, apievents.EventProtocolSSH, entry.Protocol)
		require.Equal(t, "node", entry.ResourceKind)
		require.Equal(t, "node-1", entry.ResourceName)
		require.Equal(t, []string{"alice", "carol"}, entry.Participants)
		require.Equal(t, "prod", entry.Labels["env"])
	})

	t.Run("windows desktop", func(t *testing.T) {
		event := &apievents.WindowsDesktopSessionEnd{
			Metadata:        apievents.Metadata{Type: events.WindowsDesktopSessionEndEvent, Time: now, ClusterName: "root.example"},
			UserMetadata:    apievents.UserMetadata{User: "bob"},
			SessionMetadata: apievents.SessionMetadata{SessionID: string(sid)},
			DesktopName:     "desktop-1",
			DesktopLabels:   map[string]string{"os": "windows"},
			StartTime:       now.Add(-time.Minute),
			EndTime:         now,
			Participants:    []string{"bob"},
			Recorded:        true,
		}
		entry, err := EntryFromSessionEndEvent(event, "")
		require.NoError(t, err)
		require.Equal(t, apievents.EventProtocolTDP, entry.Protocol)
		require.Equal(t, "windows_desktop", entry.ResourceKind)
		require.Equal(t, "recorded", entry.RecordingType)
	})

	t.Run("database", func(t *testing.T) {
		event := &apievents.DatabaseSessionEnd{
			Metadata:        apievents.Metadata{Type: events.DatabaseSessionEndEvent, Time: now},
			UserMetadata:    apievents.UserMetadata{User: "dbuser"},
			SessionMetadata: apievents.SessionMetadata{SessionID: string(sid)},
			DatabaseMetadata: apievents.DatabaseMetadata{
				DatabaseService: "db-1",
				DatabaseLabels:  map[string]string{"team": "data"},
			},
			StartTime: now.Add(-time.Minute),
			EndTime:   now,
		}
		entry, err := EntryFromSessionEndEvent(event, "root.example")
		require.NoError(t, err)
		require.Equal(t, apievents.EventProtocolDB, entry.Protocol)
		require.Equal(t, "db", entry.ResourceKind)
	})

	t.Run("app", func(t *testing.T) {
		event := &apievents.AppSessionEnd{
			Metadata:        apievents.Metadata{Type: events.AppSessionEndEvent, Time: now},
			UserMetadata:    apievents.UserMetadata{User: "appuser"},
			SessionMetadata: apievents.SessionMetadata{SessionID: string(sid)},
			AppMetadata: apievents.AppMetadata{
				AppName:   "app-1",
				AppLabels: map[string]string{"tier": "web"},
			},
		}
		entry, err := EntryFromSessionEndEvent(event, "root.example")
		require.NoError(t, err)
		require.Equal(t, apievents.EventProtocolApp, entry.Protocol)
		require.Equal(t, "app", entry.ResourceKind)
	})

	t.Run("mcp falls back to unknown protocol", func(t *testing.T) {
		event := &apievents.MCPSessionEnd{
			Metadata:        apievents.Metadata{Type: events.MCPSessionEndEvent, Time: now},
			UserMetadata:    apievents.UserMetadata{User: "mcpuser"},
			SessionMetadata: apievents.SessionMetadata{SessionID: string(sid)},
			AppMetadata:     apievents.AppMetadata{AppName: "mcp-1"},
		}
		entry, err := EntryFromSessionEndEvent(event, "root.example")
		require.NoError(t, err)
		require.Equal(t, ProtocolUnknown, entry.Protocol)
		require.Equal(t, events.MCPSessionEndEvent, entry.OriginalEventType)
	})

	t.Run("unsupported event rejected", func(t *testing.T) {
		_, err := EntryFromSessionEndEvent(&apievents.SessionStart{}, "root.example")
		require.Error(t, err)
		require.True(t, trace.IsBadParameter(err))
	})
}
