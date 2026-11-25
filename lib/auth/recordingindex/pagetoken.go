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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/gravitational/trace"

	"github.com/gravitational/teleport/lib/session"
)

// pageToken is the keyset pagination cursor. It captures the query
// fingerprint, the keyset position of the last returned entry, the traversal
// direction and an absolute expiry. Tokens are authenticated with an HMAC so
// tampering is detected without any server-side token storage.
type pageToken struct {
	// Fingerprint is Query.Fingerprint of the query that created the token.
	Fingerprint string `json:"fp"`
	// LastStartNano is the StartTime (Unix nano) of the last returned entry.
	LastStartNano int64 `json:"ls"`
	// LastSessionID is the SessionID of the last returned entry, used as the
	// tie-breaker so equal timestamps remain fully ordered.
	LastSessionID string `json:"sid"`
	// Order is the traversal direction.
	Order Order `json:"ord"`
	// ExpiresAtNano is the absolute expiry time.
	ExpiresAtNano int64 `json:"exp"`
}

// signPageToken serializes and authenticates a token. The secret is owned by
// the index and never leaves the process.
func signPageToken(t pageToken, secret []byte) (string, error) {
	payload, err := json.Marshal(t)
	if err != nil {
		return "", trace.Wrap(err)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	signature := mac.Sum(nil)

	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(signature), nil
}

// verifyPageToken validates the signature, structure and expiry of a token and
// checks it matches the query it is resumed against.
func verifyPageToken(raw string, query Query, now time.Time, secret []byte) (pageToken, error) {
	var token pageToken
	if raw == "" {
		return token, nil
	}
	enc := base64.RawURLEncoding
	encoded, encodedSig, ok := splitToken(raw)
	if !ok {
		return token, trace.BadParameter("malformed page token")
	}

	payload, err := enc.DecodeString(encoded)
	if err != nil {
		return token, trace.BadParameter("malformed page token: %v", err)
	}
	signature, err := enc.DecodeString(encodedSig)
	if err != nil {
		return token, trace.BadParameter("malformed page token signature: %v", err)
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return token, trace.BadParameter("page token signature does not match")
	}

	if err := json.Unmarshal(payload, &token); err != nil {
		return token, trace.BadParameter("malformed page token payload: %v", err)
	}
	if token.ExpiresAtNano <= 0 {
		return token, trace.BadParameter("page token is missing an expiry")
	}
	// Clock moves are tolerated by comparing against the monotonic wall clock
	// supplied by the caller; an expiry in the past is rejected regardless.
	if !now.IsZero() && token.ExpiresAtNano <= now.UnixNano() {
		return token, trace.BadParameter("page token has expired")
	}
	if token.Fingerprint != query.Fingerprint() {
		return token, trace.BadParameter("page token does not match the query fingerprint")
	}
	if token.Order != query.Order {
		return token, trace.BadParameter("page token direction does not match the query order")
	}
	if token.LastSessionID != "" {
		if _, err := session.ParseID(token.LastSessionID); err != nil {
			return token, trace.BadParameter("page token contains an invalid session ID")
		}
	}
	return token, nil
}

func splitToken(raw string) (payload, signature string, ok bool) {
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] == '.' {
			return raw[:i], raw[i+1:], i > 0 && i+1 < len(raw)
		}
	}
	return "", "", false
}

// strconvFormatInt is a thin indirection kept so query.go stays free of strconv
// imports and time formatting stays in one place.
func strconvFormatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
