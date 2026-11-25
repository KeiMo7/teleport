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
	"errors"
	"fmt"
)

// The pinned trace version used by this repository does not provide
// Unavailable or Aborted error types. The index contract requires a fixed
// error taxonomy, so the two missing classes are defined locally. They remain
// compatible with errors.Is / errors.As and can be wrapped with trace.Wrap
// without losing their type.

// UnavailableError indicates the recording index backend is unavailable or
// too slow to serve the request. Callers should retry after the advertised
// back-off.
type UnavailableError struct {
	Message string
}

// NewUnavailable builds an Unavailable error.
func NewUnavailable(format string, args ...any) error {
	return &UnavailableError{Message: fmt.Sprintf(format, args...)}
}

// Error implements error.
func (e *UnavailableError) Error() string {
	return e.Message
}

// Is supports errors.Is.
func (e *UnavailableError) Is(target error) bool {
	_, ok := target.(*UnavailableError)
	return ok
}

// IsUnavailable reports whether err is (or wraps) an UnavailableError.
func IsUnavailable(err error) bool {
	var unavailable *UnavailableError
	return errors.As(err, &unavailable)
}

// AbortedError indicates a write conflict exhausted its retry budget. The
// caller may retry the whole operation.
type AbortedError struct {
	Message string
}

// NewAborted builds an Aborted error.
func NewAborted(format string, args ...any) error {
	return &AbortedError{Message: fmt.Sprintf(format, args...)}
}

// Error implements error.
func (e *AbortedError) Error() string {
	return e.Message
}

// Is supports errors.Is.
func (e *AbortedError) Is(target error) bool {
	_, ok := target.(*AbortedError)
	return ok
}

// IsAborted reports whether err is (or wraps) an AbortedError.
func IsAborted(err error) bool {
	var aborted *AbortedError
	return errors.As(err, &aborted)
}
