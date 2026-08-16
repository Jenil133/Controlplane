package model

import (
	"errors"
	"fmt"
)

// Sentinel errors shared by every layer. Wrap them with context and test with
// errors.Is; the transport layer maps them to status codes.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalid       = errors.New("invalid argument")
	// ErrConflict means an optimistic-concurrency precondition failed: the
	// entry changed since the caller read it. Retrying after a re-read is safe.
	ErrConflict = errors.New("conflict")
	// ErrFailedPrecondition means the operation is not allowed in the current
	// state, e.g. advancing a rollout that already completed.
	ErrFailedPrecondition = errors.New("failed precondition")
)

// NotFoundf returns an error wrapping ErrNotFound.
func NotFoundf(format string, args ...any) error {
	return wrapf(ErrNotFound, format, args...)
}

// AlreadyExistsf returns an error wrapping ErrAlreadyExists.
func AlreadyExistsf(format string, args ...any) error {
	return wrapf(ErrAlreadyExists, format, args...)
}

// Invalidf returns an error wrapping ErrInvalid.
func Invalidf(format string, args ...any) error {
	return wrapf(ErrInvalid, format, args...)
}

// Conflictf returns an error wrapping ErrConflict.
func Conflictf(format string, args ...any) error {
	return wrapf(ErrConflict, format, args...)
}

// FailedPreconditionf returns an error wrapping ErrFailedPrecondition.
func FailedPreconditionf(format string, args ...any) error {
	return wrapf(ErrFailedPrecondition, format, args...)
}

func wrapf(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
