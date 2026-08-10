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

func wrapf(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
