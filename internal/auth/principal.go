// Package auth authenticates API callers with bearer tokens and authorizes
// them by role.
package auth

import (
	"context"
	"fmt"
)

// Role is what a caller may do. Higher roles include lower ones.
type Role int

const (
	// RoleReader may read state, history and audit events and watch namespaces.
	RoleReader Role = iota + 1
	// RoleEditor may also change entries, run rollouts and roll back.
	RoleEditor
	// RoleAdmin may also create namespaces.
	RoleAdmin
)

func (r Role) String() string {
	switch r {
	case RoleReader:
		return "reader"
	case RoleEditor:
		return "editor"
	case RoleAdmin:
		return "admin"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// ParseRole parses "reader", "editor" or "admin".
func ParseRole(s string) (Role, error) {
	switch s {
	case "reader":
		return RoleReader, nil
	case "editor":
		return RoleEditor, nil
	case "admin":
		return RoleAdmin, nil
	default:
		return 0, fmt.Errorf("unknown role %q (want reader, editor or admin)", s)
	}
}

// Principal is an authenticated caller.
type Principal struct {
	Name string
	Role Role
}

// Can reports whether the principal has at least role r.
func (p Principal) Can(r Role) bool { return p.Role >= r }

type principalKey struct{}

// WithPrincipal returns a context carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the principal stored by WithPrincipal.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
