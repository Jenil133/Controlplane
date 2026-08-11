// Package store defines persistence for the control plane.
//
// Every mutation inside a namespace bumps the namespace revision by exactly one
// in the same transaction as the change, so revisions are a total order of
// changes per namespace and a snapshot at revision N is reproducible.
//
// Implementations assume their input has already been validated by the caller.
package store

import (
	"context"

	"github.com/Jenil133/Controlplane/internal/model"
)

// Store is implemented by the PostgreSQL and in-memory backends.
type Store interface {
	// CreateNamespace creates an empty namespace at revision 1.
	// Returns model.ErrAlreadyExists if it exists.
	CreateNamespace(ctx context.Context, ns model.Namespace) (model.Namespace, error)
	GetNamespace(ctx context.Context, name string) (model.Namespace, error)
	// ListNamespaces returns all namespaces sorted by name.
	ListNamespaces(ctx context.Context) ([]model.Namespace, error)

	// Put* upserts an entry, recording entry.UpdatedBy as the actor, and returns
	// it with Revision and UpdatedAt set. Returns model.ErrNotFound if the
	// namespace does not exist.
	PutConfig(ctx context.Context, namespace string, c model.Config) (model.Config, error)
	PutFlag(ctx context.Context, namespace string, f model.Flag) (model.Flag, error)
	PutExperiment(ctx context.Context, namespace string, e model.Experiment) (model.Experiment, error)

	// Delete* removes an entry and returns the new namespace revision.
	// Returns model.ErrNotFound, without bumping the revision, if the entry or
	// namespace does not exist.
	DeleteConfig(ctx context.Context, namespace, key string) (int64, error)
	DeleteFlag(ctx context.Context, namespace, key string) (int64, error)
	DeleteExperiment(ctx context.Context, namespace, key string) (int64, error)

	// Snapshot returns a consistent view of a namespace: the revision and the
	// entries always belong together.
	Snapshot(ctx context.Context, namespace string) (model.Snapshot, error)

	// Ping reports whether the backend is reachable.
	Ping(ctx context.Context) error
	Close()
}
