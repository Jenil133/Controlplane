// Package store defines persistence for the control plane.
//
// Every mutation inside a namespace bumps the namespace revision by exactly one
// in the same transaction as the change, so revisions are a total order of
// changes per namespace. The same transaction records the full namespace
// state at the new revision (the revision history, used for diffs and
// rollback) and one audit event per changed entity.
//
// Implementations assume their input has already been validated by the caller.
package store

import (
	"context"

	"github.com/Jenil133/Controlplane/internal/model"
)

// WriteOptions carry who is writing and the preconditions of a write.
type WriteOptions struct {
	// Actor is recorded as UpdatedBy (CreatedBy for namespaces), in the audit
	// log and in the revision history.
	Actor string
	// ExpectedRevision, when non-zero, makes the write fail with
	// model.ErrConflict unless the entry's current revision equals it; a
	// missing entry never matches. For Rollback it is compared with the
	// namespace revision instead.
	ExpectedRevision int64
	// Action overrides the audit action, which otherwise is create, update or
	// delete (rollback for Rollback).
	Action string
	// Message is stored on the audit event and, when set, used as the
	// revision summary instead of the default "<action> <entity> <key>".
	Message string
}

// Store is implemented by the PostgreSQL and in-memory backends.
type Store interface {
	// CreateNamespace creates an empty namespace at revision 1, records
	// opts.Actor as CreatedBy, writes revision 1 (an empty snapshot, summary
	// "create namespace") to the history and a namespace "create" audit event.
	// Returns model.ErrAlreadyExists if it exists.
	CreateNamespace(ctx context.Context, ns model.Namespace, opts WriteOptions) (model.Namespace, error)
	GetNamespace(ctx context.Context, name string) (model.Namespace, error)
	// ListNamespaces returns all namespaces sorted by name.
	ListNamespaces(ctx context.Context) ([]model.Namespace, error)

	// Put* upserts an entry and returns it with Revision, UpdatedAt and
	// UpdatedBy (= opts.Actor) set. Returns model.ErrNotFound if the namespace
	// does not exist and model.ErrConflict if opts.ExpectedRevision does not
	// match. A failed write never changes the namespace revision.
	PutConfig(ctx context.Context, namespace string, c model.Config, opts WriteOptions) (model.Config, error)
	PutFlag(ctx context.Context, namespace string, f model.Flag, opts WriteOptions) (model.Flag, error)
	PutExperiment(ctx context.Context, namespace string, e model.Experiment, opts WriteOptions) (model.Experiment, error)
	PutRateLimit(ctx context.Context, namespace string, r model.RateLimit, opts WriteOptions) (model.RateLimit, error)
	PutCircuitBreaker(ctx context.Context, namespace string, c model.CircuitBreaker, opts WriteOptions) (model.CircuitBreaker, error)

	// Delete* removes an entry and returns the new namespace revision.
	// Returns model.ErrNotFound if the entry or namespace does not exist and
	// model.ErrConflict if opts.ExpectedRevision does not match.
	DeleteConfig(ctx context.Context, namespace, key string, opts WriteOptions) (int64, error)
	DeleteFlag(ctx context.Context, namespace, key string, opts WriteOptions) (int64, error)
	DeleteExperiment(ctx context.Context, namespace, key string, opts WriteOptions) (int64, error)
	DeleteRateLimit(ctx context.Context, namespace, key string, opts WriteOptions) (int64, error)
	DeleteCircuitBreaker(ctx context.Context, namespace, key string, opts WriteOptions) (int64, error)

	// GetFlag returns one flag, for read-modify-write with ExpectedRevision.
	GetFlag(ctx context.Context, namespace, key string) (model.Flag, error)

	// Snapshot returns a consistent view of a namespace: the revision and the
	// entries always belong together.
	Snapshot(ctx context.Context, namespace string) (model.Snapshot, error)

	// ListRevisions returns up to limit history entries older than
	// beforeRevision (0 = from the latest), newest first, without snapshots.
	// limit <= 0 means 50; it is capped at 500. Returns model.ErrNotFound if
	// the namespace does not exist.
	ListRevisions(ctx context.Context, namespace string, beforeRevision int64, limit int) ([]model.Revision, error)
	// GetRevision returns one history entry including its full snapshot.
	GetRevision(ctx context.Context, namespace string, revision int64) (model.Revision, error)

	// Rollback makes the namespace match its state at toRevision, after
	// model.PrepareRollback, by writing every differing entry as one new
	// revision (summary "rollback to revision N", one audit event per change
	// with action "rollback"). Restored entries get the new revision and
	// opts.Actor. Returns the new revision and the applied changes; when
	// nothing differs it returns the current revision, no changes, and writes
	// nothing. opts.ExpectedRevision, when non-zero, must equal the current
	// namespace revision or model.ErrConflict is returned. Returns
	// model.ErrNotFound if the namespace or revision does not exist.
	Rollback(ctx context.Context, namespace string, toRevision int64, opts WriteOptions) (int64, []model.Change, error)

	// ListAuditEvents returns matching events, newest (highest ID) first.
	ListAuditEvents(ctx context.Context, filter model.AuditFilter) ([]model.AuditEvent, error)

	// ListActiveRollouts returns every flag, across namespaces, whose rollout
	// plan is in state model.RolloutActive.
	ListActiveRollouts(ctx context.Context) ([]model.RolloutRef, error)

	// Ping reports whether the backend is reachable.
	Ping(ctx context.Context) error
	Close()
}
