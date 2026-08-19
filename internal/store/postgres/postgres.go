// Package postgres is the PostgreSQL store.Store.
//
// Writes lock the namespace row while bumping its revision, which serializes
// writers per namespace and gives every change a unique, gap-free revision.
// The same transaction checks the write's preconditions, applies it, appends
// one audit event per changed entity and stores the resulting namespace
// state as the revision's history entry, so a failed write leaves no trace.
// Snapshots read under REPEATABLE READ so the revision and the entries always
// come from the same point in time.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

var _ store.Store = (*Store)(nil)

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// Store is backed by a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to PostgreSQL. Call Migrate before first use of a new database.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Close() { s.pool.Close() }

// querier is implemented by *pgxpool.Pool and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func pageSize(limit int) int {
	if limit <= 0 {
		return defaultPageSize
	}
	return min(limit, maxPageSize)
}

const namespaceColumns = `name, description, revision, created_at, updated_at, created_by`

func scanNamespace(row pgx.Row) (model.Namespace, error) {
	var ns model.Namespace
	err := row.Scan(&ns.Name, &ns.Description, &ns.Revision, &ns.CreatedAt, &ns.UpdatedAt, &ns.CreatedBy)
	ns.CreatedAt, ns.UpdatedAt = ns.CreatedAt.UTC(), ns.UpdatedAt.UTC()
	return ns, err
}

func (s *Store) CreateNamespace(ctx context.Context, ns model.Namespace, opts store.WriteOptions) (model.Namespace, error) {
	var out model.Namespace
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The revision defaults to 1 and both timestamps to now(), which is
		// the same instant throughout the transaction.
		var err error
		out, err = scanNamespace(tx.QueryRow(ctx, `
			INSERT INTO namespaces (name, description, created_by) VALUES ($1, $2, $3)
			ON CONFLICT (name) DO NOTHING
			RETURNING `+namespaceColumns,
			ns.Name, ns.Description, opts.Actor))
		if errors.Is(err, pgx.ErrNoRows) {
			return model.AlreadyExistsf("namespace %q", ns.Name)
		}
		if err != nil {
			return fmt.Errorf("insert namespace: %w", err)
		}
		after, err := model.EncodeEntry(out)
		if err != nil {
			return fmt.Errorf("encode audit image: %w", err)
		}
		return record(ctx, tx, out.Name, out.Revision, out.UpdatedAt, opts, "create namespace", []event{{
			entity: model.EntityNamespace, key: out.Name, action: model.ActionCreate, after: after,
		}})
	})
	if err != nil {
		return model.Namespace{}, err
	}
	return out, nil
}

func (s *Store) GetNamespace(ctx context.Context, name string) (model.Namespace, error) {
	ns, err := scanNamespace(s.pool.QueryRow(ctx,
		`SELECT `+namespaceColumns+` FROM namespaces WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Namespace{}, model.NotFoundf("namespace %q", name)
	}
	if err != nil {
		return model.Namespace{}, fmt.Errorf("get namespace: %w", err)
	}
	return ns, nil
}

func (s *Store) ListNamespaces(ctx context.Context) ([]model.Namespace, error) {
	// COLLATE "C" keeps ordering byte-wise, independent of the database locale.
	rows, err := s.pool.Query(ctx, `SELECT `+namespaceColumns+` FROM namespaces ORDER BY name COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Namespace, error) {
		return scanNamespace(row)
	})
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	return out, nil
}

// namespaceExists returns model.ErrNotFound unless the namespace exists.
// Reads that find nothing use it to tell a missing namespace from a missing
// entry.
func namespaceExists(ctx context.Context, q querier, name string) error {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM namespaces WHERE name = $1)`, name).Scan(&exists); err != nil {
		return fmt.Errorf("check namespace: %w", err)
	}
	if !exists {
		return model.NotFoundf("namespace %q", name)
	}
	return nil
}

// event is one entity change for the audit log, with the entity's JSON image
// before and after the change (nil where it did not exist).
type event struct {
	entity, key, action string
	before, after       json.RawMessage
}

// bump moves the namespace to its next revision. The UPDATE takes the row
// lock that serializes writers to the namespace until the transaction ends.
// clock_timestamp() (not now(), the transaction start) is evaluated after the
// lock is held, so updated_at never goes backwards between revisions.
func bump(ctx context.Context, tx pgx.Tx, namespace string) (int64, time.Time, error) {
	var (
		rev int64
		at  time.Time
	)
	err := tx.QueryRow(ctx,
		`UPDATE namespaces SET revision = revision + 1, updated_at = clock_timestamp() WHERE name = $1 RETURNING revision, updated_at`,
		namespace).Scan(&rev, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, model.NotFoundf("namespace %q", namespace)
	}
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("bump revision: %w", err)
	}
	return rev, at.UTC(), nil
}

// write runs fn in a transaction that holds the namespace row lock and has
// already moved the namespace to its next revision, then records the events
// fn returns. An error from fn rolls everything back, the bump included.
func (s *Store) write(ctx context.Context, namespace string, opts store.WriteOptions,
	fn func(tx pgx.Tx, rev int64, at time.Time) ([]event, error),
) (int64, error) {
	var rev int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var (
			at  time.Time
			err error
		)
		if rev, at, err = bump(ctx, tx, namespace); err != nil {
			return err
		}
		events, err := fn(tx, rev, at)
		if err != nil {
			return err
		}
		return record(ctx, tx, namespace, rev, at, opts, "", events)
	})
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// record appends the audit events of revision rev and its history entry,
// whose snapshot is the namespace state read back inside the transaction.
// summary is used when opts.Message is empty; when both are empty, a single
// event is summarized as "<action> <entity> <key>".
func record(ctx context.Context, tx pgx.Tx, namespace string, rev int64, at time.Time, opts store.WriteOptions, summary string, events []event) error {
	snap, err := readSnapshot(ctx, tx, namespace)
	if err != nil {
		return err
	}
	doc, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	b := &pgx.Batch{}
	for _, e := range events {
		b.Queue(`
			INSERT INTO audit_events (namespace, revision, actor, created_at, action, entity_type, entity_key, before_image, after_image, message)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			namespace, rev, opts.Actor, at, auditAction(e.action, opts), e.entity, e.key, e.before, e.after, opts.Message)
	}
	switch {
	case opts.Message != "":
		summary = opts.Message
	case summary == "" && len(events) == 1:
		summary = fmt.Sprintf("%s %s %s", auditAction(events[0].action, opts), events[0].entity, events[0].key)
	}
	b.Queue(`INSERT INTO revisions (namespace, revision, actor, created_at, summary, snapshot) VALUES ($1, $2, $3, $4, $5, $6)`,
		namespace, rev, opts.Actor, at, summary, json.RawMessage(doc))
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("record revision %d: %w", rev, err)
	}
	return nil
}

// auditAction is the action recorded for an event: opts.Action when set,
// except that rollback events always say rollback.
func auditAction(action string, opts store.WriteOptions) string {
	if opts.Action != "" && action != model.ActionRollback {
		return opts.Action
	}
	return action
}

func (s *Store) Snapshot(ctx context.Context, namespace string) (model.Snapshot, error) {
	var snap model.Snapshot
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err := pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		var err error
		snap, err = readSnapshot(ctx, tx, namespace)
		return err
	})
	if err != nil {
		return model.Snapshot{}, err
	}
	return snap, nil
}

// readSnapshot reads the state of a namespace in one round trip. The caller
// runs it in a REPEATABLE READ transaction or holds the namespace row lock,
// so the revision and the entries belong together.
func readSnapshot(ctx context.Context, tx pgx.Tx, namespace string) (model.Snapshot, error) {
	snap := model.Snapshot{Namespace: namespace}
	b := &pgx.Batch{}
	b.Queue(`SELECT revision, updated_at FROM namespaces WHERE name = $1`, namespace).QueryRow(func(row pgx.Row) error {
		err := row.Scan(&snap.Revision, &snap.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.NotFoundf("namespace %q", namespace)
		}
		if err != nil {
			return fmt.Errorf("read namespace: %w", err)
		}
		snap.UpdatedAt = snap.UpdatedAt.UTC()
		return nil
	})
	configs.queueList(b, namespace, &snap.Configs)
	flags.queueList(b, namespace, &snap.Flags)
	experiments.queueList(b, namespace, &snap.Experiments)
	rateLimits.queueList(b, namespace, &snap.RateLimits)
	breakers.queueList(b, namespace, &snap.CircuitBreakers)
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return model.Snapshot{}, err
	}
	return snap, nil
}

func (s *Store) PutConfig(ctx context.Context, namespace string, c model.Config, opts store.WriteOptions) (model.Config, error) {
	return put(ctx, s, configs, namespace, c, opts)
}

func (s *Store) PutFlag(ctx context.Context, namespace string, f model.Flag, opts store.WriteOptions) (model.Flag, error) {
	return put(ctx, s, flags, namespace, f, opts)
}

func (s *Store) PutExperiment(ctx context.Context, namespace string, e model.Experiment, opts store.WriteOptions) (model.Experiment, error) {
	return put(ctx, s, experiments, namespace, e, opts)
}

func (s *Store) PutRateLimit(ctx context.Context, namespace string, r model.RateLimit, opts store.WriteOptions) (model.RateLimit, error) {
	return put(ctx, s, rateLimits, namespace, r, opts)
}

func (s *Store) PutCircuitBreaker(ctx context.Context, namespace string, c model.CircuitBreaker, opts store.WriteOptions) (model.CircuitBreaker, error) {
	return put(ctx, s, breakers, namespace, c, opts)
}

func (s *Store) DeleteConfig(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(ctx, s, configs, namespace, key, opts)
}

func (s *Store) DeleteFlag(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(ctx, s, flags, namespace, key, opts)
}

func (s *Store) DeleteExperiment(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(ctx, s, experiments, namespace, key, opts)
}

func (s *Store) DeleteRateLimit(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(ctx, s, rateLimits, namespace, key, opts)
}

func (s *Store) DeleteCircuitBreaker(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(ctx, s, breakers, namespace, key, opts)
}

func put[T any](ctx context.Context, s *Store, t table[T], namespace string, v T, opts store.WriteOptions) (T, error) {
	var out T
	key := t.key(v)
	_, err := s.write(ctx, namespace, opts, func(tx pgx.Tx, rev int64, at time.Time) ([]event, error) {
		old, existed, err := t.get(ctx, tx, namespace, key)
		if err != nil {
			return nil, err
		}
		if opts.ExpectedRevision != 0 && (!existed || t.revision(old) != opts.ExpectedRevision) {
			return nil, model.Conflictf("%s %q in namespace %q is not at revision %d", t.entity, key, namespace, opts.ExpectedRevision)
		}
		t.stamp(&v, rev, at, opts.Actor)
		if out, err = t.upsert(ctx, tx, namespace, v); err != nil {
			return nil, err
		}
		ev := event{entity: t.entity, key: key, action: model.ActionCreate}
		if existed {
			ev.action = model.ActionUpdate
			if ev.before, err = model.EncodeEntry(old); err != nil {
				return nil, fmt.Errorf("encode audit image: %w", err)
			}
		}
		if ev.after, err = model.EncodeEntry(out); err != nil {
			return nil, fmt.Errorf("encode audit image: %w", err)
		}
		return []event{ev}, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

func remove[T any](ctx context.Context, s *Store, t table[T], namespace, key string, opts store.WriteOptions) (int64, error) {
	return s.write(ctx, namespace, opts, func(tx pgx.Tx, _ int64, _ time.Time) ([]event, error) {
		old, existed, err := t.get(ctx, tx, namespace, key)
		if err != nil {
			return nil, err
		}
		if !existed {
			return nil, model.NotFoundf("%s %q in namespace %q", t.entity, key, namespace)
		}
		if opts.ExpectedRevision != 0 && t.revision(old) != opts.ExpectedRevision {
			return nil, model.Conflictf("%s %q in namespace %q is not at revision %d", t.entity, key, namespace, opts.ExpectedRevision)
		}
		if err := t.delete(ctx, tx, namespace, key); err != nil {
			return nil, err
		}
		before, err := model.EncodeEntry(old)
		if err != nil {
			return nil, fmt.Errorf("encode audit image: %w", err)
		}
		return []event{{entity: t.entity, key: key, action: model.ActionDelete, before: before}}, nil
	})
}

func (s *Store) GetFlag(ctx context.Context, namespace, key string) (model.Flag, error) {
	f, ok, err := flags.get(ctx, s.pool, namespace, key)
	if err != nil {
		return model.Flag{}, err
	}
	if !ok {
		if err := namespaceExists(ctx, s.pool, namespace); err != nil {
			return model.Flag{}, err
		}
		return model.Flag{}, model.NotFoundf("flag %q in namespace %q", key, namespace)
	}
	return f, nil
}
