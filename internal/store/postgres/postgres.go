// Package postgres is the PostgreSQL store.Store.
//
// Writes lock the namespace row while bumping its revision, which serializes
// writers per namespace and gives every change a unique, gap-free revision.
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

var _ store.Store = (*Store)(nil)

// uniqueViolation is the SQLSTATE for unique_violation.
const uniqueViolation = "23505"

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

const namespaceColumns = `name, description, revision, created_at, updated_at`

func scanNamespace(row pgx.Row) (model.Namespace, error) {
	var ns model.Namespace
	err := row.Scan(&ns.Name, &ns.Description, &ns.Revision, &ns.CreatedAt, &ns.UpdatedAt)
	ns.CreatedAt, ns.UpdatedAt = ns.CreatedAt.UTC(), ns.UpdatedAt.UTC()
	return ns, err
}

func (s *Store) CreateNamespace(ctx context.Context, ns model.Namespace) (model.Namespace, error) {
	out, err := scanNamespace(s.pool.QueryRow(ctx,
		`INSERT INTO namespaces (name, description) VALUES ($1, $2) RETURNING `+namespaceColumns,
		ns.Name, ns.Description))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return model.Namespace{}, model.AlreadyExistsf("namespace %q", ns.Name)
		}
		return model.Namespace{}, fmt.Errorf("insert namespace: %w", err)
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

// write runs fn in a transaction after bumping the namespace revision. The
// UPDATE takes a row lock that serializes concurrent writers to the namespace.
// clock_timestamp() (not now(), the transaction start) is evaluated after the
// lock is held, so updated_at never goes backwards between revisions.
func (s *Store) write(ctx context.Context, namespace string, fn func(tx pgx.Tx, rev int64, at time.Time) error) (int64, error) {
	var rev int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var at time.Time
		err := tx.QueryRow(ctx,
			`UPDATE namespaces SET revision = revision + 1, updated_at = clock_timestamp() WHERE name = $1 RETURNING revision, updated_at`,
			namespace).Scan(&rev, &at)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.NotFoundf("namespace %q", namespace)
		}
		if err != nil {
			return fmt.Errorf("bump revision: %w", err)
		}
		return fn(tx, rev, at.UTC())
	})
	return rev, err
}

func (s *Store) PutConfig(ctx context.Context, namespace string, c model.Config) (model.Config, error) {
	out := c
	_, err := s.write(ctx, namespace, func(tx pgx.Tx, rev int64, at time.Time) error {
		out.Revision, out.UpdatedAt = rev, at
		return tx.QueryRow(ctx, `
			INSERT INTO configs (namespace, key, value, description, revision, updated_at, updated_by)
			VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
			ON CONFLICT (namespace, key) DO UPDATE SET
				value = EXCLUDED.value, description = EXCLUDED.description, revision = EXCLUDED.revision,
				updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
			RETURNING value`,
			namespace, c.Key, string(c.Value), c.Description, rev, at, c.UpdatedBy).Scan(&out.Value)
	})
	if err != nil {
		return model.Config{}, err
	}
	return out, nil
}

func (s *Store) PutFlag(ctx context.Context, namespace string, f model.Flag) (model.Flag, error) {
	_, err := s.write(ctx, namespace, func(tx pgx.Tx, rev int64, at time.Time) error {
		f.Revision, f.UpdatedAt = rev, at
		_, err := tx.Exec(ctx, `
			INSERT INTO flags (namespace, key, enabled, description, revision, updated_at, updated_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (namespace, key) DO UPDATE SET
				enabled = EXCLUDED.enabled, description = EXCLUDED.description, revision = EXCLUDED.revision,
				updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by`,
			namespace, f.Key, f.Enabled, f.Description, rev, at, f.UpdatedBy)
		return err
	})
	if err != nil {
		return model.Flag{}, err
	}
	return f, nil
}

func (s *Store) PutExperiment(ctx context.Context, namespace string, e model.Experiment) (model.Experiment, error) {
	variants, err := json.Marshal(e.Variants)
	if err != nil {
		return model.Experiment{}, fmt.Errorf("encode variants: %w", err)
	}
	out := e
	_, err = s.write(ctx, namespace, func(tx pgx.Tx, rev int64, at time.Time) error {
		out.Revision, out.UpdatedAt = rev, at
		var stored []byte
		err := tx.QueryRow(ctx, `
			INSERT INTO experiments (namespace, key, enabled, description, salt, variants, revision, updated_at, updated_by)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
			ON CONFLICT (namespace, key) DO UPDATE SET
				enabled = EXCLUDED.enabled, description = EXCLUDED.description, salt = EXCLUDED.salt,
				variants = EXCLUDED.variants, revision = EXCLUDED.revision,
				updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
			RETURNING variants`,
			namespace, e.Key, e.Enabled, e.Description, e.Salt, string(variants), rev, at, e.UpdatedBy).Scan(&stored)
		if err != nil {
			return err
		}
		out.Variants = nil
		return json.Unmarshal(stored, &out.Variants)
	})
	if err != nil {
		return model.Experiment{}, err
	}
	return out, nil
}

func (s *Store) DeleteConfig(ctx context.Context, namespace, key string) (int64, error) {
	return s.remove(ctx, "configs", "config", namespace, key)
}

func (s *Store) DeleteFlag(ctx context.Context, namespace, key string) (int64, error) {
	return s.remove(ctx, "flags", "flag", namespace, key)
}

func (s *Store) DeleteExperiment(ctx context.Context, namespace, key string) (int64, error) {
	return s.remove(ctx, "experiments", "experiment", namespace, key)
}

// remove deletes one entry. table is always a constant from this file.
func (s *Store) remove(ctx context.Context, table, kind, namespace, key string) (int64, error) {
	return s.write(ctx, namespace, func(tx pgx.Tx, _ int64, _ time.Time) error {
		tag, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE namespace = $1 AND key = $2`, namespace, key)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// Returning an error rolls back the revision bump.
			return model.NotFoundf("%s %q in namespace %q", kind, key, namespace)
		}
		return nil
	})
}

func (s *Store) Snapshot(ctx context.Context, namespace string) (model.Snapshot, error) {
	snap := model.Snapshot{Namespace: namespace}
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err := pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT revision, updated_at FROM namespaces WHERE name = $1`, namespace).
			Scan(&snap.Revision, &snap.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.NotFoundf("namespace %q", namespace)
		}
		if err != nil {
			return err
		}
		snap.UpdatedAt = snap.UpdatedAt.UTC()

		rows, _ := tx.Query(ctx, `
			SELECT key, value, description, revision, updated_at, updated_by
			FROM configs WHERE namespace = $1 ORDER BY key COLLATE "C"`, namespace)
		snap.Configs, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Config, error) {
			var c model.Config
			err := row.Scan(&c.Key, &c.Value, &c.Description, &c.Revision, &c.UpdatedAt, &c.UpdatedBy)
			c.UpdatedAt = c.UpdatedAt.UTC()
			return c, err
		})
		if err != nil {
			return fmt.Errorf("read configs: %w", err)
		}

		rows, _ = tx.Query(ctx, `
			SELECT key, enabled, description, revision, updated_at, updated_by
			FROM flags WHERE namespace = $1 ORDER BY key COLLATE "C"`, namespace)
		snap.Flags, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Flag, error) {
			var f model.Flag
			err := row.Scan(&f.Key, &f.Enabled, &f.Description, &f.Revision, &f.UpdatedAt, &f.UpdatedBy)
			f.UpdatedAt = f.UpdatedAt.UTC()
			return f, err
		})
		if err != nil {
			return fmt.Errorf("read flags: %w", err)
		}

		rows, _ = tx.Query(ctx, `
			SELECT key, enabled, description, salt, variants, revision, updated_at, updated_by
			FROM experiments WHERE namespace = $1 ORDER BY key COLLATE "C"`, namespace)
		snap.Experiments, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Experiment, error) {
			var (
				e        model.Experiment
				variants []byte
			)
			if err := row.Scan(&e.Key, &e.Enabled, &e.Description, &e.Salt, &variants, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
				return e, err
			}
			e.UpdatedAt = e.UpdatedAt.UTC()
			return e, json.Unmarshal(variants, &e.Variants)
		})
		if err != nil {
			return fmt.Errorf("read experiments: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.Snapshot{}, err
	}
	return snap, nil
}
