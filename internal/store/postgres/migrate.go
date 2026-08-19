package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID is an arbitrary constant for pg_advisory_lock so that
// replicas starting together apply migrations one at a time.
const migrationLockID = 0x636f6e74726f6c // "control"

// Migrate applies any migrations that have not run yet. Each file runs in its
// own transaction and is recorded in schema_migrations.
func (s *Store) Migrate(ctx context.Context) error {
	return s.migrate(ctx, "")
}

// migrate applies pending migrations in order, stopping after version last
// when it is set (tests use that to build the schema of an older release).
func (s *Store) migrate(ctx context.Context, last string) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer func() {
		// Also released when the session ends, so a failure here is harmless.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT        PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	slices.Sort(files)
	for _, file := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(file, "migrations/"), ".sql")
		if !slices.Contains(applied, version) {
			sql, err := migrationFS.ReadFile(file)
			if err != nil {
				return err
			}
			err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, string(sql)); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version)
				return err
			})
			if err != nil {
				return fmt.Errorf("apply migration %s: %w", version, err)
			}
		}
		if version == last {
			return nil
		}
	}
	if last != "" {
		return fmt.Errorf("unknown migration %s", last)
	}
	return nil
}
