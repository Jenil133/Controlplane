package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

func (s *Store) ListRevisions(ctx context.Context, namespace string, beforeRevision int64, limit int) ([]model.Revision, error) {
	query := `SELECT revision, actor, created_at, summary FROM revisions WHERE namespace = $1`
	args := []any{namespace, pageSize(limit)}
	if beforeRevision > 0 {
		query += ` AND revision < $3`
		args = append(args, beforeRevision)
	}
	rows, err := s.pool.Query(ctx, query+` ORDER BY revision DESC LIMIT $2`, args...)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.Revision, error) {
		r := model.Revision{Namespace: namespace}
		err := row.Scan(&r.Revision, &r.Actor, &r.CreatedAt, &r.Summary)
		r.CreatedAt = r.CreatedAt.UTC()
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	if len(out) == 0 {
		if err := namespaceExists(ctx, s.pool, namespace); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) GetRevision(ctx context.Context, namespace string, revision int64) (model.Revision, error) {
	r := model.Revision{Namespace: namespace, Revision: revision}
	var doc []byte
	err := s.pool.QueryRow(ctx, `SELECT actor, created_at, summary, snapshot FROM revisions WHERE namespace = $1 AND revision = $2`,
		namespace, revision).Scan(&r.Actor, &r.CreatedAt, &r.Summary, &doc)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := namespaceExists(ctx, s.pool, namespace); err != nil {
			return model.Revision{}, err
		}
		// Also the case for revisions written before the history existed.
		return model.Revision{}, model.NotFoundf("revision %d of namespace %q", revision, namespace)
	}
	if err != nil {
		return model.Revision{}, fmt.Errorf("get revision: %w", err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	if err := json.Unmarshal(doc, &r.Snapshot); err != nil {
		return model.Revision{}, fmt.Errorf("decode snapshot of revision %d: %w", revision, err)
	}
	return r, nil
}

func (s *Store) Rollback(ctx context.Context, namespace string, toRevision int64, opts store.WriteOptions) (int64, []model.Change, error) {
	var (
		rev     int64
		changes []model.Change
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Take the writers' lock before comparing, so the state cannot move
		// between the diff and the writes. The revision is bumped only once
		// there is something to write.
		err := tx.QueryRow(ctx, `SELECT revision FROM namespaces WHERE name = $1 FOR UPDATE`, namespace).Scan(&rev)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.NotFoundf("namespace %q", namespace)
		}
		if err != nil {
			return fmt.Errorf("lock namespace: %w", err)
		}
		if opts.ExpectedRevision != 0 && opts.ExpectedRevision != rev {
			return model.Conflictf("namespace %q is at revision %d, not %d", namespace, rev, opts.ExpectedRevision)
		}

		var doc []byte
		err = tx.QueryRow(ctx, `SELECT snapshot FROM revisions WHERE namespace = $1 AND revision = $2`, namespace, toRevision).Scan(&doc)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.NotFoundf("revision %d of namespace %q", toRevision, namespace)
		}
		if err != nil {
			return fmt.Errorf("read revision %d: %w", toRevision, err)
		}
		var target model.Snapshot
		if err := json.Unmarshal(doc, &target); err != nil {
			return fmt.Errorf("decode snapshot of revision %d: %w", toRevision, err)
		}
		current, err := readSnapshot(ctx, tx, namespace)
		if err != nil {
			return err
		}
		diff, err := model.Diff(current, model.PrepareRollback(target))
		if err != nil {
			return err
		}
		if len(diff) == 0 {
			return nil
		}

		var at time.Time
		if rev, at, err = bump(ctx, tx, namespace); err != nil {
			return err
		}
		events := make([]event, len(diff))
		changes = make([]model.Change, len(diff))
		for i, c := range diff {
			after, err := restore(ctx, tx, namespace, c, rev, at, opts.Actor)
			if err != nil {
				return err
			}
			// c.Before is the encoding of the current entry, i.e. its image.
			events[i] = event{entity: c.EntityType, key: c.Key, action: model.ActionRollback, before: c.Before, after: after}
			changes[i] = model.Change{EntityType: c.EntityType, Key: c.Key, Type: c.Type, Before: c.Before, After: after}
		}
		if opts.Message == "" {
			opts.Message = fmt.Sprintf("rollback to revision %d", toRevision)
		}
		return record(ctx, tx, namespace, rev, at, opts, "", events)
	})
	if err != nil {
		return 0, nil, err
	}
	return rev, changes, nil
}

// restore makes one entry match the rollback target, writing the target's
// version (c.After) with the new revision metadata or deleting the entry,
// and returns the entry's new image.
func restore(ctx context.Context, tx pgx.Tx, namespace string, c model.Change, rev int64, at time.Time, actor string) (json.RawMessage, error) {
	switch c.EntityType {
	case model.EntityConfig:
		return restoreEntry(ctx, tx, configs, namespace, c, rev, at, actor)
	case model.EntityFlag:
		return restoreEntry(ctx, tx, flags, namespace, c, rev, at, actor)
	case model.EntityExperiment:
		return restoreEntry(ctx, tx, experiments, namespace, c, rev, at, actor)
	case model.EntityRateLimit:
		return restoreEntry(ctx, tx, rateLimits, namespace, c, rev, at, actor)
	case model.EntityCircuitBreaker:
		return restoreEntry(ctx, tx, breakers, namespace, c, rev, at, actor)
	default:
		return nil, fmt.Errorf("rollback: unknown entity type %q", c.EntityType)
	}
}

func restoreEntry[T any](ctx context.Context, tx pgx.Tx, t table[T], namespace string, c model.Change, rev int64, at time.Time, actor string) (json.RawMessage, error) {
	if c.After == nil {
		return nil, t.delete(ctx, tx, namespace, c.Key)
	}
	var v T
	if err := json.Unmarshal(c.After, &v); err != nil {
		return nil, fmt.Errorf("decode %s %q: %w", t.entity, c.Key, err)
	}
	t.stamp(&v, rev, at, actor)
	out, err := t.upsert(ctx, tx, namespace, v)
	if err != nil {
		return nil, err
	}
	after, err := model.EncodeEntry(out)
	if err != nil {
		return nil, fmt.Errorf("encode audit image: %w", err)
	}
	return after, nil
}

func (s *Store) ListAuditEvents(ctx context.Context, f model.AuditFilter) ([]model.AuditEvent, error) {
	// Only placeholders are added to the SQL text; every value is an argument.
	var (
		conds []string
		args  []any
	)
	where := func(cond string, arg any) {
		args = append(args, arg)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if f.Namespace != "" {
		where("namespace = $%d", f.Namespace)
	}
	if f.EntityType != "" {
		where("entity_type = $%d", f.EntityType)
	}
	if f.EntityKey != "" {
		where("entity_key = $%d", f.EntityKey)
	}
	if f.Actor != "" {
		where("actor = $%d", f.Actor)
	}
	if !f.Since.IsZero() {
		where("created_at >= $%d", ceilMicro(f.Since))
	}
	if !f.Until.IsZero() {
		where("created_at < $%d", ceilMicro(f.Until))
	}
	if f.BeforeID > 0 {
		where("id < $%d", f.BeforeID)
	}
	query := `SELECT id, namespace, revision, actor, created_at, action, entity_type, entity_key, before_image, after_image, message FROM audit_events`
	if len(conds) > 0 {
		query += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	args = append(args, pageSize(f.Limit))
	query += fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.AuditEvent, error) {
		var (
			e             model.AuditEvent
			before, after []byte
		)
		err := row.Scan(&e.ID, &e.Namespace, &e.Revision, &e.Actor, &e.Time, &e.Action, &e.EntityType, &e.EntityKey,
			&before, &after, &e.Message)
		e.Time, e.Before, e.After = e.Time.UTC(), before, after
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	return out, nil
}

// ceilMicro rounds t up to a whole microsecond. Stored times have
// microsecond precision, so comparing them against the rounded bound gives
// the same result as comparing against t, whereas PostgreSQL would truncate
// t and let an event just before Since (or at Until) through.
func ceilMicro(t time.Time) time.Time {
	if r := t.Truncate(time.Microsecond); r.Before(t) {
		return r.Add(time.Microsecond)
	}
	return t
}

func (s *Store) ListActiveRollouts(ctx context.Context) ([]model.RolloutRef, error) {
	// The literal state matches the predicate of the flags_active_rollouts
	// partial index.
	rows, err := s.pool.Query(ctx, `
		SELECT namespace, key FROM flags WHERE rollout ->> 'state' = 'active'
		ORDER BY namespace COLLATE "C", key COLLATE "C"`)
	if err != nil {
		return nil, fmt.Errorf("list active rollouts: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.RolloutRef, error) {
		var r model.RolloutRef
		err := row.Scan(&r.Namespace, &r.Key)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("list active rollouts: %w", err)
	}
	return out, nil
}
