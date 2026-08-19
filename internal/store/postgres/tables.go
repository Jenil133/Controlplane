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
)

// table describes how one entity type is stored, so that reads, writes and
// rollback are implemented once for all of them.
type table[T any] struct {
	entity string // audit entity type
	name   string // SQL table, always a constant
	// columns starts with key and lists the columns in the order that scan
	// reads and values returns them; namespace is implied.
	columns  []string
	scan     func(row pgx.Row) (T, error)
	values   func(v T) ([]any, error)
	key      func(v T) string
	revision func(v T) int64
	stamp    func(v *T, rev int64, at time.Time, by string)

	// Derived from the above by newTable.
	selectSQL, upsertSQL string
}

func newTable[T any](t table[T]) table[T] {
	columns := strings.Join(t.columns, ", ")
	params := make([]string, len(t.columns)+1)
	for i := range params {
		params[i] = fmt.Sprintf("$%d", i+1)
	}
	updates := make([]string, 0, len(t.columns)-1)
	for _, c := range t.columns[1:] {
		updates = append(updates, c+" = EXCLUDED."+c)
	}
	t.selectSQL = `SELECT ` + columns + ` FROM ` + t.name
	t.upsertSQL = `INSERT INTO ` + t.name + ` (namespace, ` + columns + `) VALUES (` + strings.Join(params, ", ") + `)` +
		` ON CONFLICT (namespace, key) DO UPDATE SET ` + strings.Join(updates, ", ") +
		` RETURNING ` + columns
	return t
}

// get reads one entry; ok is false when it does not exist.
func (t table[T]) get(ctx context.Context, q querier, namespace, key string) (v T, ok bool, err error) {
	v, err = t.scan(q.QueryRow(ctx, t.selectSQL+` WHERE namespace = $1 AND key = $2`, namespace, key))
	if errors.Is(err, pgx.ErrNoRows) {
		var zero T
		return zero, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("read %s %q: %w", t.entity, key, err)
	}
	return v, true, nil
}

// upsert writes v and returns it as stored, which is how every later read
// returns it (JSON values come back in PostgreSQL's normalized form).
func (t table[T]) upsert(ctx context.Context, tx pgx.Tx, namespace string, v T) (T, error) {
	values, err := t.values(v)
	if err != nil {
		var zero T
		return zero, err
	}
	out, err := t.scan(tx.QueryRow(ctx, t.upsertSQL, append([]any{namespace}, values...)...))
	if err != nil {
		return out, fmt.Errorf("write %s %q: %w", t.entity, t.key(v), err)
	}
	return out, nil
}

func (t table[T]) delete(ctx context.Context, tx pgx.Tx, namespace, key string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM `+t.name+` WHERE namespace = $1 AND key = $2`, namespace, key); err != nil {
		return fmt.Errorf("delete %s %q: %w", t.entity, key, err)
	}
	return nil
}

// queueList adds a query to b that reads every entry of namespace into out,
// sorted by key byte-wise like model.SortSnapshot, whatever the database
// locale.
func (t table[T]) queueList(b *pgx.Batch, namespace string, out *[]T) {
	b.Queue(t.selectSQL+` WHERE namespace = $1 ORDER BY key COLLATE "C"`, namespace).Query(func(rows pgx.Rows) error {
		var err error
		*out, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (T, error) { return t.scan(row) })
		if err != nil {
			return fmt.Errorf("read %s: %w", t.name, err)
		}
		return nil
	})
}

var configs = newTable(table[model.Config]{
	entity:  model.EntityConfig,
	name:    "configs",
	columns: []string{"key", "value", "description", "revision", "updated_at", "updated_by"},
	scan: func(row pgx.Row) (model.Config, error) {
		var c model.Config
		err := row.Scan(&c.Key, &c.Value, &c.Description, &c.Revision, &c.UpdatedAt, &c.UpdatedBy)
		c.UpdatedAt = c.UpdatedAt.UTC()
		return c, err
	},
	values: func(c model.Config) ([]any, error) {
		return []any{c.Key, c.Value, c.Description, c.Revision, c.UpdatedAt, c.UpdatedBy}, nil
	},
	key:      func(c model.Config) string { return c.Key },
	revision: func(c model.Config) int64 { return c.Revision },
	stamp: func(c *model.Config, rev int64, at time.Time, by string) {
		c.Revision, c.UpdatedAt, c.UpdatedBy = rev, at, by
	},
})

var flags = newTable(table[model.Flag]{
	entity: model.EntityFlag,
	name:   "flags",
	columns: []string{
		"key", "enabled", "description", "rollout_percent", "salt", "allowlist", "rollout",
		"revision", "updated_at", "updated_by",
	},
	scan: func(row pgx.Row) (model.Flag, error) {
		var (
			f    model.Flag
			plan []byte
		)
		err := row.Scan(&f.Key, &f.Enabled, &f.Description, &f.RolloutPercent, &f.Salt, &f.Allowlist, &plan,
			&f.Revision, &f.UpdatedAt, &f.UpdatedBy)
		if err != nil {
			return f, err
		}
		f.UpdatedAt = f.UpdatedAt.UTC()
		if len(f.Allowlist) == 0 {
			f.Allowlist = nil
		}
		if plan != nil {
			f.Rollout = new(model.RolloutPlan)
			if err := json.Unmarshal(plan, f.Rollout); err != nil {
				return f, fmt.Errorf("decode rollout plan of flag %q: %w", f.Key, err)
			}
			f.Rollout.StartedAt = f.Rollout.StartedAt.UTC()
			f.Rollout.StageStartedAt = f.Rollout.StageStartedAt.UTC()
		}
		return f, nil
	},
	values: func(f model.Flag) ([]any, error) {
		var plan json.RawMessage // NULL without a plan
		if f.Rollout != nil {
			var err error
			if plan, err = json.Marshal(f.Rollout); err != nil {
				return nil, fmt.Errorf("encode rollout plan of flag %q: %w", f.Key, err)
			}
		}
		allowlist := f.Allowlist
		if allowlist == nil {
			allowlist = []string{} // pgx sends a nil slice as NULL
		}
		return []any{
			f.Key, f.Enabled, f.Description, f.RolloutPercent, f.Salt, allowlist, plan,
			f.Revision, f.UpdatedAt, f.UpdatedBy,
		}, nil
	},
	key:      func(f model.Flag) string { return f.Key },
	revision: func(f model.Flag) int64 { return f.Revision },
	stamp: func(f *model.Flag, rev int64, at time.Time, by string) {
		f.Revision, f.UpdatedAt, f.UpdatedBy = rev, at, by
	},
})

var experiments = newTable(table[model.Experiment]{
	entity:  model.EntityExperiment,
	name:    "experiments",
	columns: []string{"key", "enabled", "description", "salt", "variants", "revision", "updated_at", "updated_by"},
	scan: func(row pgx.Row) (model.Experiment, error) {
		var (
			e        model.Experiment
			variants []byte
		)
		if err := row.Scan(&e.Key, &e.Enabled, &e.Description, &e.Salt, &variants, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
			return e, err
		}
		e.UpdatedAt = e.UpdatedAt.UTC()
		if err := json.Unmarshal(variants, &e.Variants); err != nil {
			return e, fmt.Errorf("decode variants of experiment %q: %w", e.Key, err)
		}
		return e, nil
	},
	values: func(e model.Experiment) ([]any, error) {
		variants, err := json.Marshal(e.Variants)
		if err != nil {
			return nil, fmt.Errorf("encode variants of experiment %q: %w", e.Key, err)
		}
		return []any{e.Key, e.Enabled, e.Description, e.Salt, json.RawMessage(variants), e.Revision, e.UpdatedAt, e.UpdatedBy}, nil
	},
	key:      func(e model.Experiment) string { return e.Key },
	revision: func(e model.Experiment) int64 { return e.Revision },
	stamp: func(e *model.Experiment, rev int64, at time.Time, by string) {
		e.Revision, e.UpdatedAt, e.UpdatedBy = rev, at, by
	},
})

var rateLimits = newTable(table[model.RateLimit]{
	entity:  model.EntityRateLimit,
	name:    "rate_limits",
	columns: []string{"key", "enabled", "description", "requests_per_second", "burst", "revision", "updated_at", "updated_by"},
	scan: func(row pgx.Row) (model.RateLimit, error) {
		var r model.RateLimit
		err := row.Scan(&r.Key, &r.Enabled, &r.Description, &r.RequestsPerSecond, &r.Burst, &r.Revision, &r.UpdatedAt, &r.UpdatedBy)
		r.UpdatedAt = r.UpdatedAt.UTC()
		return r, err
	},
	values: func(r model.RateLimit) ([]any, error) {
		return []any{r.Key, r.Enabled, r.Description, r.RequestsPerSecond, int64(r.Burst), r.Revision, r.UpdatedAt, r.UpdatedBy}, nil
	},
	key:      func(r model.RateLimit) string { return r.Key },
	revision: func(r model.RateLimit) int64 { return r.Revision },
	stamp: func(r *model.RateLimit, rev int64, at time.Time, by string) {
		r.Revision, r.UpdatedAt, r.UpdatedBy = rev, at, by
	},
})

var breakers = newTable(table[model.CircuitBreaker]{
	entity: model.EntityCircuitBreaker,
	name:   "circuit_breakers",
	columns: []string{
		"key", "enabled", "description", "failure_rate_threshold", "min_requests", "window_ns", "open_duration_ns",
		"half_open_max_requests", "revision", "updated_at", "updated_by",
	},
	scan: func(row pgx.Row) (model.CircuitBreaker, error) {
		var (
			c                    model.CircuitBreaker
			window, openDuration int64
		)
		err := row.Scan(&c.Key, &c.Enabled, &c.Description, &c.FailureRateThreshold, &c.MinRequests, &window, &openDuration,
			&c.HalfOpenMaxRequests, &c.Revision, &c.UpdatedAt, &c.UpdatedBy)
		c.Window, c.OpenDuration = time.Duration(window), time.Duration(openDuration)
		c.UpdatedAt = c.UpdatedAt.UTC()
		return c, err
	},
	values: func(c model.CircuitBreaker) ([]any, error) {
		return []any{
			c.Key, c.Enabled, c.Description, c.FailureRateThreshold, int64(c.MinRequests), int64(c.Window), int64(c.OpenDuration),
			int64(c.HalfOpenMaxRequests), c.Revision, c.UpdatedAt, c.UpdatedBy,
		}, nil
	},
	key:      func(c model.CircuitBreaker) string { return c.Key },
	revision: func(c model.CircuitBreaker) int64 { return c.Revision },
	stamp: func(c *model.CircuitBreaker, rev int64, at time.Time, by string) {
		c.Revision, c.UpdatedAt, c.UpdatedBy = rev, at, by
	},
})
