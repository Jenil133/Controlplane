package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// ControllerActor is recorded as the actor of every advance the controller
// makes: in the flag's updated_by, the audit log and the revision history.
const ControllerActor = "rollout-controller"

const defaultInterval = 5 * time.Second

// ControllerConfig configures a Controller. Store is required.
type ControllerConfig struct {
	Store store.Store
	// Interval is how often Run calls Tick. Defaults to 5s.
	Interval time.Duration
	// Now returns the current time. Defaults to time.Now().UTC().
	Now func() time.Time
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// OnChange, if set, is called after each committed advance with the
	// namespace's new revision, e.g. to push the change to watchers.
	OnChange func(ctx context.Context, namespace string, revision int64)
}

// Controller advances active rollouts whose current stage has run for its
// duration, at most one stage per flag per tick. It keeps no state between
// ticks: every decision is re-read from the store and every write is a
// compare-and-swap on the revision read, so concurrent ticks, on this replica
// or others, advance each stage exactly once. Safe for concurrent use.
type Controller struct {
	store    store.Store
	interval time.Duration
	now      func() time.Time
	log      *slog.Logger
	onChange func(ctx context.Context, namespace string, revision int64)
}

// NewController returns a controller. Call Run to start it.
func NewController(cfg ControllerConfig) *Controller {
	c := &Controller{
		store:    cfg.Store,
		interval: cfg.Interval,
		now:      cfg.Now,
		log:      cfg.Logger,
		onChange: cfg.OnChange,
	}
	if c.interval <= 0 {
		c.interval = defaultInterval
	}
	if c.now == nil {
		c.now = func() time.Time { return time.Now().UTC() }
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	return c
}

// Run calls Tick every Interval, the first time one Interval after it starts,
// until ctx is done; then it returns nil. Tick logs its own failures and the
// next tick retries, so Run never gives up.
func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, _ = c.Tick(ctx)
		}
	}
}

// Tick advances every due rollout by one stage and returns how many it
// advanced. A flag that changed or disappeared after it was listed is
// skipped: another replica or a user acted on it first. Other failures are
// logged and do not stop the remaining flags; the first is returned once all
// are processed. When ctx is done Tick stops early and returns ctx's error.
func (c *Controller) Tick(ctx context.Context) (advanced int, err error) {
	refs, err := c.store.ListActiveRollouts(ctx)
	if err != nil {
		c.warn(ctx, "rollout: list active rollouts failed; retrying next tick", "error", err)
		return 0, fmt.Errorf("list active rollouts: %w", err)
	}
	var first error
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return advanced, err
		}
		ok, err := c.advance(ctx, ref)
		if err != nil {
			c.warn(ctx, "rollout: advance failed; retrying next tick", "namespace", ref.Namespace, "flag", ref.Key, "error", err)
			if first == nil {
				first = fmt.Errorf("advance rollout of flag %q in namespace %q: %w", ref.Key, ref.Namespace, err)
			}
			continue
		}
		if ok {
			advanced++
		}
	}
	return advanced, first
}

// advance moves one flag to its next stage if its current stage is due, and
// reports whether it committed a change.
func (c *Controller) advance(ctx context.Context, ref model.RolloutRef) (bool, error) {
	f, err := c.store.GetFlag(ctx, ref.Namespace, ref.Key)
	if errors.Is(err, model.ErrNotFound) {
		return false, nil // deleted since it was listed
	}
	if err != nil {
		return false, err
	}
	now := c.now()
	if !Due(f, now) {
		return false, nil
	}
	next, err := Advance(f, now)
	if err != nil {
		return false, err
	}
	stage := Describe(next.Rollout)
	out, err := c.store.PutFlag(ctx, ref.Namespace, next, store.WriteOptions{
		Actor:            ControllerActor,
		ExpectedRevision: f.Revision,
		Action:           model.ActionRolloutAdvance,
		Message:          fmt.Sprintf("advance flag %s to %s", ref.Key, stage),
	})
	if errors.Is(err, model.ErrConflict) {
		return false, nil // another replica or a user changed it after our read
	}
	if err != nil {
		return false, err
	}
	c.log.Info("rollout advanced", "namespace", ref.Namespace, "flag", ref.Key, "stage", stage, "state", next.Rollout.State, "revision", out.Revision)
	if c.onChange != nil {
		c.onChange(ctx, ref.Namespace, out.Revision)
	}
	return true, nil
}

// warn logs a failure unless ctx is done: the failure is then most likely the
// cancellation itself, i.e. shutdown, and not worth a warning.
func (c *Controller) warn(ctx context.Context, msg string, args ...any) {
	if ctx.Err() == nil {
		c.log.Warn(msg, args...)
	}
}
