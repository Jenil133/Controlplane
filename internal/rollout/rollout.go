// Package rollout implements staged percentage rollouts of feature flags.
//
// A rollout plan moves a flag's rollout_percent through stages whose
// percentages never decrease:
//
//	Start -> active <-> paused       (Pause, Resume)
//	active | paused -> completed     (Advance into the final stage)
//	active | paused -> aborted       (Abort; rollout_percent becomes 0)
//
// While a plan is active or paused it owns rollout_percent, which always
// equals the current stage's percentage. Only active plans move on their own:
// once the current stage has run for its duration, the Controller advances
// the flag to the next stage. Stages without a duration wait for a manual
// Advance.
//
// The transitions are pure: they take a flag as read from the store and
// return its next version, with times in UTC, without modifying the input.
// Callers write the result back with a compare-and-swap on the revision they
// read. That is what lets every replica run a Controller: when several race
// to advance the same stage, exactly one write succeeds.
package rollout

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
)

// Start begins a rollout of f through stages. The flag takes the first
// stage's percentage at once, and a single-stage plan is completed
// immediately. A completed or aborted plan is replaced; an active or paused
// one must be aborted first.
func Start(f model.Flag, stages []model.RolloutStage, actor string, now time.Time) (model.Flag, error) {
	if err := model.ValidateRolloutStages(stages); err != nil {
		return model.Flag{}, err
	}
	if p := f.Rollout; p != nil && p.State.Owns() {
		return model.Flag{}, model.FailedPreconditionf("flag %q: a rollout is already %s at %s; abort it first", f.Key, p.State, Describe(p))
	}
	now = now.UTC()
	f = f.Clone()
	f.Rollout = &model.RolloutPlan{
		// Copied so the stored plan never aliases the caller's slice.
		Stages:         slices.Clone(stages),
		State:          model.RolloutActive,
		StartedAt:      now,
		StageStartedAt: now,
		StartedBy:      actor,
	}
	if len(stages) == 1 {
		f.Rollout.State = model.RolloutCompleted
	}
	f.RolloutPercent = stages[0].Percent
	return checked(f)
}

// Advance moves an active or paused rollout to its next stage and the flag to
// that stage's percentage. Entering the final stage completes the rollout;
// otherwise the state is kept, so a paused rollout stays paused.
func Advance(f model.Flag, now time.Time) (model.Flag, error) {
	p, err := requirePlan(f, "advance", model.RolloutActive, model.RolloutPaused)
	if err != nil {
		return model.Flag{}, err
	}
	if p.CurrentStage < 0 || p.CurrentStage >= len(p.Stages)-1 {
		return model.Flag{}, model.FailedPreconditionf("cannot advance the rollout of flag %q past %s", f.Key, Describe(p))
	}
	f = f.Clone()
	p = f.Rollout
	p.CurrentStage++
	p.StageStartedAt = now.UTC()
	if p.CurrentStage == len(p.Stages)-1 {
		p.State = model.RolloutCompleted
	}
	f.RolloutPercent = p.Stages[p.CurrentStage].Percent
	return checked(f)
}

// Pause holds an active rollout at its current stage. The controller leaves
// paused rollouts alone; they can still be advanced or aborted by hand.
// Pause takes a time only to share the signature of the other transitions:
// nothing is recorded, because Resume restarts the stage timer.
func Pause(f model.Flag, _ time.Time) (model.Flag, error) {
	if _, err := requirePlan(f, "pause", model.RolloutActive); err != nil {
		return model.Flag{}, err
	}
	f = f.Clone()
	f.Rollout.State = model.RolloutPaused
	return checked(f)
}

// Resume reactivates a paused rollout. The current stage starts over, so it
// runs for its full duration before the controller advances it.
func Resume(f model.Flag, now time.Time) (model.Flag, error) {
	if _, err := requirePlan(f, "resume", model.RolloutPaused); err != nil {
		return model.Flag{}, err
	}
	f = f.Clone()
	f.Rollout.State = model.RolloutActive
	f.Rollout.StageStartedAt = now.UTC()
	return checked(f)
}

// Abort stops an active or paused rollout and sets the flag's percentage to
// 0, so only allowlisted units still see it on. The plan stays on the flag,
// at the stage where it stopped, as a record. Like Pause, Abort records no
// time.
func Abort(f model.Flag, _ time.Time) (model.Flag, error) {
	if _, err := requirePlan(f, "abort", model.RolloutActive, model.RolloutPaused); err != nil {
		return model.Flag{}, err
	}
	f = f.Clone()
	f.Rollout.State = model.RolloutAborted
	f.RolloutPercent = 0
	return checked(f)
}

// Due reports whether the controller should advance f at now: the rollout is
// active, a later stage exists, and the current stage has a duration that has
// fully elapsed since the stage started.
func Due(f model.Flag, now time.Time) bool {
	p := f.Rollout
	if p == nil || p.State != model.RolloutActive || p.CurrentStage < 0 || p.CurrentStage >= len(p.Stages)-1 {
		return false
	}
	d := p.Stages[p.CurrentStage].Duration
	return d > 0 && !now.Before(p.StageStartedAt.Add(d))
}

// Describe summarizes where a plan stands, e.g. "stage 2/4 (25%)". Stages are
// numbered from 1.
func Describe(p *model.RolloutPlan) string {
	if p == nil {
		return "no rollout"
	}
	if p.CurrentStage < 0 || p.CurrentStage >= len(p.Stages) {
		return fmt.Sprintf("stage %d/%d", p.CurrentStage+1, len(p.Stages))
	}
	return fmt.Sprintf("stage %d/%d (%s%%)", p.CurrentStage+1, len(p.Stages), formatPercent(p.Stages[p.CurrentStage].Percent))
}

// formatPercent prints a percentage with at most two decimals and no trailing
// zeros. Rounding first hides float noise such as 0.30000000000000004, which
// model.ValidatePercent accepts as 0.3.
func formatPercent(p float64) string {
	return strconv.FormatFloat(math.Round(p*100)/100, 'f', -1, 64)
}

// requirePlan returns f's rollout plan if it is in one of states, and
// otherwise an error naming the refused operation.
func requirePlan(f model.Flag, verb string, states ...model.RolloutState) (*model.RolloutPlan, error) {
	p := f.Rollout
	if p == nil {
		return nil, model.FailedPreconditionf("flag %q has no rollout to %s", f.Key, verb)
	}
	if !slices.Contains(states, p.State) {
		return nil, model.FailedPreconditionf("cannot %s the rollout of flag %q: it is %s", verb, f.Key, p.State)
	}
	return p, nil
}

// checked returns f if it is valid. The transitions keep valid flags valid;
// this stops a malformed input, or a zero time, before it reaches the store,
// which trusts its callers to validate.
func checked(f model.Flag) (model.Flag, error) {
	if err := f.Validate(); err != nil {
		return model.Flag{}, err
	}
	return f, nil
}
