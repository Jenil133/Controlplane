package rollout

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
)

const (
	active    = model.RolloutActive
	paused    = model.RolloutPaused
	completed = model.RolloutCompleted
	aborted   = model.RolloutAborted
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// testStages has two timed stages, a manual one and the final 100%.
var testStages = []model.RolloutStage{
	{Percent: 1, Duration: time.Hour},
	{Percent: 5, Duration: 2 * time.Hour},
	{Percent: 25},
	{Percent: 100},
}

// plainFlag returns a flag as the store holds it, without a rollout.
func plainFlag() model.Flag {
	return model.Flag{
		Key: "new-cart", Enabled: true, Description: "new checkout cart",
		RolloutPercent: 100, Salt: "new-cart", Allowlist: []string{"qa-1", "qa-2"},
		Revision: 7, UpdatedAt: t0.Add(-time.Hour), UpdatedBy: "alice",
	}
}

// flagAt returns plainFlag with a testStages rollout, started at t0, in state
// at stage, as the transitions would have left it.
func flagAt(state model.RolloutState, stage int) model.Flag {
	f := plainFlag()
	f.Rollout = &model.RolloutPlan{
		Stages: slices.Clone(testStages), CurrentStage: stage, State: state,
		StartedAt: t0, StageStartedAt: t0, StartedBy: "alice",
	}
	f.RolloutPercent = testStages[stage].Percent
	if state == aborted {
		f.RolloutPercent = 0
	}
	return f
}

func sameBacking[T any](a, b []T) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

// transition runs fn on in and fails the test if fn modified in or, when it
// succeeds, if the result is invalid, shares memory with in, or differs from
// in outside the rollout plan and percentage.
func transition(t *testing.T, in model.Flag, fn func(model.Flag) (model.Flag, error)) (model.Flag, error) {
	t.Helper()
	before := in.Clone()
	out, err := fn(in)
	if !reflect.DeepEqual(in, before) {
		t.Fatalf("input modified:\n got  %+v\n want %+v", in, before)
	}
	if err != nil {
		return out, err
	}
	if verr := out.Validate(); verr != nil {
		t.Fatalf("result fails Validate: %v", verr)
	}
	if in.Rollout != nil && (out.Rollout == in.Rollout || sameBacking(out.Rollout.Stages, in.Rollout.Stages)) {
		t.Fatal("result shares the input's rollout plan")
	}
	if sameBacking(out.Allowlist, in.Allowlist) {
		t.Fatal("result shares the input's allowlist")
	}
	rest, wantRest := out.Clone(), in.Clone()
	rest.Rollout, rest.RolloutPercent = nil, 0
	wantRest.Rollout, wantRest.RolloutPercent = nil, 0
	if !reflect.DeepEqual(rest, wantRest) {
		t.Fatalf("fields outside the rollout changed:\n got  %+v\n want %+v", rest, wantRest)
	}
	return out, nil
}

func TestStart(t *testing.T) {
	now := t0.Add(5 * time.Minute)
	tests := []struct {
		name      string
		in        model.Flag
		stages    []model.RolloutStage
		wantErr   error
		wantState model.RolloutState
	}{
		{"no previous rollout", plainFlag(), testStages, nil, active},
		{"single stage completes at once", plainFlag(), testStages[3:], nil, completed},
		{"replaces a completed rollout", flagAt(completed, 3), testStages, nil, active},
		{"replaces an aborted rollout", flagAt(aborted, 1), testStages, nil, active},
		{"refuses an active rollout", flagAt(active, 1), testStages, model.ErrFailedPrecondition, ""},
		{"refuses a paused rollout", flagAt(paused, 1), testStages, model.ErrFailedPrecondition, ""},
		{"no stages", plainFlag(), nil, model.ErrInvalid, ""},
		{"decreasing stages", plainFlag(), []model.RolloutStage{{Percent: 50}, {Percent: 10}}, model.ErrInvalid, ""},
		{"final stage at 0%", plainFlag(), []model.RolloutStage{{Percent: 0}}, model.ErrInvalid, ""},
		{"stages are checked before the state", flagAt(active, 1), nil, model.ErrInvalid, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stagesBefore := slices.Clone(tt.stages)
			out, err := transition(t, tt.in, func(f model.Flag) (model.Flag, error) {
				return Start(f, tt.stages, "bob", now)
			})
			if !slices.Equal(tt.stages, stagesBefore) {
				t.Fatalf("stages argument modified: %v", tt.stages)
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Start error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			p := out.Rollout
			if p.State != tt.wantState || p.CurrentStage != 0 || p.StartedBy != "bob" {
				t.Fatalf("plan = %+v, want %s at stage 0, started by bob", p, tt.wantState)
			}
			if !p.StartedAt.Equal(now) || !p.StageStartedAt.Equal(now) {
				t.Fatalf("plan times = %v, %v; want both %v", p.StartedAt, p.StageStartedAt, now)
			}
			if !slices.Equal(p.Stages, tt.stages) || sameBacking(p.Stages, tt.stages) {
				t.Fatalf("plan stages = %v, want an unshared copy of %v", p.Stages, tt.stages)
			}
			if out.RolloutPercent != tt.stages[0].Percent {
				t.Fatalf("RolloutPercent = %v, want %v", out.RolloutPercent, tt.stages[0].Percent)
			}
		})
	}
}

func TestAdvance(t *testing.T) {
	now := t0.Add(3 * time.Hour)
	tests := []struct {
		name      string
		in        model.Flag
		wantErr   error
		wantStage int
		wantState model.RolloutState
	}{
		{"active moves to the next stage", flagAt(active, 0), nil, 1, active},
		{"paused stays paused", flagAt(paused, 1), nil, 2, paused},
		{"active into the final stage completes", flagAt(active, 2), nil, 3, completed},
		{"paused into the final stage completes", flagAt(paused, 2), nil, 3, completed},
		{"no rollout", plainFlag(), model.ErrFailedPrecondition, 0, ""},
		{"completed", flagAt(completed, 3), model.ErrFailedPrecondition, 0, ""},
		{"aborted", flagAt(aborted, 1), model.ErrFailedPrecondition, 0, ""},
		{"active at the final stage", flagAt(active, 3), model.ErrFailedPrecondition, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := transition(t, tt.in, func(f model.Flag) (model.Flag, error) { return Advance(f, now) })
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Advance error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Advance: %v", err)
			}
			p := out.Rollout
			if p.CurrentStage != tt.wantStage || p.State != tt.wantState {
				t.Fatalf("plan at stage %d %s, want stage %d %s", p.CurrentStage, p.State, tt.wantStage, tt.wantState)
			}
			if want := testStages[tt.wantStage].Percent; out.RolloutPercent != want {
				t.Fatalf("RolloutPercent = %v, want %v", out.RolloutPercent, want)
			}
			if !p.StageStartedAt.Equal(now) || !p.StartedAt.Equal(t0) {
				t.Fatalf("StageStartedAt = %v, StartedAt = %v; want %v, %v", p.StageStartedAt, p.StartedAt, now, t0)
			}
		})
	}
}

// TestTransitionStates checks which plan states each transition accepts;
// everything else, including a flag without a rollout, is a failed
// precondition.
func TestTransitionStates(t *testing.T) {
	now := t0.Add(time.Hour)
	tests := []struct {
		name   string
		fn     func(model.Flag, time.Time) (model.Flag, error)
		accept []model.RolloutState
	}{
		{"Advance", Advance, []model.RolloutState{active, paused}},
		{"Pause", Pause, []model.RolloutState{active}},
		{"Resume", Resume, []model.RolloutState{paused}},
		{"Abort", Abort, []model.RolloutState{active, paused}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, state := range []model.RolloutState{active, paused, completed, aborted} {
				stage := 1
				if state == completed {
					stage = len(testStages) - 1
				}
				_, err := transition(t, flagAt(state, stage), func(f model.Flag) (model.Flag, error) { return tt.fn(f, now) })
				if want := slices.Contains(tt.accept, state); want && err != nil {
					t.Errorf("from %s: %v, want success", state, err)
				} else if !want && !errors.Is(err, model.ErrFailedPrecondition) {
					t.Errorf("from %s: error = %v, want ErrFailedPrecondition", state, err)
				}
			}
			if _, err := tt.fn(plainFlag(), now); !errors.Is(err, model.ErrFailedPrecondition) {
				t.Errorf("without a rollout: error = %v, want ErrFailedPrecondition", err)
			}
		})
	}
}

func TestPauseKeepsStageAndTimer(t *testing.T) {
	out, err := transition(t, flagAt(active, 1), func(f model.Flag) (model.Flag, error) { return Pause(f, t0.Add(time.Hour)) })
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	p := out.Rollout
	if p.State != paused || p.CurrentStage != 1 || out.RolloutPercent != testStages[1].Percent || !p.StageStartedAt.Equal(t0) {
		t.Fatalf("after Pause: plan %+v, percent %v; want paused at stage 1 (%v%%) since %v", p, out.RolloutPercent, testStages[1].Percent, t0)
	}
	if Due(out, t0.Add(100*time.Hour)) {
		t.Fatal("a paused rollout is due")
	}
}

func TestResumeRestartsStageTimer(t *testing.T) {
	now := t0.Add(10 * time.Hour)
	out, err := transition(t, flagAt(paused, 1), func(f model.Flag) (model.Flag, error) { return Resume(f, now) })
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	p := out.Rollout
	if p.State != active || p.CurrentStage != 1 || out.RolloutPercent != testStages[1].Percent {
		t.Fatalf("after Resume: plan %+v, percent %v; want active at stage 1 (%v%%)", p, out.RolloutPercent, testStages[1].Percent)
	}
	if !p.StageStartedAt.Equal(now) || !p.StartedAt.Equal(t0) {
		t.Fatalf("StageStartedAt = %v, StartedAt = %v; want %v, %v", p.StageStartedAt, p.StartedAt, now, t0)
	}
	// The stage ran 10h before the pause, yet it gets its full 2h again.
	d := testStages[1].Duration
	if Due(out, now.Add(d-time.Nanosecond)) || !Due(out, now.Add(d)) {
		t.Fatalf("resumed stage is not due exactly %v after Resume", d)
	}
}

func TestAbort(t *testing.T) {
	for _, state := range []model.RolloutState{active, paused} {
		t.Run(string(state), func(t *testing.T) {
			out, err := transition(t, flagAt(state, 2), func(f model.Flag) (model.Flag, error) { return Abort(f, t0.Add(time.Hour)) })
			if err != nil {
				t.Fatalf("Abort: %v", err)
			}
			p := out.Rollout
			if p.State != aborted || p.CurrentStage != 2 || out.RolloutPercent != 0 {
				t.Fatalf("after Abort: plan %+v, percent %v; want aborted at stage 2 with 0%%", p, out.RolloutPercent)
			}
			if !p.StartedAt.Equal(t0) || !p.StageStartedAt.Equal(t0) || p.StartedBy != "alice" {
				t.Fatalf("Abort rewrote the record of the plan: %+v", p)
			}
		})
	}
}

// TestTimesAreUTC: a time's JSON encoding carries its zone, so a non-UTC time
// would encode an unchanged instant differently from the stored one.
func TestTimesAreUTC(t *testing.T) {
	now := t0.Add(time.Hour).In(time.FixedZone("UTC+5", 5*60*60))
	started, err := Start(plainFlag(), testStages, "bob", now)
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := Advance(flagAt(active, 0), now)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := Resume(flagAt(paused, 1), now)
	if err != nil {
		t.Fatal(err)
	}
	times := map[string]time.Time{
		"Start StartedAt":        started.Rollout.StartedAt,
		"Start StageStartedAt":   started.Rollout.StageStartedAt,
		"Advance StageStartedAt": advanced.Rollout.StageStartedAt,
		"Resume StageStartedAt":  resumed.Rollout.StageStartedAt,
	}
	for name, got := range times {
		if got.Location() != time.UTC || !got.Equal(now) {
			t.Errorf("%s = %v, want %v in UTC", name, got, now.UTC())
		}
	}
}

func TestZeroTimeIsRejected(t *testing.T) {
	if _, err := Start(plainFlag(), testStages, "bob", time.Time{}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Start at the zero time: error = %v, want ErrInvalid", err)
	}
	if _, err := Advance(flagAt(active, 0), time.Time{}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Advance at the zero time: error = %v, want ErrInvalid", err)
	}
	if _, err := Resume(flagAt(paused, 0), time.Time{}); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("Resume at the zero time: error = %v, want ErrInvalid", err)
	}
}

func TestInvalidFlagIsRejected(t *testing.T) {
	f := flagAt(active, 0)
	f.Salt = "" // never normalized
	if _, err := Pause(f, t0); !errors.Is(err, model.ErrInvalid) {
		t.Fatalf("Pause of an invalid flag: error = %v, want ErrInvalid", err)
	}
}

func TestDue(t *testing.T) {
	lastTimed := flagAt(active, 0)
	lastTimed.Rollout.Stages = []model.RolloutStage{{Percent: 1, Duration: time.Hour}, {Percent: 100, Duration: time.Hour}}
	lastTimed.Rollout.CurrentStage = 1
	lastTimed.RolloutPercent = 100
	outOfRange := flagAt(active, 0)
	outOfRange.Rollout.CurrentStage = 7
	negative := flagAt(active, 0)
	negative.Rollout.CurrentStage = -1

	far := t0.Add(1000 * time.Hour)
	tests := []struct {
		name string
		f    model.Flag
		now  time.Time
		want bool
	}{
		{"stage still running", flagAt(active, 0), t0.Add(time.Hour - time.Nanosecond), false},
		{"stage duration just elapsed", flagAt(active, 0), t0.Add(time.Hour), true},
		{"long overdue", flagAt(active, 1), far, true},
		{"clock behind the stage start", flagAt(active, 0), t0.Add(-time.Hour), false},
		{"manual stage", flagAt(active, 2), far, false},
		{"final stage with a duration", lastTimed, far, false},
		{"paused", flagAt(paused, 0), far, false},
		{"completed", flagAt(completed, 3), far, false},
		{"aborted", flagAt(aborted, 0), far, false},
		{"no rollout", plainFlag(), far, false},
		{"stage index out of range", outOfRange, far, false},
		{"negative stage index", negative, far, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Due(tt.f, tt.now); got != tt.want {
				t.Fatalf("Due = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDescribe(t *testing.T) {
	plan := func(stage int, percents ...float64) *model.RolloutPlan {
		p := &model.RolloutPlan{CurrentStage: stage}
		for _, pct := range percents {
			p.Stages = append(p.Stages, model.RolloutStage{Percent: pct})
		}
		return p
	}
	// A runtime sum: the constant expression 0.1+0.2 would be exactly 0.3.
	tenth := 0.1
	noisy := tenth + 0.2
	tests := []struct {
		plan *model.RolloutPlan
		want string
	}{
		{nil, "no rollout"},
		{plan(1, 1, 25, 50, 100), "stage 2/4 (25%)"},
		{plan(0, 0.5, 100), "stage 1/2 (0.5%)"},
		{plan(0, 12.25, 100), "stage 1/2 (12.25%)"},
		{plan(0, noisy, 100), "stage 1/2 (0.3%)"},
		{plan(2, 1, 5, 100), "stage 3/3 (100%)"},
		{plan(0, 0, 100), "stage 1/2 (0%)"},
		{plan(5, 1, 100), "stage 6/2"},
		{plan(-1, 1, 100), "stage 0/2"},
	}
	for _, tt := range tests {
		if got := Describe(tt.plan); got != tt.want {
			t.Errorf("Describe(%+v) = %q, want %q", tt.plan, got, tt.want)
		}
	}
}
