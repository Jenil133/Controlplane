package server

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/rollout"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

const (
	active    = cpv1.RolloutState_ROLLOUT_STATE_ACTIVE
	paused    = cpv1.RolloutState_ROLLOUT_STATE_PAUSED
	completed = cpv1.RolloutState_ROLLOUT_STATE_COMPLETED
	aborted   = cpv1.RolloutState_ROLLOUT_STATE_ABORTED
)

// rolloutCall invokes one rollout RPC for flag f in namespace svc.
type rolloutCall func(ctx context.Context, r *replica) (*cpv1.Flag, error)

func startRollout(stages ...*cpv1.RolloutStage) rolloutCall {
	return func(ctx context.Context, r *replica) (*cpv1.Flag, error) {
		resp, err := r.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: "svc", Flag: "f", Stages: stages})
		return resp.GetFlag(), err
	}
}

func advanceRollout(ctx context.Context, r *replica) (*cpv1.Flag, error) {
	resp, err := r.admin.AdvanceRollout(ctx, &cpv1.AdvanceRolloutRequest{Namespace: "svc", Flag: "f"})
	return resp.GetFlag(), err
}

func pauseRollout(ctx context.Context, r *replica) (*cpv1.Flag, error) {
	resp, err := r.admin.PauseRollout(ctx, &cpv1.PauseRolloutRequest{Namespace: "svc", Flag: "f"})
	return resp.GetFlag(), err
}

func resumeRollout(ctx context.Context, r *replica) (*cpv1.Flag, error) {
	resp, err := r.admin.ResumeRollout(ctx, &cpv1.ResumeRolloutRequest{Namespace: "svc", Flag: "f"})
	return resp.GetFlag(), err
}

func abortRollout(ctx context.Context, r *replica) (*cpv1.Flag, error) {
	resp, err := r.admin.AbortRollout(ctx, &cpv1.AbortRolloutRequest{Namespace: "svc", Flag: "f"})
	return resp.GetFlag(), err
}

func stage(percent float64, d time.Duration) *cpv1.RolloutStage {
	return &cpv1.RolloutStage{Percent: percent, Duration: durationpb.New(d)}
}

// checkRollout fails unless f is at the given stage of its rollout.
func checkRollout(t *testing.T, step string, f *cpv1.Flag, percent float64, index int32, state cpv1.RolloutState) {
	t.Helper()
	if p := f.GetRollout(); f.GetRolloutPercent() != percent || p.GetCurrentStage() != index || p.GetState() != state {
		t.Fatalf("%s: flag at %v%%, stage %d, %v; want %v%%, stage %d, %v",
			step, f.GetRolloutPercent(), p.GetCurrentStage(), p.GetState(), percent, index, state)
	}
}

func TestRolloutLifecycleReachesWatchers(t *testing.T) {
	clock := newFakeClock()
	r := startReplica(t, memory.New(), replicaOptions{now: clock.Now})
	createNamespace(t, r, "svc")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)
	ctx := ctxAs(t, "alice")
	t0 := clock.Now()

	steps := []struct {
		name    string
		advance time.Duration // fake time passing before the call
		call    rolloutCall
		percent float64
		stage   int32
		state   cpv1.RolloutState
		action  string // audited as
		message string // in the audit event and the revision history
	}{
		{"start", 0, startRollout(stage(1, time.Hour), stage(5, time.Hour), &cpv1.RolloutStage{Percent: 25}, stage(100, 0)),
			1, 0, active, model.ActionRolloutStart, "start rollout of flag f: stage 1/4 (1%)"},
		{"pause", time.Minute, pauseRollout, 1, 0, paused, model.ActionRolloutPause, "pause rollout of flag f: stage 1/4 (1%)"},
		{"advance while paused", time.Minute, advanceRollout, 5, 1, paused, model.ActionRolloutAdvance, "advance rollout of flag f: stage 2/4 (5%)"},
		{"resume", time.Minute, resumeRollout, 5, 1, active, model.ActionRolloutResume, "resume rollout of flag f: stage 2/4 (5%)"},
		{"advance", time.Minute, advanceRollout, 25, 2, active, model.ActionRolloutAdvance, "advance rollout of flag f: stage 3/4 (25%)"},
		{"abort", time.Minute, abortRollout, 0, 2, aborted, model.ActionRolloutAbort, "abort rollout of flag f: stage 3/4 (25%)"},
		{"restart", time.Minute, startRollout(stage(50, 0), stage(100, 0)), 50, 0, active, model.ActionRolloutStart, "start rollout of flag f: stage 1/2 (50%)"},
		{"complete", time.Minute, advanceRollout, 100, 1, completed, model.ActionRolloutAdvance, "advance rollout of flag f: stage 2/2 (100%)"},
	}
	var stageStarted time.Time
	for _, step := range steps {
		clock.Advance(step.advance)
		f, err := step.call(ctx, r)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		checkRollout(t, step.name, f, step.percent, step.stage, step.state)
		pushed := snapshotFlag(t, recv(t, stream, propagationBudget), "f")
		if !proto.Equal(pushed, f) {
			t.Fatalf("%s: watcher got %v, RPC returned %v", step.name, pushed, f)
		}
		if f.GetUpdatedBy() != "alice" || f.GetRollout().GetStartedBy() != "alice" {
			t.Fatalf("%s: updated by %q, rollout started by %q; want alice", step.name, f.GetUpdatedBy(), f.GetRollout().GetStartedBy())
		}
		// Stage timers restart on start, advance and resume only.
		switch step.name {
		case "start", "advance while paused", "resume", "advance", "restart", "complete":
			stageStarted = clock.Now()
		}
		if got := f.GetRollout().GetStageStartedAt().AsTime(); !got.Equal(stageStarted) {
			t.Fatalf("%s: stage_started_at = %v, want %v", step.name, got, stageStarted)
		}
	}

	// The first plan's stages and start survive on the wire unchanged.
	first, err := r.admin.GetRevision(ctx, &cpv1.GetRevisionRequest{Namespace: "svc", Revision: 3})
	if err != nil {
		t.Fatal(err)
	}
	plan := snapshotFlag(t, first.GetRevision().GetSnapshot(), "f").GetRollout()
	var durations []time.Duration
	for _, s := range plan.GetStages() {
		durations = append(durations, s.GetDuration().AsDuration())
	}
	if !plan.GetStartedAt().AsTime().Equal(t0) || len(durations) != 4 ||
		durations[0] != time.Hour || durations[1] != time.Hour || durations[2] != 0 || durations[3] != 0 {
		t.Fatalf("plan at revision 3 = %v, want started at %v with durations 1h, 1h, 0, 0", plan, t0)
	}

	// Every transition is audited and summarized in the history.
	events, err := r.admin.ListAuditEvents(ctx, &cpv1.ListAuditEventsRequest{Namespace: "svc", EntityType: model.EntityFlag, EntityKey: "f"})
	if err != nil {
		t.Fatal(err)
	}
	revs, err := r.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: "svc", PageSize: int32(len(steps))})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.GetEvents()) != len(steps)+1 || len(revs.GetRevisions()) != len(steps) {
		t.Fatalf("got %d audit events and %d revisions, want %d and %d", len(events.GetEvents()), len(revs.GetRevisions()), len(steps)+1, len(steps))
	}
	for i, step := range steps {
		e := events.GetEvents()[len(steps)-1-i] // newest first
		rev := revs.GetRevisions()[len(steps)-1-i]
		if e.GetAction() != step.action || e.GetMessage() != step.message || e.GetActor() != "alice" || rev.GetSummary() != step.message {
			t.Errorf("%s: audit %s %q by %q, revision summary %q; want %s %q by alice",
				step.name, e.GetAction(), e.GetMessage(), e.GetActor(), rev.GetSummary(), step.action, step.message)
		}
	}
}

func TestRolloutErrors(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	ctx := ctxAs(t, "alice")

	steps := []struct {
		name string
		call rolloutCall
		code codes.Code
	}{
		{"advance without rollout", advanceRollout, codes.FailedPrecondition},
		{"pause without rollout", pauseRollout, codes.FailedPrecondition},
		{"resume without rollout", resumeRollout, codes.FailedPrecondition},
		{"abort without rollout", abortRollout, codes.FailedPrecondition},
		{"no stages", startRollout(), codes.InvalidArgument},
		{"decreasing stages", startRollout(stage(50, 0), stage(10, 0)), codes.InvalidArgument},
		{"stage above 100%", startRollout(stage(101, 0)), codes.InvalidArgument},
		{"final stage at 0%", startRollout(stage(0, 0)), codes.InvalidArgument},
		{"negative duration", startRollout(stage(10, -time.Second), stage(100, 0)), codes.InvalidArgument},
		{"malformed duration", startRollout(&cpv1.RolloutStage{Percent: 10, Duration: &durationpb.Duration{Seconds: 1, Nanos: -1}}, stage(100, 0)), codes.InvalidArgument},
		{"start", startRollout(stage(10, 0), stage(100, 0)), codes.OK},
		{"start twice", startRollout(stage(20, 0), stage(100, 0)), codes.FailedPrecondition},
		{"resume while active", resumeRollout, codes.FailedPrecondition},
		{"pause", pauseRollout, codes.OK},
		{"pause twice", pauseRollout, codes.FailedPrecondition},
		{"start while paused", startRollout(stage(20, 0), stage(100, 0)), codes.FailedPrecondition},
		{"complete", advanceRollout, codes.OK},
		{"advance past the end", advanceRollout, codes.FailedPrecondition},
		{"abort after completion", abortRollout, codes.FailedPrecondition},
	}
	for _, step := range steps {
		rev := namespaceRevision(t, r, "svc")
		_, err := step.call(ctx, r)
		if got := status.Code(err); got != step.code {
			t.Fatalf("%s: got %v (%v), want %v", step.name, got, err, step.code)
		}
		if got := namespaceRevision(t, r, "svc"); step.code != codes.OK && got != rev {
			t.Fatalf("%s: failed call moved the namespace from revision %d to %d", step.name, rev, got)
		}
	}

	for _, tt := range []struct {
		name string
		req  *cpv1.AdvanceRolloutRequest
		code codes.Code
	}{
		{"missing flag", &cpv1.AdvanceRolloutRequest{Namespace: "svc", Flag: "missing"}, codes.NotFound},
		{"missing namespace", &cpv1.AdvanceRolloutRequest{Namespace: "missing", Flag: "f"}, codes.NotFound},
		{"invalid flag key", &cpv1.AdvanceRolloutRequest{Namespace: "svc", Flag: "bad key"}, codes.InvalidArgument},
		{"invalid namespace", &cpv1.AdvanceRolloutRequest{Namespace: "Bad", Flag: "f"}, codes.InvalidArgument},
	} {
		_, err := r.admin.AdvanceRollout(ctx, tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
	_, err := r.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: "svc", Flag: "missing", Stages: []*cpv1.RolloutStage{stage(100, 0)}})
	wantCode(t, err, codes.NotFound)
}

// TestControllerAdvancesRolloutsForWatchers runs the rollout controller on a
// replica and moves the clock past each stage's duration.
func TestControllerAdvancesRolloutsForWatchers(t *testing.T) {
	clock := newFakeClock()
	sources := &sourceRecorder{}
	r := startReplica(t, memory.New(), replicaOptions{now: clock.Now, rollout: 5 * time.Millisecond, observer: sources})
	createNamespace(t, r, "svc")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)
	f, err := startRollout(stage(10, time.Hour), stage(50, 2*time.Hour), stage(100, 0))(ctxAs(t, "alice"), r)
	if err != nil {
		t.Fatal(err)
	}
	checkRollout(t, "start", snapshotFlag(t, recv(t, stream, propagationBudget), "f"), 10, 0, active)

	for _, step := range []struct {
		wait    time.Duration
		percent float64
		stage   int32
		state   cpv1.RolloutState
	}{
		{time.Hour, 50, 1, active},
		{2 * time.Hour, 100, 2, completed},
	} {
		clock.Advance(step.wait)
		start := time.Now()
		snap := recv(t, stream, propagationBudget)
		f = snapshotFlag(t, snap, "f")
		checkRollout(t, "auto-advance", f, step.percent, step.stage, step.state)
		if f.GetUpdatedBy() != rollout.ControllerActor || !f.GetRollout().GetStageStartedAt().AsTime().Equal(clock.Now()) {
			t.Fatalf("advanced by %q at %v; want %q at %v", f.GetUpdatedBy(), f.GetRollout().GetStageStartedAt().AsTime(), rollout.ControllerActor, clock.Now())
		}
		if elapsed := time.Since(start); elapsed > propagationBudget {
			t.Fatalf("auto-advance took %v to reach the watcher, budget %v", elapsed, propagationBudget)
		}
	}

	events, err := r.admin.ListAuditEvents(ctxAs(t, "alice"), &cpv1.ListAuditEventsRequest{Namespace: "svc", Actor: rollout.ControllerActor})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.GetEvents()) != 2 {
		t.Fatalf("controller audit events = %v, want 2", events.GetEvents())
	}
	for _, e := range events.GetEvents() {
		if e.GetAction() != model.ActionRolloutAdvance {
			t.Fatalf("controller audit action = %q, want %q", e.GetAction(), model.ActionRolloutAdvance)
		}
	}
	// User writes and controller advances are told apart.
	if w, ro := sources.count("svc", SourceWrite), sources.count("svc", SourceRollout); w != 2 || ro != 2 {
		t.Fatalf("changes received: %d from writes, %d from the rollout controller; want 2 and 2", w, ro)
	}
}

// TestRunControlsRolloutController checks that Run starts the rollout
// controller only when an interval is configured and stops it on return.
func TestRunControlsRolloutController(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Second} {
		synctest.Test(t, func(t *testing.T) {
			st := memory.New()
			sources := &sourceRecorder{}
			srv := New(Options{Store: st, Logger: discard, RolloutInterval: interval, Observer: sources})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error)
			go func() { done <- srv.Run(ctx) }()

			admin := srv.AdminServer()
			if _, err := admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: "svc"}); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			stages := []*cpv1.RolloutStage{stage(10, time.Minute), stage(50, time.Minute), stage(100, 0)}
			if _, err := admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: "svc", Flag: "f", Stages: stages}); err != nil {
				t.Fatal(err)
			}
			percent := func() float64 {
				t.Helper()
				synctest.Wait()
				f, err := st.GetFlag(ctx, "svc", "f")
				if err != nil {
					t.Fatal(err)
				}
				return f.RolloutPercent
			}

			time.Sleep(time.Minute + time.Second)
			want := 10.0
			if interval > 0 {
				want = 50
			}
			if got := percent(); got != want {
				t.Fatalf("interval %v: rollout at %v%% after its first stage elapsed, want %v%%", interval, got, want)
			}
			if got := sources.count("svc", SourceRollout); interval > 0 && got != 1 {
				t.Fatalf("changes received from the rollout controller = %d, want 1", got)
			}

			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Run = %v", err)
				}
			case <-time.After(time.Hour):
				t.Fatal("Run did not return after its context was canceled")
			}
			// Run has returned, so nothing advances any more.
			time.Sleep(time.Hour)
			if got := percent(); got != want {
				t.Fatalf("interval %v: rollout moved to %v%% after Run returned", interval, got)
			}
		})
	}
}
