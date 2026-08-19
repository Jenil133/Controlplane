package rollout

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

const testNS = "checkout"

var discard = slog.New(slog.DiscardHandler)

// fakeClock is a time source the test moves by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stoppedAt returns a clock that always reads t.
func stoppedAt(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

type change struct {
	namespace string
	revision  int64
}

// changeLog records OnChange calls.
type changeLog struct {
	mu   sync.Mutex
	list []change
}

func (l *changeLog) record(_ context.Context, namespace string, revision int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.list = append(l.list, change{namespace, revision})
}

func (l *changeLog) get() []change {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.list)
}

// faultyStore wraps a store to inject failures and concurrent writes.
type faultyStore struct {
	store.Store
	listErr   error
	extraRefs []model.RolloutRef          // listed after the real ones
	getErr    map[string]error            // GetFlag failures by flag key
	putErr    map[string]error            // PutFlag failures by flag key
	afterGet  func(namespace, key string) // runs after every successful GetFlag
	// listOutage and getOutage fail that many of the next ListActiveRollouts
	// and GetFlag calls with errOutage, like a database failover that heals
	// by itself. Counting down, rather than a switch the test flips, makes
	// the failures land on the next calls whenever a running controller
	// makes them.
	listOutage, getOutage atomic.Int32
}

var errOutage = errors.New("database failing over")

// failNext reports whether a call must fail, counting down the failures left
// in n.
func failNext(n *atomic.Int32) bool { return n.Add(-1) >= 0 }

func (s *faultyStore) ListActiveRollouts(ctx context.Context) ([]model.RolloutRef, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	if failNext(&s.listOutage) {
		return nil, errOutage
	}
	refs, err := s.Store.ListActiveRollouts(ctx)
	return append(refs, s.extraRefs...), err
}

func (s *faultyStore) GetFlag(ctx context.Context, namespace, key string) (model.Flag, error) {
	if err := s.getErr[key]; err != nil {
		return model.Flag{}, err
	}
	if failNext(&s.getOutage) {
		return model.Flag{}, errOutage
	}
	f, err := s.Store.GetFlag(ctx, namespace, key)
	if err == nil && s.afterGet != nil {
		s.afterGet(namespace, key)
	}
	return f, err
}

func (s *faultyStore) PutFlag(ctx context.Context, namespace string, f model.Flag, opts store.WriteOptions) (model.Flag, error) {
	if err := s.putErr[f.Key]; err != nil {
		return model.Flag{}, err
	}
	return s.Store.PutFlag(ctx, namespace, f, opts)
}

// barrier releases waiters in groups of n.
type barrier struct {
	n       int
	mu      sync.Mutex
	waiting int
	release chan struct{}
}

func newBarrier(n int) *barrier {
	return &barrier{n: n, release: make(chan struct{})}
}

func (b *barrier) wait() {
	b.mu.Lock()
	release := b.release
	if b.waiting++; b.waiting == b.n {
		close(release)
		b.waiting, b.release = 0, make(chan struct{})
	}
	b.mu.Unlock()
	<-release
}

// seed writes flag key with a rollout of stages started at start, creating
// the namespace if needed.
func seed(t *testing.T, st store.Store, namespace, key string, stages []model.RolloutStage, start time.Time) model.Flag {
	t.Helper()
	ctx := t.Context()
	if _, err := st.CreateNamespace(ctx, model.Namespace{Name: namespace}, store.WriteOptions{Actor: "alice"}); err != nil && !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("CreateNamespace: %v", err)
	}
	f := model.Flag{Key: key, Enabled: true, RolloutPercent: 100}
	f.Normalize()
	f, err := Start(f, stages, "alice", start)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	out, err := st.PutFlag(ctx, namespace, f, store.WriteOptions{Actor: "alice", Action: model.ActionRolloutStart})
	if err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	return out
}

// update applies a transition to a stored flag and writes it back, as a user
// acting through the API would.
func update(t *testing.T, st store.Store, namespace, key string, fn func(model.Flag) (model.Flag, error)) {
	t.Helper()
	f := getFlag(t, st, namespace, key)
	next, err := fn(f)
	if err != nil {
		t.Fatalf("transition of %s/%s: %v", namespace, key, err)
	}
	if _, err := st.PutFlag(t.Context(), namespace, next, store.WriteOptions{Actor: "alice", ExpectedRevision: f.Revision}); err != nil {
		t.Fatalf("PutFlag %s/%s: %v", namespace, key, err)
	}
}

func getFlag(t *testing.T, st store.Store, namespace, key string) model.Flag {
	t.Helper()
	f, err := st.GetFlag(t.Context(), namespace, key)
	if err != nil {
		t.Fatalf("GetFlag %s/%s: %v", namespace, key, err)
	}
	return f
}

func nsRevision(t *testing.T, st store.Store, namespace string) int64 {
	t.Helper()
	ns, err := st.GetNamespace(t.Context(), namespace)
	if err != nil {
		t.Fatalf("GetNamespace %s: %v", namespace, err)
	}
	return ns.Revision
}

// wantStage fails unless the stored flag's rollout is at stage in state.
func wantStage(t *testing.T, st store.Store, namespace, key string, stage int, state model.RolloutState) model.Flag {
	t.Helper()
	f := getFlag(t, st, namespace, key)
	if p := f.Rollout; p.CurrentStage != stage || p.State != state {
		t.Fatalf("%s/%s at stage %d %s, want stage %d %s", namespace, key, p.CurrentStage, p.State, stage, state)
	}
	return f
}

// controllerEvents returns the audit events the controller wrote in namespace.
func controllerEvents(t *testing.T, st store.Store, namespace string) []model.AuditEvent {
	t.Helper()
	evs, err := st.ListAuditEvents(t.Context(), model.AuditFilter{Namespace: namespace, Actor: ControllerActor, Limit: 500})
	if err != nil {
		t.Fatalf("ListAuditEvents: %v", err)
	}
	return evs
}

// tickAll runs Tick on every controller at once and returns the total number
// of advances.
func tickAll(t *testing.T, ctrls []*Controller) int {
	t.Helper()
	ctx := t.Context()
	var (
		wg    sync.WaitGroup
		total atomic.Int64
		errs  = make([]error, len(ctrls))
	)
	for i, c := range ctrls {
		wg.Go(func() {
			n, err := c.Tick(ctx)
			total.Add(int64(n))
			errs[i] = err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	return int(total.Load())
}

func textLogger(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level}))
}

func TestTickAdvancesDueStage(t *testing.T) {
	st := memory.New()
	before := seed(t, st, testNS, "new-cart", testStages, t0)
	clk := &fakeClock{now: t0.Add(testStages[0].Duration - time.Nanosecond)}
	var changes changeLog
	c := NewController(ControllerConfig{Store: st, Now: clk.Now, Logger: discard, OnChange: changes.record})

	if n, err := c.Tick(t.Context()); n != 0 || err != nil {
		t.Fatalf("Tick before the stage ends = %d, %v; want 0, nil", n, err)
	}
	clk.Advance(time.Nanosecond)
	if n, err := c.Tick(t.Context()); n != 1 || err != nil {
		t.Fatalf("Tick when the stage ends = %d, %v; want 1, nil", n, err)
	}

	f := wantStage(t, st, testNS, "new-cart", 1, active)
	if f.RolloutPercent != testStages[1].Percent || !f.Rollout.StageStartedAt.Equal(clk.Now()) {
		t.Fatalf("flag at %v%% since %v, want %v%% since %v", f.RolloutPercent, f.Rollout.StageStartedAt, testStages[1].Percent, clk.Now())
	}
	if f.Revision != before.Revision+1 || f.UpdatedBy != ControllerActor {
		t.Fatalf("flag at revision %d by %q, want %d by %q", f.Revision, f.UpdatedBy, before.Revision+1, ControllerActor)
	}
	if got, want := changes.get(), []change{{testNS, f.Revision}}; !slices.Equal(got, want) {
		t.Fatalf("OnChange calls = %v, want %v", got, want)
	}

	const msg = "advance flag new-cart to stage 2/4 (5%)"
	evs := controllerEvents(t, st, testNS)
	if len(evs) != 1 {
		t.Fatalf("controller wrote %d audit events, want 1", len(evs))
	}
	if e := evs[0]; e.Action != model.ActionRolloutAdvance || e.EntityKey != "new-cart" || e.Revision != f.Revision || e.Message != msg {
		t.Fatalf("audit event: %s %s at revision %d, %q; want %s new-cart at revision %d, %q",
			e.Action, e.EntityKey, e.Revision, e.Message, model.ActionRolloutAdvance, f.Revision, msg)
	}
	revs, err := st.ListRevisions(t.Context(), testNS, 0, 1)
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(revs) != 1 || revs[0].Revision != f.Revision || revs[0].Actor != ControllerActor || revs[0].Summary != msg {
		t.Fatalf("latest revisions = %+v, want revision %d by %s summarized %q", revs, f.Revision, ControllerActor, msg)
	}
}

func TestTickMovesOneStagePerTick(t *testing.T) {
	st := memory.New()
	stages := []model.RolloutStage{{Percent: 1, Duration: time.Hour}, {Percent: 10, Duration: time.Hour}, {Percent: 100}}
	seed(t, st, testNS, "new-cart", stages, t0)
	// Long overdue, as after every replica was down for a day.
	clk := &fakeClock{now: t0.Add(24 * time.Hour)}
	c := NewController(ControllerConfig{Store: st, Now: clk.Now, Logger: discard})

	steps := []struct {
		name      string
		wait      time.Duration
		want      int
		wantStage int
		wantState model.RolloutState
	}{
		{"one stage although a day has passed", 0, 1, 1, active},
		{"the new stage started at the advance", 0, 0, 1, active},
		{"just before the new stage ends", time.Hour - time.Nanosecond, 0, 1, active},
		{"entering the final stage completes", time.Nanosecond, 1, 2, completed},
		{"completed rollouts stay put", 24 * time.Hour, 0, 2, completed},
	}
	for _, s := range steps {
		clk.Advance(s.wait)
		if n, err := c.Tick(t.Context()); n != s.want || err != nil {
			t.Fatalf("%s: Tick = %d, %v; want %d, nil", s.name, n, err, s.want)
		}
		wantStage(t, st, testNS, "new-cart", s.wantStage, s.wantState)
	}
}

func TestTickLeavesOtherFlagsAlone(t *testing.T) {
	st := memory.New()
	seed(t, st, testNS, "due", testStages, t0)
	seed(t, st, testNS, "manual", []model.RolloutStage{{Percent: 5}, {Percent: 100}}, t0)
	seed(t, st, testNS, "paused", testStages, t0)
	update(t, st, testNS, "paused", func(f model.Flag) (model.Flag, error) { return Pause(f, t0) })
	seed(t, st, testNS, "aborted", testStages, t0)
	update(t, st, testNS, "aborted", func(f model.Flag) (model.Flag, error) { return Abort(f, t0) })
	seed(t, st, testNS, "completed", testStages[3:], t0)
	plain := model.Flag{Key: "plain", Enabled: true, RolloutPercent: 50}
	plain.Normalize()
	if _, err := st.PutFlag(t.Context(), testNS, plain, store.WriteOptions{Actor: "alice"}); err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	before, err := st.Snapshot(t.Context(), testNS)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	c := NewController(ControllerConfig{Store: st, Now: stoppedAt(t0.Add(1000 * time.Hour)), Logger: discard})
	if n, err := c.Tick(t.Context()); n != 1 || err != nil {
		t.Fatalf("Tick = %d, %v; want 1 (the due rollout), nil", n, err)
	}
	after, err := st.Snapshot(t.Context(), testNS)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	changes, err := model.Diff(before, after)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(changes) != 1 || changes[0].Key != "due" {
		var keys []string
		for _, ch := range changes {
			keys = append(keys, ch.Key)
		}
		t.Fatalf("Tick changed %v, want only [due]", keys)
	}
}

func TestTickSkipsVanishedFlags(t *testing.T) {
	mem := memory.New()
	seed(t, mem, testNS, "new-cart", testStages, t0)
	st := &faultyStore{Store: mem, extraRefs: []model.RolloutRef{
		{Namespace: testNS, Key: "deleted-flag"},
		{Namespace: "deleted-namespace", Key: "new-cart"},
	}}
	var logs bytes.Buffer
	c := NewController(ControllerConfig{Store: st, Now: stoppedAt(t0.Add(time.Hour)), Logger: textLogger(&logs, slog.LevelWarn)})
	if n, err := c.Tick(t.Context()); n != 1 || err != nil {
		t.Fatalf("Tick = %d, %v; want 1, nil", n, err)
	}
	if logs.Len() != 0 {
		t.Fatalf("vanished flags were logged:\n%s", &logs)
	}
}

// TestTickSkipsFlagsChangedAfterRead changes the flag between the
// controller's read and its compare-and-swap write.
func TestTickSkipsFlagsChangedAfterRead(t *testing.T) {
	now := t0.Add(time.Hour)
	tests := []struct {
		name      string
		interfere func(t *testing.T, st store.Store)
	}{
		{"paused by a user", func(t *testing.T, st store.Store) {
			update(t, st, testNS, "new-cart", func(f model.Flag) (model.Flag, error) { return Pause(f, now) })
		}},
		{"deleted by a user", func(t *testing.T, st store.Store) {
			if _, err := st.DeleteFlag(t.Context(), testNS, "new-cart", store.WriteOptions{Actor: "alice"}); err != nil {
				t.Fatalf("DeleteFlag: %v", err)
			}
		}},
		{"advanced by another replica", func(t *testing.T, st store.Store) {
			other := NewController(ControllerConfig{Store: st, Now: stoppedAt(now), Logger: discard})
			if n, err := other.Tick(t.Context()); n != 1 || err != nil {
				t.Fatalf("other replica's Tick = %d, %v; want 1, nil", n, err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.New()
			seed(t, mem, testNS, "new-cart", testStages, t0)
			var wantRev int64
			st := &faultyStore{Store: mem, afterGet: func(string, string) {
				tt.interfere(t, mem)
				wantRev = nsRevision(t, mem, testNS)
			}}
			var (
				logs    bytes.Buffer
				changes changeLog
			)
			c := NewController(ControllerConfig{Store: st, Now: stoppedAt(now), Logger: textLogger(&logs, slog.LevelDebug), OnChange: changes.record})

			if n, err := c.Tick(t.Context()); n != 0 || err != nil {
				t.Fatalf("Tick = %d, %v; want 0, nil", n, err)
			}
			if got := nsRevision(t, mem, testNS); got != wantRev {
				t.Fatalf("namespace at revision %d, want %d: the controller wrote over a newer change", got, wantRev)
			}
			if got := changes.get(); len(got) != 0 {
				t.Fatalf("OnChange calls = %v, want none", got)
			}
			if logs.Len() != 0 {
				t.Fatalf("a lost race was logged:\n%s", &logs)
			}
		})
	}
}

func TestTickReturnsFirstErrorAfterProcessingAll(t *testing.T) {
	mem := memory.New()
	for _, key := range []string{"f1", "f2", "f3", "f4", "f5"} {
		seed(t, mem, testNS, key, testStages, t0)
	}
	// The store trusts its callers, so a malformed flag can sit in it; the
	// controller must refuse to write it back rather than make it worse.
	malformed := getFlag(t, mem, testNS, "f5")
	malformed.Salt = ""
	if _, err := mem.PutFlag(t.Context(), testNS, malformed, store.WriteOptions{Actor: "alice"}); err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	errGet, errPut := errors.New("get exploded"), errors.New("put exploded")
	st := &faultyStore{Store: mem, getErr: map[string]error{"f1": errGet}, putErr: map[string]error{"f3": errPut}}
	var (
		logs    bytes.Buffer
		changes changeLog
	)
	c := NewController(ControllerConfig{Store: st, Now: stoppedAt(t0.Add(time.Hour)), Logger: textLogger(&logs, slog.LevelWarn), OnChange: changes.record})

	n, err := c.Tick(t.Context())
	if n != 2 {
		t.Fatalf("Tick advanced %d flags, want the 2 healthy ones", n)
	}
	if !errors.Is(err, errGet) || errors.Is(err, errPut) || !strings.Contains(err.Error(), `"f1"`) {
		t.Fatalf("Tick error = %v, want only the first failure (%v) naming flag f1", err, errGet)
	}
	for key, stage := range map[string]int{"f1": 0, "f2": 1, "f3": 0, "f4": 1, "f5": 0} {
		wantStage(t, mem, testNS, key, stage, active)
	}
	if got := len(changes.get()); got != 2 {
		t.Fatalf("OnChange called %d times, want 2", got)
	}
	for _, want := range []string{"flag=f1", errGet.Error(), "flag=f3", errPut.Error(), "flag=f5", "salt"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, &logs)
		}
	}
}

func TestTickListFailure(t *testing.T) {
	errList := errors.New("database unreachable")
	var logs bytes.Buffer
	c := NewController(ControllerConfig{
		Store:  &faultyStore{Store: memory.New(), listErr: errList},
		Logger: textLogger(&logs, slog.LevelWarn),
	})
	if n, err := c.Tick(t.Context()); n != 0 || !errors.Is(err, errList) {
		t.Fatalf("Tick = %d, %v; want 0, %v", n, err, errList)
	}
	if !strings.Contains(logs.String(), errList.Error()) {
		t.Fatalf("log lacks the failure:\n%s", &logs)
	}
}

func TestTickStopsQuietlyWhenContextEnds(t *testing.T) {
	mem := memory.New()
	seed(t, mem, testNS, "a", testStages, t0)
	seed(t, mem, testNS, "b", testStages, t0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var logs bytes.Buffer
	c := NewController(ControllerConfig{Store: mem, Now: stoppedAt(t0.Add(time.Hour)), Logger: textLogger(&logs, slog.LevelWarn)})
	if n, err := c.Tick(ctx); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick = %d, %v; want 0, context.Canceled", n, err)
	}
	wantStage(t, mem, testNS, "a", 0, active)
	wantStage(t, mem, testNS, "b", 0, active)

	// A store call cut short by the cancellation is not worth a warning.
	c = NewController(ControllerConfig{Store: &faultyStore{Store: mem, listErr: context.Canceled}, Logger: textLogger(&logs, slog.LevelWarn)})
	if _, err := c.Tick(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick error = %v, want context.Canceled", err)
	}
	if logs.Len() != 0 {
		t.Fatalf("shutdown was logged:\n%s", &logs)
	}
}

// TestTwoControllersRaceForEveryStage ticks two controllers on one store at
// the same instant for every stage. Each reads the flag before the other
// writes, so both try to advance the same revision, and the compare-and-swap
// must let exactly one of them win.
func TestTwoControllersRaceForEveryStage(t *testing.T) {
	// In a bubble a stuck barrier fails the test as a deadlock instead of
	// hanging it.
	synctest.Test(t, func(t *testing.T) {
		mem := memory.New()
		stages := []model.RolloutStage{
			{Percent: 1, Duration: time.Hour},
			{Percent: 10, Duration: time.Hour},
			{Percent: 50, Duration: time.Hour},
			{Percent: 100},
		}
		seed(t, mem, testNS, "new-cart", stages, t0)
		gate := newBarrier(2)
		st := &faultyStore{Store: mem, afterGet: func(string, string) { gate.wait() }}
		clk := &fakeClock{now: t0}
		var changes changeLog
		ctrls := make([]*Controller, 2)
		for i := range ctrls {
			ctrls[i] = NewController(ControllerConfig{Store: st, Now: clk.Now, Logger: discard, OnChange: changes.record})
		}

		for stage := 1; stage < len(stages); stage++ {
			clk.Advance(time.Hour)
			if n := tickAll(t, ctrls); n != 1 {
				t.Fatalf("stage %d: %d advances, want exactly 1", stage, n)
			}
			state := active
			if stage == len(stages)-1 {
				state = completed
			}
			f := wantStage(t, mem, testNS, "new-cart", stage, state)
			if got := changes.get(); len(got) != stage || got[stage-1] != (change{testNS, f.Revision}) {
				t.Fatalf("stage %d: OnChange calls %v, want one per stage, the last at revision %d", stage, got, f.Revision)
			}
		}
		if evs := controllerEvents(t, mem, testNS); len(evs) != len(stages)-1 {
			t.Fatalf("controllers wrote %d audit events, want %d", len(evs), len(stages)-1)
		}
	})
}

func TestConcurrentControllersAdvanceEachFlagOncePerStage(t *testing.T) {
	mem := memory.New()
	stages := []model.RolloutStage{
		{Percent: 1, Duration: 10 * time.Minute},
		{Percent: 10, Duration: 10 * time.Minute},
		{Percent: 50, Duration: 10 * time.Minute},
		{Percent: 100},
	}
	namespaces, keys := []string{"checkout", "search"}, []string{"a", "b", "c", "d"}
	for _, ns := range namespaces {
		for _, key := range keys {
			seed(t, mem, ns, key, stages, t0)
		}
	}
	flags := len(namespaces) * len(keys)
	clk := &fakeClock{now: t0}
	var changes changeLog
	ctrls := make([]*Controller, 3)
	for i := range ctrls {
		ctrls[i] = NewController(ControllerConfig{Store: mem, Now: clk.Now, Logger: discard, OnChange: changes.record})
	}

	for stage := 1; stage < len(stages); stage++ {
		clk.Advance(10 * time.Minute)
		if n := tickAll(t, ctrls); n != flags {
			t.Fatalf("stage %d: %d advances, want one per flag (%d)", stage, n, flags)
		}
	}
	if got, want := len(changes.get()), flags*(len(stages)-1); got != want {
		t.Fatalf("OnChange called %d times, want %d", got, want)
	}
	for _, ns := range namespaces {
		for _, key := range keys {
			wantStage(t, mem, ns, key, len(stages)-1, completed)
		}
		if got, want := len(controllerEvents(t, mem, ns)), len(keys)*(len(stages)-1); got != want {
			t.Fatalf("%s: controllers wrote %d audit events, want %d", ns, got, want)
		}
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		every    time.Duration // how often Run should tick
	}{
		{"configured interval", time.Second, time.Second},
		{"documented default interval", 0, 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mem := memory.New()
				// The bubble's fake clock, which the default Now reads too.
				start := time.Now()
				// Stages shorter than the interval advance exactly on a tick,
				// which pins down when Run ticks.
				stages := []model.RolloutStage{
					{Percent: 10, Duration: tt.every / 2},
					{Percent: 50, Duration: tt.every / 2},
					{Percent: 100},
				}
				seed(t, mem, testNS, "new-cart", stages, start)
				var changes changeLog
				c := NewController(ControllerConfig{Store: mem, Interval: tt.interval, Logger: discard, OnChange: changes.record})
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- c.Run(ctx) }()

				steps := []struct {
					at    time.Duration
					stage int
					state model.RolloutState
					since time.Duration // when the current stage started
				}{
					{tt.every - time.Nanosecond, 0, active, 0},
					{tt.every, 1, active, tt.every},
					{2*tt.every - time.Nanosecond, 1, active, tt.every},
					{2 * tt.every, 2, completed, 2 * tt.every},
				}
				for _, s := range steps {
					time.Sleep(time.Until(start.Add(s.at)))
					synctest.Wait()
					f := wantStage(t, mem, testNS, "new-cart", s.stage, s.state)
					if want := start.Add(s.since); !f.Rollout.StageStartedAt.Equal(want) {
						t.Fatalf("at %v: stage started at %v, want %v", s.at, f.Rollout.StageStartedAt, want)
					}
				}
				if got := len(changes.get()); got != 2 {
					t.Fatalf("OnChange called %d times, want 2", got)
				}

				cancel()
				if err := <-done; err != nil {
					t.Fatalf("Run = %v after cancel, want nil", err)
				}
			})
		})
	}
}

// TestRunKeepsTickingAfterFailedTicks: a store outage, such as a database
// failover, fails the ticks of every replica at once, so a Run that gave up
// on a failed tick would stop timed rollouts everywhere until a restart.
func TestRunKeepsTickingAfterFailedTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := memory.New()
		start := time.Now()
		const interval = time.Second
		// Due by the first tick, so only a failed tick keeps the flag at its
		// first stage.
		stages := []model.RolloutStage{{Percent: 10, Duration: interval / 2}, {Percent: 100}}
		seed(t, mem, testNS, "new-cart", stages, start)
		// The first tick fails to list the rollouts. The second lists them but
		// fails to read the flag, Tick's other way of failing.
		st := &faultyStore{Store: mem}
		st.listOutage.Store(1)
		st.getOutage.Store(1)
		c := NewController(ControllerConfig{Store: st, Interval: interval, Logger: discard})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		steps := []struct {
			at    time.Duration
			stage int
			state model.RolloutState
		}{
			{interval, 0, active},        // listing failed
			{2 * interval, 0, active},    // reading the flag failed
			{3 * interval, 1, completed}, // the store is back
		}
		for _, s := range steps {
			time.Sleep(time.Until(start.Add(s.at)))
			synctest.Wait()
			select {
			case err := <-done:
				t.Fatalf("Run returned %v by %v, want it to keep ticking until cancelled", err, s.at)
			default:
			}
			wantStage(t, mem, testNS, "new-cart", s.stage, s.state)
		}

		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v after cancel, want nil", err)
		}
	})
}
