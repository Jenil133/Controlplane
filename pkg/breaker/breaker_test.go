package breaker

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// testSettings open the breaker at 50% failures once a 10s window (1s slots)
// holds 4 outcomes, keep it open for 5s and close it after 2 good trials.
var testSettings = Settings{
	FailureRateThreshold: 0.5,
	MinRequests:          4,
	Window:               10 * time.Second,
	OpenDuration:         5 * time.Second,
	HalfOpenMaxRequests:  2,
}

// tweak returns testSettings changed by edit.
func tweak(edit func(*Settings)) Settings {
	s := testSettings
	edit(&s)
	return s
}

// clock is a fake time source that tests move by hand.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock {
	return &clock{now: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (tr transition) String() string { return tr.from.String() + " -> " + tr.to.String() }

// changes records state changes in the order the breaker reports them.
type changes struct {
	mu   sync.Mutex
	list []transition
}

func (c *changes) record(from, to State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = append(c.list, transition{from, to})
}

// take returns the changes recorded since the previous take.
func (c *changes) take() []transition {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.list
	c.list = nil
	return out
}

func newBreaker(t *testing.T, s Settings) (*Breaker, *clock, *changes) {
	t.Helper()
	clk, ch := newClock(), &changes{}
	return New(s, WithClock(clk.Now), WithStateChange(ch.record)), clk, ch
}

// admit makes Allow admit a call, failing the test if it is rejected.
func admit(t *testing.T, b *Breaker) func(bool) {
	t.Helper()
	done, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow() = %v, want the call admitted", err)
	}
	return done
}

// call makes one admitted call that reports the given outcome.
func call(t *testing.T, b *Breaker, success bool) {
	t.Helper()
	admit(t, b)(success)
}

// trip opens b, which must be closed with an empty window, by failing
// MinRequests calls.
func trip(t *testing.T, b *Breaker) {
	t.Helper()
	for range b.Settings().MinRequests {
		call(t, b, false)
	}
	wantState(t, b, Open)
}

// halfOpen trips b and waits out its OpenDuration.
func halfOpen(t *testing.T, b *Breaker, clk *clock) {
	t.Helper()
	trip(t, b)
	clk.Advance(b.Settings().OpenDuration)
	wantState(t, b, HalfOpen)
}

func wantState(t *testing.T, b *Breaker, want State) {
	t.Helper()
	if got := b.State(); got != want {
		t.Fatalf("State() = %v, want %v", got, want)
	}
}

// wantRejected checks that Allow rejects a call with want and that the done
// func it returns is a harmless no-op.
func wantRejected(t *testing.T, b *Breaker, want error) {
	t.Helper()
	before := b.Counts()
	done, err := b.Allow()
	if !errors.Is(err, want) {
		t.Fatalf("Allow() = %v, want %v", err, want)
	}
	done(false)
	if got := b.Counts(); got != before {
		t.Fatalf("done of a rejected call changed Counts from %+v to %+v", before, got)
	}
}

func wantCounts(t *testing.T, b *Breaker, want Counts) {
	t.Helper()
	if got := b.Counts(); got != want {
		t.Fatalf("Counts() = %+v, want %+v", got, want)
	}
}

func wantChanges(t *testing.T, ch *changes, want ...transition) {
	t.Helper()
	if got := ch.take(); !slices.Equal(got, want) {
		t.Fatalf("state changes = %v, want %v", got, want)
	}
}

// wantPanic runs fn and checks that it panics with want.
func wantPanic(t *testing.T, want any, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != want {
			t.Fatalf("recovered %v, want a panic with %v", r, want)
		}
	}()
	fn()
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{Closed, "closed"},
		{Open, "open"},
		{HalfOpen, "half-open"},
		{State(7), "State(7)"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", int(tt.state), got, tt.want)
		}
	}
}

func TestInvalidSettingsUseDefaults(t *testing.T) {
	custom := Settings{
		FailureRateThreshold: 0.25,
		MinRequests:          7,
		Window:               3 * time.Second,
		OpenDuration:         time.Minute,
		HalfOpenMaxRequests:  3,
	}
	edit := func(fn func(*Settings)) Settings {
		s := custom
		fn(&s)
		return s
	}
	defaultThreshold := edit(func(s *Settings) { s.FailureRateThreshold = DefaultFailureRateThreshold })
	defaultWindow := edit(func(s *Settings) { s.Window = DefaultWindow })
	defaultOpen := edit(func(s *Settings) { s.OpenDuration = DefaultOpenDuration })

	tests := []struct {
		name     string
		in, want Settings
	}{
		{"zero value", Settings{}, Settings{
			FailureRateThreshold: DefaultFailureRateThreshold,
			MinRequests:          DefaultMinRequests,
			Window:               DefaultWindow,
			OpenDuration:         DefaultOpenDuration,
			HalfOpenMaxRequests:  DefaultHalfOpenMaxRequests,
		}},
		{"valid", custom, custom},
		{"threshold NaN", edit(func(s *Settings) { s.FailureRateThreshold = math.NaN() }), defaultThreshold},
		{"threshold zero", edit(func(s *Settings) { s.FailureRateThreshold = 0 }), defaultThreshold},
		{"threshold negative", edit(func(s *Settings) { s.FailureRateThreshold = -0.5 }), defaultThreshold},
		{"threshold above 1", edit(func(s *Settings) { s.FailureRateThreshold = 1.01 }), defaultThreshold},
		{"threshold infinite", edit(func(s *Settings) { s.FailureRateThreshold = math.Inf(1) }), defaultThreshold},
		{"threshold 1", edit(func(s *Settings) { s.FailureRateThreshold = 1 }), edit(func(s *Settings) { s.FailureRateThreshold = 1 })},
		{"min requests zero", edit(func(s *Settings) { s.MinRequests = 0 }), edit(func(s *Settings) { s.MinRequests = DefaultMinRequests })},
		{"min requests 1", edit(func(s *Settings) { s.MinRequests = 1 }), edit(func(s *Settings) { s.MinRequests = 1 })},
		{"window zero", edit(func(s *Settings) { s.Window = 0 }), defaultWindow},
		{"window negative", edit(func(s *Settings) { s.Window = -time.Second }), defaultWindow},
		{"window too short for 10 slots", edit(func(s *Settings) { s.Window = 9 }), defaultWindow},
		{"window of 10 one-nanosecond slots", edit(func(s *Settings) { s.Window = 10 }), edit(func(s *Settings) { s.Window = 10 })},
		{"open duration zero", edit(func(s *Settings) { s.OpenDuration = 0 }), defaultOpen},
		{"open duration negative", edit(func(s *Settings) { s.OpenDuration = -time.Nanosecond }), defaultOpen},
		{"half-open trials zero", edit(func(s *Settings) { s.HalfOpenMaxRequests = 0 }), edit(func(s *Settings) { s.HalfOpenMaxRequests = DefaultHalfOpenMaxRequests })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.in).Settings(); got != tt.want {
				t.Errorf("New: Settings() = %+v, want %+v", got, tt.want)
			}
			b := New(testSettings)
			b.Update(tt.in)
			if got := b.Settings(); got != tt.want {
				t.Errorf("Update: Settings() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClosedOpensAfterFailureAtThreshold(t *testing.T) {
	const ok, fail = true, false
	tests := []struct {
		name      string
		threshold float64
		outcomes  []bool
		want      State
	}{
		{"below MinRequests", 0.5, []bool{fail, fail, fail}, Closed},
		{"MinRequests failures", 0.5, []bool{fail, fail, fail, fail}, Open},
		{"rate equal to threshold", 0.5, []bool{ok, ok, fail, fail}, Open},
		// 0.3*10 is 3.0000000000000004 in floating point; 3/10 is exactly 0.3.
		{"rate equal to a decimal threshold", 0.3, []bool{ok, ok, ok, ok, ok, ok, ok, fail, fail, fail}, Open},
		{"rate below threshold", 0.5, []bool{ok, ok, ok, fail}, Closed},
		{"two of five below threshold", 0.5, []bool{ok, ok, ok, fail, fail}, Closed},
		{"successes only", 0.5, []bool{ok, ok, ok, ok, ok, ok}, Closed},
		// Only a failure evaluates the rate, so a success that brings the
		// window to MinRequests does not open the breaker.
		{"success reaching MinRequests", 0.5, []bool{fail, fail, fail, ok}, Closed},
		{"next failure evaluates", 0.5, []bool{fail, fail, fail, ok, fail}, Open},
		{"threshold 1 needs all failed", 1, []bool{fail, fail, fail, ok, fail}, Closed},
		{"threshold 1 all failed", 1, []bool{fail, fail, fail, fail}, Open},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, _, ch := newBreaker(t, tweak(func(s *Settings) { s.FailureRateThreshold = tt.threshold }))
			for _, success := range tt.outcomes {
				call(t, b, success)
			}
			wantState(t, b, tt.want)
			if tt.want == Open {
				wantChanges(t, ch, transition{Closed, Open})
			} else {
				wantChanges(t, ch)
			}
		})
	}
}

func TestOpenRejectsUntilOpenDuration(t *testing.T) {
	b, clk, ch := newBreaker(t, testSettings)
	trip(t, b)
	wantChanges(t, ch, transition{Closed, Open})
	wantRejected(t, b, ErrOpen)

	clk.Advance(testSettings.OpenDuration - time.Nanosecond)
	wantRejected(t, b, ErrOpen)
	wantChanges(t, ch)

	clk.Advance(time.Nanosecond)
	admit(t, b)
	wantChanges(t, ch, transition{Open, HalfOpen})
}

func TestStateAppliesLazyHalfOpen(t *testing.T) {
	b, clk, ch := newBreaker(t, testSettings)
	trip(t, b)
	clk.Advance(time.Hour)
	ch.take()
	wantState(t, b, HalfOpen)
	wantChanges(t, ch, transition{Open, HalfOpen})
}

func TestHalfOpenLimitsTrialsAndCloses(t *testing.T) {
	b, clk, ch := newBreaker(t, testSettings)
	halfOpen(t, b, clk)
	first, second := admit(t, b), admit(t, b)
	wantRejected(t, b, ErrTooManyRequests)
	wantCounts(t, b, Counts{}) // trials in flight have no outcome yet

	first(true)
	wantState(t, b, HalfOpen)
	wantCounts(t, b, Counts{Requests: 1})
	// Completed trials still use up the limit.
	wantRejected(t, b, ErrTooManyRequests)

	second(true)
	wantState(t, b, Closed)
	wantCounts(t, b, Counts{})
	wantChanges(t, ch, transition{Closed, Open}, transition{Open, HalfOpen}, transition{HalfOpen, Closed})

	// The fresh window needs MinRequests outcomes again before it opens.
	for range testSettings.MinRequests - 1 {
		call(t, b, false)
	}
	wantState(t, b, Closed)
	call(t, b, false)
	wantState(t, b, Open)
}

func TestHalfOpenFailureReopensAndRestartsTimer(t *testing.T) {
	b, clk, ch := newBreaker(t, testSettings)
	halfOpen(t, b, clk)
	first, second := admit(t, b), admit(t, b)
	first(true)
	clk.Advance(time.Second)
	second(false)
	wantState(t, b, Open)
	wantCounts(t, b, Counts{})
	wantChanges(t, ch, transition{Closed, Open}, transition{Open, HalfOpen}, transition{HalfOpen, Open})

	clk.Advance(testSettings.OpenDuration - time.Nanosecond)
	wantRejected(t, b, ErrOpen)
	clk.Advance(time.Nanosecond)
	wantState(t, b, HalfOpen)
	// The new half-open period has its own trial budget.
	admit(t, b)
	admit(t, b)
	wantRejected(t, b, ErrTooManyRequests)
}

func TestCountsFollowState(t *testing.T) {
	b, clk, _ := newBreaker(t, testSettings)
	call(t, b, true)
	call(t, b, false)
	wantCounts(t, b, Counts{Requests: 2, Failures: 1})
	call(t, b, false)
	call(t, b, false)
	wantState(t, b, Open)
	wantCounts(t, b, Counts{})

	clk.Advance(testSettings.OpenDuration)
	done := admit(t, b)
	wantCounts(t, b, Counts{})
	done(true)
	wantCounts(t, b, Counts{Requests: 1})
}

func TestWindowExpiry(t *testing.T) {
	b, clk, _ := newBreaker(t, testSettings)
	call(t, b, false)
	call(t, b, false)
	clk.Advance(5 * time.Second)
	call(t, b, true)

	clk.Advance(5*time.Second - time.Nanosecond)
	wantCounts(t, b, Counts{Requests: 3, Failures: 2})
	clk.Advance(time.Nanosecond) // the slot with the first two failures expires
	wantCounts(t, b, Counts{Requests: 1})

	// Expired failures no longer push the breaker open.
	call(t, b, false)
	call(t, b, false)
	wantState(t, b, Closed)

	clk.Advance(time.Hour)
	wantCounts(t, b, Counts{})
	for range testSettings.MinRequests - 1 {
		call(t, b, false)
	}
	wantState(t, b, Closed)
}

func TestDoneCountsOnce(t *testing.T) {
	b, _, _ := newBreaker(t, testSettings)
	done := admit(t, b)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { done(false) })
	}
	wg.Wait()
	done(true)
	wantCounts(t, b, Counts{Requests: 1, Failures: 1})
}

// TestStaleOutcomesAreIgnored checks that a call only reports to the
// generation that admitted it.
func TestStaleOutcomesAreIgnored(t *testing.T) {
	oneTrial := tweak(func(s *Settings) { s.HalfOpenMaxRequests = 1 })

	t.Run("failure while open does not restart the timer", func(t *testing.T) {
		b, clk, _ := newBreaker(t, testSettings)
		slow := admit(t, b)
		trip(t, b)
		clk.Advance(2 * time.Second)
		slow(false)
		clk.Advance(testSettings.OpenDuration - 2*time.Second)
		wantState(t, b, HalfOpen)
	})

	t.Run("success from before opening is no trial success", func(t *testing.T) {
		b, clk, _ := newBreaker(t, oneTrial)
		slow := admit(t, b)
		halfOpen(t, b, clk)
		slow(true)
		wantState(t, b, HalfOpen)
		wantCounts(t, b, Counts{})
		call(t, b, true)
		wantState(t, b, Closed)
	})

	t.Run("failure from before opening does not reopen", func(t *testing.T) {
		b, clk, ch := newBreaker(t, testSettings)
		slow := admit(t, b)
		halfOpen(t, b, clk)
		ch.take()
		slow(false)
		wantState(t, b, HalfOpen)
		wantChanges(t, ch)
	})

	t.Run("trial from an earlier half-open period", func(t *testing.T) {
		b, clk, _ := newBreaker(t, testSettings)
		halfOpen(t, b, clk)
		slow, failing := admit(t, b), admit(t, b)
		failing(false)
		clk.Advance(testSettings.OpenDuration)
		wantState(t, b, HalfOpen)
		slow(true)
		wantCounts(t, b, Counts{})
		// Both trial slots of the new period are still free.
		admit(t, b)
		admit(t, b)
		wantRejected(t, b, ErrTooManyRequests)
	})

	t.Run("call from before a full cycle back to closed", func(t *testing.T) {
		b, clk, _ := newBreaker(t, oneTrial)
		slow := admit(t, b)
		halfOpen(t, b, clk)
		call(t, b, true)
		wantState(t, b, Closed)
		// Same state as when the call was admitted, but a later generation.
		slow(false)
		wantCounts(t, b, Counts{})
	})
}

func TestDo(t *testing.T) {
	b, clk, _ := newBreaker(t, testSettings)
	if err := b.Do(func() error { return nil }); err != nil {
		t.Fatalf("Do(success) = %v", err)
	}
	if err := b.Do(func() error { return errBoom }); err != errBoom {
		t.Fatalf("Do(failure) = %v, want fn's error unchanged", err)
	}
	wantCounts(t, b, Counts{Requests: 2, Failures: 1})

	wantPanic(t, "kaboom", func() { _ = b.Do(func() error { panic("kaboom") }) })
	wantCounts(t, b, Counts{Requests: 3, Failures: 2})

	called := false
	fn := func() error { called = true; return nil }
	call(t, b, false)
	wantState(t, b, Open)
	if err := b.Do(fn); !errors.Is(err, ErrOpen) || called {
		t.Fatalf("Do while open = %v (fn called: %v), want ErrOpen without calling fn", err, called)
	}

	clk.Advance(testSettings.OpenDuration)
	admit(t, b)
	admit(t, b)
	if err := b.Do(fn); !errors.Is(err, ErrTooManyRequests) || called {
		t.Fatalf("Do over the trial limit = %v (fn called: %v), want ErrTooManyRequests without calling fn", err, called)
	}
}

func TestUpdateKeepsOpenTimer(t *testing.T) {
	longer := tweak(func(s *Settings) { s.OpenDuration = 10 * time.Second })

	t.Run("longer OpenDuration", func(t *testing.T) {
		b, clk, _ := newBreaker(t, testSettings)
		trip(t, b)
		clk.Advance(2 * time.Second)
		b.Update(longer)
		wantState(t, b, Open)
		clk.Advance(8*time.Second - time.Nanosecond)
		wantRejected(t, b, ErrOpen)
		clk.Advance(time.Nanosecond)
		wantState(t, b, HalfOpen)
	})
	t.Run("shorter OpenDuration already passed", func(t *testing.T) {
		b, clk, _ := newBreaker(t, testSettings)
		trip(t, b)
		clk.Advance(2 * time.Second)
		b.Update(tweak(func(s *Settings) { s.OpenDuration = time.Second }))
		wantState(t, b, HalfOpen)
	})

	// Once the old OpenDuration has passed the breaker is half-open, even if
	// no call has applied the lazy change yet, so a longer OpenDuration must
	// not send it back to open; reading the state first must not matter.
	tests := []struct {
		name      string
		readFirst bool
	}{
		{"longer OpenDuration after the old one passed", false},
		{"longer OpenDuration after the old one passed and was seen", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, clk, ch := newBreaker(t, testSettings)
			trip(t, b)
			clk.Advance(testSettings.OpenDuration + time.Second)
			if tt.readFirst {
				wantState(t, b, HalfOpen)
			}
			b.Update(longer)
			wantChanges(t, ch, transition{Closed, Open}, transition{Open, HalfOpen})
			wantState(t, b, HalfOpen)
			admit(t, b)
		})
	}
}

func TestUpdateWindowResetsCounts(t *testing.T) {
	b, _, _ := newBreaker(t, testSettings)
	call(t, b, false)
	call(t, b, true)

	b.Update(tweak(func(s *Settings) { s.FailureRateThreshold, s.MinRequests = 0.9, 6 }))
	wantCounts(t, b, Counts{Requests: 2, Failures: 1})

	b.Update(tweak(func(s *Settings) { s.Window = 20 * time.Second }))
	wantCounts(t, b, Counts{})
	wantState(t, b, Closed)
}

func TestUpdateWindowComparesEffectiveValue(t *testing.T) {
	b := New(tweak(func(s *Settings) { s.Window = 0 })) // DefaultWindow
	call(t, b, false)
	b.Update(tweak(func(s *Settings) { s.Window = DefaultWindow }))
	wantCounts(t, b, Counts{Requests: 1, Failures: 1})
}

func TestUpdateThresholdAppliesFromNextFailure(t *testing.T) {
	b, _, _ := newBreaker(t, testSettings)
	for _, success := range []bool{true, true, true, false} {
		call(t, b, success)
	}
	wantState(t, b, Closed) // 1 of 4 failed

	b.Update(tweak(func(s *Settings) { s.FailureRateThreshold = 0.2 }))
	wantState(t, b, Closed)
	call(t, b, true)
	wantState(t, b, Closed) // successes do not evaluate the rate
	call(t, b, false)
	wantState(t, b, Open) // 2 of 6 failed
}

func TestUpdateKeepsCallsInFlight(t *testing.T) {
	b, _, _ := newBreaker(t, testSettings)
	done := admit(t, b)
	b.Update(tweak(func(s *Settings) { s.Window = time.Minute }))
	done(false)
	wantCounts(t, b, Counts{Requests: 1, Failures: 1})
}

func TestUpdateHalfOpenTrials(t *testing.T) {
	trials := func(n uint32) Settings {
		return tweak(func(s *Settings) { s.HalfOpenMaxRequests = n })
	}

	t.Run("raising the limit admits more trials", func(t *testing.T) {
		b, clk, _ := newBreaker(t, trials(2))
		halfOpen(t, b, clk)
		call(t, b, true)
		b.Update(trials(3))
		wantState(t, b, HalfOpen)
		second, third := admit(t, b), admit(t, b)
		wantRejected(t, b, ErrTooManyRequests)
		second(true)
		wantState(t, b, HalfOpen)
		third(true)
		wantState(t, b, Closed)
	})

	t.Run("lowering the limit to the successes so far closes", func(t *testing.T) {
		b, clk, ch := newBreaker(t, trials(3))
		halfOpen(t, b, clk)
		call(t, b, true)
		call(t, b, true)
		ch.take()
		b.Update(trials(2))
		wantChanges(t, ch, transition{HalfOpen, Closed})
		wantState(t, b, Closed)
		wantCounts(t, b, Counts{})
	})

	t.Run("lowering the limit below the trials in flight", func(t *testing.T) {
		b, clk, _ := newBreaker(t, trials(3))
		halfOpen(t, b, clk)
		first, second := admit(t, b), admit(t, b)
		b.Update(trials(1))
		wantRejected(t, b, ErrTooManyRequests)
		first(true)
		wantState(t, b, Closed)
		// The other trial reports into a later generation.
		second(false)
		wantState(t, b, Closed)
		wantCounts(t, b, Counts{})
	})

	t.Run("lowering the limit while open", func(t *testing.T) {
		b, clk, _ := newBreaker(t, trials(3))
		trip(t, b)
		b.Update(trials(1))
		clk.Advance(testSettings.OpenDuration)
		admit(t, b)
		wantRejected(t, b, ErrTooManyRequests)
	})
}

func TestStateChangeCallbackMayUseBreaker(t *testing.T) {
	clk := newClock()
	var (
		b               *Breaker
		got             []transition
		depth, maxDepth int
	)
	b = New(testSettings, WithClock(clk.Now), WithStateChange(func(from, to State) {
		depth++
		maxDepth = max(maxDepth, depth)
		defer func() { depth-- }()
		got = append(got, transition{from, to})
		if to == Open {
			// Calling back in must not deadlock. The change this causes is
			// delivered after this callback returns, not nested inside it.
			clk.Advance(testSettings.OpenDuration)
			if s := b.State(); s != HalfOpen {
				t.Errorf("State() inside the callback = %v, want %v", s, HalfOpen)
			}
			_ = b.Counts()
			b.Update(testSettings)
		}
	}))
	for range testSettings.MinRequests {
		call(t, b, false)
	}
	want := []transition{{Closed, Open}, {Open, HalfOpen}}
	if !slices.Equal(got, want) {
		t.Fatalf("state changes = %v, want %v", got, want)
	}
	if maxDepth != 1 {
		t.Fatalf("callbacks nested %d deep, want 1", maxDepth)
	}
}

func TestStateChangeCallbackPanicKeepsDelivering(t *testing.T) {
	clk := newClock()
	var (
		b        *Breaker
		got      []transition
		panicked bool
	)
	b = New(testSettings, WithClock(clk.Now), WithStateChange(func(from, to State) {
		got = append(got, transition{from, to})
		if !panicked {
			panicked = true
			// Queue another change behind this one, then fail.
			clk.Advance(testSettings.OpenDuration)
			b.State()
			panic("callback failed")
		}
	}))
	for range testSettings.MinRequests - 1 {
		call(t, b, false)
	}
	done := admit(t, b)
	wantPanic(t, "callback failed", func() { done(false) })
	if want := []transition{{Closed, Open}}; !slices.Equal(got, want) {
		t.Fatalf("state changes = %v, want %v", got, want)
	}

	// The next call delivers the change left queued behind the panic.
	wantState(t, b, HalfOpen)
	if want := []transition{{Closed, Open}, {Open, HalfOpen}}; !slices.Equal(got, want) {
		t.Fatalf("state changes = %v, want %v", got, want)
	}
}

// TestAllowPanicFreesTrialSlot checks that an Allow that took a trial slot
// and then panicked in a state-change callback gives the slot back: its
// caller has no done to free it with.
func TestAllowPanicFreesTrialSlot(t *testing.T) {
	t.Run("panic reporting the change to half-open", func(t *testing.T) {
		oneTrial := tweak(func(s *Settings) { s.HalfOpenMaxRequests = 1 })
		clk := newClock()
		panicked := false
		b := New(oneTrial, WithClock(clk.Now), WithStateChange(func(_, to State) {
			if to == HalfOpen && !panicked {
				panicked = true
				panic("callback failed")
			}
		}))
		trip(t, b)
		clk.Advance(oneTrial.OpenDuration)
		wantPanic(t, "callback failed", func() { _, _ = b.Allow() })

		// The only trial slot is free again, so the breaker can still close.
		call(t, b, true)
		wantState(t, b, Closed)
	})

	t.Run("breaker moved on before the panic", func(t *testing.T) {
		clk := newClock()
		var b *Breaker
		panicked := false
		b = New(testSettings, WithClock(clk.Now), WithStateChange(func(_, to State) {
			if to != HalfOpen || panicked {
				return
			}
			panicked = true
			// Fail the other trial and wait out OpenDuration again: the
			// slot of the panicking call belongs to a generation that is
			// gone, and the new one has taken no trials yet.
			call(t, b, false)
			clk.Advance(testSettings.OpenDuration)
			b.State()
			panic("callback failed")
		}))
		trip(t, b)
		clk.Advance(testSettings.OpenDuration)
		wantPanic(t, "callback failed", func() { _, _ = b.Allow() })

		wantState(t, b, HalfOpen)
		admit(t, b)
		admit(t, b)
		wantRejected(t, b, ErrTooManyRequests)
	})
}

func TestHalfOpenAdmitsLimitUnderContention(t *testing.T) {
	b, clk, _ := newBreaker(t, tweak(func(s *Settings) { s.HalfOpenMaxRequests = 5 }))
	trip(t, b)
	clk.Advance(testSettings.OpenDuration)

	var admitted, rejected atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			<-start
			switch _, err := b.Allow(); {
			case err == nil:
				admitted.Add(1)
			case errors.Is(err, ErrTooManyRequests):
				rejected.Add(1)
			default:
				t.Errorf("Allow() = %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 5 || rejected.Load() != 45 {
		t.Fatalf("admitted %d and rejected %d calls, want 5 and 45", admitted.Load(), rejected.Load())
	}
}

// TestConcurrentUseKeepsChangesOrdered runs calls, reads and updates from
// several goroutines under -race and checks that the state changes reach the
// callback as one consistent sequence.
func TestConcurrentUseKeepsChangesOrdered(t *testing.T) {
	settings := Settings{
		FailureRateThreshold: 0.3,
		MinRequests:          5,
		Window:               time.Second,
		OpenDuration:         20 * time.Millisecond,
		HalfOpenMaxRequests:  3,
	}
	b, clk, ch := newBreaker(t, settings)

	const workers, ops = 8, 1000
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for i := range ops {
				clk.Advance(time.Millisecond)
				switch {
				case i%97 == 0:
					s := settings
					s.HalfOpenMaxRequests = uint32(1 + rng.IntN(3))
					s.Window = time.Duration(1+rng.IntN(2)) * time.Second
					b.Update(s)
				case i%31 == 0:
					b.State()
					b.Counts()
				default:
					_ = b.Do(func() error {
						if rng.IntN(3) == 0 {
							return errBoom
						}
						return nil
					})
				}
			}
		})
	}
	wg.Wait()

	final := b.State()
	got := ch.take()
	if len(got) == 0 {
		t.Fatal("no state changes happened")
	}
	valid := map[transition]bool{
		{Closed, Open}: true, {Open, HalfOpen}: true, {HalfOpen, Open}: true, {HalfOpen, Closed}: true,
	}
	prev := Closed
	for i, tr := range got {
		if tr.from != prev || !valid[tr] {
			t.Fatalf("change %d is %v after a change to %v", i, tr, prev)
		}
		prev = tr.to
	}
	if final != prev {
		t.Fatalf("State() = %v, but the last change was to %v", final, prev)
	}
}

func BenchmarkAllowDone(b *testing.B) {
	br := New(Settings{})
	for b.Loop() {
		done, _ := br.Allow()
		done(true)
	}
}

func BenchmarkAllowRejected(b *testing.B) {
	br := New(Settings{MinRequests: 1, OpenDuration: time.Hour})
	_ = br.Do(func() error { return errBoom })
	for b.Loop() {
		if _, err := br.Allow(); err == nil {
			b.Fatal("call admitted while open")
		}
	}
}
