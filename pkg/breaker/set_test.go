package breaker

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func enabled(key string, s Settings) Policy {
	return Policy{Key: key, Enabled: true, Settings: s}
}

func wantKeys(t *testing.T, s *Set, want ...string) {
	t.Helper()
	if got := s.Keys(); !slices.Equal(got, want) {
		t.Fatalf("Keys() = %q, want %q", got, want)
	}
}

func mustBreaker(t *testing.T, s *Set, key string) *Breaker {
	t.Helper()
	b, ok := s.Breaker(key)
	if !ok {
		t.Fatalf("Breaker(%q) not found", key)
	}
	return b
}

// keyedChange is a state change a set reported for key.
type keyedChange struct {
	key      string
	from, to State
}

func (c keyedChange) String() string { return c.key + ": " + c.from.String() + " -> " + c.to.String() }

// keyedChanges records the state changes a set reports, in order.
type keyedChanges struct {
	mu   sync.Mutex
	list []keyedChange
}

func (c *keyedChanges) record(key string, from, to State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = append(c.list, keyedChange{key, from, to})
}

// take returns the changes recorded since the previous take.
func (c *keyedChanges) take() []keyedChange {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.list
	c.list = nil
	return out
}

func wantKeyedChanges(t *testing.T, ch *keyedChanges, want ...keyedChange) {
	t.Helper()
	if got := ch.take(); !slices.Equal(got, want) {
		t.Fatalf("state changes = %v, want %v", got, want)
	}
}

func TestSetUpdateAddsReconfiguresRemoves(t *testing.T) {
	clk := newClock()
	s := NewSet(WithSetClock(clk.Now))
	wantKeys(t, s)

	s.Update([]Policy{
		enabled("payments", testSettings),
		enabled("inventory", testSettings),
		{Key: "search", Enabled: false, Settings: testSettings},
	})
	wantKeys(t, s, "inventory", "payments")
	if _, ok := s.Breaker("search"); ok {
		t.Fatal(`Breaker("search") found for a disabled policy`)
	}
	payments := mustBreaker(t, s, "payments")
	trip(t, payments)

	// Reconfiguring keeps the breaker and its state.
	longer := tweak(func(st *Settings) { st.OpenDuration = time.Minute })
	s.Update([]Policy{enabled("payments", longer), enabled("inventory", testSettings)})
	if got := mustBreaker(t, s, "payments"); got != payments {
		t.Fatal("Update replaced the breaker instead of reconfiguring it")
	}
	wantState(t, payments, Open)
	if got := payments.Settings(); got != longer {
		t.Fatalf("Settings() = %+v, want %+v", got, longer)
	}

	// Disabled and missing keys are dropped.
	s.Update([]Policy{{Key: "payments", Enabled: false, Settings: longer}})
	wantKeys(t, s)

	// Enabling the key again starts afresh; the dropped breaker lives on alone.
	s.Update([]Policy{enabled("payments", longer)})
	again := mustBreaker(t, s, "payments")
	if again == payments {
		t.Fatal("re-enabled key reused the dropped breaker")
	}
	wantState(t, again, Closed)
	wantState(t, payments, Open)

	s.Update(nil)
	wantKeys(t, s)
}

func TestSetDuplicateKeysLastWins(t *testing.T) {
	s := NewSet()
	other := tweak(func(st *Settings) { st.MinRequests = 9 })
	s.Update([]Policy{enabled("payments", testSettings), enabled("payments", other)})
	if got := mustBreaker(t, s, "payments").Settings(); got != other {
		t.Fatalf("Settings() = %+v, want the last policy's %+v", got, other)
	}
	s.Update([]Policy{enabled("payments", testSettings), {Key: "payments"}})
	wantKeys(t, s)
}

func TestSetWithoutBreakerAllowsEverything(t *testing.T) {
	s := NewSet()
	s.Update([]Policy{enabled("payments", testSettings)})
	done, err := s.Allow("search")
	if err != nil || done == nil {
		t.Fatalf("Allow(no breaker) = %v, %v; want a no-op done and nil", done != nil, err)
	}
	done(false)
	done(true)

	called := false
	if err := s.Do("search", func() error { called = true; return errBoom }); err != errBoom || !called {
		t.Fatalf("Do(no breaker) = %v (fn called: %v), want fn called and its error", err, called)
	}
	wantCounts(t, mustBreaker(t, s, "payments"), Counts{})
}

func TestSetAllowAndDoUseTheKeysBreaker(t *testing.T) {
	clk := newClock()
	s := NewSet(WithSetClock(clk.Now))
	s.Update([]Policy{enabled("payments", testSettings), enabled("inventory", testSettings)})

	for range testSettings.MinRequests {
		if err := s.Do("payments", func() error { return errBoom }); err != errBoom {
			t.Fatalf("Do = %v, want %v", err, errBoom)
		}
	}
	if _, err := s.Allow("payments"); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow(payments) = %v, want ErrOpen", err)
	}
	called := false
	if err := s.Do("payments", func() error { called = true; return nil }); !errors.Is(err, ErrOpen) || called {
		t.Fatalf("Do(payments) = %v (fn called: %v), want ErrOpen without calling fn", err, called)
	}
	if err := s.Do("inventory", func() error { return nil }); err != nil {
		t.Fatalf("Do(inventory) = %v, want other keys unaffected", err)
	}

	// The set's clock drives its breakers.
	clk.Advance(testSettings.OpenDuration)
	done, err := s.Allow("payments")
	if err != nil {
		t.Fatalf("Allow(payments) after OpenDuration = %v", err)
	}
	done(true)
	wantState(t, mustBreaker(t, s, "payments"), HalfOpen)
}

func TestSetStateChangeNamesTheKey(t *testing.T) {
	clk, ch := newClock(), &keyedChanges{}
	s := NewSet(WithSetClock(clk.Now), WithSetStateChange(ch.record))
	s.Update([]Policy{enabled("payments", testSettings), enabled("inventory", testSettings)})
	trip(t, mustBreaker(t, s, "payments"))
	clk.Advance(testSettings.OpenDuration)
	wantState(t, mustBreaker(t, s, "payments"), HalfOpen)
	wantState(t, mustBreaker(t, s, "inventory"), Closed)
	wantKeyedChanges(t, ch, keyedChange{"payments", Closed, Open}, keyedChange{"payments", Open, HalfOpen})
}

// TestSetStopsReportingDroppedBreakers checks that a breaker's state changes
// stop reaching the set's callback once Update has dropped it, so they cannot
// be mistaken for, or overlap with, those of the key's later breaker.
func TestSetStopsReportingDroppedBreakers(t *testing.T) {
	t.Run("calls admitted before the drop", func(t *testing.T) {
		clk, ch := newClock(), &keyedChanges{}
		s := NewSet(WithSetClock(clk.Now), WithSetStateChange(ch.record))
		s.Update([]Policy{enabled("payments", testSettings)})
		dropped := mustBreaker(t, s, "payments")
		var inFlight []func(bool)
		for range testSettings.MinRequests {
			done, err := s.Allow("payments")
			if err != nil {
				t.Fatalf("Allow(payments) = %v, want the call admitted", err)
			}
			inFlight = append(inFlight, done)
		}

		// The calls still count on the dropped breaker, which opens.
		s.Update(nil)
		for _, done := range inFlight {
			done(false)
		}
		wantState(t, dropped, Open)
		wantKeyedChanges(t, ch)

		// Added again, the key reports its new breaker only.
		s.Update([]Policy{enabled("payments", testSettings)})
		trip(t, mustBreaker(t, s, "payments"))
		clk.Advance(testSettings.OpenDuration)
		wantState(t, dropped, HalfOpen)
		wantKeyedChanges(t, ch, keyedChange{"payments", Closed, Open})
	})

	t.Run("change not yet reported at the drop", func(t *testing.T) {
		clk, ch := newClock(), &keyedChanges{}
		var (
			s *Set
			b *Breaker
		)
		s = NewSet(WithSetClock(clk.Now), WithSetStateChange(func(key string, from, to State) {
			ch.record(key, from, to)
			if to == Open {
				// The change to half-open waits for this callback to
				// return, and by then the breaker is no longer in the set.
				clk.Advance(testSettings.OpenDuration)
				b.State()
				s.Update(nil)
			}
		}))
		s.Update([]Policy{enabled("payments", testSettings)})
		b = mustBreaker(t, s, "payments")
		for range testSettings.MinRequests {
			call(t, b, false)
		}
		wantKeys(t, s)
		wantState(t, b, HalfOpen)
		wantKeyedChanges(t, ch, keyedChange{"payments", Closed, Open})
	})
}

// TestSetReconfigureKeepsDueHalfOpen checks that Update reports the lazy
// change to half-open of a kept breaker whose OpenDuration has passed, rather
// than letting a longer OpenDuration send it back to open.
func TestSetReconfigureKeepsDueHalfOpen(t *testing.T) {
	clk, ch := newClock(), &keyedChanges{}
	s := NewSet(WithSetClock(clk.Now), WithSetStateChange(ch.record))
	s.Update([]Policy{enabled("payments", testSettings)})
	b := mustBreaker(t, s, "payments")
	trip(t, b)
	clk.Advance(testSettings.OpenDuration)

	s.Update([]Policy{enabled("payments", tweak(func(st *Settings) { st.OpenDuration = time.Minute }))})
	wantKeyedChanges(t, ch, keyedChange{"payments", Closed, Open}, keyedChange{"payments", Open, HalfOpen})
	wantState(t, b, HalfOpen)
}

func TestSetUpdateRunsCallbacksAfterUnlocking(t *testing.T) {
	clk := newClock()
	policies := func(trials uint32) []Policy {
		return []Policy{enabled("payments", tweak(func(st *Settings) { st.HalfOpenMaxRequests = trials }))}
	}
	var s *Set
	s = NewSet(WithSetClock(clk.Now), WithSetStateChange(func(_ string, _, to State) {
		if to == Closed {
			// Deadlocks if Update still holds the set's lock.
			s.Update(policies(1))
			_ = s.Keys()
		}
	}))
	s.Update(policies(2))
	b := mustBreaker(t, s, "payments")
	halfOpen(t, b, clk)
	call(t, b, true)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		s.Update(policies(1)) // closes: one success already meets the new limit
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Update deadlocked in a state-change callback")
	}
	wantState(t, b, Closed)
}

// TestSetConcurrentUse runs calls and reconfiguration concurrently under
// -race.
func TestSetConcurrentUse(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	policies := func(round int) []Policy {
		var ps []Policy
		for i, key := range keys {
			ps = append(ps, Policy{
				Key:      key,
				Enabled:  (round+i)%3 != 0,
				Settings: Settings{MinRequests: uint32(1 + round%5), Window: time.Duration(1+round%2) * time.Second},
			})
		}
		return ps
	}
	s := NewSet()
	const rounds = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		for round := range rounds {
			s.Update(policies(round))
		}
	})
	for w := range 4 {
		wg.Go(func() {
			for i := range 2000 {
				_ = s.Do(keys[(w+i)%len(keys)], func() error {
					if i%4 == 0 {
						return errBoom
					}
					return nil
				})
				if i%100 == 0 {
					_ = s.Keys()
				}
			}
		})
	}
	wg.Wait()

	var want []string
	for _, p := range policies(rounds - 1) {
		if p.Enabled {
			want = append(want, p.Key)
		}
	}
	wantKeys(t, s, want...)
}

func BenchmarkSetAllowParallel(b *testing.B) {
	s := NewSet()
	s.Update([]Policy{enabled("payments", Settings{})})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			done, _ := s.Allow("payments")
			done(true)
		}
	})
}
