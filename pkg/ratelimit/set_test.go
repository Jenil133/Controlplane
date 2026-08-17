package ratelimit

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestSet() (*Set, *fakeClock) {
	clock := newFakeClock()
	return NewSet(WithClock(clock.Now)), clock
}

func enabled(key string, rate float64, burst int) Policy {
	return Policy{Key: key, Enabled: true, RequestsPerSecond: rate, Burst: burst}
}

func disabled(key string, rate float64, burst int) Policy {
	return Policy{Key: key, RequestsPerSecond: rate, Burst: burst}
}

func wantKeys(t *testing.T, s *Set, want ...string) {
	t.Helper()
	if got := s.Keys(); !slices.Equal(got, want) {
		t.Fatalf("Keys() = %q, want %q", got, want)
	}
}

func mustLimiter(t *testing.T, s *Set, key string) *Limiter {
	t.Helper()
	l, ok := s.Limiter(key)
	if !ok {
		t.Fatalf("Limiter(%q): no limiter", key)
	}
	return l
}

func TestEmptySetAllowsEverything(t *testing.T) {
	for name, s := range map[string]*Set{"NewSet": NewSet(), "zero value": new(Set)} {
		t.Run(name, func(t *testing.T) {
			for range 100 {
				if !s.Allow("checkout") {
					t.Fatal("Allow without a policy = false")
				}
			}
			if _, ok := s.Limiter("checkout"); ok {
				t.Fatal("Limiter found for a key without a policy")
			}
			wantKeys(t, s)
		})
	}
}

func TestZeroSetEnforcesPolicies(t *testing.T) {
	var s Set
	// A zero rate never refills, so this holds whatever the real clock does.
	s.Update([]Policy{enabled("checkout", 0, 2)})
	wantKeys(t, &s, "checkout")
	if !s.Allow("checkout") || !s.Allow("checkout") || s.Allow("checkout") {
		t.Fatal("want the burst of 2, then a refusal")
	}
}

func TestSetAddsReconfiguresAndRemovesPolicies(t *testing.T) {
	s, clock := newTestSet()
	s.Update([]Policy{
		enabled("search", 10, 1),
		enabled("checkout", 1, 2),
		disabled("export", 1, 1),
	})
	wantKeys(t, s, "checkout", "search")
	if !s.Allow("checkout") || !s.Allow("checkout") || s.Allow("checkout") {
		t.Fatal("checkout: want the burst of 2, then a refusal")
	}
	for range 5 {
		if !s.Allow("export") {
			t.Fatal("a disabled policy limited its key")
		}
	}
	checkout := mustLimiter(t, s, "checkout")

	// Reconfiguring keeps the Limiter and its empty bucket.
	s.Update([]Policy{enabled("checkout", 4, 8), enabled("search", 10, 1)})
	if l := mustLimiter(t, s, "checkout"); l != checkout {
		t.Fatal("reconfiguring replaced the Limiter, losing its state")
	}
	wantLimit(t, checkout, 4, 8)
	if s.Allow("checkout") {
		t.Fatal("reconfiguring refilled the bucket")
	}
	clock.Advance(250 * time.Millisecond)
	if !s.Allow("checkout") {
		t.Fatal("no token 250ms after switching to 4/s")
	}

	// Missing and disabled policies are removed: their keys are unlimited.
	s.Update([]Policy{disabled("checkout", 4, 8)})
	wantKeys(t, s)
	for range 5 {
		if !s.Allow("checkout") || !s.Allow("search") {
			t.Fatal("a removed policy still limits its key")
		}
	}

	// Adding a key back starts a fresh, full bucket.
	s.Update([]Policy{enabled("checkout", 4, 8)})
	if l := mustLimiter(t, s, "checkout"); l == checkout {
		t.Fatal("a re-added key reused the removed Limiter")
	} else {
		wantTokens(t, l, 8)
	}
}

// TestSetReapplyingPoliciesLeavesBucketsAlone mirrors the SDK, which calls
// Update with every snapshot. An unchanged policy must not touch its bucket:
// rebasing it each time would add up rounding error (0.1 added ten times is
// below 1) and refuse a token that is due.
func TestSetReapplyingPoliciesLeavesBucketsAlone(t *testing.T) {
	s, clock := newTestSet()
	policies := []Policy{enabled("checkout", 0.1, 1)}
	s.Update(policies)
	if !s.Allow("checkout") {
		t.Fatal("Allow failed on a full bucket")
	}
	for range 10 {
		clock.Advance(time.Second)
		s.Update(policies)
	}
	if !s.Allow("checkout") {
		t.Fatalf("no token 10s after emptying at 0.1/s; Tokens() = %v", mustLimiter(t, s, "checkout").Tokens())
	}
}

func TestSetKeysAreSorted(t *testing.T) {
	s, _ := newTestSet()
	s.Update([]Policy{
		enabled("b", 1, 1), enabled("e", 1, 1), enabled("a", 1, 1),
		disabled("c", 1, 1), enabled("d", 1, 1), enabled("B", 1, 1),
	})
	wantKeys(t, s, "B", "a", "b", "d", "e")
}

func TestSetUpdateLastPolicyForKeyWins(t *testing.T) {
	tests := []struct {
		name      string
		policies  []Policy
		wantBurst int // 0: the key is unlimited
	}{
		{"enabled twice", []Policy{enabled("k", 1, 1), enabled("k", 1, 5)}, 5},
		{"disabled after enabled", []Policy{enabled("k", 1, 1), disabled("k", 1, 5)}, 0},
		{"enabled after disabled", []Policy{disabled("k", 1, 5), enabled("k", 1, 3)}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestSet()
			s.Update(tt.policies)
			l, ok := s.Limiter("k")
			if tt.wantBurst == 0 {
				if ok {
					t.Fatalf("key limited with %v, want unlimited", l)
				}
				return
			}
			if !ok {
				t.Fatal("key unlimited, want a Limiter")
			}
			wantLimit(t, l, 1, tt.wantBurst)
		})
	}
}

// TestSetConcurrentAllow runs Allow from many goroutines while two updaters
// keep swapping the map. The clock is frozen, so exactly the burst of the
// stable key may be granted: re-applying its policy never refills it.
func TestSetConcurrentAllow(t *testing.T) {
	const (
		burst      = 100
		goroutines = 8
		calls      = 500
	)
	s, _ := newTestSet()
	stable := enabled("checkout", 1, burst)
	s.Update([]Policy{stable})

	var granted atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range calls {
				if s.Allow("checkout") {
					granted.Add(1)
				}
				s.Allow("search") // limited only while its policy exists
				s.Keys()
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for i := range calls {
				if i%2 == 0 {
					s.Update([]Policy{stable, enabled("search", 1, 1)})
				} else {
					s.Update([]Policy{stable})
				}
			}
		})
	}
	wg.Wait()
	if got := granted.Load(); got != burst {
		t.Fatalf("granted %d requests with a frozen clock, want exactly the burst of %d", got, burst)
	}
}

// TestSetConcurrentUpdatesAreAtomic races two updates that configure the same
// key differently. Whichever update's map ends up installed, the key must
// carry that update's limit, never a mix of the two.
func TestSetConcurrentUpdatesAreAtomic(t *testing.T) {
	a := []Policy{enabled("shared", 1, 1), enabled("from-a", 1, 1)}
	b := []Policy{enabled("shared", 2, 2), enabled("from-b", 1, 1)}
	for range 2000 {
		s, _ := newTestSet()
		s.Update([]Policy{enabled("shared", 3, 3)})
		var wg sync.WaitGroup
		wg.Go(func() { s.Update(a) })
		wg.Go(func() { s.Update(b) })
		wg.Wait()
		want := 2
		if _, ok := s.Limiter("from-a"); ok {
			want = 1
		}
		wantLimit(t, mustLimiter(t, s, "shared"), float64(want), want)
	}
}

func BenchmarkSetAllow(b *testing.B) {
	s := NewSet()
	policies := make([]Policy, 100)
	for i := range policies {
		policies[i] = enabled(fmt.Sprintf("route-%d", i), 1e9, 1_000_000)
	}
	s.Update(policies)
	for _, key := range []string{"route-42", "no-policy"} {
		b.Run(key, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					s.Allow(key)
				}
			})
		})
	}
}
