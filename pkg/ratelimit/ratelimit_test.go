package ratelimit

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// fakeClock is a manually advanced clock, safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
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

func newTestLimiter(rate float64, burst int) (*Limiter, *fakeClock) {
	clock := newFakeClock()
	return NewLimiter(rate, burst, WithClock(clock.Now)), clock
}

func wantTokens(t *testing.T, l *Limiter, want float64) {
	t.Helper()
	if got := l.Tokens(); got != want {
		t.Fatalf("Tokens() = %v, want %v", got, want)
	}
}

func wantLimit(t *testing.T, l *Limiter, wantRate float64, wantBurst int) {
	t.Helper()
	if rate, burst := l.Limit(); rate != wantRate || burst != wantBurst {
		t.Fatalf("Limit() = %v, %d; want %v, %d", rate, burst, wantRate, wantBurst)
	}
}

// allowed calls Allow n times and returns how many calls succeeded.
func allowed(l *Limiter, n int) int {
	granted := 0
	for range n {
		if l.Allow() {
			granted++
		}
	}
	return granted
}

func TestNewLimiterNormalizesLimits(t *testing.T) {
	tests := []struct {
		name      string
		rate      float64
		burst     int
		wantRate  float64
		wantBurst int
	}{
		{"as given", 10, 5, 10, 5},
		{"zero rate", 0, 5, 0, 5},
		{"negative rate", -1, 5, 0, 5},
		{"NaN rate", math.NaN(), 5, 0, 5},
		{"zero burst", 10, 0, 10, 1},
		{"negative burst", 10, -3, 10, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, _ := newTestLimiter(tt.rate, tt.burst)
			wantLimit(t, l, tt.wantRate, tt.wantBurst)
			wantTokens(t, l, float64(tt.wantBurst)) // starts full
		})
	}
}

func TestBurstThenRefill(t *testing.T) {
	l, clock := newTestLimiter(10, 5)
	if got := allowed(l, 8); got != 5 {
		t.Fatalf("%d of 8 requests allowed from a full bucket, want the burst of 5", got)
	}
	clock.Advance(50 * time.Millisecond)
	wantTokens(t, l, 0.5)
	if l.Allow() {
		t.Fatal("Allow succeeded with half a token")
	}
	clock.Advance(50 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("Allow failed after 100ms at 10/s")
	}
	clock.Advance(time.Hour)
	wantTokens(t, l, 5) // refilling stops at the burst
	if got := allowed(l, 8); got != 5 {
		t.Fatalf("%d of 8 requests allowed after a long pause, want 5", got)
	}
}

func TestFractionalRate(t *testing.T) {
	l, clock := newTestLimiter(0.5, 1)
	if got := allowed(l, 3); got != 1 {
		t.Fatalf("%d of 3 requests allowed from a full bucket, want 1", got)
	}
	clock.Advance(time.Second)
	wantTokens(t, l, 0.5)
	if l.Allow() {
		t.Fatal("Allow succeeded 1s into a 2s refill")
	}
	clock.Advance(time.Second)
	if !l.Allow() {
		t.Fatal("Allow failed after 2s at 0.5/s")
	}
	if d := l.Delay(); d != 2*time.Second {
		t.Fatalf("Delay() = %v, want 2s", d)
	}
}

func TestAllowN(t *testing.T) {
	tests := []struct {
		name       string
		spend      int // taken before the call under test
		n          int
		want       bool
		wantTokens float64
	}{
		{"zero is always allowed", 5, 0, true, 0},
		{"negative is always allowed", 5, -1, true, 0},
		{"whole burst", 0, 5, true, 0},
		{"more than the burst never is", 0, 6, false, 5},
		{"exactly what is left", 3, 2, true, 0},
		{"more than is left takes nothing", 3, 3, false, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, _ := newTestLimiter(1, 5)
			if !l.AllowN(tt.spend) {
				t.Fatalf("AllowN(%d) on a full bucket failed", tt.spend)
			}
			if got := l.AllowN(tt.n); got != tt.want {
				t.Fatalf("AllowN(%d) = %v, want %v", tt.n, got, tt.want)
			}
			wantTokens(t, l, tt.wantTokens)
		})
	}
}

func TestDelay(t *testing.T) {
	tests := []struct {
		name    string
		rate    float64
		burst   int
		spend   int
		advance time.Duration
		want    time.Duration
	}{
		{"token available", 2, 2, 1, 0, 0},
		{"empty bucket", 2, 2, 2, 0, 500 * time.Millisecond},
		{"partly refilled", 2, 2, 2, 200 * time.Millisecond, 300 * time.Millisecond},
		{"refilled", 2, 2, 2, 500 * time.Millisecond, 0},
		{"fractional rate", 0.5, 1, 1, 0, 2 * time.Second},
		{"infinite rate refills once time passes", math.Inf(1), 1, 1, 0, time.Nanosecond},
		{"zero rate never refills", 0, 1, 1, time.Hour, InfDuration},
		{"too long for a Duration", 1e-12, 1, 1, 0, InfDuration},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, clock := newTestLimiter(tt.rate, tt.burst)
			if !l.AllowN(tt.spend) {
				t.Fatalf("AllowN(%d) on a full bucket failed", tt.spend)
			}
			clock.Advance(tt.advance)
			d := l.Delay()
			if d != tt.want {
				t.Fatalf("Delay() = %v, want %v", d, tt.want)
			}
			if d == InfDuration {
				if l.Allow() {
					t.Fatal("Allow succeeded although Delay reports no token ever")
				}
				return
			}
			// Allow agrees with Delay: refused a nanosecond early, granted on
			// time.
			if d > 0 {
				clock.Advance(d - time.Nanosecond)
				if l.Allow() {
					t.Fatal("Allow succeeded before Delay passed")
				}
				clock.Advance(time.Nanosecond)
			}
			if !l.Allow() {
				t.Fatal("Allow failed once Delay passed")
			}
		})
	}
}

// TestDelayIsNeverShort uses rates whose arithmetic does not come out exact.
// Waiting Delay must always be enough for Allow to succeed.
func TestDelayIsNeverShort(t *testing.T) {
	// At 3e-9/s a single closed-form step leaves the bucket 1e-16 tokens
	// short, because a nanosecond adds less than float64 can resolve.
	rates := []float64{3e-9, 1e-7, 0.03, 0.3, 0.7, 3, 7, 13.37, 123.456, 999.99, 333333}
	waits := []time.Duration{0, time.Millisecond, 333 * time.Millisecond, 1700 * time.Millisecond}
	for _, rate := range rates {
		for _, wait := range waits {
			l, clock := newTestLimiter(rate, 1)
			l.Allow()
			clock.Advance(wait)
			d := l.Delay()
			clock.Advance(d)
			if !l.Allow() {
				t.Errorf("rate %v/s, %v after emptying: Allow failed after waiting Delay() = %v", rate, wait, d)
			}
		}
	}
}

// TestDelayAtTheEndOfCountedTime uses rates so low that the token falls due
// around InfDuration after the bucket emptied, where it stops counting time.
// Delay must still find a token due just before that point, and see at once
// that one due just after never arrives: stepping towards it in microseconds
// while holding the lock would take longer than any test timeout from a
// century away.
func TestDelayAtTheEndOfCountedTime(t *testing.T) {
	tests := []struct {
		name string
		rate float64
		due  bool
	}{
		{"due just before", float64(time.Second) / (math.Exp2(63) - math.Exp2(40)), true},
		{"due just after", math.Nextafter(float64(time.Second)/math.Exp2(63), 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, idle := range []time.Duration{0, time.Hour, 100 * 365 * 24 * time.Hour} {
				l, clock := newTestLimiter(tt.rate, 1)
				l.Allow()
				clock.Advance(idle)
				d := l.Delay()
				if !tt.due {
					if d != InfDuration {
						t.Errorf("%v after emptying: Delay() = %v, want InfDuration", idle, d)
					}
					if l.Allow() {
						t.Errorf("%v after emptying: Allow succeeded although no token is due", idle)
					}
					continue
				}
				if d == InfDuration {
					t.Errorf("%v after emptying: Delay() = InfDuration for a token that is due", idle)
					continue
				}
				clock.Advance(d)
				if !l.Allow() {
					t.Errorf("%v after emptying: Allow failed after waiting Delay() = %v", idle, d)
				}
			}
		})
	}
}

func TestSetLimit(t *testing.T) {
	t.Run("keeps tokens earned at the old rate", func(t *testing.T) {
		l, clock := newTestLimiter(1, 10)
		l.AllowN(10)
		clock.Advance(2 * time.Second)
		l.SetLimit(100, 10)
		wantLimit(t, l, 100, 10)
		wantTokens(t, l, 2) // earned at 1/s, not 100/s
		clock.Advance(10 * time.Millisecond)
		wantTokens(t, l, 3) // the new rate applies from the change on
	})
	t.Run("lowering the burst clamps tokens", func(t *testing.T) {
		l, _ := newTestLimiter(1, 10)
		l.SetLimit(1, 4)
		wantTokens(t, l, 4)
		if l.AllowN(5) {
			t.Fatal("AllowN(5) succeeded above the new burst of 4")
		}
	})
	t.Run("raising the burst adds no tokens", func(t *testing.T) {
		l, clock := newTestLimiter(1, 4)
		l.SetLimit(1, 20)
		wantTokens(t, l, 4)
		clock.Advance(time.Minute)
		wantTokens(t, l, 20)
	})
	t.Run("raising the burst keeps the old cap on past refills", func(t *testing.T) {
		l, clock := newTestLimiter(1, 4)
		l.AllowN(4)
		// A minute at 1/s refills the bucket many times over, but only up
		// to the burst in force while it passed.
		clock.Advance(time.Minute)
		l.SetLimit(1, 20)
		wantTokens(t, l, 4)
		if l.AllowN(5) {
			t.Fatal("AllowN(5) succeeded right after raising the burst from 4")
		}
		clock.Advance(time.Minute)
		wantTokens(t, l, 20)
	})
	t.Run("normalizes like NewLimiter", func(t *testing.T) {
		l, clock := newTestLimiter(1, 4)
		l.SetLimit(-1, 0)
		wantLimit(t, l, 0, 1)
		wantTokens(t, l, 1)
		l.Allow()
		clock.Advance(time.Hour)
		wantTokens(t, l, 0)
	})
}

func TestClockSteppingBackwards(t *testing.T) {
	l, clock := newTestLimiter(1, 2)
	l.Allow()
	clock.Advance(-10 * time.Second)
	if !l.Allow() {
		t.Fatal("the saved token was refused after the clock stepped back")
	}
	if l.Allow() {
		t.Fatal("Allow succeeded with the bucket empty")
	}
	// Back where it was: the 10s already lived through must not refill.
	clock.Advance(10 * time.Second)
	wantTokens(t, l, 0)
	clock.Advance(time.Second)
	wantTokens(t, l, 1)
}

// TestLimiterConcurrentUse mixes every method under -race. The clock is
// frozen, so exactly the initial burst may be granted: reconfiguring never
// mints tokens.
func TestLimiterConcurrentUse(t *testing.T) {
	const (
		burst      = 50
		goroutines = 8
		calls      = 200
	)
	l, _ := newTestLimiter(1, burst)
	var granted atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range calls {
				if l.Allow() {
					granted.Add(1)
				}
				l.Tokens()
				l.Delay()
				l.Limit()
			}
		})
	}
	wg.Go(func() {
		for i := range calls {
			if i%2 == 0 {
				l.SetLimit(2, 2*burst)
			} else {
				l.SetLimit(1, burst)
			}
		}
	})
	wg.Wait()
	if got := granted.Load(); got != burst {
		t.Fatalf("granted %d requests with a frozen clock, want exactly the burst of %d", got, burst)
	}
}

func TestWaitTakesAvailableToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLimiter(1, 2)
		start := time.Now()
		if err := l.Wait(t.Context()); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("Wait took %v with tokens available", waited)
		}
		wantTokens(t, l, 1)
	})
}

func TestWaitBlocksUntilRefill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLimiter(4, 1)
		l.Allow()
		start := time.Now()
		if err := l.Wait(t.Context()); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if waited := time.Since(start); waited != 250*time.Millisecond {
			t.Fatalf("Wait took %v, want 250ms at 4/s", waited)
		}
		wantTokens(t, l, 0)
	})
}

func TestWaitServesWaitersAsTokensArrive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLimiter(10, 1)
		l.Allow()
		start := time.Now()
		var (
			mu    sync.Mutex
			times []time.Duration
			wg    sync.WaitGroup
		)
		for range 5 {
			wg.Go(func() {
				if err := l.Wait(t.Context()); err != nil {
					t.Errorf("Wait: %v", err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				times = append(times, time.Since(start))
			})
		}
		wg.Wait()
		slices.Sort(times)
		want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond}
		if !slices.Equal(times, want) {
			t.Fatalf("waiters got tokens after %v, want one every 100ms: %v", times, want)
		}
	})
}

func TestWaitReturnsContextError(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := NewLimiter(1, 1)
			l.Allow()
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := l.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Wait = %v, want %v", err, context.DeadlineExceeded)
			}
			if waited := time.Since(start); waited != 300*time.Millisecond {
				t.Fatalf("Wait returned after %v, want the 300ms deadline", waited)
			}
			wantTokens(t, l, 0.3) // nothing taken
		})
	})
	t.Run("canceled while waiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := NewLimiter(1, 1)
			l.Allow()
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			if err := l.Wait(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Wait = %v, want %v", err, context.Canceled)
			}
			wantTokens(t, l, 0.1)
		})
	})
	t.Run("already canceled", func(t *testing.T) {
		l, _ := newTestLimiter(1, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := l.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v, want %v", err, context.Canceled)
		}
		wantTokens(t, l, 1) // the available token was not taken
	})
}

func TestWaitWakesOnLimitChange(t *testing.T) {
	t.Run("bucket that never refills", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := NewLimiter(0, 1)
			l.Allow()
			done := make(chan error, 1)
			go func() { done <- l.Wait(t.Context()) }()

			time.Sleep(time.Hour)
			synctest.Wait()
			select {
			case err := <-done:
				t.Fatalf("Wait returned %v from a bucket that never refills", err)
			default:
			}

			start := time.Now()
			l.SetLimit(10, 1)
			if err := <-done; err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if waited := time.Since(start); waited != 100*time.Millisecond {
				t.Fatalf("Wait returned %v after the change, want 100ms at the new 10/s", waited)
			}
		})
	})
	// Here Wait sleeps on a timer for the token due at the old rate, which
	// must not keep it from re-planning.
	t.Run("token due at the old rate", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := NewLimiter(0.001, 1) // the next token is due in 1000s
			l.Allow()
			done := make(chan error, 1)
			go func() { done <- l.Wait(t.Context()) }()
			synctest.Wait()

			start := time.Now()
			l.SetLimit(10, 1)
			if err := <-done; err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if waited := time.Since(start); waited != 100*time.Millisecond {
				t.Fatalf("Wait returned %v after the change, want 100ms at the new 10/s", waited)
			}
		})
	})
}

func BenchmarkLimiterAllow(b *testing.B) {
	l := NewLimiter(1e9, 1_000_000)
	for b.Loop() {
		l.Allow()
	}
}
