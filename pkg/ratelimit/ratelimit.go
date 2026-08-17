// Package ratelimit enforces token-bucket rate limits that can be
// reconfigured while traffic flows.
//
// A Limiter's bucket holds up to burst tokens and refills continuously at
// rate tokens per second; every allowed request takes one token and a new
// bucket starts full. SetLimit reconfigures a Limiter in place: the tokens
// saved up so far are kept, capped at the new burst, so pushing a policy
// neither hands every client a fresh burst nor resets their budget.
//
// A Set holds one Limiter per rate-limit policy as pushed by the control
// plane. Its request path reads an immutable map through an atomic pointer,
// so requests only contend when they share a key, on that key's Limiter;
// Update builds a new map and swaps it in.
package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// InfDuration is what Delay reports when no token will ever arrive: the rate
// is zero, or so low that the token is not due within InfDuration (about 292
// years) of tokens last being taken or the limit last changing. A bucket
// stops refilling that long after either.
const InfDuration = time.Duration(math.MaxInt64)

type options struct {
	now func() time.Time
}

// Option configures a Limiter, or every Limiter of a Set.
type Option func(*options)

// WithClock makes limiters read the time from now instead of time.Now, e.g.
// a fake clock in tests. Wait still sleeps on runtime timers for the delay
// computed from now and then checks again. A reading earlier than the
// limiter's last update counts as that update, so a clock that steps
// backwards pauses refills instead of refilling the same interval twice.
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

func applyOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Limiter is a token bucket, created with NewLimiter. It is safe for
// concurrent use.
type Limiter struct {
	now func() time.Time

	mu    sync.Mutex
	rate  float64 // tokens per second, >= 0
	burst int     // capacity, >= 1
	// tokens is the level at last; the level at any later time is derived
	// from the two. Only taking tokens or changing the limit rewrites them,
	// so frequent reads never accumulate rounding error.
	tokens float64
	last   time.Time
	// changed is closed by the next limit change so that sleeping Waits
	// re-plan. The first Wait that has to sleep creates it.
	changed chan struct{}
}

// NewLimiter returns a full bucket of burst tokens that refills at rate
// tokens per second. A burst below 1 means 1; a negative or NaN rate means 0,
// a bucket that never refills.
func NewLimiter(rate float64, burst int, opts ...Option) *Limiter {
	return newLimiter(rate, burst, applyOptions(opts))
}

func newLimiter(rate float64, burst int, o options) *Limiter {
	now := o.now
	if now == nil {
		now = time.Now
	}
	rate, burst = normalize(rate, burst)
	return &Limiter{now: now, rate: rate, burst: burst, tokens: float64(burst), last: now()}
}

func normalize(rate float64, burst int) (float64, int) {
	if rate < 0 || math.IsNaN(rate) {
		rate = 0
	}
	return rate, max(burst, 1)
}

// Allow reports whether one request may proceed now, taking a token if so.
func (l *Limiter) Allow() bool {
	return l.AllowN(1)
}

// AllowN takes n tokens if all of them are available now. n <= 0 is always
// allowed; n above the burst never is, as the bucket never holds more.
func (l *Limiter) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.take(l.clock(), float64(n))
}

// Wait blocks until a token is available and takes it. If ctx ends first,
// or has already ended, Wait returns ctx.Err() and takes nothing. Waiters
// are not served in arrival order; a limit change wakes them to re-plan.
func (l *Limiter) Wait(ctx context.Context) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, delay, changed := l.tryTake()
		if ok {
			return nil
		}
		var expired <-chan time.Time
		if delay != InfDuration {
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}
			expired = timer.C
		}
		// Another caller may take the token first, so every wake-up only
		// leads to a new attempt.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-expired:
		}
	}
}

// tryTake takes a token if one is available. Otherwise it returns how long
// until one should be, and a channel closed by the next limit change.
func (l *Limiter) tryTake() (ok bool, delay time.Duration, changed <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	if l.take(now, 1) {
		return true, 0, nil
	}
	if l.changed == nil {
		l.changed = make(chan struct{})
	}
	return false, l.delay(now), l.changed
}

// Delay returns how long until a token is available, if nobody else takes
// it first: 0 if one is available now, InfDuration if none will ever be.
func (l *Limiter) Delay() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.delay(l.clock())
}

// SetLimit changes the rate and burst, normalized as by NewLimiter, without
// resetting the bucket: tokens accrue at the old rate up to now and at the
// new rate from then on. The level is capped at the new burst; raising the
// burst adds no tokens.
func (l *Limiter) SetLimit(rate float64, burst int) {
	rate, burst = normalize(rate, burst)
	l.mu.Lock()
	defer l.mu.Unlock()
	// Set.Update re-applies every policy on each snapshot. Rebasing the
	// bucket every time would accumulate rounding error, and waking
	// sleeping Waits would be pointless.
	if rate == l.rate && burst == l.burst {
		return
	}
	now := l.clock()
	l.tokens, l.last = min(l.tokensAt(now), float64(burst)), now
	l.rate, l.burst = rate, burst
	if l.changed != nil {
		close(l.changed)
		l.changed = nil
	}
}

// Limit returns the current rate and burst.
func (l *Limiter) Limit() (rate float64, burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate, l.burst
}

// Tokens returns the number of tokens available now. It is fractional while
// the bucket refills.
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokensAt(l.clock())
}

// clock reads the time, never earlier than the last update. Callers hold
// l.mu.
func (l *Limiter) clock() time.Time {
	now := l.now()
	if now.Before(l.last) {
		return l.last
	}
	return now
}

// tokensAt returns the level at now, which is not before l.last. Callers
// hold l.mu.
func (l *Limiter) tokensAt(now time.Time) float64 {
	// Sub saturates, so the level stops rising InfDuration after l.last.
	return l.tokensAfter(now.Sub(l.last))
}

// tokensAfter returns the level elapsed after l.last. Callers hold l.mu.
func (l *Limiter) tokensAfter(elapsed time.Duration) float64 {
	tokens := l.tokens
	// Skipping zero elapsed time also keeps an infinite rate from making NaN.
	if elapsed > 0 {
		// Multiplying before dividing keeps common boundaries exact: 100ms
		// at 10/s is exactly one token.
		tokens += float64(elapsed) * l.rate / float64(time.Second)
	}
	return min(tokens, float64(l.burst))
}

// take removes n tokens if they are available at now. Callers hold l.mu.
func (l *Limiter) take(now time.Time, n float64) bool {
	tokens := l.tokensAt(now)
	if tokens < n {
		return false
	}
	l.tokens, l.last = tokens-n, now
	return true
}

// delay returns how long after now, which is not before l.last, the bucket
// holds a token. Callers hold l.mu.
func (l *Limiter) delay(now time.Time) time.Duration {
	// The search runs on the time since l.last, which tokensAt counts only
	// up to InfDuration. A token not due by then never arrives, and stepping
	// past that point would find the same level again and again, crawling
	// on in tiny steps while holding l.mu.
	elapsed := now.Sub(l.last)
	var d time.Duration
	for {
		missing := 1 - l.tokensAfter(elapsed+d)
		if missing <= 0 {
			return d
		}
		if l.rate == 0 {
			return InfDuration
		}
		// Rounding can leave a sliver missing at the computed instant; at
		// very low rates a nanosecond adds less than float64 resolves. As
		// it errs by a few units in the last place at most, repeating the
		// step on the remainder settles within a few steps, so Allow is
		// sure to succeed once the delay has passed.
		ns := math.Ceil(missing * float64(time.Second) / l.rate)
		if ns >= float64(InfDuration) {
			return InfDuration
		}
		step := max(time.Duration(ns), 1)
		if step > InfDuration-elapsed-d {
			return InfDuration
		}
		d += step
	}
}
