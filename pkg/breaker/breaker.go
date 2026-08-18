// Package breaker implements circuit breakers that can be reconfigured while
// in use, and a Set that keeps one breaker per circuit breaker policy.
//
// A breaker starts closed. Closed, it lets every call through and records
// each outcome in a rolling window of 10 slots of Window/10. After a failure
// it opens if the window holds at least MinRequests outcomes and at least
// FailureRateThreshold of them are failures. Open, it rejects calls with
// ErrOpen until OpenDuration has passed, then turns half-open. Half-open, it
// admits HalfOpenMaxRequests trial calls, counting those in flight as well as
// those completed, and rejects the rest with ErrTooManyRequests. One failed
// trial opens it again and restarts the OpenDuration timer;
// HalfOpenMaxRequests successful trials close it with an empty window.
//
// Every state change starts a new generation, and a call reports its outcome
// to the generation that admitted it. Outcomes reported to an older
// generation are dropped, because a call that was already running when the
// breaker changed state says nothing about the new state: a slow call that
// started before the breaker opened must not count as a failed trial once it
// is half-open, and a trial still running when the breaker closed must not
// count against the fresh window. Comparing states instead of generations
// would not be enough, since a call can report after a whole
// closed → open → half-open → closed cycle.
//
// The open → half-open change is applied lazily, by the next Allow, State or
// Update once OpenDuration has passed, so breakers need no timers or
// goroutines.
package breaker

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// State is a breaker's position in its state machine.
type State int

// Breaker states. A new breaker is Closed.
const (
	Closed State = iota
	Open
	HalfOpen
)

// String returns "closed", "open" or "half-open".
func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

var (
	// ErrOpen rejects a call while the breaker is open.
	ErrOpen = errors.New("breaker: circuit open")
	// ErrTooManyRequests rejects a call while the breaker is half-open and
	// has already admitted HalfOpenMaxRequests trial calls.
	ErrTooManyRequests = errors.New("breaker: half-open trial limit reached")
)

// Defaults that New and Update use in place of invalid settings.
const (
	DefaultFailureRateThreshold = 0.5
	DefaultMinRequests          = 20
	DefaultWindow               = 10 * time.Second
	DefaultOpenDuration         = 5 * time.Second
	DefaultHalfOpenMaxRequests  = 1
)

// windowSlots is the number of slots the rolling window is kept in.
const windowSlots = 10

// Settings configure a breaker. New and Update replace every invalid field
// with its default, so the zero Settings describe a working breaker:
//
//   - FailureRateThreshold NaN or outside (0, 1]: DefaultFailureRateThreshold (0.5)
//   - MinRequests 0: DefaultMinRequests (20)
//   - Window shorter than 10ns, too short to split into 10 slots: DefaultWindow (10s)
//   - OpenDuration zero or negative: DefaultOpenDuration (5s)
//   - HalfOpenMaxRequests 0: DefaultHalfOpenMaxRequests (1)
//
// Only values that cannot work are replaced; the stricter limits the control
// plane enforces on stored policies do not apply here.
type Settings struct {
	// FailureRateThreshold is the share of failed calls, in (0, 1], at which
	// a closed breaker opens.
	FailureRateThreshold float64
	// MinRequests is how many outcomes the window must hold before the
	// failure rate counts, so a few early failures cannot open the breaker.
	MinRequests uint32
	// Window is how far back outcomes are counted. It is kept as 10 slots
	// of Window/10 (rounded down to whole nanoseconds) that expire whole, so
	// an outcome counts for more than 0.9 and at most 1 times Window.
	Window time.Duration
	// OpenDuration is how long the breaker stays open before trial calls.
	OpenDuration time.Duration
	// HalfOpenMaxRequests is how many trial calls a half-open breaker
	// admits; that many must succeed to close it.
	HalfOpenMaxRequests uint32
}

// withDefaults returns s with every invalid field replaced by its default.
func (s Settings) withDefaults() Settings {
	if math.IsNaN(s.FailureRateThreshold) || s.FailureRateThreshold <= 0 || s.FailureRateThreshold > 1 {
		s.FailureRateThreshold = DefaultFailureRateThreshold
	}
	if s.MinRequests == 0 {
		s.MinRequests = DefaultMinRequests
	}
	if s.Window < windowSlots*time.Nanosecond {
		s.Window = DefaultWindow
	}
	if s.OpenDuration <= 0 {
		s.OpenDuration = DefaultOpenDuration
	}
	if s.HalfOpenMaxRequests == 0 {
		s.HalfOpenMaxRequests = DefaultHalfOpenMaxRequests
	}
	return s
}

// Counts are the call outcomes a breaker holds for its current state.
type Counts struct {
	// Requests is the number of calls that reported an outcome.
	Requests uint32
	// Failures is how many of them failed.
	Failures uint32
}

// Option configures a Breaker.
type Option func(*options)

type options struct {
	now      func() time.Time
	onChange func(from, to State)
}

// WithClock makes the breaker read the time from now instead of time.Now.
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

// WithStateChange calls fn after every state change, with the breaker's lock
// released so that fn may use the breaker. Calls to fn never overlap and
// follow the order of the changes. fn runs on the goroutine whose call
// caused the change, unless fn is already running for an earlier change; the
// goroutine running it then delivers the later change too, once fn returns.
// A panic in fn propagates to the breaker call that ran it, and the changes
// still queued are delivered by a later call. An Allow that panics this way
// has admitted no call.
func WithStateChange(fn func(from, to State)) Option {
	return func(o *options) { o.onChange = fn }
}

// Breaker is a circuit breaker. It is safe for concurrent use.
type Breaker struct {
	now      func() time.Time
	onChange func(from, to State)

	mu         sync.Mutex
	settings   Settings
	state      State
	generation uint64
	window     window
	openedAt   time.Time
	// trials counts the calls admitted while half-open, in flight or
	// completed; successes counts the completed ones (all succeeded, since a
	// failure reopens the breaker).
	trials, successes uint32
	// pending holds state changes not yet passed to onChange; delivering is
	// set while a goroutine is passing them on.
	pending    []transition
	delivering bool
}

type transition struct{ from, to State }

// New returns a closed breaker. Invalid settings are replaced by defaults;
// see Settings.
func New(s Settings, opts ...Option) *Breaker {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.now == nil {
		o.now = time.Now
	}
	b := &Breaker{now: o.now, onChange: o.onChange, settings: s.withDefaults()}
	b.window.reset(b.settings.Window, b.now())
	return b
}

// Allow asks to make one call. If err is nil the call may go ahead, and the
// caller must report its outcome by calling done when it finishes. Only the
// first call to done counts, and it is ignored if the breaker changed state
// since Allow (see the package comment). If the call is rejected, err is
// ErrOpen or ErrTooManyRequests and done does nothing.
//
// While half-open, an admitted call holds one of the HalfOpenMaxRequests
// trial slots until it reports, so a call that never reports can keep the
// breaker half-open and rejecting for good: always call done, e.g. with
// defer.
func (b *Breaker) Allow() (done func(success bool), err error) {
	b.mu.Lock()
	b.halfOpenIfDue()
	trial := false
	switch {
	case b.state == Open:
		err = ErrOpen
	case b.state == HalfOpen && b.trials >= b.settings.HalfOpenMaxRequests:
		err = ErrTooManyRequests
	case b.state == HalfOpen:
		b.trials++
		trial = true
	}
	gen := b.generation
	if trial {
		// unlock runs the queued state-change callbacks, such as the one for
		// the change to half-open just made. If one panics, done stays nil:
		// the caller gets nothing to free the trial slot with, and a slot
		// taken for good could keep the breaker half-open and rejecting.
		defer func() {
			if done == nil {
				b.freeTrial(gen)
			}
		}()
	}
	b.unlock()
	if err != nil {
		return noop, err
	}
	return b.reporter(gen), nil
}

// Do calls fn if the breaker admits the call and records the outcome: an
// error or a panic is a failure. A rejected call returns ErrOpen or
// ErrTooManyRequests without calling fn.
func (b *Breaker) Do(fn func() error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	success := false
	defer func() { done(success) }()
	err = fn()
	success = err == nil
	return err
}

// State returns the current state, first turning an open breaker half-open
// if its OpenDuration has passed.
func (b *Breaker) State() State {
	b.mu.Lock()
	b.halfOpenIfDue()
	s := b.state
	b.unlock()
	return s
}

// Counts returns the outcomes recorded in the current state: those in the
// rolling window while closed, the completed trial calls while half-open
// (never a failure, since one reopens the breaker) and none while open.
// Every state change starts again from zero.
func (b *Breaker) Counts() Counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		t := b.window.sum(b.now())
		return Counts{Requests: saturate(t.requests), Failures: saturate(t.failures)}
	case HalfOpen:
		return Counts{Requests: b.successes}
	default:
		return Counts{}
	}
}

// Settings returns the settings in use, with defaults applied.
func (b *Breaker) Settings() Settings {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.settings
}

// Update reconfigures the breaker in place. Like State, it first turns an
// open breaker half-open if its OpenDuration has passed, so a longer new
// OpenDuration cannot send it back to open; a breaker still open turns
// half-open once the new OpenDuration has passed since it opened. Otherwise
// it keeps the state and the generation, so calls in flight still report. A
// changed Window empties the window; other changes apply to the outcomes
// already in it from the next failure on. The one exception to keeping the
// state: a half-open breaker whose successful trials already reach the new
// HalfOpenMaxRequests closes, as it might otherwise never see another
// success. Invalid settings are replaced by defaults; see Settings.
func (b *Breaker) Update(s Settings) {
	b.apply(s)
	b.flush()
}

// apply installs new settings but leaves the resulting state-change
// callbacks queued, so Set.Update can run them after releasing its own lock.
func (b *Breaker) apply(s Settings) {
	s = s.withDefaults()
	b.mu.Lock()
	defer b.mu.Unlock()
	// Apply the change to half-open that the old OpenDuration made due, as
	// State would have: otherwise a longer OpenDuration would send the
	// breaker back to open, but only if no call had applied the change yet.
	b.halfOpenIfDue()
	now := b.now()
	if s.Window != b.settings.Window {
		b.window.reset(s.Window, now)
	}
	b.settings = s
	if b.state == HalfOpen && b.successes >= s.HalfOpenMaxRequests {
		b.setState(Closed, now)
	}
}

// flush runs the state-change callbacks that are still queued.
func (b *Breaker) flush() {
	b.mu.Lock()
	b.unlock()
}

// reporter returns the done func of a call admitted in generation gen.
func (b *Breaker) reporter(gen uint64) func(bool) {
	var reported atomic.Bool
	return func(success bool) {
		if reported.CompareAndSwap(false, true) {
			b.record(gen, success)
		}
	}
}

func noop(bool) {}

// record applies the outcome of a call admitted in generation gen.
func (b *Breaker) record(gen uint64, success bool) {
	b.mu.Lock()
	if gen != b.generation {
		b.mu.Unlock()
		return
	}
	// The generation still matches, so the state is the one the call was
	// admitted in, and an open breaker admits nothing.
	now := b.now()
	switch b.state {
	case Closed:
		b.window.add(now, success)
		if !success && b.failureRateReached(now) {
			b.setState(Open, now)
		}
	case HalfOpen:
		if !success {
			b.setState(Open, now)
			break
		}
		b.successes++
		if b.successes >= b.settings.HalfOpenMaxRequests {
			b.setState(Closed, now)
		}
	}
	b.unlock()
}

// failureRateReached reports whether the window holds enough outcomes, and
// enough failures among them, to open the breaker. Callers hold b.mu.
func (b *Breaker) failureRateReached(now time.Time) bool {
	t := b.window.sum(now)
	return t.requests >= uint64(b.settings.MinRequests) &&
		float64(t.failures)/float64(t.requests) >= b.settings.FailureRateThreshold
}

// freeTrial gives back the trial slot of a call admitted in generation gen
// that will never report. A later generation has its own trial count, which
// that call never entered. It runs while a callback's panic unwinds, so it
// leaves queued state changes to a later call instead of running the
// callback again.
func (b *Breaker) freeTrial(gen uint64) {
	b.mu.Lock()
	if b.generation == gen {
		b.trials--
	}
	b.mu.Unlock()
}

// halfOpenIfDue applies the lazy open → half-open change. Callers hold b.mu.
func (b *Breaker) halfOpenIfDue() {
	if b.state != Open {
		return
	}
	if now := b.now(); now.Sub(b.openedAt) >= b.settings.OpenDuration {
		b.setState(HalfOpen, now)
	}
}

// setState starts a new generation in state to, with nothing recorded yet,
// and queues the change for onChange. Callers hold b.mu.
func (b *Breaker) setState(to State, now time.Time) {
	if b.onChange != nil {
		b.pending = append(b.pending, transition{from: b.state, to: to})
	}
	b.state = to
	b.generation++
	b.trials, b.successes = 0, 0
	b.window.reset(b.settings.Window, now)
	if to == Open {
		b.openedAt = now
	}
}

// unlock releases b.mu, which the caller holds. If state changes are queued
// and no goroutine is delivering them yet, it first passes them to onChange
// one at a time, releasing b.mu around each call; changes queued meanwhile,
// by the callback itself or by other goroutines, are delivered by the same
// loop, which keeps callbacks ordered and never nested.
func (b *Breaker) unlock() {
	if b.delivering || len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.delivering = true
	finished := false
	defer func() {
		if !finished {
			// onChange panicked: let a later call deliver what is left.
			b.mu.Lock()
			b.delivering = false
			b.mu.Unlock()
		}
	}()
	for len(b.pending) > 0 {
		t := b.pending[0]
		b.pending = b.pending[1:]
		b.mu.Unlock()
		b.onChange(t.from, t.to)
		b.mu.Lock()
	}
	b.pending, b.delivering = nil, false
	finished = true
	b.mu.Unlock()
}

func saturate(n uint64) uint32 {
	return uint32(min(n, math.MaxUint32))
}
