package breaker

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Policy configures the breaker for one key, such as a downstream service.
type Policy struct {
	Key string
	// Enabled false has the same effect as leaving the policy out.
	Enabled  bool
	Settings Settings
}

// SetOption configures a Set.
type SetOption func(*setOptions)

type setOptions struct {
	now      func() time.Time
	onChange func(key string, from, to State)
}

// WithSetClock makes every breaker in the set read the time from now instead
// of time.Now.
func WithSetClock(now func() time.Time) SetOption {
	return func(o *setOptions) { o.now = now }
}

// WithSetStateChange calls fn with the breaker's key after every state change
// of a breaker in the set, as WithStateChange does for one breaker: calls for
// one breaker never overlap and arrive in order, but calls for different
// breakers may run concurrently. Once Update has dropped a breaker, fn is no
// longer called for it, even for a change it made before but had yet to
// report. A key that is dropped and added again gets a new breaker, so a call
// for the old one that is still running may overlap those for the new one.
func WithSetStateChange(fn func(key string, from, to State)) SetOption {
	return func(o *setOptions) { o.onChange = fn }
}

// Set keeps one breaker per enabled policy and is reconfigured as a whole
// with Update, e.g. whenever a new snapshot arrives. Lookups load an
// immutable map through an atomic pointer, so Allow and Do never wait for
// Update. Safe for concurrent use; create one with NewSet.
type Set struct {
	opts setOptions
	// mu serializes Update, which replaces the map instead of changing it.
	mu       sync.Mutex
	breakers atomic.Pointer[map[string]*Breaker]
}

// NewSet returns an empty set.
func NewSet(opts ...SetOption) *Set {
	s := &Set{}
	for _, opt := range opts {
		opt(&s.opts)
	}
	s.breakers.Store(&map[string]*Breaker{})
	return s
}

// Update makes the set match policies. A key that is new or was dropped
// before gets a new closed breaker; an existing breaker is reconfigured in
// place by Breaker.Update, keeping its state; keys that are missing or
// disabled are dropped. When a key appears more than once, its last policy
// wins. State-change callbacks caused by the update run after Update has
// released its lock, so they may call Update themselves.
func (s *Set) Update(policies []Policy) {
	latest := make(map[string]Policy, len(policies))
	for _, p := range policies {
		latest[p.Key] = p
	}

	s.mu.Lock()
	cur := *s.breakers.Load()
	next := make(map[string]*Breaker, len(latest))
	var kept []*Breaker
	for key, p := range latest {
		if !p.Enabled {
			continue
		}
		if b, ok := cur[key]; ok {
			b.apply(p.Settings)
			next[key] = b
			kept = append(kept, b)
			continue
		}
		next[key] = s.newBreaker(key, p.Settings)
	}
	s.breakers.Store(&next)
	s.mu.Unlock()

	for _, b := range kept {
		b.flush()
	}
}

func (s *Set) newBreaker(key string, settings Settings) *Breaker {
	opts := []Option{WithClock(s.opts.now)}
	var b *Breaker
	if fn := s.opts.onChange; fn != nil {
		opts = append(opts, WithStateChange(func(from, to State) {
			// Calls admitted before Update dropped b can still change its
			// state, but the key's policy is gone or belongs to a new
			// breaker now. b is set before Update publishes it, and only
			// calls made after that can change its state.
			if cur, ok := s.Breaker(key); ok && cur == b {
				fn(key, from, to)
			}
		}))
	}
	b = New(settings, opts...)
	return b
}

// Breaker returns the breaker for key, if the set has one. A breaker that a
// later Update drops keeps working on its own, but it is no longer in the set
// and its state changes are no longer reported (see WithSetStateChange).
func (s *Set) Breaker(key string) (*Breaker, bool) {
	b, ok := (*s.breakers.Load())[key]
	return b, ok
}

// Allow is Breaker.Allow on the breaker for key. A key without a breaker
// allows every call, and done then does nothing.
func (s *Set) Allow(key string) (done func(success bool), err error) {
	if b, ok := s.Breaker(key); ok {
		return b.Allow()
	}
	return noop, nil
}

// Do is Breaker.Do on the breaker for key. For a key without a breaker it
// just calls fn.
func (s *Set) Do(key string, fn func() error) error {
	if b, ok := s.Breaker(key); ok {
		return b.Do(fn)
	}
	return fn()
}

// Keys returns the keys that have a breaker, sorted.
func (s *Set) Keys() []string {
	return slices.Sorted(maps.Keys(*s.breakers.Load()))
}
