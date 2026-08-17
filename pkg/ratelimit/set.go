package ratelimit

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"
)

// Policy is one rate limit as configured in the control plane.
type Policy struct {
	// Key names what is limited, such as a route or a gRPC method.
	Key string
	// Enabled false is the same as having no policy: the key is unlimited.
	Enabled bool
	// RequestsPerSecond and Burst become the Limiter's rate and burst,
	// normalized as by NewLimiter.
	RequestsPerSecond float64
	Burst             int
}

// Set enforces a group of policies by key. It is safe for concurrent use.
// The zero value is an empty Set whose limiters use time.Now.
type Set struct {
	opts options

	// mu serializes Update; Allow and the other readers never take it.
	mu sync.Mutex
	// limiters is replaced wholesale by Update and never modified once
	// stored, so readers can use it without locking.
	limiters atomic.Pointer[map[string]*Limiter]
}

// NewSet returns an empty Set. opts apply to every Limiter it creates.
func NewSet(opts ...Option) *Set {
	return &Set{opts: applyOptions(opts)}
}

// Update makes the set enforce exactly the enabled policies. A new key gets a
// full bucket. An existing key keeps its Limiter, reconfigured in place with
// SetLimit, so the tokens it has saved carry over. Keys that are missing or
// disabled are removed; adding one back later starts it full again. If a key
// appears more than once, the last policy for it wins.
func (s *Set) Update(policies []Policy) {
	want := make(map[string]Policy, len(policies))
	for _, p := range policies {
		if p.Enabled {
			want[p.Key] = p
		} else {
			delete(want, p.Key)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.load()
	next := make(map[string]*Limiter, len(want))
	for key, p := range want {
		l, ok := current[key]
		if ok {
			l.SetLimit(p.RequestsPerSecond, p.Burst)
		} else {
			l = newLimiter(p.RequestsPerSecond, p.Burst, s.opts)
		}
		next[key] = l
	}
	s.limiters.Store(&next)
}

// Allow reports whether a request for key may proceed, taking a token from
// its Limiter if so. Keys without an enabled policy are always allowed.
func (s *Set) Allow(key string) bool {
	l, ok := s.load()[key]
	return !ok || l.Allow()
}

// Limiter returns the Limiter enforcing key, e.g. to turn its Delay into a
// Retry-After hint.
func (s *Set) Limiter(key string) (*Limiter, bool) {
	l, ok := s.load()[key]
	return l, ok
}

// Keys returns the keys of the enabled policies, sorted.
func (s *Set) Keys() []string {
	return slices.Sorted(maps.Keys(s.load()))
}

func (s *Set) load() map[string]*Limiter {
	if m := s.limiters.Load(); m != nil {
		return *m
	}
	return nil
}
