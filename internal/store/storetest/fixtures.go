package storetest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// writer performs setup writes in one namespace and fails the test on error.
type writer struct {
	t    *testing.T
	s    store.Store
	ns   string
	opts store.WriteOptions
}

func newWriter(t *testing.T, s store.Store, ns, actor string) writer {
	return writer{t: t, s: s, ns: ns, opts: store.WriteOptions{Actor: actor}}
}

// as returns a writer acting as actor.
func (w writer) as(actor string) writer {
	w.opts.Actor = actor
	return w
}

func (w writer) config(key, value string) model.Config {
	w.t.Helper()
	c, err := w.s.PutConfig(context.Background(), w.ns, model.Config{Key: key, Value: json.RawMessage(value)}, w.opts)
	if err != nil {
		w.t.Fatalf("PutConfig(%q, %q): %v", w.ns, key, err)
	}
	return c
}

func (w writer) flag(f model.Flag) model.Flag {
	w.t.Helper()
	out, err := w.s.PutFlag(context.Background(), w.ns, f, w.opts)
	if err != nil {
		w.t.Fatalf("PutFlag(%q, %q): %v", w.ns, f.Key, err)
	}
	return out
}

func (w writer) experiment(e model.Experiment) model.Experiment {
	w.t.Helper()
	out, err := w.s.PutExperiment(context.Background(), w.ns, e, w.opts)
	if err != nil {
		w.t.Fatalf("PutExperiment(%q, %q): %v", w.ns, e.Key, err)
	}
	return out
}

func (w writer) rateLimit(r model.RateLimit) model.RateLimit {
	w.t.Helper()
	out, err := w.s.PutRateLimit(context.Background(), w.ns, r, w.opts)
	if err != nil {
		w.t.Fatalf("PutRateLimit(%q, %q): %v", w.ns, r.Key, err)
	}
	return out
}

func (w writer) breaker(c model.CircuitBreaker) model.CircuitBreaker {
	w.t.Helper()
	out, err := w.s.PutCircuitBreaker(context.Background(), w.ns, c, w.opts)
	if err != nil {
		w.t.Fatalf("PutCircuitBreaker(%q, %q): %v", w.ns, c.Key, err)
	}
	return out
}

// del deletes an entry of any type and returns the new namespace revision.
func (w writer) del(entity, key string) int64 {
	w.t.Helper()
	rev, err := kindOf(w.t, entity).del(w.s, w.ns, key, w.opts)
	if err != nil {
		w.t.Fatalf("delete %s %q in %q: %v", entity, key, w.ns, err)
	}
	return rev
}

func newFlag(key string, percent float64) model.Flag {
	return model.Flag{Key: key, Enabled: true, RolloutPercent: percent, Salt: key}
}

// rolloutStart is when the test rollout plans started. It has nanoseconds,
// which a store must not lose.
var rolloutStart = time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)

// rolloutFlag returns a flag with a three-stage rollout plan (1%, 25%, 100%)
// in state at stage, with the rollout percentage the plan implies.
func rolloutFlag(key string, state model.RolloutState, stage int) model.Flag {
	plan := &model.RolloutPlan{
		Stages: []model.RolloutStage{
			{Percent: 1, Duration: 10*time.Minute + time.Nanosecond},
			{Percent: 25, Duration: time.Hour},
			{Percent: 100},
		},
		CurrentStage:   stage,
		State:          state,
		StartedAt:      rolloutStart,
		StageStartedAt: rolloutStart.Add(time.Duration(stage)*time.Hour + 7),
		StartedBy:      "alice",
	}
	f := newFlag(key, plan.Stages[stage].Percent)
	if state == model.RolloutAborted {
		f.RolloutPercent = 0
	}
	f.Rollout = plan
	return f
}

// newExperiment returns an experiment with one variant per weight; all but
// the first carry a payload.
func newExperiment(key string, weights ...uint32) model.Experiment {
	e := model.Experiment{Key: key, Enabled: true, Salt: key + "-salt"}
	for i, w := range weights {
		v := model.Variant{Name: fmt.Sprintf("v%d", i), Weight: w}
		if i > 0 {
			v.Payload = json.RawMessage(fmt.Sprintf(`{"variant":%d,"color":"green"}`, i))
		}
		e.Variants = append(e.Variants, v)
	}
	return e
}

func newRateLimit(key string, rps float64, burst uint32) model.RateLimit {
	return model.RateLimit{Key: key, Enabled: true, RequestsPerSecond: rps, Burst: burst}
}

func newBreaker(key string, minRequests uint32) model.CircuitBreaker {
	return model.CircuitBreaker{
		Key: key, Enabled: true, FailureRateThreshold: 0.5, MinRequests: minRequests,
		Window: 10 * time.Second, OpenDuration: 30 * time.Second, HalfOpenMaxRequests: 3,
	}
}

// entityKind adapts one entity type for the tests that cover all of them.
type entityKind struct {
	entity string
	// put writes the entry key with content that depends on version (1 to
	// 100) and returns the stored entry's revision and image.
	put func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error)
	// rewrite is put as a read-modify-write: it reads the stored entry
	// (GetFlag for flags, Snapshot for the others), sets the content put
	// would for version and writes it back with the revision metadata it
	// read, which the store must replace.
	rewrite func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error)
	del     func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error)
	// keys lists the snapshot's entries of this type.
	keys func(model.Snapshot) []string
}

var entityKinds = []entityKind{
	{
		entity: model.EntityConfig,
		put: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			out, err := s.PutConfig(context.Background(), ns, model.Config{Key: key, Value: configValue(version)}, opts)
			return written(out.Revision, out, err)
		},
		rewrite: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			c, err := stored(s, ns, key, func(snap model.Snapshot) []model.Config { return snap.Configs }, func(v model.Config) string { return v.Key })
			if err != nil {
				return 0, nil, err
			}
			c.Value = configValue(version)
			out, err := s.PutConfig(context.Background(), ns, c, opts)
			return written(out.Revision, out, err)
		},
		del: func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error) {
			return s.DeleteConfig(context.Background(), ns, key, opts)
		},
		keys: func(snap model.Snapshot) []string {
			return keysOf(snap.Configs, func(v model.Config) string { return v.Key })
		},
	},
	{
		entity: model.EntityFlag,
		put: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			out, err := s.PutFlag(context.Background(), ns, newFlag(key, float64(version)), opts)
			return written(out.Revision, out, err)
		},
		rewrite: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			f, err := s.GetFlag(context.Background(), ns, key)
			if err != nil {
				return 0, nil, err
			}
			f.RolloutPercent = float64(version)
			out, err := s.PutFlag(context.Background(), ns, f, opts)
			return written(out.Revision, out, err)
		},
		del: func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error) {
			return s.DeleteFlag(context.Background(), ns, key, opts)
		},
		keys: func(snap model.Snapshot) []string {
			return keysOf(snap.Flags, func(v model.Flag) string { return v.Key })
		},
	},
	{
		entity: model.EntityExperiment,
		put: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			out, err := s.PutExperiment(context.Background(), ns, newExperiment(key, uint32(version), 1), opts)
			return written(out.Revision, out, err)
		},
		rewrite: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			e, err := stored(s, ns, key, func(snap model.Snapshot) []model.Experiment { return snap.Experiments }, func(v model.Experiment) string { return v.Key })
			if err != nil {
				return 0, nil, err
			}
			e.Variants[0].Weight = uint32(version)
			out, err := s.PutExperiment(context.Background(), ns, e, opts)
			return written(out.Revision, out, err)
		},
		del: func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error) {
			return s.DeleteExperiment(context.Background(), ns, key, opts)
		},
		keys: func(snap model.Snapshot) []string {
			return keysOf(snap.Experiments, func(v model.Experiment) string { return v.Key })
		},
	},
	{
		entity: model.EntityRateLimit,
		put: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			out, err := s.PutRateLimit(context.Background(), ns, newRateLimit(key, float64(version)/2, uint32(version)), opts)
			return written(out.Revision, out, err)
		},
		rewrite: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			r, err := stored(s, ns, key, func(snap model.Snapshot) []model.RateLimit { return snap.RateLimits }, func(v model.RateLimit) string { return v.Key })
			if err != nil {
				return 0, nil, err
			}
			r.RequestsPerSecond, r.Burst = float64(version)/2, uint32(version)
			out, err := s.PutRateLimit(context.Background(), ns, r, opts)
			return written(out.Revision, out, err)
		},
		del: func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error) {
			return s.DeleteRateLimit(context.Background(), ns, key, opts)
		},
		keys: func(snap model.Snapshot) []string {
			return keysOf(snap.RateLimits, func(v model.RateLimit) string { return v.Key })
		},
	},
	{
		entity: model.EntityCircuitBreaker,
		put: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			out, err := s.PutCircuitBreaker(context.Background(), ns, newBreaker(key, uint32(version)), opts)
			return written(out.Revision, out, err)
		},
		rewrite: func(s store.Store, ns, key string, version int, opts store.WriteOptions) (int64, json.RawMessage, error) {
			c, err := stored(s, ns, key, func(snap model.Snapshot) []model.CircuitBreaker { return snap.CircuitBreakers }, func(v model.CircuitBreaker) string { return v.Key })
			if err != nil {
				return 0, nil, err
			}
			c.MinRequests = uint32(version)
			out, err := s.PutCircuitBreaker(context.Background(), ns, c, opts)
			return written(out.Revision, out, err)
		},
		del: func(s store.Store, ns, key string, opts store.WriteOptions) (int64, error) {
			return s.DeleteCircuitBreaker(context.Background(), ns, key, opts)
		},
		keys: func(snap model.Snapshot) []string {
			return keysOf(snap.CircuitBreakers, func(v model.CircuitBreaker) string { return v.Key })
		},
	},
}

func configValue(version int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"version":%d,"on":true}`, version))
}

// stored returns the entry key from one of the snapshot's lists, as the
// store hands it out: revision metadata included.
func stored[T any](s store.Store, ns, key string, list func(model.Snapshot) []T, keyOf func(T) string) (T, error) {
	var zero T
	snap, err := s.Snapshot(context.Background(), ns)
	if err != nil {
		return zero, err
	}
	for _, v := range list(snap) {
		if keyOf(v) == key {
			return v, nil
		}
	}
	return zero, fmt.Errorf("%q is not in the snapshot of %q", key, ns)
}

func kindOf(t *testing.T, entity string) entityKind {
	t.Helper()
	for _, k := range entityKinds {
		if k.entity == entity {
			return k
		}
	}
	t.Fatalf("unknown entity type %q", entity)
	return entityKind{}
}

// written returns the revision and image of an entry a Put returned.
func written(rev int64, v any, err error) (int64, json.RawMessage, error) {
	if err != nil {
		return 0, nil, err
	}
	img, err := model.EncodeEntry(v)
	return rev, img, err
}

func keysOf[T any](entries []T, key func(T) string) []string {
	out := make([]string, len(entries))
	for i, v := range entries {
		out[i] = key(v)
	}
	return out
}
