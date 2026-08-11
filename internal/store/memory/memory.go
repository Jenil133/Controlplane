// Package memory is an in-process store.Store used by tests and by
// `controlplane --store memory` for local development. State is lost on exit.
package memory

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

var _ store.Store = (*Store)(nil)

type nsState struct {
	meta        model.Namespace
	configs     map[string]model.Config
	flags       map[string]model.Flag
	experiments map[string]model.Experiment
}

// Store keeps all state in maps guarded by one mutex.
type Store struct {
	mu         sync.RWMutex
	namespaces map[string]*nsState
	now        func() time.Time
}

// New returns an empty store.
func New() *Store {
	return &Store{
		namespaces: make(map[string]*nsState),
		now:        func() time.Time { return time.Now().UTC() },
	}
}

func (s *Store) CreateNamespace(_ context.Context, ns model.Namespace) (model.Namespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.namespaces[ns.Name]; ok {
		return model.Namespace{}, model.AlreadyExistsf("namespace %q", ns.Name)
	}
	now := s.now()
	meta := model.Namespace{
		Name:        ns.Name,
		Description: ns.Description,
		Revision:    1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.namespaces[ns.Name] = &nsState{
		meta:        meta,
		configs:     make(map[string]model.Config),
		flags:       make(map[string]model.Flag),
		experiments: make(map[string]model.Experiment),
	}
	return meta, nil
}

func (s *Store) GetNamespace(_ context.Context, name string) (model.Namespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, ok := s.namespaces[name]
	if !ok {
		return model.Namespace{}, model.NotFoundf("namespace %q", name)
	}
	return ns.meta, nil
}

func (s *Store) ListNamespaces(context.Context) ([]model.Namespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Namespace, 0, len(s.namespaces))
	for _, ns := range s.namespaces {
		out = append(out, ns.meta)
	}
	slices.SortFunc(out, func(a, b model.Namespace) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

// bump advances the namespace revision. Callers hold s.mu for writing.
func (s *Store) bump(ns *nsState) (int64, time.Time) {
	ns.meta.Revision++
	ns.meta.UpdatedAt = s.now()
	return ns.meta.Revision, ns.meta.UpdatedAt
}

func (s *Store) lookup(name string) (*nsState, error) {
	ns, ok := s.namespaces[name]
	if !ok {
		return nil, model.NotFoundf("namespace %q", name)
	}
	return ns, nil
}

func (s *Store) PutConfig(_ context.Context, namespace string, c model.Config) (model.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Config{}, err
	}
	c = c.Clone()
	c.Revision, c.UpdatedAt = s.bump(ns)
	ns.configs[c.Key] = c
	return c.Clone(), nil
}

func (s *Store) PutFlag(_ context.Context, namespace string, f model.Flag) (model.Flag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Flag{}, err
	}
	f.Revision, f.UpdatedAt = s.bump(ns)
	ns.flags[f.Key] = f
	return f, nil
}

func (s *Store) PutExperiment(_ context.Context, namespace string, e model.Experiment) (model.Experiment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Experiment{}, err
	}
	e = e.Clone()
	e.Revision, e.UpdatedAt = s.bump(ns)
	ns.experiments[e.Key] = e
	return e.Clone(), nil
}

func (s *Store) DeleteConfig(_ context.Context, namespace, key string) (int64, error) {
	return s.remove(namespace, "config", key, func(ns *nsState) bool {
		_, ok := ns.configs[key]
		delete(ns.configs, key)
		return ok
	})
}

func (s *Store) DeleteFlag(_ context.Context, namespace, key string) (int64, error) {
	return s.remove(namespace, "flag", key, func(ns *nsState) bool {
		_, ok := ns.flags[key]
		delete(ns.flags, key)
		return ok
	})
}

func (s *Store) DeleteExperiment(_ context.Context, namespace, key string) (int64, error) {
	return s.remove(namespace, "experiment", key, func(ns *nsState) bool {
		_, ok := ns.experiments[key]
		delete(ns.experiments, key)
		return ok
	})
}

func (s *Store) remove(namespace, kind, key string, del func(*nsState) bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return 0, err
	}
	if !del(ns) {
		return 0, model.NotFoundf("%s %q in namespace %q", kind, key, namespace)
	}
	rev, _ := s.bump(ns)
	return rev, nil
}

func (s *Store) Snapshot(_ context.Context, namespace string) (model.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Snapshot{}, err
	}
	snap := model.Snapshot{
		Namespace:   ns.meta.Name,
		Revision:    ns.meta.Revision,
		UpdatedAt:   ns.meta.UpdatedAt,
		Configs:     make([]model.Config, 0, len(ns.configs)),
		Flags:       make([]model.Flag, 0, len(ns.flags)),
		Experiments: make([]model.Experiment, 0, len(ns.experiments)),
	}
	for _, k := range slices.Sorted(maps.Keys(ns.configs)) {
		snap.Configs = append(snap.Configs, ns.configs[k].Clone())
	}
	for _, k := range slices.Sorted(maps.Keys(ns.flags)) {
		snap.Flags = append(snap.Flags, ns.flags[k])
	}
	for _, k := range slices.Sorted(maps.Keys(ns.experiments)) {
		snap.Experiments = append(snap.Experiments, ns.experiments[k].Clone())
	}
	return snap, nil
}

func (s *Store) Ping(context.Context) error { return nil }

func (s *Store) Close() {}
