// Package memory is an in-process store.Store used by tests and by
// `controlplane --store memory` for local development. State is lost on exit.
package memory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

var _ store.Store = (*Store)(nil)

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

type nsState struct {
	meta        model.Namespace
	configs     map[string]model.Config
	flags       map[string]model.Flag
	experiments map[string]model.Experiment
	rateLimits  map[string]model.RateLimit
	breakers    map[string]model.CircuitBreaker
	// history[i] is revision i+1.
	history []model.Revision
}

// Store keeps all state in maps guarded by one mutex.
type Store struct {
	mu         sync.RWMutex
	namespaces map[string]*nsState
	audit      []model.AuditEvent // audit[i] has ID i+1
	now        func() time.Time
}

// New returns an empty store.
func New() *Store {
	return &Store{
		namespaces: make(map[string]*nsState),
		now:        func() time.Time { return time.Now().UTC() },
	}
}

// kind describes one entity type so writes can be implemented once.
type kind[T any] struct {
	entity   string
	table    func(*nsState) map[string]T
	clone    func(T) T
	stamp    func(v *T, rev int64, at time.Time, by string)
	revision func(T) int64
}

var (
	configKind = kind[model.Config]{
		entity: model.EntityConfig,
		table:  func(n *nsState) map[string]model.Config { return n.configs },
		clone:  model.Config.Clone,
		stamp: func(v *model.Config, rev int64, at time.Time, by string) {
			v.Revision, v.UpdatedAt, v.UpdatedBy = rev, at, by
		},
		revision: func(v model.Config) int64 { return v.Revision },
	}
	flagKind = kind[model.Flag]{
		entity: model.EntityFlag,
		table:  func(n *nsState) map[string]model.Flag { return n.flags },
		clone:  model.Flag.Clone,
		stamp: func(v *model.Flag, rev int64, at time.Time, by string) {
			v.Revision, v.UpdatedAt, v.UpdatedBy = rev, at, by
		},
		revision: func(v model.Flag) int64 { return v.Revision },
	}
	experimentKind = kind[model.Experiment]{
		entity: model.EntityExperiment,
		table:  func(n *nsState) map[string]model.Experiment { return n.experiments },
		clone:  model.Experiment.Clone,
		stamp: func(v *model.Experiment, rev int64, at time.Time, by string) {
			v.Revision, v.UpdatedAt, v.UpdatedBy = rev, at, by
		},
		revision: func(v model.Experiment) int64 { return v.Revision },
	}
	rateLimitKind = kind[model.RateLimit]{
		entity: model.EntityRateLimit,
		table:  func(n *nsState) map[string]model.RateLimit { return n.rateLimits },
		clone:  func(v model.RateLimit) model.RateLimit { return v },
		stamp: func(v *model.RateLimit, rev int64, at time.Time, by string) {
			v.Revision, v.UpdatedAt, v.UpdatedBy = rev, at, by
		},
		revision: func(v model.RateLimit) int64 { return v.Revision },
	}
	breakerKind = kind[model.CircuitBreaker]{
		entity: model.EntityCircuitBreaker,
		table:  func(n *nsState) map[string]model.CircuitBreaker { return n.breakers },
		clone:  func(v model.CircuitBreaker) model.CircuitBreaker { return v },
		stamp: func(v *model.CircuitBreaker, rev int64, at time.Time, by string) {
			v.Revision, v.UpdatedAt, v.UpdatedBy = rev, at, by
		},
		revision: func(v model.CircuitBreaker) int64 { return v.Revision },
	}
)

// event is one entity change about to be recorded in the audit log.
type event struct {
	entity, key, action string
	before, after       any // entity values; nil when absent
}

func (s *Store) CreateNamespace(_ context.Context, ns model.Namespace, opts store.WriteOptions) (model.Namespace, error) {
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
		CreatedBy:   opts.Actor,
	}
	state := &nsState{
		meta:        meta,
		configs:     make(map[string]model.Config),
		flags:       make(map[string]model.Flag),
		experiments: make(map[string]model.Experiment),
		rateLimits:  make(map[string]model.RateLimit),
		breakers:    make(map[string]model.CircuitBreaker),
	}
	if err := s.commit(state, opts, "create namespace", []event{{
		entity: model.EntityNamespace, key: ns.Name, action: model.ActionCreate, after: meta,
	}}); err != nil {
		return model.Namespace{}, err
	}
	s.namespaces[ns.Name] = state
	return meta, nil
}

func (s *Store) GetNamespace(_ context.Context, name string) (model.Namespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(name)
	if err != nil {
		return model.Namespace{}, err
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

func (s *Store) lookup(name string) (*nsState, error) {
	ns, ok := s.namespaces[name]
	if !ok {
		return nil, model.NotFoundf("namespace %q", name)
	}
	return ns, nil
}

func (s *Store) PutConfig(_ context.Context, namespace string, c model.Config, opts store.WriteOptions) (model.Config, error) {
	return put(s, configKind, namespace, c.Key, c, opts)
}

func (s *Store) PutFlag(_ context.Context, namespace string, f model.Flag, opts store.WriteOptions) (model.Flag, error) {
	return put(s, flagKind, namespace, f.Key, f, opts)
}

func (s *Store) PutExperiment(_ context.Context, namespace string, e model.Experiment, opts store.WriteOptions) (model.Experiment, error) {
	return put(s, experimentKind, namespace, e.Key, e, opts)
}

func (s *Store) PutRateLimit(_ context.Context, namespace string, r model.RateLimit, opts store.WriteOptions) (model.RateLimit, error) {
	return put(s, rateLimitKind, namespace, r.Key, r, opts)
}

func (s *Store) PutCircuitBreaker(_ context.Context, namespace string, c model.CircuitBreaker, opts store.WriteOptions) (model.CircuitBreaker, error) {
	return put(s, breakerKind, namespace, c.Key, c, opts)
}

func (s *Store) DeleteConfig(_ context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(s, configKind, namespace, key, opts)
}

func (s *Store) DeleteFlag(_ context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(s, flagKind, namespace, key, opts)
}

func (s *Store) DeleteExperiment(_ context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(s, experimentKind, namespace, key, opts)
}

func (s *Store) DeleteRateLimit(_ context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(s, rateLimitKind, namespace, key, opts)
}

func (s *Store) DeleteCircuitBreaker(_ context.Context, namespace, key string, opts store.WriteOptions) (int64, error) {
	return remove(s, breakerKind, namespace, key, opts)
}

func put[T any](s *Store, k kind[T], namespace, key string, v T, opts store.WriteOptions) (T, error) {
	var zero T
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return zero, err
	}
	table := k.table(ns)
	old, existed := table[key]
	if opts.ExpectedRevision != 0 && (!existed || k.revision(old) != opts.ExpectedRevision) {
		return zero, model.Conflictf("%s %q in namespace %q is not at revision %d", k.entity, key, namespace, opts.ExpectedRevision)
	}

	rev, at := ns.meta.Revision+1, s.now()
	v = k.clone(v)
	k.stamp(&v, rev, at, opts.Actor)
	ev := event{entity: k.entity, key: key, action: model.ActionCreate, after: v}
	if existed {
		ev.action, ev.before = model.ActionUpdate, old
	}
	if err := s.commitAt(ns, rev, at, opts, "", []event{ev}, func() { table[key] = v }); err != nil {
		return zero, err
	}
	return k.clone(v), nil
}

func remove[T any](s *Store, k kind[T], namespace, key string, opts store.WriteOptions) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return 0, err
	}
	table := k.table(ns)
	old, existed := table[key]
	if !existed {
		return 0, model.NotFoundf("%s %q in namespace %q", k.entity, key, namespace)
	}
	if opts.ExpectedRevision != 0 && k.revision(old) != opts.ExpectedRevision {
		return 0, model.Conflictf("%s %q in namespace %q is not at revision %d", k.entity, key, namespace, opts.ExpectedRevision)
	}
	rev, at := ns.meta.Revision+1, s.now()
	ev := event{entity: k.entity, key: key, action: model.ActionDelete, before: old}
	if err := s.commitAt(ns, rev, at, opts, "", []event{ev}, func() { delete(table, key) }); err != nil {
		return 0, err
	}
	return rev, nil
}

// commit records the namespace's current revision (used on creation).
func (s *Store) commit(ns *nsState, opts store.WriteOptions, summary string, events []event) error {
	return s.commitAt(ns, ns.meta.Revision, ns.meta.UpdatedAt, opts, summary, events, func() {})
}

// commitAt encodes the audit images first, so nothing changes if encoding
// fails, then applies the mutation, moves the namespace to rev and appends
// the audit events and the history entry. Callers hold s.mu for writing.
func (s *Store) commitAt(ns *nsState, rev int64, at time.Time, opts store.WriteOptions, summary string, events []event, apply func()) error {
	audit := make([]model.AuditEvent, 0, len(events))
	for _, e := range events {
		ae := model.AuditEvent{
			Namespace:  ns.meta.Name,
			Revision:   rev,
			Actor:      opts.Actor,
			Time:       at,
			Action:     e.action,
			EntityType: e.entity,
			EntityKey:  e.key,
			Message:    opts.Message,
		}
		if opts.Action != "" && e.action != model.ActionRollback {
			ae.Action = opts.Action
		}
		var err error
		if e.before != nil {
			if ae.Before, err = model.EncodeEntry(e.before); err != nil {
				return fmt.Errorf("encode audit image: %w", err)
			}
		}
		if e.after != nil {
			if ae.After, err = model.EncodeEntry(e.after); err != nil {
				return fmt.Errorf("encode audit image: %w", err)
			}
		}
		audit = append(audit, ae)
	}
	switch {
	case opts.Message != "":
		summary = opts.Message
	case summary == "" && len(audit) == 1:
		summary = fmt.Sprintf("%s %s %s", audit[0].Action, audit[0].EntityType, audit[0].EntityKey)
	}

	apply()
	ns.meta.Revision, ns.meta.UpdatedAt = rev, at
	for _, ae := range audit {
		ae.ID = int64(len(s.audit) + 1)
		s.audit = append(s.audit, ae)
	}
	ns.history = append(ns.history, model.Revision{
		Namespace: ns.meta.Name,
		Revision:  rev,
		Actor:     opts.Actor,
		CreatedAt: at,
		Summary:   summary,
		Snapshot:  snapshotOf(ns),
	})
	return nil
}

func (s *Store) GetFlag(_ context.Context, namespace, key string) (model.Flag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Flag{}, err
	}
	f, ok := ns.flags[key]
	if !ok {
		return model.Flag{}, model.NotFoundf("flag %q in namespace %q", key, namespace)
	}
	return f.Clone(), nil
}

func (s *Store) Snapshot(_ context.Context, namespace string) (model.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Snapshot{}, err
	}
	return snapshotOf(ns), nil
}

// snapshotOf returns a deep copy of the namespace state with sorted lists.
func snapshotOf(ns *nsState) model.Snapshot {
	return model.Snapshot{
		Namespace:       ns.meta.Name,
		Revision:        ns.meta.Revision,
		UpdatedAt:       ns.meta.UpdatedAt,
		Configs:         sortedValues(ns.configs, model.Config.Clone),
		Flags:           sortedValues(ns.flags, model.Flag.Clone),
		Experiments:     sortedValues(ns.experiments, model.Experiment.Clone),
		RateLimits:      sortedValues(ns.rateLimits, func(v model.RateLimit) model.RateLimit { return v }),
		CircuitBreakers: sortedValues(ns.breakers, func(v model.CircuitBreaker) model.CircuitBreaker { return v }),
	}
}

func sortedValues[T any](m map[string]T, clone func(T) T) []T {
	out := make([]T, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, clone(m[k]))
	}
	return out
}

func pageSize(limit int) int {
	if limit <= 0 {
		return defaultPageSize
	}
	return min(limit, maxPageSize)
}

func (s *Store) ListRevisions(_ context.Context, namespace string, beforeRevision int64, limit int) ([]model.Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return nil, err
	}
	limit = pageSize(limit)
	out := make([]model.Revision, 0, min(limit, len(ns.history)))
	for i := len(ns.history) - 1; i >= 0 && len(out) < limit; i-- {
		r := ns.history[i]
		if beforeRevision > 0 && r.Revision >= beforeRevision {
			continue
		}
		r.Snapshot = model.Snapshot{}
		out = append(out, r)
	}
	return out, nil
}

func (s *Store) GetRevision(_ context.Context, namespace string, revision int64) (model.Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return model.Revision{}, err
	}
	r, ok := findRevision(ns, revision)
	if !ok {
		return model.Revision{}, model.NotFoundf("revision %d of namespace %q", revision, namespace)
	}
	r.Snapshot = r.Snapshot.Clone()
	return r, nil
}

func findRevision(ns *nsState, revision int64) (model.Revision, bool) {
	if revision < 1 || revision > int64(len(ns.history)) {
		return model.Revision{}, false
	}
	return ns.history[revision-1], true
}

func (s *Store) Rollback(_ context.Context, namespace string, toRevision int64, opts store.WriteOptions) (int64, []model.Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.lookup(namespace)
	if err != nil {
		return 0, nil, err
	}
	if opts.ExpectedRevision != 0 && opts.ExpectedRevision != ns.meta.Revision {
		return 0, nil, model.Conflictf("namespace %q is at revision %d, not %d", namespace, ns.meta.Revision, opts.ExpectedRevision)
	}
	target, ok := findRevision(ns, toRevision)
	if !ok {
		return 0, nil, model.NotFoundf("revision %d of namespace %q", toRevision, namespace)
	}
	want := model.PrepareRollback(target.Snapshot)
	diff, err := model.Diff(snapshotOf(ns), want)
	if err != nil {
		return 0, nil, err
	}
	if len(diff) == 0 {
		return ns.meta.Revision, nil, nil
	}

	rev, at := ns.meta.Revision+1, s.now()
	index := indexSnapshot(want)
	var (
		events  []event
		applies []func()
	)
	for _, c := range diff {
		ev, apply := s.restore(ns, c, index, rev, at, opts.Actor)
		events = append(events, ev)
		applies = append(applies, apply)
	}
	if opts.Message == "" {
		opts.Message = fmt.Sprintf("rollback to revision %d", toRevision)
	}
	opts.Action = ""
	if err := s.commitAt(ns, rev, at, opts, "", events, func() {
		for _, apply := range applies {
			apply()
		}
	}); err != nil {
		return 0, nil, err
	}

	// Report the changes as written, with the new revision metadata. The
	// images are copies: the audit log must not change if the caller edits them.
	changes := make([]model.Change, len(events))
	for i, ev := range events {
		ae := s.audit[len(s.audit)-len(events)+i]
		changes[i] = model.Change{
			EntityType: ev.entity, Key: ev.key, Type: diff[i].Type,
			Before: slices.Clone(ae.Before), After: slices.Clone(ae.After),
		}
	}
	return rev, changes, nil
}

// snapshotIndex holds a snapshot's entries by entity type and key.
type snapshotIndex struct {
	configs     map[string]model.Config
	flags       map[string]model.Flag
	experiments map[string]model.Experiment
	rateLimits  map[string]model.RateLimit
	breakers    map[string]model.CircuitBreaker
}

func indexSnapshot(s model.Snapshot) snapshotIndex {
	idx := snapshotIndex{
		configs:     make(map[string]model.Config),
		flags:       make(map[string]model.Flag),
		experiments: make(map[string]model.Experiment),
		rateLimits:  make(map[string]model.RateLimit),
		breakers:    make(map[string]model.CircuitBreaker),
	}
	for _, v := range s.Configs {
		idx.configs[v.Key] = v
	}
	for _, v := range s.Flags {
		idx.flags[v.Key] = v
	}
	for _, v := range s.Experiments {
		idx.experiments[v.Key] = v
	}
	for _, v := range s.RateLimits {
		idx.rateLimits[v.Key] = v
	}
	for _, v := range s.CircuitBreakers {
		idx.breakers[v.Key] = v
	}
	return idx
}

// restore prepares the write that makes one entry match the rollback target.
func (s *Store) restore(ns *nsState, c model.Change, idx snapshotIndex, rev int64, at time.Time, actor string) (event, func()) {
	switch c.EntityType {
	case model.EntityConfig:
		return restoreEntry(ns, configKind, c, idx.configs, rev, at, actor)
	case model.EntityFlag:
		return restoreEntry(ns, flagKind, c, idx.flags, rev, at, actor)
	case model.EntityExperiment:
		return restoreEntry(ns, experimentKind, c, idx.experiments, rev, at, actor)
	case model.EntityRateLimit:
		return restoreEntry(ns, rateLimitKind, c, idx.rateLimits, rev, at, actor)
	default:
		return restoreEntry(ns, breakerKind, c, idx.breakers, rev, at, actor)
	}
}

func restoreEntry[T any](ns *nsState, k kind[T], c model.Change, target map[string]T, rev int64, at time.Time, actor string) (event, func()) {
	table := k.table(ns)
	ev := event{entity: k.entity, key: c.Key, action: model.ActionRollback}
	if old, ok := table[c.Key]; ok {
		ev.before = old
	}
	v, ok := target[c.Key]
	if !ok {
		return ev, func() { delete(table, c.Key) }
	}
	v = k.clone(v)
	k.stamp(&v, rev, at, actor)
	ev.after = v
	return ev, func() { table[c.Key] = v }
}

func (s *Store) ListAuditEvents(_ context.Context, f model.AuditFilter) ([]model.AuditEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit := pageSize(f.Limit)
	var out []model.AuditEvent
	for i := len(s.audit) - 1; i >= 0 && len(out) < limit; i-- {
		e := s.audit[i]
		switch {
		case f.BeforeID > 0 && e.ID >= f.BeforeID,
			f.Namespace != "" && e.Namespace != f.Namespace,
			f.EntityType != "" && e.EntityType != f.EntityType,
			f.EntityKey != "" && e.EntityKey != f.EntityKey,
			f.Actor != "" && e.Actor != f.Actor,
			!f.Since.IsZero() && e.Time.Before(f.Since),
			!f.Until.IsZero() && !e.Time.Before(f.Until):
			continue
		}
		e.Before, e.After = slices.Clone(e.Before), slices.Clone(e.After)
		out = append(out, e)
	}
	return out, nil
}

func (s *Store) ListActiveRollouts(context.Context) ([]model.RolloutRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []model.RolloutRef
	for _, name := range slices.Sorted(maps.Keys(s.namespaces)) {
		ns := s.namespaces[name]
		for _, key := range slices.Sorted(maps.Keys(ns.flags)) {
			if p := ns.flags[key].Rollout; p != nil && p.State == model.RolloutActive {
				out = append(out, model.RolloutRef{Namespace: name, Key: key})
			}
		}
	}
	return out, nil
}

func (s *Store) Ping(context.Context) error { return nil }

func (s *Store) Close() {}
