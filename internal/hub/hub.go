// Package hub fans namespace snapshots out to watchers on one replica.
//
// The hub caches the latest snapshot of every namespace that has at least one
// watcher. When told a namespace reached a newer revision it loads the
// snapshot once and offers it to every subscriber. Each subscriber has a
// latest-wins mailbox: a slow consumer only ever skips intermediate
// revisions, it never blocks the hub or other subscribers and never buffers
// more than one snapshot.
package hub

import (
	"context"
	"errors"
	"sync"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// ErrClosed is returned once the hub or a subscription has been closed.
var ErrClosed = errors.New("hub closed")

// Loader fetches the current snapshot of a namespace. Snapshots handed to the
// hub are shared between subscribers and must not be modified afterwards.
type Loader func(ctx context.Context, namespace string) (*cpv1.Snapshot, error)

// Hub is safe for concurrent use.
type Hub struct {
	load Loader

	mu     sync.Mutex
	topics map[string]*topic
	closed bool
}

type topic struct {
	name string
	// loadMu serializes snapshot loads so a burst of notifications for one
	// namespace results in as few loads as possible.
	loadMu sync.Mutex

	// Guarded by Hub.mu.
	current *cpv1.Snapshot
	subs    map[*Subscription]struct{}
}

// New returns a hub that loads snapshots with load.
func New(load Loader) *Hub {
	return &Hub{load: load, topics: make(map[string]*topic)}
}

// Subscribe registers a watcher for namespace. The current snapshot is
// delivered first unless its revision is <= knownRevision. Returns the
// loader's error if the namespace cannot be loaded.
func (h *Hub) Subscribe(ctx context.Context, namespace string, knownRevision int64) (*Subscription, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrClosed
	}
	t, ok := h.topics[namespace]
	if !ok {
		t = &topic{name: namespace, subs: make(map[*Subscription]struct{})}
		h.topics[namespace] = t
	}
	sub := &Subscription{hub: h, topic: t, notify: make(chan struct{}, 1), last: knownRevision}
	// Register before loading so no notification can slip between the load
	// and the registration.
	t.subs[sub] = struct{}{}
	h.mu.Unlock()

	snap, err := h.refresh(ctx, t, 0)
	if err != nil {
		sub.Close()
		return nil, err
	}
	sub.offer(snap)
	return sub, nil
}

// Notify tells the hub that namespace has reached revision. If anyone is
// watching and the cached snapshot is older, a fresh snapshot is loaded and
// fanned out. Cheap when nobody watches the namespace or it is up to date.
func (h *Hub) Notify(ctx context.Context, namespace string, revision int64) error {
	h.mu.Lock()
	t, ok := h.topics[namespace]
	stale := ok && (t.current == nil || t.current.GetRevision() < revision)
	h.mu.Unlock()
	if !stale {
		return nil
	}
	_, err := h.refresh(ctx, t, revision)
	return err
}

// Revisions returns the cached revision of every watched namespace.
func (h *Hub) Revisions() map[string]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int64, len(h.topics))
	for name, t := range h.topics {
		out[name] = t.current.GetRevision()
	}
	return out
}

// Watchers returns the number of active subscriptions.
func (h *Hub) Watchers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, t := range h.topics {
		n += len(t.subs)
	}
	return n
}

// Close ends every subscription; Next returns ErrClosed. Further Subscribe
// calls fail with ErrClosed.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var subs []*Subscription
	for _, t := range h.topics {
		for s := range t.subs {
			subs = append(subs, s)
		}
	}
	h.topics = make(map[string]*topic)
	h.mu.Unlock()

	for _, s := range subs {
		s.close()
	}
}

// refresh makes sure t holds a snapshot at revision >= minRevision (or any
// snapshot when minRevision is 0), loading it if needed, and offers a newly
// loaded snapshot to all subscribers.
func (h *Hub) refresh(ctx context.Context, t *topic, minRevision int64) (*cpv1.Snapshot, error) {
	t.loadMu.Lock()
	defer t.loadMu.Unlock()

	h.mu.Lock()
	cur := t.current
	h.mu.Unlock()
	if cur != nil && cur.GetRevision() >= minRevision {
		return cur, nil // another caller loaded it while we waited
	}

	snap, err := h.load(ctx, t.name)
	if err != nil {
		return nil, err
	}

	h.mu.Lock()
	if t.current == nil || snap.GetRevision() > t.current.GetRevision() {
		t.current = snap
	}
	snap = t.current
	subs := make([]*Subscription, 0, len(t.subs))
	for s := range t.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()

	for _, s := range subs {
		s.offer(snap)
	}
	return snap, nil
}

func (h *Hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t := s.topic
	delete(t.subs, s)
	// Drop the cache with the last watcher; the next Subscribe reloads.
	if len(t.subs) == 0 && h.topics[t.name] == t {
		delete(h.topics, t.name)
	}
}

// Subscription receives snapshots of one namespace in increasing revision order.
type Subscription struct {
	hub    *Hub
	topic  *topic
	notify chan struct{}

	mu      sync.Mutex
	pending *cpv1.Snapshot
	last    int64 // highest revision offered so far
	closed  bool
}

// offer replaces the pending snapshot if snap is newer than anything this
// subscriber has seen. Never blocks.
func (s *Subscription) offer(snap *cpv1.Snapshot) {
	s.mu.Lock()
	if s.closed || snap.GetRevision() <= s.last {
		s.mu.Unlock()
		return
	}
	s.pending, s.last = snap, snap.GetRevision()
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Next blocks until a newer snapshot is available, ctx is done, or the
// subscription is closed.
func (s *Subscription) Next(ctx context.Context) (*cpv1.Snapshot, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if p := s.pending; p != nil {
			s.pending = nil
			s.mu.Unlock()
			return p, nil
		}
		s.mu.Unlock()

		select {
		case <-s.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close unregisters the subscription. Safe to call more than once.
func (s *Subscription) Close() {
	if s.close() {
		s.hub.unsubscribe(s)
	}
}

func (s *Subscription) close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed, s.pending = true, nil
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return true
}
