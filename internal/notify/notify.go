// Package notify broadcasts "namespace X reached revision N" events between
// control plane replicas so each replica can refresh its watchers.
//
// Delivery is best effort. A replica that misses an event catches up through
// the periodic reconciler, so events carry only the revision, never the data.
package notify

import (
	"context"
	"sync"
)

// Event announces a new namespace revision.
type Event struct {
	Namespace string `json:"ns"`
	Revision  int64  `json:"rev"`
}

// Handler is called for every received event. It must not block for long.
type Handler func(Event)

// Notifier publishes events to every replica, including the sender.
type Notifier interface {
	Publish(ctx context.Context, ev Event) error
	// Run delivers events to h until ctx is done. It returns nil on
	// cancellation and an error if the subscription cannot be established.
	Run(ctx context.Context, h Handler) error
}

// localBuffer bounds each local subscriber's queue. Overflow drops events,
// which the reconciler repairs.
const localBuffer = 256

// Local is an in-process Notifier. It is used when the control plane runs as a
// single replica, and in tests to connect several servers in one process.
type Local struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewLocal returns an empty in-process bus.
func NewLocal() *Local {
	return &Local{subs: make(map[chan Event]struct{})}
}

// Publish never blocks.
func (l *Local) Publish(_ context.Context, ev Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs {
		select {
		case ch <- ev:
		default:
		}
	}
	return nil
}

func (l *Local) Run(ctx context.Context, h Handler) error {
	ch := make(chan Event, localBuffer)
	l.mu.Lock()
	l.subs[ch] = struct{}{}
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.subs, ch)
		l.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			h(ev)
		}
	}
}
