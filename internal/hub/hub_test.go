package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

var errMissing = errors.New("missing namespace")

// fakeSource stands in for the store: it holds a revision per namespace and
// counts loads.
type fakeSource struct {
	mu    sync.Mutex
	revs  map[string]int64
	loads int
}

func newFakeSource(revs map[string]int64) *fakeSource {
	return &fakeSource{revs: revs}
}

func (f *fakeSource) load(_ context.Context, ns string) (*cpv1.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	rev, ok := f.revs[ns]
	if !ok {
		return nil, errMissing
	}
	return &cpv1.Snapshot{Namespace: ns, Revision: rev}, nil
}

func (f *fakeSource) set(ns string, rev int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revs[ns] = rev
}

func (f *fakeSource) loadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads
}

func next(t *testing.T, s *Subscription) *cpv1.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snap, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return snap
}

func expectNothing(t *testing.T, s *Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if snap, err := s.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next = %v, %v; want nothing pending", snap, err)
	}
}

func subscribe(t *testing.T, h *Hub, ns string, known int64) *Subscription {
	t.Helper()
	s, err := h.Subscribe(context.Background(), ns, known)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestSubscribeDeliversCurrentSnapshot(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 4})
	h := New(src.load)
	s := subscribe(t, h, "svc", 0)
	if got := next(t, s).GetRevision(); got != 4 {
		t.Fatalf("revision = %d, want 4", got)
	}
	expectNothing(t, s)
}

func TestSubscribeSkipsSnapshotClientAlreadyHas(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 4})
	h := New(src.load)
	s := subscribe(t, h, "svc", 4)
	expectNothing(t, s)

	src.set("svc", 5)
	if err := h.Notify(context.Background(), "svc", 5); err != nil {
		t.Fatal(err)
	}
	if got := next(t, s).GetRevision(); got != 5 {
		t.Fatalf("revision = %d, want 5", got)
	}
}

func TestNotifyFansOutToEverySubscriber(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 1, "other": 1})
	h := New(src.load)
	subs := []*Subscription{subscribe(t, h, "svc", 0), subscribe(t, h, "svc", 0), subscribe(t, h, "svc", 0)}
	other := subscribe(t, h, "other", 0)
	for _, s := range subs {
		next(t, s)
	}
	next(t, other)

	loadsBefore := src.loadCount()
	src.set("svc", 2)
	if err := h.Notify(context.Background(), "svc", 2); err != nil {
		t.Fatal(err)
	}
	for i, s := range subs {
		if got := next(t, s).GetRevision(); got != 2 {
			t.Fatalf("subscriber %d got revision %d, want 2", i, got)
		}
	}
	if loads := src.loadCount() - loadsBefore; loads != 1 {
		t.Fatalf("snapshot loaded %d times for one change, want 1", loads)
	}
	expectNothing(t, other)
}

func TestSlowSubscriberOnlySeesLatest(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 1})
	h := New(src.load)
	s := subscribe(t, h, "svc", 0)

	for rev := int64(2); rev <= 6; rev++ {
		src.set("svc", rev)
		if err := h.Notify(context.Background(), "svc", rev); err != nil {
			t.Fatal(err)
		}
	}
	if got := next(t, s).GetRevision(); got != 6 {
		t.Fatalf("revision = %d, want latest 6", got)
	}
	expectNothing(t, s)
}

func TestNotifySkipsWorkWhenNotNeeded(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 3, "unwatched": 1})
	h := New(src.load)
	s := subscribe(t, h, "svc", 0)
	next(t, s)
	loads := src.loadCount()

	ctx := context.Background()
	_ = h.Notify(ctx, "svc", 3)       // already current
	_ = h.Notify(ctx, "svc", 2)       // older
	_ = h.Notify(ctx, "unwatched", 9) // nobody watching
	if got := src.loadCount(); got != loads {
		t.Fatalf("loads went from %d to %d, want no extra loads", loads, got)
	}
	expectNothing(t, s)
}

func TestSubscribeLoadErrorCleansUp(t *testing.T) {
	src := newFakeSource(map[string]int64{})
	h := New(src.load)
	if _, err := h.Subscribe(context.Background(), "missing", 0); !errors.Is(err, errMissing) {
		t.Fatalf("Subscribe error = %v, want %v", err, errMissing)
	}
	if n := h.Watchers(); n != 0 {
		t.Fatalf("Watchers = %d after failed subscribe, want 0", n)
	}
	if revs := h.Revisions(); len(revs) != 0 {
		t.Fatalf("Revisions = %v, want empty", revs)
	}
}

func TestLastUnsubscribeDropsCache(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 1})
	h := New(src.load)
	a, _ := h.Subscribe(context.Background(), "svc", 0)
	b, _ := h.Subscribe(context.Background(), "svc", 0)
	if revs := h.Revisions(); revs["svc"] != 1 || h.Watchers() != 2 {
		t.Fatalf("Revisions = %v Watchers = %d", revs, h.Watchers())
	}
	a.Close()
	a.Close() // idempotent
	if h.Watchers() != 1 {
		t.Fatalf("Watchers = %d, want 1", h.Watchers())
	}
	b.Close()
	if revs := h.Revisions(); len(revs) != 0 {
		t.Fatalf("Revisions = %v after last unsubscribe, want empty", revs)
	}
}

func TestCloseEndsSubscriptions(t *testing.T) {
	src := newFakeSource(map[string]int64{"svc": 1})
	h := New(src.load)
	s := subscribe(t, h, "svc", 0)
	next(t, s)

	done := make(chan error, 1)
	go func() {
		_, err := s.Next(context.Background())
		done <- err
	}()
	h.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Next after Close = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return after hub Close")
	}
	if _, err := h.Subscribe(context.Background(), "svc", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close = %v, want ErrClosed", err)
	}
}

// TestConcurrentWritersAndWatchers checks under -race that every watcher sees
// strictly increasing revisions and converges on the final one.
func TestConcurrentWritersAndWatchers(t *testing.T) {
	const (
		namespaces = 3
		watchers   = 20
		finalRev   = 200
	)
	revs := make(map[string]int64)
	for i := range namespaces {
		revs[fmt.Sprintf("ns%d", i)] = 1
	}
	src := newFakeSource(revs)
	h := New(src.load)
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, namespaces*watchers)
	for i := range namespaces {
		ns := fmt.Sprintf("ns%d", i)
		for range watchers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, err := h.Subscribe(ctx, ns, 0)
				if err != nil {
					errs <- err
					return
				}
				defer s.Close()
				var last int64
				for last < finalRev {
					snap, err := s.Next(ctx)
					if err != nil {
						errs <- fmt.Errorf("%s: stuck at revision %d: %w", ns, last, err)
						return
					}
					if snap.GetRevision() <= last {
						errs <- fmt.Errorf("%s: revision went from %d to %d", ns, last, snap.GetRevision())
						return
					}
					last = snap.GetRevision()
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rev := int64(2); rev <= finalRev; rev++ {
				src.set(ns, rev)
				if err := h.Notify(ctx, ns, rev); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
