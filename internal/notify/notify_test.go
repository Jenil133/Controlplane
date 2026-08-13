package notify

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// startRun runs n.Run in the background and returns received events.
func startRun(t *testing.T, ctx context.Context, n Notifier) <-chan Event {
	t.Helper()
	got := make(chan Event, 16)
	go func() {
		if err := n.Run(ctx, func(ev Event) { got <- ev }); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	return got
}

const warmupNamespace = "warmup"

// expectEvent waits for want, skipping leftover warmup events.
func expectEvent(t *testing.T, got <-chan Event, want Event) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev := <-got:
			if ev.Namespace == warmupNamespace {
				continue
			}
			if ev != want {
				t.Fatalf("event = %+v, want %+v", ev, want)
			}
			return
		case <-timeout:
			t.Fatalf("timed out waiting for %+v", want)
		}
	}
}

// waitSubscribed publishes warmup events until every subscriber has received
// one, because Run starts in a goroutine and may not be registered yet.
func waitSubscribed(t *testing.T, ctx context.Context, n Notifier, subs ...<-chan Event) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for _, got := range subs {
	retry:
		for {
			if time.Now().After(deadline) {
				t.Fatal("subscriber never received an event")
			}
			if err := n.Publish(ctx, Event{Namespace: warmupNamespace, Revision: 1}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			select {
			case <-got:
				break retry
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

func TestLocalDeliversToAllSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewLocal()
	a, b := startRun(t, ctx, bus), startRun(t, ctx, bus)

	waitSubscribed(t, ctx, bus, a, b)

	ev := Event{Namespace: "svc", Revision: 7}
	if err := bus.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, a, ev)
	expectEvent(t, b, ev)
}

func TestLocalPublishNeverBlocks(t *testing.T) {
	bus := NewLocal()
	bus.subs[make(chan Event)] = struct{}{} // unbuffered, nobody reading
	done := make(chan struct{})
	go func() {
		_ = bus.Publish(context.Background(), Event{Namespace: "svc", Revision: 1})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber")
	}
}

func TestRedisRoundTripBetweenReplicas(t *testing.T) {
	mr := miniredis.RunT(t)
	newClient := func() *redis.Client {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })
		return c
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replicaA := NewRedis(newClient(), "", nil)
	replicaB := NewRedis(newClient(), "", nil)
	gotA, gotB := startRun(t, ctx, replicaA), startRun(t, ctx, replicaB)

	waitSubscribed(t, ctx, replicaA, gotA, gotB)

	ev := Event{Namespace: "checkout/prod", Revision: 42}
	if err := replicaA.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, gotB, ev)
	expectEvent(t, gotA, ev) // the sender hears its own events too
}

func TestRedisIgnoresMalformedPayloads(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := NewRedis(client, "test-channel", slog.New(slog.DiscardHandler))
	got := startRun(t, ctx, n)
	waitSubscribed(t, ctx, n, got)

	mr.Publish("test-channel", "not json")
	mr.Publish("test-channel", `{"rev":3}`)
	want := Event{Namespace: "svc", Revision: 3}
	if err := n.Publish(ctx, want); err != nil {
		t.Fatal(err)
	}
	expectEvent(t, got, want)
}

func TestRedisRunStopsOnCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewRedis(client, "", nil).Run(ctx, func(Event) {}) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancel, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}
