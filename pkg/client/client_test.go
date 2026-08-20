package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// changeLog records the revisions OnChange listeners see.
type changeLog struct {
	mu    sync.Mutex
	calls []string
}

// listener returns an OnChange listener that records "name old->new".
func (l *changeLog) listener(name string) func(old, new *cpv1.Snapshot) {
	return func(old, new *cpv1.Snapshot) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.calls = append(l.calls, fmt.Sprintf("%s %d->%d", name, old.GetRevision(), new.GetRevision()))
	}
}

func (l *changeLog) want(t *testing.T, want ...string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !slices.Equal(l.calls, want) {
		t.Fatalf("listener calls = %q, want %q", l.calls, want)
	}
}

func wantStatus(t *testing.T, c *Client, state State, source Source, revision int64) Status {
	t.Helper()
	st := c.Status()
	if st.State != state || st.Source != source || st.Revision != revision {
		t.Fatalf("Status() = %v/%v/revision %d (last error %v), want %v/%v/revision %d",
			st.State, st.Source, st.Revision, st.LastError, state, source, revision)
	}
	return st
}

func TestNewValidatesOptions(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"no namespace", Options{Address: "localhost:9090"}},
		{"invalid namespace", Options{Address: "localhost:9090", Namespace: "Checkout Prod"}},
		{"neither address nor conn", Options{Namespace: testNamespace}},
		{"both address and conn", Options{Address: "localhost:9090", Conn: conn, Namespace: testNamespace}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if c, err := New(context.Background(), tc.opts); err == nil {
				c.Close()
				t.Fatal("New succeeded")
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(ctx, Options{Address: "localhost:9090", Namespace: testNamespace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("New with a canceled context: %v, want context.Canceled", err)
	}
}

func TestHotReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c := e.start(e.options())
		w := e.accept()
		w.push(t, snap(1, config(t, "greeting", "hello"), flag("new-cart", false, 100)))
		if err := c.WaitReady(context.Background()); err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
		if c.Revision() != 1 || c.String("greeting", "") != "hello" || c.IsEnabled("new-cart", "alice") {
			t.Fatalf("revision 1 not applied: revision %d, greeting %q, new-cart %v",
				c.Revision(), c.String("greeting", ""), c.IsEnabled("new-cart", "alice"))
		}

		// No fake time passes: the change is served as soon as it arrives.
		next := snap(2, config(t, "greeting", "hi"), flag("new-cart", true, 100))
		w.push(t, next)
		if c.Revision() != 2 || c.String("greeting", "") != "hi" || !c.IsEnabled("new-cart", "alice") {
			t.Fatalf("revision 2 not applied: revision %d, greeting %q, new-cart %v",
				c.Revision(), c.String("greeting", ""), c.IsEnabled("new-cart", "alice"))
		}
		if !proto.Equal(c.Snapshot(), next) {
			t.Fatalf("Snapshot() = %v, want %v", c.Snapshot(), next)
		}
	})
}

func TestLargeSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		var configs []any
		for i := range 24 {
			configs = append(configs, config(t, fmt.Sprint("blob-", i), strings.Repeat("x", 256<<10)))
		}
		big := snap(1, configs...)
		if size := proto.Size(big); size <= 4<<20 {
			t.Fatalf("snapshot of %d bytes is within gRPC's default limit", size)
		}
		c, _ := e.connect(e.options(), big)
		if c.Revision() != 1 || len(c.String("blob-23", "")) != 256<<10 {
			t.Fatalf("snapshot beyond 4 MiB not applied: revision %d, last error %v", c.Revision(), c.Status().LastError)
		}
	})
}

func TestOnChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c := e.start(e.options())
		var log changeLog
		removeA := c.OnChange(log.listener("a"))
		c.OnChange(func(_, _ *cpv1.Snapshot) { panic("listener bug") })
		c.OnChange(log.listener("b"))
		var removeOnce func()
		removeOnce = c.OnChange(func(old, new *cpv1.Snapshot) {
			log.listener("once")(old, new)
			removeOnce()
		})

		w := e.accept()
		w.push(t, snap(1))
		w.push(t, snap(2))
		removeA()
		removeA()
		w.push(t, snap(3))

		log.want(t, "a 0->1", "b 0->1", "once 0->1", "a 1->2", "b 1->2", "b 2->3")
		if n := e.logs.count(slog.LevelError, "OnChange listener panicked"); n != 3 {
			t.Fatalf("logged %d listener panics, want 3", n)
		}
		if c.Revision() != 3 {
			t.Fatalf("revision %d after panicking listeners, want 3", c.Revision())
		}
	})
}

func TestOnChangeRemovalDuringDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c := e.start(e.options())
		var log changeLog
		var removeB func()
		c.OnChange(func(old, new *cpv1.Snapshot) {
			log.listener("a")(old, new)
			removeB()
		})
		removeB = c.OnChange(log.listener("b"))

		w := e.accept()
		w.push(t, snap(1))
		w.push(t, snap(2))
		log.want(t, "a 0->1", "a 1->2")
	})
}

func TestOnChangeNilListenerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("OnChange(nil) did not panic")
		}
	}()
	c := &Client{}
	c.OnChange(nil)
}

func TestStatusTransitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		gate := make(chan struct{})
		opts := e.options()
		opts.DialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return e.dial(ctx, addr)
		})}
		c := e.start(opts)
		synctest.Wait()
		if st := wantStatus(t, c, StateConnecting, SourceNone, 0); !st.LastUpdate.IsZero() || st.LastError != nil {
			t.Fatalf("Status() before connecting = %+v", st)
		}

		close(gate)
		w := e.accept()
		synctest.Wait()
		wantStatus(t, c, StateLive, SourceNone, 0)

		w.push(t, snap(1))
		if st := wantStatus(t, c, StateLive, SourceServer, 1); !st.LastUpdate.Equal(time.Now()) || st.LastError != nil {
			t.Fatalf("Status() after the first snapshot = %+v, want LastUpdate %v", st, time.Now())
		}

		w.close(t, status.Error(codes.Unavailable, "server restarting"))
		st := wantStatus(t, c, StateDisconnected, SourceServer, 1)
		if status.Code(st.LastError) != codes.Unavailable {
			t.Fatalf("LastError = %v, want code Unavailable", st.LastError)
		}

		// The error that caused the reconnect stays visible.
		e.accept()
		synctest.Wait()
		if st := wantStatus(t, c, StateLive, SourceServer, 1); status.Code(st.LastError) != codes.Unavailable {
			t.Fatalf("LastError after reconnecting = %v, want the Unavailable error kept", st.LastError)
		}

		c.Close()
		if st := wantStatus(t, c, StateDisconnected, SourceServer, 1); !errors.Is(st.LastError, ErrClosed) {
			t.Fatalf("LastError after Close = %v, want ErrClosed", st.LastError)
		}
	})
}

func TestStateAndSourceNames(t *testing.T) {
	for v, want := range map[fmt.Stringer]string{
		StateConnecting:   "connecting",
		StateLive:         "live",
		StateDisconnected: "disconnected",
		State(7):          "State(7)",
		SourceNone:        "none",
		SourceCache:       "cache",
		SourceServer:      "server",
		Source(7):         "Source(7)",
	} {
		if got := v.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
	text, err := StateLive.MarshalText()
	if err != nil || string(text) != "live" {
		t.Errorf("StateLive.MarshalText() = %q, %v", text, err)
	}
	text, err = SourceCache.MarshalText()
	if err != nil || string(text) != "cache" {
		t.Errorf("SourceCache.MarshalText() = %q, %v", text, err)
	}
}

func TestReconnectBacksOffAndSendsKnownRevision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		// Always draw the largest jitter, so every wait is exactly the
		// backoff ceiling: MinBackoff 100ms doubling up to MaxBackoff 1s.
		c, err := newClient(context.Background(), e.options(), func(n int64) int64 { return n - 1 })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })

		w := e.accept()
		if got := w.req.GetKnownRevision(); got != 0 {
			t.Fatalf("first Watch: known_revision %d, want 0", got)
		}
		w.push(t, snap(1))
		w.push(t, snap(2))

		// Each step: what the current stream does before failing, then the
		// expected wait and known_revision of the next attempt.
		steps := []struct {
			name      string
			push      int64 // revision delivered first, 0 for none
			open      time.Duration
			wantDelay time.Duration
			wantKnown int64
		}{
			{name: "stream delivered revisions 1 and 2", wantDelay: 200 * time.Millisecond, wantKnown: 2},
			{name: "2nd failure in a row", wantDelay: 400 * time.Millisecond, wantKnown: 2},
			{name: "3rd failure in a row", wantDelay: 800 * time.Millisecond, wantKnown: 2},
			{name: "4th failure, capped", wantDelay: time.Second, wantKnown: 2},
			{name: "5th failure, capped", wantDelay: time.Second, wantKnown: 2},
			{name: "delivery resets", push: 3, wantDelay: 200 * time.Millisecond, wantKnown: 3},
			{name: "silent but long-lived stream resets", open: healthyAfter, wantDelay: 200 * time.Millisecond, wantKnown: 3},
			{name: "short silent stream does not", open: healthyAfter - time.Second, wantDelay: 400 * time.Millisecond, wantKnown: 3},
		}
		for _, s := range steps {
			if s.push != 0 {
				w.push(t, snap(s.push))
			}
			time.Sleep(s.open)
			failed := time.Now()
			w.close(t, status.Error(codes.Unavailable, "server restarting"))
			next := e.accept()
			if got := time.Since(failed); got != s.wantDelay {
				t.Errorf("%s: reconnected after %v, want %v", s.name, got, s.wantDelay)
			}
			if got := next.req.GetKnownRevision(); got != s.wantKnown {
				t.Errorf("%s: known_revision %d, want %d", s.name, got, s.wantKnown)
			}
			w = next
		}

		// A stream the server ends without an error is retried as well.
		w.close(t, nil)
		if st := c.Status(); !errors.Is(st.LastError, errStreamEnded) {
			t.Fatalf("LastError after a clean end = %v, want errStreamEnded", st.LastError)
		}
		e.accept()
	})
}

func TestBackoffOptions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		min, max time.Duration
		// The first retry waits within [lo, hi].
		lo, hi time.Duration
	}{
		{"defaults", 0, 0, defaultMinBackoff, 2 * defaultMinBackoff},
		{"MaxBackoff below MinBackoff", 2 * time.Second, time.Second, 2 * time.Second, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newEnv(t)
				opts := e.options()
				opts.MinBackoff, opts.MaxBackoff = tc.min, tc.max
				e.start(opts)
				failed := time.Now()
				e.accept().close(t, status.Error(codes.Unavailable, "server restarting"))
				e.accept()
				if d := time.Since(failed); d < tc.lo || d > tc.hi {
					t.Fatalf("retried after %v, want %v to %v", d, tc.lo, tc.hi)
				}
			})
		})
	}
}

func TestRetryDelay(t *testing.T) {
	const lo, hi = 100 * time.Millisecond, 3 * time.Second
	smallest := func(int64) int64 { return 0 }
	largest := func(n int64) int64 { return n - 1 }
	for _, tc := range []struct {
		failures    int
		wantCeiling time.Duration
	}{
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1600 * time.Millisecond},
		{5, hi},
		{6, hi},
		{1 << 20, hi},
	} {
		if got := retryDelay(tc.failures, lo, hi, smallest); got != lo {
			t.Errorf("retryDelay(%d) with the smallest jitter = %v, want %v", tc.failures, got, lo)
		}
		if got := retryDelay(tc.failures, lo, hi, largest); got != tc.wantCeiling {
			t.Errorf("retryDelay(%d) with the largest jitter = %v, want %v", tc.failures, got, tc.wantCeiling)
		}
	}
	if got := retryDelay(3, time.Second, time.Second, largest); got != time.Second {
		t.Errorf("retryDelay with MinBackoff == MaxBackoff = %v, want 1s", got)
	}
	if got := retryDelay(100, time.Nanosecond, math.MaxInt64, largest); got != math.MaxInt64 {
		t.Errorf("retryDelay near the time.Duration limit = %v, want %v", got, time.Duration(math.MaxInt64))
	}

	seen := make(map[time.Duration]bool)
	for range 1000 {
		d := retryDelay(3, lo, hi, rand.Int64N)
		if d < lo || d > 800*time.Millisecond {
			t.Fatalf("retryDelay(3) = %v, outside [%v, 800ms]", d, lo)
		}
		seen[d] = true
	}
	if len(seen) < 100 {
		t.Fatalf("1000 delays took only %d values: no jitter", len(seen))
	}
}

func TestWatchRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		opts := e.options()
		opts.Token = "s3cret"
		// Values of New's context reach the Watch calls; its cancellation
		// does not stop the client.
		ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.Background(), "x-team", "payments"))
		c, err := New(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		cancel()

		w := e.accept()
		if got := w.md.Get("authorization"); !slices.Equal(got, []string{"Bearer s3cret"}) {
			t.Errorf("authorization = %q, want the bearer token", got)
		}
		if got := w.md.Get("x-team"); !slices.Equal(got, []string{"payments"}) {
			t.Errorf("x-team = %q, want New's outgoing metadata", got)
		}
		host, _ := os.Hostname()
		if want := fmt.Sprintf("%s-%d", host, os.Getpid()); w.req.GetClientId() != want {
			t.Errorf("client_id = %q, want %q", w.req.GetClientId(), want)
		}
		if w.req.GetNamespace() != testNamespace {
			t.Errorf("namespace = %q, want %q", w.req.GetNamespace(), testNamespace)
		}
		w.push(t, snap(1))
		if c.Revision() != 1 {
			t.Fatal("canceling New's context stopped the client")
		}

		opts = e.options()
		opts.ClientID = "checkout-7"
		e.start(opts)
		w = e.accept()
		if got := w.md.Get("authorization"); len(got) != 0 {
			t.Errorf("authorization = %q without a token", got)
		}
		if w.req.GetClientId() != "checkout-7" {
			t.Errorf("client_id = %q, want checkout-7", w.req.GetClientId())
		}
	})
}

func TestStaleAndForeignSnapshotsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(5, flag("new-cart", true, 100)))
		var log changeLog
		c.OnChange(log.listener("l"))

		w.push(t, snap(3, flag("new-cart", false, 0)))
		w.push(t, snap(5, flag("new-cart", false, 0)))
		other := snap(9)
		other.Namespace = "billing/prod"
		w.push(t, other)
		w.push(t, nil)
		if c.Revision() != 5 || !c.IsEnabled("new-cart", "alice") {
			t.Fatalf("revision %d, new-cart %v after stale and foreign snapshots; want revision 5 kept",
				c.Revision(), c.IsEnabled("new-cart", "alice"))
		}
		if n := e.logs.count(slog.LevelWarn, "older than the one in use"); n != 1 {
			t.Errorf("logged %d regressions, want 1", n)
		}
		if n := e.logs.count(slog.LevelWarn, "another namespace"); n != 2 {
			t.Errorf("logged %d foreign snapshots, want 2", n)
		}

		w.push(t, snap(6, flag("new-cart", false, 0)))
		if c.Revision() != 6 || c.IsEnabled("new-cart", "alice") {
			t.Fatal("revision 6 not applied")
		}
		log.want(t, "l 5->6")
	})
}

func TestCloseWhileLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(1, flag("new-cart", true, 100)))

		start := time.Now()
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(func() {
				if err := c.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
		}
		wg.Wait()
		if d := time.Since(start); d != 0 {
			t.Fatalf("Close took %v", d)
		}
		synctest.Wait()
		if !w.ended() {
			t.Fatal("the Watch stream is still open after Close")
		}
		if err := c.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if st := wantStatus(t, c, StateDisconnected, SourceServer, 1); !errors.Is(st.LastError, ErrClosed) {
			t.Fatalf("LastError = %v, want ErrClosed", st.LastError)
		}

		// The last snapshot is still served, and nothing reconnects.
		if !c.IsEnabled("new-cart", "alice") {
			t.Fatal("a closed client stopped serving its snapshot")
		}
		if err := c.WaitReady(context.Background()); err != nil {
			t.Fatalf("WaitReady on a closed, ready client: %v", err)
		}
		time.Sleep(time.Hour)
		select {
		case <-e.srv.watches:
			t.Fatal("the client watched again after Close")
		default:
		}
	})
}

func TestCloseWaitsForListener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c := e.start(e.options())
		release := make(chan struct{})
		c.OnChange(func(_, _ *cpv1.Snapshot) { <-release })
		e.accept().push(t, snap(1))

		var closed atomic.Bool
		go func() {
			c.Close()
			closed.Store(true)
		}()
		synctest.Wait()
		if closed.Load() {
			t.Fatal("Close returned while a listener was running")
		}
		close(release)
		synctest.Wait()
		if !closed.Load() {
			t.Fatal("Close did not return once the listener finished")
		}
	})
}

func TestCloseWhileBackingOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		e.down()
		opts := e.options()
		opts.MinBackoff, opts.MaxBackoff = time.Hour, time.Hour
		c := e.start(opts)
		synctest.Wait()
		wantStatus(t, c, StateDisconnected, SourceNone, 0)

		start := time.Now()
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if d := time.Since(start); d != 0 {
			t.Fatalf("Close waited %v for the backoff", d)
		}
		if err := c.WaitReady(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("WaitReady after Close = %v, want ErrClosed", err)
		}
	})
}

func TestCloseLeavesCallersConnOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		conn, err := grpc.NewClient("passthrough:///controlplane",
			grpc.WithContextDialer(e.dial), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		opts := e.options()
		opts.Address, opts.DialOptions, opts.Conn = "", nil, conn
		c, _ := e.connect(opts, snap(1))
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if s := conn.GetState(); s == connectivity.Shutdown {
			t.Fatal("Close closed the caller's connection")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if _, err := cpv1.NewDistributionServiceClient(conn).Watch(ctx, &cpv1.WatchRequest{Namespace: testNamespace}); err != nil {
			t.Fatalf("Watch on the caller's connection after Close: %v", err)
		}
		e.accept()
	})
}

func TestWaitReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		e.down()
		c := e.start(e.options())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := c.WaitReady(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "Unavailable") {
			t.Fatalf("WaitReady with the server down = %v, want a deadline error naming the watch error", err)
		}

		e.up()
		done := make(chan error, 1)
		go func() { done <- c.WaitReady(context.Background()) }()
		e.accept().push(t, snap(1))
		if err := <-done; err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
	})
}

func TestLargeSnapshotOnCallersConn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		conn, err := grpc.NewClient("passthrough:///controlplane",
			grpc.WithContextDialer(e.dial), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		var configs []any
		for i := range 24 {
			configs = append(configs, config(t, fmt.Sprint("blob-", i), strings.Repeat("x", 256<<10)))
		}
		opts := e.options()
		opts.Address, opts.DialOptions, opts.Conn = "", nil, conn
		c, _ := e.connect(opts, snap(1, configs...))
		if c.Revision() != 1 {
			t.Fatalf("snapshot beyond 4 MiB not applied on the caller's connection: last error %v", c.Status().LastError)
		}
	})
}

// A stream can fail with any status; each is retried after the backoff, never
// in a tight loop, and none of them disturbs the snapshot in use.
func TestStreamErrorsAreRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(1, flag("f", true, 100)))
		for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
			// Streams that delivered a snapshot reset the ceiling, so every
			// wait is between MinBackoff and twice that: 100ms to 200ms.
			w.push(t, snap(int64(code)+2, flag("f", true, 100)))
			failed := time.Now()
			w.close(t, status.Error(code, "boom"))
			if st := c.Status(); st.State != StateDisconnected || status.Code(st.LastError) != code {
				t.Fatalf("after %v: Status() = %v, last error %v", code, st.State, st.LastError)
			}
			w = e.accept()
			if d := time.Since(failed); d < 100*time.Millisecond || d > 200*time.Millisecond {
				t.Fatalf("after %v: reconnected after %v, want 100ms to 200ms", code, d)
			}
			if c.Revision() != int64(code)+2 || !c.IsEnabled("f", "alice") {
				t.Fatalf("after %v: snapshot in use changed to revision %d", code, c.Revision())
			}
		}
	})
}

// Whatever a server or a cache file holds, installing it must not panic and
// must leave a usable client.
func TestHostileSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		hostile := &cpv1.Snapshot{
			Namespace: testNamespace,
			Revision:  1,
			Configs:   []*cpv1.Config{nil, {Key: "nil-value"}, {Key: "dup", Value: structpb.NewNumberValue(math.NaN())}},
			Flags:     []*cpv1.Flag{nil, {Key: "nan", Enabled: true, RolloutPercent: math.NaN()}, {Key: "inf", Enabled: true, RolloutPercent: math.Inf(1)}},
			Experiments: []*cpv1.Experiment{nil, {Key: "empty", Enabled: true},
				{Key: "nil-variant", Enabled: true, Variants: []*cpv1.Variant{nil, {Name: "a", Weight: 1}}}},
			RateLimits: []*cpv1.RateLimit{nil, {Key: "nan", Enabled: true, RequestsPerSecond: math.NaN(), Burst: math.MaxUint32},
				{Key: "inf", Enabled: true, RequestsPerSecond: math.Inf(1), Burst: 1}, {Key: "neg", Enabled: true, RequestsPerSecond: -1}},
			CircuitBreakers: []*cpv1.CircuitBreaker{nil, {Key: "bare", Enabled: true},
				{Key: "nan", Enabled: true, FailureRateThreshold: math.NaN(), Window: durationpb.New(-time.Hour), OpenDuration: durationpb.New(-1)}},
		}
		c, _ := e.connect(e.options(), hostile)
		if c.Revision() != 1 {
			t.Fatalf("revision %d, want 1", c.Revision())
		}
		c.IsEnabled("nan", "alice")
		c.IsEnabled("inf", "")
		c.Variant("empty", "alice")
		c.Variant("nil-variant", "alice")
		c.Allow("nan")
		c.Allow("inf")
		c.Allow("neg")
		c.Float("dup", 0)
		c.Int("dup", 0)
		_ = c.Decode("nil-value", new(any))
		_ = c.Decode("dup", new(float64))
		for _, key := range []string{"bare", "nan"} {
			_ = c.Do(key, func() error { return errors.New("x") })
		}
		if got := c.String("missing", "def"); got != "def" {
			t.Fatalf("String = %q", got)
		}
	})
}
