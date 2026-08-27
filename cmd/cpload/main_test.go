package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func TestActorHeaderMatchesServer(t *testing.T) {
	if actorHeader != server.ActorHeader {
		t.Fatalf("actorHeader = %q, server expects %q", actorHeader, server.ActorHeader)
	}
}

func TestParseConfig(t *testing.T) {
	t.Setenv("CPLOAD_TOKEN", "from-env")
	got, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		Addrs: []string{"localhost:9090"}, Namespace: "loadtest", Watchers: 1000, Writes: 50,
		Interval: 100 * time.Millisecond, ConnsPerAddr: 4, Token: "from-env", SLO: time.Second, Timeout: time.Minute,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults = %+v, want %+v", got, want)
	}

	got, err = parseConfig([]string{
		"--addrs", " a:1, ,b:2 ", "--namespace", "perf/x", "--watchers", "7", "--writes", "3", "--interval", "0",
		"--conns-per-addr", "1", "--token", "t", "--slo", "250ms", "--timeout", "5s", "--json",
	})
	if err != nil {
		t.Fatal(err)
	}
	want = config{
		Addrs: []string{"a:1", "b:2"}, Namespace: "perf/x", Watchers: 7, Writes: 3,
		ConnsPerAddr: 1, Token: "t", SLO: 250 * time.Millisecond, Timeout: 5 * time.Second, JSON: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed %+v, want %+v", got, want)
	}

	if _, err := parseConfig([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h: %v, want flag.ErrHelp", err)
	}
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"--addrs", " , "}, "--addrs"},
		{[]string{"--watchers", "0"}, "--watchers"},
		{[]string{"--writes", "0"}, "--writes"},
		{[]string{"--interval", "-1s"}, "--interval"},
		{[]string{"--conns-per-addr", "0"}, "--conns-per-addr"},
		{[]string{"--slo", "0s"}, "--slo"},
		{[]string{"--timeout", "0s"}, "--timeout"},
		{[]string{"--writes", "12", "--interval", "1s", "--timeout", "10s"}, "--timeout 10s is shorter than 12 writes"},
		{[]string{"--namespace", "Bad Name"}, "--namespace"},
		{[]string{"--watchers", "many"}, "-watchers"},
		{[]string{"--unknown"}, "-unknown"},
		{[]string{"extra"}, `unexpected argument "extra"`},
	} {
		if _, err := parseConfig(tt.args); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("parseConfig(%q) = %v, want an error mentioning %q", tt.args, err, tt.want)
		}
	}
	// Exactly as long as the writes take is still allowed.
	if _, err := parseConfig([]string{"--writes", "11", "--interval", "1s", "--timeout", "10s"}); err != nil {
		t.Fatalf("timeout equal to the write phase rejected: %v", err)
	}
}

func TestTracker(t *testing.T) {
	isClosed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	tr := newTracker(3)

	ready := tr.await(1)
	tr.observe(0, 4)
	tr.observe(1, 4)
	if isClosed(ready) {
		t.Fatal("await(1) done while watcher 2 has no snapshot")
	}
	boom := errors.New("boom")
	tr.fail(2, boom)
	if !isClosed(ready) {
		t.Fatal("a failed watcher still holds up await(1)")
	}
	if ready, failed, first := tr.started(); ready != 2 || failed != 1 || first != boom {
		t.Fatalf("started() = %d, %d, %v; want 2, 1, boom", ready, failed, first)
	}

	done := tr.await(6)
	tr.observe(0, 5)
	tr.observe(1, 7) // past the target counts
	if isClosed(done) {
		t.Fatal("await(6) done while watcher 0 is at 5")
	}
	tr.observe(0, 6)
	if !isClosed(done) {
		t.Fatal("await(6) not done although every live watcher holds 6 or later")
	}
	tr.observe(0, 8) // must not close the channel again

	done = tr.await(9)
	tr.observe(0, 9)
	tr.fail(0, boom) // was not behind: must not release the wait for watcher 1
	if isClosed(done) {
		t.Fatal("await(9) done while watcher 1 is at 7")
	}
	tr.fail(1, boom) // behind: never catching up, so stop waiting for it
	if !isClosed(done) {
		t.Fatal("await(9) not done although no live watcher is behind")
	}
	if !isClosed(tr.await(100)) {
		t.Fatal("await with every watcher failed must be done at once")
	}
}

// fakeAdmin answers GetNamespace and CreateNamespace with the given errors
// and records the namespace each call asked for. Any other method panics on
// the nil embedded client.
type fakeAdmin struct {
	cpv1.AdminServiceClient
	getErr, createErr error
	gets, creates     []string
}

func (f *fakeAdmin) GetNamespace(_ context.Context, req *cpv1.GetNamespaceRequest, _ ...grpc.CallOption) (*cpv1.GetNamespaceResponse, error) {
	f.gets = append(f.gets, req.GetName())
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &cpv1.GetNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName()}}, nil
}

func (f *fakeAdmin) CreateNamespace(_ context.Context, req *cpv1.CreateNamespaceRequest, _ ...grpc.CallOption) (*cpv1.CreateNamespaceResponse, error) {
	f.creates = append(f.creates, req.GetName())
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &cpv1.CreateNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName()}}, nil
}

func TestEnsureNamespace(t *testing.T) {
	var (
		notFound      = status.Error(codes.NotFound, `not found: namespace "loadtest"`)
		alreadyExists = status.Error(codes.AlreadyExists, `already exists: namespace "loadtest"`)
		denied        = status.Error(codes.PermissionDenied, "CreateNamespace needs the admin role")
		unavailable   = status.Error(codes.Unavailable, "connection refused")
	)
	tests := []struct {
		name              string
		getErr, createErr error
		wantCreated       bool
		wantErr           error
		wantCreateCall    bool
	}{
		{name: "exists"},
		{name: "missing", getErr: notFound, wantCreateCall: true, wantCreated: true},
		// Another run, e.g. a parallel CI job, created it between the calls.
		{name: "created meanwhile", getErr: notFound, createErr: alreadyExists, wantCreateCall: true},
		{name: "missing and may not create", getErr: notFound, createErr: denied, wantCreateCall: true, wantErr: denied},
		// Only NotFound says the namespace is missing.
		{name: "lookup fails", getErr: unavailable, wantErr: unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeAdmin{getErr: tt.getErr, createErr: tt.createErr}
			created, err := ensureNamespace(context.Background(), f, "loadtest")
			if created != tt.wantCreated || !errors.Is(err, tt.wantErr) {
				t.Fatalf("ensureNamespace = %v, %v; want %v, %v", created, err, tt.wantCreated, tt.wantErr)
			}
			var wantCreates []string
			if tt.wantCreateCall {
				wantCreates = []string{"loadtest"}
			}
			if !reflect.DeepEqual(f.gets, []string{"loadtest"}) || !reflect.DeepEqual(f.creates, wantCreates) {
				t.Fatalf("GetNamespace calls %q, CreateNamespace calls %q; want one lookup and creates %q", f.gets, f.creates, wantCreates)
			}
		})
	}
}

// replica is an in-process control plane server on a real TCP listener.
type replica struct {
	srv   *server.Server
	addr  string
	obs   *observer
	peers *streamPeers
}

// observer counts the server events the tests check.
type observer struct {
	server.NopObserver

	mu       sync.Mutex
	writes   int // changes committed on this replica
	notified int // changes heard from the notifier
	onWrite  func()
}

func (o *observer) ChangeReceived(_ string, source server.ChangeSource) {
	o.mu.Lock()
	var hook func()
	switch source {
	case server.SourceWrite:
		o.writes++
		hook = o.onWrite
	case server.SourceNotifier:
		o.notified++
	}
	o.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// setOnWrite makes the replica call fn whenever it commits a write, before
// it refreshes its watchers.
func (o *observer) setOnWrite(fn func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onWrite = fn
}

func (o *observer) counts() (writes, notified int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writes, o.notified
}

// streamPeers counts streams per client connection (remote address).
type streamPeers struct {
	mu sync.Mutex
	n  map[string]int
}

func (p *streamPeers) intercept(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if pr, ok := peer.FromContext(ss.Context()); ok {
		p.mu.Lock()
		p.n[pr.Addr.String()]++
		p.mu.Unlock()
	}
	return handler(srv, ss)
}

func (p *streamPeers) counts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.n))
	for k, v := range p.n {
		out[k] = v
	}
	return out
}

func startReplica(t *testing.T, st store.Store, notifier notify.Notifier, opts ...grpc.ServerOption) *replica {
	t.Helper()
	r := &replica{obs: &observer{}, peers: &streamPeers{n: make(map[string]int)}}
	r.srv = server.New(server.Options{
		Store:    st,
		Notifier: notifier,
		Logger:   slog.New(slog.DiscardHandler),
		// Changes must arrive by push; the reconciler would mask a lost one.
		ReconcileInterval: time.Hour,
		Observer:          r.obs,
	})
	g := grpc.NewServer(append(opts, grpc.ChainStreamInterceptor(r.peers.intercept))...)
	r.srv.Register(g)
	r.addr = serve(t, g)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.srv.Run(ctx) }()
	// Runs before serve's cleanup: Watch streams end before the server stops.
	t.Cleanup(func() {
		r.srv.Shutdown()
		cancel()
	})
	return r
}

// serve runs g on a fresh local TCP port until the test ends.
func serve(t *testing.T, g *grpc.Server) (addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

// eventually polls cond until it holds, failing the test after 5s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitSubscribed returns once every replica hears events published on bus,
// so that no change event is lost to a subscription that is still starting.
func waitSubscribed(t *testing.T, bus *notify.Local, replicas ...*replica) {
	t.Helper()
	eventually(t, "replicas to subscribe to the change bus", func() bool {
		for _, r := range replicas {
			if _, notified := r.obs.counts(); notified == 0 {
				// Events published before a replica subscribed are lost:
				// probe again.
				_ = bus.Publish(context.Background(), notify.Event{Namespace: "probe", Revision: 1})
				return false
			}
		}
		return true
	})
}

func testConfig(addrs ...string) config {
	return config{
		Addrs:        addrs,
		Namespace:    "loadtest",
		Watchers:     50,
		Writes:       10,
		Interval:     5 * time.Millisecond,
		ConnsPerAddr: 2,
		SLO:          time.Second,
		Timeout:      10 * time.Second,
	}
}

// checkCleanup fails the test if run left a goroutine of its own running or
// a Watch stream open on any replica.
func checkCleanup(t *testing.T, replicas ...*replica) {
	t.Helper()
	checkNoLeaks(t)
	for _, r := range replicas {
		// A server notices a cancelled stream asynchronously.
		eventually(t, "replica "+r.addr+" to close its Watch streams", func() bool { return r.srv.Watchers() == 0 })
	}
}

// checkNoLeaks fails the test if a goroutine is still running this
// command's code. run waits for its goroutines, so no polling is needed.
func checkNoLeaks(t *testing.T) {
	t.Helper()
	if leaked := leakedGoroutines(); len(leaked) > 0 {
		t.Errorf("run left %d goroutine(s) behind:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
	}
}

// leakedGoroutines returns the stacks of all goroutines but the caller's
// that are executing code of this command (as opposed to its tests).
func leakedGoroutines() []string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var leaked []string
	// The caller's stack comes first.
	for _, g := range strings.Split(string(buf), "\n\n")[1:] {
		for _, line := range strings.Split(g, "\n") {
			if strings.Contains(line, "/cmd/cpload/") && !strings.Contains(line, "_test.go") {
				leaked = append(leaked, g)
				break
			}
		}
	}
	return leaked
}

// TestRunAcrossReplicas runs the load test against two replicas that share
// a store and a change bus, as replicas behind Redis do.
func TestRunAcrossReplicas(t *testing.T) {
	st, bus := memory.New(), notify.NewLocal()
	a := startReplica(t, st, bus)
	b := startReplica(t, st, bus)
	waitSubscribed(t, bus, a, b)

	cfg := testConfig(a.addr, b.addr)
	var progress bytes.Buffer
	rep, err := run(context.Background(), cfg, &progress)
	if err != nil {
		t.Fatal(err)
	}
	checkCleanup(t, a, b)

	if !rep.Passed() {
		t.Fatalf("run failed: %q, errors %v", rep.Failures, rep.Errors)
	}
	if rep.Watchers != 50 || rep.Writes != 10 || rep.WriteErrors != 0 || rep.WatchErrors != 0 || rep.Unconverged != 0 || len(rep.Errors) != 0 {
		t.Fatalf("report = %+v", rep)
	}
	// Each watcher either received a write's revision or skipped past it.
	if rep.Deliveries == 0 || rep.Deliveries+rep.Skipped != cfg.Watchers*cfg.Writes || rep.Latency.Count != rep.Deliveries {
		t.Fatalf("%d deliveries + %d skipped, %d latency samples; want deliveries + skipped = %d",
			rep.Deliveries, rep.Skipped, rep.Latency.Count, cfg.Watchers*cfg.Writes)
	}
	if rep.Convergence.Count != cfg.Writes {
		t.Fatalf("convergence samples = %d, want %d", rep.Convergence.Count, cfg.Writes)
	}
	for name, s := range map[string]stats{"latency": rep.Latency, "convergence": rep.Convergence} {
		if s.P50 <= 0 || s.P50 > s.P90 || s.P90 > s.P99 || s.P99 > s.Max {
			t.Errorf("%s percentiles out of order: %+v", name, s)
		}
	}
	// A write has not converged before its slowest delivery.
	if rep.Latency.Max > rep.Convergence.Max {
		t.Errorf("latency max %v > convergence max %v", rep.Latency.Max, rep.Convergence.Max)
	}

	// Watchers and writes went round-robin over both replicas, and each
	// replica's watchers shared ConnsPerAddr connections.
	for _, r := range []*replica{a, b} {
		if writes, _ := r.obs.counts(); writes != cfg.Writes/2 {
			t.Errorf("replica %s committed %d writes, want %d", r.addr, writes, cfg.Writes/2)
		}
		streams := r.peers.counts()
		total := 0
		for _, n := range streams {
			total += n
		}
		if len(streams) != cfg.ConnsPerAddr || total != cfg.Watchers/2 {
			t.Errorf("replica %s got streams %v, want %d over %d connections", r.addr, streams, cfg.Watchers/2, cfg.ConnsPerAddr)
		}
	}

	snap, err := st.Snapshot(context.Background(), cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Revision != 11 || len(snap.Configs) != 1 {
		t.Fatalf("namespace at revision %d with %d configs, want a new namespace written 10 times", snap.Revision, len(snap.Configs))
	}
	// The key is documented, so check the literal rather than seqKey.
	if c := snap.Configs[0]; c.Key != "loadtest.seq" || string(c.Value) != "10" || c.UpdatedBy != actor {
		t.Fatalf("config = %s=%s by %q", c.Key, c.Value, c.UpdatedBy)
	}
	if !strings.Contains(progress.String(), "created namespace loadtest") || !strings.Contains(progress.String(), "50 watchers ready") {
		t.Fatalf("progress output:\n%s", progress.String())
	}
}

// nopNotifier drops every change event.
type nopNotifier struct{}

func (nopNotifier) Publish(context.Context, notify.Event) error { return nil }

func (nopNotifier) Run(ctx context.Context, _ notify.Handler) error {
	<-ctx.Done()
	return nil
}

// TestRunMeasuresFromSendToReceive holds every write back for a known delay
// between its commit and its fan-out. Latency runs from the client's clock
// reading just before the write to its reading on receipt, so no delivery
// and no write's convergence can take less than that delay. A send time
// taken after the write returned, or a receive time taken from the server's
// commit timestamp, comes in well under it.
func TestRunMeasuresFromSendToReceive(t *testing.T) {
	const delay = 30 * time.Millisecond
	// A replica also hears its own writes on the change bus and pushes them
	// from there without the delay, so the bus must stay silent.
	r := startReplica(t, memory.New(), nopNotifier{})
	r.obs.setOnWrite(func() { time.Sleep(delay) })

	cfg := testConfig(r.addr)
	cfg.Watchers, cfg.Writes = 10, 5
	rep, err := run(context.Background(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	checkCleanup(t, r)

	if !rep.Passed() || rep.Writes != cfg.Writes || rep.Deliveries == 0 || rep.Convergence.Count != cfg.Writes {
		t.Fatalf("report = %+v", rep)
	}
	for name, s := range map[string]stats{"latency": rep.Latency, "convergence": rep.Convergence} {
		// The upper bound only rules out gross errors, such as send and
		// receive times taken from different origins.
		if s.P50 < delay || s.P50 > delay+time.Second {
			t.Errorf("%s p50 = %v, want the %v push delay plus a little", name, s.P50, delay)
		}
	}
}

// TestRunCountsFailedWatchers ends every Watch stream after the first write,
// as a replica shutting down does. The run carries on, counts every
// watcher's error and fails without waiting for --timeout.
func TestRunCountsFailedWatchers(t *testing.T) {
	r := startReplica(t, memory.New(), notify.NewLocal())
	r.obs.setOnWrite(sync.OnceFunc(r.srv.Shutdown))

	cfg := testConfig(r.addr)
	cfg.Watchers, cfg.Writes, cfg.Interval = 10, 5, time.Millisecond
	rep, err := run(context.Background(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	checkCleanup(t, r)

	if rep.Passed() || rep.WatchErrors != 10 || rep.Writes != 5 || rep.WriteErrors != 0 {
		t.Fatalf("report = %+v", rep)
	}
	// The streams ended while the first write was being pushed, so at most
	// that write reached every watcher.
	if rep.Unconverged < 4 || rep.Unconverged+rep.Convergence.Count != rep.Writes {
		t.Fatalf("%d unconverged and %d converged writes, want at least 4 of 5 unconverged", rep.Unconverged, rep.Convergence.Count)
	}
	if len(rep.Errors) != 1 || rep.Errors[0].Count != 10 || !strings.Contains(rep.Errors[0].Message, "watch "+r.addr) ||
		!strings.Contains(rep.Errors[0].Message, codes.Unavailable.String()) {
		t.Fatalf("errors = %v, want one Unavailable message counted 10 times", rep.Errors)
	}
	// Above all, no "run stopped early": failed watchers must not hold up
	// the run until --timeout.
	want := []string{"10 watch error(s)", fmt.Sprintf("%d write(s) never reached every watcher", rep.Unconverged)}
	if !reflect.DeepEqual(rep.Failures, want) {
		t.Fatalf("failures = %q, want %q", rep.Failures, want)
	}
}

// TestRunReportsAChangeAReplicaMissed isolates the replicas' change events,
// so watchers on b never see the last write, which goes to a. The run must
// end at --timeout and report that write as unconverged.
func TestRunReportsAChangeAReplicaMissed(t *testing.T) {
	st := memory.New()
	a := startReplica(t, st, notify.NewLocal())
	b := startReplica(t, st, notify.NewLocal())

	cfg := testConfig(a.addr, b.addr)
	cfg.Watchers, cfg.Writes, cfg.Interval, cfg.ConnsPerAddr = 4, 3, time.Millisecond, 1
	cfg.Timeout = 500 * time.Millisecond
	rep, err := run(context.Background(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	checkCleanup(t, a, b)

	// Writes go to a, b, a. Each replica pushes the revisions written on it,
	// so watchers on a reach the last revision and watchers on b stop one
	// short of it.
	if rep.Passed() || rep.Writes != 3 || rep.WatchErrors != 0 || rep.Unconverged != 1 || rep.Convergence.Count != 2 {
		t.Fatalf("report = %+v", rep)
	}
	// 4 watchers x 2 converged writes, plus the 2 watchers on a holding the
	// last revision, whether received or skipped.
	if rep.Deliveries+rep.Skipped != 10 {
		t.Fatalf("%d deliveries + %d skipped, want 10", rep.Deliveries, rep.Skipped)
	}
	want := []string{"run stopped early: timed out after 500ms", "1 write(s) never reached every watcher"}
	if !reflect.DeepEqual(rep.Failures, want) {
		t.Fatalf("failures = %q, want %q", rep.Failures, want)
	}
}

// runWithin runs cpload, failing the test if run has not returned after d
// instead of letting a broken stop path hang the test binary.
func runWithin(t *testing.T, d time.Duration, ctx context.Context, cfg config) (*report, error) {
	t.Helper()
	type result struct {
		rep *report
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := run(ctx, cfg, io.Discard)
		done <- result{rep, err}
	}()
	select {
	case r := <-done:
		return r.rep, r.err
	case <-time.After(d):
		t.Fatalf("run did not return within %v", d)
		return nil, nil
	}
}

// TestRunStopsEarly ends the run's context in the middle of a write and
// while run waits for the next one: run must return at once, with a report
// saying why.
func TestRunStopsEarly(t *testing.T) {
	tests := []struct {
		name                 string
		cancelOnWrite        bool
		timeout              time.Duration
		minWrites, maxWrites int
		failure              string
	}{
		{
			name:          "cancelled during the first write",
			cancelOnWrite: true,
			timeout:       10 * time.Second,
			// Whether the write's response beat the cancellation is a race.
			minWrites: 0, maxWrites: 1,
			failure: "run stopped early: context canceled",
		},
		{
			name:      "timed out waiting for the second write",
			timeout:   500 * time.Millisecond,
			minWrites: 1, maxWrites: 1,
			failure: "run stopped early: timed out after 500ms",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := startReplica(t, memory.New(), notify.NewLocal())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelOnWrite {
				r.obs.setOnWrite(cancel)
			}
			cfg := testConfig(r.addr)
			// The second write is due in an hour, so only ctx ends the run.
			cfg.Watchers, cfg.Writes, cfg.Interval, cfg.Timeout = 5, 3, time.Hour, tt.timeout
			rep, err := runWithin(t, 5*time.Second, ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			checkCleanup(t, r)

			if rep.Passed() || rep.Writes < tt.minWrites || rep.Writes > tt.maxWrites || rep.WriteErrors != 0 || rep.WatchErrors != 0 {
				t.Fatalf("report = %+v", rep)
			}
			if len(rep.Failures) == 0 || rep.Failures[0] != tt.failure {
				t.Fatalf("failures = %q, want %q first", rep.Failures, tt.failure)
			}
		})
	}
}

func TestRunWithToken(t *testing.T) {
	const token = "cp_load-test-token"
	authn, err := auth.New([]auth.Entry{{Name: "load-bot", Role: "editor", TokenSHA256: auth.HashToken(token)}})
	if err != nil {
		t.Fatal(err)
	}
	st := memory.New()
	// Editors may not create namespaces, so cpload must use this one as is.
	if _, err := st.CreateNamespace(context.Background(), model.Namespace{Name: "loadtest"}, store.WriteOptions{Actor: "admin"}); err != nil {
		t.Fatal(err)
	}
	r := startReplica(t, st, notify.NewLocal(),
		grpc.ChainUnaryInterceptor(authn.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authn.StreamServerInterceptor()))

	cfg := testConfig(r.addr)
	// Interval 0: back-to-back writes.
	cfg.Watchers, cfg.Writes, cfg.Interval = 5, 3, 0
	if _, err := run(context.Background(), cfg, io.Discard); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("run without a token: %v, want Unauthenticated", err)
	}
	checkCleanup(t, r)

	cfg.Token = token
	rep, err := run(context.Background(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	checkCleanup(t, r)
	if !rep.Passed() || rep.Writes != 3 {
		t.Fatalf("report = %+v", rep)
	}
	snap, err := st.Snapshot(context.Background(), cfg.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if by := snap.Configs[0].UpdatedBy; by != "load-bot" {
		t.Fatalf("writes attributed to %q, want the token's name", by)
	}
}

// adminOnlyServer serves the real AdminService, so that cpload's namespace
// check passes, next to dist (nil: no DistributionService at all).
func adminOnlyServer(t *testing.T, dist cpv1.DistributionServiceServer) (addr string) {
	t.Helper()
	g := grpc.NewServer()
	srv := server.New(server.Options{Store: memory.New(), Logger: slog.New(slog.DiscardHandler)})
	cpv1.RegisterAdminServiceServer(g, srv.AdminServer())
	if dist != nil {
		cpv1.RegisterDistributionServiceServer(g, dist)
	}
	return serve(t, g)
}

func TestRunFailsToStartWhenWatchersFail(t *testing.T) {
	cfg := testConfig(adminOnlyServer(t, nil))
	cfg.Watchers = 3
	_, err := run(context.Background(), cfg, io.Discard)
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "3 of 3 watchers failed to start") {
		t.Fatalf("run = %v, want every watcher failing with Unimplemented", err)
	}
	checkNoLeaks(t)
}

// fakeDistribution sends a snapshot for each of revisions on every Watch
// stream, then holds the stream open without sending anything more.
type fakeDistribution struct {
	cpv1.UnimplementedDistributionServiceServer
	revisions    []int64
	opened, open atomic.Int64
}

func (f *fakeDistribution) Watch(req *cpv1.WatchRequest, stream grpc.ServerStreamingServer[cpv1.WatchResponse]) error {
	f.opened.Add(1)
	f.open.Add(1)
	defer f.open.Add(-1)
	for _, rev := range f.revisions {
		if err := stream.Send(&cpv1.WatchResponse{Snapshot: &cpv1.Snapshot{Namespace: req.GetNamespace(), Revision: rev}}); err != nil {
			return err
		}
	}
	<-stream.Context().Done()
	return status.FromContextError(stream.Context().Err()).Err()
}

// TestWatcherRejectsRevisionsGoingBackwards: convergence relies on revisions
// arriving in increasing order, so a violation fails the watcher, which
// must close its stream although the run goes on.
func TestWatcherRejectsRevisionsGoingBackwards(t *testing.T) {
	fake := &fakeDistribution{revisions: []int64{5, 3}}
	cfg := testConfig(adminOnlyServer(t, fake))
	cfg.Watchers = 1
	conns, err := dial(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll(conns)

	lt := &loadTest{cfg: cfg, conns: conns, start: time.Now(), track: newTracker(cfg.Watchers)}
	w := &watcher{addr: conns[0].addr}
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		lt.watch(context.Background(), 0, w, cpv1.NewDistributionServiceClient(conns[0].cc))
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher accepted revision 3 after revision 5")
	}
	if w.err == nil || !strings.Contains(w.err.Error(), "watch "+w.addr+": revision 3 arrived after revision 5") {
		t.Fatalf("watcher error = %v", w.err)
	}
	if len(w.deliveries) != 1 || w.deliveries[0].revision != 5 {
		t.Fatalf("deliveries = %v, want only revision 5", w.deliveries)
	}
	if _, failed, first := lt.track.started(); failed != 1 || first != w.err {
		t.Fatalf("tracker: %d failed, first %v; want the watcher's error", failed, first)
	}
	eventually(t, "the failed watcher's stream to close", func() bool { return fake.open.Load() == 0 })
}

// TestStopWatchersEndsEveryStream checks the cleanup contract run relies on:
// when stopWatchers returns no watcher goroutine is left, and the server
// sees every stream end.
func TestStopWatchersEndsEveryStream(t *testing.T) {
	silent := &fakeDistribution{}
	cfg := testConfig(adminOnlyServer(t, silent))
	cfg.Watchers = 8
	conns, err := dial(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll(conns)

	lt := &loadTest{cfg: cfg, conns: conns, start: time.Now(), track: newTracker(cfg.Watchers)}
	lt.startWatchers(context.Background())
	eventually(t, "every Watch stream to open", func() bool { return silent.open.Load() == 8 })
	if n := len(leakedGoroutines()); n != 8 {
		t.Fatalf("leak check sees %d live watcher goroutines, want 8", n)
	}
	lt.stopWatchers()
	checkNoLeaks(t)
	eventually(t, "every Watch stream to close", func() bool { return silent.open.Load() == 0 })
	if _, failed, _ := lt.track.started(); failed != 0 {
		t.Fatalf("%d watchers counted as failed after a deliberate stop", failed)
	}
}

// TestRunGivesUpWhenNoSnapshotArrives times out while every watcher is still
// waiting for its first snapshot: run must end the live streams itself.
func TestRunGivesUpWhenNoSnapshotArrives(t *testing.T) {
	silent := &fakeDistribution{}
	cfg := testConfig(adminOnlyServer(t, silent))
	cfg.Watchers, cfg.Timeout = 4, 200*time.Millisecond
	_, err := run(context.Background(), cfg, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "(0 of 4 watchers ready): timed out after 200ms") {
		t.Fatalf("run = %v, want a timeout waiting for initial snapshots", err)
	}
	checkNoLeaks(t)
	if opened := silent.opened.Load(); opened != 4 {
		t.Fatalf("%d Watch streams opened, want 4", opened)
	}
	eventually(t, "the silent server's Watch streams to close", func() bool { return silent.open.Load() == 0 })
}

func TestCLI(t *testing.T) {
	t.Setenv("CPLOAD_TOKEN", "")
	r := startReplica(t, memory.New(), notify.NewLocal())
	runCLI := func(args ...string) (code int, stdout, stderr string) {
		t.Helper()
		var out, errOut bytes.Buffer
		code = cli(context.Background(), args, &out, &errOut)
		return code, out.String(), errOut.String()
	}

	if code, out, _ := runCLI("--help"); code != 0 || !strings.Contains(out, "Usage:") {
		t.Fatalf("--help: exit %d, output:\n%s", code, out)
	}
	if code, _, errOut := runCLI("--watchers", "0"); code != 2 || !strings.Contains(errOut, "--watchers") {
		t.Fatalf("--watchers 0: exit %d, stderr:\n%s", code, errOut)
	}

	small := []string{"--addrs", r.addr, "--watchers", "5", "--writes", "3", "--interval", "1ms"}
	code, out, errOut := runCLI(small...)
	if code != 0 || !strings.Contains(out, "result        PASS") || !strings.Contains(errOut, "5 watchers ready") {
		t.Fatalf("passing run: exit %d, stdout:\n%s\nstderr:\n%s", code, out, errOut)
	}

	// No write converges in a nanosecond.
	code, out, _ = runCLI(append(small, "--slo", "1ns", "--json")...)
	var rep struct {
		Passed   bool     `json:"passed"`
		Writes   int      `json:"writes"`
		Failures []string `json:"failures"`
	}
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("--json output %q: %v", out, err)
	}
	if code != 1 || rep.Passed || rep.Writes != 3 || len(rep.Failures) != 1 || !strings.Contains(rep.Failures[0], "exceeds slo 1ns") {
		t.Fatalf("run over the slo: exit %d, report %+v", code, rep)
	}
	checkCleanup(t, r)

	// Nothing listens on a closed listener's address.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis.Close()
	if code, out, errOut := runCLI("--addrs", lis.Addr().String(), "--watchers", "5"); code != 1 || out != "" ||
		!strings.Contains(errOut, "cpload: namespace loadtest:") {
		t.Fatalf("unreachable server: exit %d, stdout:\n%s\nstderr:\n%s", code, out, errOut)
	}
}
