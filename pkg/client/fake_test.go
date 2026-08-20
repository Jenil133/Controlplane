package client

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// The tests run in synctest bubbles: gRPC over bufconn only ever blocks in
// ways synctest recognizes, so fake time drives backoff and timeouts, and
// synctest.Wait returns once the client has handled whatever the test did.

const testNamespace = "checkout/prod"

// fakeServer is a DistributionService whose Watch calls the test drives.
type fakeServer struct {
	cpv1.UnimplementedDistributionServiceServer
	watches chan *fakeWatch
}

// fakeWatch is one Watch call, in the server's view.
type fakeWatch struct {
	req  *cpv1.WatchRequest
	md   metadata.MD
	send chan *cpv1.Snapshot
	end  chan error
	done chan struct{} // closed when the call has returned
}

func (s *fakeServer) Watch(req *cpv1.WatchRequest, stream grpc.ServerStreamingServer[cpv1.WatchResponse]) error {
	ctx := stream.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	w := &fakeWatch{req: req, md: md, send: make(chan *cpv1.Snapshot), end: make(chan error), done: make(chan struct{})}
	defer close(w.done)
	select {
	case s.watches <- w:
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		select {
		case snap := <-w.send:
			if err := stream.Send(&cpv1.WatchResponse{Snapshot: snap}); err != nil {
				return err
			}
		case err := <-w.end:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// push sends snap down the stream and waits until the client has handled it.
func (w *fakeWatch) push(t *testing.T, snap *cpv1.Snapshot) {
	t.Helper()
	select {
	case w.send <- snap:
	case <-w.done:
		t.Fatalf("push of revision %d: the watch has ended", snap.GetRevision())
	}
	synctest.Wait()
}

// close ends the stream with err (nil ends it cleanly) and waits until the
// client has handled that.
func (w *fakeWatch) close(t *testing.T, err error) {
	t.Helper()
	select {
	case w.end <- err:
	case <-w.done:
		t.Fatal("close: the watch has already ended")
	}
	<-w.done
	synctest.Wait()
}

// ended reports whether the call has returned, e.g. because the client went
// away.
func (w *fakeWatch) ended() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// env is a control plane the test can take down and bring back. It must be
// created inside a synctest bubble.
type env struct {
	t    *testing.T
	srv  *fakeServer
	logs *logRecorder

	mu  sync.Mutex
	lis *bufconn.Listener // nil while down
	gs  *grpc.Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, srv: &fakeServer{watches: make(chan *fakeWatch, 16)}, logs: &logRecorder{}}
	e.up()
	t.Cleanup(e.down)
	return e
}

// up starts serving.
func (e *env) up() {
	lis := bufconn.Listen(1 << 20)
	// The real server's keepalive policy, which the client's pings respect.
	gs := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))
	cpv1.RegisterDistributionServiceServer(gs, e.srv)
	go func() { _ = gs.Serve(lis) }()
	e.mu.Lock()
	e.lis, e.gs = lis, gs
	e.mu.Unlock()
}

// down stops the server, dropping every connection; dialing fails until up.
func (e *env) down() {
	e.mu.Lock()
	gs := e.gs
	e.lis, e.gs = nil, nil
	e.mu.Unlock()
	if gs != nil {
		gs.Stop()
	}
}

func (e *env) dial(ctx context.Context, _ string) (net.Conn, error) {
	e.mu.Lock()
	lis := e.lis
	e.mu.Unlock()
	if lis == nil {
		return nil, errors.New("connection refused")
	}
	return lis.DialContext(ctx)
}

// options returns client options for this control plane.
func (e *env) options() Options {
	return Options{
		Address:     "passthrough:///controlplane",
		Namespace:   testNamespace,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(e.dial)},
		Logger:      slog.New(e.logs),
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  time.Second,
	}
}

// start creates a client that is closed when the test ends.
func (e *env) start(opts Options) *Client {
	e.t.Helper()
	c, err := New(context.Background(), opts)
	if err != nil {
		e.t.Fatalf("New: %v", err)
	}
	e.t.Cleanup(func() { c.Close() })
	return c
}

// accept returns the next Watch call, failing the test if none comes within
// an hour of fake time.
func (e *env) accept() *fakeWatch {
	e.t.Helper()
	select {
	case w := <-e.srv.watches:
		return w
	case <-time.After(time.Hour):
		e.t.Fatal("no Watch call within an hour")
		return nil
	}
}

// connect starts a client and serves it snap on its first Watch call.
func (e *env) connect(opts Options, snap *cpv1.Snapshot) (*Client, *fakeWatch) {
	e.t.Helper()
	c := e.start(opts)
	w := e.accept()
	w.push(e.t, snap)
	return c, w
}

// logRecorder is a slog.Handler that keeps every record.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecorder) WithGroup(string) slog.Handler      { return h }

// count returns how many records at level have a message containing msg.
func (h *logRecorder) count(level slog.Level, msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, msg) {
			n++
		}
	}
	return n
}

// snap returns a snapshot of the test namespace at rev holding entries,
// which may be *cpv1.Config, *cpv1.Flag, *cpv1.Experiment, *cpv1.RateLimit
// or *cpv1.CircuitBreaker.
func snap(rev int64, entries ...any) *cpv1.Snapshot {
	s := &cpv1.Snapshot{Namespace: testNamespace, Revision: rev, UpdatedAt: timestamppb.New(time.Unix(1700000000, 0))}
	for _, e := range entries {
		switch e := e.(type) {
		case *cpv1.Config:
			s.Configs = append(s.Configs, e)
		case *cpv1.Flag:
			s.Flags = append(s.Flags, e)
		case *cpv1.Experiment:
			s.Experiments = append(s.Experiments, e)
		case *cpv1.RateLimit:
			s.RateLimits = append(s.RateLimits, e)
		case *cpv1.CircuitBreaker:
			s.CircuitBreakers = append(s.CircuitBreakers, e)
		default:
			panic("snap: unexpected entry type")
		}
	}
	return s
}

func config(t *testing.T, key string, v any) *cpv1.Config {
	t.Helper()
	pv, err := structpb.NewValue(v)
	if err != nil {
		t.Fatal(err)
	}
	return &cpv1.Config{Key: key, Value: pv}
}

func flag(key string, enabled bool, percent float64, allow ...string) *cpv1.Flag {
	return &cpv1.Flag{Key: key, Enabled: enabled, RolloutPercent: percent, Salt: key, Allowlist: allow}
}

func rateLimit(key string, rps float64, burst uint32) *cpv1.RateLimit {
	return &cpv1.RateLimit{Key: key, Enabled: true, RequestsPerSecond: rps, Burst: burst}
}

// circuitBreaker opens once at least minRequests calls in a 10s window
// failed at a rate of at least threshold, and admits one trial call after
// open.
func circuitBreaker(key string, threshold float64, minRequests uint32, open time.Duration) *cpv1.CircuitBreaker {
	return &cpv1.CircuitBreaker{
		Key:                  key,
		Enabled:              true,
		FailureRateThreshold: threshold,
		MinRequests:          minRequests,
		Window:               durationpb.New(10 * time.Second),
		OpenDuration:         durationpb.New(open),
		HalfOpenMaxRequests:  1,
	}
}
