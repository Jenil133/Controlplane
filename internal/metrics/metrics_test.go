package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/server"
)

// Exposition headers of the gRPC metrics, for tests that compare them as text.
const (
	requestsHeader = `
# HELP controlplane_grpc_requests_total Completed gRPC requests (unary and streaming), by full method name and status code.
# TYPE controlplane_grpc_requests_total counter
`
	durationHeader = `
# HELP controlplane_grpc_request_duration_seconds Duration of unary gRPC requests, by full method name.
# TYPE controlplane_grpc_request_duration_seconds histogram
`
)

// expectMetrics compares the named metric families in m's registry with
// want, given in the text exposition format.
func expectMetrics(t *testing.T, m *Metrics, want string, names ...string) {
	t.Helper()
	if err := testutil.GatherAndCompare(m.Registry(), strings.NewReader(want), names...); err != nil {
		t.Fatal(err)
	}
}

// observations returns how many values the histogram family name holds,
// summed over its series.
func observations(t *testing.T, m *Metrics, name string) uint64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			n += metric.GetHistogram().GetSampleCount()
		}
	}
	return n
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("got %v (%v), want code %v", got, err, code)
	}
}

func TestObserver(t *testing.T) {
	m := New()
	m.WatchStarted("checkout")
	m.WatchStarted("checkout")
	m.WatchStarted("search")
	m.WatchEnded("checkout")
	m.WatchEnded("search")

	// Lags are binary fractions of a second so the expected _sum is exact.
	m.SnapshotPushed("checkout", 7812500*time.Nanosecond) // 2^-7 s
	m.SnapshotPushed("checkout", 250*time.Millisecond)    // bucket bounds are inclusive
	m.SnapshotPushed("checkout", 12*time.Second)          // past the last bound: +Inf only
	m.SnapshotPushed("search", 500*time.Millisecond)
	m.SnapshotPushed("search", -time.Second) // clock skew: observed as 0

	m.ChangeReceived("checkout", server.SourceWrite)
	m.ChangeReceived("search", server.SourceWrite)
	m.ChangeReceived("checkout", server.SourceNotifier)

	m.PublishFailed("checkout")
	m.PublishFailed("search")

	expectMetrics(t, m, `
# HELP controlplane_watch_streams Open Watch streams on this replica, by namespace.
# TYPE controlplane_watch_streams gauge
controlplane_watch_streams{namespace="checkout"} 1
controlplane_watch_streams{namespace="search"} 0
# HELP controlplane_snapshots_pushed_total Snapshots sent on Watch streams, by namespace.
# TYPE controlplane_snapshots_pushed_total counter
controlplane_snapshots_pushed_total{namespace="checkout"} 3
controlplane_snapshots_pushed_total{namespace="search"} 2
# HELP controlplane_push_lag_seconds Time from a change being committed to its snapshot being sent on a Watch stream.
# TYPE controlplane_push_lag_seconds histogram
controlplane_push_lag_seconds_bucket{le="0.001"} 1
controlplane_push_lag_seconds_bucket{le="0.0025"} 1
controlplane_push_lag_seconds_bucket{le="0.005"} 1
controlplane_push_lag_seconds_bucket{le="0.01"} 2
controlplane_push_lag_seconds_bucket{le="0.025"} 2
controlplane_push_lag_seconds_bucket{le="0.05"} 2
controlplane_push_lag_seconds_bucket{le="0.1"} 2
controlplane_push_lag_seconds_bucket{le="0.25"} 3
controlplane_push_lag_seconds_bucket{le="0.5"} 4
controlplane_push_lag_seconds_bucket{le="1"} 4
controlplane_push_lag_seconds_bucket{le="2.5"} 4
controlplane_push_lag_seconds_bucket{le="5"} 4
controlplane_push_lag_seconds_bucket{le="10"} 4
controlplane_push_lag_seconds_bucket{le="+Inf"} 5
controlplane_push_lag_seconds_sum 12.7578125
controlplane_push_lag_seconds_count 5
# HELP controlplane_changes_received_total Namespace revisions this replica learned about, by source.
# TYPE controlplane_changes_received_total counter
controlplane_changes_received_total{source="notifier"} 1
controlplane_changes_received_total{source="reconcile"} 0
controlplane_changes_received_total{source="rollout"} 0
controlplane_changes_received_total{source="write"} 2
# HELP controlplane_publish_failures_total Change events that could not be broadcast to the other replicas.
# TYPE controlplane_publish_failures_total counter
controlplane_publish_failures_total 2
`,
		"controlplane_watch_streams",
		"controlplane_snapshots_pushed_total",
		"controlplane_push_lag_seconds",
		"controlplane_changes_received_total",
		"controlplane_publish_failures_total",
	)
}

// TestUnaryServerInterceptor runs in a synctest bubble, where time only moves
// when the handler sleeps, so the recorded durations are exact.
func TestUnaryServerInterceptor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New()
		intercept := m.UnaryServerInterceptor()
		call := func(method string, took time.Duration, err error) {
			t.Helper()
			info := &grpc.UnaryServerInfo{FullMethod: method}
			resp, gotErr := intercept(context.Background(), "request", info, func(_ context.Context, req any) (any, error) {
				time.Sleep(took)
				return req, err
			})
			if resp != "request" || gotErr != err {
				t.Fatalf("interceptor returned (%v, %v), want the handler's (request, %v)", resp, gotErr, err)
			}
		}
		call(cpv1.AdminService_PutConfig_FullMethodName, 7812500*time.Nanosecond, nil)
		call(cpv1.AdminService_PutConfig_FullMethodName, 250*time.Millisecond, status.Error(codes.Aborted, "conflict"))
		call(cpv1.AdminService_GetNamespace_FullMethodName, 0, status.Error(codes.NotFound, "no such namespace"))

		expectMetrics(t, m, requestsHeader+`
controlplane_grpc_requests_total{code="Aborted",method="/controlplane.v1.AdminService/PutConfig"} 1
controlplane_grpc_requests_total{code="NotFound",method="/controlplane.v1.AdminService/GetNamespace"} 1
controlplane_grpc_requests_total{code="OK",method="/controlplane.v1.AdminService/PutConfig"} 1
`+durationHeader+`
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.001"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.0025"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.005"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.01"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.025"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.05"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.1"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.25"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="0.5"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="1"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="2.5"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="5"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="10"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/GetNamespace",le="+Inf"} 1
controlplane_grpc_request_duration_seconds_sum{method="/controlplane.v1.AdminService/GetNamespace"} 0
controlplane_grpc_request_duration_seconds_count{method="/controlplane.v1.AdminService/GetNamespace"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.001"} 0
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.0025"} 0
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.005"} 0
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.01"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.025"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.05"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.1"} 1
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.25"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="0.5"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="1"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="2.5"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="5"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="10"} 2
controlplane_grpc_request_duration_seconds_bucket{method="/controlplane.v1.AdminService/PutConfig",le="+Inf"} 2
controlplane_grpc_request_duration_seconds_sum{method="/controlplane.v1.AdminService/PutConfig"} 0.2578125
controlplane_grpc_request_duration_seconds_count{method="/controlplane.v1.AdminService/PutConfig"} 2
`, "controlplane_grpc_requests_total", "controlplane_grpc_request_duration_seconds")
	})
}

// TestStatusCodes checks that both interceptors label a request with the
// code the client receives and return the handler's error untouched.
func TestStatusCodes(t *testing.T) {
	const unaryMethod, streamMethod = cpv1.AdminService_PutFlag_FullMethodName, cpv1.DistributionService_Watch_FullMethodName
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"success", nil, codes.OK},
		{"status error", status.Error(codes.InvalidArgument, "bad key"), codes.InvalidArgument},
		{"wrapped status error", fmt.Errorf("put flag: %w", status.Error(codes.Aborted, "conflict")), codes.Aborted},
		{"context canceled", context.Canceled, codes.Canceled},
		{"wrapped deadline", fmt.Errorf("load snapshot: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"plain error", errors.New("boom"), codes.Unknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := New()
			_, err := m.UnaryServerInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: unaryMethod},
				func(context.Context, any) (any, error) { return nil, tc.err })
			if err != tc.err {
				t.Fatalf("unary interceptor returned %v, want the handler's %v", err, tc.err)
			}
			err = m.StreamServerInterceptor()(nil, nil, &grpc.StreamServerInfo{FullMethod: streamMethod, IsServerStream: true},
				func(any, grpc.ServerStream) error { return tc.err })
			if err != tc.err {
				t.Fatalf("stream interceptor returned %v, want the handler's %v", err, tc.err)
			}
			expectMetrics(t, m, requestsHeader+fmt.Sprintf(
				"controlplane_grpc_requests_total{code=%q,method=%q} 1\ncontrolplane_grpc_requests_total{code=%q,method=%q} 1\n",
				tc.want, unaryMethod, tc.want, streamMethod,
			), "controlplane_grpc_requests_total")
		})
	}
}

// fakeAdmin serves GetNamespace; every other method keeps the embedded
// Unimplemented behaviour.
type fakeAdmin struct {
	cpv1.UnimplementedAdminServiceServer
}

func (fakeAdmin) GetNamespace(_ context.Context, req *cpv1.GetNamespaceRequest) (*cpv1.GetNamespaceResponse, error) {
	if req.GetName() != "checkout" {
		return nil, status.Error(codes.NotFound, "no such namespace")
	}
	return &cpv1.GetNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName()}}, nil
}

// fakeDistribution sends one snapshot per Watch, then holds the stream open
// until release is closed and ends it the way a replica shutting down does.
type fakeDistribution struct {
	cpv1.UnimplementedDistributionServiceServer
	release chan struct{}
}

func (f fakeDistribution) Watch(req *cpv1.WatchRequest, stream grpc.ServerStreamingServer[cpv1.WatchResponse]) error {
	if err := stream.Send(&cpv1.WatchResponse{Snapshot: &cpv1.Snapshot{Namespace: req.GetNamespace(), Revision: 1}}); err != nil {
		return err
	}
	select {
	case <-f.release:
		return status.Error(codes.Unavailable, "server is shutting down, reconnect")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

// TestInterceptorsOverGRPC installs both interceptors on a real gRPC server
// and checks labels, and that a stream is counted only once it ends.
func TestInterceptorsOverGRPC(t *testing.T) {
	m := New()
	release := make(chan struct{})
	g := grpc.NewServer(
		grpc.ChainUnaryInterceptor(m.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(m.StreamServerInterceptor()),
	)
	cpv1.RegisterAdminServiceServer(g, fakeAdmin{})
	cpv1.RegisterDistributionServiceServer(g, fakeDistribution{release: release})
	lis := bufconn.Listen(1 << 20)
	go func() { _ = g.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		g.Stop()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin := cpv1.NewAdminServiceClient(conn)
	if _, err := admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "checkout"}); err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	_, err = admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "missing"})
	wantCode(t, err, codes.NotFound)
	_, err = admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: "checkout", Key: "k"})
	wantCode(t, err, codes.Unimplemented)

	stream, err := cpv1.NewDistributionServiceClient(conn).Watch(ctx, &cpv1.WatchRequest{Namespace: "checkout"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}

	// grpc-go sends a unary response only after the interceptors return, so
	// the calls above are recorded; the Watch is still open and is not.
	unary := requestsHeader + `
controlplane_grpc_requests_total{code="NotFound",method="/controlplane.v1.AdminService/GetNamespace"} 1
controlplane_grpc_requests_total{code="OK",method="/controlplane.v1.AdminService/GetNamespace"} 1
controlplane_grpc_requests_total{code="Unimplemented",method="/controlplane.v1.AdminService/PutConfig"} 1
`
	expectMetrics(t, m, unary, "controlplane_grpc_requests_total")

	// The final status is written after the stream interceptor returns.
	close(release)
	_, err = stream.Recv()
	wantCode(t, err, codes.Unavailable)
	expectMetrics(t, m, unary+`controlplane_grpc_requests_total{code="Unavailable",method="/controlplane.v1.DistributionService/Watch"} 1
`, "controlplane_grpc_requests_total")

	if n := testutil.CollectAndCount(m.grpcDuration); n != 2 {
		t.Errorf("duration series = %d, want 2 (GetNamespace and PutConfig; streams are not timed)", n)
	}
	if n := observations(t, m, "controlplane_grpc_request_duration_seconds"); n != 3 {
		t.Errorf("duration observations = %d, want 3", n)
	}
}

// scrape fetches m's handler and returns the body of a successful response.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body:\n%s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want the text format", ct)
	}
	return rec.Body.String()
}

// expectLines checks that body has a line starting with each of lines.
func expectLines(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(body, "\n"+line) {
			t.Errorf("scrape lacks a line starting %q", line)
		}
	}
}

func TestHandler(t *testing.T) {
	m := New()
	m.WatchStarted("checkout")
	expectLines(t, scrape(t, m),
		`controlplane_watch_streams{namespace="checkout"} 1`+"\n",
		"go_goroutines ",
		"process_cpu_seconds_total ",
		`promhttp_metric_handler_requests_total{code="200"} 0`+"\n",
	)
	// The handler counts its scrapes on m's registry.
	expectLines(t, scrape(t, m), `promhttp_metric_handler_requests_total{code="200"} 1`+"\n")
}

// failingCollector always fails, like a process collector that cannot read
// /proc.
type failingCollector struct{ desc *prometheus.Desc }

func (c failingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c failingCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.NewInvalidMetric(c.desc, errors.New("cannot read /proc"))
}

func TestHandlerServesDespiteFailingCollector(t *testing.T) {
	m := New()
	m.Registry().MustRegister(failingCollector{prometheus.NewDesc("broken", "Always fails.", nil, nil)})
	m.WatchStarted("checkout")
	expectLines(t, scrape(t, m), `controlplane_watch_streams{namespace="checkout"} 1`+"\n")
	// The failure of the first scrape is counted where alerts can see it.
	expectLines(t, scrape(t, m), `promhttp_metric_handler_errors_total{cause="gathering"} 1`+"\n")
}

func TestNewUsesOwnRegistry(t *testing.T) {
	a, b := New(), New() // a shared registry would panic on the second registration
	a.PublishFailed("checkout")
	if got := testutil.ToFloat64(a.publishFailures); got != 1 {
		t.Fatalf("publish failures = %v, want 1", got)
	}
	if got := testutil.ToFloat64(b.publishFailures); got != 0 {
		t.Fatalf("other instance counts %v publish failures, want 0", got)
	}

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if name := f.GetName(); strings.HasPrefix(name, "controlplane_") || strings.HasPrefix(name, "promhttp_") {
			t.Errorf("%s is registered on the global default registry", name)
		}
	}
}

// TestConcurrentUse records from many goroutines while scraping; it is meant
// for -race.
func TestConcurrentUse(t *testing.T) {
	m := New()
	unary := m.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: cpv1.AdminService_PutConfig_FullMethodName}
	ok := func(context.Context, any) (any, error) { return nil, nil }

	stop := make(chan struct{})
	scraped := make(chan struct{})
	go func() {
		defer close(scraped)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rec := httptest.NewRecorder()
			m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if rec.Code != http.StatusOK {
				t.Errorf("scrape status = %d", rec.Code)
				return
			}
		}
	}()

	const workers, rounds = 8, 250
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range rounds {
				m.WatchStarted("checkout")
				m.SnapshotPushed("checkout", time.Millisecond)
				m.ChangeReceived("checkout", server.SourceNotifier)
				m.PublishFailed("checkout")
				_, _ = unary(context.Background(), nil, info, ok)
				m.WatchEnded("checkout")
			}
		})
	}
	wg.Wait()
	close(stop)
	<-scraped

	const n = workers * rounds
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		{"watch streams", testutil.ToFloat64(m.watchStreams.WithLabelValues("checkout")), 0},
		{"snapshots pushed", testutil.ToFloat64(m.snapshotsPushed.WithLabelValues("checkout")), n},
		{"push lag observations", float64(observations(t, m, "controlplane_push_lag_seconds")), n},
		{"changes received", testutil.ToFloat64(m.changesReceived.WithLabelValues(string(server.SourceNotifier))), n},
		{"publish failures", testutil.ToFloat64(m.publishFailures), n},
		{"grpc requests", testutil.ToFloat64(m.grpcRequests.WithLabelValues(info.FullMethod, codes.OK.String())), n},
		{"grpc duration observations", float64(observations(t, m, "controlplane_grpc_request_duration_seconds")), n},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}
