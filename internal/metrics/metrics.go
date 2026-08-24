// Package metrics exports the control plane's Prometheus metrics: Watch
// stream activity and change propagation (as a server.Observer) and gRPC
// request counts and latencies (as interceptors).
//
// Each Metrics has its own registry, never the global default one, so
// several servers can run in one process (tests, end-to-end harnesses)
// without duplicate-registration panics or mixed-up numbers.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Jenil133/Controlplane/internal/server"
)

var _ server.Observer = (*Metrics)(nil)

// latencyBuckets (seconds) resolve the 1s propagation budget finely and stop
// at 10s, the default reconcile interval, which bounds how late a change
// missed by pub/sub arrives. Unary RPCs are store round trips of a few
// milliseconds, so they use the same layout.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// Metrics records the control plane's metrics in its own registry. It
// implements server.Observer; all methods are safe for concurrent use and
// never block.
type Metrics struct {
	registry *prometheus.Registry
	handler  http.Handler

	watchStreams    *prometheus.GaugeVec
	snapshotsPushed *prometheus.CounterVec
	pushLag         prometheus.Histogram
	changesReceived *prometheus.CounterVec
	publishFailures prometheus.Counter
	grpcRequests    *prometheus.CounterVec
	grpcDuration    *prometheus.HistogramVec
}

// New returns Metrics registered on a fresh registry that also carries the
// Go runtime and process collectors.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		watchStreams: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "controlplane_watch_streams",
			Help: "Open Watch streams on this replica, by namespace.",
		}, []string{"namespace"}),
		snapshotsPushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "controlplane_snapshots_pushed_total",
			Help: "Snapshots sent on Watch streams, by namespace.",
		}, []string{"namespace"}),
		pushLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "controlplane_push_lag_seconds",
			Help:    "Time from a change being committed to its snapshot being sent on a Watch stream.",
			Buckets: latencyBuckets,
		}),
		changesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "controlplane_changes_received_total",
			Help: "Namespace revisions this replica learned about, by source.",
		}, []string{"source"}),
		publishFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "controlplane_publish_failures_total",
			Help: "Change events that could not be broadcast to the other replicas.",
		}),
		// grpc-go answers unknown methods before any interceptor runs (unless
		// an UnknownServiceHandler is installed), so "method" only holds
		// registered method names and its cardinality stays bounded.
		grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "controlplane_grpc_requests_total",
			Help: "Completed gRPC requests (unary and streaming), by full method name and status code.",
		}, []string{"method", "code"}),
		grpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "controlplane_grpc_request_duration_seconds",
			Help:    "Duration of unary gRPC requests, by full method name.",
			Buckets: latencyBuckets,
		}, []string{"method"}),
	}
	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.watchStreams, m.snapshotsPushed, m.pushLag, m.changesReceived,
		m.publishFailures, m.grpcRequests, m.grpcDuration,
	)
	// Export every source from the first scrape so rates and alerts on a
	// source that has not fired yet see 0 rather than no data.
	for _, src := range []server.ChangeSource{server.SourceWrite, server.SourceNotifier, server.SourceReconcile, server.SourceRollout} {
		m.changesReceived.WithLabelValues(string(src))
	}
	m.handler = promhttp.InstrumentMetricHandler(m.registry, promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A failing collector costs only its own metrics, not the whole
		// scrape; such failures are counted in
		// promhttp_metric_handler_errors_total.
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      m.registry,
	}))
	return m
}

// Registry returns the registry holding every metric, e.g. to add collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus exposition format and counts
// its own scrapes in promhttp_metric_handler_requests_total.
func (m *Metrics) Handler() http.Handler { return m.handler }

// WatchStarted implements server.Observer.
func (m *Metrics) WatchStarted(namespace string) {
	m.watchStreams.WithLabelValues(namespace).Inc()
}

// WatchEnded implements server.Observer.
func (m *Metrics) WatchEnded(namespace string) {
	m.watchStreams.WithLabelValues(namespace).Dec()
}

// SnapshotPushed implements server.Observer.
func (m *Metrics) SnapshotPushed(namespace string, lag time.Duration) {
	m.snapshotsPushed.WithLabelValues(namespace).Inc()
	// The commit time is stamped by the database or the writing replica, so
	// clock skew can make lag negative. A negative observation would shrink
	// the histogram's _sum, which rate() reads as a counter reset.
	m.pushLag.Observe(max(lag, 0).Seconds())
}

// ChangeReceived implements server.Observer. The source, not the namespace,
// tells whether pub/sub keeps up (changes arriving by reconcile mean it did
// not), so it is the only label.
func (m *Metrics) ChangeReceived(_ string, source server.ChangeSource) {
	m.changesReceived.WithLabelValues(string(source)).Inc()
}

// PublishFailed implements server.Observer. Failures describe the link to
// the notifier rather than a namespace; the server's log names the namespace.
func (m *Metrics) PublishFailed(string) {
	m.publishFailures.Inc()
}

// UnaryServerInterceptor counts unary requests by full method name and status
// code and records their duration. Chain it before the auth interceptor so
// rejected calls are counted too.
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.grpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		m.grpcRequests.WithLabelValues(info.FullMethod, statusCode(err).String()).Inc()
		return resp, err
	}
}

// StreamServerInterceptor counts streaming requests by full method name and
// the status code they end with. Streams are not timed: a Watch lasts as long
// as its client stays connected, which says nothing about server latency.
func (m *Metrics) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		m.grpcRequests.WithLabelValues(info.FullMethod, statusCode(err).String()).Inc()
		return err
	}
}

// statusCode returns the code the client receives for a handler's error.
// grpc-go sends a status error's own code, maps bare context errors to
// Canceled or DeadlineExceeded and anything else to Unknown.
func statusCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	return status.FromContextError(err).Code()
}
