package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/ratelimit"
)

// allowed counts how many of n requests for key the client allows.
func allowed(c *Client, key string, n int) int {
	ok := 0
	for range n {
		if c.Allow(key) {
			ok++
		}
	}
	return ok
}

func TestRateLimitsReconfiguredLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(1, rateLimit("checkout", 1, 2)))
		if got := allowed(c, "checkout", 10); got != 2 {
			t.Fatalf("allowed %d of 10 with burst 2, want 2", got)
		}
		if got := allowed(c, "search", 10); got != 10 {
			t.Fatalf("allowed %d of 10 for a key without a policy", got)
		}

		// A larger burst does not refill the bucket: the limiter keeps its
		// state across the reconfiguration.
		w.push(t, snap(2, rateLimit("checkout", 1, 5)))
		if got := allowed(c, "checkout", 10); got != 0 {
			t.Fatalf("allowed %d right after reconfiguring, want 0", got)
		}
		time.Sleep(3 * time.Second)
		if got := allowed(c, "checkout", 10); got != 3 {
			t.Fatalf("allowed %d after 3s at 1/s, want 3", got)
		}

		disabled := rateLimit("checkout", 1, 5)
		disabled.Enabled = false
		w.push(t, snap(3, disabled))
		if got := allowed(c, "checkout", 100); got != 100 {
			t.Fatalf("allowed %d of 100 with the policy disabled", got)
		}

		// Enabled again, the key starts with a full bucket.
		w.push(t, snap(4, rateLimit("checkout", 1, 5)))
		if got := allowed(c, "checkout", 10); got != 5 {
			t.Fatalf("allowed %d of 10 after re-enabling with burst 5, want 5", got)
		}
	})
}

func TestBreakersReconfiguredLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(1, circuitBreaker("payments", 0.5, 2, time.Second)))
		down := errors.New("payments down")
		calls := 0
		fail := func() error { calls++; return down }
		for range 2 {
			if err := c.Do("payments", fail); !errors.Is(err, down) {
				t.Fatalf("Do = %v, want the call's error", err)
			}
		}
		if err := c.Do("payments", fail); !errors.Is(err, breaker.ErrOpen) || calls != 2 {
			t.Fatalf("Do after 2 failures = %v with %d calls, want ErrOpen without calling", err, calls)
		}
		b, ok := c.Breaker("payments")
		if !ok || b.State() != breaker.Open {
			t.Fatalf("Breaker(payments) = %v, %v; want an open breaker", b, ok)
		}
		if e.logs.count(slog.LevelWarn, "circuit breaker changed state") != 1 {
			t.Error("opening the breaker was not logged")
		}

		// Reconfiguring keeps the breaker and its state; the longer open
		// duration applies from when it opened.
		w.push(t, snap(2, circuitBreaker("payments", 0.5, 2, time.Minute)))
		if again, _ := c.Breaker("payments"); again != b {
			t.Fatal("the breaker was replaced instead of reconfigured")
		}
		time.Sleep(time.Second)
		if s := b.State(); s != breaker.Open {
			t.Fatalf("state %v after the old open duration, want open", s)
		}
		time.Sleep(time.Minute)
		if err := c.Do("payments", func() error { return nil }); err != nil {
			t.Fatalf("trial call: %v", err)
		}
		if s := b.State(); s != breaker.Closed {
			t.Fatalf("state %v after a successful trial, want closed", s)
		}

		w.push(t, snap(3))
		if _, ok := c.Breaker("payments"); ok {
			t.Fatal("breaker kept after its policy was removed")
		}
		for range 10 {
			_ = c.Do("payments", fail)
		}
		if calls != 12 {
			t.Fatalf("fn called %d times, want every call through without a policy", calls)
		}
	})
}

func TestPolicyConversion(t *testing.T) {
	rl := rateLimitPolicies([]*cpv1.RateLimit{{Key: "a", Enabled: true, RequestsPerSecond: 2.5, Burst: 7}, {Key: "b"}})
	if want := []ratelimit.Policy{{Key: "a", Enabled: true, RequestsPerSecond: 2.5, Burst: 7}, {Key: "b"}}; !slices.Equal(rl, want) {
		t.Errorf("rateLimitPolicies = %+v, want %+v", rl, want)
	}
	cb := breakerPolicies([]*cpv1.CircuitBreaker{{
		Key: "p", Enabled: true, FailureRateThreshold: 0.25, MinRequests: 9,
		Window: durationpb.New(1500 * time.Millisecond), OpenDuration: durationpb.New(90 * time.Second), HalfOpenMaxRequests: 3,
	}})
	want := breaker.Policy{Key: "p", Enabled: true, Settings: breaker.Settings{
		FailureRateThreshold: 0.25, MinRequests: 9, Window: 1500 * time.Millisecond, OpenDuration: 90 * time.Second, HalfOpenMaxRequests: 3,
	}}
	if len(cb) != 1 || cb[0] != want {
		t.Errorf("breakerPolicies = %+v, want %+v", cb, want)
	}
}

func TestServerInterceptors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		// Validation keeps "" out of real snapshots; exempting must not
		// depend on that.
		c, _ := e.connect(e.options(), snap(1,
			rateLimit("shop.v1.Shop/Buy", 1, 1),
			rateLimit("buy", 1, 1),
			rateLimit("shop.v1.Shop/Feed", 1, 1),
			rateLimit("", 1, 1),
		))
		info := &grpc.UnaryServerInfo{FullMethod: "/shop.v1.Shop/Buy"}
		limited := []codes.Code{codes.OK, codes.ResourceExhausted, codes.ResourceExhausted}
		for _, tc := range []struct {
			name string
			ic   grpc.UnaryServerInterceptor
			want []codes.Code
		}{
			{"default key", c.UnaryServerInterceptor(nil), limited},
			{"custom key", c.UnaryServerInterceptor(func(string) string { return "buy" }), limited},
			{"exempting func", c.UnaryServerInterceptor(func(string) string { return "" }), []codes.Code{codes.OK, codes.OK, codes.OK}},
		} {
			handled := 0
			handler := func(context.Context, any) (any, error) { handled++; return "ok", nil }
			var got []codes.Code
			for range 3 {
				_, err := tc.ic(context.Background(), nil, info, handler)
				got = append(got, status.Code(err))
			}
			if !slices.Equal(got, tc.want) || handled != countOK(tc.want) {
				t.Errorf("%s: codes %v with %d handled, want %v", tc.name, got, handled, tc.want)
			}
		}

		streams := 0
		streamHandler := func(any, grpc.ServerStream) error { streams++; return nil }
		sic := c.StreamServerInterceptor(nil)
		sinfo := &grpc.StreamServerInfo{FullMethod: "/shop.v1.Shop/Feed", IsServerStream: true}
		if err := sic(nil, nil, sinfo, streamHandler); err != nil {
			t.Fatalf("first stream: %v", err)
		}
		err := sic(nil, nil, sinfo, streamHandler)
		if status.Code(err) != codes.ResourceExhausted || streams != 1 {
			t.Fatalf("second stream: %v with %d streams handled, want ResourceExhausted and 1", err, streams)
		}
		if !strings.Contains(err.Error(), "shop.v1.Shop/Feed") {
			t.Errorf("error %q does not name the rate limit", err)
		}
	})
}

func countOK(cs []codes.Code) int {
	n := 0
	for _, c := range cs {
		if c == codes.OK {
			n++
		}
	}
	return n
}

func TestUnaryClientInterceptorCountsFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		// Never opens, so every outcome lands in the window.
		c, _ := e.connect(e.options(), snap(1,
			circuitBreaker("shop.v1.Shop/Buy", 1, 1000, time.Minute),
			circuitBreaker("", 1, 1000, time.Minute),
		))
		ic := c.UnaryClientInterceptor(nil)
		b, _ := c.Breaker("shop.v1.Shop/Buy")

		failures := uint32(0)
		for _, tc := range []struct {
			err     error
			failure bool
		}{
			{nil, false},
			{status.Error(codes.NotFound, "no such order"), false},
			{status.Error(codes.InvalidArgument, "bad order"), false},
			{status.Error(codes.ResourceExhausted, "slow down"), false},
			{status.Error(codes.Canceled, "caller gave up"), false},
			{status.Error(codes.Unavailable, "down"), true},
			{status.Error(codes.DeadlineExceeded, "slow"), true},
			{status.Error(codes.Internal, "bug"), true},
			{status.Error(codes.Unknown, "?"), true},
			{status.Error(codes.DataLoss, "corrupt"), true},
			{errors.New("not a status"), true},
		} {
			invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return tc.err }
			if err := ic(context.Background(), "/shop.v1.Shop/Buy", nil, nil, nil, invoker); err != tc.err {
				t.Fatalf("interceptor returned %v, want the call's %v", err, tc.err)
			}
			if tc.failure {
				failures++
			}
			if got := b.Counts().Failures; got != failures {
				t.Fatalf("after %v: %d failures counted, want %d", tc.err, got, failures)
			}
		}

		// A key func returning "" bypasses the breakers.
		bypass := c.UnaryClientInterceptor(func(string) string { return "" })
		invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Unavailable, "down")
		}
		_ = bypass(context.Background(), "/shop.v1.Shop/Buy", nil, nil, nil, invoker)
		empty, _ := c.Breaker("")
		if got := b.Counts().Failures; got != failures || empty.Counts().Requests != 0 {
			t.Fatalf("a bypassed call was counted: %d failures, want %d; %+v under the empty key",
				got, failures, empty.Counts())
		}
	})
}

// shop is a downstream gRPC service: DistributionService.GetSnapshot stands
// in for any unary method.
type shop struct {
	cpv1.UnimplementedDistributionServiceServer
	calls atomic.Int32
	err   atomic.Pointer[error]
}

func (s *shop) GetSnapshot(context.Context, *cpv1.GetSnapshotRequest) (*cpv1.GetSnapshotResponse, error) {
	s.calls.Add(1)
	if err := s.err.Load(); err != nil {
		return nil, *err
	}
	return &cpv1.GetSnapshotResponse{}, nil
}

func TestInterceptorsOverGRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		const method = "controlplane.v1.DistributionService/GetSnapshot"
		c, w := e.connect(e.options(), snap(1, rateLimit(method, 1, 2)))

		down := &shop{}
		lis := bufconn.Listen(1 << 20)
		gs := grpc.NewServer(grpc.UnaryInterceptor(c.UnaryServerInterceptor(nil)))
		cpv1.RegisterDistributionServiceServer(gs, down)
		go func() { _ = gs.Serve(lis) }()
		t.Cleanup(gs.Stop)
		conn, err := grpc.NewClient("passthrough:///shop",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(c.UnaryClientInterceptor(func(string) string { return "shop" })))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		shopClient := cpv1.NewDistributionServiceClient(conn)
		call := func() error {
			_, err := shopClient.GetSnapshot(context.Background(), &cpv1.GetSnapshotRequest{})
			return err
		}

		// The downstream server enforces the rate limit.
		for i, want := range []codes.Code{codes.OK, codes.OK, codes.ResourceExhausted} {
			if err := call(); status.Code(err) != want {
				t.Fatalf("call %d: %v, want %v", i, err, want)
			}
		}

		// The next revision lifts the limit and guards the calls with a
		// breaker.
		w.push(t, snap(2, circuitBreaker("shop", 0.5, 2, time.Minute)))
		unavailable := status.Error(codes.Unavailable, "shop overloaded")
		down.err.Store(&unavailable)
		for range 2 {
			if err := call(); status.Code(err) != codes.Unavailable {
				t.Fatalf("call: %v, want the server's Unavailable", err)
			}
		}
		before := down.calls.Load()
		err = call()
		if status.Code(err) != codes.Unavailable || !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("call with the breaker open: %v, want Unavailable wrapping breaker.ErrOpen", err)
		}
		if down.calls.Load() != before {
			t.Fatal("the call reached the server through an open breaker")
		}
		if !strings.Contains(err.Error(), "shop") {
			t.Errorf("error %q does not name the breaker", err)
		}
	})
}

func TestRateLimitMiddleware(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, w := e.connect(e.options(), snap(1,
			rateLimit("checkout", 0.5, 1),
			rateLimit("everything", 0.5, 1),
			rateLimit("", 0.5, 1),
		))
		ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
		serve := func(h http.Handler, path string) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			return rec
		}

		h := c.RateLimit(nil, ok)
		if rec := serve(h, "/checkout"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("first request: %d %q", rec.Code, rec.Body)
		}
		for _, step := range []struct {
			sleep      time.Duration
			wantRetry  string
			wantStatus int
		}{
			{0, "2", http.StatusTooManyRequests},
			{1500 * time.Millisecond, "1", http.StatusTooManyRequests},
			{500 * time.Millisecond, "", http.StatusOK},
		} {
			time.Sleep(step.sleep)
			rec := serve(h, "/checkout")
			if rec.Code != step.wantStatus || rec.Header().Get("Retry-After") != step.wantRetry {
				t.Fatalf("after %v more: %d with Retry-After %q, want %d with %q",
					step.sleep, rec.Code, rec.Header().Get("Retry-After"), step.wantStatus, step.wantRetry)
			}
		}
		if rec := serve(h, "/search"); rec.Code != http.StatusOK {
			t.Fatalf("path without a rate limit: %d", rec.Code)
		}

		everything := c.RateLimit(func(*http.Request) string { return "everything" }, ok)
		if serve(everything, "/a").Code != http.StatusOK || serve(everything, "/b").Code != http.StatusTooManyRequests {
			t.Fatal("custom key not shared across paths")
		}
		// A limit that never refills still gets a usable Retry-After.
		w.push(t, snap(2, rateLimit("frozen", 0, 1)))
		frozen := c.RateLimit(func(*http.Request) string { return "frozen" }, ok)
		serve(frozen, "/")
		if rec := serve(frozen, "/"); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "86400" {
			t.Fatalf("zero-rate limit: %d with Retry-After %q, want 429 with 86400", rec.Code, rec.Header().Get("Retry-After"))
		}
		exempt := c.RateLimit(func(*http.Request) string { return "" }, ok)
		for range 5 {
			if rec := serve(exempt, "/checkout"); rec.Code != http.StatusOK {
				t.Fatalf("exempted request: %d", rec.Code)
			}
		}
	})
}

func TestRetryAfterSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int64{
		0:                       1,
		time.Millisecond:        1,
		time.Second:             1,
		time.Second + 1:         2,
		90 * time.Second:        90,
		ratelimit.InfDuration:   int64(maxRetryAfter / time.Second),
		maxRetryAfter + 1:       int64(maxRetryAfter / time.Second),
		-5 * time.Millisecond:   1,
		2*time.Second - 1:       2,
		1999 * time.Millisecond: 2,
	} {
		if got := retryAfterSeconds(d); got != want {
			t.Errorf("retryAfterSeconds(%v) = %d, want %d", d, got, want)
		}
	}
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// trackedBody records whether it was closed.
type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestBreakerTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, _ := e.connect(e.options(), snap(1,
			circuitBreaker("v1/charge", 0.5, 2, time.Minute),
			circuitBreaker("counted", 1, 1000, time.Minute),
			circuitBreaker("", 1, 1000, time.Minute),
		))

		var reply func(*http.Request) (*http.Response, error)
		sent := 0
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			sent++
			return reply(r)
		})
		withStatus := func(code int) func(*http.Request) (*http.Response, error) {
			return func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: code, Body: http.NoBody, Request: r}, nil
			}
		}

		// Outcomes: a 5xx or a transport error is a failure; a 4xx or a
		// request canceled by its caller is not.
		counted := c.BreakerTransport(func(*http.Request) string { return "counted" }, base)
		b, _ := c.Breaker("counted")
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		for _, tc := range []struct {
			name    string
			ctx     context.Context
			reply   func(*http.Request) (*http.Response, error)
			failure bool
		}{
			{"200", context.Background(), withStatus(http.StatusOK), false},
			{"404", context.Background(), withStatus(http.StatusNotFound), false},
			{"429", context.Background(), withStatus(http.StatusTooManyRequests), false},
			{"500", context.Background(), withStatus(http.StatusInternalServerError), true},
			{"503", context.Background(), withStatus(http.StatusServiceUnavailable), true},
			{"transport error", context.Background(), func(*http.Request) (*http.Response, error) { return nil, errors.New("connection reset") }, true},
			{"canceled", canceled, func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() }, false},
		} {
			before := b.Counts().Failures
			reply = tc.reply
			req := httptest.NewRequestWithContext(tc.ctx, http.MethodGet, "http://payments.internal/v1/charge", nil)
			if resp, err := counted.RoundTrip(req); err == nil {
				resp.Body.Close()
			}
			want := uint32(0)
			if tc.failure {
				want = 1
			}
			if got := b.Counts().Failures - before; got != want {
				t.Errorf("%s: counted %d failures, want %d", tc.name, got, want)
			}
		}

		// The default key is the URL path without its leading slash.
		reply = withStatus(http.StatusBadGateway)
		hc := &http.Client{Transport: c.BreakerTransport(nil, base)}
		for range 2 {
			resp, err := hc.Get("http://payments.internal/v1/charge")
			if err != nil || resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("GET: %v, %v; want the server's 502", resp, err)
			}
			resp.Body.Close()
		}
		before := sent
		body := &trackedBody{Reader: strings.NewReader("amount=10")}
		req, err := http.NewRequest(http.MethodPost, "http://payments.internal/v1/charge", body)
		if err != nil {
			t.Fatal(err)
		}
		_, err = hc.Do(req)
		if !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("POST with the breaker open: %v, want breaker.ErrOpen", err)
		}
		if sent != before {
			t.Fatal("the request reached the transport through an open breaker")
		}
		if !body.closed {
			t.Fatal("the rejected request's body was not closed")
		}

		// Other paths, and keys that bypass the breakers, go straight through.
		if resp, err := hc.Get("http://payments.internal/v1/refund"); err != nil {
			t.Fatalf("GET of a path without a breaker: %v", err)
		} else {
			resp.Body.Close()
		}
		bypass := c.BreakerTransport(func(*http.Request) string { return "" }, base)
		if resp, err := bypass.RoundTrip(httptest.NewRequest(http.MethodGet, "http://payments.internal/v1/charge", nil)); err != nil {
			t.Fatalf("bypassed request: %v", err)
		} else {
			resp.Body.Close()
		}
		if empty, _ := c.Breaker(""); empty.Counts().Requests != 0 {
			t.Fatalf("a bypassed request was counted under the empty key: %+v", empty.Counts())
		}
	})
}

func TestBreakerTransportDefaultsToDefaultTransport(t *testing.T) {
	c := &Client{}
	if bt := c.BreakerTransport(nil, nil).(*breakerTransport); bt.base != http.DefaultTransport {
		t.Fatalf("base = %v, want http.DefaultTransport", bt.base)
	}
}
