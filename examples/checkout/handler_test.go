package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store/memory"
	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/client"
)

// Most tests run a real control plane (internal/server on the memory store)
// and SDK clients inside a synctest bubble, connected over bufconn. Fake time
// drives rate limit refills, breaker timeouts and the payments latency
// exactly. After a write, synctest.Wait returns once every client has applied
// the change, and fake time cannot pass while it waits: a change visible
// right after a write was pushed, not picked up by a timer such as the
// server's reconciler. TestRun checks the propagation budget in real time.

const testNamespace = "checkout/dev"

var discard = slog.New(slog.DiscardHandler)

// env is an in-process control plane. It must be created inside a synctest
// bubble.
type env struct {
	t     *testing.T
	admin cpv1.AdminServiceClient
	dial  grpc.DialOption
	down  func() // stops the control plane, once
}

func newEnv(t *testing.T) *env {
	t.Helper()
	srv := server.New(server.Options{Store: memory.New(), Logger: discard})
	// The real server's keepalive policy, which the SDK's pings respect.
	gs := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))
	srv.Register(gs)
	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = srv.Run(ctx)
	}()
	go func() { _ = gs.Serve(lis) }()

	dial := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	conn, err := grpc.NewClient("passthrough:///controlplane", dial, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	e := &env{
		t:     t,
		admin: cpv1.NewAdminServiceClient(conn),
		dial:  dial,
		down:  func() { once.Do(func() { srv.Shutdown(); gs.Stop() }) },
	}
	t.Cleanup(func() {
		conn.Close()
		e.down()
		cancel()
		<-stopped
	})
	if _, err := e.admin.CreateNamespace(context.Background(), &cpv1.CreateNamespaceRequest{Name: testNamespace}); err != nil {
		t.Fatal(err)
	}
	return e
}

// service is the handler under test and what it reads from.
type service struct {
	*handler
	client *client.Client
	random *fakeRandom
}

// start runs a checkout service on a new client of the test namespace and
// waits until it serves the current revision. random is what the payments
// simulation draws, in turn; none means 0.5.
func (e *env) start(random ...float64) *service {
	e.t.Helper()
	c, err := client.New(context.Background(), client.Options{
		Address:     "passthrough:///controlplane",
		Namespace:   testNamespace,
		DialOptions: []grpc.DialOption{e.dial},
		Logger:      discard,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { c.Close() })
	synctest.Wait()
	if c.Revision() == 0 {
		e.t.Fatal("the client has no snapshot after connecting")
	}
	if len(random) == 0 {
		random = []float64{0.5}
	}
	r := &fakeRandom{values: random}
	return &service{handler: newHandler(c, r.Float64), client: c, random: r}
}

// The put helpers write through the admin API, wait until the change has
// reached every client and return the new revision.

func (e *env) putConfig(key string, v any) int64 {
	e.t.Helper()
	pv, err := structpb.NewValue(v)
	if err != nil {
		e.t.Fatal(err)
	}
	resp, err := e.admin.PutConfig(context.Background(), &cpv1.PutConfigRequest{Namespace: testNamespace, Key: key, Value: pv})
	return e.applied(resp.GetConfig().GetRevision(), err)
}

func (e *env) putFlag(req *cpv1.PutFlagRequest) int64 {
	e.t.Helper()
	req.Namespace = testNamespace
	resp, err := e.admin.PutFlag(context.Background(), req)
	return e.applied(resp.GetFlag().GetRevision(), err)
}

func (e *env) putExperiment(req *cpv1.PutExperimentRequest) int64 {
	e.t.Helper()
	req.Namespace = testNamespace
	resp, err := e.admin.PutExperiment(context.Background(), req)
	return e.applied(resp.GetExperiment().GetRevision(), err)
}

func (e *env) putRateLimit(req *cpv1.PutRateLimitRequest) int64 {
	e.t.Helper()
	req.Namespace = testNamespace
	resp, err := e.admin.PutRateLimit(context.Background(), req)
	return e.applied(resp.GetRateLimit().GetRevision(), err)
}

func (e *env) putBreaker(req *cpv1.PutCircuitBreakerRequest) int64 {
	e.t.Helper()
	req.Namespace = testNamespace
	resp, err := e.admin.PutCircuitBreaker(context.Background(), req)
	return e.applied(resp.GetCircuitBreaker().GetRevision(), err)
}

func (e *env) applied(revision int64, err error) int64 {
	e.t.Helper()
	if err != nil {
		e.t.Fatal(err)
	}
	synctest.Wait()
	return revision
}

// fakeRandom hands out its values in turn, starting over after the last.
// Every payments call draws exactly one, so calls counts the calls made.
type fakeRandom struct {
	mu     sync.Mutex
	values []float64
	n      int
}

func (r *fakeRandom) Float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.values[r.n%len(r.values)]
	r.n++
	return v
}

func (r *fakeRandom) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// get serves a GET request for target.
func get(h http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

// checkout requests a checkout for user and decodes the response.
func checkout(t *testing.T, h http.Handler, user string) (int, checkoutResponse) {
	t.Helper()
	rec := get(h, "/checkout?user="+url.QueryEscape(user))
	var resp checkoutResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("checkout for %s: %d %q: %v", user, rec.Code, rec.Body, err)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("checkout for %s: Content-Type %q, want application/json", user, ct)
	}
	return rec.Code, resp
}

func TestCheckoutRejectsBadRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc := newEnv(t).start()
		for _, tt := range []struct {
			method, target string
			want           int
		}{
			{http.MethodGet, "/checkout", http.StatusBadRequest},
			{http.MethodGet, "/checkout?user=", http.StatusBadRequest},
			{http.MethodGet, "/checkout?user=" + strings.Repeat("u", maxUserLen+1), http.StatusBadRequest},
			{http.MethodGet, "/checkout?user=" + strings.Repeat("u", maxUserLen), http.StatusOK},
			{http.MethodPost, "/checkout?user=alice", http.StatusMethodNotAllowed},
			{http.MethodGet, "/nope", http.StatusNotFound},
		} {
			rec := httptest.NewRecorder()
			svc.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, nil))
			if rec.Code != tt.want {
				t.Errorf("%s %.40s: status %d, want %d", tt.method, tt.target, rec.Code, tt.want)
			}
			if rec.Code == http.StatusBadRequest {
				var resp errorResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || !strings.Contains(resp.Error, "user") {
					t.Errorf("%s %.40s: body %q, want a JSON error about the user", tt.method, tt.target, rec.Body)
				}
			}
		}
		if n := svc.random.calls(); n != 1 {
			t.Errorf("payments called %d times, want once: only the valid request may reach it", n)
		}
	})
}

func TestNewCartFlag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start()
		cart := func(user string) (string, int64) {
			t.Helper()
			code, resp := checkout(t, svc, user)
			if code != http.StatusOK {
				t.Fatalf("checkout for %s: status %d", user, code)
			}
			return resp.Cart, resp.Revision
		}

		// No flag: everyone keeps the old cart.
		if got, _ := cart("alice"); got != "v1" {
			t.Fatalf("cart without the flag = %s, want v1", got)
		}

		// Each flip is served as soon as it is pushed, and the response
		// carries the revision of the write.
		rev := e.putFlag(&cpv1.PutFlagRequest{Key: newCartFlag, Enabled: true})
		if got, gotRev := cart("alice"); got != "v2" || gotRev != rev {
			t.Fatalf("after enabling the flag: cart %s at revision %d, want v2 at %d", got, gotRev, rev)
		}
		rev = e.putFlag(&cpv1.PutFlagRequest{Key: newCartFlag, Enabled: false})
		if got, gotRev := cart("alice"); got != "v1" || gotRev != rev {
			t.Fatalf("after disabling the flag: cart %s at revision %d, want v1 at %d", got, gotRev, rev)
		}

		// The flag is evaluated per user: with a 0% rollout, only the
		// allowlisted user gets the new cart.
		e.putFlag(&cpv1.PutFlagRequest{Key: newCartFlag, Enabled: true, RolloutPercent: new(float64), Allowlist: []string{"bob"}})
		alice, _ := cart("alice")
		bob, _ := cart("bob")
		if alice != "v1" || bob != "v2" {
			t.Fatalf("0%% rollout allowing bob: alice gets %s, bob gets %s; want v1 and v2", alice, bob)
		}
	})
}

func TestCTAExperiment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		payloads := map[string]any{
			"green":  map[string]any{"color": "#2da44e", "label": "Buy now"},
			"orange": map[string]any{"color": "#fb8500", "label": "Complete purchase"},
		}
		var variants []*cpv1.Variant
		for _, name := range []string{"green", "orange"} {
			p, err := structpb.NewValue(payloads[name])
			if err != nil {
				t.Fatal(err)
			}
			variants = append(variants, &cpv1.Variant{Name: name, Weight: 50, Payload: p})
		}
		e.putExperiment(&cpv1.PutExperimentRequest{Key: ctaExperiment, Enabled: true, Variants: variants})

		// Two independent instances of the service, as behind a load
		// balancer, must agree on every user's variant.
		first, second := e.start(), e.start()
		seen := map[string]int{}
		for i := range 40 {
			user := fmt.Sprintf("user-%d", i)
			_, resp := checkout(t, first, user)
			assigned := resp.CTA
			if !reflect.DeepEqual(assigned.Payload, payloads[assigned.Variant]) {
				t.Fatalf("%s: variant %q with payload %v, want payload %v", user, assigned.Variant, assigned.Payload, payloads[assigned.Variant])
			}
			seen[assigned.Variant]++
			for _, svc := range []*service{first, second} {
				if _, again := checkout(t, svc, user); !reflect.DeepEqual(again.CTA, assigned) {
					t.Fatalf("%s: assigned %+v, later %+v", user, assigned, again.CTA)
				}
			}
		}
		if seen["green"] == 0 || seen["orange"] == 0 {
			t.Fatalf("variants seen %v, want both", seen)
		}

		// A disabled experiment enrolls nobody.
		e.putExperiment(&cpv1.PutExperimentRequest{Key: ctaExperiment, Enabled: false, Variants: variants})
		rec := get(first, "/checkout?user=user-0")
		var raw map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"variant": "", "payload": nil}; !reflect.DeepEqual(raw["cta"], want) {
			t.Fatalf("cta with the experiment disabled = %v, want %v", raw["cta"], want)
		}
	})
}

func TestCheckoutRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start()
		// A token every 2s, bursts of 3.
		e.putRateLimit(&cpv1.PutRateLimitRequest{Key: rateLimitKey, Enabled: true, RequestsPerSecond: 0.5, Burst: 3})
		checkoutCode := func() int { return get(svc, "/checkout?user=alice").Code }

		for i := range 3 {
			if code := checkoutCode(); code != http.StatusOK {
				t.Fatalf("request %d of the burst: status %d", i+1, code)
			}
		}
		rec := get(svc, "/checkout?user=alice")
		if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
			t.Fatalf("request after the burst: status %d, Retry-After %q; want 429 and 2",
				rec.Code, rec.Header().Get("Retry-After"))
		}
		if n := svc.random.calls(); n != 3 {
			t.Fatalf("payments called %d times, want 3: a limited request must stop before it", n)
		}
		if code := get(svc, "/status").Code; code != http.StatusOK {
			t.Fatalf("/status is not rate limited, got %d", code)
		}

		time.Sleep(2 * time.Second)
		if a, b := checkoutCode(), checkoutCode(); a != http.StatusOK || b != http.StatusTooManyRequests {
			t.Fatalf("2s later: statuses %d, %d; want one more request allowed", a, b)
		}

		// Disabling the limit takes effect with the next request.
		e.putRateLimit(&cpv1.PutRateLimitRequest{Key: rateLimitKey, Enabled: false, RequestsPerSecond: 0.5, Burst: 3})
		for i := range 10 {
			if code := checkoutCode(); code != http.StatusOK {
				t.Fatalf("request %d with the limit disabled: status %d", i+1, code)
			}
		}
	})
}

func TestPaymentsBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start()
		const (
			openFor = 5 * time.Second
			latency = 100 * time.Millisecond
		)
		e.putBreaker(&cpv1.PutCircuitBreakerRequest{
			Key:                  paymentsBreaker,
			Enabled:              true,
			FailureRateThreshold: 0.5,
			MinRequests:          4,
			Window:               durationpb.New(10 * time.Second),
			OpenDuration:         durationpb.New(openFor),
			HalfOpenMaxRequests:  1,
		})
		e.putConfig(latencyConfig, latency.Milliseconds())
		e.putConfig(failureRateConfig, 1)

		pay := func() (code int, payment string, took time.Duration, calls int) {
			t.Helper()
			start, before := time.Now(), svc.random.calls()
			code, resp := checkout(t, svc, "alice")
			if resp.Cart != "v1" || resp.User != "alice" {
				t.Fatalf("response %+v lacks the rest of the checkout", resp)
			}
			return code, resp.Payment, time.Since(start), svc.random.calls() - before
		}

		// Every call fails, and the breaker opens once it has seen
		// MinRequests of them.
		for i := range 4 {
			if code, payment, took, calls := pay(); code != http.StatusBadGateway || payment != paymentFailed || took != latency || calls != 1 {
				t.Fatalf("call %d: %d %q after %v with %d payments calls; want 502 %q after %v with one",
					i+1, code, payment, took, calls, paymentFailed, latency)
			}
		}
		opened := time.Now()

		// Open: requests fail fast, without calling payments.
		if code, payment, took, calls := pay(); code != http.StatusServiceUnavailable || payment != paymentUnavailable || took != 0 || calls != 0 {
			t.Fatalf("open breaker: %d %q after %v with %d payments calls; want 503 %q at once without any",
				code, payment, took, calls, paymentUnavailable)
		}

		// Payments recover, but the breaker stays open for OpenDuration.
		e.putConfig(failureRateConfig, 0)
		time.Sleep(openFor - time.Since(opened) - time.Millisecond)
		if code, payment, _, calls := pay(); code != http.StatusServiceUnavailable || calls != 0 {
			t.Fatalf("1ms before the breaker may close: %d %q with %d payments calls; want 503 without any", code, payment, calls)
		}

		// Then a trial call goes through, succeeds and closes the breaker.
		time.Sleep(time.Millisecond)
		for i := range 3 {
			if code, payment, took, calls := pay(); code != http.StatusOK || payment != paymentOK || took != latency || calls != 1 {
				t.Fatalf("call %d after OpenDuration: %d %q after %v with %d payments calls; want 200 %q after %v with one",
					i+1, code, payment, took, calls, paymentOK, latency)
			}
		}
		if b, ok := svc.client.Breaker(paymentsBreaker); !ok || b.State() != breaker.Closed {
			t.Fatal("the breaker is not closed after successful calls")
		}
	})
}

func TestPaymentsFailureRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		// A call fails when its draw is below the failure rate.
		svc := e.start(0.1, 0.25, 0.9, 0.2)
		e.putConfig(failureRateConfig, 0.25)
		var got []string
		for range 4 {
			_, resp := checkout(t, svc, "alice")
			got = append(got, resp.Payment)
		}
		if want := []string{paymentFailed, paymentOK, paymentOK, paymentFailed}; !reflect.DeepEqual(got, want) {
			t.Fatalf("payments at failure rate 0.25 = %v, want %v", got, want)
		}

		// Without a breaker for payments, failures never fail fast.
		e.putConfig(failureRateConfig, 1)
		for i := range 50 {
			if code, resp := checkout(t, svc, "alice"); code != http.StatusBadGateway || resp.Payment != paymentFailed {
				t.Fatalf("call %d at failure rate 1 without a breaker: %d %q, want 502 %q", i+1, code, resp.Payment, paymentFailed)
			}
		}

		// The latency is capped.
		e.putConfig(latencyConfig, 1e9)
		start := time.Now()
		if code, _ := checkout(t, svc, "alice"); code != http.StatusBadGateway || time.Since(start) != maxLatency {
			t.Fatalf("latency 1e9ms: status %d after %v, want 502 after the %v cap", code, time.Since(start), maxLatency)
		}

		// A caller giving up ends the wait and fails the call, as a timeout
		// would, whatever the failure rate.
		e.putConfig(failureRateConfig, 0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start = time.Now()
		rec := httptest.NewRecorder()
		svc.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/checkout?user=alice", nil))
		if took := time.Since(start); rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), `"payment":"failed"`) || took != time.Second {
			t.Fatalf("caller gone after 1s: %d %s after %v, want the call failed then", rec.Code, rec.Body, took)
		}
	})
}

// Configs of the wrong type, or gone, must leave the payments call at its
// defaults, an instant success, and never fail a request.
func TestPaymentsIgnoresInvalidConfigs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start(0)
		assertPaid := func(what string) {
			t.Helper()
			start := time.Now()
			if code, resp := checkout(t, svc, "alice"); code != http.StatusOK || resp.Payment != paymentOK || time.Since(start) != 0 {
				t.Fatalf("%s: %d %q after %v, want 200 %q at once", what, code, resp.Payment, time.Since(start), paymentOK)
			}
		}
		assertPaid("no configs")

		for _, v := range []any{"fast", "0.5", nil, true, []any{1.0}, map[string]any{"n": 1.0}, -3.0} {
			e.putConfig(failureRateConfig, v)
			e.putConfig(latencyConfig, v)
			assertPaid(fmt.Sprintf("configs %v", v))
		}

		// Out-of-range numbers keep their plain meaning, with the latency
		// capped: every call fails at a failure rate above 1.
		e.putConfig(failureRateConfig, 7)
		e.putConfig(latencyConfig, 1e300)
		start := time.Now()
		if code, resp := checkout(t, svc, "alice"); code != http.StatusBadGateway || resp.Payment != paymentFailed || time.Since(start) != maxLatency {
			t.Fatalf("failure rate 7, latency 1e300: %d %q after %v, want 502 %q after %v", code, resp.Payment, time.Since(start), paymentFailed, maxLatency)
		}

		for _, key := range []string{failureRateConfig, latencyConfig} {
			if _, err := e.admin.DeleteConfig(context.Background(), &cpv1.DeleteConfigRequest{Namespace: testNamespace, Key: key}); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		assertPaid("configs deleted again")
	})
}

// A variant without a payload is still a variant: the payload is null.
func TestCTAVariantWithoutPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start()
		e.putExperiment(&cpv1.PutExperimentRequest{Key: ctaExperiment, Enabled: true, Variants: []*cpv1.Variant{{Name: "plain", Weight: 1}}})
		var raw map[string]any
		rec := get(svc, "/checkout?user=alice")
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil || rec.Code != http.StatusOK {
			t.Fatalf("checkout: %d %q: %v", rec.Code, rec.Body, err)
		}
		if want := map[string]any{"variant": "plain", "payload": nil}; !reflect.DeepEqual(raw["cta"], want) {
			t.Fatalf("cta = %v, want %v", raw["cta"], want)
		}
	})
}

func TestStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		svc := e.start()
		rev := e.putFlag(&cpv1.PutFlagRequest{Key: newCartFlag, Enabled: true})
		status := func() map[string]any {
			t.Helper()
			rec := get(svc, "/status")
			var m map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil || rec.Code != http.StatusOK {
				t.Fatalf("/status: %d %q: %v", rec.Code, rec.Body, err)
			}
			return m
		}

		want := map[string]any{
			"state":       "live",
			"source":      "server",
			"revision":    float64(rev),
			"last_update": time.Now().UTC().Format(time.RFC3339Nano),
		}
		if got := status(); !reflect.DeepEqual(got, want) {
			t.Fatalf("/status = %v, want %v", got, want)
		}

		// With the control plane gone, the service reports it and keeps
		// serving the last revision it received.
		e.down()
		synctest.Wait()
		got := status()
		if lastErr, _ := got["last_error"].(string); got["state"] != "disconnected" || got["revision"] != float64(rev) || !strings.Contains(lastErr, "Unavailable") {
			t.Fatalf("/status with the control plane down = %v, want disconnected at revision %d with an Unavailable error", got, rev)
		}
		if code, resp := checkout(t, svc, "alice"); code != http.StatusOK || resp.Cart != "v2" || resp.Revision != rev {
			t.Fatalf("checkout with the control plane down: %d %+v, want cart v2 at revision %d", code, resp, rev)
		}
	})
}
