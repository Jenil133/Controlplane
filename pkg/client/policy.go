package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/ratelimit"
)

// Allow reports whether a request for rateLimitKey may proceed under the
// namespace's rate limits, taking a token if so. A key without an enabled
// rate limit is never limited.
func (c *Client) Allow(rateLimitKey string) bool {
	return c.limits.Allow(rateLimitKey)
}

// Breaker returns the circuit breaker for key, if the namespace has an
// enabled one. Breakers are reconfigured in place by later snapshots; one
// whose policy is removed keeps working but is no longer the client's.
func (c *Client) Breaker(key string) (*breaker.Breaker, bool) {
	return c.breakers.Breaker(key)
}

// Do calls fn through the circuit breaker breakerKey, recording an error or
// panic as a failure. While the breaker rejects calls, Do returns
// breaker.ErrOpen or breaker.ErrTooManyRequests without calling fn. Without
// an enabled breaker for the key, Do just calls fn.
func (c *Client) Do(breakerKey string, fn func() error) error {
	return c.breakers.Do(breakerKey, fn)
}

// UnaryServerInterceptor rate-limits incoming unary calls by the rate limit
// key(fullMethod), failing calls over the limit with RESOURCE_EXHAUSTED.
// A nil key uses the method name without its leading slash, such as
// "checkout.v1.Checkout/Pay"; a key func may return "" to exempt a call.
func (c *Client) UnaryServerInterceptor(key func(fullMethod string) string) grpc.UnaryServerInterceptor {
	key = methodKey(key)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := c.limit(key(info.FullMethod)); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor is the streaming counterpart of
// UnaryServerInterceptor: opening a stream takes one token.
func (c *Client) StreamServerInterceptor(key func(fullMethod string) string) grpc.StreamServerInterceptor {
	key = methodKey(key)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := c.limit(key(info.FullMethod)); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func (c *Client) limit(key string) error {
	if key == "" || c.limits.Allow(key) {
		return nil
	}
	return status.Errorf(codes.ResourceExhausted, "rate limit %q exceeded", key)
}

// UnaryClientInterceptor sends outgoing unary calls through the circuit
// breaker key(fullMethod). Calls failing with UNAVAILABLE,
// DEADLINE_EXCEEDED, INTERNAL, UNKNOWN or DATA_LOSS count as failures; any
// other outcome is an answer from a working server, or the caller giving up,
// and counts as a success. A call the breaker rejects is not sent and fails
// with UNAVAILABLE; the error also wraps breaker.ErrOpen or
// breaker.ErrTooManyRequests for errors.Is. A nil key uses the method name
// without its leading slash; a key func may return "" to bypass the
// breakers.
func (c *Client) UnaryClientInterceptor(key func(fullMethod string) string) grpc.UnaryClientInterceptor {
	key = methodKey(key)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		k := key(method)
		if k == "" {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		done, err := c.breakers.Allow(k)
		if err != nil {
			return &rejectedError{key: k, err: err}
		}
		success := false
		defer func() { done(success) }()
		err = invoker(ctx, method, req, reply, cc, opts...)
		success = !isFailure(err)
		return err
	}
}

// isFailure reports whether a call's error says the server is unhealthy.
func isFailure(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.DataLoss:
		return true
	default:
		return false
	}
}

// RateLimit wraps next with the rate limit key(r). A request over the limit
// gets 429 Too Many Requests with a Retry-After header: the whole seconds,
// at least 1 and at most a day, until the limit allows a request again. A nil key uses the URL
// path without its leading slash, so "/checkout" is limited by the rate
// limit "checkout"; a key func may return "" to exempt a request.
func (c *Client) RateLimit(key func(*http.Request) string, next http.Handler) http.Handler {
	key = pathKey(key)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := key(r)
		if k == "" || c.limits.Allow(k) {
			next.ServeHTTP(w, r)
			return
		}
		var delay time.Duration
		if l, ok := c.limits.Limiter(k); ok {
			delay = l.Delay()
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSeconds(delay), 10))
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
	})
}

// maxRetryAfter caps Retry-After. A limit that never refills reports
// ratelimit.InfDuration, about 292 years, which some clients cannot parse and
// none could act on; a day says "not soon" just as well.
const maxRetryAfter = 24 * time.Hour

func retryAfterSeconds(d time.Duration) int64 {
	d = min(d, maxRetryAfter)
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return max(secs, 1)
}

// BreakerTransport returns a RoundTripper that sends requests through base
// (http.DefaultTransport if nil) guarded by the circuit breaker key(req). A
// transport error or a 5xx response counts as a failure, except an error
// caused by canceling the request's context, which says nothing about the
// server. A request the breaker rejects does not reach base and fails with an
// error wrapping breaker.ErrOpen or breaker.ErrTooManyRequests. A nil key
// uses the URL path without its leading slash; a key func may return "" to
// bypass the breakers.
func (c *Client) BreakerTransport(key func(*http.Request) string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &breakerTransport{breakers: c.breakers, key: pathKey(key), base: base}
}

type breakerTransport struct {
	breakers *breaker.Set
	key      func(*http.Request) string
	base     http.RoundTripper
}

func (t *breakerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	k := t.key(req)
	if k == "" {
		return t.base.RoundTrip(req)
	}
	done, err := t.breakers.Allow(k)
	if err != nil {
		// A RoundTripper must close the body even when it fails.
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, &rejectedError{key: k, err: err}
	}
	success := false
	defer func() { done(success) }()
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		success = errors.Is(req.Context().Err(), context.Canceled)
	} else {
		success = resp.StatusCode < http.StatusInternalServerError
	}
	return resp, err
}

// rejectedError is a call a circuit breaker refused. gRPC callers see it as
// UNAVAILABLE, the code a server that is down produces.
type rejectedError struct {
	key string
	err error // breaker.ErrOpen or breaker.ErrTooManyRequests
}

func (e *rejectedError) Error() string { return fmt.Sprintf("%v (%s)", e.err, e.key) }

func (e *rejectedError) Unwrap() error { return e.err }

func (e *rejectedError) GRPCStatus() *status.Status { return status.New(codes.Unavailable, e.Error()) }

func methodKey(key func(string) string) func(string) string {
	if key != nil {
		return key
	}
	return func(fullMethod string) string { return strings.TrimPrefix(fullMethod, "/") }
}

func pathKey(key func(*http.Request) string) func(*http.Request) string {
	if key != nil {
		return key
	}
	return func(r *http.Request) string { return strings.TrimPrefix(r.URL.Path, "/") }
}

func rateLimitPolicies(rls []*cpv1.RateLimit) []ratelimit.Policy {
	ps := make([]ratelimit.Policy, len(rls))
	for i, rl := range rls {
		ps[i] = ratelimit.Policy{
			Key:               rl.GetKey(),
			Enabled:           rl.GetEnabled(),
			RequestsPerSecond: rl.GetRequestsPerSecond(),
			Burst:             int(rl.GetBurst()),
		}
	}
	return ps
}

func breakerPolicies(cbs []*cpv1.CircuitBreaker) []breaker.Policy {
	ps := make([]breaker.Policy, len(cbs))
	for i, cb := range cbs {
		ps[i] = breaker.Policy{
			Key:     cb.GetKey(),
			Enabled: cb.GetEnabled(),
			Settings: breaker.Settings{
				FailureRateThreshold: cb.GetFailureRateThreshold(),
				MinRequests:          cb.GetMinRequests(),
				Window:               cb.GetWindow().AsDuration(),
				OpenDuration:         cb.GetOpenDuration().AsDuration(),
				HalfOpenMaxRequests:  cb.GetHalfOpenMaxRequests(),
			},
		}
	}
	return ps
}
