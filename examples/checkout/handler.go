package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/client"
)

// The control plane entries the service reads; see the package
// documentation.
const (
	rateLimitKey      = "checkout"
	newCartFlag       = "new-cart"
	ctaExperiment     = "cta-color"
	paymentsBreaker   = "payments"
	failureRateConfig = "payments.failure_rate"
	latencyConfig     = "payments.latency_ms"
)

// Outcomes of the payments call, as reported in checkout responses.
const (
	paymentOK          = "ok"
	paymentFailed      = "failed"
	paymentUnavailable = "unavailable"
)

const (
	// maxLatency caps the simulated payments latency, so that a mistyped
	// config cannot hold requests, or a graceful shutdown, for long.
	maxLatency = 10 * time.Second
	// maxUserLen matches the longest unit ID the control plane accepts, in
	// flag allowlists, and bounds what the service hashes and echoes back.
	maxUserLen = 256
)

// errPaymentFailed is a simulated payments call failing.
var errPaymentFailed = errors.New("payments: simulated failure")

// handler serves the checkout API. It reads every decision from the control
// plane client while serving the request, so each change applies from the
// first request after the client received it.
type handler struct {
	client *client.Client
	// random returns uniformly distributed numbers in [0, 1) and decides
	// which simulated payments calls fail. It must be safe for concurrent
	// use.
	random func() float64
	mux    *http.ServeMux
}

// newHandler returns the service's HTTP handler. random decides which
// simulated payments calls fail; see handler.random.
func newHandler(c *client.Client, random func() float64) *handler {
	h := &handler{client: c, random: random, mux: http.NewServeMux()}
	limitKey := func(*http.Request) string { return rateLimitKey }
	h.mux.Handle("GET /checkout", c.RateLimit(limitKey, http.HandlerFunc(h.checkout)))
	h.mux.HandleFunc("GET /status", h.status)
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type checkoutResponse struct {
	User     string `json:"user"`
	Cart     string `json:"cart"`
	CTA      cta    `json:"cta"`
	Payment  string `json:"payment"`
	Revision int64  `json:"revision"`
}

// cta is the call to action shown to the user: their variant of the
// experiment and its payload, empty when the user is not enrolled.
type cta struct {
	Variant string `json:"variant"`
	Payload any    `json:"payload"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *handler) checkout(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" || len(user) > maxUserLen {
		writeJSON(w, http.StatusBadRequest, errorResponse{fmt.Sprintf("the user query parameter must be 1 to %d bytes", maxUserLen)})
		return
	}
	resp := checkoutResponse{User: user, Cart: "v1", Revision: h.client.Revision()}
	if h.client.IsEnabled(newCartFlag, user) {
		resp.Cart = "v2"
	}
	if a, ok := h.client.Variant(ctaExperiment, user); ok {
		resp.CTA = cta{Variant: a.Variant, Payload: a.Payload.AsInterface()}
	}
	resp.Payment = h.pay(r.Context())
	code := http.StatusOK
	switch resp.Payment {
	case paymentFailed:
		code = http.StatusBadGateway
	case paymentUnavailable:
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

// pay makes the simulated payments call through the payments circuit
// breaker. While the breaker rejects calls, none is made: the request fails
// at once instead of waiting on a service that is known to be failing.
func (h *handler) pay(ctx context.Context) string {
	err := h.client.Do(paymentsBreaker, func() error { return h.callPayments(ctx) })
	switch {
	case err == nil:
		return paymentOK
	case errors.Is(err, breaker.ErrOpen), errors.Is(err, breaker.ErrTooManyRequests):
		return paymentUnavailable
	default:
		return paymentFailed
	}
}

// callPayments stands in for a payments provider whose speed and reliability
// are set live through configs: a call takes payments.latency_ms and fails
// with probability payments.failure_rate. A caller that gives up while it
// waits fails the call, as a timeout would.
func (h *handler) callPayments(ctx context.Context) error {
	if ms := h.client.Float(latencyConfig, 0); ms > 0 {
		// Capped while still a float: converting one too large for a
		// Duration gives an implementation-dependent result.
		d := time.Duration(min(ms, float64(maxLatency/time.Millisecond)) * float64(time.Millisecond))
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if h.random() < h.client.Float(failureRateConfig, 0) {
		return errPaymentFailed
	}
	return nil
}

// statusResponse is client.Status as JSON.
type statusResponse struct {
	State      client.State  `json:"state"`
	Source     client.Source `json:"source"`
	Revision   int64         `json:"revision"`
	LastUpdate time.Time     `json:"last_update,omitzero"`
	LastError  string        `json:"last_error,omitempty"`
}

func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	s := h.client.Status()
	resp := statusResponse{State: s.State, Source: s.Source, Revision: s.Revision, LastUpdate: s.LastUpdate.UTC()}
	if s.LastError != nil {
		resp.LastError = s.LastError.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	// Encoding these values cannot fail, so an error means the client has
	// gone away and there is nobody left to tell.
	_ = json.NewEncoder(w).Encode(v)
}
