// Package httpapi serves the control plane's JSON API and the embedded admin
// UI over HTTP.
//
// The API transcodes HTTP to the gRPC services in-process. Every unary RPC is
// POST /api/v1/<Service>/<Method> with the protojson encoding of its request
// message as the body, and answers with the protojson encoding of the
// response: camelCase field names, 64-bit integers as strings, durations such
// as "30s", enums by name and unpopulated fields included. Calls reach the
// same service implementations as gRPC calls and are authorized with the same
// per-method roles, so the two transports cannot drift apart.
//
// A failed call answers {"code": "<gRPC code name>", "message": "..."} with a
// matching HTTP status, e.g. NotFound 404, or 409 for Aborted, which is what
// a failed expected_revision check returns.
package httpapi

import (
	"log/slog"
	"net/http"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

// Options configures the handler returned by New.
type Options struct {
	Admin        cpv1.AdminServiceServer
	Distribution cpv1.DistributionServiceServer
	// Auth authorizes every API call by its bearer token. Nil disables auth:
	// every call is allowed and the X-Controlplane-Actor header names the
	// actor recorded for changes. Without auth the only barrier against other
	// websites is the JSON Content-Type requirement (see api.call), which
	// stops cross-site requests but not DNS rebinding, so run without auth
	// only where the listener is not reachable from a browser.
	Auth *auth.Authenticator
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// New returns a handler that serves the API under /api/v1/, the admin UI
// under /ui/ and a redirect from / to the UI. Request paths are matched in
// full, so the handler is meant to be mounted at the root of a mux. A nil
// service is not exposed.
func New(opts Options) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", newAPI(opts))
	mux.Handle("GET /ui/", newUI())
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	return withSecurityHeaders(mux)
}

// withSecurityHeaders sets the headers every response carries. The UI loads
// nothing but its own files and renders data as text, so the policy can
// forbid inline script and style outright, which defuses any markup that
// slips into the page; framing is refused against clickjacking.
func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
