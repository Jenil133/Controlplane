package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/metrics"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

var discard = slog.New(slog.DiscardHandler)

// testTimeout bounds every wait, so a broken build fails instead of hanging.
const testTimeout = 10 * time.Second

// testServer is the binary's wiring serving on loopback ports.
type testServer struct {
	grpcAddr string
	httpURL  string

	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// startServer opens and serves like run does, configured by the given
// command-line flags, but on listeners bound to random 127.0.0.1 ports.
func startServer(t *testing.T, args ...string) *testServer {
	t.Helper()
	return startLoggedServer(t, discard, args...)
}

// startLoggedServer is startServer with the server logging to log.
func startLoggedServer(t *testing.T, log *slog.Logger, args ...string) *testServer {
	t.Helper()
	cfg := mustParse(t, args...)
	ctx, cancel := context.WithCancel(context.Background())
	a, err := open(ctx, cfg, log)
	if err != nil {
		cancel()
		t.Fatalf("open: %v", err)
	}
	grpcLis, httpLis := listen(t), listen(t)
	ts := &testServer{
		grpcAddr: grpcLis.Addr().String(),
		httpURL:  "http://" + httpLis.Addr().String(),
		cancel:   cancel,
		done:     make(chan error, 1),
	}
	go func() {
		err := a.serve(ctx, grpcLis, httpLis)
		a.close()
		ts.done <- err
	}()
	t.Cleanup(func() {
		if err := ts.stop(); err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return ts
}

// stop shuts the server down like a signal would and returns what serve
// returned. Calls after the first only return the result again.
func (ts *testServer) stop() error {
	ts.once.Do(func() {
		ts.cancel()
		select {
		case ts.err = <-ts.done:
		case <-time.After(testTimeout):
			ts.err = errors.New("serve did not return after its context was cancelled")
		}
	})
	return ts.err
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { lis.Close() })
	return lis
}

// writeTokens writes a tokens file with one token per role, each named
// "e2e-<role>", and returns its path and the tokens by role.
func writeTokens(t *testing.T) (path string, tokens map[string]string) {
	t.Helper()
	var file struct {
		Tokens []auth.Entry `json:"tokens"`
	}
	tokens = make(map[string]string)
	for _, role := range []string{"reader", "editor", "admin"} {
		token, err := auth.GenerateToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[role] = token
		file.Tokens = append(file.Tokens, auth.Entry{Name: "e2e-" + role, Role: role, TokenSHA256: auth.HashToken(token)})
	}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, tokens
}

// clients are the gRPC services as one caller sees them.
type clients struct {
	admin  cpv1.AdminServiceClient
	dist   cpv1.DistributionServiceClient
	health healthpb.HealthClient
}

// dial connects to addr, sending token with every call unless it is empty.
func dial(t *testing.T, addr, token string) clients {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(auth.TokenCredentials(token, false)))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return clients{
		admin:  cpv1.NewAdminServiceClient(conn),
		dist:   cpv1.NewDistributionServiceClient(conn),
		health: healthpb.NewHealthClient(conn),
	}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("error = %v, want code %v", err, want)
	}
}

// watch opens a Watch stream and returns it with the first snapshot.
func watch(t *testing.T, c clients, namespace string) (cpv1.DistributionService_WatchClient, *cpv1.Snapshot) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	stream, err := c.dist.Watch(ctx, &cpv1.WatchRequest{Namespace: namespace, ClientId: "e2e"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Watch: first snapshot: %v", err)
	}
	return stream, resp.GetSnapshot()
}

// awaitSnapshot reads the stream until a snapshot satisfies ok. Watchers may
// skip revisions, so tests wait for a state rather than for a message.
func awaitSnapshot(t *testing.T, stream cpv1.DistributionService_WatchClient, ok func(*cpv1.Snapshot) bool) *cpv1.Snapshot {
	t.Helper()
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		if ok(resp.GetSnapshot()) {
			return resp.GetSnapshot()
		}
	}
}

func findConfig(s *cpv1.Snapshot, key string) *cpv1.Config {
	for _, c := range s.GetConfigs() {
		if c.GetKey() == key {
			return c
		}
	}
	return nil
}

func findFlag(s *cpv1.Snapshot, key string) *cpv1.Flag {
	for _, f := range s.GetFlags() {
		if f.GetKey() == key {
			return f
		}
	}
	return nil
}

// httpClient reports redirects instead of following them.
var httpClient = &http.Client{
	Transport:     &http.Transport{},
	Timeout:       testTimeout,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// newRequest builds a request; a non-empty body is sent as JSON, with a
// bearer token unless token is empty.
func newRequest(t *testing.T, method, target, token, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

// do sends req and returns the response with its whole body.
func do(t *testing.T, req *http.Request) (*http.Response, string) {
	t.Helper()
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", req.Method, req.URL.Path, err)
	}
	return resp, string(body)
}

// wantMetrics fails unless the exposition holds each of the samples, given
// as complete lines: name{labels} value.
func wantMetrics(t *testing.T, exposition string, samples ...string) {
	t.Helper()
	lines := make(map[string]bool)
	for _, l := range strings.Split(exposition, "\n") {
		lines[l] = true
	}
	for _, s := range samples {
		if !lines[s] {
			t.Errorf("metrics lack the sample %s", s)
		}
	}
}

func TestServeEndToEnd(t *testing.T) {
	clearEnv(t)
	tokensFile, tokens := writeTokens(t)
	// The short rollout interval lets the controller advance a stage while
	// the test watches.
	ts := startServer(t, "--store", "memory", "--auth-tokens-file", tokensFile, "--rollout-interval", "50ms")
	anon := dial(t, ts.grpcAddr, "")
	reader := dial(t, ts.grpcAddr, tokens["reader"])
	editor := dial(t, ts.grpcAddr, tokens["editor"])
	admin := dial(t, ts.grpcAddr, tokens["admin"])
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	const ns = "e2e"

	// Health checks need no token, over gRPC or HTTP.
	hc, err := anon.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || hc.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("gRPC health check = %v, %v; want SERVING", hc.GetStatus(), err)
	}
	for path, want := range map[string]string{"/healthz": "ok\n", "/readyz": "ready\n"} {
		if resp, body := do(t, newRequest(t, http.MethodGet, ts.httpURL+path, "", "")); resp.StatusCode != http.StatusOK || body != want {
			t.Errorf("GET %s = %d %q, want 200 %q", path, resp.StatusCode, body, want)
		}
	}

	// Without a token every other call is refused, streams included.
	_, err = anon.admin.ListNamespaces(ctx, &cpv1.ListNamespacesRequest{})
	wantCode(t, err, codes.Unauthenticated)
	stream, err := anon.dist.Watch(ctx, &cpv1.WatchRequest{Namespace: ns})
	if err == nil {
		_, err = stream.Recv()
	}
	wantCode(t, err, codes.Unauthenticated)

	// Only admins create namespaces; the token's name is the actor.
	_, err = editor.admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: ns})
	wantCode(t, err, codes.PermissionDenied)
	created, err := admin.admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: ns})
	if err != nil {
		t.Fatalf("CreateNamespace as admin: %v", err)
	}
	if got := created.GetNamespace().GetCreatedBy(); got != "e2e-admin" {
		t.Errorf("created_by = %q, want e2e-admin", got)
	}

	// Readers watch but cannot write.
	w, first := watch(t, reader, ns)
	if first.GetRevision() != created.GetNamespace().GetRevision() {
		t.Fatalf("first snapshot at revision %d, want %d", first.GetRevision(), created.GetNamespace().GetRevision())
	}
	_, err = reader.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: ns, Key: "greeting", Value: structpb.NewStringValue("hi")})
	wantCode(t, err, codes.PermissionDenied)

	// An editor's write reaches the watcher.
	put, err := editor.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: ns, Key: "greeting", Value: structpb.NewStringValue("hello")})
	if err != nil {
		t.Fatalf("PutConfig as editor: %v", err)
	}
	snap := awaitSnapshot(t, w, func(s *cpv1.Snapshot) bool { return s.GetRevision() >= put.GetConfig().GetRevision() })
	if c := findConfig(snap, "greeting"); c.GetValue().GetStringValue() != "hello" || c.GetUpdatedBy() != "e2e-editor" {
		t.Fatalf("watcher got config %v, want greeting=hello by e2e-editor", c)
	}

	// The HTTP API takes the same tokens, and its writes reach gRPC watchers.
	const putBody = `{"namespace": "e2e", "key": "from-http", "value": {"color": "blue"}}`
	putPath := ts.httpURL + "/api/v1/AdminService/PutConfig"
	resp, body := do(t, newRequest(t, http.MethodPost, putPath, "", putBody))
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, `"Unauthenticated"`) {
		t.Fatalf("PutConfig over HTTP without a token = %d %s, want 401 Unauthenticated", resp.StatusCode, body)
	}
	resp, body = do(t, newRequest(t, http.MethodPost, putPath, tokens["editor"], putBody))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutConfig over HTTP as editor = %d %s, want 200", resp.StatusCode, body)
	}
	var putResp cpv1.PutConfigResponse
	if err := protojson.Unmarshal([]byte(body), &putResp); err != nil {
		t.Fatalf("PutConfig response %s: %v", body, err)
	}
	if got := putResp.GetConfig().GetUpdatedBy(); got != "e2e-editor" {
		t.Errorf("PutConfig over HTTP: updated_by = %q, want e2e-editor", got)
	}
	awaitSnapshot(t, w, func(s *cpv1.Snapshot) bool { return findConfig(s, "from-http") != nil })

	// The admin UI is served with its security headers; / leads to it.
	resp, body = do(t, newRequest(t, http.MethodGet, ts.httpURL+"/ui/", "", ""))
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") || !strings.Contains(strings.ToLower(body), "<html") {
		t.Fatalf("GET /ui/ = %d %q, want 200 with an HTML page", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if got := resp.Header.Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'self'") {
		t.Errorf("GET /ui/: Content-Security-Policy = %q, want default-src 'self'", got)
	}
	if resp, _ := do(t, newRequest(t, http.MethodGet, ts.httpURL+"/", "", "")); resp.StatusCode/100 != 3 || resp.Header.Get("Location") != "/ui/" {
		t.Errorf("GET / = %d to %q, want a redirect to /ui/", resp.StatusCode, resp.Header.Get("Location"))
	}

	// The rollout controller advances a stage once its duration has passed.
	if _, err := editor.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: ns, Key: "new-cart", Enabled: true}); err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	started, err := editor.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: ns, Flag: "new-cart", Stages: []*cpv1.RolloutStage{
		{Percent: 10, Duration: durationpb.New(time.Millisecond)},
		{Percent: 100},
	}})
	if err != nil {
		t.Fatalf("StartRollout: %v", err)
	}
	if got := started.GetFlag().GetRolloutPercent(); got != 10 {
		t.Fatalf("rollout started at %v%%, want 10%%", got)
	}
	snap = awaitSnapshot(t, w, func(s *cpv1.Snapshot) bool {
		return findFlag(s, "new-cart").GetRollout().GetState() == cpv1.RolloutState_ROLLOUT_STATE_COMPLETED
	})
	if f := findFlag(snap, "new-cart"); f.GetRolloutPercent() != 100 || f.GetUpdatedBy() != "rollout-controller" {
		t.Fatalf("after the controller advanced: flag %v, want 100%% by rollout-controller", f)
	}

	// Metrics need no token and count the calls auth rejected.
	resp, body = do(t, newRequest(t, http.MethodGet, ts.httpURL+"/metrics", "", ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	wantMetrics(t, body,
		`controlplane_grpc_requests_total{code="Unauthenticated",method="/controlplane.v1.AdminService/ListNamespaces"} 1`,
		`controlplane_grpc_requests_total{code="Unauthenticated",method="/controlplane.v1.DistributionService/Watch"} 1`,
		`controlplane_grpc_requests_total{code="PermissionDenied",method="/controlplane.v1.AdminService/CreateNamespace"} 1`,
		`controlplane_grpc_requests_total{code="OK",method="/controlplane.v1.AdminService/CreateNamespace"} 1`,
		`controlplane_watch_streams{namespace="e2e"} 1`,
		`controlplane_changes_received_total{source="rollout"} 1`,
	)

	// Shutting down ends the watch with UNAVAILABLE so that clients
	// reconnect to another replica.
	if err := ts.stop(); err != nil {
		t.Fatalf("serve: %v", err)
	}
	for {
		if _, err := w.Recv(); err != nil {
			wantCode(t, err, codes.Unavailable)
			break
		}
	}
}

func TestServeWithoutAuth(t *testing.T) {
	clearEnv(t)
	ts := startServer(t, "--store", "memory", "--rollout-interval", "0")
	c := dial(t, ts.grpcAddr, "")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Every call is allowed; the actor header names who made a change.
	created, err := c.admin.CreateNamespace(metadata.AppendToOutgoingContext(ctx, "x-controlplane-actor", "alice"),
		&cpv1.CreateNamespaceRequest{Name: "open"})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if got := created.GetNamespace().GetCreatedBy(); got != "alice" {
		t.Errorf("created_by = %q, want alice", got)
	}

	req := newRequest(t, http.MethodPost, ts.httpURL+"/api/v1/AdminService/PutConfig", "", `{"namespace": "open", "key": "greeting", "value": "hello"}`)
	req.Header.Set("X-Controlplane-Actor", "bob")
	resp, body := do(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PutConfig over HTTP = %d %s, want 200", resp.StatusCode, body)
	}
	var putResp cpv1.PutConfigResponse
	if err := protojson.Unmarshal([]byte(body), &putResp); err != nil {
		t.Fatalf("PutConfig response %s: %v", body, err)
	}
	if got := putResp.GetConfig().GetUpdatedBy(); got != "bob" {
		t.Errorf("PutConfig over HTTP: updated_by = %q, want bob", got)
	}

	snap, err := c.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "open"})
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got := findConfig(snap.GetSnapshot(), "greeting").GetValue().GetStringValue(); got != "hello" {
		t.Errorf("greeting = %q, want hello", got)
	}

	resp, body = do(t, newRequest(t, http.MethodGet, ts.httpURL+"/metrics", "", ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	wantMetrics(t, body,
		`controlplane_grpc_requests_total{code="OK",method="/controlplane.v1.AdminService/CreateNamespace"} 1`,
		`controlplane_grpc_requests_total{code="OK",method="/controlplane.v1.DistributionService/GetSnapshot"} 1`,
	)
}

// syncBuffer is a log destination safe for the server's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestLogsNeverHoldTokens runs a debug-level server through authenticated and
// rejected calls and checks that no token, or even its hash, was logged.
func TestLogsNeverHoldTokens(t *testing.T) {
	clearEnv(t)
	tokensFile, tokens := writeTokens(t)
	var logs syncBuffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ts := startLoggedServer(t, log, "--store", "memory", "--auth-tokens-file", tokensFile)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if _, err := dial(t, ts.grpcAddr, tokens["admin"]).admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: "logs"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	_, err := dial(t, ts.grpcAddr, "cp_not-a-real-token").admin.ListNamespaces(ctx, &cpv1.ListNamespacesRequest{})
	wantCode(t, err, codes.Unauthenticated)
	do(t, newRequest(t, http.MethodPost, ts.httpURL+"/api/v1/AdminService/ListNamespaces", tokens["editor"], "{}"))
	if err := ts.stop(); err != nil {
		t.Fatalf("serve: %v", err)
	}

	out := logs.String()
	if !strings.Contains(out, "authentication enabled") || strings.Contains(out, "AUTHENTICATION IS DISABLED") {
		t.Errorf("logs do not say that authentication is enabled:\n%s", out)
	}
	for role, token := range tokens {
		for _, secret := range []string{token, auth.HashToken(token)} {
			if strings.Contains(out, secret) {
				t.Errorf("logs contain the %s token or its hash", role)
			}
		}
	}
	if strings.Contains(out, "cp_not-a-real-token") {
		t.Error("logs contain a rejected token")
	}
}

// TestAuthDisabledIsLogged checks that running without a tokens file is
// announced at warning level, since it leaves the API open.
func TestAuthDisabledIsLogged(t *testing.T) {
	clearEnv(t)
	var logs syncBuffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	authn, err := loadAuth(mustParse(t, "--store", "memory"), log)
	if err != nil || authn != nil {
		t.Fatalf("loadAuth = %v, %v; want no authenticator and no error", authn, err)
	}
	if out := logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "AUTHENTICATION IS DISABLED") {
		t.Errorf("log = %q, want a WARN that authentication is disabled", out)
	}
}

// pingStore is a store whose Ping is the given function.
type pingStore struct {
	store.Store
	ping func(context.Context) error
}

func (s pingStore) Ping(ctx context.Context) error { return s.ping(ctx) }

func TestReadyz(t *testing.T) {
	clearEnv(t)
	var shuttingDown atomic.Bool
	var deadline time.Time
	pingErr := errors.New("store down")
	a := &app{cfg: mustParse(t, "--store", "memory"), log: discard, store: pingStore{
		Store: memory.New(),
		ping: func(ctx context.Context) error {
			d, ok := ctx.Deadline()
			if !ok {
				return errors.New("ping without a deadline")
			}
			deadline = d
			return pingErr
		},
	}}
	srv := server.New(server.Options{Store: a.store, Notifier: notify.NewLocal(), Logger: discard})
	handler := a.httpHandler(srv, metrics.New(), &shuttingDown)
	get := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec
	}

	// A store that fails its ping, within the 2s budget the probe allows.
	start := time.Now()
	rec := get()
	end := time.Now()
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("store down: /readyz = %d, want 503", rec.Code)
	}
	if deadline.Before(start.Add(readyTimeout)) || deadline.After(end.Add(readyTimeout)) {
		t.Errorf("ping deadline = %v after the request began, want %v", deadline.Sub(start), readyTimeout)
	}

	pingErr = nil
	if rec := get(); rec.Code != http.StatusOK {
		t.Errorf("store up: /readyz = %d, want 200", rec.Code)
	}

	// Shutting down fails readiness without asking the store.
	shuttingDown.Store(true)
	pingErr = errors.New("must not be asked")
	deadline = time.Time{}
	if rec := get(); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "shutting down") {
		t.Errorf("shutting down: /readyz = %d %q, want 503 shutting down", rec.Code, rec.Body.String())
	}
	if !deadline.IsZero() {
		t.Error("/readyz pinged the store while shutting down")
	}
	// Liveness stays up throughout.
	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if live.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", live.Code)
	}
}

func TestRunStartupErrors(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte(`{"tokens": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	busy := listen(t).Addr().String()

	for _, tc := range []struct {
		name string
		args []string
		want string // substring of the error
	}{
		{
			name: "missing tokens file",
			args: []string{"--store", "memory", "--auth-tokens-file", missing},
			want: "tokens file",
		},
		{
			name: "tokens file without tokens",
			args: []string{"--store", "memory", "--auth-tokens-file", empty},
			want: "tokens file",
		},
		{
			// Nothing listens on port 1: were the database dialed first,
			// that would be the error.
			name: "tokens file checked before the database",
			args: []string{"--database-url", "postgres://127.0.0.1:1/controlplane?connect_timeout=5", "--auth-tokens-file", missing},
			want: "tokens file",
		},
		{
			name: "HTTP address in use",
			args: []string{"--store", "memory", "--grpc-addr", "127.0.0.1:0", "--http-addr", busy},
			want: "listen " + busy,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), mustParse(t, tc.args...), discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run = %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}

// TestRunCancelledDuringStartup covers a signal that arrives before the
// servers are up: shutdown must not wait for servers that never started.
func TestRunCancelledDuringStartup(t *testing.T) {
	clearEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := mustParse(t, "--store", "memory", "--grpc-addr", "127.0.0.1:0", "--http-addr", "127.0.0.1:0")
	if err := run(ctx, cfg, discard); err != nil {
		t.Fatalf("run = %v, want nil", err)
	}
}

// TestServeShutdownWithUnresponsiveRedis covers a Redis that never confirms
// the change subscription, e.g. one cut off by a network partition. go-redis
// then waits without a deadline, whatever the context says, and shutdown
// must not wait with it.
func TestServeShutdownWithUnresponsiveRedis(t *testing.T) {
	clearEnv(t)
	addr, subscribed := unresponsiveRedis(t)
	client := redis.NewClient(&redis.Options{Addr: addr})
	a := &app{
		cfg:           mustParse(t, "--store", "memory", "--shutdown-timeout", "100ms"),
		log:           discard,
		store:         memory.New(),
		notifier:      notify.NewRedis(client, "", discard),
		closeNotifier: func() { client.Close() },
	}
	defer a.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.serve(ctx, listen(t), listen(t)) }()

	select {
	case <-subscribed:
	case <-time.After(testTimeout):
		t.Fatal("the server never subscribed to change events")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("serve did not return while the change subscription was stuck")
	}
}

// unresponsiveRedis accepts Redis clients and answers every command with an
// error, which go-redis's connection handshake tolerates, except SUBSCRIBE,
// which it never answers. It returns its address and a channel that receives
// a value per SUBSCRIBE.
func unresponsiveRedis(t *testing.T) (addr string, subscribed <-chan struct{}) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	subs := make(chan struct{}, 1)
	var (
		mu     sync.Mutex
		conns  []net.Conn
		closed bool
	)
	t.Cleanup(func() {
		lis.Close()
		mu.Lock()
		defer mu.Unlock()
		closed = true
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				c.Close()
				return
			}
			conns = append(conns, c)
			mu.Unlock()
			go func() {
				r := bufio.NewReader(c)
				for {
					cmd, err := readCommand(r)
					if err != nil {
						return
					}
					if strings.EqualFold(cmd[0], "subscribe") {
						select {
						case subs <- struct{}{}:
						default:
						}
						continue
					}
					if _, err := io.WriteString(c, "-ERR unknown command\r\n"); err != nil {
						return
					}
				}
			}()
		}
	}()
	return lis.Addr().String(), subs
}

// readCommand reads a command the way clients send them: a RESP array of
// bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	n, err := readLength(r, '*')
	if err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, fmt.Errorf("command with %d arguments", n)
	}
	args := make([]string, n)
	for i := range args {
		size, err := readLength(r, '$')
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, fmt.Errorf("argument of %d bytes", size)
		}
		buf := make([]byte, size+2) // and the trailing CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

// readLength reads a RESP header line: prefix, a decimal number, CRLF.
func readLength(r *bufio.Reader, prefix byte) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	if line[0] != prefix {
		return 0, fmt.Errorf("got %q, want a line starting with %q", line, prefix)
	}
	return strconv.Atoi(strings.TrimSuffix(line[1:], "\r\n"))
}

func TestMigrateCommand(t *testing.T) {
	clearEnv(t)
	dsn := os.Getenv("CONTROLPLANE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set CONTROLPLANE_TEST_DATABASE_URL to run PostgreSQL tests")
	}
	databaseURL := throwawaySchema(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	cfg := mustParse(t, "migrate", "--database-url", databaseURL)
	for i := range 2 { // the second run finds nothing to do
		if err := migrate(ctx, cfg, discard); err != nil {
			t.Fatalf("migrate (run %d): %v", i+1, err)
		}
	}

	// A server that does not migrate on startup works on the schema.
	a, err := open(ctx, mustParse(t, "--database-url", databaseURL, "--migrate=false"), discard)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer a.close()
	if _, err := a.store.CreateNamespace(ctx, model.Namespace{Name: "migrated"}, store.WriteOptions{Actor: "test"}); err != nil {
		t.Fatalf("CreateNamespace on the migrated schema: %v", err)
	}

	if err := migrate(ctx, mustParse(t, "migrate", "--database-url", "postgres://127.0.0.1:1/controlplane?connect_timeout=5"), discard); err == nil {
		t.Fatal("migrate without a reachable database succeeded")
	}
}

// throwawaySchema creates an empty schema, dropped when the test ends, and
// returns a database URL whose connections use it.
func throwawaySchema(t *testing.T, dsn string) string {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	schema := fmt.Sprintf("cp_cmd_test_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse CONTROLPLANE_TEST_DATABASE_URL: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
