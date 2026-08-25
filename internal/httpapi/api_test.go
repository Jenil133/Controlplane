package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

// call is one request a fake service received.
type call struct {
	method string
	ctx    context.Context
	req    proto.Message
}

// recorder collects the calls of the fake services.
type recorder struct {
	mu    sync.Mutex
	calls []call
}

func (r *recorder) record(ctx context.Context, method string, req proto.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call{method: method, ctx: ctx, req: req})
}

func (r *recorder) all() []call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// only returns the single call the services received.
func (r *recorder) only(t *testing.T) call {
	t.Helper()
	calls := r.all()
	if len(calls) != 1 {
		t.Fatalf("services received %d calls, want 1", len(calls))
	}
	return calls[0]
}

// fakeAdmin records the calls of the methods the tests use and answers them
// with err when it is set. Every other method answers Unimplemented.
type fakeAdmin struct {
	cpv1.UnimplementedAdminServiceServer
	rec  *recorder
	err  error
	flag *cpv1.Flag // returned by PutFlag
}

func (f *fakeAdmin) CreateNamespace(ctx context.Context, req *cpv1.CreateNamespaceRequest) (*cpv1.CreateNamespaceResponse, error) {
	f.rec.record(ctx, "CreateNamespace", req)
	if f.err != nil {
		return nil, f.err
	}
	return &cpv1.CreateNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName(), Revision: 1}}, nil
}

func (f *fakeAdmin) ListNamespaces(ctx context.Context, req *cpv1.ListNamespacesRequest) (*cpv1.ListNamespacesResponse, error) {
	f.rec.record(ctx, "ListNamespaces", req)
	if f.err != nil {
		return nil, f.err
	}
	return &cpv1.ListNamespacesResponse{Namespaces: []*cpv1.Namespace{{Name: "checkout/prod", Revision: 3}}}, nil
}

func (f *fakeAdmin) PutFlag(ctx context.Context, req *cpv1.PutFlagRequest) (*cpv1.PutFlagResponse, error) {
	f.rec.record(ctx, "PutFlag", req)
	if f.err != nil {
		return nil, f.err
	}
	return &cpv1.PutFlagResponse{Flag: f.flag}, nil
}

func (f *fakeAdmin) DeleteFlag(ctx context.Context, req *cpv1.DeleteFlagRequest) (*cpv1.DeleteFlagResponse, error) {
	f.rec.record(ctx, "DeleteFlag", req)
	if f.err != nil {
		return nil, f.err
	}
	return &cpv1.DeleteFlagResponse{Revision: 9}, nil
}

type fakeDistribution struct {
	cpv1.UnimplementedDistributionServiceServer
	rec      *recorder
	snapshot *cpv1.Snapshot
}

func (f *fakeDistribution) GetSnapshot(ctx context.Context, req *cpv1.GetSnapshotRequest) (*cpv1.GetSnapshotResponse, error) {
	f.rec.record(ctx, "GetSnapshot", req)
	return &cpv1.GetSnapshotResponse{Snapshot: f.snapshot}, nil
}

type fixture struct {
	handler http.Handler
	admin   *fakeAdmin
	dist    *fakeDistribution
	rec     *recorder
}

// newFixture serves fake services, with auth when authn is not nil.
func newFixture(t *testing.T, authn *auth.Authenticator) *fixture {
	t.Helper()
	rec := &recorder{}
	f := &fixture{admin: &fakeAdmin{rec: rec}, dist: &fakeDistribution{rec: rec}, rec: rec}
	f.handler = New(Options{Admin: f.admin, Distribution: f.dist, Auth: authn, Logger: slog.New(slog.DiscardHandler)})
	return f
}

func (f *fixture) serve(req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, req)
	return w
}

// apiRequest is a POST with a JSON body and headers given as name, value
// pairs.
func apiRequest(path, body string, headers ...string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	return req
}

// wantError checks a failed call's status and JSON body and returns its
// message.
func wantError(t *testing.T, w *httptest.ResponseRecorder, httpStatus int, code codes.Code) string {
	t.Helper()
	if w.Code != httpStatus {
		t.Fatalf("status = %d, want %d; body %s", w.Code, httpStatus, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	dec := json.NewDecoder(w.Body)
	dec.DisallowUnknownFields()
	var body errorBody
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("error body: %v", err)
	}
	if body.Code != code.String() {
		t.Errorf("code = %q, want %q (message %q)", body.Code, code, body.Message)
	}
	return body.Message
}

// wantOK checks a successful call and decodes its response.
func wantOK(t *testing.T, w *httptest.ResponseRecorder, resp proto.Message) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body)
	}
	for name, want := range map[string]string{"Content-Type": "application/json", "Cache-Control": "no-store"} {
		if got := w.Header().Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if err := protojson.Unmarshal(w.Body.Bytes(), resp); err != nil {
		t.Fatalf("response %s: %v", w.Body, err)
	}
}

// TestEveryUnaryRPCIsServed walks the service descriptors, so an RPC added
// to the API is checked without touching this test. The services are bare
// Unimplemented servers, whose error names the method that ran: proof that
// each path reaches its own method.
func TestEveryUnaryRPCIsServed(t *testing.T) {
	opts := Options{
		Admin:        cpv1.UnimplementedAdminServiceServer{},
		Distribution: cpv1.UnimplementedDistributionServiceServer{},
		Logger:       slog.New(slog.DiscardHandler),
	}
	table := newAPI(opts).methods
	handler := New(opts)

	unary := 0
	for _, sd := range []*grpc.ServiceDesc{&cpv1.AdminService_ServiceDesc, &cpv1.DistributionService_ServiceDesc} {
		service := strings.TrimPrefix(sd.ServiceName, "controlplane.v1.")
		for _, md := range sd.Methods {
			unary++
			path := "/api/v1/" + service + "/" + md.MethodName
			m, ok := table[path]
			if !ok {
				t.Errorf("%s is missing from the dispatch table", path)
				continue
			}
			if want := "/" + sd.ServiceName + "/" + md.MethodName; m.fullMethod != want {
				t.Errorf("%s authorizes as %s, want %s", path, m.fullMethod, want)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, apiRequest(path, "{}"))
			msg := wantError(t, w, http.StatusInternalServerError, codes.Unimplemented)
			if want := "method " + md.MethodName + " not implemented"; msg != want {
				t.Errorf("%s ran %q, want %q", path, msg, want)
			}
		}
		for _, st := range sd.Streams {
			path := "/api/v1/" + service + "/" + st.StreamName
			if _, ok := table[path]; ok {
				t.Errorf("stream %s is in the dispatch table", path)
			}
		}
	}
	if len(table) != unary {
		t.Errorf("dispatch table has %d methods, the services have %d unary RPCs", len(table), unary)
	}
}

func TestNilServicesAreNotExposed(t *testing.T) {
	h := New(Options{Distribution: cpv1.UnimplementedDistributionServiceServer{}, Logger: slog.New(slog.DiscardHandler)})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, apiRequest("/api/v1/AdminService/ListNamespaces", "{}"))
	wantError(t, w, http.StatusNotFound, codes.NotFound)

	w = httptest.NewRecorder()
	h.ServeHTTP(w, apiRequest("/api/v1/DistributionService/GetSnapshot", "{}"))
	wantError(t, w, http.StatusInternalServerError, codes.Unimplemented)
}

func TestPutFlagRoundTrip(t *testing.T) {
	f := newFixture(t, nil)
	started := time.Date(2026, 10, 3, 1, 2, 3, 0, time.UTC)
	f.admin.flag = &cpv1.Flag{
		Key:            "new-cart",
		Enabled:        true,
		Revision:       8,
		UpdatedAt:      timestamppb.New(started),
		UpdatedBy:      "alice",
		RolloutPercent: 5,
		Salt:           "s1",
		Allowlist:      []string{"u1", "u2"},
		Rollout: &cpv1.RolloutPlan{
			Stages: []*cpv1.RolloutStage{
				{Percent: 5, Duration: durationpb.New(10 * time.Minute)},
				{Percent: 100},
			},
			State:          cpv1.RolloutState_ROLLOUT_STATE_ACTIVE,
			StartedAt:      timestamppb.New(started),
			StageStartedAt: timestamppb.New(started),
			StartedBy:      "alice",
		},
	}

	// rolloutPercent 0 must arrive as set: unset would keep the current
	// value. int64s may come as strings or numbers.
	w := f.serve(apiRequest("/api/v1/AdminService/PutFlag", `{
		"namespace": "checkout/prod",
		"key": "new-cart",
		"enabled": true,
		"expectedRevision": "7",
		"rolloutPercent": 0,
		"salt": "s1",
		"allowlist": ["u1", "u2"]
	}`))
	var resp cpv1.PutFlagResponse
	wantOK(t, w, &resp)

	wantReq := &cpv1.PutFlagRequest{
		Namespace:        "checkout/prod",
		Key:              "new-cart",
		Enabled:          true,
		ExpectedRevision: 7,
		RolloutPercent:   proto.Float64(0),
		Salt:             "s1",
		Allowlist:        []string{"u1", "u2"},
	}
	if got := f.rec.only(t); got.method != "PutFlag" || !proto.Equal(got.req, wantReq) {
		t.Errorf("service got %s(%v), want PutFlag(%v)", got.method, got.req, wantReq)
	}
	if !proto.Equal(resp.GetFlag(), f.admin.flag) {
		t.Errorf("response flag = %v, want %v", resp.GetFlag(), f.admin.flag)
	}

	// The UI depends on this exact shape.
	var shape struct {
		Flag map[string]any `json:"flag"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &shape); err != nil {
		t.Fatal(err)
	}
	flag := shape.Flag
	rollout, _ := flag["rollout"].(map[string]any)
	stages, _ := rollout["stages"].([]any)
	if len(stages) != 2 {
		t.Fatalf("rollout.stages = %v", rollout["stages"])
	}
	first, _ := stages[0].(map[string]any)
	last, _ := stages[1].(map[string]any)
	for _, c := range []struct {
		name      string
		got, want any
	}{
		{"revision", flag["revision"], "8"},
		{"rolloutPercent", flag["rolloutPercent"], 5.0},
		{"description (unpopulated)", flag["description"], ""},
		{"updatedAt", flag["updatedAt"], "2026-10-03T01:02:03Z"},
		{"rollout.state", rollout["state"], "ROLLOUT_STATE_ACTIVE"},
		{"rollout.currentStage (unpopulated)", rollout["currentStage"], 0.0},
		{"stages[0].duration", first["duration"], "600s"},
		{"stages[1].duration (unset)", last["duration"], nil},
	} {
		if c.got != c.want {
			t.Errorf("%s = %#v, want %#v", c.name, c.got, c.want)
		}
	}
	if _, ok := flag["rollout_percent"]; ok {
		t.Error("response uses proto field names, want lowerCamelCase JSON names")
	}
}

func TestGetSnapshot(t *testing.T) {
	f := newFixture(t, nil)
	value, err := structpb.NewValue(map[string]any{"timeout_ms": 250.0, "regions": []any{"eu", "us"}})
	if err != nil {
		t.Fatal(err)
	}
	f.dist.snapshot = &cpv1.Snapshot{
		Namespace:  "checkout/prod",
		Revision:   12,
		UpdatedAt:  timestamppb.New(time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)),
		Configs:    []*cpv1.Config{{Key: "payments", Value: value, Revision: 4}},
		RateLimits: []*cpv1.RateLimit{{Key: "checkout", Enabled: true, RequestsPerSecond: 0.5, Burst: 3, Revision: 10}},
		CircuitBreakers: []*cpv1.CircuitBreaker{{
			Key: "payments", Enabled: true, FailureRateThreshold: 0.5, MinRequests: 20,
			Window: durationpb.New(10 * time.Second), OpenDuration: durationpb.New(1500 * time.Millisecond),
			HalfOpenMaxRequests: 5, Revision: 12,
		}},
	}

	w := f.serve(apiRequest("/api/v1/DistributionService/GetSnapshot", `{"namespace": "checkout/prod"}`))
	var resp cpv1.GetSnapshotResponse
	wantOK(t, w, &resp)
	if got := f.rec.only(t); !proto.Equal(got.req, &cpv1.GetSnapshotRequest{Namespace: "checkout/prod"}) {
		t.Errorf("request = %v", got.req)
	}
	if !proto.Equal(resp.GetSnapshot(), f.dist.snapshot) {
		t.Errorf("snapshot = %v, want %v", resp.GetSnapshot(), f.dist.snapshot)
	}
	body := w.Body.String()
	for _, want := range []string{`"rateLimits"`, `"circuitBreakers"`, `"openDuration"`, `"1.500s"`, `"experiments"`} {
		if !strings.Contains(body, want) {
			t.Errorf("response lacks %s: %s", want, body)
		}
	}
}

func TestServiceErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		httpStatus int
		code       codes.Code
		message    string
	}{
		{"invalid argument", status.Error(codes.InvalidArgument, "key is required"), http.StatusBadRequest, codes.InvalidArgument, "key is required"},
		{"not found", status.Error(codes.NotFound, "no flag x"), http.StatusNotFound, codes.NotFound, "no flag x"},
		{"already exists", status.Error(codes.AlreadyExists, "exists"), http.StatusConflict, codes.AlreadyExists, "exists"},
		{"aborted", status.Error(codes.Aborted, "conflict: at revision 8"), http.StatusConflict, codes.Aborted, "conflict: at revision 8"},
		{"failed precondition", status.Error(codes.FailedPrecondition, "rollout owns it"), http.StatusPreconditionFailed, codes.FailedPrecondition, "rollout owns it"},
		{"unauthenticated", status.Error(codes.Unauthenticated, "who"), http.StatusUnauthorized, codes.Unauthenticated, "who"},
		{"permission denied", status.Error(codes.PermissionDenied, "no"), http.StatusForbidden, codes.PermissionDenied, "no"},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "slow down"), http.StatusTooManyRequests, codes.ResourceExhausted, "slow down"},
		{"unavailable", status.Error(codes.Unavailable, "shutting down"), http.StatusServiceUnavailable, codes.Unavailable, "shutting down"},
		{"internal", status.Error(codes.Internal, "internal error"), http.StatusInternalServerError, codes.Internal, "internal error"},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "too slow"), http.StatusInternalServerError, codes.DeadlineExceeded, "too slow"},
		{"plain error", errors.New("disk on fire"), http.StatusInternalServerError, codes.Unknown, "disk on fire"},
		{"context error", fmt.Errorf("load: %w", context.Canceled), http.StatusInternalServerError, codes.Canceled, "load: context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			f.admin.err = tc.err
			w := f.serve(apiRequest("/api/v1/AdminService/DeleteFlag", `{"namespace": "ns", "key": "x"}`))
			if msg := wantError(t, w, tc.httpStatus, tc.code); msg != tc.message {
				t.Errorf("message = %q, want %q", msg, tc.message)
			}
			if got := w.Header().Get("WWW-Authenticate") != ""; got != (tc.httpStatus == http.StatusUnauthorized) {
				t.Errorf("WWW-Authenticate = %q", w.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestRejectedRequests(t *testing.T) {
	const listNamespaces = "/api/v1/AdminService/ListNamespaces"
	for _, tc := range []struct {
		name        string
		method      string // default POST
		path        string
		contentType string // default application/json; "-" for none
		body        string
		httpStatus  int
		code        codes.Code
	}{
		{name: "unknown method", path: "/api/v1/AdminService/DropEverything", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "unknown service", path: "/api/v1/HealthService/Check", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "streaming method", path: "/api/v1/DistributionService/Watch", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "full gRPC name", path: "/api/v1/controlplane.v1.AdminService/ListNamespaces", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "trailing slash", path: listNamespaces + "/", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "other version", path: "/api/v2/AdminService/ListNamespaces", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "service only", path: "/api/v1/AdminService", httpStatus: http.StatusNotFound, code: codes.NotFound},
		{name: "GET", method: http.MethodGet, path: listNamespaces, httpStatus: http.StatusMethodNotAllowed, code: codes.Unimplemented},
		{name: "PUT", method: http.MethodPut, path: listNamespaces, httpStatus: http.StatusMethodNotAllowed, code: codes.Unimplemented},
		{name: "CORS preflight", method: http.MethodOptions, path: listNamespaces, httpStatus: http.StatusMethodNotAllowed, code: codes.Unimplemented},
		{name: "no content type", path: listNamespaces, contentType: "-", httpStatus: http.StatusUnsupportedMediaType, code: codes.InvalidArgument},
		{name: "form post", path: listNamespaces, contentType: "application/x-www-form-urlencoded", httpStatus: http.StatusUnsupportedMediaType, code: codes.InvalidArgument},
		{name: "text/plain", path: listNamespaces, contentType: "text/plain", httpStatus: http.StatusUnsupportedMediaType, code: codes.InvalidArgument},
		{name: "not UTF-8", path: listNamespaces, contentType: "application/json; charset=iso-8859-1", httpStatus: http.StatusUnsupportedMediaType, code: codes.InvalidArgument},
		{name: "malformed content type", path: listNamespaces, contentType: "application/json; charset", httpStatus: http.StatusUnsupportedMediaType, code: codes.InvalidArgument},
		{name: "malformed JSON", path: listNamespaces, body: "{", httpStatus: http.StatusBadRequest, code: codes.InvalidArgument},
		{name: "not an object", path: listNamespaces, body: "[]", httpStatus: http.StatusBadRequest, code: codes.InvalidArgument},
		{name: "unknown field", path: listNamespaces, body: `{"verbose": true}`, httpStatus: http.StatusBadRequest, code: codes.InvalidArgument},
		{name: "wrong type", path: "/api/v1/AdminService/PutFlag", body: `{"enabled": "yes"}`, httpStatus: http.StatusBadRequest, code: codes.InvalidArgument},
		{name: "trailing data", path: listNamespaces, body: "{} {}", httpStatus: http.StatusBadRequest, code: codes.InvalidArgument},
		{name: "body over 1 MiB", path: listNamespaces, body: "{}" + strings.Repeat(" ", maxBodyBytes-1), httpStatus: http.StatusRequestEntityTooLarge, code: codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, tc.path, strings.NewReader(tc.body))
			switch tc.contentType {
			case "":
				req.Header.Set("Content-Type", "application/json")
			case "-":
			default:
				req.Header.Set("Content-Type", tc.contentType)
			}
			w := f.serve(req)
			wantError(t, w, tc.httpStatus, tc.code)
			if tc.httpStatus == http.StatusMethodNotAllowed {
				if allow := w.Header().Get("Allow"); allow != http.MethodPost {
					t.Errorf("Allow = %q, want POST", allow)
				}
			}
			if calls := f.rec.all(); len(calls) != 0 {
				t.Errorf("services were called: %v", calls)
			}
		})
	}
}

func TestRequestBodies(t *testing.T) {
	// A body of exactly the limit is accepted.
	prefix, suffix := `{"namespace": "ns", "key": "k", "description": "`, `"}`
	description := strings.Repeat("d", maxBodyBytes-len(prefix)-len(suffix))
	limit := prefix + description + suffix

	for _, tc := range []struct {
		name, path, contentType, body string
		want                          proto.Message
	}{
		{"empty body", "/api/v1/AdminService/ListNamespaces", "application/json", "", &cpv1.ListNamespacesRequest{}},
		{"blank body", "/api/v1/AdminService/ListNamespaces", "application/json", " \n", &cpv1.ListNamespacesRequest{}},
		{"charset", "/api/v1/AdminService/ListNamespaces", "application/json; charset=utf-8", "{}", &cpv1.ListNamespacesRequest{}},
		{"letter case", "/api/v1/AdminService/ListNamespaces", "Application/JSON;Charset=UTF-8", "{}", &cpv1.ListNamespacesRequest{}},
		{"proto field names", "/api/v1/AdminService/PutFlag", "application/json", `{"namespace": "ns", "key": "k", "expected_revision": 3}`, &cpv1.PutFlagRequest{Namespace: "ns", Key: "k", ExpectedRevision: 3}},
		{"largest body", "/api/v1/AdminService/PutFlag", "application/json", limit, &cpv1.PutFlagRequest{Namespace: "ns", Key: "k", Description: description}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			req := apiRequest(tc.path, tc.body)
			req.Header.Set("Content-Type", tc.contentType)
			w := f.serve(req)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d; body %.200s", w.Code, w.Body)
			}
			if got := f.rec.only(t); !proto.Equal(got.req, tc.want) {
				t.Errorf("service got %.200v, want %.200v", got.req, tc.want)
			}
		})
	}
}

// testTokens registers one caller per role.
var testTokens = map[auth.Role]struct{ name, token string }{
	auth.RoleReader: {"dashboard", "cp_reader-test-token"},
	auth.RoleEditor: {"deploy-bot", "cp_editor-test-token"},
	auth.RoleAdmin:  {"ops", "cp_admin-test-token"},
}

func newTestAuthenticator(t *testing.T) *auth.Authenticator {
	t.Helper()
	var entries []auth.Entry
	for role, c := range testTokens {
		entries = append(entries, auth.Entry{Name: c.name, Role: role.String(), TokenSHA256: auth.HashToken(c.token)})
	}
	a, err := auth.New(entries)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func bearer(role auth.Role) string { return "Bearer " + testTokens[role].token }

func TestAuth(t *testing.T) {
	const (
		listNamespaces  = "/api/v1/AdminService/ListNamespaces"
		createNamespace = "/api/v1/AdminService/CreateNamespace"
		putFlag         = "/api/v1/AdminService/PutFlag"
		getSnapshot     = "/api/v1/DistributionService/GetSnapshot"
	)
	for _, tc := range []struct {
		name          string
		path          string
		authorization []string
		body          string // default {}
		httpStatus    int
		code          codes.Code
		want          auth.Role // principal the service sees
	}{
		{name: "no token", path: listNamespaces, httpStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "unknown token", path: listNamespaces, authorization: []string{"Bearer cp_unknown"}, httpStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "not a bearer token", path: listNamespaces, authorization: []string{"Basic b3BzOnNlY3JldA=="}, httpStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "two tokens", path: listNamespaces, authorization: []string{bearer(auth.RoleAdmin), bearer(auth.RoleAdmin)}, httpStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "checked before the body", path: putFlag, body: "{", httpStatus: http.StatusUnauthorized, code: codes.Unauthenticated},
		{name: "reader lists", path: listNamespaces, authorization: []string{bearer(auth.RoleReader)}, httpStatus: http.StatusOK, want: auth.RoleReader},
		{name: "reader reads a snapshot", path: getSnapshot, authorization: []string{bearer(auth.RoleReader)}, httpStatus: http.StatusOK, want: auth.RoleReader},
		{name: "reader cannot write", path: putFlag, authorization: []string{bearer(auth.RoleReader)}, httpStatus: http.StatusForbidden, code: codes.PermissionDenied},
		{name: "editor writes", path: putFlag, authorization: []string{bearer(auth.RoleEditor)}, httpStatus: http.StatusOK, want: auth.RoleEditor},
		{name: "scheme is case-insensitive", path: putFlag, authorization: []string{"bearer " + testTokens[auth.RoleEditor].token}, httpStatus: http.StatusOK, want: auth.RoleEditor},
		{name: "editor cannot create namespaces", path: createNamespace, authorization: []string{bearer(auth.RoleEditor)}, httpStatus: http.StatusForbidden, code: codes.PermissionDenied},
		{name: "admin creates namespaces", path: createNamespace, authorization: []string{bearer(auth.RoleAdmin)}, httpStatus: http.StatusOK, want: auth.RoleAdmin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, newTestAuthenticator(t))
			body := tc.body
			if body == "" {
				body = "{}"
			}
			// With auth on, the actor is the token's owner: the header is
			// not passed on.
			headers := []string{"X-Controlplane-Actor", "mallory"}
			for _, v := range tc.authorization {
				headers = append(headers, "Authorization", v)
			}
			w := f.serve(apiRequest(tc.path, body, headers...))

			if tc.httpStatus != http.StatusOK {
				wantError(t, w, tc.httpStatus, tc.code)
				if got := w.Header().Get("WWW-Authenticate") != ""; got != (tc.httpStatus == http.StatusUnauthorized) {
					t.Errorf("WWW-Authenticate = %q", w.Header().Get("WWW-Authenticate"))
				}
				if calls := f.rec.all(); len(calls) != 0 {
					t.Errorf("service was called: %v", calls)
				}
				return
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d; body %s", w.Code, w.Body)
			}
			ctx := f.rec.only(t).ctx
			want := auth.Principal{Name: testTokens[tc.want].name, Role: tc.want}
			if p, ok := auth.PrincipalFrom(ctx); !ok || p != want {
				t.Errorf("principal = %v, %v; want %v", p, ok, want)
			}
			if md, _ := metadata.FromIncomingContext(ctx); len(md.Get(actorMetadataKey)) != 0 {
				t.Errorf("actor header passed on with auth enabled: %v", md)
			}
		})
	}
}

func TestActorWithoutAuth(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers []string
		want    []string
	}{
		{name: "actor header", headers: []string{"X-Controlplane-Actor", "alice"}, want: []string{"alice"}},
		{name: "no header"},
		{name: "token ignored", headers: []string{"Authorization", bearer(auth.RoleAdmin)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			w := f.serve(apiRequest("/api/v1/AdminService/ListNamespaces", "{}", tc.headers...))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d; body %s", w.Code, w.Body)
			}
			ctx := f.rec.only(t).ctx
			md, _ := metadata.FromIncomingContext(ctx)
			if got := md.Get(actorMetadataKey); !slices.Equal(got, tc.want) {
				t.Errorf("%s metadata = %q, want %q", actorMetadataKey, got, tc.want)
			}
			if p, ok := auth.PrincipalFrom(ctx); ok {
				t.Errorf("principal %v with auth disabled", p)
			}
		})
	}
}

// TestAuthGuardsEveryMethod walks the dispatch table with auth on: no route
// answers without a valid token, and each method demands the role the auth
// package assigns to it, so a method added to the API cannot be left open.
func TestAuthGuardsEveryMethod(t *testing.T) {
	opts := Options{
		Admin:        cpv1.UnimplementedAdminServiceServer{},
		Distribution: cpv1.UnimplementedDistributionServiceServer{},
		Auth:         newTestAuthenticator(t),
		Logger:       slog.New(slog.DiscardHandler),
	}
	handler := New(opts)
	for path, m := range newAPI(opts).methods {
		for _, role := range []auth.Role{0, auth.RoleReader, auth.RoleEditor, auth.RoleAdmin} {
			var headers []string
			if role != 0 {
				headers = []string{"Authorization", bearer(role)}
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, apiRequest(path, "{}", headers...))
			required, _ := auth.RequiredRole(m.fullMethod)
			switch {
			case role == 0:
				wantError(t, w, http.StatusUnauthorized, codes.Unauthenticated)
			case role < required:
				wantError(t, w, http.StatusForbidden, codes.PermissionDenied)
			default:
				// Authorized: the bare service then answers Unimplemented.
				wantError(t, w, http.StatusInternalServerError, codes.Unimplemented)
			}
		}
	}
}

// TestAuthIsNotBypassedByPathForms sends encoded and unclean forms of a
// protected path without a token. Whatever way the path is spelt, the call
// either fails authentication or never reaches a service.
func TestAuthIsNotBypassedByPathForms(t *testing.T) {
	for _, path := range []string{
		"/api/v1/AdminService/Put%46lag",
		"/api/v1/AdminService%2FPutFlag",
		"/api/v1//AdminService/PutFlag",
		"/api/v1/AdminService/./PutFlag",
		"/api/v1/x/../AdminService/PutFlag",
		"/api/v1/AdminService/putflag",
		"/api/v1/AdminService/PutFlag/",
		"/API/v1/AdminService/PutFlag",
	} {
		t.Run(path, func(t *testing.T) {
			f := newFixture(t, newTestAuthenticator(t))
			w := f.serve(apiRequest(path, `{"namespace": "ns", "key": "k"}`, "X-Controlplane-Actor", "mallory"))
			if w.Code == http.StatusOK {
				t.Errorf("status 200 without a token; body %s", w.Body)
			}
			if calls := f.rec.all(); len(calls) != 0 {
				t.Errorf("service was called: %v", calls)
			}
		})
	}
}

// TestOtherVerbsDoNotReachServices confirms that no other HTTP verb, even
// with a method-override header, reaches the services, with or without a
// token.
func TestOtherVerbsDoNotReachServices(t *testing.T) {
	for _, verb := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodTrace} {
		for _, authorization := range []string{"", bearer(auth.RoleAdmin)} {
			f := newFixture(t, newTestAuthenticator(t))
			req := httptest.NewRequest(verb, "/api/v1/AdminService/DeleteFlag", strings.NewReader(`{"namespace": "ns", "key": "k"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-HTTP-Method-Override", "POST")
			if authorization != "" {
				req.Header.Set("Authorization", authorization)
			}
			if w := f.serve(req); w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s: status %d, want 405", verb, w.Code)
			}
			if calls := f.rec.all(); len(calls) != 0 {
				t.Errorf("%s: service was called: %v", verb, calls)
			}
		}
	}
}
