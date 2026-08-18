package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

func TestBearerToken(t *testing.T) {
	for _, tc := range []struct{ header, want string }{
		{"Bearer cp_abc", "cp_abc"},
		{"bearer cp_abc", "cp_abc"},
		{"BEARER cp_abc", "cp_abc"},
		{"Bearer   cp_abc", "cp_abc"},
		{"  Bearer cp_abc \t", "cp_abc"},
		{"Bearer cp-abc_09.~+/=", "cp-abc_09.~+/="},
		{"", ""},
		{"cp_abc", ""},
		{"Bearer", ""},
		{"Bearer ", ""},
		{"Bearercp_abc", ""},
		{"Bearer\tcp_abc", ""},
		{"Basic dXNlcjpwYXNz", ""},
		{"Token cp_abc", ""},
		{"Bearer cp_abc extra", ""},
		{"Bearer cp_abc\textra", ""},
	} {
		if got := BearerToken(tc.header); got != tc.want {
			t.Errorf("BearerToken(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}

// connInfo is the AuthInfo of a connection with a given security level.
type connInfo struct{ credentials.CommonAuthInfo }

func (connInfo) AuthType() string { return "test" }

func onConnection(level credentials.SecurityLevel) context.Context {
	return credentials.NewContextWithRequestInfo(context.Background(), credentials.RequestInfo{
		AuthInfo: connInfo{credentials.CommonAuthInfo{SecurityLevel: level}},
	})
}

func TestTokenCredentialsMetadata(t *testing.T) {
	bearer := map[string]string{"authorization": "Bearer cp_abc"}
	for _, tc := range []struct {
		name       string
		token      string
		requireTLS bool
		ctx        context.Context
		want       map[string]string // nil: no header
		wantErr    bool
	}{
		{name: "plaintext allowed", token: "cp_abc", ctx: onConnection(credentials.NoSecurity), want: bearer},
		{name: "outside a connection", token: "cp_abc", ctx: context.Background(), want: bearer},
		{name: "TLS required and present", token: "cp_abc", requireTLS: true, ctx: onConnection(credentials.PrivacyAndIntegrity), want: bearer},
		{name: "TLS required on plaintext", token: "cp_abc", requireTLS: true, ctx: onConnection(credentials.NoSecurity), wantErr: true},
		{name: "TLS required on integrity only", token: "cp_abc", requireTLS: true, ctx: onConnection(credentials.IntegrityOnly), wantErr: true},
		{name: "TLS required outside a connection", token: "cp_abc", requireTLS: true, ctx: context.Background(), wantErr: true},
		{name: "empty token", ctx: onConnection(credentials.NoSecurity)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds := TokenCredentials(tc.token, tc.requireTLS)
			if creds.RequireTransportSecurity() != tc.requireTLS {
				t.Errorf("RequireTransportSecurity = %v, want %v", creds.RequireTransportSecurity(), tc.requireTLS)
			}
			md, err := creds.GetRequestMetadata(tc.ctx, "https://bufconn/controlplane.v1.AdminService")
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetRequestMetadata error = %v, want error %v", err, tc.wantErr)
			}
			if len(md) != len(tc.want) || md["authorization"] != tc.want["authorization"] {
				t.Fatalf("GetRequestMetadata = %v, want %v", md, tc.want)
			}
		})
	}
}

// testCallKey is metadata the tests send to tell their calls apart.
const testCallKey = "x-test-call"

// recorder notes what each fake handler saw of its call.
type recorder struct {
	mu    sync.Mutex
	calls []string
	// watchEnded receives the error that ended each Watch handler's context.
	watchEnded chan error
}

// record notes the call as "<method> by <principal>". The interceptors must
// hand handlers the context the call came in with, plus the principal:
// handlers rely on its metadata (e.g. the actor header), deadline and peer.
// So record also notes the call's testCallKey metadata as " in <value>" and
// flags a missing deadline or peer.
func (r *recorder) record(ctx context.Context, method string) {
	who := "no principal"
	if p, ok := PrincipalFrom(ctx); ok {
		who = p.Role.String() + " " + p.Name
	}
	call := method + " by " + who
	if id := metadata.ValueFromIncomingContext(ctx, testCallKey); len(id) > 0 {
		call += " in " + strings.Join(id, ",")
	}
	if _, ok := ctx.Deadline(); !ok {
		call += " without deadline"
	}
	if _, ok := peer.FromContext(ctx); !ok {
		call += " without peer"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

// take returns and forgets the calls recorded so far.
func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := r.calls
	r.calls = nil
	return calls
}

// fakeAdmin and fakeDistribution stand in for internal/server.
type fakeAdmin struct {
	cpv1.UnimplementedAdminServiceServer
	rec *recorder
}

func (f *fakeAdmin) CreateNamespace(ctx context.Context, req *cpv1.CreateNamespaceRequest) (*cpv1.CreateNamespaceResponse, error) {
	f.rec.record(ctx, "CreateNamespace")
	return &cpv1.CreateNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName()}}, nil
}

func (f *fakeAdmin) GetNamespace(ctx context.Context, req *cpv1.GetNamespaceRequest) (*cpv1.GetNamespaceResponse, error) {
	f.rec.record(ctx, "GetNamespace")
	return &cpv1.GetNamespaceResponse{Namespace: &cpv1.Namespace{Name: req.GetName()}}, nil
}

func (f *fakeAdmin) PutFlag(ctx context.Context, req *cpv1.PutFlagRequest) (*cpv1.PutFlagResponse, error) {
	f.rec.record(ctx, "PutFlag")
	return &cpv1.PutFlagResponse{Flag: &cpv1.Flag{Key: req.GetKey()}}, nil
}

type fakeDistribution struct {
	cpv1.UnimplementedDistributionServiceServer
	rec *recorder
}

// Watch sends one snapshot and then, like the real one, serves the stream
// until the client goes away, reporting how its context ended.
func (f *fakeDistribution) Watch(req *cpv1.WatchRequest, stream grpc.ServerStreamingServer[cpv1.WatchResponse]) error {
	ctx := stream.Context()
	f.rec.record(ctx, "Watch")
	if err := stream.Send(&cpv1.WatchResponse{Snapshot: &cpv1.Snapshot{Namespace: req.GetNamespace(), Revision: 1}}); err != nil {
		return err
	}
	<-ctx.Done()
	f.rec.watchEnded <- ctx.Err()
	return ctx.Err()
}

// startServer serves the fake API plus the standard health and reflection
// services behind a's interceptors over an in-memory listener.
func startServer(t *testing.T, a *Authenticator, opts ...grpc.ServerOption) (*bufconn.Listener, *recorder) {
	t.Helper()
	rec := &recorder{watchEnded: make(chan error, 1)}
	g := grpc.NewServer(append(opts,
		grpc.ChainUnaryInterceptor(a.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(a.StreamServerInterceptor()),
	)...)
	cpv1.RegisterAdminServiceServer(g, &fakeAdmin{rec: rec})
	cpv1.RegisterDistributionServiceServer(g, &fakeDistribution{rec: rec})
	healthpb.RegisterHealthServer(g, health.NewServer())
	reflection.Register(g)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis, rec
}

func dial(t *testing.T, lis *bufconn.Listener, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufconn", append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func dialInsecure(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	return dial(t, lis, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestInterceptorsOverGRPC(t *testing.T) {
	a, callers := newTestAuthenticator(t)
	lis, rec := startServer(t, a)
	conn := dialInsecure(t, lis)
	admin, dist := cpv1.NewAdminServiceClient(conn), cpv1.NewDistributionServiceClient(conn)

	rpcs := map[string]func(t *testing.T, ctx context.Context, opts ...grpc.CallOption) error{
		"GetNamespace": func(_ *testing.T, ctx context.Context, opts ...grpc.CallOption) error {
			_, err := admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "svc"}, opts...)
			return err
		},
		"PutFlag": func(_ *testing.T, ctx context.Context, opts ...grpc.CallOption) error {
			_, err := admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart"}, opts...)
			return err
		},
		"CreateNamespace": func(_ *testing.T, ctx context.Context, opts ...grpc.CallOption) error {
			_, err := admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: "svc"}, opts...)
			return err
		},
		// A server stream reports its status on the first Recv. The client
		// then hangs up, which the handler must notice: a Watch that never
		// learns that its client is gone keeps a goroutine and a hub
		// subscription until shutdown.
		"Watch": func(t *testing.T, ctx context.Context, opts ...grpc.CallOption) error {
			callCtx, hangUp := context.WithCancel(ctx)
			defer hangUp()
			stream, err := dist.Watch(callCtx, &cpv1.WatchRequest{Namespace: "svc"}, opts...)
			if err != nil {
				return err
			}
			if _, err := stream.Recv(); err != nil {
				return err
			}
			hangUp()
			select {
			case err := <-rec.watchEnded:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Watch handler's context ended with %v after the client hung up, want %v", err, context.Canceled)
				}
			case <-ctx.Done():
				t.Fatal("Watch handler still running after its client hung up")
			}
			return nil
		},
	}

	// Callers, in the order of the want columns below.
	type who struct {
		name  string
		token string // "" sends no credentials
		as    Principal
	}
	whos := []who{
		{name: "anonymous"},
		{name: "unknown token", token: "cp_bogus"},
		{name: "reader", token: callers[RoleReader].token, as: callers[RoleReader].principal},
		{name: "editor", token: callers[RoleEditor].token, as: callers[RoleEditor].principal},
		{name: "admin", token: callers[RoleAdmin].token, as: callers[RoleAdmin].principal},
	}
	const (
		ok     = codes.OK
		unauth = codes.Unauthenticated
		denied = codes.PermissionDenied
	)
	for _, tc := range []struct {
		rpc  string
		want [5]codes.Code
	}{
		{"GetNamespace", [5]codes.Code{unauth, unauth, ok, ok, ok}},
		{"Watch", [5]codes.Code{unauth, unauth, ok, ok, ok}},
		{"PutFlag", [5]codes.Code{unauth, unauth, denied, ok, ok}},
		{"CreateNamespace", [5]codes.Code{unauth, unauth, denied, denied, ok}},
	} {
		for i, w := range whos {
			t.Run(tc.rpc+"/"+w.name, func(t *testing.T) {
				// The handler sees this metadata only in the call's own context.
				ctx := metadata.AppendToOutgoingContext(testContext(t), testCallKey, t.Name())
				var opts []grpc.CallOption
				if w.token != "" {
					opts = append(opts, grpc.PerRPCCredentials(TokenCredentials(w.token, false)))
				}
				err := rpcs[tc.rpc](t, ctx, opts...)
				if got := status.Code(err); got != tc.want[i] {
					t.Fatalf("%s as %s: %v, want code %v", tc.rpc, w.name, err, tc.want[i])
				}
				var want []string
				if tc.want[i] == codes.OK {
					want = []string{tc.rpc + " by " + w.as.Role.String() + " " + w.as.Name + " in " + t.Name()}
				}
				// A rejected call must never reach the handler.
				if got := rec.take(); !slices.Equal(got, want) {
					t.Fatalf("handler calls = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestAuthorizationMetadataOverGRPC(t *testing.T) {
	a, callers := newTestAuthenticator(t)
	lis, rec := startServer(t, a)
	admin := cpv1.NewAdminServiceClient(dialInsecure(t, lis))
	tok := callers[RoleReader].token

	for _, tc := range []struct {
		name   string
		values []string
		want   codes.Code
	}{
		{"canonical", []string{"Bearer " + tok}, codes.OK},
		{"lowercase scheme", []string{"bearer " + tok}, codes.OK},
		{"bare token", []string{tok}, codes.Unauthenticated},
		{"other scheme", []string{"Basic " + tok}, codes.Unauthenticated},
		{"repeated header", []string{"Bearer " + tok, "Bearer " + tok}, codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			for _, v := range tc.values {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", v)
			}
			_, err := admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "svc"})
			if got := status.Code(err); got != tc.want {
				t.Fatalf("GetNamespace = %v, want code %v", err, tc.want)
			}
			if calls := rec.take(); (len(calls) == 1) != (tc.want == codes.OK) {
				t.Fatalf("handler calls = %q", calls)
			}
		})
	}
}

func TestHealthAndReflectionStayPublic(t *testing.T) {
	a, _ := newTestAuthenticator(t)
	lis, _ := startServer(t, a)
	conn := dialInsecure(t, lis)
	healthClient := healthpb.NewHealthClient(conn)

	for name, opts := range map[string][]grpc.CallOption{
		"no token":      nil,
		"unknown token": {grpc.PerRPCCredentials(TokenCredentials("cp_bogus", false))},
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := healthClient.Check(testContext(t), &healthpb.HealthCheckRequest{}, opts...)
			if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				t.Fatalf("Health.Check = %v, %v; want SERVING", resp, err)
			}

			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			watch, err := healthClient.Watch(ctx, &healthpb.HealthCheckRequest{}, opts...)
			if err != nil {
				t.Fatalf("Health.Watch: %v", err)
			}
			if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				t.Fatalf("Health.Watch Recv = %v, %v; want SERVING", resp, err)
			}

			refl, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx, opts...)
			if err != nil {
				t.Fatalf("ServerReflectionInfo: %v", err)
			}
			req := &reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{}}
			if err := refl.Send(req); err != nil {
				t.Fatalf("reflection Send: %v", err)
			}
			got, err := refl.Recv()
			if err != nil {
				t.Fatalf("reflection Recv: %v", err)
			}
			if !slices.ContainsFunc(got.GetListServicesResponse().GetService(), func(s *reflectionv1.ServiceResponse) bool {
				return s.GetName() == cpv1.AdminService_ServiceDesc.ServiceName
			}) {
				t.Fatalf("reflection did not list %s: %v", cpv1.AdminService_ServiceDesc.ServiceName, got)
			}
		})
	}
}

// selfSignedTLS returns credentials for a server holding a throwaway
// certificate for "bufconn" and for a client that trusts only it.
func selfSignedTLS(t *testing.T) (server, client credentials.TransportCredentials) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bufconn"},
		DNSNames:     []string{"bufconn"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	server = credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	client = credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "bufconn"})
	return server, client
}

func TestTokenCredentialsRequireTLS(t *testing.T) {
	a, callers := newTestAuthenticator(t)
	editor := callers[RoleEditor]
	tlsOnly := TokenCredentials(editor.token, true)

	serverCreds, clientCreds := selfSignedTLS(t)
	tlsLis, tlsRec := startServer(t, a, grpc.Creds(serverCreds))
	conn := dial(t, tlsLis, grpc.WithTransportCredentials(clientCreds), grpc.WithPerRPCCredentials(tlsOnly))
	if _, err := cpv1.NewAdminServiceClient(conn).PutFlag(testContext(t), &cpv1.PutFlagRequest{Namespace: "svc", Key: "f"}); err != nil {
		t.Fatalf("PutFlag over TLS: %v", err)
	}
	if got, want := tlsRec.take(), []string{"PutFlag by editor deploy-bot"}; !slices.Equal(got, want) {
		t.Fatalf("handler calls = %q, want %q", got, want)
	}

	// gRPC refuses to set up a plaintext connection that would carry them...
	if _, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(tlsOnly)); err == nil {
		t.Fatal("NewClient accepted TLS-only token credentials on an insecure connection")
	}

	// ...and to attach them to a single call on one.
	plainLis, plainRec := startServer(t, a)
	_, err := cpv1.NewAdminServiceClient(dialInsecure(t, plainLis)).PutFlag(testContext(t),
		&cpv1.PutFlagRequest{Namespace: "svc", Key: "f"}, grpc.PerRPCCredentials(tlsOnly))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("PutFlag over plaintext with TLS-only credentials = %v, want Unauthenticated", err)
	}
	if calls := plainRec.take(); len(calls) != 0 {
		t.Fatalf("handler calls = %q, want none", calls)
	}
}
