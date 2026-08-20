package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func TestParseConfig(t *testing.T) {
	t.Setenv("CHECKOUT_TOKEN", "")
	got, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := config{ControlPlane: "localhost:9090", Namespace: "checkout/dev", Listen: ":8090"}
	if got != want {
		t.Fatalf("defaults = %+v, want %+v", got, want)
	}

	got, err = parseConfig([]string{
		"--controlplane", "cp:9090", "--namespace", "checkout/prod", "--listen", "127.0.0.1:0",
		"--cache-path", "/var/cache/checkout.json", "--token", "secret",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want = config{ControlPlane: "cp:9090", Namespace: "checkout/prod", Listen: "127.0.0.1:0", CachePath: "/var/cache/checkout.json", Token: "secret"}
	if got != want {
		t.Fatalf("parsed %+v, want %+v", got, want)
	}

	// The environment supplies the token unless the flag does, and usage
	// never shows it.
	t.Setenv("CHECKOUT_TOKEN", "from-env")
	if got, err := parseConfig(nil, io.Discard); err != nil || got.Token != "from-env" {
		t.Fatalf("token from the environment: %+v, %v", got, err)
	}
	if got, err := parseConfig([]string{"--token", "flag"}, io.Discard); err != nil || got.Token != "flag" {
		t.Fatalf("--token with the environment set: %+v, %v", got, err)
	}
	var usage strings.Builder
	if _, err := parseConfig([]string{"-h"}, &usage); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h: %v, want flag.ErrHelp", err)
	}
	if !strings.Contains(usage.String(), "-cache-path") || strings.Contains(usage.String(), "from-env") {
		t.Fatalf("usage lacks the flags or shows the token:\n%s", usage.String())
	}

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"--controlplane", ""}, "--controlplane"},
		{[]string{"--unknown"}, "-unknown"},
		{[]string{"extra"}, `unexpected argument "extra"`},
	} {
		if _, err := parseConfig(tt.args, io.Discard); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("parseConfig(%q) = %v, want an error mentioning %q", tt.args, err, tt.want)
		}
	}
}

// Tokens of the control plane in TestRun.
const (
	adminToken  = "admin-token"
	readerToken = "reader-token"
)

// TestRun runs the service as main does, against a control plane with auth
// enabled on real TCP connections, in real time.
func TestRun(t *testing.T) {
	addr, admin := startControlPlane(t)
	ctx := context.Background()
	if _, err := admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: testNamespace}); err != nil {
		t.Fatal(err)
	}
	flagResp, err := admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: testNamespace, Key: newCartFlag})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("live", func(t *testing.T) {
		cachePath := filepath.Join(t.TempDir(), "snapshot.json")
		svc := startService(t, config{ControlPlane: addr, Namespace: testNamespace, Token: readerToken, CachePath: cachePath})
		svc.waitStatus(t, func(s statusJSON) bool {
			return s.State == "live" && s.Source == "server" && s.Revision == flagResp.GetFlag().GetRevision()
		})
		if got := svc.checkout(t, "alice"); got.Cart != "v1" || got.Payment != paymentOK {
			t.Fatalf("checkout = %+v, want cart v1 paid", got)
		}

		// The hot-reload promise: a write shows in responses within 1s.
		start := time.Now()
		if _, err := admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: testNamespace, Key: newCartFlag, Enabled: true}); err != nil {
			t.Fatal(err)
		}
		for svc.checkout(t, "alice").Cart != "v2" {
			if time.Since(start) >= time.Second {
				t.Fatal("the flag flip did not show within 1s")
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Logf("flag flip visible after %v", time.Since(start))

		if _, err := os.Stat(cachePath); err != nil {
			t.Fatalf("snapshot cache: %v", err)
		}
		if err := svc.stop(); err != nil {
			t.Fatalf("run = %v after the context was canceled, want nil", err)
		}
		if resp, err := http.Get(svc.url + "/status"); err == nil {
			resp.Body.Close()
			t.Fatal("still serving after run returned")
		}
	})

	t.Run("without a token", func(t *testing.T) {
		svc := startService(t, config{ControlPlane: addr, Namespace: testNamespace})
		s := svc.waitStatus(t, func(s statusJSON) bool { return s.State == "disconnected" })
		if s.Source != "none" || !strings.Contains(s.LastError, "Unauthenticated") {
			t.Fatalf("status = %+v, want no snapshot and an Unauthenticated error", s)
		}
		// Until the control plane answers, the service runs on defaults.
		want := checkoutResponse{User: "alice", Cart: "v1", Payment: paymentOK}
		if got := svc.checkout(t, "alice"); !reflect.DeepEqual(got, want) {
			t.Fatalf("checkout = %+v, want %+v", got, want)
		}
		if err := svc.stop(); err != nil {
			t.Fatalf("run = %v, want nil", err)
		}
	})
}

// startControlPlane serves an in-process control plane with auth enabled on
// a loopback TCP port, and returns its address and an admin client.
func startControlPlane(t *testing.T) (string, cpv1.AdminServiceClient) {
	t.Helper()
	authn, err := auth.New([]auth.Entry{
		{Name: "admin", Role: "admin", TokenSHA256: auth.HashToken(adminToken)},
		{Name: "checkout", Role: "reader", TokenSHA256: auth.HashToken(readerToken)},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Options{Store: memory.New(), Logger: discard})
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authn.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authn.StreamServerInterceptor()),
	)
	srv.Register(gs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = srv.Run(ctx)
	}()
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(auth.TokenCredentials(adminToken, false)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Shutdown()
		gs.Stop()
		cancel()
		<-stopped
	})
	return lis.Addr().String(), cpv1.NewAdminServiceClient(conn)
}

// runningService is run in progress on a loopback port.
type runningService struct {
	url  string
	http *http.Client
	stop func() error // cancels run's context and returns its result
}

func startService(t *testing.T, cfg config) *runningService {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, lis, discard) }()
	svc := &runningService{
		url:  "http://" + lis.Addr().String(),
		http: &http.Client{Timeout: 5 * time.Second},
	}
	var result error
	stopped := false
	svc.stop = func() error {
		if !stopped {
			stopped = true
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("run did not return within 10s of its context being canceled")
			}
			svc.http.CloseIdleConnections()
		}
		return result
	}
	t.Cleanup(func() { _ = svc.stop() })
	return svc
}

// statusJSON is the /status body.
type statusJSON struct {
	State      string    `json:"state"`
	Source     string    `json:"source"`
	Revision   int64     `json:"revision"`
	LastUpdate time.Time `json:"last_update"`
	LastError  string    `json:"last_error"`
}

// waitStatus polls /status until ok accepts it, for up to 5s.
func (s *runningService) waitStatus(t *testing.T, ok func(statusJSON) bool) statusJSON {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var st statusJSON
		s.getJSON(t, "/status", http.StatusOK, &st)
		if ok(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("status still %+v after 5s", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *runningService) checkout(t *testing.T, user string) checkoutResponse {
	t.Helper()
	var resp checkoutResponse
	s.getJSON(t, "/checkout?user="+user, http.StatusOK, &resp)
	return resp
}

func (s *runningService) getJSON(t *testing.T, path string, wantCode int, out any) {
	t.Helper()
	resp, err := s.http.Get(s.url + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantCode {
		t.Fatalf("GET %s: %d %q, want status %d", path, resp.StatusCode, body, wantCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("GET %s: %q: %v", path, body, err)
	}
}
