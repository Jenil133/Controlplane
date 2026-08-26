package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

// testServer is an in-process control plane on a loopback port, reached the
// way users reach one: through --addr.
type testServer struct {
	srv  *server.Server
	addr string
}

// startServer serves a fresh server backed by the memory store. With an
// authenticator every call passes the auth interceptors, as in production.
func startServer(t *testing.T, a *auth.Authenticator) *testServer {
	t.Helper()
	srv := server.New(server.Options{Store: memory.New(), Logger: slog.New(slog.DiscardHandler)})
	var opts []grpc.ServerOption
	if a != nil {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(a.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(a.StreamServerInterceptor()))
	}
	g := grpc.NewServer(opts...)
	srv.Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(func() {
		srv.Shutdown()
		g.Stop()
	})
	return &testServer{srv: srv, addr: lis.Addr().String()}
}

// run runs cpctl against the server. args may start with global flags.
func (s *testServer) run(args ...string) (string, error) {
	var out bytes.Buffer
	err := run(context.Background(), append([]string{"--addr", s.addr}, args...), &out)
	return out.String(), err
}

// cpctl runs cpctl and fails the test if it fails.
func (s *testServer) cpctl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := s.run(args...)
	if err != nil {
		t.Fatalf("cpctl %s: %v", strings.Join(args, " "), err)
	}
	return out
}

// fail checks that cpctl fails with gRPC status code want, which is
// codes.Unknown for errors cpctl finds before calling the server, and that
// it printed nothing.
func (s *testServer) fail(t *testing.T, want codes.Code, args ...string) {
	t.Helper()
	out, err := s.run(args...)
	if err == nil || status.Code(err) != want {
		t.Fatalf("cpctl %s: error %v, want %s", strings.Join(args, " "), err, want)
	}
	if out != "" {
		t.Fatalf("cpctl %s failed (%v) but printed:\n%s", strings.Join(args, " "), err, out)
	}
}

// decode parses JSON printed by cpctl into m.
func decode[M proto.Message](t *testing.T, out string, m M) M {
	t.Helper()
	if err := protojson.Unmarshal([]byte(out), m); err != nil {
		t.Fatalf("decode %T: %v\n%s", m, err, out)
	}
	return m
}

// lines splits output into lines with runs of spaces collapsed, so that the
// tabwriter-aligned "state    active" reads "state active".
func lines(out string) []string {
	ls := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	for i, l := range ls {
		ls[i] = strings.Join(strings.Fields(l), " ")
	}
	return ls
}

// wantLines checks that each of want is a whole line of output, padding
// collapsed.
func wantLines(t *testing.T, out string, want ...string) {
	t.Helper()
	got := lines(out)
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Fatalf("output lacks the line %q:\n%s", w, out)
		}
	}
}

// wantMatch checks that a whole line of output, padding collapsed, matches
// the regular expression pattern.
func wantMatch(t *testing.T, out, pattern string) {
	t.Helper()
	re := regexp.MustCompile("^" + pattern + "$")
	if !slices.ContainsFunc(lines(out), re.MatchString) {
		t.Fatalf("no line of output matches %q:\n%s", pattern, out)
	}
}

// waitFor polls cond until it holds, failing the test after five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// syncBuffer lets a test read what a command running in another goroutine
// has written so far.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

func TestActorHeaderMatchesServer(t *testing.T) {
	if actorHeader != server.ActorHeader {
		t.Fatalf("actorHeader = %q, server expects %q", actorHeader, server.ActorHeader)
	}
}

func TestUsageListsEveryCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"--help"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out); err != nil || out.String() != usage {
			t.Fatalf("cpctl %v: error %v, output:\n%s", args, err, out.String())
		}
	}
	var described []string
	for _, l := range strings.Split(usage, "\n") {
		if strings.HasPrefix(l, "  ") {
			described = append(described, strings.TrimSpace(l))
		}
	}
	for _, cmd := range []string{
		"ns create", "ns get", "ns list", "config put", "config delete", "flag put", "flag delete",
		"experiment put", "experiment delete", "ratelimit put", "ratelimit delete", "breaker put",
		"breaker delete", "rollout start", "rollout advance|pause|resume|abort", "rollout status",
		"history", "revision", "diff", "rollback", "audit", "snapshot", "watch", "eval", "token generate",
		"--token string",
	} {
		if !slices.ContainsFunc(described, func(l string) bool { return l == cmd || strings.HasPrefix(l, cmd+" ") }) {
			t.Errorf("usage does not describe %q", cmd)
		}
	}
	if !strings.Contains(usage, "(env CPCTL_TOKEN)") {
		t.Error("usage does not mention CPCTL_TOKEN")
	}
	if err := run(context.Background(), []string{"frobnicate"}, io.Discard); err == nil {
		t.Error("unknown command accepted")
	}
}

func TestParseArgsInterspersed(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	desc := fs.String("description", "", "")
	enabled := fs.Bool("enabled", false, "")

	pos, err := parseArgs(fs, []string{"ns", "--enabled", "key", "--description", "hi there"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(pos, "|") != "ns|key" || !*enabled || *desc != "hi there" {
		t.Fatalf("pos=%v enabled=%v desc=%q", pos, *enabled, *desc)
	}

	pos, err = parseArgs(fs, []string{"ns", "key", "--", "-5"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if pos[2] != "-5" {
		t.Fatalf("value after -- = %q", pos[2])
	}

	if _, err := parseArgs(fs, []string{"only-one"}, 2); err == nil {
		t.Fatal("expected argument count error")
	}
}

func TestParseArgsRange(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "")
	for _, tc := range []struct {
		args []string
		want string // positional arguments joined by |, or "error"
	}{
		{[]string{"ns"}, "error"},
		{[]string{"ns", "3"}, "ns|3"},
		{[]string{"ns", "--json", "3", "7"}, "ns|3|7"},
		{[]string{"ns", "3", "7", "9"}, "error"},
	} {
		pos, err := parseArgsRange(fs, tc.args, 2, 3)
		got := strings.Join(pos, "|")
		if err != nil {
			got = "error"
		}
		if got != tc.want {
			t.Errorf("parseArgsRange(%q) = %q (error %v), want %q", tc.args, got, err, tc.want)
		}
	}
	if !*asJSON {
		t.Error("--json between positional arguments was not parsed")
	}
}

func TestParseValue(t *testing.T) {
	tests := map[string]*structpb.Value{
		`42`:        structpb.NewNumberValue(42),
		`true`:      structpb.NewBoolValue(true),
		`"quoted"`:  structpb.NewStringValue("quoted"),
		`plain`:     structpb.NewStringValue("plain"),
		`{"a":[1]}`: nil, // checked separately
	}
	for in, want := range tests {
		got, err := parseValue(in)
		if err != nil {
			t.Fatalf("parseValue(%q): %v", in, err)
		}
		if want == nil {
			if got.GetStructValue().GetFields()["a"].GetListValue().GetValues()[0].GetNumberValue() != 1 {
				t.Fatalf("parseValue(%q) = %v", in, got)
			}
			continue
		}
		if got.String() != want.String() {
			t.Fatalf("parseValue(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestParseVariants(t *testing.T) {
	vs, err := parseVariants("control=70, green=30", keyValues{"green": `{"color":"green"}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 2 || vs[0].GetName() != "control" || vs[0].GetWeight() != 70 || vs[0].GetPayload() != nil ||
		vs[1].GetPayload().GetStructValue().GetFields()["color"].GetStringValue() != "green" {
		t.Fatalf("variants = %v", vs)
	}
	for _, bad := range []string{"", "a", "a=x", "=5"} {
		if _, err := parseVariants(bad, nil); err == nil {
			t.Errorf("parseVariants(%q) succeeded", bad)
		}
	}
	if _, err := parseVariants("a=1", keyValues{"b": "1"}); err == nil {
		t.Error("payload for unknown variant accepted")
	}
}

func TestStringList(t *testing.T) {
	var l stringList
	for _, s := range []string{"u1, u2", "", "u2,u3,,", " u1 "} {
		if err := l.Set(s); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(l, []string{"u1", "u2", "u3"}) {
		t.Fatalf("list = %q, want [u1 u2 u3]", l)
	}
}

func TestPercent(t *testing.T) {
	for in, want := range map[string]float64{"25": 25, " 0.5 ": 0.5, "25%": 25, "100%": 100, "0": 0} {
		if got, err := parsePercent(in); err != nil || got != want {
			t.Errorf("parsePercent(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "%", "half", "5%%"} {
		if _, err := parsePercent(bad); err == nil {
			t.Errorf("parsePercent(%q) succeeded", bad)
		}
	}
	for p, want := range map[float64]string{0: "0", 1: "1", 0.5: "0.5", 0.1 + 0.2: "0.3", 99.99: "99.99", 100: "100"} {
		if got := formatPercent(p); got != want {
			t.Errorf("formatPercent(%v) = %q, want %q", p, got, want)
		}
	}
}

// TestCommandsAgainstServer drives the Phase 1 commands end to end.
func TestCommandsAgainstServer(t *testing.T) {
	s := startServer(t, nil)
	cpctl := func(args ...string) string {
		t.Helper()
		return s.cpctl(t, append([]string{"--actor", "tester"}, args...)...)
	}

	cpctl("ns", "create", "shop/prod", "--description", "shop")
	cpctl("config", "put", "shop/prod", "retry.max", "3")
	cpctl("flag", "put", "shop/prod", "dark-mode", "--enabled")
	cpctl("experiment", "put", "shop/prod", "cta", "--variants", "control=50,bold=50", "--payload", `bold={"weight":700}`, "--enabled")

	if out := cpctl("ns", "list"); !strings.Contains(out, "shop/prod") || !strings.Contains(out, "4") {
		t.Fatalf("ns list output:\n%s", out)
	}
	snap := cpctl("snapshot", "shop/prod")
	for _, want := range []string{`"revision": "4"`, `"retry.max"`, `"dark-mode"`, `"updatedBy": "tester"`, `"weight": 700`} {
		if !strings.Contains(snap, want) {
			t.Fatalf("snapshot missing %s:\n%s", want, snap)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var watchOut syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--addr", s.addr, "watch", "shop/prod"}, &watchOut)
	}()
	waitFor(t, "the initial snapshot", func() bool { return strings.Contains(watchOut.String(), "revision=4 ") })
	if out := cpctl("config", "delete", "shop/prod", "retry.max"); out != "deleted config retry.max (namespace shop/prod now at revision 5)\n" {
		t.Fatalf("delete output: %s", out)
	}
	waitFor(t, "the pushed change", func() bool { return strings.Contains(watchOut.String(), "revision=5 configs=0") })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch: %v", err)
	}
}

func TestFlagPut(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop") // revision 1

	f := decode(t, s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "25%", "--salt", "cart-v1",
		"--allow", "vip-1, vip-2", "--allow", "vip-1,vip-3", "--description", "new cart"), &cpv1.Flag{})
	if !f.GetEnabled() || f.GetRolloutPercent() != 25 || f.GetSalt() != "cart-v1" || f.GetDescription() != "new cart" ||
		!slices.Equal(f.GetAllowlist(), []string{"vip-1", "vip-2", "vip-3"}) || f.GetRevision() != 2 {
		t.Fatalf("flag after the first put: %v", f)
	}

	// Without --rollout and --salt the server keeps both; the enabled state,
	// description and allowlist are replaced.
	f = decode(t, s.cpctl(t, "flag", "put", "shop", "new-cart", "--expected-revision", "2"), &cpv1.Flag{})
	if f.GetEnabled() || f.GetRolloutPercent() != 25 || f.GetSalt() != "cart-v1" || f.GetDescription() != "" ||
		len(f.GetAllowlist()) != 0 || f.GetRevision() != 3 {
		t.Fatalf("flag after the second put: %v", f)
	}
	// --rollout 0 is sent, not mistaken for an absent --rollout.
	if f = decode(t, s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "0"), &cpv1.Flag{}); f.GetRolloutPercent() != 0 {
		t.Fatalf("flag after --rollout 0: %v", f)
	}
	if f = decode(t, s.cpctl(t, "flag", "put", "shop", "dark-mode", "--enabled"), &cpv1.Flag{}); f.GetRolloutPercent() != 100 || f.GetSalt() != "dark-mode" {
		t.Fatalf("new flag without --rollout and --salt: %v", f)
	}

	s.fail(t, codes.Aborted, "flag", "put", "shop", "new-cart", "--expected-revision", "2")
	s.fail(t, codes.Aborted, "flag", "delete", "shop", "new-cart", "--expected-revision", "2")
	s.fail(t, codes.InvalidArgument, "flag", "put", "shop", "new-cart", "--rollout", "100.5")
	s.fail(t, codes.Unknown, "flag", "put", "shop", "new-cart", "--rollout", "most")
	if out := s.cpctl(t, "flag", "delete", "shop", "new-cart", "--expected-revision", "4"); out != "deleted flag new-cart (namespace shop now at revision 6)\n" {
		t.Fatalf("delete output: %q", out)
	}

	// Every put and delete takes --expected-revision.
	s.cpctl(t, "config", "put", "shop", "retry", "3") // revision 7
	s.fail(t, codes.Aborted, "config", "put", "shop", "retry", "4", "--expected-revision", "2")
	if c := decode(t, s.cpctl(t, "config", "put", "shop", "retry", "4", "--expected-revision", "7"), &cpv1.Config{}); c.GetRevision() != 8 {
		t.Fatalf("config after a matching --expected-revision: %v", c)
	}
	s.fail(t, codes.Aborted, "config", "delete", "shop", "retry", "--expected-revision", "7")
	s.fail(t, codes.Aborted, "experiment", "put", "shop", "cta", "--variants", "a=1", "--expected-revision", "1")
	s.cpctl(t, "experiment", "put", "shop", "cta", "--variants", "a=1") // revision 9
	s.fail(t, codes.Aborted, "experiment", "delete", "shop", "cta", "--expected-revision", "8")
	s.cpctl(t, "experiment", "delete", "shop", "cta", "--expected-revision", "9")
}
