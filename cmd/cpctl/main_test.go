package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func TestActorHeaderMatchesServer(t *testing.T) {
	if actorHeader != server.ActorHeader {
		t.Fatalf("actorHeader = %q, server expects %q", actorHeader, server.ActorHeader)
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

// TestCommandsAgainstServer drives the CLI end to end against a real server.
func TestCommandsAgainstServer(t *testing.T) {
	srv := server.New(server.Options{Store: memory.New(), Logger: slog.New(slog.DiscardHandler)})
	g := grpc.NewServer()
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

	cpctl := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		all := append([]string{"--addr", lis.Addr().String(), "--actor", "tester"}, args...)
		if err := run(context.Background(), all, &out); err != nil {
			t.Fatalf("cpctl %v: %v", args, err)
		}
		return out.String()
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var watchOut bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--addr", lis.Addr().String(), "watch", "shop/prod"}, &watchOut)
	}()
	deadline := time.Now().Add(time.Second)
	for srv.Watchers() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if out := cpctl("config", "delete", "shop/prod", "retry.max"); !strings.Contains(out, "revision 5") {
		t.Fatalf("delete output: %s", out)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch: %v", err)
	}
	if out := watchOut.String(); !strings.Contains(out, "revision=4") || !strings.Contains(out, "revision=5 configs=0") {
		t.Fatalf("watch output:\n%s", out)
	}
}
