package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

// TestAuthEnabled runs cpctl against a server that enforces tokens made by
// "cpctl token generate", the way an operator would set one up.
func TestAuthEnabled(t *testing.T) {
	t.Setenv("CPCTL_TOKEN", "")
	tokens := make(map[string]string)
	var entries []string
	for _, role := range []string{"reader", "editor", "admin"} {
		token, entry := generateToken(t, role+"-bot", role)
		tokens[role] = token
		entries = append(entries, entry)
	}
	s := startServer(t, loadTokens(t, entries...))
	// as runs a command with a role's token; --actor is ignored once a
	// token identifies the caller.
	as := func(role string, args ...string) []string {
		return append([]string{"--token", tokens[role], "--actor", "mallory"}, args...)
	}

	s.fail(t, codes.Unauthenticated, "ns", "list")
	s.fail(t, codes.Unauthenticated, "--token", "cp_not-a-real-token", "ns", "list")
	s.fail(t, codes.PermissionDenied, as("editor", "ns", "create", "shop")...)
	if ns := decode(t, s.cpctl(t, as("admin", "ns", "create", "shop")...), &cpv1.Namespace{}); ns.GetCreatedBy() != "admin-bot" {
		t.Fatalf("namespace created by %q, want admin-bot", ns.GetCreatedBy())
	}

	// CPCTL_TOKEN stands in for --token.
	t.Setenv("CPCTL_TOKEN", tokens["editor"])
	editor := [][]string{
		{"flag", "put", "shop", "new-cart", "--enabled", "--rollout", "0"},
		{"ratelimit", "put", "shop", "checkout", "--rps", "5", "--burst", "10", "--enabled"},
		{"breaker", "put", "shop", "payments", "--failure-rate", "0.5", "--min-requests", "10", "--window", "10s",
			"--open-duration", "5s", "--half-open-requests", "1", "--enabled"},
		{"rollout", "start", "shop", "new-cart", "--stages", "10:1h,50,100"},
		{"rollout", "advance", "shop", "new-cart"},
		{"rollout", "pause", "shop", "new-cart"},
		{"rollout", "resume", "shop", "new-cart"},
		{"rollout", "abort", "shop", "new-cart"},
		{"ratelimit", "delete", "shop", "checkout"},
		{"rollback", "shop", "2", "--yes"},
	}
	for _, args := range editor {
		s.cpctl(t, append([]string{"--actor", "mallory"}, args...)...)
	}
	if f := decode(t, s.cpctl(t, "flag", "put", "shop", "dark-mode"), &cpv1.Flag{}); f.GetUpdatedBy() != "editor-bot" {
		t.Fatalf("flag updated by %q, want editor-bot", f.GetUpdatedBy())
	}

	// --token wins over CPCTL_TOKEN. Readers read everything, rollback
	// previews included, and change nothing.
	for _, args := range [][]string{
		{"ns", "list"}, {"ns", "get", "shop"}, {"snapshot", "shop"}, {"history", "shop"}, {"revision", "shop", "2"},
		{"diff", "shop", "2"}, {"rollback", "shop", "4"}, {"audit", "shop"}, {"eval", "shop", "user-1"},
		{"rollout", "status", "shop", "new-cart"},
	} {
		s.cpctl(t, as("reader", args...)...)
	}
	for _, args := range append(editor, []string{"config", "put", "shop", "k", "1"}, []string{"ns", "create", "other"}) {
		s.fail(t, codes.PermissionDenied, as("reader", args...)...)
	}

	// Every change is attributed to a token's name.
	out := s.cpctl(t, "audit", "shop")
	if strings.Contains(out, "mallory") || !strings.Contains(out, "admin-bot") || !strings.Contains(out, "editor-bot") {
		t.Fatalf("audit log:\n%s", out)
	}

	// Watch streams carry the token too.
	t.Setenv("CPCTL_TOKEN", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := run(ctx, []string{"--addr", s.addr, "watch", "shop"}, io.Discard); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("watch without a token: %v", err)
	}
	var watchOut syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--addr", s.addr, "--token", tokens["reader"], "watch", "shop"}, &watchOut)
	}()
	waitFor(t, "the reader's snapshot", func() bool { return strings.Contains(watchOut.String(), " shop revision=") })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("watch with a reader token: %v", err)
	}
}

// TestTokenIsNeverEchoed checks that a token reaches the server and nothing
// else: it is not in any output or error, whether the call succeeds or not,
// and stray whitespace around it, as `$(cat file)` leaves, is ignored.
func TestTokenIsNeverEchoed(t *testing.T) {
	t.Setenv("CPCTL_TOKEN", "")
	good, entry := generateToken(t, "ops", "admin")
	s := startServer(t, loadTokens(t, entry))
	bad := "cp_wrong-token-value"

	for _, args := range [][]string{
		{"ns", "list"}, {"ns", "create", "shop"}, {"flag", "put", "shop", "f", "--rollout", "NaN"},
		{"ns", "get", "missing"}, {"audit", "--since", "yesterday"}, {"nosuch"}, {"flag", "put"},
	} {
		for _, token := range []string{good, bad} {
			out, err := s.run(append([]string{"--token", token}, args...)...)
			text := out
			if err != nil {
				text += errorText(err) + err.Error()
			}
			if strings.Contains(text, token) || strings.Contains(text, auth.HashToken(token)) {
				t.Errorf("cpctl --token ... %v leaked the token:\n%s", args, text)
			}
		}
	}
	out := s.cpctl(t, "--token", " "+good+"\n", "ns", "get", "shop")
	if !strings.Contains(out, `"name": "shop"`) {
		t.Fatalf("padded token rejected:\n%s", out)
	}
	t.Setenv("CPCTL_TOKEN", good+"\r\n")
	s.cpctl(t, "ns", "get", "shop")
}
