package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Jenil133/Controlplane/internal/auth"
)

// generateToken runs "cpctl token generate" and returns the token and the
// tokens-file entry it printed, verbatim.
func generateToken(t *testing.T, name, role string) (token, entry string) {
	t.Helper()
	var out bytes.Buffer
	if err := run(context.Background(), []string{"token", "generate", "--name", name, "--role", role}, &out); err != nil {
		t.Fatalf("token generate: %v", err)
	}
	for _, l := range strings.Split(out.String(), "\n") {
		if v, ok := strings.CutPrefix(l, "token: "); ok {
			token = v
		}
		if v, ok := strings.CutPrefix(l, "entry: "); ok {
			entry = v
		}
	}
	if token == "" || entry == "" || strings.Count(out.String(), token) != 1 {
		t.Fatalf("token generate output:\n%s", out.String())
	}
	return token, entry
}

// loadTokens writes entries, as token generate printed them, into a tokens
// file and loads it the way the server does.
func loadTokens(t *testing.T, entries ...string) *auth.Authenticator {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(`{"tokens": [`+strings.Join(entries, ",")+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := auth.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	return a
}

func TestTokenGenerate(t *testing.T) {
	// The server address is never dialed: tokens are made offline.
	t.Setenv("CPCTL_ADDR", "127.0.0.1:1")
	token, line := generateToken(t, "ci-bot", "editor")
	var entry auth.Entry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("entry %q: %v", line, err)
	}
	if !strings.HasPrefix(token, "cp_") || entry.Name != "ci-bot" || entry.Role != "editor" || entry.TokenSHA256 != auth.HashToken(token) {
		t.Fatalf("token %q, entry %+v", token, entry)
	}
	if other, _ := generateToken(t, "ci-bot", "editor"); other == token {
		t.Fatal("two runs printed the same token")
	}
	a := loadTokens(t, line)
	if p, err := a.Authenticate(token); err != nil || p != (auth.Principal{Name: "ci-bot", Role: auth.RoleEditor}) {
		t.Fatalf("Authenticate = %+v, %v", p, err)
	}

	// Nothing is printed for an entry the server would refuse.
	for _, args := range [][]string{
		{"token"},
		{"token", "revoke"},
		{"token", "generate"},
		{"token", "generate", "--role", "admin"},
		{"token", "generate", "--name", "x"},
		{"token", "generate", "--name", "x", "--role", "root"},
		{"token", "generate", "--name", strings.Repeat("n", 129), "--role", "reader"},
		{"token", "generate", "extra", "--name", "x", "--role", "reader"},
	} {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out); err == nil || out.Len() != 0 {
			t.Errorf("cpctl %v: error %v, output %q", args, err, out.String())
		}
	}
}
