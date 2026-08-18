package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Jenil133/Controlplane/internal/model"
)

func mustToken(t *testing.T) string {
	t.Helper()
	tok, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	return tok
}

func mustNew(t *testing.T, entries ...Entry) *Authenticator {
	t.Helper()
	a, err := New(entries)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func TestHashToken(t *testing.T) {
	for _, tc := range []struct{ token, want string }{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	} {
		if got := HashToken(tc.token); got != tc.want {
			t.Errorf("HashToken(%q) = %s, want %s", tc.token, got, tc.want)
		}
	}
}

func TestGenerateToken(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		tok := mustToken(t)
		payload, ok := strings.CutPrefix(tok, "cp_")
		if !ok {
			t.Fatalf("token %q lacks the cp_ prefix", tok)
		}
		if b, err := base64.RawURLEncoding.DecodeString(payload); err != nil || len(b) != 32 {
			t.Fatalf("token %q: payload is %d bytes (%v), want 32 bytes of unpadded base64url", tok, len(b), err)
		}
		if seen[tok] {
			t.Fatalf("token %q generated twice", tok)
		}
		seen[tok] = true
	}
}

func TestNewRejectsInvalidEntries(t *testing.T) {
	valid := Entry{Name: "deploy-bot", Role: "editor", TokenSHA256: HashToken("t1")}
	with := func(change func(*Entry)) Entry {
		e := valid
		change(&e)
		return e
	}
	for _, tc := range []struct {
		name    string
		entries []Entry
		wantMsg string
	}{
		{"no entries", nil, "no tokens configured"},
		{"empty name", []Entry{with(func(e *Entry) { e.Name = "" })}, "tokens[0]: name is required"},
		{"long name", []Entry{with(func(e *Entry) { e.Name = strings.Repeat("n", model.MaxActorLen+1) })}, "longer than 128"},
		{"duplicate name", []Entry{valid, with(func(e *Entry) { e.TokenSHA256 = HashToken("t2") })}, `tokens[1]: name "deploy-bot" is used more than once`},
		{"empty role", []Entry{with(func(e *Entry) { e.Role = "" })}, "unknown role"},
		{"unknown role", []Entry{with(func(e *Entry) { e.Role = "root" })}, `unknown role "root"`},
		{"roles are lowercase", []Entry{with(func(e *Entry) { e.Role = "Admin" })}, "unknown role"},
		{"missing hash", []Entry{with(func(e *Entry) { e.TokenSHA256 = "" })}, "must be 64 hex digits, got 0"},
		{"short hash", []Entry{with(func(e *Entry) { e.TokenSHA256 = e.TokenSHA256[1:] })}, "got 63"},
		{"long hash", []Entry{with(func(e *Entry) { e.TokenSHA256 += "0" })}, "got 65"},
		{"not hex", []Entry{with(func(e *Entry) { e.TokenSHA256 = "g" + e.TokenSHA256[1:] })}, "must be hex digits"},
		{"token instead of its hash", []Entry{with(func(e *Entry) { e.TokenSHA256 = "t1" })}, "must be 64 hex digits"},
		{"hash of an empty token", []Entry{with(func(e *Entry) { e.TokenSHA256 = HashToken("") })}, "hash of an empty token"},
		{"duplicate hash", []Entry{valid, with(func(e *Entry) { e.Name = "ops" })}, `tokens[1] ("ops"): token_sha256 is also used by "deploy-bot"`},
		{
			"duplicate hash in other case",
			[]Entry{valid, with(func(e *Entry) { e.Name, e.TokenSHA256 = "ops", strings.ToUpper(e.TokenSHA256) })},
			"also used by",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := New(tc.entries)
			if !errors.Is(err, model.ErrInvalid) || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("New = %v, %v; want an invalid-argument error containing %q", a, err, tc.wantMsg)
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	readerTok, adminTok := mustToken(t), mustToken(t)
	// A registered token that is too long to ever be accepted.
	oversized := strings.Repeat("x", maxTokenLen+1)
	longest := strings.Repeat("y", maxTokenLen)
	a := mustNew(t,
		Entry{Name: "dashboard", Role: "reader", TokenSHA256: HashToken(readerTok)},
		// Upper-case hex digests are accepted too.
		Entry{Name: "ops", Role: "admin", TokenSHA256: strings.ToUpper(HashToken(adminTok))},
		Entry{Name: "oversized", Role: "admin", TokenSHA256: HashToken(oversized)},
		Entry{Name: "longest", Role: "reader", TokenSHA256: HashToken(longest)},
	)

	for _, tc := range []struct {
		name  string
		token string
		want  Principal
	}{
		{"reader", readerTok, Principal{Name: "dashboard", Role: RoleReader}},
		{"admin", adminTok, Principal{Name: "ops", Role: RoleAdmin}},
		{"longest accepted length", longest, Principal{Name: "longest", Role: RoleReader}},
	} {
		got, err := a.Authenticate(tc.token)
		if err != nil || got != tc.want {
			t.Errorf("%s: Authenticate = %+v, %v; want %+v", tc.name, got, err, tc.want)
		}
	}

	for _, tc := range []struct{ name, token string }{
		{"empty", ""},
		{"unknown", mustToken(t)},
		{"case matters", strings.ToUpper(readerTok)},
		{"prefix of a token", readerTok[:len(readerTok)-1]},
		{"trailing space", readerTok + " "},
		{"the stored hash itself", HashToken(readerTok)},
		{"over the length limit", oversized},
	} {
		if got, err := a.Authenticate(tc.token); !errors.Is(err, ErrUnauthenticated) || got != (Principal{}) {
			t.Errorf("%s: Authenticate = %+v, %v; want ErrUnauthenticated", tc.name, got, err)
		}
	}
}

// TestAuthenticateRejectsEmptyToken does not rely on New refusing the hash
// of an empty token.
func TestAuthenticateRejectsEmptyToken(t *testing.T) {
	a := &Authenticator{byHash: map[[sha256.Size]byte]Principal{emptyDigest: {Name: "nobody", Role: RoleAdmin}}}
	if p, err := a.Authenticate(""); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf(`Authenticate("") = %+v, %v; want ErrUnauthenticated`, p, err)
	}
}

func writeTokensFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFile(t *testing.T) {
	editorTok, readerTok := mustToken(t), mustToken(t)
	path := writeTokensFile(t, `{
  "tokens": [
    {"name": "deploy-bot", "role": "editor", "token_sha256": "`+HashToken(editorTok)+`"},
    {"name": "dashboard", "role": "reader", "token_sha256": "`+HashToken(readerTok)+`"}
  ]
}
`)
	a, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	for tok, want := range map[string]Principal{
		editorTok: {Name: "deploy-bot", Role: RoleEditor},
		readerTok: {Name: "dashboard", Role: RoleReader},
	} {
		if got, err := a.Authenticate(tok); err != nil || got != want {
			t.Errorf("Authenticate = %+v, %v; want %+v", got, err, want)
		}
	}
}

func TestLoadFileErrors(t *testing.T) {
	_, err := LoadFile(filepath.Join(t.TempDir(), "missing.json"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("LoadFile(missing) = %v, want fs.ErrNotExist", err)
	}

	entry := `{"name": "deploy-bot", "role": "editor", "token_sha256": "` + HashToken("t") + `"}`
	for _, tc := range []struct{ name, content, wantMsg string }{
		{"empty file", ``, "EOF"},
		{"not JSON", `tokens: []`, "invalid character"},
		{"wrong shape", `{"tokens": {}}`, "cannot unmarshal"},
		{"unknown top-level field", `{"tokens": [` + entry + `], "version": 2}`, `unknown field "version"`},
		{"plaintext token instead of hash", `{"tokens": [{"name": "deploy-bot", "role": "editor", "token": "cp_x"}]}`, `unknown field "token"`},
		{"trailing data", `{"tokens": [` + entry + `]} {"tokens": []}`, "unexpected data after the tokens object"},
		{"no tokens key", `{}`, "no tokens configured"},
		{"no tokens", `{"tokens": []}`, "no tokens configured"},
		{"invalid entry", `{"tokens": [{"name": "deploy-bot", "role": "root", "token_sha256": "` + HashToken("t") + `"}]}`, `unknown role "root"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTokensFile(t, tc.content)
			a, err := LoadFile(path)
			if !errors.Is(err, model.ErrInvalid) || !strings.Contains(err.Error(), tc.wantMsg) || !strings.Contains(err.Error(), path) {
				t.Fatalf("LoadFile = %v, %v; want an invalid-argument error naming the file and containing %q", a, err, tc.wantMsg)
			}
		})
	}
}
