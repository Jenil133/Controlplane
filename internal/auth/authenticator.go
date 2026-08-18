package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Jenil133/Controlplane/internal/model"
)

// ErrUnauthenticated is returned by Authenticate for a missing or unknown
// token.
var ErrUnauthenticated = errors.New("auth: missing or invalid token")

const (
	// tokenPrefix marks generated tokens so that secret scanners and people
	// can recognize a leaked one.
	tokenPrefix = "cp_"
	tokenBytes  = 32
	// maxTokenLen bounds how much an unauthenticated caller can make us hash.
	// Generated tokens are 46 characters.
	maxTokenLen = 4096
)

// emptyDigest is the SHA-256 of an empty token, which is what hashing an
// unset shell variable produces. Empty tokens never authenticate, so an entry
// with this hash could never be used.
var emptyDigest = sha256.Sum256(nil)

// Entry is one API token in the tokens file. Only the token's hash is kept;
// the token itself is shown once, when it is generated.
type Entry struct {
	Name        string `json:"name"`
	Role        string `json:"role"`         // reader | editor | admin
	TokenSHA256 string `json:"token_sha256"` // lowercase hex SHA-256 of the token
}

// tokensFile is the layout LoadFile reads.
type tokensFile struct {
	Tokens []Entry `json:"tokens"`
}

// Authenticator maps bearer tokens to principals. It is immutable and safe
// for concurrent use.
type Authenticator struct {
	// byHash is keyed by the SHA-256 of a token, so a lookup only ever
	// compares the digest of what the caller sent. Its timing can at most
	// reveal something about stored digests, and turning a digest back into
	// a token takes a SHA-256 preimage.
	byHash map[[sha256.Size]byte]Principal
}

// New builds an Authenticator from token entries. It requires at least one
// entry, unique non-empty names of at most model.MaxActorLen bytes (the name
// is recorded as the actor of every change), valid roles and unique 64-digit
// hex hashes (either case), none of them the hash of an empty token. Errors
// wrap model.ErrInvalid.
func New(entries []Entry) (*Authenticator, error) {
	if len(entries) == 0 {
		return nil, model.Invalidf("no tokens configured")
	}
	a := &Authenticator{byHash: make(map[[sha256.Size]byte]Principal, len(entries))}
	names := make(map[string]bool, len(entries))
	for i, e := range entries {
		switch {
		case e.Name == "":
			return nil, model.Invalidf("tokens[%d]: name is required", i)
		case len(e.Name) > model.MaxActorLen:
			return nil, model.Invalidf("tokens[%d]: name is longer than %d characters", i, model.MaxActorLen)
		case names[e.Name]:
			return nil, model.Invalidf("tokens[%d]: name %q is used more than once", i, e.Name)
		}
		names[e.Name] = true

		role, err := ParseRole(e.Role)
		if err != nil {
			return nil, model.Invalidf("tokens[%d] (%q): %v", i, e.Name, err)
		}
		digest, err := parseDigest(e.TokenSHA256)
		if err != nil {
			return nil, model.Invalidf("tokens[%d] (%q): token_sha256 %v", i, e.Name, err)
		}
		if other, dup := a.byHash[digest]; dup {
			return nil, model.Invalidf("tokens[%d] (%q): token_sha256 is also used by %q", i, e.Name, other.Name)
		}
		a.byHash[digest] = Principal{Name: e.Name, Role: role}
	}
	return a, nil
}

func parseDigest(s string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(s) != hex.EncodedLen(sha256.Size) {
		return digest, fmt.Errorf("must be %d hex digits, got %d characters", hex.EncodedLen(sha256.Size), len(s))
	}
	if _, err := hex.Decode(digest[:], []byte(s)); err != nil {
		return digest, errors.New("must be hex digits")
	}
	if digest == emptyDigest {
		return digest, errors.New("is the hash of an empty token")
	}
	return digest, nil
}

// LoadFile reads a tokens file, {"tokens": [Entry, ...]}, and calls New.
// Unknown fields and trailing data are rejected: in a security setting, a
// misspelt or newer field must not be silently ignored.
func LoadFile(path string) (*Authenticator, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read tokens file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var f tokensFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("auth: tokens file %s: %w", path, model.Invalidf("%v", err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("auth: tokens file %s: %w", path, model.Invalidf("unexpected data after the tokens object"))
	}
	a, err := New(f.Tokens)
	if err != nil {
		return nil, fmt.Errorf("auth: tokens file %s: %w", path, err)
	}
	return a, nil
}

// GenerateToken returns a new random token: "cp_" followed by 32 random
// bytes in unpadded base64url.
func GenerateToken() (string, error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: generate token: %w", err)
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// HashToken returns the lowercase hex SHA-256 of token, as stored in
// Entry.TokenSHA256. Tokens are random, so a fast unsalted hash is enough.
func HashToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// Authenticate returns the principal that owns token, or ErrUnauthenticated.
func (a *Authenticator) Authenticate(token string) (Principal, error) {
	if token == "" || len(token) > maxTokenLen {
		return Principal{}, ErrUnauthenticated
	}
	p, ok := a.byHash[sha256.Sum256([]byte(token))]
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	return p, nil
}
