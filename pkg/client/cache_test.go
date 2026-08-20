package client

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// readCache parses the cache file at path.
func readCache(t *testing.T, path string) *cpv1.Snapshot {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	s := &cpv1.Snapshot{}
	if err := protojson.Unmarshal(b, s); err != nil {
		t.Fatalf("parse cache: %v", err)
	}
	return s
}

func TestCacheRoundTripAcrossInstances(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		opts := e.options()
		opts.CachePath = filepath.Join(t.TempDir(), "state", "checkout.json")
		want := snap(3,
			config(t, "greeting", "hello"),
			flag("new-cart", true, 50, "alice"),
			&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
				{Name: "blue", Weight: 1}, {Name: "green", Weight: 1, Payload: structpb.NewStringValue("#0f0")},
			}},
			rateLimit("checkout", 10, 20),
			circuitBreaker("payments", 0.5, 10, 30*time.Second),
		)
		first, _ := e.connect(opts, want)
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}

		// The control plane is unreachable when the second instance starts.
		e.down()
		second := e.start(opts)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if err := second.WaitReady(ctx); err != nil {
			t.Fatalf("WaitReady with a cache: %v", err)
		}
		if st := second.Status(); st.Source != SourceCache || st.Revision != 3 {
			t.Fatalf("Status() = %+v, want revision 3 from the cache", st)
		}
		if !proto.Equal(second.Snapshot(), want) {
			t.Fatalf("snapshot from cache = %v, want %v", second.Snapshot(), want)
		}
		for i := range 200 {
			u := fmt.Sprint("user-", i)
			if first.IsEnabled("new-cart", u) != second.IsEnabled("new-cart", u) {
				t.Fatalf("new-cart for %s differs between the instances", u)
			}
			a1, _ := first.Variant("cta-color", u)
			a2, _ := second.Variant("cta-color", u)
			if a1.Variant != a2.Variant {
				t.Fatalf("cta-color for %s: %q vs %q", u, a1.Variant, a2.Variant)
			}
		}
		if _, ok := second.Breaker("payments"); !ok {
			t.Fatal("circuit breaker from the cache not configured")
		}
		if l, ok := second.limits.Limiter("checkout"); !ok {
			t.Fatal("rate limit from the cache not configured")
		} else if rate, burst := l.Limit(); rate != 10 || burst != 20 {
			t.Fatalf("rate limit from the cache = %v/s burst %d, want 10/s burst 20", rate, burst)
		}

		synctest.Wait()
		if st := wantStatus(t, second, StateDisconnected, SourceCache, 3); status.Code(st.LastError) != codes.Unavailable {
			t.Fatalf("LastError = %v, want code Unavailable", st.LastError)
		}
	})
}

func TestServerDownAtStartThenCatchUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		opts := e.options()
		opts.CachePath = filepath.Join(t.TempDir(), "snapshot.json")
		if err := writeCache(opts.CachePath, snap(4, flag("new-cart", true, 100))); err != nil {
			t.Fatal(err)
		}
		e.down()
		c := e.start(opts)
		if !c.IsEnabled("new-cart", "alice") {
			t.Fatal("cached flag not served")
		}
		synctest.Wait()
		wantStatus(t, c, StateDisconnected, SourceCache, 4)

		e.up()
		w := e.accept()
		if got := w.req.GetKnownRevision(); got != 4 {
			t.Fatalf("known_revision %d, want the cached 4", got)
		}
		synctest.Wait()
		// The server has nothing newer to send, so the cached copy stays.
		wantStatus(t, c, StateLive, SourceCache, 4)

		w.push(t, snap(5, flag("new-cart", false, 100)))
		wantStatus(t, c, StateLive, SourceServer, 5)
		if c.IsEnabled("new-cart", "alice") {
			t.Fatal("revision 5 not applied")
		}
		if got := readCache(t, opts.CachePath).GetRevision(); got != 5 {
			t.Fatalf("cache holds revision %d, want 5", got)
		}
	})
}

func TestUnusableCacheIgnored(t *testing.T) {
	writeFile := func(content string) func(*testing.T, string) {
		return func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeSnapshot := func(s *cpv1.Snapshot) func(*testing.T, string) {
		return func(t *testing.T, path string) {
			if err := writeCache(path, s); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name    string
		create  func(*testing.T, string)
		wantLog string
		// writable is false when the cache path cannot hold a file at all.
		writable bool
	}{
		{"not JSON", writeFile("{not json"), "corrupt", true},
		{"empty file", writeFile(""), "corrupt", true},
		{"wrong field type", writeFile(`{"namespace": "checkout/prod", "revision": "seven"}`), "corrupt", true},
		{"other namespace", writeSnapshot(&cpv1.Snapshot{Namespace: "billing/prod", Revision: 3}), "another namespace", true},
		{"no revision", writeSnapshot(&cpv1.Snapshot{Namespace: testNamespace}), "without a revision", true},
		{"directory", func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}, "unreadable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newEnv(t)
				opts := e.options()
				opts.CachePath = filepath.Join(t.TempDir(), "snapshot.json")
				tc.create(t, opts.CachePath)

				c := e.start(opts)
				if c.Snapshot() != nil {
					t.Fatalf("served %v from an unusable cache", c.Snapshot())
				}
				if e.logs.count(slog.LevelWarn, tc.wantLog) != 1 {
					t.Fatalf("no warning containing %q", tc.wantLog)
				}

				e.accept().push(t, snap(1))
				wantStatus(t, c, StateLive, SourceServer, 1)
				if !tc.writable {
					if e.logs.count(slog.LevelWarn, "cannot write control plane snapshot cache") != 1 {
						t.Fatal("failed cache write not logged")
					}
					return
				}
				if got := readCache(t, opts.CachePath).GetRevision(); got != 1 {
					t.Fatalf("cache holds revision %d after the server sent 1", got)
				}
			})
		})
	}
}

func TestCacheFromNewerVersionLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		e.down()
		opts := e.options()
		opts.CachePath = filepath.Join(t.TempDir(), "snapshot.json")
		content := `{"namespace": "checkout/prod", "revision": "7", "flags": [{"key": "f", "enabled": true, "rolloutPercent": 100, "futureField": 1}], "futureList": []}`
		if err := os.WriteFile(opts.CachePath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		c := e.start(opts)
		synctest.Wait()
		wantStatus(t, c, StateDisconnected, SourceCache, 7)
		if !c.IsEnabled("f", "alice") {
			t.Fatal("flag from a cache with unknown fields not served")
		}
	})
}

func TestWriteCache(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "snapshot.json")
	for rev := int64(1); rev <= 3; rev++ {
		if err := writeCache(path, snap(rev, flag("f", true, 100))); err != nil {
			t.Fatalf("writeCache revision %d: %v", rev, err)
		}
	}
	if got, want := readCache(t, path), snap(3, flag("f", true, 100)); !proto.Equal(got, want) {
		t.Fatalf("cache = %v, want %v", got, want)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "snapshot.json" {
		t.Fatalf("directory holds %v, want only snapshot.json", entries)
	}

	// Replacing a file readable by others makes it private again.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeCache(path, snap(4)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("cache mode %v, want 0600", mode)
	}

	// A cache path below a regular file cannot be written.
	if err := writeCache(filepath.Join(path, "nested.json"), snap(1)); err == nil {
		t.Fatal("writeCache below a regular file succeeded")
	}
}

func TestCacheNeverOverwritesNewerSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		opts := e.options()
		opts.CachePath = filepath.Join(t.TempDir(), "snapshot.json")
		if err := writeCache(opts.CachePath, snap(5, flag("f", true, 100))); err != nil {
			t.Fatal(err)
		}
		c := e.start(opts)
		w := e.accept()

		// A server that went back in time, or repeats itself, changes neither
		// what is served nor what is cached.
		w.push(t, snap(3, flag("f", false, 0)))
		w.push(t, snap(5, flag("f", false, 0)))
		wantStatus(t, c, StateLive, SourceCache, 5)
		if got := readCache(t, opts.CachePath); got.GetRevision() != 5 || !got.GetFlags()[0].GetEnabled() {
			t.Fatalf("cache = %v, want the revision 5 it held", got)
		}
		w.push(t, snap(6, flag("f", false, 0)))
		if got := readCache(t, opts.CachePath).GetRevision(); got != 6 {
			t.Fatalf("cache holds revision %d, want 6", got)
		}
	})
}

func TestOversizedCacheIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		opts := e.options()
		opts.CachePath = filepath.Join(t.TempDir(), "snapshot.json")
		f, err := os.Create(opts.CachePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(maxCacheBytes + 1); err != nil {
			t.Fatal(err)
		}
		f.Close()
		c := e.start(opts)
		if c.Snapshot() != nil || e.logs.count(slog.LevelWarn, "unreadable") != 1 {
			t.Fatalf("an oversized cache was not ignored with a warning: %v", c.Snapshot())
		}
	})
}

func TestWriteCacheConcurrently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "snapshot.json")
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			if err := writeCache(path, snap(int64(i+1), flag("f", true, 100))); err != nil {
				t.Errorf("writeCache: %v", err)
			}
		})
	}
	wg.Wait()
	// Whichever write landed last, the file holds one of them in full.
	if rev := readCache(t, path).GetRevision(); rev < 1 || rev > 16 {
		t.Fatalf("cache holds revision %d", rev)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("directory holds %v, want only the cache file", entries)
	}
}
