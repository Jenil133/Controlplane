package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/postgres"
	"github.com/Jenil133/Controlplane/internal/store/storetest"
)

// These tests need a real PostgreSQL, e.g.
//
//	make compose-up
//	CONTROLPLANE_TEST_DATABASE_URL=postgres://controlplane:controlplane@localhost:5432/controlplane?sslmode=disable go test ./internal/store/postgres/
//
// Every test runs in its own throwaway schema.
const envDatabaseURL = "CONTROLPLANE_TEST_DATABASE_URL"

var schemaSeq atomic.Int64

type testDB struct {
	baseURL string
	admin   *pgxpool.Pool
}

func newTestDB(t *testing.T) *testDB {
	t.Helper()
	dsn := os.Getenv(envDatabaseURL)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL tests", envDatabaseURL)
	}
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)
	return &testDB{baseURL: dsn, admin: admin}
}

// schemaURL creates an empty schema and returns a URL whose connections use it.
func (db *testDB) schemaURL(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("cp_test_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	if _, err := db.admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	if !strings.Contains(db.baseURL, "://") {
		return db.baseURL + " search_path=" + schema
	}
	u, err := url.Parse(db.baseURL)
	if err != nil {
		t.Fatalf("parse %s: %v", envDatabaseURL, err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func open(t *testing.T, dsn string) *postgres.Store {
	t.Helper()
	s, err := postgres.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestConformance(t *testing.T) {
	db := newTestDB(t)
	storetest.Run(t, func(t *testing.T) store.Store {
		s := open(t, db.schemaURL(t))
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		return s
	})
}

func TestMigrateConcurrentAndIdempotent(t *testing.T) {
	db := newTestDB(t)
	dsn := db.schemaURL(t)

	stores := []*postgres.Store{open(t, dsn), open(t, dsn), open(t, dsn), open(t, dsn)}
	var wg sync.WaitGroup
	errs := make(chan error, len(stores))
	for _, s := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Migrate(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Migrate: %v", err)
		}
	}

	s := stores[0]
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if _, err := s.ListNamespaces(context.Background()); err != nil {
		t.Fatalf("schema unusable after migrations: %v", err)
	}
}

// connect opens a plain pool on dsn, for SQL the store does not offer.
func connect(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// TestMigrateUpgradesPhase1Data runs the migrations after 0001 on rows a
// Phase 1 release wrote, then checks the documented defaults, that no
// history was invented, and that writes, CAS and rollback work afterwards.
func TestMigrateUpgradesPhase1Data(t *testing.T) {
	db := newTestDB(t)
	dsn := db.schemaURL(t)
	ctx := context.Background()
	s := open(t, dsn)
	if err := s.MigrateTo(ctx, "0001_init"); err != nil {
		t.Fatalf("MigrateTo(0001_init): %v", err)
	}

	// The rows exactly as the Phase 1 store wrote them: four revisions, no
	// history, no audit log.
	pool := connect(t, dsn)
	at := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	exec(t, pool, `INSERT INTO namespaces (name, description, revision, created_at, updated_at) VALUES ('legacy', 'phase 1', 4, $1, $2)`,
		at, at.Add(3*time.Minute))
	exec(t, pool, `INSERT INTO configs (namespace, key, value, description, revision, updated_at, updated_by)
		VALUES ('legacy', 'db.pool', '{"max":10}', 'pool', 2, $1, 'alice')`, at.Add(time.Minute))
	exec(t, pool, `INSERT INTO flags (namespace, key, enabled, description, revision, updated_at, updated_by)
		VALUES ('legacy', 'new-cart', true, 'new cart', 3, $1, 'bob')`, at.Add(2*time.Minute))
	exec(t, pool, `INSERT INTO experiments (namespace, key, enabled, description, salt, variants, revision, updated_at, updated_by)
		VALUES ('legacy', 'cta', true, '', 'cta-v1', '[{"name":"control","weight":1},{"name":"green","weight":1,"payload":{"color":"green"}}]', 4, $1, 'carol')`,
		at.Add(3*time.Minute))

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate on Phase 1 data: %v", err)
	}

	ns, err := s.GetNamespace(ctx, "legacy")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if ns.Revision != 4 || ns.Description != "phase 1" || ns.CreatedBy != "" || !ns.CreatedAt.Equal(at) || !ns.UpdatedAt.Equal(at.Add(3*time.Minute)) {
		t.Fatalf("migrated namespace = %+v", ns)
	}
	snap, err := s.Snapshot(ctx, "legacy")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Revision != 4 || len(snap.Configs) != 1 || len(snap.Flags) != 1 || len(snap.Experiments) != 1 ||
		len(snap.RateLimits) != 0 || len(snap.CircuitBreakers) != 0 {
		t.Fatalf("migrated snapshot = %+v", snap)
	}
	flag := snap.Flags[0]
	if flag.Key != "new-cart" || !flag.Enabled || flag.Description != "new cart" || flag.RolloutPercent != 100 || flag.Salt != "new-cart" ||
		len(flag.Allowlist) != 0 || flag.Rollout != nil || flag.Revision != 3 || flag.UpdatedBy != "bob" || !flag.UpdatedAt.Equal(at.Add(2*time.Minute)) {
		t.Fatalf("migrated flag = %+v, want rollout 100%%, salt = key, no allowlist or plan", flag)
	}
	if got, err := s.GetFlag(ctx, "legacy", "new-cart"); err != nil || got.Salt != "new-cart" || got.RolloutPercent != 100 {
		t.Fatalf("GetFlag = %+v, %v", got, err)
	}
	if c := snap.Configs[0]; c.Key != "db.pool" || c.Revision != 2 || c.UpdatedBy != "alice" || !jsonEquivalent(c.Value, `{"max":10}`) {
		t.Fatalf("migrated config = %+v", c)
	}
	if e := snap.Experiments[0]; e.Salt != "cta-v1" || len(e.Variants) != 2 || !jsonEquivalent(e.Variants[1].Payload, `{"color":"green"}`) {
		t.Fatalf("migrated experiment = %+v", e)
	}

	// Nothing before the upgrade is in the history, and asking for it fails cleanly.
	if revs, err := s.ListRevisions(ctx, "legacy", 0, 0); err != nil || len(revs) != 0 {
		t.Fatalf("ListRevisions = %+v, %v; want none", revs, err)
	}
	for _, rev := range []int64{1, 4} {
		if _, err := s.GetRevision(ctx, "legacy", rev); !errors.Is(err, model.ErrNotFound) {
			t.Fatalf("GetRevision(%d) = %v, want ErrNotFound", rev, err)
		}
	}
	if _, _, err := s.Rollback(ctx, "legacy", 3, store.WriteOptions{Actor: "x"}); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("Rollback to a Phase 1 revision = %v, want ErrNotFound", err)
	}
	if events, err := s.ListAuditEvents(ctx, model.AuditFilter{Namespace: "legacy"}); err != nil || len(events) != 0 {
		t.Fatalf("ListAuditEvents = %+v, %v; want none", events, err)
	}

	// Writes continue the revision sequence, with CAS against Phase 1 revisions.
	flag.RolloutPercent, flag.Allowlist = 50, []string{"u1"}
	if _, err := s.PutFlag(ctx, "legacy", flag, store.WriteOptions{Actor: "dave", ExpectedRevision: 2}); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("PutFlag at a stale revision = %v, want ErrConflict", err)
	}
	out, err := s.PutFlag(ctx, "legacy", flag, store.WriteOptions{Actor: "dave", ExpectedRevision: 3})
	if err != nil || out.Revision != 5 {
		t.Fatalf("PutFlag = %+v, %v; want revision 5", out, err)
	}
	events, err := s.ListAuditEvents(ctx, model.AuditFilter{Namespace: "legacy"})
	if err != nil || len(events) != 1 || events[0].Action != model.ActionUpdate || events[0].Revision != 5 {
		t.Fatalf("ListAuditEvents = %+v, %v; want one update at revision 5", events, err)
	}
	var before model.Flag
	if err := json.Unmarshal(events[0].Before, &before); err != nil || before.Salt != "new-cart" || before.RolloutPercent != 100 || before.Revision != 3 {
		t.Fatalf("audit before image = %s (%v), want the migrated flag", events[0].Before, err)
	}
	r5, err := s.GetRevision(ctx, "legacy", 5)
	if err != nil {
		t.Fatalf("GetRevision(5): %v", err)
	}
	now, err := s.Snapshot(ctx, "legacy")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if diff, err := model.Diff(now, r5.Snapshot); err != nil || len(diff) != 0 || r5.Snapshot.Revision != 5 || r5.Summary != "update flag new-cart" {
		t.Fatalf("revision 5 = %+v (diff %v, %v), want the current state", r5, diff, err)
	}
	if rev, err := s.DeleteConfig(ctx, "legacy", "db.pool", store.WriteOptions{Actor: "dave"}); err != nil || rev != 6 {
		t.Fatalf("DeleteConfig = %d, %v; want 6", rev, err)
	}
	rev, changes, err := s.Rollback(ctx, "legacy", 5, store.WriteOptions{Actor: "erin"})
	if err != nil || rev != 7 || len(changes) != 1 || changes[0].Type != model.ChangeAdded || changes[0].Key != "db.pool" {
		t.Fatalf("Rollback to 5 = %d, %+v, %v; want revision 7 restoring db.pool", rev, changes, err)
	}

	// A Phase 1 replica still running during a rolling upgrade can insert flags.
	exec(t, pool, `INSERT INTO flags (namespace, key, enabled, description, revision, updated_at, updated_by)
		VALUES ('legacy', 'old-writer', true, '', 8, now(), 'phase1')`)
	if got, err := s.GetFlag(ctx, "legacy", "old-writer"); err != nil || got.RolloutPercent != 100 || len(got.Allowlist) != 0 || got.Rollout != nil {
		t.Fatalf("flag inserted by Phase 1 = %+v, %v", got, err)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

// jsonEquivalent reports whether got is the JSON document want, ignoring
// formatting (PostgreSQL normalizes jsonb).
func jsonEquivalent(got json.RawMessage, want string) bool {
	var g, w any
	return json.Unmarshal(got, &g) == nil && json.Unmarshal([]byte(want), &w) == nil && reflect.DeepEqual(g, w)
}

// TestListingsUseByteOrder gives the name and key columns a locale
// collation that orders "a_b" before "a-b" and "a" before "B", and checks
// that every listing still sorts byte-wise. The conformance suite cannot tell
// on a database whose default collation already is byte order.
func TestListingsUseByteOrder(t *testing.T) {
	db := newTestDB(t)
	dsn := db.schemaURL(t)
	ctx := context.Background()
	s := open(t, dsn)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	pool := connect(t, dsn)
	var icu bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'en-US-x-icu')`).Scan(&icu); err != nil {
		t.Fatal(err)
	}
	if !icu {
		t.Skip("ICU collation en-US-x-icu is not available")
	}
	var reversed bool
	err := pool.QueryRow(ctx, `SELECT 'a_b' < 'a-b' COLLATE "en-US-x-icu" AND 'a' < 'B' COLLATE "en-US-x-icu"`).Scan(&reversed)
	if err != nil || !reversed {
		t.Fatalf("the collation does not reverse the test names (%v); the test proves nothing", err)
	}
	exec(t, pool, `ALTER TABLE namespaces ALTER COLUMN name TYPE TEXT COLLATE "en-US-x-icu"`)
	for _, table := range []string{"configs", "flags", "experiments", "rate_limits", "circuit_breakers"} {
		exec(t, pool, `ALTER TABLE `+table+` ALTER COLUMN key TYPE TEXT COLLATE "en-US-x-icu"`)
	}

	opts := store.WriteOptions{Actor: "t"}
	active := func(key string) model.Flag {
		now := time.Now().UTC()
		return model.Flag{Key: key, Salt: key, RolloutPercent: 10, Rollout: &model.RolloutPlan{
			Stages: []model.RolloutStage{{Percent: 10}, {Percent: 100}}, State: model.RolloutActive, StartedAt: now, StageStartedAt: now,
		}}
	}
	for _, name := range []string{"a_b", "a-b"} {
		if _, err := s.CreateNamespace(ctx, model.Namespace{Name: name}, opts); err != nil {
			t.Fatalf("CreateNamespace(%q): %v", name, err)
		}
	}
	if _, err := s.PutFlag(ctx, "a_b", active("x"), opts); err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	for _, key := range []string{"a", "B"} {
		_, err := s.PutConfig(ctx, "a-b", model.Config{Key: key, Value: json.RawMessage(`1`)}, opts)
		if err == nil {
			_, err = s.PutFlag(ctx, "a-b", active(key), opts)
		}
		if err == nil {
			_, err = s.PutExperiment(ctx, "a-b", model.Experiment{Key: key, Salt: key, Variants: []model.Variant{{Name: "v", Weight: 1}}}, opts)
		}
		if err == nil {
			_, err = s.PutRateLimit(ctx, "a-b", model.RateLimit{Key: key, RequestsPerSecond: 1, Burst: 1}, opts)
		}
		if err == nil {
			_, err = s.PutCircuitBreaker(ctx, "a-b", model.CircuitBreaker{
				Key: key, FailureRateThreshold: 1, MinRequests: 1, Window: time.Second, OpenDuration: time.Second, HalfOpenMaxRequests: 1,
			}, opts)
		}
		if err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
	}

	list, err := s.ListNamespaces(ctx)
	if got := keysOf(list, func(n model.Namespace) string { return n.Name }); err != nil || !slices.Equal(got, []string{"a-b", "a_b"}) {
		t.Errorf("ListNamespaces = %q, %v; want [a-b a_b]", got, err)
	}
	snap, err := s.Snapshot(ctx, "a-b")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	rev, err := s.GetRevision(ctx, "a-b", snap.Revision)
	if err != nil {
		t.Fatalf("GetRevision: %v", err)
	}
	want := []string{"B", "a"}
	for _, src := range []struct {
		name string
		snap model.Snapshot
	}{{"Snapshot", snap}, {"GetRevision", rev.Snapshot}} {
		for list, got := range map[string][]string{
			"configs":          keysOf(src.snap.Configs, func(v model.Config) string { return v.Key }),
			"flags":            keysOf(src.snap.Flags, func(v model.Flag) string { return v.Key }),
			"experiments":      keysOf(src.snap.Experiments, func(v model.Experiment) string { return v.Key }),
			"rate limits":      keysOf(src.snap.RateLimits, func(v model.RateLimit) string { return v.Key }),
			"circuit breakers": keysOf(src.snap.CircuitBreakers, func(v model.CircuitBreaker) string { return v.Key }),
		} {
			if !slices.Equal(got, want) {
				t.Errorf("%s %s = %q, want %q", src.name, list, got, want)
			}
		}
	}
	refs, err := s.ListActiveRollouts(ctx)
	wantRefs := []model.RolloutRef{{Namespace: "a-b", Key: "B"}, {Namespace: "a-b", Key: "a"}, {Namespace: "a_b", Key: "x"}}
	if err != nil || !slices.Equal(refs, wantRefs) {
		t.Errorf("ListActiveRollouts = %+v, %v; want %+v", refs, err, wantRefs)
	}
}

func keysOf[T any](entries []T, key func(T) string) []string {
	out := make([]string, len(entries))
	for i, v := range entries {
		out[i] = key(v)
	}
	return out
}
