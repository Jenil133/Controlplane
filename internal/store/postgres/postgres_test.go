package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
