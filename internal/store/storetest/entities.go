package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// recorded describes what one single-entity write must leave behind.
type recorded struct {
	rev                 int64
	actor               string
	action, entity, key string
	before, after       json.RawMessage
	message, summary    string
}

// checkRecorded verifies that the latest write to ns is the one described:
// the namespace revision, the written entry's metadata, the entry as stored
// (want.after, or gone), exactly one audit event and a history entry whose
// snapshot is the current state, all stamped with the namespace's update
// time.
func checkRecorded(t *testing.T, s store.Store, ns string, want recorded) {
	t.Helper()
	snap := mustSnapshot(t, s, ns)
	at := snap.UpdatedAt
	if snap.Revision != want.rev {
		t.Fatalf("namespace %q is at revision %d, want %d", ns, snap.Revision, want.rev)
	}
	if want.entity != model.EntityNamespace {
		if want.after != nil {
			m := decode[entryMetadata](t, want.after)
			if m.Revision != want.rev || !m.UpdatedAt.Equal(at) || m.UpdatedBy != want.actor {
				t.Fatalf("written %s: revision %d updated %v by %q, want %d, %v, %q",
					want.entity, m.Revision, m.UpdatedAt, m.UpdatedBy, want.rev, at, want.actor)
			}
			requireUTC(t, "written "+want.entity, m.UpdatedAt)
		}
		sameImage(t, "stored "+want.entity, images(t, snap)[want.entity+" "+want.key], want.after)
	}

	events := mustAudit(t, s, model.AuditFilter{Namespace: ns, Limit: 2})
	if len(events) == 0 {
		t.Fatalf("no audit event in %q", ns)
	}
	if len(events) == 2 && events[1].Revision >= want.rev {
		t.Fatalf("revision %d has more than one audit event", want.rev)
	}
	e := events[0]
	if e.ID <= 0 || e.Namespace != ns || e.Revision != want.rev || e.Actor != want.actor || !e.Time.Equal(at) ||
		e.Action != want.action || e.EntityType != want.entity || e.EntityKey != want.key || e.Message != want.message {
		t.Fatalf("audit event #%d: %s rev %d by %q at %v: %s %s %q (message %q)\nwant: %s rev %d by %q at %v: %s %s %q (message %q)",
			e.ID, e.Namespace, e.Revision, e.Actor, e.Time, e.Action, e.EntityType, e.EntityKey, e.Message,
			ns, want.rev, want.actor, at, want.action, want.entity, want.key, want.message)
	}
	sameImage(t, "audit before image", e.Before, want.before)
	sameImage(t, "audit after image", e.After, want.after)
	requireUTC(t, "audit event", e.Time)

	revs := mustRevisions(t, s, ns, 0, 1)
	if len(revs) != 1 {
		t.Fatalf("ListRevisions(limit 1) returned %d entries", len(revs))
	}
	if r := revs[0]; r.Namespace != ns || r.Revision != want.rev || r.Actor != want.actor || !r.CreatedAt.Equal(at) || r.Summary != want.summary {
		t.Fatalf("history entry: %s rev %d by %q at %v %q, want %s rev %d by %q at %v %q",
			r.Namespace, r.Revision, r.Actor, r.CreatedAt, r.Summary, ns, want.rev, want.actor, at, want.summary)
	}
	requireUTC(t, "history entry", revs[0].CreatedAt)
	sameSnapshot(t, fmt.Sprintf("revision %d", want.rev), mustGetRevision(t, s, ns, want.rev).Snapshot, snap)
}

func testNamespaceRecords(t *testing.T, s store.Store) {
	ctx := context.Background()
	ns, err := s.CreateNamespace(ctx, model.Namespace{Name: "svc", Description: "checkout"}, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if ns.Name != "svc" || ns.Description != "checkout" || ns.Revision != 1 || ns.CreatedBy != "alice" || !ns.CreatedAt.Equal(ns.UpdatedAt) {
		t.Fatalf("CreateNamespace = %+v, want revision 1 created by alice, created_at = updated_at", ns)
	}
	requireUTC(t, "CreateNamespace", ns.CreatedAt, ns.UpdatedAt)
	got, err := s.GetNamespace(ctx, "svc")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	sameValue(t, "GetNamespace", got, ns)
	list, err := s.ListNamespaces(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListNamespaces = %+v, %v", list, err)
	}
	sameValue(t, "ListNamespaces", list[0], ns)
	if snap := mustSnapshot(t, s, "svc"); !snap.UpdatedAt.Equal(ns.UpdatedAt) {
		t.Fatalf("snapshot updated %v, want %v", snap.UpdatedAt, ns.UpdatedAt)
	}

	checkRecorded(t, s, "svc", recorded{
		rev: 1, actor: "alice", action: model.ActionCreate, entity: model.EntityNamespace, key: "svc",
		after: encode(t, ns), summary: "create namespace",
	})
	first := mustGetRevision(t, s, "svc", 1).Snapshot
	if n := len(first.Configs) + len(first.Flags) + len(first.Experiments) + len(first.RateLimits) + len(first.CircuitBreakers); n != 0 {
		t.Fatalf("revision 1 has %d entries, want an empty namespace", n)
	}

	cp := takeCheckpoint(t, s, "svc")
	_, err = s.CreateNamespace(ctx, model.Namespace{Name: "svc"}, store.WriteOptions{Actor: "bob"})
	wantErr(t, err, model.ErrAlreadyExists, "duplicate CreateNamespace")
	unchanged(t, s, "svc", cp, "duplicate CreateNamespace")
	if got, err := s.GetNamespace(ctx, "svc"); err != nil || got.CreatedBy != "alice" {
		t.Fatalf("after duplicate create: %+v, %v; want created by alice", got, err)
	}
}

// testEntityWrites checks the write contract for every entity type: returned
// metadata, audit images, default actions and summaries, optimistic
// concurrency against the entry's own revision, read-modify-write, and that
// failed writes leave no trace.
func testEntityWrites(t *testing.T, s store.Store) {
	for _, k := range entityKinds {
		t.Run(k.entity, func(t *testing.T) {
			ns := "svc/" + k.entity
			mustCreate(t, s, ns)
			// unrelated moves the namespace past "key" with a write to another entry.
			unrelated := func(version int) {
				t.Helper()
				if _, _, err := k.put(s, ns, "other", version, store.WriteOptions{Actor: "frank"}); err != nil {
					t.Fatalf("write other: %v", err)
				}
			}

			rev, v1, err := k.put(s, ns, "key", 1, store.WriteOptions{Actor: "alice"})
			if err != nil || rev != 2 {
				t.Fatalf("create: revision %d, %v; want 2", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 2, actor: "alice", action: model.ActionCreate, entity: k.entity, key: "key",
				after: v1, summary: "create " + k.entity + " key",
			})

			rev, v2, err := k.put(s, ns, "key", 2, store.WriteOptions{Actor: "bob", ExpectedRevision: 2})
			if err != nil || rev != 3 {
				t.Fatalf("update at expected revision: revision %d, %v; want 3", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 3, actor: "bob", action: model.ActionUpdate, entity: k.entity, key: "key",
				before: v1, after: v2, summary: "update " + k.entity + " key",
			})

			// From here on the namespace is ahead of the entry, so an expected
			// revision must match the entry's revision (3), not the
			// namespace's (4): a rollout controller's CAS must survive
			// unrelated writes.
			unrelated(1)
			put := func(ns, key string, expected int64) func() error {
				return func() error {
					_, _, err := k.put(s, ns, key, 9, store.WriteOptions{Actor: "eve", ExpectedRevision: expected})
					return err
				}
			}
			del := func(ns, key string, expected int64) func() error {
				return func() error {
					_, err := k.del(s, ns, key, store.WriteOptions{Actor: "eve", ExpectedRevision: expected})
					return err
				}
			}
			cp := takeCheckpoint(t, s, ns)
			for _, f := range []struct {
				name   string
				write  func() error
				target error
			}{
				{"put at a stale revision", put(ns, "key", 2), model.ErrConflict},
				{"put at the namespace revision", put(ns, "key", 4), model.ErrConflict},
				{"put at a future revision", put(ns, "key", 5), model.ErrConflict},
				{"put of a missing entry with an expected revision", put(ns, "missing", 4), model.ErrConflict},
				{"delete at a stale revision", del(ns, "key", 2), model.ErrConflict},
				{"delete at the namespace revision", del(ns, "key", 4), model.ErrConflict},
				{"delete of a missing entry", del(ns, "missing", 0), model.ErrNotFound},
				{"delete of a missing entry with an expected revision", del(ns, "missing", 4), model.ErrNotFound},
				{"put in a missing namespace", put("missing", "key", 0), model.ErrNotFound},
				{"put in a missing namespace with an expected revision", put("missing", "key", 3), model.ErrNotFound},
				{"delete in a missing namespace", del("missing", "key", 0), model.ErrNotFound},
			} {
				wantErr(t, f.write(), f.target, f.name)
				unchanged(t, s, ns, cp, f.name)
			}

			// The value read back carries the stored revision 3, bob and its
			// time; the write must replace all three.
			rev, v3, err := k.rewrite(s, ns, "key", 3, store.WriteOptions{Actor: "carol", ExpectedRevision: 3})
			if err != nil || rev != 5 {
				t.Fatalf("read-modify-write at the entry's revision: revision %d, %v; want 5", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 5, actor: "carol", action: model.ActionUpdate, entity: k.entity, key: "key",
				before: v2, after: v3, summary: "update " + k.entity + " key",
			})

			unrelated(2)
			rev, err = k.del(s, ns, "key", store.WriteOptions{Actor: "dave", ExpectedRevision: 5})
			if err != nil || rev != 7 {
				t.Fatalf("delete at the entry's revision: revision %d, %v; want 7", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 7, actor: "dave", action: model.ActionDelete, entity: k.entity, key: "key",
				before: v3, summary: "delete " + k.entity + " key",
			})

			rev, v4, err := k.put(s, ns, "key", 4, store.WriteOptions{Actor: "erin"})
			if err != nil || rev != 8 {
				t.Fatalf("re-create: revision %d, %v; want 8", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 8, actor: "erin", action: model.ActionCreate, entity: k.entity, key: "key",
				after: v4, summary: "create " + k.entity + " key",
			})
			rev, v5, err := k.put(s, ns, "key", 5, store.WriteOptions{Actor: "grace"})
			if err != nil || rev != 9 {
				t.Fatalf("unconditional update: revision %d, %v; want 9", rev, err)
			}
			checkRecorded(t, s, ns, recorded{
				rev: 9, actor: "grace", action: model.ActionUpdate, entity: k.entity, key: "key",
				before: v4, after: v5, summary: "update " + k.entity + " key",
			})
		})
	}
}

func stripRateLimit(r model.RateLimit) model.RateLimit {
	r.Revision, r.UpdatedAt, r.UpdatedBy = 0, time.Time{}, ""
	return r
}

func stripBreaker(c model.CircuitBreaker) model.CircuitBreaker {
	c.Revision, c.UpdatedAt, c.UpdatedBy = 0, time.Time{}, ""
	return c
}

func sameRateLimit(t *testing.T, what string, got, want model.RateLimit) {
	t.Helper()
	if stripRateLimit(got) != stripRateLimit(want) || got.Revision != want.Revision ||
		got.UpdatedBy != want.UpdatedBy || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("%s: rate limit = %+v, want %+v", what, got, want)
	}
	requireUTC(t, what, got.UpdatedAt)
}

func sameBreaker(t *testing.T, what string, got, want model.CircuitBreaker) {
	t.Helper()
	if stripBreaker(got) != stripBreaker(want) || got.Revision != want.Revision ||
		got.UpdatedBy != want.UpdatedBy || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("%s: circuit breaker = %+v, want %+v", what, got, want)
	}
	requireUTC(t, what, got.UpdatedAt)
}

// testPoliciesRoundTrip checks that traffic policies come back exactly,
// floats and nanosecond durations included, from every read path.
func testPoliciesRoundTrip(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	rl := model.RateLimit{Key: "checkout", Enabled: true, Description: "checkout API", RequestsPerSecond: 0.1, Burst: model.MaxBurst}
	cb := model.CircuitBreaker{
		Key: "payments", Enabled: true, Description: "payments API", FailureRateThreshold: 0.375,
		MinRequests: model.MaxMinRequests, Window: 1234567891 * time.Nanosecond, OpenDuration: model.MaxOpenDuration - time.Nanosecond,
		HalfOpenMaxRequests: model.MaxHalfOpenRequests,
	}
	putRL, err := s.PutRateLimit(ctx, "svc", rl, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutRateLimit: %v", err)
	}
	putCB, err := s.PutCircuitBreaker(ctx, "svc", cb, store.WriteOptions{Actor: "bob"})
	if err != nil {
		t.Fatalf("PutCircuitBreaker: %v", err)
	}
	if stripRateLimit(putRL) != rl || putRL.Revision != 2 || putRL.UpdatedBy != "alice" {
		t.Fatalf("PutRateLimit = %+v, want %+v at revision 2 by alice", putRL, rl)
	}
	if stripBreaker(putCB) != cb || putCB.Revision != 3 || putCB.UpdatedBy != "bob" {
		t.Fatalf("PutCircuitBreaker = %+v, want %+v at revision 3 by bob", putCB, cb)
	}
	// Disabled policies without descriptions, sorting before the others.
	w := newWriter(t, s, "svc", "carol")
	otherRL := w.rateLimit(model.RateLimit{Key: "api", RequestsPerSecond: model.MaxRequestsPerSecond, Burst: 1})
	otherCB := w.breaker(model.CircuitBreaker{
		Key: "inventory", FailureRateThreshold: 1, MinRequests: 1, Window: model.MaxBreakerWindow,
		OpenDuration: model.MinOpenDuration, HalfOpenMaxRequests: 1,
	})

	snap := mustSnapshot(t, s, "svc")
	for _, src := range []struct {
		name string
		snap model.Snapshot
	}{
		{"Snapshot", snap},
		{"GetRevision", mustGetRevision(t, s, "svc", snap.Revision).Snapshot},
	} {
		if len(src.snap.RateLimits) != 2 || len(src.snap.CircuitBreakers) != 2 {
			t.Fatalf("%s: %d rate limits and %d breakers, want 2 and 2", src.name, len(src.snap.RateLimits), len(src.snap.CircuitBreakers))
		}
		sameRateLimit(t, src.name, src.snap.RateLimits[0], otherRL)
		sameRateLimit(t, src.name, src.snap.RateLimits[1], putRL)
		sameBreaker(t, src.name, src.snap.CircuitBreakers[0], otherCB)
		sameBreaker(t, src.name, src.snap.CircuitBreakers[1], putCB)
	}

	rlEvents := mustAudit(t, s, model.AuditFilter{Namespace: "svc", EntityType: model.EntityRateLimit, EntityKey: "checkout"})
	cbEvents := mustAudit(t, s, model.AuditFilter{Namespace: "svc", EntityType: model.EntityCircuitBreaker, EntityKey: "payments"})
	if len(rlEvents) != 1 || len(cbEvents) != 1 {
		t.Fatalf("got %d and %d audit events, want 1 and 1", len(rlEvents), len(cbEvents))
	}
	sameRateLimit(t, "audit image", decode[model.RateLimit](t, rlEvents[0].After), putRL)
	sameBreaker(t, "audit image", decode[model.CircuitBreaker](t, cbEvents[0].After), putCB)
}

// checkFlag compares everything but the revision metadata: timestamps as
// instants, durations and percentages exactly, the allowlist in order.
func checkFlag(t *testing.T, what string, got, want model.Flag) {
	t.Helper()
	if got.Key != want.Key || got.Enabled != want.Enabled || got.Description != want.Description ||
		got.RolloutPercent != want.RolloutPercent || got.Salt != want.Salt || !slices.Equal(got.Allowlist, want.Allowlist) {
		t.Fatalf("%s: flag = %+v, want %+v", what, got, want)
	}
	if (got.Rollout == nil) != (want.Rollout == nil) {
		t.Fatalf("%s: rollout plan = %+v, want %+v", what, got.Rollout, want.Rollout)
	}
	if want.Rollout == nil {
		return
	}
	g, w := got.Rollout, want.Rollout
	if !slices.Equal(g.Stages, w.Stages) || g.CurrentStage != w.CurrentStage || g.State != w.State ||
		!g.StartedAt.Equal(w.StartedAt) || !g.StageStartedAt.Equal(w.StageStartedAt) || g.StartedBy != w.StartedBy {
		t.Fatalf("%s: rollout plan = %+v, want %+v", what, *g, *w)
	}
	requireUTC(t, what, g.StartedAt, g.StageStartedAt)
}

func testFlagFields(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	in := rolloutFlag("new-cart", model.RolloutActive, 1)
	in.Description = "new cart"
	in.Salt = "new-cart-v2"
	in.Allowlist = []string{"user-9", "user-1", "user-5"} // not sorted: the order is kept

	out, err := s.PutFlag(ctx, "svc", in, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	checkFlag(t, "PutFlag", out, in)
	got, err := s.GetFlag(ctx, "svc", "new-cart")
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	checkFlag(t, "GetFlag", got, in)
	sameValue(t, "GetFlag", got, out)
	requireUTC(t, "PutFlag and GetFlag", out.UpdatedAt, got.UpdatedAt)
	snap := mustSnapshot(t, s, "svc")
	checkFlag(t, "Snapshot", snap.Flags[0], in)
	sameValue(t, "Snapshot", snap.Flags[0], out)
	checkFlag(t, "GetRevision", mustGetRevision(t, s, "svc", out.Revision).Snapshot.Flags[0], in)
	events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", EntityType: model.EntityFlag, Limit: 1})
	checkFlag(t, "audit image", decode[model.Flag](t, events[0].After), in)

	// Clearing the optional fields clears them.
	plain := newFlag("new-cart", 100)
	if _, err := s.PutFlag(ctx, "svc", plain, store.WriteOptions{Actor: "bob"}); err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	got, err = s.GetFlag(ctx, "svc", "new-cart")
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	checkFlag(t, "cleared flag", got, plain)
}

func testGetFlag(t *testing.T, s store.Store) {
	ctx := context.Background()
	_, err := s.GetFlag(ctx, "missing", "f")
	wantErr(t, err, model.ErrNotFound, "GetFlag in a missing namespace")
	mustCreate(t, s, "svc")
	_, err = s.GetFlag(ctx, "svc", "f")
	wantErr(t, err, model.ErrNotFound, "GetFlag of a missing flag")

	w := newWriter(t, s, "svc", "alice")
	w.flag(newFlag("f", 10))
	latest := w.as("bob").flag(rolloutFlag("f", model.RolloutPaused, 1))
	w.config("f", `"not a flag"`)
	got, err := s.GetFlag(ctx, "svc", "f")
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	sameValue(t, "GetFlag", got, latest)

	w.del(model.EntityFlag, "f")
	_, err = s.GetFlag(ctx, "svc", "f")
	wantErr(t, err, model.ErrNotFound, "GetFlag of a deleted flag")
}

// testSnapshotByteOrder checks that every list is sorted byte-wise, which
// puts upper case first and orders - . / digits _ lower case, unlike
// locale-aware collations.
func testSnapshotByteOrder(t *testing.T, s store.Store) {
	mustCreate(t, s, "svc")
	keys := []string{"b", "a_b", "B", "a.b", "a-b", "a/b", "a0", "A"}
	for _, k := range entityKinds {
		for i, key := range keys {
			if _, _, err := k.put(s, "svc", key, i+1, store.WriteOptions{Actor: "a"}); err != nil {
				t.Fatalf("put %s %q: %v", k.entity, key, err)
			}
		}
	}
	want := slices.Sorted(slices.Values(keys))
	snap := mustSnapshot(t, s, "svc")
	for _, k := range entityKinds {
		if got := k.keys(snap); !slices.Equal(got, want) {
			t.Errorf("%s keys = %q, want %q", k.entity, got, want)
		}
	}
	sameSnapshot(t, "latest revision", mustGetRevision(t, s, "svc", snap.Revision).Snapshot, snap)
}

func testListActiveRollouts(t *testing.T, s store.Store) {
	check := func(want []model.RolloutRef) {
		t.Helper()
		got, err := s.ListActiveRollouts(context.Background())
		if err != nil {
			t.Fatalf("ListActiveRollouts: %v", err)
		}
		if len(got) != len(want) || (len(want) > 0 && !slices.Equal(got, want)) {
			t.Fatalf("ListActiveRollouts = %+v, want %+v", got, want)
		}
	}
	check(nil)

	for _, ns := range []string{"b", "ab", "a-z"} {
		mustCreate(t, s, ns)
	}
	newWriter(t, s, "b", "alice").flag(rolloutFlag("alpha", model.RolloutActive, 0))
	ab := newWriter(t, s, "ab", "alice")
	ab.flag(rolloutFlag("alpha", model.RolloutActive, 1))
	ab.flag(rolloutFlag("Zeta", model.RolloutActive, 0))
	ab.flag(rolloutFlag("beta", model.RolloutPaused, 1))
	ab.flag(rolloutFlag("gamma", model.RolloutCompleted, 2))
	ab.flag(rolloutFlag("delta", model.RolloutAborted, 1))
	ab.flag(newFlag("epsilon", 50))
	newWriter(t, s, "a-z", "alice").flag(rolloutFlag("x", model.RolloutActive, 0))

	// Byte order: "a-z" < "ab" and "Zeta" < "alpha".
	check([]model.RolloutRef{
		{Namespace: "a-z", Key: "x"},
		{Namespace: "ab", Key: "Zeta"},
		{Namespace: "ab", Key: "alpha"},
		{Namespace: "b", Key: "alpha"},
	})

	ab.flag(rolloutFlag("alpha", model.RolloutPaused, 1))
	newWriter(t, s, "b", "alice").del(model.EntityFlag, "alpha")
	check([]model.RolloutRef{{Namespace: "a-z", Key: "x"}, {Namespace: "ab", Key: "Zeta"}})
}

// testConcurrentCAS races writers on the flag revision they read, as rollout
// controllers on several replicas do: exactly one may win, also after an
// unrelated write moved the namespace past the flag.
func testConcurrentCAS(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	f, err := s.PutFlag(ctx, "svc", rolloutFlag("f", model.RolloutActive, 0), store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	newWriter(t, s, "svc", "bob").config("unrelated", `1`)
	next := rolloutFlag("f", model.RolloutActive, 1)

	const racers = 8
	var (
		wg              sync.WaitGroup
		wins, conflicts atomic.Int32
		errs            = make(chan error, racers)
	)
	for i := range racers {
		wg.Go(func() {
			_, err := s.PutFlag(ctx, "svc", next, store.WriteOptions{
				Actor: fmt.Sprintf("controller-%d", i), ExpectedRevision: f.Revision, Action: model.ActionRolloutAdvance,
			})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, model.ErrConflict):
				conflicts.Add(1)
			default:
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("racing PutFlag: %v", err)
	}
	if wins.Load() != 1 || conflicts.Load() != racers-1 {
		t.Fatalf("%d writes won and %d conflicted, want 1 and %d", wins.Load(), conflicts.Load(), racers-1)
	}
	if rev := mustRevision(t, s, "svc"); rev != 4 {
		t.Fatalf("namespace revision = %d, want 4", rev)
	}
	if n := len(mustRevisions(t, s, "svc", 0, maxLimit)); n != 4 {
		t.Fatalf("history has %d revisions, want 4", n)
	}
	if n := len(mustAudit(t, s, model.AuditFilter{Namespace: "svc", EntityType: model.EntityFlag})); n != 2 {
		t.Fatalf("flag has %d audit events, want 2", n)
	}
}

// testConcurrentHistory checks that concurrent writers leave a gap-free
// history in which every revision has its own audit event and a snapshot
// containing its write and nothing later.
func testConcurrentHistory(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	const writers, perWriter = 6, 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Go(func() {
			opts := store.WriteOptions{Actor: fmt.Sprintf("writer-%d", w)}
			for i := range perWriter {
				var err error
				if i%2 == 0 {
					_, err = s.PutConfig(ctx, "svc", model.Config{Key: fmt.Sprintf("c%d", w), Value: json.RawMessage(strconv.Itoa(i))}, opts)
				} else {
					_, err = s.PutFlag(ctx, "svc", newFlag(fmt.Sprintf("f%d", w), float64(i)), opts)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write: %v", err)
	}

	const latest = 1 + writers*perWriter
	revs := mustRevisions(t, s, "svc", 0, maxLimit)
	events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: maxLimit})
	if len(revs) != latest || len(events) != latest {
		t.Fatalf("%d revisions and %d audit events, want %d of each", len(revs), len(events), latest)
	}
	for i, r := range revs {
		want := int64(latest - i)
		if r.Revision != want || events[i].Revision != want {
			t.Fatalf("entry %d: revision %d, audit event for revision %d; want %d", i, r.Revision, events[i].Revision, want)
		}
		if events[i].Actor != r.Actor || !events[i].Time.Equal(r.CreatedAt) {
			t.Fatalf("revision %d: audit event by %q at %v, history entry by %q at %v",
				want, events[i].Actor, events[i].Time, r.Actor, r.CreatedAt)
		}
		snap := mustGetRevision(t, s, "svc", want).Snapshot
		written := 0
		for _, c := range snap.Configs {
			if c.Revision > want {
				t.Fatalf("revision %d snapshot holds config %q from revision %d", want, c.Key, c.Revision)
			}
			if c.Revision == want {
				written++
			}
		}
		for _, f := range snap.Flags {
			if f.Revision > want {
				t.Fatalf("revision %d snapshot holds flag %q from revision %d", want, f.Key, f.Revision)
			}
			if f.Revision == want {
				written++
			}
		}
		if snap.Revision != want || (want > 1 && written != 1) {
			t.Fatalf("revision %d snapshot is at revision %d with %d entries written at %d, want 1", want, snap.Revision, written, want)
		}
	}
}

// testConcurrentSnapshots reads snapshots while writers commit and checks
// that each one is the state at the revision it reports, as watchers are
// promised: the revision and the entries always belong together.
func testConcurrentSnapshots(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	const writers, perWriter, readers = 4, 40, 4
	var (
		ready, reading, writing sync.WaitGroup
		done                    atomic.Bool
		errs                    = make(chan error, writers+readers)
	)
	ready.Add(readers)
	for range readers {
		reading.Go(func() {
			for n := 0; ; n++ {
				last := done.Load()
				snap, err := s.Snapshot(ctx, "svc")
				if n == 0 {
					ready.Done()
				}
				if err == nil {
					err = tornSnapshot(snap)
				}
				if err != nil {
					errs <- err
					return
				}
				if last {
					return
				}
			}
		})
	}
	// Writing starts once every reader is reading. Each revision writes one
	// entry, a config or a flag, and nothing is deleted.
	ready.Wait()
	for w := range writers {
		writing.Go(func() {
			opts := store.WriteOptions{Actor: fmt.Sprintf("writer-%d", w)}
			key := fmt.Sprintf("k%d", w)
			for i := range perWriter {
				var err error
				if i%2 == 0 {
					_, err = s.PutConfig(ctx, "svc", model.Config{Key: key, Value: json.RawMessage(strconv.Itoa(i))}, opts)
				} else {
					_, err = s.PutFlag(ctx, "svc", newFlag(key, float64(i)), opts)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	writing.Wait()
	done.Store(true)
	reading.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// tornSnapshot describes how snap fails to be the state at its revision in a
// namespace where every revision after the first wrote one entry and none
// was deleted: it holds an entry from a later revision, or not exactly one
// entry from its own revision stamped with the revision's time.
func tornSnapshot(snap model.Snapshot) error {
	var written []entry
	for _, e := range entries(snap) {
		if e.Revision > snap.Revision {
			return fmt.Errorf("snapshot at revision %d holds %s %q from revision %d", snap.Revision, e.entity, e.key, e.Revision)
		}
		if e.Revision == snap.Revision {
			written = append(written, e)
		}
	}
	if snap.Revision > 1 && (len(written) != 1 || !written[0].UpdatedAt.Equal(snap.UpdatedAt)) {
		return fmt.Errorf("snapshot at revision %d updated %v holds %+v from that revision, want one entry updated then",
			snap.Revision, snap.UpdatedAt, written)
	}
	return nil
}

// scribble overwrites b so that any store sharing the memory is caught.
func scribble(b []byte) {
	for i := range b {
		b[i] = '#'
	}
}

func scribbleFlag(f *model.Flag) {
	for i := range f.Allowlist {
		f.Allowlist[i] = "#"
	}
	if p := f.Rollout; p != nil {
		for i := range p.Stages {
			p.Stages[i].Percent = -1
		}
		p.State, p.StartedBy = "#", "#"
	}
}

func scribbleExperiment(e *model.Experiment) {
	for i := range e.Variants {
		e.Variants[i].Name = "#"
		scribble(e.Variants[i].Payload)
	}
}

func scribbleSnapshot(s model.Snapshot) {
	for i := range s.Configs {
		scribble(s.Configs[i].Value)
	}
	for i := range s.Flags {
		scribbleFlag(&s.Flags[i])
	}
	for i := range s.Experiments {
		scribbleExperiment(&s.Experiments[i])
	}
	for i := range s.RateLimits {
		s.RateLimits[i].Burst = 0
	}
	for i := range s.CircuitBreakers {
		s.CircuitBreakers[i].Window = 0
	}
}

// testDeepCopies edits every value handed to or returned by the store and
// checks that the stored state, history and audit log do not change.
func testDeepCopies(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	f := rolloutFlag("f", model.RolloutPaused, 1)
	f.Allowlist = []string{"u1", "u2"}
	e := newExperiment("e", 1, 2)
	putF := w.flag(f)
	putE := w.experiment(e)
	w.rateLimit(newRateLimit("rl", 1, 1))
	w.breaker(newBreaker("cb", 1))
	// A copy, in case the store shares memory with what it returns.
	want := mustSnapshot(t, s, "svc").Clone()

	scribbleFlag(&f)
	scribbleExperiment(&e)
	scribbleFlag(&putF)
	scribbleExperiment(&putE)
	got, err := s.GetFlag(ctx, "svc", "f")
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	scribbleFlag(&got)
	scribbleSnapshot(mustSnapshot(t, s, "svc"))
	scribbleSnapshot(mustGetRevision(t, s, "svc", want.Revision).Snapshot)
	sameSnapshot(t, "state after editing values", mustSnapshot(t, s, "svc"), want)
	sameSnapshot(t, "history after editing values", mustGetRevision(t, s, "svc", want.Revision).Snapshot, want)

	// The rollback removes c and restores rl, so its changes carry both images.
	w.config("c", `[1,2,3]`)
	w.del(model.EntityRateLimit, "rl")
	_, changes, err := s.Rollback(ctx, "svc", want.Revision, store.WriteOptions{Actor: "bob"})
	if err != nil || len(changes) != 2 {
		t.Fatalf("Rollback = %d changes, %v; want 2", len(changes), err)
	}
	events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: maxLimit})
	type images struct{ before, after json.RawMessage }
	saved := make([]images, len(events))
	for i, e := range events {
		saved[i] = images{slices.Clone(e.Before), slices.Clone(e.After)}
	}
	for _, c := range changes {
		scribble(c.Before)
		scribble(c.After)
	}
	for _, e := range events {
		scribble(e.Before)
		scribble(e.After)
	}
	for i, e := range mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: maxLimit}) {
		sameImage(t, fmt.Sprintf("audit event %d before image", e.ID), e.Before, saved[i].before)
		sameImage(t, fmt.Sprintf("audit event %d after image", e.ID), e.After, saved[i].after)
	}
}

func testPing(t *testing.T, s store.Store) {
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
