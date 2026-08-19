package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// testRevisionSnapshots checks that every revision's history entry holds
// exactly the state Snapshot returned right after the write, with its
// actor, time and summary.
func testRevisionSnapshots(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	steps := []struct {
		actor, summary string
		write          func(store.WriteOptions) error
	}{
		{"alice", "create config db", func(o store.WriteOptions) error {
			_, err := s.PutConfig(ctx, "svc", model.Config{Key: "db", Value: json.RawMessage(`{"pool":10,"hosts":["a","b"]}`)}, o)
			return err
		}},
		{"alice", "create flag new-cart", func(o store.WriteOptions) error {
			f := rolloutFlag("new-cart", model.RolloutActive, 0)
			f.Allowlist = []string{"u2", "u1"}
			_, err := s.PutFlag(ctx, "svc", f, o)
			return err
		}},
		{"bob", "create experiment cta", func(o store.WriteOptions) error {
			_, err := s.PutExperiment(ctx, "svc", newExperiment("cta", 50, 50), o)
			return err
		}},
		{"bob", "create rate_limit api", func(o store.WriteOptions) error {
			_, err := s.PutRateLimit(ctx, "svc", newRateLimit("api", 2.5, 10), o)
			return err
		}},
		{"carol", "create circuit_breaker payments", func(o store.WriteOptions) error {
			_, err := s.PutCircuitBreaker(ctx, "svc", newBreaker("payments", 20), o)
			return err
		}},
		{"carol", "update config db", func(o store.WriteOptions) error {
			_, err := s.PutConfig(ctx, "svc", model.Config{Key: "db", Value: json.RawMessage(`{"pool":20}`)}, o)
			return err
		}},
		{"alice", "delete flag new-cart", func(o store.WriteOptions) error {
			_, err := s.DeleteFlag(ctx, "svc", "new-cart", o)
			return err
		}},
		{"bob", "update rate_limit api", func(o store.WriteOptions) error {
			_, err := s.PutRateLimit(ctx, "svc", newRateLimit("api", 5, 10), o)
			return err
		}},
		{"bob", "delete circuit_breaker payments", func(o store.WriteOptions) error {
			_, err := s.DeleteCircuitBreaker(ctx, "svc", "payments", o)
			return err
		}},
		{"carol", "create flag new-cart", func(o store.WriteOptions) error {
			_, err := s.PutFlag(ctx, "svc", newFlag("new-cart", 50), o)
			return err
		}},
	}
	// Indexed by revision.
	snaps := []model.Snapshot{{}, mustSnapshot(t, s, "svc")}
	actors := []string{"", "test"}
	summaries := []string{"", "create namespace"}
	for _, st := range steps {
		if err := st.write(store.WriteOptions{Actor: st.actor}); err != nil {
			t.Fatalf("%s: %v", st.summary, err)
		}
		snaps = append(snaps, mustSnapshot(t, s, "svc"))
		actors = append(actors, st.actor)
		summaries = append(summaries, st.summary)
	}
	latest := int64(len(snaps) - 1)

	revs := mustRevisions(t, s, "svc", 0, 0)
	if int64(len(revs)) != latest {
		t.Fatalf("ListRevisions returned %d entries, want %d", len(revs), latest)
	}
	for i, r := range revs {
		rev := latest - int64(i)
		if r.Namespace != "svc" || r.Revision != rev || r.Actor != actors[rev] || r.Summary != summaries[rev] || !r.CreatedAt.Equal(snaps[rev].UpdatedAt) {
			t.Fatalf("history entry %d: %s rev %d by %q at %v %q, want svc rev %d by %q at %v %q",
				i, r.Namespace, r.Revision, r.Actor, r.CreatedAt, r.Summary, rev, actors[rev], snaps[rev].UpdatedAt, summaries[rev])
		}
		if !reflect.DeepEqual(r.Snapshot, model.Snapshot{}) {
			t.Fatalf("ListRevisions returned the snapshot of revision %d", rev)
		}
	}
	for rev := int64(1); rev <= latest; rev++ {
		r := mustGetRevision(t, s, "svc", rev)
		if r.Namespace != "svc" || r.Revision != rev || r.Actor != actors[rev] || r.Summary != summaries[rev] || !r.CreatedAt.Equal(snaps[rev].UpdatedAt) {
			t.Fatalf("GetRevision(%d): %s rev %d by %q at %v %q, want svc rev %d by %q at %v %q",
				rev, r.Namespace, r.Revision, r.Actor, r.CreatedAt, r.Summary, rev, actors[rev], snaps[rev].UpdatedAt, summaries[rev])
		}
		sameSnapshot(t, fmt.Sprintf("revision %d", rev), r.Snapshot, snaps[rev])
	}
}

// revisionRange describes a listing by its first and last revision.
func revisionRange(revs []model.Revision) string {
	if len(revs) == 0 {
		return "[]"
	}
	return fmt.Sprintf("[%d..%d] (%d)", revs[0].Revision, revs[len(revs)-1].Revision, len(revs))
}

// latestRevision returns the newest history entry of ns.
func latestRevision(t *testing.T, s store.Store, ns string) model.Revision {
	t.Helper()
	revs := mustRevisions(t, s, ns, 0, 1)
	if len(revs) != 1 {
		t.Fatalf("ListRevisions(%q, limit 1) returned %d entries", ns, len(revs))
	}
	return revs[0]
}

func revisionNumbers(revs []model.Revision) []int64 {
	out := make([]int64, len(revs))
	for i, r := range revs {
		out[i] = r.Revision
	}
	return out
}

func testListRevisionsPaging(t *testing.T, s store.Store) {
	ctx := context.Background()
	_, err := s.ListRevisions(ctx, "missing", 0, 0)
	wantErr(t, err, model.ErrNotFound, "ListRevisions of a missing namespace")
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	for i := range 12 {
		w.config(fmt.Sprintf("k%d", i%4), strconv.Itoa(i))
	}
	for _, tt := range []struct {
		before   int64
		limit    int
		from, to int64 // the expected revisions, newest first; none when from is 0
	}{
		{0, 5, 13, 9},
		{9, 5, 8, 4},
		{4, 5, 3, 1},
		{1, 5, 0, 0},
		{13, 1, 12, 12},
		{100, 3, 13, 11},
		{0, 0, 13, 1},
		{0, -1, 13, 1},
	} {
		var want []int64
		for r := tt.from; r >= tt.to && r > 0; r-- {
			want = append(want, r)
		}
		if got := revisionNumbers(mustRevisions(t, s, "svc", tt.before, tt.limit)); !slices.Equal(got, want) {
			t.Errorf("ListRevisions(before %d, limit %d) = %v, want %v", tt.before, tt.limit, got, want)
		}
	}
	for _, tt := range []struct {
		ns  string
		rev int64
	}{{"missing", 1}, {"svc", 0}, {"svc", -1}, {"svc", 14}} {
		_, err := s.GetRevision(ctx, tt.ns, tt.rev)
		wantErr(t, err, model.ErrNotFound, fmt.Sprintf("GetRevision(%q, %d)", tt.ns, tt.rev))
	}
}

// testHistoryLimits checks the default and maximum page sizes of both
// listings, which needs more than maxLimit revisions.
func testHistoryLimits(t *testing.T, s store.Store) {
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	const writes = maxLimit + 5
	for i := range writes {
		w.config("counter", strconv.Itoa(i))
	}
	const latest = writes + 1
	for _, tt := range []struct{ limit, want int }{
		{0, defaultLimit},
		{-1, defaultLimit},
		{1, 1},
		{defaultLimit + 1, defaultLimit + 1},
		{maxLimit, maxLimit},
		{maxLimit + 1, maxLimit},
		{10 * maxLimit, maxLimit},
	} {
		revs := mustRevisions(t, s, "svc", 0, tt.limit)
		if len(revs) != tt.want || revs[0].Revision != latest || revs[len(revs)-1].Revision != int64(latest-tt.want+1) {
			t.Errorf("ListRevisions(limit %d) = revisions %v, want the newest %d", tt.limit, revisionRange(revs), tt.want)
		}
		events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: tt.limit})
		if len(events) != tt.want || events[0].Revision != latest {
			t.Errorf("ListAuditEvents(limit %d) = %d events, want the newest %d", tt.limit, len(events), tt.want)
		}
	}
	if n := len(mustAudit(t, s, model.AuditFilter{})); n != defaultLimit {
		t.Errorf("ListAuditEvents without a filter = %d events, want %d", n, defaultLimit)
	}
}

// images maps "<entity> <key>" to the image of every entry in s.
func images(t *testing.T, s model.Snapshot) map[string]json.RawMessage {
	t.Helper()
	out := make(map[string]json.RawMessage)
	for _, v := range s.Configs {
		out[model.EntityConfig+" "+v.Key] = encode(t, v)
	}
	for _, v := range s.Flags {
		out[model.EntityFlag+" "+v.Key] = encode(t, v)
	}
	for _, v := range s.Experiments {
		out[model.EntityExperiment+" "+v.Key] = encode(t, v)
	}
	for _, v := range s.RateLimits {
		out[model.EntityRateLimit+" "+v.Key] = encode(t, v)
	}
	for _, v := range s.CircuitBreakers {
		out[model.EntityCircuitBreaker+" "+v.Key] = encode(t, v)
	}
	return out
}

// testRollback rolls back additions, modifications and deletions of every
// entity type in one revision and checks what it returns and records.
func testRollback(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	w.config("keep", `1`)                               // 2, never changed again
	w.config("modified", `{"v":1}`)                     // 3
	w.config("deleted", `true`)                         // 4
	w.flag(rolloutFlag("flag", model.RolloutActive, 1)) // 5
	w.experiment(newExperiment("exp", 50, 50))          // 6
	w.rateLimit(newRateLimit("rl", 10, 20))             // 7
	w.breaker(newBreaker("cb", 20))                     // 8
	const target = 8
	targetSnap := mustSnapshot(t, s, "svc")

	w = w.as("bob")
	w.config("modified", `{"v":2}`)                      // 9
	w.del(model.EntityConfig, "deleted")                 // 10
	w.config("added", `"new"`)                           // 11
	w.flag(rolloutFlag("flag", model.RolloutAborted, 1)) // 12
	w.flag(newFlag("flag2", 50))                         // 13
	w.experiment(newExperiment("exp", 10, 90))           // 14
	w.experiment(newExperiment("exp2", 1))               // 15
	w.del(model.EntityRateLimit, "rl")                   // 16
	w.rateLimit(newRateLimit("rl2", 1, 1))               // 17
	w.breaker(newBreaker("cb", 50))                      // 18
	w.breaker(newBreaker("cb2", 5))                      // 19
	before := mustSnapshot(t, s, "svc")

	// opts.Action does not apply: rollback events always say rollback.
	rev, changes, err := s.Rollback(ctx, "svc", target, store.WriteOptions{Actor: "carol", ExpectedRevision: 19, Action: model.ActionUpdate})
	if err != nil || rev != 20 {
		t.Fatalf("Rollback = revision %d, %v; want 20", rev, err)
	}
	after := mustSnapshot(t, s, "svc")
	at := after.UpdatedAt
	if after.Revision != 20 {
		t.Fatalf("namespace at revision %d after rollback, want 20", after.Revision)
	}

	diff, err := model.Diff(before, model.PrepareRollback(mustGetRevision(t, s, "svc", target).Snapshot))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"removed config added", "added config deleted", "modified config modified",
		"modified flag flag", "removed flag flag2",
		"modified experiment exp", "removed experiment exp2",
		"added rate_limit rl", "removed rate_limit rl2",
		"modified circuit_breaker cb", "removed circuit_breaker cb2",
	}
	if got := describe(changes); !slices.Equal(got, want) || !slices.Equal(got, describe(diff)) {
		t.Fatalf("changes = %v\nwant %v (model.Diff order: %v)", got, want, describe(diff))
	}
	beforeImages, afterImages := images(t, before), images(t, after)
	for _, c := range changes {
		id := c.EntityType + " " + c.Key
		sameImage(t, id+" before", c.Before, beforeImages[id])
		sameImage(t, id+" after", c.After, afterImages[id])
		if c.After == nil {
			continue
		}
		if m := decode[entryMetadata](t, c.After); m.Revision != 20 || !m.UpdatedAt.Equal(at) || m.UpdatedBy != "carol" {
			t.Fatalf("restored %s: revision %d updated %v by %q, want 20 at %v by carol", id, m.Revision, m.UpdatedAt, m.UpdatedBy, at)
		}
	}

	// The content is the target's, its active rollout paused, and the
	// untouched entry keeps its revision.
	if d, err := model.Diff(model.PrepareRollback(targetSnap), after); err != nil || len(d) != 0 {
		t.Fatalf("state after rollback differs from the target: %v, %v", describe(d), err)
	}
	if keep := after.Configs[slices.IndexFunc(after.Configs, func(c model.Config) bool { return c.Key == "keep" })]; keep.Revision != 2 {
		t.Fatalf("untouched config rewritten at revision %d", keep.Revision)
	}
	f, err := s.GetFlag(ctx, "svc", "flag")
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	checkFlag(t, "restored flag", f, rolloutFlag("flag", model.RolloutPaused, 1))
	if refs, err := s.ListActiveRollouts(ctx); err != nil || len(refs) != 0 {
		t.Fatalf("ListActiveRollouts = %+v, %v; want none", refs, err)
	}

	// One audit event per change with the same images, and one history entry.
	events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: len(changes) + 1})
	if len(events) != len(changes)+1 || events[len(changes)].Revision != 19 {
		t.Fatalf("revision 20 has %d audit events, want %d", len(events)-1, len(changes))
	}
	byEntry := make(map[string]model.AuditEvent)
	for _, e := range events[:len(changes)] {
		if e.Revision != 20 || e.Action != model.ActionRollback || e.Actor != "carol" || !e.Time.Equal(at) || e.Message != "rollback to revision 8" {
			t.Fatalf("audit event: rev %d %s by %q at %v (message %q), want rev 20 rollback by carol at %v (message %q)",
				e.Revision, e.Action, e.Actor, e.Time, e.Message, at, "rollback to revision 8")
		}
		byEntry[e.EntityType+" "+e.EntityKey] = e
	}
	for _, c := range changes {
		id := c.EntityType + " " + c.Key
		e, ok := byEntry[id]
		if !ok {
			t.Fatalf("no audit event for %s", id)
		}
		sameImage(t, id+" audit before image", e.Before, c.Before)
		sameImage(t, id+" audit after image", e.After, c.After)
	}
	if r := latestRevision(t, s, "svc"); r.Revision != 20 || r.Actor != "carol" || r.Summary != "rollback to revision 8" || !r.CreatedAt.Equal(at) {
		t.Fatalf("history entry: rev %d by %q at %v %q, want rev 20 by carol at %v %q", r.Revision, r.Actor, r.CreatedAt, r.Summary, at, "rollback to revision 8")
	}
	sameSnapshot(t, "revision 20", mustGetRevision(t, s, "svc", 20).Snapshot, after)

	// Rolling forward again is another rollback, here with a message.
	rev, changes, err = s.Rollback(ctx, "svc", 19, store.WriteOptions{Actor: "dave", Message: "undo the rollback"})
	if err != nil || rev != 21 || len(changes) != len(want) {
		t.Fatalf("Rollback to 19 = revision %d with %d changes, %v; want 21 with %d", rev, len(changes), err, len(want))
	}
	if d, err := model.Diff(before, mustSnapshot(t, s, "svc")); err != nil || len(d) != 0 {
		t.Fatalf("state differs from revision 19: %v, %v", describe(d), err)
	}
	for _, e := range mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: len(changes)}) {
		if e.Revision != 21 || e.Action != model.ActionRollback || e.Actor != "dave" || e.Message != "undo the rollback" {
			t.Fatalf("audit event: rev %d %s by %q (message %q), want rev 21 rollback by dave (message %q)",
				e.Revision, e.Action, e.Actor, e.Message, "undo the rollback")
		}
	}
	if r := latestRevision(t, s, "svc"); r.Revision != 21 || r.Actor != "dave" || r.Summary != "undo the rollback" {
		t.Fatalf("history entry: rev %d by %q %q, want rev 21 by dave %q", r.Revision, r.Actor, r.Summary, "undo the rollback")
	}
}

func testRollbackNoop(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	w.flag(rolloutFlag("done", model.RolloutCompleted, 2)) // 2
	w.config("c", `{"a":1,"b":2}`)                         // 3
	w.config("c", `2`)                                     // 4
	w.as("bob").config("c", `{"b":2,"a":1}`)               // 5: the content of 3, written by someone else
	cp := takeCheckpoint(t, s, "svc")
	for _, tt := range []struct{ to, expected int64 }{{3, 0}, {5, 0}, {5, 5}} {
		rev, changes, err := s.Rollback(ctx, "svc", tt.to, store.WriteOptions{Actor: "carol", ExpectedRevision: tt.expected})
		if err != nil || rev != 5 || len(changes) != 0 {
			t.Fatalf("Rollback(to %d, expected %d) = %d, %v, %v; want 5 and no changes", tt.to, tt.expected, rev, describe(changes), err)
		}
		unchanged(t, s, "svc", cp, fmt.Sprintf("no-op rollback to %d", tt.to))
	}
}

func testRollbackPreconditions(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	w.config("c", `1`) // 2
	w.config("c", `2`) // 3
	cp := takeCheckpoint(t, s, "svc")
	for _, tt := range []struct {
		name         string
		ns           string
		to, expected int64
		target       error
	}{
		{"a stale expected revision", "svc", 2, 2, model.ErrConflict},
		{"a future expected revision", "svc", 2, 4, model.ErrConflict},
		{"revision 0", "svc", 0, 0, model.ErrNotFound},
		{"a negative revision", "svc", -1, 0, model.ErrNotFound},
		{"a future revision", "svc", 4, 0, model.ErrNotFound},
		{"a missing namespace", "missing", 1, 0, model.ErrNotFound},
		{"a missing namespace and an expected revision", "missing", 1, 3, model.ErrNotFound},
	} {
		_, _, err := s.Rollback(ctx, tt.ns, tt.to, store.WriteOptions{Actor: "eve", ExpectedRevision: tt.expected})
		wantErr(t, err, tt.target, "Rollback with "+tt.name)
		unchanged(t, s, "svc", cp, "Rollback with "+tt.name)
	}
	rev, changes, err := s.Rollback(ctx, "svc", 2, store.WriteOptions{Actor: "bob", ExpectedRevision: 3})
	if err != nil || rev != 4 || !slices.Equal(describe(changes), []string{"modified config c"}) {
		t.Fatalf("Rollback at the expected revision = %d, %v, %v; want revision 4 modifying config c", rev, describe(changes), err)
	}
}

// testConcurrentRollback races rollbacks against writes. A rollback must hold
// the writers' lock from its ExpectedRevision check to its own write, so one
// that succeeds wrote the revision right after the one it expected, returned
// what differed from the target just before its revision, and left exactly
// the target's content, also when a racing write changed an entry the
// target agreed with.
func testConcurrentRollback(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	w.config("steady", `{"v":0}`)
	target := w.config("drift", `0`).Revision
	want := model.PrepareRollback(mustGetRevision(t, s, "svc", target).Snapshot)

	const rounds = 60
	for i := range rounds {
		// The drift gives the rollback work in every round. The racing write
		// adds an entry the target lacks, which a rollback applied after it
		// must remove. Every other round the rollback is unconditional.
		expected := w.config("drift", strconv.Itoa(i+1)).Revision
		if i%2 == 1 {
			expected = 0
		}
		var (
			race    sync.WaitGroup
			raceErr error
		)
		race.Go(func() {
			_, raceErr = s.PutConfig(ctx, "svc", model.Config{Key: fmt.Sprintf("raced-%d", i), Value: json.RawMessage(`true`)},
				store.WriteOptions{Actor: "bob"})
		})
		rev, changes, err := s.Rollback(ctx, "svc", target, store.WriteOptions{Actor: "carol", ExpectedRevision: expected})
		race.Wait()
		if raceErr != nil {
			t.Fatalf("round %d: racing PutConfig: %v", i, raceErr)
		}
		if expected != 0 && errors.Is(err, model.ErrConflict) {
			continue // the racing write came first
		}
		if err != nil {
			t.Fatalf("round %d: Rollback(expected revision %d): %v", i, expected, err)
		}
		if expected != 0 && rev != expected+1 {
			t.Fatalf("round %d: Rollback expecting revision %d wrote revision %d", i, expected, rev)
		}
		diff, err := model.Diff(mustGetRevision(t, s, "svc", rev-1).Snapshot, want)
		if err != nil {
			t.Fatal(err)
		}
		if got := describe(changes); !slices.Equal(got, describe(diff)) {
			t.Fatalf("round %d: Rollback wrote revision %d with changes %v, but revision %d differed from the target by %v",
				i, rev, got, rev-1, describe(diff))
		}
		if d, err := model.Diff(want, mustGetRevision(t, s, "svc", rev).Snapshot); err != nil || len(d) != 0 {
			t.Fatalf("round %d: revision %d written by the rollback differs from the target: %v, %v", i, rev, describe(d), err)
		}
	}
}

func testRollbackPausesActiveRollouts(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	w := newWriter(t, s, "svc", "alice")
	w.flag(rolloutFlag("f", model.RolloutActive, 0)) // 2
	w.flag(rolloutFlag("f", model.RolloutActive, 1)) // 3

	checkRestored := func(to, wantRev int64, want model.Flag) {
		t.Helper()
		rev, changes, err := s.Rollback(ctx, "svc", to, store.WriteOptions{Actor: "bob"})
		if err != nil || rev != wantRev || !slices.Equal(describe(changes), []string{"modified flag f"}) {
			t.Fatalf("Rollback to %d = %d, %v, %v; want revision %d modifying flag f", to, rev, describe(changes), err, wantRev)
		}
		got, err := s.GetFlag(ctx, "svc", "f")
		if err != nil {
			t.Fatalf("GetFlag: %v", err)
		}
		checkFlag(t, fmt.Sprintf("flag after rollback to %d", to), got, want)
		if got.Revision != wantRev || got.UpdatedBy != "bob" {
			t.Fatalf("restored flag at revision %d by %q, want %d by bob", got.Revision, got.UpdatedBy, wantRev)
		}
		if refs, err := s.ListActiveRollouts(ctx); err != nil || len(refs) != 0 {
			t.Fatalf("ListActiveRollouts = %+v, %v; want none", refs, err)
		}
	}
	checkRestored(2, 4, rolloutFlag("f", model.RolloutPaused, 0))

	// Rolling back to the current revision pauses a plan that is active now,
	// and the history still shows it active.
	w.flag(rolloutFlag("f", model.RolloutActive, 1)) // 5
	checkRestored(5, 6, rolloutFlag("f", model.RolloutPaused, 1))
	checkFlag(t, "revision 5", mustGetRevision(t, s, "svc", 5).Snapshot.Flags[0], rolloutFlag("f", model.RolloutActive, 1))
}

// testRollbackExactNumbers rolls back between two configs that differ only in
// a number beyond float64 precision: they must count as different content,
// and the restored value must be the target's digits, not a rounded copy.
func testRollbackExactNumbers(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	const first, second = "9007199254740993", "9007199254740992" // 2^53+1 and 2^53
	w := newWriter(t, s, "svc", "alice")
	w.config("big", `{"id":`+first+`}`)  // 2
	w.config("big", `{"id":`+second+`}`) // 3

	// idOf returns the digits of the config's "id", which must survive
	// untouched.
	idOf := func(raw json.RawMessage) string {
		t.Helper()
		var v struct {
			ID json.Number `json:"id"`
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		return v.ID.String()
	}
	restore := func(to, wantRev int64, wantID string) {
		t.Helper()
		rev, changes, err := s.Rollback(ctx, "svc", to, store.WriteOptions{Actor: "bob"})
		if err != nil || rev != wantRev || !slices.Equal(describe(changes), []string{"modified config big"}) {
			t.Fatalf("Rollback to %d = %d, %v, %v; want revision %d modifying config big", to, rev, describe(changes), err, wantRev)
		}
		if got := idOf(decode[model.Config](t, changes[0].After).Value); got != wantID {
			t.Fatalf("Rollback to %d returned id %s, want %s", to, got, wantID)
		}
		if got := idOf(decode[model.Config](t, changes[0].Before).Value); got == wantID {
			t.Fatalf("Rollback to %d returned the same id %s before and after", to, got)
		}
		snap := mustSnapshot(t, s, "svc")
		if got := idOf(snap.Configs[0].Value); got != wantID || snap.Revision != wantRev {
			t.Fatalf("after Rollback to %d: id %s at revision %d, want %s at %d", to, got, snap.Revision, wantID, wantRev)
		}
		if got := idOf(mustGetRevision(t, s, "svc", wantRev).Snapshot.Configs[0].Value); got != wantID {
			t.Fatalf("revision %d holds id %s, want %s", wantRev, got, wantID)
		}
		events := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: 1})
		if got := idOf(decode[model.Config](t, events[0].After).Value); got != wantID {
			t.Fatalf("audit after image holds id %s, want %s", got, wantID)
		}
	}
	restore(2, 4, first)
	restore(3, 5, second)

	// The target the namespace already matches is a no-op, however close
	// its number is to the current one.
	rev, changes, err := s.Rollback(ctx, "svc", 3, store.WriteOptions{Actor: "bob"})
	if err != nil || rev != 5 || len(changes) != 0 {
		t.Fatalf("Rollback to the current content = %d, %v, %v; want 5 and no changes", rev, describe(changes), err)
	}
}
