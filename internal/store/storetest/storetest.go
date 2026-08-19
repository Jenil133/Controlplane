// Package storetest is a conformance suite every store.Store implementation
// must pass. Each subtest gets a fresh, empty store from the factory.
//
// The suite is the executable form of the store contract: what every write
// returns and records (revision history and audit log), optimistic
// concurrency, rollback, consistency under concurrent writers, and the
// listing, filtering and paging rules.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// Factory returns an empty store. Cleanup should be registered on t.
type Factory func(t *testing.T) store.Store

// Paging limits of the store contract.
const (
	defaultLimit = 50
	maxLimit     = 500
)

// Run executes the suite.
func Run(t *testing.T, newStore Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, store.Store)
	}{
		{"Namespaces", testNamespaces},
		{"ConfigLifecycle", testConfigLifecycle},
		{"FlagLifecycle", testFlagLifecycle},
		{"ExperimentLifecycle", testExperimentLifecycle},
		{"MissingNamespace", testMissingNamespace},
		{"SnapshotSortedByKey", testSnapshotSorted},
		{"NamespaceIsolation", testNamespaceIsolation},
		{"ConcurrentWritesGetUniqueRevisions", testConcurrentWrites},
		{"ReturnedValuesAreCopies", testReturnedValuesAreCopies},

		{"NamespaceCreationIsRecorded", testNamespaceRecords},
		{"EntityWrites", testEntityWrites},
		{"ActionAndMessage", testActionAndMessage},
		{"PoliciesRoundTrip", testPoliciesRoundTrip},
		{"FlagFieldsRoundTrip", testFlagFields},
		{"GetFlag", testGetFlag},
		{"SnapshotListsSortedByteWise", testSnapshotByteOrder},
		{"RevisionSnapshots", testRevisionSnapshots},
		{"ListRevisionsPaging", testListRevisionsPaging},
		{"HistoryLimits", testHistoryLimits},
		{"Rollback", testRollback},
		{"RollbackNoop", testRollbackNoop},
		{"RollbackPreconditions", testRollbackPreconditions},
		{"RollbackKeepsExactNumbers", testRollbackExactNumbers},
		{"RollbackPausesActiveRollouts", testRollbackPausesActiveRollouts},
		{"AuditFilters", testAuditFilters},
		{"AuditPaging", testAuditPaging},
		{"ListActiveRollouts", testListActiveRollouts},
		{"ConcurrentCASExactlyOneWins", testConcurrentCAS},
		{"ConcurrentWritesKeepHistoryConsistent", testConcurrentHistory},
		{"ConcurrentSnapshotsAreConsistent", testConcurrentSnapshots},
		{"ConcurrentRollbacksHoldTheWritersLock", testConcurrentRollback},
		{"DeepCopies", testDeepCopies},
		{"Ping", testPing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, newStore(t))
		})
	}
}

func mustCreate(t *testing.T, s store.Store, name string) model.Namespace {
	t.Helper()
	ns, err := s.CreateNamespace(context.Background(), model.Namespace{Name: name, Description: "test " + name}, store.WriteOptions{Actor: "test"})
	if err != nil {
		t.Fatalf("CreateNamespace(%q): %v", name, err)
	}
	return ns
}

func mustSnapshot(t *testing.T, s store.Store, name string) model.Snapshot {
	t.Helper()
	snap, err := s.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatalf("Snapshot(%q): %v", name, err)
	}
	return snap
}

func wantErr(t *testing.T, err, target error, what string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("%s: got error %v, want %v", what, err, target)
	}
}

// jsonEqual compares JSON semantically; backends may reformat documents.
func jsonEqual(t *testing.T, got, want json.RawMessage) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got invalid JSON %s: %v", got, err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want invalid JSON %s: %v", want, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("JSON mismatch:\n got  %s\n want %s", got, want)
	}
}

func mustRevision(t *testing.T, s store.Store, ns string) int64 {
	t.Helper()
	n, err := s.GetNamespace(context.Background(), ns)
	if err != nil {
		t.Fatalf("GetNamespace(%q): %v", ns, err)
	}
	return n.Revision
}

func mustRevisions(t *testing.T, s store.Store, ns string, before int64, limit int) []model.Revision {
	t.Helper()
	revs, err := s.ListRevisions(context.Background(), ns, before, limit)
	if err != nil {
		t.Fatalf("ListRevisions(%q, %d, %d): %v", ns, before, limit, err)
	}
	return revs
}

func mustGetRevision(t *testing.T, s store.Store, ns string, rev int64) model.Revision {
	t.Helper()
	r, err := s.GetRevision(context.Background(), ns, rev)
	if err != nil {
		t.Fatalf("GetRevision(%q, %d): %v", ns, rev, err)
	}
	return r
}

func mustAudit(t *testing.T, s store.Store, f model.AuditFilter) []model.AuditEvent {
	t.Helper()
	events, err := s.ListAuditEvents(context.Background(), f)
	if err != nil {
		t.Fatalf("ListAuditEvents(%+v): %v", f, err)
	}
	return events
}

func encode(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := model.EncodeEntry(v)
	if err != nil {
		t.Fatalf("encode %+v: %v", v, err)
	}
	return b
}

func decode[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

// sameImage compares two entity images (audit or change): both absent, or
// semantically equal JSON, since backends may reformat documents.
func sameImage(t *testing.T, what string, got, want json.RawMessage) {
	t.Helper()
	if got == nil || want == nil {
		if got != nil || want != nil {
			t.Fatalf("%s: image %s, want %s", what, got, want)
		}
		return
	}
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: invalid JSON %s: %v", what, got, err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("%s: invalid JSON %s: %v", what, want, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("%s: JSON mismatch:\n got  %s\n want %s", what, got, want)
	}
}

// sameValue compares two values by their storage encoding, metadata
// included. Timestamps compare as UTC strings, so it also checks that the
// store returns UTC.
func sameValue(t *testing.T, what string, got, want any) {
	t.Helper()
	sameImage(t, what, encode(t, got), encode(t, want))
}

// requireUTC fails unless every time is in UTC, which the store hands out
// for every timestamp it sets.
func requireUTC(t *testing.T, what string, times ...time.Time) {
	t.Helper()
	for _, tm := range times {
		if tm.Location() != time.UTC {
			t.Fatalf("%s: time %v is not in UTC", what, tm)
		}
	}
}

// entryMetadata is the revision metadata in every entity image.
type entryMetadata struct {
	Revision  int64     `json:"revision"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

// sameSnapshot fails unless got is the namespace state want describes: same
// revision and update time, no content difference (model.Diff) and the same
// entries in the same order with the same revision metadata, which Diff
// ignores.
func sameSnapshot(t *testing.T, what string, got, want model.Snapshot) {
	t.Helper()
	if got.Namespace != want.Namespace || got.Revision != want.Revision || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("%s: snapshot of %q at revision %d updated %v, want %q at %d updated %v",
			what, got.Namespace, got.Revision, got.UpdatedAt, want.Namespace, want.Revision, want.UpdatedAt)
	}
	changes, err := model.Diff(want, got)
	if err != nil {
		t.Fatalf("%s: Diff: %v", what, err)
	}
	if len(changes) != 0 {
		t.Fatalf("%s: snapshots differ: %v", what, describe(changes))
	}
	if g, w := entryMeta(got), entryMeta(want); !slices.Equal(g, w) {
		t.Fatalf("%s: entries differ:\n got  %v\n want %v", what, g, w)
	}
	requireUTC(t, what, got.UpdatedAt)
	requireUTC(t, what, updateTimes(got)...)
}

// entry is one entry of a snapshot with its revision metadata.
type entry struct {
	entity, key string
	entryMetadata
}

// entries lists every entry of s in snapshot order.
func entries(s model.Snapshot) []entry {
	var out []entry
	add := func(entity, key string, rev int64, at time.Time, by string) {
		out = append(out, entry{entity, key, entryMetadata{rev, at, by}})
	}
	for _, v := range s.Configs {
		add(model.EntityConfig, v.Key, v.Revision, v.UpdatedAt, v.UpdatedBy)
	}
	for _, v := range s.Flags {
		add(model.EntityFlag, v.Key, v.Revision, v.UpdatedAt, v.UpdatedBy)
	}
	for _, v := range s.Experiments {
		add(model.EntityExperiment, v.Key, v.Revision, v.UpdatedAt, v.UpdatedBy)
	}
	for _, v := range s.RateLimits {
		add(model.EntityRateLimit, v.Key, v.Revision, v.UpdatedAt, v.UpdatedBy)
	}
	for _, v := range s.CircuitBreakers {
		add(model.EntityCircuitBreaker, v.Key, v.Revision, v.UpdatedAt, v.UpdatedBy)
	}
	return out
}

// updateTimes returns the UpdatedAt of every entry in s.
func updateTimes(s model.Snapshot) []time.Time {
	var out []time.Time
	for _, e := range entries(s) {
		out = append(out, e.UpdatedAt)
	}
	return out
}

// entryMeta lists the snapshot's entries with their revision metadata.
func entryMeta(s model.Snapshot) []string {
	var out []string
	for _, e := range entries(s) {
		out = append(out, fmt.Sprintf("%s/%s@%d %s by %q", e.entity, e.key, e.Revision, e.UpdatedAt.UTC().Format(time.RFC3339Nano), e.UpdatedBy))
	}
	return out
}

func describe(changes []model.Change) []string {
	out := make([]string, len(changes))
	for i, c := range changes {
		out[i] = fmt.Sprintf("%s %s %s", c.Type, c.EntityType, c.Key)
	}
	return out
}

// checkpoint is what a failed write must leave untouched.
type checkpoint struct {
	snapshot  model.Snapshot
	revisions []model.Revision
	audit     []model.AuditEvent
}

func takeCheckpoint(t *testing.T, s store.Store, ns string) checkpoint {
	t.Helper()
	return checkpoint{
		snapshot:  mustSnapshot(t, s, ns),
		revisions: mustRevisions(t, s, ns, 0, maxLimit),
		audit:     mustAudit(t, s, model.AuditFilter{Limit: maxLimit}),
	}
}

// unchanged fails if the namespace state, its history or the audit log (of
// every namespace) changed since c was taken.
func unchanged(t *testing.T, s store.Store, ns string, c checkpoint, after string) {
	t.Helper()
	now := takeCheckpoint(t, s, ns)
	sameSnapshot(t, after, now.snapshot, c.snapshot)
	if !slices.EqualFunc(now.revisions, c.revisions, func(a, b model.Revision) bool { return a.Revision == b.Revision }) {
		t.Fatalf("%s changed the history: %d revisions, want %d", after, len(now.revisions), len(c.revisions))
	}
	if !slices.Equal(ids(now.audit), ids(c.audit)) {
		t.Fatalf("%s changed the audit log: events %v, want %v", after, ids(now.audit), ids(c.audit))
	}
}

func ids(events []model.AuditEvent) []int64 {
	out := make([]int64, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func testNamespaces(t *testing.T, s store.Store) {
	ctx := context.Background()

	b := mustCreate(t, s, "b-service/prod")
	if b.Revision != 1 {
		t.Fatalf("new namespace revision = %d, want 1", b.Revision)
	}
	if b.CreatedAt.IsZero() || b.UpdatedAt.IsZero() {
		t.Fatalf("timestamps not set: %+v", b)
	}
	mustCreate(t, s, "a-service/prod")

	_, err := s.CreateNamespace(ctx, model.Namespace{Name: "b-service/prod"}, store.WriteOptions{})
	wantErr(t, err, model.ErrAlreadyExists, "duplicate CreateNamespace")

	got, err := s.GetNamespace(ctx, "b-service/prod")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	if got.Name != b.Name || got.Description != b.Description || got.Revision != 1 {
		t.Fatalf("GetNamespace = %+v, want %+v", got, b)
	}

	_, err = s.GetNamespace(ctx, "missing")
	wantErr(t, err, model.ErrNotFound, "GetNamespace(missing)")

	list, err := s.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if len(list) != 2 || list[0].Name != "a-service/prod" || list[1].Name != "b-service/prod" {
		t.Fatalf("ListNamespaces = %+v, want [a-service/prod b-service/prod]", list)
	}
}

func testConfigLifecycle(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")

	c, err := s.PutConfig(ctx, "svc", model.Config{
		Key: "db.pool", Value: json.RawMessage(`{"max":10,"idle":2}`), Description: "pool",
	}, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if c.Revision != 2 || c.UpdatedBy != "alice" || c.UpdatedAt.IsZero() {
		t.Fatalf("PutConfig = %+v, want revision 2 by alice", c)
	}
	jsonEqual(t, c.Value, json.RawMessage(`{"idle":2,"max":10}`))

	c, err = s.PutConfig(ctx, "svc", model.Config{Key: "db.pool", Value: json.RawMessage(`{"max":20}`)}, store.WriteOptions{Actor: "bob"})
	if err != nil {
		t.Fatalf("PutConfig update: %v", err)
	}
	if c.Revision != 3 || c.UpdatedBy != "bob" {
		t.Fatalf("updated config = %+v, want revision 3 by bob", c)
	}

	snap := mustSnapshot(t, s, "svc")
	if snap.Revision != 3 || len(snap.Configs) != 1 {
		t.Fatalf("snapshot = rev %d with %d configs, want rev 3 with 1", snap.Revision, len(snap.Configs))
	}
	got := snap.Configs[0]
	if got.Key != "db.pool" || got.Revision != 3 || got.UpdatedBy != "bob" || got.Description != "" || !got.UpdatedAt.Equal(c.UpdatedAt) {
		t.Fatalf("snapshot config = %+v, want %+v", got, c)
	}
	jsonEqual(t, got.Value, json.RawMessage(`{"max":20}`))

	rev, err := s.DeleteConfig(ctx, "svc", "db.pool", store.WriteOptions{})
	if err != nil {
		t.Fatalf("DeleteConfig: %v", err)
	}
	if rev != 4 {
		t.Fatalf("DeleteConfig revision = %d, want 4", rev)
	}
	_, err = s.DeleteConfig(ctx, "svc", "db.pool", store.WriteOptions{})
	wantErr(t, err, model.ErrNotFound, "DeleteConfig twice")

	snap = mustSnapshot(t, s, "svc")
	if snap.Revision != 4 || len(snap.Configs) != 0 {
		t.Fatalf("after delete: rev %d with %d configs, want rev 4 with 0 (failed delete must not bump)", snap.Revision, len(snap.Configs))
	}
}

func testFlagLifecycle(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")

	f, err := s.PutFlag(ctx, "svc", model.Flag{Key: "new-checkout", Enabled: true, Description: "d"}, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutFlag: %v", err)
	}
	if f.Revision != 2 || !f.Enabled {
		t.Fatalf("PutFlag = %+v", f)
	}
	snap := mustSnapshot(t, s, "svc")
	if len(snap.Flags) != 1 {
		t.Fatalf("snapshot flags = %+v", snap.Flags)
	}
	got := snap.Flags[0]
	if got.Key != f.Key || got.Enabled != f.Enabled || got.Description != f.Description ||
		got.Revision != f.Revision || got.UpdatedBy != f.UpdatedBy || !got.UpdatedAt.Equal(f.UpdatedAt) {
		t.Fatalf("snapshot flag = %+v, want %+v", got, f)
	}

	rev, err := s.DeleteFlag(ctx, "svc", "new-checkout", store.WriteOptions{})
	if err != nil || rev != 3 {
		t.Fatalf("DeleteFlag = %d, %v; want 3, nil", rev, err)
	}
	_, err = s.DeleteFlag(ctx, "svc", "new-checkout", store.WriteOptions{})
	wantErr(t, err, model.ErrNotFound, "DeleteFlag twice")
}

func testExperimentLifecycle(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")

	in := model.Experiment{
		Key:     "button-color",
		Enabled: true,
		Salt:    "button-color-v2",
		Variants: []model.Variant{
			{Name: "control", Weight: 70},
			{Name: "green", Weight: 30, Payload: json.RawMessage(`{"color":"#0f0"}`)},
		},
	}
	e, err := s.PutExperiment(ctx, "svc", in, store.WriteOptions{Actor: "alice"})
	if err != nil {
		t.Fatalf("PutExperiment: %v", err)
	}
	if e.Revision != 2 || e.Salt != "button-color-v2" || len(e.Variants) != 2 {
		t.Fatalf("PutExperiment = %+v", e)
	}

	snap := mustSnapshot(t, s, "svc")
	if len(snap.Experiments) != 1 {
		t.Fatalf("snapshot experiments = %+v", snap.Experiments)
	}
	got := snap.Experiments[0]
	if got.Key != in.Key || !got.Enabled || got.Salt != in.Salt || got.Revision != 2 || got.UpdatedBy != "alice" {
		t.Fatalf("snapshot experiment = %+v", got)
	}
	if len(got.Variants) != 2 || got.Variants[0].Name != "control" || got.Variants[0].Weight != 70 ||
		len(got.Variants[0].Payload) != 0 || got.Variants[1].Name != "green" || got.Variants[1].Weight != 30 {
		t.Fatalf("variants = %+v", got.Variants)
	}
	jsonEqual(t, got.Variants[1].Payload, json.RawMessage(`{"color":"#0f0"}`))

	rev, err := s.DeleteExperiment(ctx, "svc", "button-color", store.WriteOptions{})
	if err != nil || rev != 3 {
		t.Fatalf("DeleteExperiment = %d, %v; want 3, nil", rev, err)
	}
	_, err = s.DeleteExperiment(ctx, "svc", "button-color", store.WriteOptions{})
	wantErr(t, err, model.ErrNotFound, "DeleteExperiment twice")
}

func testMissingNamespace(t *testing.T, s store.Store) {
	ctx := context.Background()
	_, err := s.PutConfig(ctx, "nope", model.Config{Key: "k", Value: json.RawMessage(`1`)}, store.WriteOptions{Actor: "a"})
	wantErr(t, err, model.ErrNotFound, "PutConfig")
	_, err = s.PutFlag(ctx, "nope", model.Flag{Key: "k"}, store.WriteOptions{Actor: "a"})
	wantErr(t, err, model.ErrNotFound, "PutFlag")
	_, err = s.PutExperiment(ctx, "nope", model.Experiment{Key: "k", Salt: "k", Variants: []model.Variant{{Name: "a", Weight: 1}}}, store.WriteOptions{Actor: "a"})
	wantErr(t, err, model.ErrNotFound, "PutExperiment")
	_, err = s.DeleteConfig(ctx, "nope", "k", store.WriteOptions{})
	wantErr(t, err, model.ErrNotFound, "DeleteConfig")
	_, err = s.Snapshot(ctx, "nope")
	wantErr(t, err, model.ErrNotFound, "Snapshot")
}

func testSnapshotSorted(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	for _, k := range []string{"c", "a", "b"} {
		if _, err := s.PutConfig(ctx, "svc", model.Config{Key: k, Value: json.RawMessage(`true`)}, store.WriteOptions{Actor: "a"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutFlag(ctx, "svc", model.Flag{Key: k}, store.WriteOptions{Actor: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	snap := mustSnapshot(t, s, "svc")
	var configs, flags []string
	for _, c := range snap.Configs {
		configs = append(configs, c.Key)
	}
	for _, f := range snap.Flags {
		flags = append(flags, f.Key)
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(configs, want) || !reflect.DeepEqual(flags, want) {
		t.Fatalf("configs %v flags %v, want both %v", configs, flags, want)
	}
	if snap.Revision != 7 {
		t.Fatalf("revision = %d, want 7", snap.Revision)
	}
}

func testNamespaceIsolation(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "one")
	mustCreate(t, s, "two")
	if _, err := s.PutFlag(ctx, "one", model.Flag{Key: "f", Enabled: true}, store.WriteOptions{Actor: "a"}); err != nil {
		t.Fatal(err)
	}
	two := mustSnapshot(t, s, "two")
	if two.Revision != 1 || len(two.Flags) != 0 {
		t.Fatalf("namespace two changed by write to one: %+v", two)
	}
}

func testConcurrentWrites(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")

	const writers, perWriter = 8, 15
	var (
		mu   sync.Mutex
		seen = make(map[int64]bool)
		wg   sync.WaitGroup
		errs = make(chan error, writers*perWriter)
	)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				c, err := s.PutConfig(ctx, "svc", model.Config{
					Key: fmt.Sprintf("k%d", w), Value: json.RawMessage(fmt.Sprint(i)),
				}, store.WriteOptions{Actor: "a"})
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				if seen[c.Revision] {
					errs <- fmt.Errorf("revision %d handed out twice", c.Revision)
				}
				seen[c.Revision] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	snap := mustSnapshot(t, s, "svc")
	if want := int64(1 + writers*perWriter); snap.Revision != want {
		t.Fatalf("revision = %d, want %d", snap.Revision, want)
	}
	if len(snap.Configs) != writers {
		t.Fatalf("configs = %d, want %d", len(snap.Configs), writers)
	}
}

func testReturnedValuesAreCopies(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	in := json.RawMessage(`[1,2,3]`)
	c, err := s.PutConfig(ctx, "svc", model.Config{Key: "k", Value: in}, store.WriteOptions{Actor: "a"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range in {
		in[i] = ' '
	}
	for i := range c.Value {
		c.Value[i] = ' '
	}
	snap := mustSnapshot(t, s, "svc")
	jsonEqual(t, snap.Configs[0].Value, json.RawMessage(`[1,2,3]`))
	for i := range snap.Configs[0].Value {
		snap.Configs[0].Value[i] = ' '
	}
	jsonEqual(t, mustSnapshot(t, s, "svc").Configs[0].Value, json.RawMessage(`[1,2,3]`))
}
