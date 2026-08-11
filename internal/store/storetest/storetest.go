// Package storetest is a conformance suite every store.Store implementation
// must pass. Each subtest gets a fresh, empty store from the factory.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// Factory returns an empty store. Cleanup should be registered on t.
type Factory func(t *testing.T) store.Store

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, newStore(t))
		})
	}
}

func mustCreate(t *testing.T, s store.Store, name string) model.Namespace {
	t.Helper()
	ns, err := s.CreateNamespace(context.Background(), model.Namespace{Name: name, Description: "test " + name})
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

	_, err := s.CreateNamespace(ctx, model.Namespace{Name: "b-service/prod"})
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
		Key: "db.pool", Value: json.RawMessage(`{"max":10,"idle":2}`), Description: "pool", UpdatedBy: "alice",
	})
	if err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if c.Revision != 2 || c.UpdatedBy != "alice" || c.UpdatedAt.IsZero() {
		t.Fatalf("PutConfig = %+v, want revision 2 by alice", c)
	}
	jsonEqual(t, c.Value, json.RawMessage(`{"idle":2,"max":10}`))

	c, err = s.PutConfig(ctx, "svc", model.Config{Key: "db.pool", Value: json.RawMessage(`{"max":20}`), UpdatedBy: "bob"})
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

	rev, err := s.DeleteConfig(ctx, "svc", "db.pool")
	if err != nil {
		t.Fatalf("DeleteConfig: %v", err)
	}
	if rev != 4 {
		t.Fatalf("DeleteConfig revision = %d, want 4", rev)
	}
	_, err = s.DeleteConfig(ctx, "svc", "db.pool")
	wantErr(t, err, model.ErrNotFound, "DeleteConfig twice")

	snap = mustSnapshot(t, s, "svc")
	if snap.Revision != 4 || len(snap.Configs) != 0 {
		t.Fatalf("after delete: rev %d with %d configs, want rev 4 with 0 (failed delete must not bump)", snap.Revision, len(snap.Configs))
	}
}

func testFlagLifecycle(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")

	f, err := s.PutFlag(ctx, "svc", model.Flag{Key: "new-checkout", Enabled: true, Description: "d", UpdatedBy: "alice"})
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

	rev, err := s.DeleteFlag(ctx, "svc", "new-checkout")
	if err != nil || rev != 3 {
		t.Fatalf("DeleteFlag = %d, %v; want 3, nil", rev, err)
	}
	_, err = s.DeleteFlag(ctx, "svc", "new-checkout")
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
		UpdatedBy: "alice",
	}
	e, err := s.PutExperiment(ctx, "svc", in)
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

	rev, err := s.DeleteExperiment(ctx, "svc", "button-color")
	if err != nil || rev != 3 {
		t.Fatalf("DeleteExperiment = %d, %v; want 3, nil", rev, err)
	}
	_, err = s.DeleteExperiment(ctx, "svc", "button-color")
	wantErr(t, err, model.ErrNotFound, "DeleteExperiment twice")
}

func testMissingNamespace(t *testing.T, s store.Store) {
	ctx := context.Background()
	_, err := s.PutConfig(ctx, "nope", model.Config{Key: "k", Value: json.RawMessage(`1`), UpdatedBy: "a"})
	wantErr(t, err, model.ErrNotFound, "PutConfig")
	_, err = s.PutFlag(ctx, "nope", model.Flag{Key: "k", UpdatedBy: "a"})
	wantErr(t, err, model.ErrNotFound, "PutFlag")
	_, err = s.PutExperiment(ctx, "nope", model.Experiment{Key: "k", Salt: "k", Variants: []model.Variant{{Name: "a", Weight: 1}}, UpdatedBy: "a"})
	wantErr(t, err, model.ErrNotFound, "PutExperiment")
	_, err = s.DeleteConfig(ctx, "nope", "k")
	wantErr(t, err, model.ErrNotFound, "DeleteConfig")
	_, err = s.Snapshot(ctx, "nope")
	wantErr(t, err, model.ErrNotFound, "Snapshot")
}

func testSnapshotSorted(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	for _, k := range []string{"c", "a", "b"} {
		if _, err := s.PutConfig(ctx, "svc", model.Config{Key: k, Value: json.RawMessage(`true`), UpdatedBy: "a"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutFlag(ctx, "svc", model.Flag{Key: k, UpdatedBy: "a"}); err != nil {
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
	if _, err := s.PutFlag(ctx, "one", model.Flag{Key: "f", Enabled: true, UpdatedBy: "a"}); err != nil {
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
					Key: fmt.Sprintf("k%d", w), Value: json.RawMessage(fmt.Sprint(i)), UpdatedBy: "a",
				})
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
	c, err := s.PutConfig(ctx, "svc", model.Config{Key: "k", Value: in, UpdatedBy: "a"})
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
