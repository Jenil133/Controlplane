package storetest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// testActionAndMessage checks that WriteOptions.Action replaces the default
// audit action and that Message becomes the audit message and the revision
// summary.
func testActionAndMessage(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustCreate(t, s, "svc")
	put := func(f model.Flag, opts store.WriteOptions) json.RawMessage {
		t.Helper()
		out, err := s.PutFlag(ctx, "svc", f, opts)
		if err != nil {
			t.Fatalf("PutFlag: %v", err)
		}
		return encode(t, out)
	}

	const started = "start rollout of flag new-cart: stage 1/3 (1%)"
	v1 := put(rolloutFlag("new-cart", model.RolloutActive, 0),
		store.WriteOptions{Actor: "alice", Action: model.ActionRolloutStart, Message: started})
	checkRecorded(t, s, "svc", recorded{
		rev: 2, actor: "alice", action: model.ActionRolloutStart, entity: model.EntityFlag, key: "new-cart",
		after: v1, message: started, summary: started,
	})

	v2 := put(rolloutFlag("new-cart", model.RolloutActive, 1),
		store.WriteOptions{Actor: "rollout-controller", Action: model.ActionRolloutAdvance, ExpectedRevision: 2})
	checkRecorded(t, s, "svc", recorded{
		rev: 3, actor: "rollout-controller", action: model.ActionRolloutAdvance, entity: model.EntityFlag, key: "new-cart",
		before: v1, after: v2, summary: "rollout.advance flag new-cart",
	})

	const removed = "remove the finished rollout"
	if rev, err := s.DeleteFlag(ctx, "svc", "new-cart", store.WriteOptions{Actor: "bob", Action: "cleanup", Message: removed}); err != nil || rev != 4 {
		t.Fatalf("DeleteFlag = %d, %v; want 4", rev, err)
	}
	checkRecorded(t, s, "svc", recorded{
		rev: 4, actor: "bob", action: "cleanup", entity: model.EntityFlag, key: "new-cart",
		before: v2, message: removed, summary: removed,
	})

	const imported = "copied from staging"
	ns, err := s.CreateNamespace(ctx, model.Namespace{Name: "imported"}, store.WriteOptions{Actor: "carol", Action: "import", Message: imported})
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	checkRecorded(t, s, "imported", recorded{
		rev: 1, actor: "carol", action: "import", entity: model.EntityNamespace, key: "imported",
		after: encode(t, ns), message: imported, summary: imported,
	})
}

// filterAudit is the reference implementation of model.AuditFilter over the
// complete log, newest first.
func filterAudit(all []model.AuditEvent, f model.AuditFilter) []model.AuditEvent {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	limit = min(limit, maxLimit)
	var out []model.AuditEvent
	for _, e := range all {
		if len(out) == limit {
			break
		}
		if (f.Namespace == "" || e.Namespace == f.Namespace) &&
			(f.EntityType == "" || e.EntityType == f.EntityType) &&
			(f.EntityKey == "" || e.EntityKey == f.EntityKey) &&
			(f.Actor == "" || e.Actor == f.Actor) &&
			(f.Since.IsZero() || !e.Time.Before(f.Since)) &&
			(f.Until.IsZero() || e.Time.Before(f.Until)) &&
			(f.BeforeID <= 0 || e.ID < f.BeforeID) {
			out = append(out, e)
		}
	}
	return out
}

func testAuditFilters(t *testing.T, s store.Store) {
	ctx := context.Background()
	for _, ns := range []struct{ name, actor string }{{"a", "alice"}, {"b", "bob"}} {
		if _, err := s.CreateNamespace(ctx, model.Namespace{Name: ns.name}, store.WriteOptions{Actor: ns.actor}); err != nil {
			t.Fatalf("CreateNamespace(%q): %v", ns.name, err)
		}
	}
	a := newWriter(t, s, "a", "alice")
	a.config("k1", `1`)
	a.as("bob").flag(newFlag("k1", 10))
	a.config("k1", `2`)
	a.as("carol").del(model.EntityConfig, "k1")
	a.rateLimit(newRateLimit("rl", 1, 1))
	b := newWriter(t, s, "b", "bob")
	b.config("k1", `1`)
	b.as("carol").breaker(newBreaker("cb", 1))

	all := mustAudit(t, s, model.AuditFilter{Limit: maxLimit})
	if len(all) != 9 {
		t.Fatalf("%d audit events, want 9", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID >= all[i-1].ID {
			t.Fatalf("audit events not newest first: %v", ids(all))
		}
	}

	// Times may repeat, so expectations come from the reference filter; the
	// pivot's own inclusion is checked separately below.
	pivot := all[len(all)/2]
	oldest, newest := all[len(all)-1], all[0]
	for _, f := range []model.AuditFilter{
		{},
		{Namespace: "a"},
		{Namespace: "b"},
		{Namespace: "missing"},
		{EntityType: model.EntityConfig},
		{EntityType: model.EntityNamespace},
		{EntityKey: "k1"},
		{Actor: "bob"},
		{Namespace: "a", EntityType: model.EntityConfig, EntityKey: "k1"},
		{Namespace: "a", Actor: "carol"},
		{Since: pivot.Time},
		{Since: pivot.Time.Add(time.Nanosecond)},
		{Until: pivot.Time},
		{Until: pivot.Time.Add(time.Nanosecond)},
		{Since: pivot.Time, Until: pivot.Time.Add(time.Nanosecond)},
		{Since: newest.Time.Add(time.Nanosecond)},
		{Until: oldest.Time},
		{BeforeID: pivot.ID},
		{BeforeID: pivot.ID, Namespace: "a"},
		{BeforeID: oldest.ID},
		{Namespace: "a", Limit: 2},
		{Actor: "alice", Since: oldest.Time, Limit: 1},
	} {
		if got, want := ids(mustAudit(t, s, f)), ids(filterAudit(all, f)); !slices.Equal(got, want) {
			t.Errorf("ListAuditEvents(%+v) = events %v, want %v", f, got, want)
		}
	}

	for _, tt := range []struct {
		f    model.AuditFilter
		want bool
	}{
		{model.AuditFilter{Since: pivot.Time}, true},
		{model.AuditFilter{Since: pivot.Time.Add(time.Nanosecond)}, false},
		{model.AuditFilter{Until: pivot.Time}, false},
		{model.AuditFilter{Until: pivot.Time.Add(time.Nanosecond)}, true},
	} {
		if got := slices.Contains(ids(mustAudit(t, s, tt.f)), pivot.ID); got != tt.want {
			t.Errorf("ListAuditEvents(%+v) includes event %d at %v: %v, want %v", tt.f, pivot.ID, pivot.Time, got, tt.want)
		}
	}
}

func testAuditPaging(t *testing.T, s store.Store) {
	mustCreate(t, s, "svc")
	mustCreate(t, s, "other")
	w := newWriter(t, s, "svc", "alice")
	other := newWriter(t, s, "other", "bob")
	for i := range 12 {
		w.config(fmt.Sprintf("k%d", i%5), strconv.Itoa(i))
		if i%3 == 0 {
			other.config("noise", strconv.Itoa(i))
		}
	}
	all := mustAudit(t, s, model.AuditFilter{Namespace: "svc", Limit: maxLimit})
	if len(all) != 13 {
		t.Fatalf("%d audit events in svc, want 13", len(all))
	}
	var (
		paged  []int64
		sizes  []int
		before int64
	)
	for len(sizes) <= len(all) {
		page := mustAudit(t, s, model.AuditFilter{Namespace: "svc", BeforeID: before, Limit: 5})
		if len(page) == 0 {
			break
		}
		paged = append(paged, ids(page)...)
		sizes = append(sizes, len(page))
		before = page[len(page)-1].ID
	}
	if !slices.Equal(paged, ids(all)) || !slices.Equal(sizes, []int{5, 5, 3}) {
		t.Fatalf("paging returned events %v in pages of %v, want %v in pages of [5 5 3]", paged, sizes, ids(all))
	}
}
