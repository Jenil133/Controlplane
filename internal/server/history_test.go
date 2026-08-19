package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func revisionNumbers(revs []*cpv1.Revision) []int64 {
	out := make([]int64, len(revs))
	for i, r := range revs {
		out[i] = r.GetRevision()
	}
	return out
}

func TestListRevisionsPages(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	for i := range 7 {
		putConfig(t, r, "svc", "k", float64(i)) // revisions 2 to 8
	}
	ctx := ctxAs(t, "alice")

	for _, tt := range []struct {
		size   int32
		before int64
		want   []int64
		next   int64
	}{
		{0, 0, []int64{8, 7, 6, 5, 4, 3, 2, 1}, 0},
		{3, 0, []int64{8, 7, 6}, 6},
		{3, 6, []int64{5, 4, 3}, 3},
		{3, 3, []int64{2, 1}, 0},
		// A full page that reaches revision 1 is the last one.
		{4, 5, []int64{4, 3, 2, 1}, 0},
		{2, 1, []int64{}, 0},
		{2, 100, []int64{8, 7}, 7},
	} {
		resp, err := r.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: "svc", PageSize: tt.size, BeforeRevision: tt.before})
		if err != nil {
			t.Fatalf("ListRevisions(size %d, before %d): %v", tt.size, tt.before, err)
		}
		if got := revisionNumbers(resp.GetRevisions()); !slices.Equal(got, tt.want) || resp.GetNextBeforeRevision() != tt.next {
			t.Errorf("ListRevisions(size %d, before %d) = %v next %d, want %v next %d",
				tt.size, tt.before, got, resp.GetNextBeforeRevision(), tt.want, tt.next)
		}
	}

	resp, err := r.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: "svc", PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if latest := resp.GetRevisions()[0]; latest.GetNamespace() != "svc" || latest.GetActor() != "test" ||
		latest.GetSummary() != "update config k" || latest.GetCreatedAt() == nil || latest.GetSnapshot() != nil {
		t.Fatalf("latest revision = %v", latest)
	}

	for _, tt := range []struct {
		name string
		req  *cpv1.ListRevisionsRequest
		code codes.Code
	}{
		{"negative page size", &cpv1.ListRevisionsRequest{Namespace: "svc", PageSize: -1}, codes.InvalidArgument},
		{"negative before_revision", &cpv1.ListRevisionsRequest{Namespace: "svc", BeforeRevision: -1}, codes.InvalidArgument},
		{"invalid namespace", &cpv1.ListRevisionsRequest{Namespace: "Bad"}, codes.InvalidArgument},
		{"missing namespace", &cpv1.ListRevisionsRequest{Namespace: "missing"}, codes.NotFound},
	} {
		_, err := r.admin.ListRevisions(ctx, tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
}

// TestPageSizesAreCapped fills more history than one page may hold.
func TestPageSizesAreCapped(t *testing.T) {
	st := memory.New()
	r := startReplica(t, st, replicaOptions{})
	createNamespace(t, r, "svc")
	ctx := ctxAs(t, "alice")
	for i := range maxPageSize + 5 {
		if _, err := st.PutConfig(ctx, "svc", model.Config{Key: "k", Value: json.RawMessage(strconv.Itoa(i))}, store.WriteOptions{Actor: "bulk"}); err != nil {
			t.Fatal(err)
		}
	}
	latest := int64(maxPageSize + 6)

	// Page size 0 means the documented default, not "everything up to the cap".
	const defaultSize = 50
	revs, err := r.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if got := revs.GetRevisions(); len(got) != defaultSize || got[0].GetRevision() != latest || revs.GetNextBeforeRevision() != latest-defaultSize+1 {
		t.Fatalf("default ListRevisions returned %d revisions, next %d; want %d from %d, next %d",
			len(got), revs.GetNextBeforeRevision(), defaultSize, latest, latest-defaultSize+1)
	}
	events, err := r.admin.ListAuditEvents(ctx, &cpv1.ListAuditEventsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := events.GetEvents(); len(got) != defaultSize || events.GetNextPageToken() != strconv.FormatInt(got[len(got)-1].GetId(), 10) ||
		got[0].GetId() != latest {
		t.Fatalf("default ListAuditEvents returned %d events, next page token %q; want %d ending in a token", len(got), events.GetNextPageToken(), defaultSize)
	}

	revs, err = r.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: "svc", PageSize: 10 * maxPageSize})
	if err != nil {
		t.Fatal(err)
	}
	if got := revs.GetRevisions(); len(got) != maxPageSize || got[0].GetRevision() != latest || revs.GetNextBeforeRevision() != latest-maxPageSize+1 {
		t.Fatalf("ListRevisions returned %d revisions from %d, next %d; want %d from %d, next %d",
			len(got), got[0].GetRevision(), revs.GetNextBeforeRevision(), maxPageSize, latest, latest-maxPageSize+1)
	}
	events, err = r.admin.ListAuditEvents(ctx, &cpv1.ListAuditEventsRequest{PageSize: 10 * maxPageSize})
	if err != nil {
		t.Fatal(err)
	}
	if got := events.GetEvents(); len(got) != maxPageSize || events.GetNextPageToken() != strconv.FormatInt(got[len(got)-1].GetId(), 10) {
		t.Fatalf("ListAuditEvents returned %d events, next page token %q; want %d and a token", len(got), events.GetNextPageToken(), maxPageSize)
	}
}

func TestGetRevision(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	putConfig(t, r, "svc", "a", 1.0)
	ctx := ctxAs(t, "alice")
	at2, err := r.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f"})
	putConfig(t, r, "svc", "a", 2.0)

	resp, err := r.admin.GetRevision(ctx, &cpv1.GetRevisionRequest{Namespace: "svc", Revision: 2})
	if err != nil {
		t.Fatal(err)
	}
	rev := resp.GetRevision()
	if rev.GetNamespace() != "svc" || rev.GetRevision() != 2 || rev.GetActor() != "test" || rev.GetSummary() != "create config a" ||
		!rev.GetCreatedAt().AsTime().Equal(at2.GetSnapshot().GetUpdatedAt().AsTime()) {
		t.Fatalf("revision 2 = %v", rev)
	}
	if !proto.Equal(rev.GetSnapshot(), at2.GetSnapshot()) {
		t.Fatalf("snapshot of revision 2 = %v, want %v", rev.GetSnapshot(), at2.GetSnapshot())
	}

	resp, err = r.admin.GetRevision(ctx, &cpv1.GetRevisionRequest{Namespace: "svc", Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s := resp.GetRevision().GetSnapshot(); resp.GetRevision().GetSummary() != "create namespace" || s.GetRevision() != 1 ||
		len(s.GetConfigs())+len(s.GetFlags())+len(s.GetExperiments())+len(s.GetRateLimits())+len(s.GetCircuitBreakers()) != 0 {
		t.Fatalf("revision 1 = %v", resp.GetRevision())
	}

	for _, tt := range []struct {
		name string
		req  *cpv1.GetRevisionRequest
		code codes.Code
	}{
		{"revision 0", &cpv1.GetRevisionRequest{Namespace: "svc"}, codes.InvalidArgument},
		{"negative revision", &cpv1.GetRevisionRequest{Namespace: "svc", Revision: -1}, codes.InvalidArgument},
		{"future revision", &cpv1.GetRevisionRequest{Namespace: "svc", Revision: 99}, codes.NotFound},
		{"missing namespace", &cpv1.GetRevisionRequest{Namespace: "missing", Revision: 1}, codes.NotFound},
		{"invalid namespace", &cpv1.GetRevisionRequest{Namespace: "", Revision: 1}, codes.InvalidArgument},
	} {
		_, err := r.admin.GetRevision(ctx, tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
}

// describeChanges renders changes as "type entity key" lines.
func describeChanges(changes []*cpv1.Change) []string {
	out := make([]string, len(changes))
	for i, c := range changes {
		out[i] = fmt.Sprintf("%v %s %s", c.GetType(), c.GetEntityType(), c.GetKey())
	}
	return out
}

// noError returns a function that fails the test if the call whose results
// it is given returned an error.
func noError(t *testing.T) func(any, error) {
	return func(_ any, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDiffRevisions(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	must := noError(t)
	ctx := ctxAs(t, "test")
	// Revisions: 1 namespace, 2 config a = 1, 3 flag f, 4 config a = 2,
	// 5 flag f deleted, 6 rate limit rl.
	createNamespace(t, r, "svc")
	putConfig(t, r, "svc", "a", 1.0)
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f"})
	putConfig(t, r, "svc", "a", 2.0)
	must(r.admin.DeleteFlag(ctx, &cpv1.DeleteFlagRequest{Namespace: "svc", Key: "f"}))
	must(r.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{Namespace: "svc", Key: "rl", RequestsPerSecond: 1, Burst: 1}))

	for _, tt := range []struct {
		from, to int64
		want     []string
	}{
		{2, 4, []string{"CHANGE_TYPE_MODIFIED config a", "CHANGE_TYPE_ADDED flag f"}},
		{1, 0, []string{"CHANGE_TYPE_ADDED config a", "CHANGE_TYPE_ADDED rate_limit rl"}},
		{3, 5, []string{"CHANGE_TYPE_MODIFIED config a", "CHANGE_TYPE_REMOVED flag f"}},
		{4, 2, []string{"CHANGE_TYPE_MODIFIED config a", "CHANGE_TYPE_REMOVED flag f"}},
		{6, 6, []string{}},
		{6, 0, []string{}},
	} {
		resp, err := r.admin.DiffRevisions(ctx, &cpv1.DiffRevisionsRequest{Namespace: "svc", FromRevision: tt.from, ToRevision: tt.to})
		if err != nil {
			t.Fatalf("DiffRevisions(%d, %d): %v", tt.from, tt.to, err)
		}
		if got := describeChanges(resp.GetChanges()); !slices.Equal(got, tt.want) {
			t.Errorf("DiffRevisions(%d, %d) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}

	// Entries travel as stored, with their metadata.
	resp, err := r.admin.DiffRevisions(ctx, &cpv1.DiffRevisionsRequest{Namespace: "svc", FromRevision: 2, ToRevision: 5})
	if err != nil {
		t.Fatal(err)
	}
	a := resp.GetChanges()[0]
	before, after := a.GetBefore().GetStructValue().GetFields(), a.GetAfter().GetStructValue().GetFields()
	if before["value"].GetNumberValue() != 1 || after["value"].GetNumberValue() != 2 ||
		before["revision"].GetNumberValue() != 2 || after["revision"].GetNumberValue() != 4 || after["updated_by"].GetStringValue() != "test" {
		t.Fatalf("config change = %v", a)
	}

	for _, tt := range []struct {
		name string
		req  *cpv1.DiffRevisionsRequest
		code codes.Code
	}{
		{"from revision 0", &cpv1.DiffRevisionsRequest{Namespace: "svc", ToRevision: 2}, codes.InvalidArgument},
		{"negative to revision", &cpv1.DiffRevisionsRequest{Namespace: "svc", FromRevision: 1, ToRevision: -1}, codes.InvalidArgument},
		{"missing from revision", &cpv1.DiffRevisionsRequest{Namespace: "svc", FromRevision: 99}, codes.NotFound},
		{"missing to revision", &cpv1.DiffRevisionsRequest{Namespace: "svc", FromRevision: 1, ToRevision: 99}, codes.NotFound},
		{"missing namespace", &cpv1.DiffRevisionsRequest{Namespace: "missing", FromRevision: 1}, codes.NotFound},
		{"invalid namespace", &cpv1.DiffRevisionsRequest{Namespace: "Bad", FromRevision: 1}, codes.InvalidArgument},
	} {
		_, err := r.admin.DiffRevisions(ctx, tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
}

func TestRollbackReachesWatchersWithinBudget(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	must := noError(t)
	ctx := ctxAs(t, "alice")
	// Revisions: 1 namespace, 2 config k = v1, 3 flag f, 4 rate limit rl,
	// 5 config k = v2, 6 flag f deleted.
	createNamespace(t, r, "svc")
	putConfig(t, r, "svc", "k", "v1")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	must(r.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{Namespace: "svc", Key: "rl", RequestsPerSecond: 1, Burst: 1}))
	putConfig(t, r, "svc", "k", "v2")
	must(r.admin.DeleteFlag(ctx, &cpv1.DeleteFlagRequest{Namespace: "svc", Key: "f"}))
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)

	start := time.Now()
	resp, err := r.admin.Rollback(ctxAs(t, "bob"), &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 3, ExpectedRevision: 6})
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	snap := recv(t, stream, propagationBudget)
	if elapsed := time.Since(start); elapsed > propagationBudget {
		t.Fatalf("rollback took %v to reach the watcher, budget %v", elapsed, propagationBudget)
	}

	want := []string{"CHANGE_TYPE_MODIFIED config k", "CHANGE_TYPE_ADDED flag f", "CHANGE_TYPE_REMOVED rate_limit rl"}
	if got := describeChanges(resp.GetChanges()); resp.GetRevision() != 7 || !slices.Equal(got, want) {
		t.Fatalf("Rollback = revision %d, changes %v; want 7, %v", resp.GetRevision(), got, want)
	}
	if after := resp.GetChanges()[1].GetAfter().GetStructValue().GetFields(); after["revision"].GetNumberValue() != 7 || after["updated_by"].GetStringValue() != "bob" {
		t.Fatalf("restored flag image = %v, want revision 7 by bob", after)
	}
	if snap.GetRevision() != 7 || len(snap.GetConfigs()) != 1 || snap.GetConfigs()[0].GetValue().GetStringValue() != "v1" ||
		len(snap.GetRateLimits()) != 0 || !snapshotFlag(t, snap, "f").GetEnabled() || snapshotFlag(t, snap, "f").GetUpdatedBy() != "bob" {
		t.Fatalf("snapshot after rollback = %v", snap)
	}

	events, err := r.admin.ListAuditEvents(ctx, &cpv1.ListAuditEventsRequest{Namespace: "svc", PageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.GetEvents()) != 3 {
		t.Fatalf("got %d audit events, want 3", len(events.GetEvents()))
	}
	for _, e := range events.GetEvents() {
		if e.GetRevision() != 7 || e.GetAction() != model.ActionRollback || e.GetActor() != "bob" || e.GetMessage() != "rollback to revision 3" {
			t.Fatalf("audit event = %v, want a rollback to revision 3 by bob at revision 7", e)
		}
	}

	// The namespace already matches revision 3: nothing to write.
	resp, err = r.admin.Rollback(ctx, &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 3})
	if err != nil || resp.GetRevision() != 7 || len(resp.GetChanges()) != 0 {
		t.Fatalf("repeated Rollback = %v, %v; want revision 7 and no changes", resp, err)
	}
	if got := namespaceRevision(t, r, "svc"); got != 7 {
		t.Fatalf("namespace revision = %d after a no-op rollback, want 7", got)
	}
}

func TestRollbackPausesActiveRollouts(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	must := noError(t)
	createNamespace(t, r, "svc")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	ctx := ctxAs(t, "alice")
	// Revision 3 starts the rollout, revision 4 advances it.
	must(startRollout(stage(10, time.Hour), stage(50, time.Hour), stage(100, 0))(ctx, r))
	must(advanceRollout(ctx, r))
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)

	if _, err := r.admin.Rollback(ctx, &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 3}); err != nil {
		t.Fatal(err)
	}
	// Its stage timer is stale, so the restored rollout must not resume on
	// its own.
	checkRollout(t, "after rollback", snapshotFlag(t, recv(t, stream, propagationBudget), "f"), 10, 0, paused)
}

func TestRollbackErrors(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	putConfig(t, r, "svc", "k", "v1")
	putConfig(t, r, "svc", "k", "v2")
	ctx := ctxAs(t, "alice")

	for _, tt := range []struct {
		name string
		ctx  context.Context
		req  *cpv1.RollbackRequest
		code codes.Code
	}{
		{"stale expected revision", ctx, &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 2, ExpectedRevision: 2}, codes.Aborted},
		{"revision 0", ctx, &cpv1.RollbackRequest{Namespace: "svc"}, codes.InvalidArgument},
		{"future revision", ctx, &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 99}, codes.NotFound},
		{"missing namespace", ctx, &cpv1.RollbackRequest{Namespace: "missing", ToRevision: 1}, codes.NotFound},
		{"invalid namespace", ctx, &cpv1.RollbackRequest{Namespace: "Bad", ToRevision: 1}, codes.InvalidArgument},
		{"actor too long", ctxAs(t, strings.Repeat("x", model.MaxActorLen+1)), &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 2}, codes.InvalidArgument},
	} {
		_, err := r.admin.Rollback(tt.ctx, tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
	if got := namespaceRevision(t, r, "svc"); got != 3 {
		t.Fatalf("namespace revision = %d after failed rollbacks, want 3", got)
	}
}

func eventIDs(events []*cpv1.AuditEvent) []int64 {
	out := make([]int64, len(events))
	for i, e := range events {
		out[i] = e.GetId()
	}
	return out
}

func TestListAuditEvents(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	must := noError(t)
	alice, bob := ctxAs(t, "alice"), ctxAs(t, "bob")
	// Events 1 to 7, in this order.
	must(r.admin.CreateNamespace(alice, &cpv1.CreateNamespaceRequest{Name: "a"}))
	must(r.admin.CreateNamespace(bob, &cpv1.CreateNamespaceRequest{Name: "b"}))
	must(r.admin.PutConfig(alice, &cpv1.PutConfigRequest{Namespace: "a", Key: "k", Value: mustValue(t, 1.0)}))
	must(r.admin.PutFlag(bob, &cpv1.PutFlagRequest{Namespace: "a", Key: "f"}))
	must(r.admin.PutConfig(bob, &cpv1.PutConfigRequest{Namespace: "a", Key: "k", Value: mustValue(t, 2.0)}))
	must(r.admin.DeleteConfig(alice, &cpv1.DeleteConfigRequest{Namespace: "a", Key: "k"}))
	must(r.admin.PutConfig(alice, &cpv1.PutConfigRequest{Namespace: "b", Key: "k", Value: mustValue(t, "x")}))

	list := func(req *cpv1.ListAuditEventsRequest) *cpv1.ListAuditEventsResponse {
		t.Helper()
		resp, err := r.admin.ListAuditEvents(alice, req)
		if err != nil {
			t.Fatalf("ListAuditEvents(%v): %v", req, err)
		}
		return resp
	}
	all := list(&cpv1.ListAuditEventsRequest{}).GetEvents()
	if got := eventIDs(all); !slices.Equal(got, []int64{7, 6, 5, 4, 3, 2, 1}) {
		t.Fatalf("event IDs = %v, want 7 to 1", got)
	}
	update := all[2]
	if update.GetNamespace() != "a" || update.GetRevision() != 4 || update.GetActor() != "bob" || update.GetAction() != model.ActionUpdate ||
		update.GetEntityType() != model.EntityConfig || update.GetEntityKey() != "k" || update.GetTime() == nil ||
		update.GetBefore().GetStructValue().GetFields()["value"].GetNumberValue() != 1 ||
		update.GetAfter().GetStructValue().GetFields()["value"].GetNumberValue() != 2 {
		t.Fatalf("update event = %v", update)
	}
	if del := all[1]; del.GetAction() != model.ActionDelete || del.GetBefore() == nil || del.GetAfter() != nil {
		t.Fatalf("delete event = %v", del)
	}
	if created := all[6]; created.GetEntityType() != model.EntityNamespace || created.GetBefore() != nil ||
		created.GetAfter().GetStructValue().GetFields()["created_by"].GetStringValue() != "alice" {
		t.Fatalf("namespace event = %v", created)
	}

	// Time filters: since inclusive, until exclusive.
	since, until := all[4].GetTime(), all[1].GetTime()
	var inRange []int64
	for _, e := range all {
		if at := e.GetTime().AsTime(); !at.Before(since.AsTime()) && at.Before(until.AsTime()) {
			inRange = append(inRange, e.GetId())
		}
	}
	for _, tt := range []struct {
		name string
		req  *cpv1.ListAuditEventsRequest
		want []int64
	}{
		{"namespace", &cpv1.ListAuditEventsRequest{Namespace: "a"}, []int64{6, 5, 4, 3, 1}},
		{"entity type", &cpv1.ListAuditEventsRequest{EntityType: model.EntityConfig}, []int64{7, 6, 5, 3}},
		{"entity", &cpv1.ListAuditEventsRequest{Namespace: "a", EntityType: model.EntityConfig, EntityKey: "k"}, []int64{6, 5, 3}},
		{"actor", &cpv1.ListAuditEventsRequest{Actor: "bob"}, []int64{5, 4, 2}},
		{"namespaces", &cpv1.ListAuditEventsRequest{EntityType: model.EntityNamespace}, []int64{2, 1}},
		{"time range", &cpv1.ListAuditEventsRequest{Since: since, Until: until}, inRange},
		{"no match", &cpv1.ListAuditEventsRequest{Namespace: "gone"}, []int64{}},
	} {
		if got := eventIDs(list(tt.req).GetEvents()); !slices.Equal(got, tt.want) {
			t.Errorf("%s: event IDs = %v, want %v", tt.name, got, tt.want)
		}
	}

	// Paging walks every event exactly once.
	for _, size := range []int32{1, 2, 3, 7} {
		var (
			got   []int64
			token string
		)
		for range 10 {
			resp := list(&cpv1.ListAuditEventsRequest{PageSize: size, PageToken: token})
			got = append(got, eventIDs(resp.GetEvents())...)
			if token = resp.GetNextPageToken(); token == "" {
				break
			}
		}
		if token != "" || !slices.Equal(got, eventIDs(all)) {
			t.Errorf("page size %d: walked %v (last token %q), want %v", size, got, token, eventIDs(all))
		}
	}

	for _, tt := range []struct {
		name string
		req  *cpv1.ListAuditEventsRequest
	}{
		{"unknown entity type", &cpv1.ListAuditEventsRequest{EntityType: "flags"}},
		{"negative page size", &cpv1.ListAuditEventsRequest{PageSize: -1}},
		{"malformed page token", &cpv1.ListAuditEventsRequest{PageToken: "abc"}},
		{"zero page token", &cpv1.ListAuditEventsRequest{PageToken: "0"}},
		{"invalid namespace", &cpv1.ListAuditEventsRequest{Namespace: "Bad Name"}},
		{"invalid since", &cpv1.ListAuditEventsRequest{Since: &timestamppb.Timestamp{Nanos: -1}}},
		{"invalid until", &cpv1.ListAuditEventsRequest{Until: &timestamppb.Timestamp{Seconds: -1 << 40}}},
	} {
		_, err := r.admin.ListAuditEvents(alice, tt.req)
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("%s: got %v (%v), want InvalidArgument", tt.name, got, err)
		}
	}
}

// TestListAuditEventsPagesStayUnderMessageLimit updates a large config often
// enough that the default page of 50 events would carry about 20 MiB of
// before and after images. The client's default receive limit is 4 MiB, so
// walking the log only works when pages are cut by size as well as by count.
func TestListAuditEventsPagesStayUnderMessageLimit(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	big := strings.Repeat("x", model.MaxValueBytes-1024)
	const updates = 30
	for i := range updates {
		putConfig(t, r, "svc", "blob", big+strconv.Itoa(i))
	}
	ctx := ctxAs(t, "alice")

	var (
		ids     []int64
		pages   int
		token   string
		biggest int
	)
	for range updates + 2 {
		resp, err := r.admin.ListAuditEvents(ctx, &cpv1.ListAuditEventsRequest{Namespace: "svc", PageToken: token})
		if err != nil {
			t.Fatalf("page %d: %v", pages+1, err)
		}
		pages++
		biggest = max(biggest, proto.Size(resp))
		if n := len(resp.GetEvents()); n == 0 || n > updates {
			t.Fatalf("page %d has %d events", pages, n)
		}
		ids = append(ids, eventIDs(resp.GetEvents())...)
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
		if last := strconv.FormatInt(ids[len(ids)-1], 10); token != last {
			t.Fatalf("page %d: next page token %q, want the last event ID %s", pages, token, last)
		}
	}
	if token != "" {
		t.Fatalf("log not exhausted after %d pages", pages)
	}
	want := make([]int64, updates+1) // the updates and the namespace creation
	for i := range want {
		want[i] = int64(len(want) - i)
	}
	if !slices.Equal(ids, want) {
		t.Fatalf("walked event IDs %v, want %v", ids, want)
	}
	if pages < 2 {
		t.Fatalf("log of %d large events came back in %d page, want it split by size", updates, pages)
	}
	if biggest > 3<<20 {
		t.Fatalf("largest page was %d bytes, want at most about %d", biggest, auditPageBytes+model.MaxValueBytes*2)
	}
}

// TestRollbackReportsChangeOnlyWhenWritten checks that watchers are told
// about a rollback only when it wrote a new revision.
func TestRollbackReportsChangeOnlyWhenWritten(t *testing.T) {
	sources := &sourceRecorder{}
	r := startReplica(t, memory.New(), replicaOptions{observer: sources})
	createNamespace(t, r, "svc")
	putConfig(t, r, "svc", "k", "v1") // revision 2
	putConfig(t, r, "svc", "k", "v2") // revision 3
	ctx := ctxAs(t, "alice")
	before := sources.count("svc", SourceWrite)

	for _, tt := range []struct {
		name     string
		wantRev  int64
		wantNote int
	}{
		{"rollback that changes the namespace", 4, 1},
		{"rollback to the state it already has", 4, 0},
	} {
		if _, err := r.admin.Rollback(ctx, &cpv1.RollbackRequest{Namespace: "svc", ToRevision: 2}); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		after := sources.count("svc", SourceWrite)
		if got := after - before; got != tt.wantNote {
			t.Errorf("%s: %d write changes reported, want %d", tt.name, got, tt.wantNote)
		}
		before = after
		if got := namespaceRevision(t, r, "svc"); got != tt.wantRev {
			t.Errorf("%s: namespace revision %d, want %d", tt.name, got, tt.wantRev)
		}
	}
}
