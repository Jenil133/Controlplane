package server

import (
	"context"
	"strconv"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500

	// auditPageBytes is the size of before/after images after which a
	// ListAuditEvents page ends early, and auditChunkSize how many events are
	// fetched from the store at a time while filling a page.
	auditPageBytes = 2 << 20
	auditChunkSize = 16
)

// pageSize applies the API's paging rule: 0 means the default, anything
// above the maximum is capped, and a negative size is an error.
func pageSize(n int32) (int, error) {
	switch {
	case n < 0:
		return 0, model.Invalidf("page_size must not be negative, got %d", n)
	case n == 0:
		return defaultPageSize, nil
	default:
		return min(int(n), maxPageSize), nil
	}
}

func (a *adminService) ListRevisions(ctx context.Context, req *cpv1.ListRevisionsRequest) (*cpv1.ListRevisionsResponse, error) {
	if err := model.ValidateNamespaceName(req.GetNamespace()); err != nil {
		return nil, a.toStatus(err)
	}
	limit, err := pageSize(req.GetPageSize())
	if err != nil {
		return nil, a.toStatus(err)
	}
	if req.GetBeforeRevision() < 0 {
		return nil, a.toStatus(model.Invalidf("before_revision must not be negative, got %d", req.GetBeforeRevision()))
	}
	revs, err := a.store.ListRevisions(ctx, req.GetNamespace(), req.GetBeforeRevision(), limit)
	if err != nil {
		return nil, a.toStatus(err)
	}
	resp := &cpv1.ListRevisionsResponse{Revisions: make([]*cpv1.Revision, len(revs))}
	for i, r := range revs {
		resp.Revisions[i] = revisionToProto(r)
	}
	// History starts at revision 1, so a full page that reaches it is the last.
	if len(revs) == limit {
		if last := revs[len(revs)-1].Revision; last > 1 {
			resp.NextBeforeRevision = last
		}
	}
	return resp, nil
}

func (a *adminService) GetRevision(ctx context.Context, req *cpv1.GetRevisionRequest) (*cpv1.GetRevisionResponse, error) {
	if err := model.ValidateNamespaceName(req.GetNamespace()); err != nil {
		return nil, a.toStatus(err)
	}
	if req.GetRevision() < 1 {
		return nil, a.toStatus(model.Invalidf("revision must be at least 1, got %d", req.GetRevision()))
	}
	r, err := a.store.GetRevision(ctx, req.GetNamespace(), req.GetRevision())
	if err != nil {
		return nil, a.toStatus(err)
	}
	out := revisionToProto(r)
	if out.Snapshot, err = SnapshotToProto(r.Snapshot); err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.GetRevisionResponse{Revision: out}, nil
}

func (a *adminService) DiffRevisions(ctx context.Context, req *cpv1.DiffRevisionsRequest) (*cpv1.DiffRevisionsResponse, error) {
	if err := model.ValidateNamespaceName(req.GetNamespace()); err != nil {
		return nil, a.toStatus(err)
	}
	if req.GetFromRevision() < 1 {
		return nil, a.toStatus(model.Invalidf("from_revision must be at least 1, got %d", req.GetFromRevision()))
	}
	if req.GetToRevision() < 0 {
		return nil, a.toStatus(model.Invalidf("to_revision must not be negative, got %d", req.GetToRevision()))
	}
	from, err := a.store.GetRevision(ctx, req.GetNamespace(), req.GetFromRevision())
	if err != nil {
		return nil, a.toStatus(err)
	}
	var to model.Snapshot
	if req.GetToRevision() == 0 {
		to, err = a.store.Snapshot(ctx, req.GetNamespace())
	} else {
		var r model.Revision
		r, err = a.store.GetRevision(ctx, req.GetNamespace(), req.GetToRevision())
		to = r.Snapshot
	}
	if err != nil {
		return nil, a.toStatus(err)
	}
	diff, err := model.Diff(from.Snapshot, to)
	if err != nil {
		return nil, a.toStatus(err)
	}
	changes, err := changesToProto(diff)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.DiffRevisionsResponse{Changes: changes}, nil
}

func (a *adminService) Rollback(ctx context.Context, req *cpv1.RollbackRequest) (*cpv1.RollbackResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	if req.GetToRevision() < 1 {
		return nil, a.toStatus(model.Invalidf("to_revision must be at least 1, got %d", req.GetToRevision()))
	}
	rev, applied, err := a.store.Rollback(ctx, req.GetNamespace(), req.GetToRevision(), store.WriteOptions{Actor: who, ExpectedRevision: req.GetExpectedRevision()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	// Nothing is written when the namespace already matches the target.
	if len(applied) > 0 {
		a.log.Info("namespace rolled back", "namespace", req.GetNamespace(), "to_revision", req.GetToRevision(), "changes", len(applied), "revision", rev, "actor", who)
		a.changed(ctx, req.GetNamespace(), rev, SourceWrite)
	}
	changes, err := changesToProto(applied)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.RollbackResponse{Revision: rev, Changes: changes}, nil
}

// auditEntityTypes are the entity types an audit event can have.
var auditEntityTypes = map[string]bool{
	model.EntityNamespace:      true,
	model.EntityConfig:         true,
	model.EntityFlag:           true,
	model.EntityExperiment:     true,
	model.EntityRateLimit:      true,
	model.EntityCircuitBreaker: true,
}

func (a *adminService) ListAuditEvents(ctx context.Context, req *cpv1.ListAuditEventsRequest) (*cpv1.ListAuditEventsResponse, error) {
	filter, err := auditFilter(req)
	if err != nil {
		return nil, a.toStatus(err)
	}
	resp := &cpv1.ListAuditEventsResponse{}
	// Before and after images can be up to model.MaxValueBytes each, so a
	// page is also bounded by size: events are fetched in small chunks and
	// the page ends once its images pass auditPageBytes, which keeps it under
	// gRPC's default 4 MiB message limit and bounds what one request loads.
	var (
		size     int
		last     int64
		cutShort bool
	)
	for !cutShort && len(resp.Events) < filter.Limit {
		chunk := filter
		chunk.Limit = min(filter.Limit-len(resp.Events), auditChunkSize)
		if last != 0 {
			chunk.BeforeID = last
		}
		events, err := a.store.ListAuditEvents(ctx, chunk)
		if err != nil {
			return nil, a.toStatus(err)
		}
		for _, e := range events {
			pe, err := auditEventToProto(e)
			if err != nil {
				return nil, a.toStatus(err)
			}
			resp.Events = append(resp.Events, pe)
			last = e.ID
			if size += len(e.Before) + len(e.After); size >= auditPageBytes {
				// At least this event is returned, however large it is.
				cutShort = true
				break
			}
		}
		if len(events) < chunk.Limit {
			break
		}
	}
	// A token is offered whenever the page may not have reached the end.
	if cutShort || len(resp.Events) == filter.Limit {
		resp.NextPageToken = strconv.FormatInt(last, 10)
	}
	return resp, nil
}

// auditFilter validates a ListAuditEvents request. Its page token is the ID
// of the last event already returned; clients treat it as opaque.
func auditFilter(req *cpv1.ListAuditEventsRequest) (model.AuditFilter, error) {
	f := model.AuditFilter{
		Namespace:  req.GetNamespace(),
		EntityType: req.GetEntityType(),
		EntityKey:  req.GetEntityKey(),
		Actor:      req.GetActor(),
	}
	if f.Namespace != "" {
		if err := model.ValidateNamespaceName(f.Namespace); err != nil {
			return model.AuditFilter{}, err
		}
	}
	if f.EntityType != "" && !auditEntityTypes[f.EntityType] {
		return model.AuditFilter{}, model.Invalidf("unknown entity_type %q", f.EntityType)
	}
	var err error
	if f.Since, err = optionalTime("since", req.GetSince()); err != nil {
		return model.AuditFilter{}, err
	}
	if f.Until, err = optionalTime("until", req.GetUntil()); err != nil {
		return model.AuditFilter{}, err
	}
	if f.Limit, err = pageSize(req.GetPageSize()); err != nil {
		return model.AuditFilter{}, err
	}
	if token := req.GetPageToken(); token != "" {
		id, err := strconv.ParseInt(token, 10, 64)
		if err != nil || id < 1 {
			return model.AuditFilter{}, model.Invalidf("invalid page_token %q", token)
		}
		f.BeforeID = id
	}
	return f, nil
}
