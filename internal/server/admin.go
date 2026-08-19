package server

import (
	"context"
	"errors"
	"sync"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/rollout"
	"github.com/Jenil133/Controlplane/internal/store"
)

type adminService struct {
	cpv1.UnimplementedAdminServiceServer
	*Server

	// createFlag serializes the creation of flags within this process, see
	// PutFlag.
	createFlag sync.Mutex
}

func (a *adminService) CreateNamespace(ctx context.Context, req *cpv1.CreateNamespaceRequest) (*cpv1.CreateNamespaceResponse, error) {
	who, err := actor(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	ns := model.Namespace{Name: req.GetName(), Description: req.GetDescription()}
	if err := ns.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.CreateNamespace(ctx, ns, store.WriteOptions{Actor: who})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("namespace created", "namespace", out.Name, "actor", who)
	return &cpv1.CreateNamespaceResponse{Namespace: namespaceToProto(out)}, nil
}

func (a *adminService) GetNamespace(ctx context.Context, req *cpv1.GetNamespaceRequest) (*cpv1.GetNamespaceResponse, error) {
	if err := model.ValidateNamespaceName(req.GetName()); err != nil {
		return nil, a.toStatus(err)
	}
	ns, err := a.store.GetNamespace(ctx, req.GetName())
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.GetNamespaceResponse{Namespace: namespaceToProto(ns)}, nil
}

func (a *adminService) ListNamespaces(ctx context.Context, _ *cpv1.ListNamespacesRequest) (*cpv1.ListNamespacesResponse, error) {
	list, err := a.store.ListNamespaces(ctx)
	if err != nil {
		return nil, a.toStatus(err)
	}
	out := make([]*cpv1.Namespace, len(list))
	for i, ns := range list {
		out[i] = namespaceToProto(ns)
	}
	return &cpv1.ListNamespacesResponse{Namespaces: out}, nil
}

// prepareWrite validates the namespace and returns the acting user.
func (a *adminService) prepareWrite(ctx context.Context, namespace string) (string, error) {
	who, err := actor(ctx)
	if err != nil {
		return "", err
	}
	return who, model.ValidateNamespaceName(namespace)
}

func (a *adminService) PutConfig(ctx context.Context, req *cpv1.PutConfigRequest) (*cpv1.PutConfigResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	value, err := valueToJSON("value", req.GetValue())
	if err != nil {
		return nil, a.toStatus(err)
	}
	c := model.Config{Key: req.GetKey(), Value: value, Description: req.GetDescription()}
	if err := c.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutConfig(ctx, req.GetNamespace(), c, store.WriteOptions{Actor: who, ExpectedRevision: req.GetExpectedRevision()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("config updated", "namespace", req.GetNamespace(), "key", out.Key, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision, SourceWrite)

	pc, err := configToProto(out)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.PutConfigResponse{Config: pc}, nil
}

func (a *adminService) DeleteConfig(ctx context.Context, req *cpv1.DeleteConfigRequest) (*cpv1.DeleteConfigResponse, error) {
	rev, err := a.delete(ctx, "config", req.GetNamespace(), req.GetKey(), req.GetExpectedRevision(), a.store.DeleteConfig)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteConfigResponse{Revision: rev}, nil
}

// PutFlag merges the request into the stored flag: rollout_percent and salt
// are kept unless the request sets them, and a rollout plan is never touched
// here. Because the merge depends on what was read, the write is a
// compare-and-swap on that read.
//
// Creating a flag cannot be a compare-and-swap: store.WriteOptions has no
// "must not exist" precondition (ExpectedRevision 0 means "unconditional" and
// a missing entry never matches a non-zero value), so two replicas creating
// the same flag at once can silently overwrite each other. Within one process
// creations are serialized and re-read under createFlag, which closes the
// race for a single replica; across replicas the window between the read and
// the write remains until the store gains a create-only precondition.
func (a *adminService) PutFlag(ctx context.Context, req *cpv1.PutFlagRequest) (*cpv1.PutFlagResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	if err := model.ValidateKey(req.GetKey()); err != nil {
		return nil, a.toStatus(err)
	}
	if req.RolloutPercent != nil {
		if err := model.ValidatePercent("rollout_percent", req.GetRolloutPercent()); err != nil {
			return nil, a.toStatus(err)
		}
	}
	read := func() (model.Flag, bool, error) {
		cur, err := a.store.GetFlag(ctx, req.GetNamespace(), req.GetKey())
		if errors.Is(err, model.ErrNotFound) {
			return model.Flag{}, false, nil
		}
		return cur, err == nil, err
	}
	write := func() (model.Flag, error) {
		cur, exists, err := read()
		if err != nil {
			return model.Flag{}, err
		}
		if !exists && req.GetExpectedRevision() == 0 {
			// Look again once no other creator in this process can be
			// between its own read and write.
			a.createFlag.Lock()
			defer a.createFlag.Unlock()
			if cur, exists, err = read(); err != nil {
				return model.Flag{}, err
			}
		}
		expected := req.GetExpectedRevision()
		if exists && expected != 0 && cur.Revision != expected {
			// Report the stale read before judging the request against
			// state the caller has not seen.
			return model.Flag{}, model.Conflictf("flag %q in namespace %q is at revision %d, not %d", cur.Key, req.GetNamespace(), cur.Revision, expected)
		}
		f, err := mergeFlag(req, cur, exists)
		if err != nil {
			return model.Flag{}, err
		}
		if expected == 0 {
			// cur.Revision is zero for a new flag: a missing entry cannot
			// be compared, so creating a flag is a plain upsert.
			expected = cur.Revision
		}
		return a.store.PutFlag(ctx, req.GetNamespace(), f, store.WriteOptions{Actor: who, ExpectedRevision: expected})
	}
	var out model.Flag
	if req.GetExpectedRevision() != 0 {
		// A conflict with the caller's own expected revision is final: no
		// fresh read can make it match again.
		out, err = write()
	} else {
		out, err = retryOnConflict(write)
	}
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("flag updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled,
		"rollout_percent", out.RolloutPercent, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision, SourceWrite)
	return &cpv1.PutFlagResponse{Flag: flagToProto(out)}, nil
}

// mergeFlag applies a PutFlag request to the flag's current version, which is
// the zero Flag when exists is false.
func mergeFlag(req *cpv1.PutFlagRequest, cur model.Flag, exists bool) (model.Flag, error) {
	f := model.Flag{
		Key:            req.GetKey(),
		Enabled:        req.GetEnabled(),
		Description:    req.GetDescription(),
		RolloutPercent: 100,
		Salt:           req.GetSalt(),
		Allowlist:      req.GetAllowlist(),
	}
	if exists {
		f.RolloutPercent, f.Rollout = cur.RolloutPercent, cur.Rollout
		if f.Salt == "" {
			f.Salt = cur.Salt
		}
	}
	if req.RolloutPercent != nil && req.GetRolloutPercent() != f.RolloutPercent {
		if p := f.Rollout; p != nil && p.State.Owns() {
			return model.Flag{}, model.FailedPreconditionf("flag %q: rollout_percent is driven by its %s rollout (%s); abort the rollout to set it by hand",
				f.Key, p.State, rollout.Describe(p))
		}
		f.RolloutPercent = req.GetRolloutPercent()
	}
	f.Normalize()
	return f, f.Validate()
}

func (a *adminService) DeleteFlag(ctx context.Context, req *cpv1.DeleteFlagRequest) (*cpv1.DeleteFlagResponse, error) {
	rev, err := a.delete(ctx, "flag", req.GetNamespace(), req.GetKey(), req.GetExpectedRevision(), a.store.DeleteFlag)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteFlagResponse{Revision: rev}, nil
}

func (a *adminService) PutExperiment(ctx context.Context, req *cpv1.PutExperimentRequest) (*cpv1.PutExperimentResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	variants, err := variantsFromProto(req.GetVariants())
	if err != nil {
		return nil, a.toStatus(err)
	}
	e := model.Experiment{
		Key:         req.GetKey(),
		Enabled:     req.GetEnabled(),
		Description: req.GetDescription(),
		Salt:        req.GetSalt(),
		Variants:    variants,
	}
	e.Normalize()
	if err := e.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutExperiment(ctx, req.GetNamespace(), e, store.WriteOptions{Actor: who, ExpectedRevision: req.GetExpectedRevision()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("experiment updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision, SourceWrite)

	pe, err := experimentToProto(out)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.PutExperimentResponse{Experiment: pe}, nil
}

func (a *adminService) DeleteExperiment(ctx context.Context, req *cpv1.DeleteExperimentRequest) (*cpv1.DeleteExperimentResponse, error) {
	rev, err := a.delete(ctx, "experiment", req.GetNamespace(), req.GetKey(), req.GetExpectedRevision(), a.store.DeleteExperiment)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteExperimentResponse{Revision: rev}, nil
}

// casRetries is how many times a read-modify-write that lost a race with
// another writer starts over before the conflict is returned (ABORTED).
const casRetries = 3

// retryOnConflict runs a read-modify-write until it succeeds, fails with
// something other than model.ErrConflict, or has been retried casRetries
// times. Each attempt must read afresh.
func retryOnConflict[T any](attempt func() (T, error)) (T, error) {
	for retries := 0; ; retries++ {
		v, err := attempt()
		if !errors.Is(err, model.ErrConflict) || retries == casRetries {
			return v, err
		}
	}
}

// delete runs one of the store's Delete* methods and returns a status error.
func (a *adminService) delete(ctx context.Context, kind, namespace, key string, expectedRevision int64,
	del func(ctx context.Context, namespace, key string, opts store.WriteOptions) (int64, error),
) (int64, error) {
	who, err := a.prepareWrite(ctx, namespace)
	if err != nil {
		return 0, a.toStatus(err)
	}
	if err := model.ValidateKey(key); err != nil {
		return 0, a.toStatus(err)
	}
	rev, err := del(ctx, namespace, key, store.WriteOptions{Actor: who, ExpectedRevision: expectedRevision})
	if err != nil {
		return 0, a.toStatus(err)
	}
	a.log.Info(kind+" deleted", "namespace", namespace, "key", key, "revision", rev, "actor", who)
	a.changed(ctx, namespace, rev, SourceWrite)
	return rev, nil
}
