package server

import (
	"context"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
)

type adminService struct {
	cpv1.UnimplementedAdminServiceServer
	*Server
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
	out, err := a.store.CreateNamespace(ctx, ns)
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
	c := model.Config{Key: req.GetKey(), Value: value, Description: req.GetDescription(), UpdatedBy: who}
	if err := c.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutConfig(ctx, req.GetNamespace(), c)
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("config updated", "namespace", req.GetNamespace(), "key", out.Key, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision)

	pc, err := configToProto(out)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.PutConfigResponse{Config: pc}, nil
}

func (a *adminService) DeleteConfig(ctx context.Context, req *cpv1.DeleteConfigRequest) (*cpv1.DeleteConfigResponse, error) {
	rev, err := a.delete(ctx, "config", req.GetNamespace(), req.GetKey(), a.store.DeleteConfig)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteConfigResponse{Revision: rev}, nil
}

func (a *adminService) PutFlag(ctx context.Context, req *cpv1.PutFlagRequest) (*cpv1.PutFlagResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	f := model.Flag{Key: req.GetKey(), Enabled: req.GetEnabled(), Description: req.GetDescription(), UpdatedBy: who}
	if err := f.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutFlag(ctx, req.GetNamespace(), f)
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("flag updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision)
	return &cpv1.PutFlagResponse{Flag: flagToProto(out)}, nil
}

func (a *adminService) DeleteFlag(ctx context.Context, req *cpv1.DeleteFlagRequest) (*cpv1.DeleteFlagResponse, error) {
	rev, err := a.delete(ctx, "flag", req.GetNamespace(), req.GetKey(), a.store.DeleteFlag)
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
		UpdatedBy:   who,
	}
	e.Normalize()
	if err := e.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutExperiment(ctx, req.GetNamespace(), e)
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("experiment updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision)

	pe, err := experimentToProto(out)
	if err != nil {
		return nil, a.toStatus(err)
	}
	return &cpv1.PutExperimentResponse{Experiment: pe}, nil
}

func (a *adminService) DeleteExperiment(ctx context.Context, req *cpv1.DeleteExperimentRequest) (*cpv1.DeleteExperimentResponse, error) {
	rev, err := a.delete(ctx, "experiment", req.GetNamespace(), req.GetKey(), a.store.DeleteExperiment)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteExperimentResponse{Revision: rev}, nil
}

// delete runs one of the store's Delete* methods and returns a status error.
func (a *adminService) delete(ctx context.Context, kind, namespace, key string,
	del func(ctx context.Context, namespace, key string) (int64, error),
) (int64, error) {
	who, err := a.prepareWrite(ctx, namespace)
	if err != nil {
		return 0, a.toStatus(err)
	}
	if err := model.ValidateKey(key); err != nil {
		return 0, a.toStatus(err)
	}
	rev, err := del(ctx, namespace, key)
	if err != nil {
		return 0, a.toStatus(err)
	}
	a.log.Info(kind+" deleted", "namespace", namespace, "key", key, "revision", rev, "actor", who)
	a.changed(ctx, namespace, rev)
	return rev, nil
}
