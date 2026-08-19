package server

import (
	"context"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
)

// Traffic policies are stored and pushed like every other entry; clients
// enforce them locally (pkg/ratelimit, pkg/breaker).

func (a *adminService) PutRateLimit(ctx context.Context, req *cpv1.PutRateLimitRequest) (*cpv1.PutRateLimitResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	r := model.RateLimit{
		Key:               req.GetKey(),
		Enabled:           req.GetEnabled(),
		Description:       req.GetDescription(),
		RequestsPerSecond: req.GetRequestsPerSecond(),
		Burst:             req.GetBurst(),
	}
	if err := r.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutRateLimit(ctx, req.GetNamespace(), r, store.WriteOptions{Actor: who, ExpectedRevision: req.GetExpectedRevision()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("rate limit updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled,
		"requests_per_second", out.RequestsPerSecond, "burst", out.Burst, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision, SourceWrite)
	return &cpv1.PutRateLimitResponse{RateLimit: rateLimitToProto(out)}, nil
}

func (a *adminService) DeleteRateLimit(ctx context.Context, req *cpv1.DeleteRateLimitRequest) (*cpv1.DeleteRateLimitResponse, error) {
	rev, err := a.delete(ctx, "rate limit", req.GetNamespace(), req.GetKey(), req.GetExpectedRevision(), a.store.DeleteRateLimit)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteRateLimitResponse{Revision: rev}, nil
}

func (a *adminService) PutCircuitBreaker(ctx context.Context, req *cpv1.PutCircuitBreakerRequest) (*cpv1.PutCircuitBreakerResponse, error) {
	who, err := a.prepareWrite(ctx, req.GetNamespace())
	if err != nil {
		return nil, a.toStatus(err)
	}
	window, err := durationFromProto("window", req.GetWindow())
	if err != nil {
		return nil, a.toStatus(err)
	}
	openFor, err := durationFromProto("open_duration", req.GetOpenDuration())
	if err != nil {
		return nil, a.toStatus(err)
	}
	c := model.CircuitBreaker{
		Key:                  req.GetKey(),
		Enabled:              req.GetEnabled(),
		Description:          req.GetDescription(),
		FailureRateThreshold: req.GetFailureRateThreshold(),
		MinRequests:          req.GetMinRequests(),
		Window:               window,
		OpenDuration:         openFor,
		HalfOpenMaxRequests:  req.GetHalfOpenMaxRequests(),
	}
	if err := c.Validate(); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := a.store.PutCircuitBreaker(ctx, req.GetNamespace(), c, store.WriteOptions{Actor: who, ExpectedRevision: req.GetExpectedRevision()})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("circuit breaker updated", "namespace", req.GetNamespace(), "key", out.Key, "enabled", out.Enabled,
		"failure_rate_threshold", out.FailureRateThreshold, "revision", out.Revision, "actor", who)
	a.changed(ctx, req.GetNamespace(), out.Revision, SourceWrite)
	return &cpv1.PutCircuitBreakerResponse{CircuitBreaker: circuitBreakerToProto(out)}, nil
}

func (a *adminService) DeleteCircuitBreaker(ctx context.Context, req *cpv1.DeleteCircuitBreakerRequest) (*cpv1.DeleteCircuitBreakerResponse, error) {
	rev, err := a.delete(ctx, "circuit breaker", req.GetNamespace(), req.GetKey(), req.GetExpectedRevision(), a.store.DeleteCircuitBreaker)
	if err != nil {
		return nil, err
	}
	return &cpv1.DeleteCircuitBreakerResponse{Revision: rev}, nil
}
