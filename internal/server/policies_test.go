package server

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func TestPoliciesReachWatchers(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)
	ctx := ctxAs(t, "alice")

	rl, err := r.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{
		Namespace: "svc", Key: "checkout", Enabled: true, Description: "d", RequestsPerSecond: 0.5, Burst: 10,
	})
	if err != nil {
		t.Fatalf("PutRateLimit: %v", err)
	}
	if got := rl.GetRateLimit(); got.GetRequestsPerSecond() != 0.5 || got.GetBurst() != 10 || !got.GetEnabled() ||
		got.GetDescription() != "d" || got.GetUpdatedBy() != "alice" || got.GetRevision() != 2 || got.GetUpdatedAt() == nil {
		t.Fatalf("rate limit = %v", got)
	}
	snap := recv(t, stream, propagationBudget)
	if len(snap.GetRateLimits()) != 1 || !proto.Equal(snap.GetRateLimits()[0], rl.GetRateLimit()) {
		t.Fatalf("pushed rate limits = %v, want %v", snap.GetRateLimits(), rl.GetRateLimit())
	}

	cb, err := r.admin.PutCircuitBreaker(ctx, &cpv1.PutCircuitBreakerRequest{
		Namespace: "svc", Key: "payments", Enabled: true, FailureRateThreshold: 0.5, MinRequests: 20,
		Window: durationpb.New(10 * time.Second), OpenDuration: durationpb.New(1500 * time.Millisecond), HalfOpenMaxRequests: 3,
	})
	if err != nil {
		t.Fatalf("PutCircuitBreaker: %v", err)
	}
	if got := cb.GetCircuitBreaker(); got.GetWindow().AsDuration() != 10*time.Second || got.GetOpenDuration().AsDuration() != 1500*time.Millisecond ||
		got.GetFailureRateThreshold() != 0.5 || got.GetMinRequests() != 20 || got.GetHalfOpenMaxRequests() != 3 || got.GetUpdatedBy() != "alice" {
		t.Fatalf("circuit breaker = %v", got)
	}
	snap = recv(t, stream, propagationBudget)
	if len(snap.GetCircuitBreakers()) != 1 || !proto.Equal(snap.GetCircuitBreakers()[0], cb.GetCircuitBreaker()) {
		t.Fatalf("pushed circuit breakers = %v, want %v", snap.GetCircuitBreakers(), cb.GetCircuitBreaker())
	}

	// Reconfiguring with the current revision as precondition.
	rl, err = r.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{
		Namespace: "svc", Key: "checkout", Enabled: true, RequestsPerSecond: 100, Burst: 200, ExpectedRevision: rl.GetRateLimit().GetRevision(),
	})
	if err != nil {
		t.Fatalf("PutRateLimit with expected revision: %v", err)
	}
	if got := recv(t, stream, propagationBudget).GetRateLimits(); len(got) != 1 || got[0].GetBurst() != 200 || got[0].GetRequestsPerSecond() != 100 {
		t.Fatalf("pushed rate limits = %v, want the reconfigured one", got)
	}

	delBreaker, err := r.admin.DeleteCircuitBreaker(ctx, &cpv1.DeleteCircuitBreakerRequest{Namespace: "svc", Key: "payments"})
	if err != nil {
		t.Fatalf("DeleteCircuitBreaker: %v", err)
	}
	if snap := recv(t, stream, propagationBudget); snap.GetRevision() != delBreaker.GetRevision() || len(snap.GetCircuitBreakers()) != 0 {
		t.Fatalf("snapshot after delete = %v", snap)
	}
	_, err = r.admin.DeleteRateLimit(ctx, &cpv1.DeleteRateLimitRequest{Namespace: "svc", Key: "checkout", ExpectedRevision: rl.GetRateLimit().GetRevision() - 1})
	wantCode(t, err, codes.Aborted)
	delLimit, err := r.admin.DeleteRateLimit(ctx, &cpv1.DeleteRateLimitRequest{Namespace: "svc", Key: "checkout", ExpectedRevision: rl.GetRateLimit().GetRevision()})
	if err != nil {
		t.Fatalf("DeleteRateLimit: %v", err)
	}
	if snap := recv(t, stream, propagationBudget); snap.GetRevision() != delLimit.GetRevision() || len(snap.GetRateLimits()) != 0 {
		t.Fatalf("snapshot after delete = %v", snap)
	}
}

func TestPolicyErrors(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	ctx := ctxAs(t, "alice")
	if _, err := r.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{Namespace: "svc", Key: "rl", RequestsPerSecond: 1, Burst: 1}); err != nil {
		t.Fatal(err)
	}
	rev := namespaceRevision(t, r, "svc")

	rateLimit := func(edit func(*cpv1.PutRateLimitRequest)) *cpv1.PutRateLimitRequest {
		req := &cpv1.PutRateLimitRequest{Namespace: "svc", Key: "rl", Enabled: true, RequestsPerSecond: 10, Burst: 20}
		edit(req)
		return req
	}
	breaker := func(edit func(*cpv1.PutCircuitBreakerRequest)) *cpv1.PutCircuitBreakerRequest {
		req := &cpv1.PutCircuitBreakerRequest{
			Namespace: "svc", Key: "cb", Enabled: true, FailureRateThreshold: 0.5, MinRequests: 10,
			Window: durationpb.New(time.Minute), OpenDuration: durationpb.New(time.Second), HalfOpenMaxRequests: 1,
		}
		edit(req)
		return req
	}
	for _, tt := range []struct {
		name string
		call func() error
		code codes.Code
	}{
		{"zero rate", func() error {
			_, err := r.admin.PutRateLimit(ctx, rateLimit(func(req *cpv1.PutRateLimitRequest) { req.RequestsPerSecond = 0 }))
			return err
		}, codes.InvalidArgument},
		{"zero burst", func() error {
			_, err := r.admin.PutRateLimit(ctx, rateLimit(func(req *cpv1.PutRateLimitRequest) { req.Burst = 0 }))
			return err
		}, codes.InvalidArgument},
		{"invalid rate limit key", func() error {
			_, err := r.admin.PutRateLimit(ctx, rateLimit(func(req *cpv1.PutRateLimitRequest) { req.Key = "-rl" }))
			return err
		}, codes.InvalidArgument},
		{"stale rate limit revision", func() error {
			_, err := r.admin.PutRateLimit(ctx, rateLimit(func(req *cpv1.PutRateLimitRequest) { req.ExpectedRevision = rev - 1 }))
			return err
		}, codes.Aborted},
		{"rate limit in missing namespace", func() error {
			_, err := r.admin.PutRateLimit(ctx, rateLimit(func(req *cpv1.PutRateLimitRequest) { req.Namespace = "missing" }))
			return err
		}, codes.NotFound},
		{"missing window", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.Window = nil }))
			return err
		}, codes.InvalidArgument},
		{"missing open duration", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.OpenDuration = nil }))
			return err
		}, codes.InvalidArgument},
		{"malformed window", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) {
				req.Window = &durationpb.Duration{Seconds: 60, Nanos: -1}
			}))
			return err
		}, codes.InvalidArgument},
		{"window below minimum", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.Window = durationpb.New(time.Millisecond) }))
			return err
		}, codes.InvalidArgument},
		{"threshold above 1", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.FailureRateThreshold = 1.5 }))
			return err
		}, codes.InvalidArgument},
		{"zero half-open requests", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.HalfOpenMaxRequests = 0 }))
			return err
		}, codes.InvalidArgument},
		{"expected revision of a missing breaker", func() error {
			_, err := r.admin.PutCircuitBreaker(ctx, breaker(func(req *cpv1.PutCircuitBreakerRequest) { req.ExpectedRevision = rev }))
			return err
		}, codes.Aborted},
		{"delete missing rate limit", func() error {
			_, err := r.admin.DeleteRateLimit(ctx, &cpv1.DeleteRateLimitRequest{Namespace: "svc", Key: "missing"})
			return err
		}, codes.NotFound},
		{"delete missing breaker", func() error {
			_, err := r.admin.DeleteCircuitBreaker(ctx, &cpv1.DeleteCircuitBreakerRequest{Namespace: "svc", Key: "cb"})
			return err
		}, codes.NotFound},
		{"delete with invalid key", func() error {
			_, err := r.admin.DeleteCircuitBreaker(ctx, &cpv1.DeleteCircuitBreakerRequest{Namespace: "svc", Key: "bad key"})
			return err
		}, codes.InvalidArgument},
	} {
		if got := status.Code(tt.call()); got != tt.code {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.code)
		}
	}
	if got := namespaceRevision(t, r, "svc"); got != rev {
		t.Fatalf("namespace revision = %d after failed writes, want %d", got, rev)
	}
}
