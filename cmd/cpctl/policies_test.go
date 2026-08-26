package main

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

func TestPolicies(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop") // revision 1

	rl := decode(t, s.cpctl(t, "--actor", "ops", "ratelimit", "put", "shop", "checkout",
		"--rps", "2.5", "--burst", "10", "--enabled", "--description", "checkout API"), &cpv1.RateLimit{})
	if rl.GetKey() != "checkout" || !rl.GetEnabled() || rl.GetRequestsPerSecond() != 2.5 || rl.GetBurst() != 10 ||
		rl.GetDescription() != "checkout API" || rl.GetUpdatedBy() != "ops" || rl.GetRevision() != 2 {
		t.Fatalf("rate limit = %v", rl)
	}
	cb := decode(t, s.cpctl(t, "breaker", "put", "shop", "payments", "--failure-rate", "0.5", "--min-requests", "20",
		"--window", "10s", "--open-duration", "1m30s", "--half-open-requests", "3", "--enabled"), &cpv1.CircuitBreaker{})
	if cb.GetKey() != "payments" || !cb.GetEnabled() || cb.GetFailureRateThreshold() != 0.5 || cb.GetMinRequests() != 20 ||
		cb.GetWindow().AsDuration() != 10*time.Second || cb.GetOpenDuration().AsDuration() != 90*time.Second ||
		cb.GetHalfOpenMaxRequests() != 3 || cb.GetRevision() != 3 {
		t.Fatalf("circuit breaker = %v", cb)
	}
	snap := decode(t, s.cpctl(t, "snapshot", "shop"), &cpv1.Snapshot{})
	if len(snap.GetRateLimits()) != 1 || len(snap.GetCircuitBreakers()) != 1 {
		t.Fatalf("snapshot policies: %v", snap)
	}

	// A put replaces the whole policy. The long names are aliases.
	rl = decode(t, s.cpctl(t, "rate-limit", "put", "shop", "checkout", "--rps", "100", "--burst", "200", "--expected-revision", "2"), &cpv1.RateLimit{})
	if rl.GetEnabled() || rl.GetRequestsPerSecond() != 100 || rl.GetBurst() != 200 || rl.GetDescription() != "" || rl.GetRevision() != 4 {
		t.Fatalf("rate limit after the second put = %v", rl)
	}

	for _, tc := range []struct {
		code codes.Code
		args []string
	}{
		{codes.InvalidArgument, []string{"ratelimit", "put", "shop", "x", "--burst", "1"}},
		{codes.InvalidArgument, []string{"ratelimit", "put", "shop", "x", "--rps", "1"}},
		{codes.Unknown, []string{"ratelimit", "put", "shop", "x", "--rps", "1", "--burst", "-1"}},
		{codes.Unknown, []string{"ratelimit", "put", "shop", "x", "--rps", "1", "--burst", "4294967296"}},
		{codes.Aborted, []string{"ratelimit", "put", "shop", "checkout", "--rps", "1", "--burst", "1", "--expected-revision", "2"}},
		{codes.InvalidArgument, []string{"breaker", "put", "shop", "y", "--failure-rate", "0.5", "--min-requests", "1",
			"--open-duration", "1s", "--half-open-requests", "1"}},
		{codes.InvalidArgument, []string{"breaker", "put", "shop", "y", "--failure-rate", "1.5", "--min-requests", "1",
			"--window", "10s", "--open-duration", "1s", "--half-open-requests", "1"}},
		{codes.Unknown, []string{"breaker", "put", "shop", "y", "--window", "10"}},
		{codes.Aborted, []string{"breaker", "delete", "shop", "payments", "--expected-revision", "2"}},
		{codes.NotFound, []string{"ratelimit", "delete", "shop", "missing"}},
		{codes.NotFound, []string{"breaker", "delete", "shop", "missing"}},
		{codes.NotFound, []string{"ratelimit", "put", "nope", "x", "--rps", "1", "--burst", "1"}},
		{codes.Unknown, []string{"breaker", "reset", "shop", "payments"}},
		{codes.Unknown, []string{"ratelimit"}},
	} {
		s.fail(t, tc.code, tc.args...)
	}

	if out := s.cpctl(t, "ratelimit", "delete", "shop", "checkout", "--expected-revision", "4"); out != "deleted rate limit checkout (namespace shop now at revision 5)\n" {
		t.Fatalf("delete output: %q", out)
	}
	if out := s.cpctl(t, "circuit-breaker", "delete", "shop", "payments"); out != "deleted circuit breaker payments (namespace shop now at revision 6)\n" {
		t.Fatalf("delete output: %q", out)
	}
	if snap := decode(t, s.cpctl(t, "snapshot", "shop"), &cpv1.Snapshot{}); len(snap.GetRateLimits())+len(snap.GetCircuitBreakers()) != 0 {
		t.Fatalf("policies left after delete: %v", snap)
	}
}
