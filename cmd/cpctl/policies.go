package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"google.golang.org/protobuf/types/known/durationpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// Traffic policies are written in full on every put, like the other entries:
// the server validates them, so missing settings fail there with the allowed
// range rather than silently taking a default.

func (c *cli) rateLimit(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ratelimit: missing subcommand (put, delete); %w", errUsage)
	}
	fs := flag.NewFlagSet("ratelimit "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	enabled := fs.Bool("enabled", false, "")
	rps := floatFlag(fs, "rps")
	burst := uint32Flag(fs, "burst")
	expected := fs.Int64("expected-revision", 0, "")
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.PutRateLimit(ctx, &cpv1.PutRateLimitRequest{
			Namespace:         pos[0],
			Key:               pos[1],
			Enabled:           *enabled,
			Description:       *description,
			RequestsPerSecond: *rps,
			Burst:             *burst,
			ExpectedRevision:  *expected,
		})
		if err != nil {
			return err
		}
		return c.print(resp.GetRateLimit())
	case "delete":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.DeleteRateLimit(ctx, &cpv1.DeleteRateLimitRequest{Namespace: pos[0], Key: pos[1], ExpectedRevision: *expected})
		if err != nil {
			return err
		}
		c.deleted("rate limit", pos[1], pos[0], resp.GetRevision())
		return nil
	default:
		return fmt.Errorf("ratelimit: unknown subcommand %q; %w", args[0], errUsage)
	}
}

func (c *cli) breaker(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("breaker: missing subcommand (put, delete); %w", errUsage)
	}
	fs := flag.NewFlagSet("breaker "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	enabled := fs.Bool("enabled", false, "")
	failureRate := floatFlag(fs, "failure-rate")
	minRequests := uint32Flag(fs, "min-requests")
	window := fs.Duration("window", 0, "")
	openDuration := fs.Duration("open-duration", 0, "")
	halfOpenRequests := uint32Flag(fs, "half-open-requests")
	expected := fs.Int64("expected-revision", 0, "")
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.PutCircuitBreaker(ctx, &cpv1.PutCircuitBreakerRequest{
			Namespace:            pos[0],
			Key:                  pos[1],
			Enabled:              *enabled,
			Description:          *description,
			FailureRateThreshold: *failureRate,
			MinRequests:          *minRequests,
			Window:               durationpb.New(*window),
			OpenDuration:         durationpb.New(*openDuration),
			HalfOpenMaxRequests:  *halfOpenRequests,
			ExpectedRevision:     *expected,
		})
		if err != nil {
			return err
		}
		return c.print(resp.GetCircuitBreaker())
	case "delete":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.DeleteCircuitBreaker(ctx, &cpv1.DeleteCircuitBreakerRequest{Namespace: pos[0], Key: pos[1], ExpectedRevision: *expected})
		if err != nil {
			return err
		}
		c.deleted("circuit breaker", pos[1], pos[0], resp.GetRevision())
		return nil
	default:
		return fmt.Errorf("breaker: unknown subcommand %q; %w", args[0], errUsage)
	}
}
