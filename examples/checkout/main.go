// Command checkout is a demo HTTP service driven by the control plane
// through the Go SDK (package client). Its configuration, feature flag,
// experiment, rate limit and circuit breaker all come from one namespace, and
// every change to them reaches the running service within a second, without a
// restart.
//
// GET /checkout?user=<id> answers 400 without a user. Otherwise it
//
//   - is rate limited by the rate limit "checkout": a request over the limit
//     gets 429 Too Many Requests with a Retry-After header;
//   - gives the user cart "v2" when the flag "new-cart" is on for them, else
//     cart "v1";
//   - returns the variant of the experiment "cta-color" the user is assigned
//     to, with the variant's payload: the call to action to show. Users the
//     experiment does not enroll get an empty variant and a null payload;
//   - makes a simulated payments call through the circuit breaker "payments".
//     The call takes payments.latency_ms milliseconds (at most 10s) and fails
//     with probability payments.failure_rate, two configs read whenever a
//     call is made. A failed call fails the request with 502. While the
//     breaker rejects calls (it is open, or half-open with its trial calls
//     under way), none is made: the request fails fast with 503.
//
// The JSON response holds the user, the cart, the cta (variant and payload),
// the payment outcome ("ok", "failed" or "unavailable") and the namespace
// revision the service held when the request arrived.
//
// GET /status returns the SDK's Status as JSON: the connection state
// ("connecting", "live" or "disconnected"), the source of the snapshot in use
// ("none", "cache" or "server"), its revision, when it was applied and the
// last watch error.
//
// # Running the demo
//
// Seed the namespace with cpctl:
//
//	cpctl ns create checkout/dev --description "checkout demo"
//	cpctl config put checkout/dev payments.failure_rate 0
//	cpctl config put checkout/dev payments.latency_ms 50
//	cpctl flag put checkout/dev new-cart --enabled --rollout 50
//	cpctl experiment put checkout/dev cta-color --enabled --variants green=50,orange=50 \
//	    --payload 'green={"color":"#2da44e","label":"Buy now"}' \
//	    --payload 'orange={"color":"#fb8500","label":"Complete purchase"}'
//	cpctl ratelimit put checkout/dev checkout --enabled --rps 5 --burst 10
//	cpctl breaker put checkout/dev payments --enabled --failure-rate 0.5 --min-requests 10 \
//	    --window 10s --open-duration 5s --half-open-requests 1
//
// then start the service (or run make example) and call it:
//
//	go run ./examples/checkout --controlplane localhost:9090 --cache-path /tmp/checkout-dev.json
//	curl -s 'localhost:8090/checkout?user=alice'
//	curl -s localhost:8090/status
//
// Bucketing is deterministic: right after this seeding, alice always gets
//
//	{"user":"alice","cart":"v1","cta":{"variant":"orange","payload":{"color":"#fb8500","label":"Complete purchase"}},"payment":"ok","revision":7}
//
// While it runs, each of these changes shows in the next response:
//
//	cpctl flag put checkout/dev new-cart --enabled --rollout 100   # every user gets cart v2
//	cpctl config put checkout/dev payments.failure_rate 1          # 502s until the breaker opens, then fast 503s
//	cpctl config put checkout/dev payments.failure_rate 0          # 5s after opening, a trial call closes the breaker
//	cpctl ratelimit put checkout/dev checkout --enabled --rps 1 --burst 1   # 429s above one request per second
//
// With --cache-path, the service keeps the last snapshot it received on disk
// and starts with it while the control plane is unreachable.
//
// When the control plane requires tokens, pass the service a reader token
// with --token (or CHECKOUT_TOKEN), and cpctl an admin token with its own
// --token (or CPCTL_TOKEN).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jenil133/Controlplane/pkg/client"
)

// shutdownTimeout bounds the wait for in-flight requests on shutdown. It
// exceeds the longest simulated payments call, so they all get to finish.
const shutdownTimeout = 15 * time.Second

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := listenAndRun(cfg, log); err != nil {
		log.Error("checkout exited", "error", err)
		os.Exit(1)
	}
}

// config is read from the command line.
type config struct {
	ControlPlane string
	Namespace    string
	Listen       string
	CachePath    string
	Token        string
}

func parseConfig(args []string, output io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("checkout", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		fmt.Fprintln(output, "Usage: checkout [flags]\n\nA demo service driven by the control plane. Flags:")
		fs.PrintDefaults()
	}
	fs.StringVar(&c.ControlPlane, "controlplane", "localhost:9090", "control plane gRPC address")
	fs.StringVar(&c.Namespace, "namespace", "checkout/dev", "control plane namespace to serve")
	fs.StringVar(&c.Listen, "listen", ":8090", "HTTP listen address")
	fs.StringVar(&c.CachePath, "cache-path", "", "file keeping the last snapshot received, served at startup while the control plane is unreachable; empty disables it")
	// The default is not taken from the environment here, as usage would
	// then print the token.
	fs.StringVar(&c.Token, "token", "", "bearer token for a control plane with auth enabled (default $CHECKOUT_TOKEN)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if c.ControlPlane == "" {
		return config{}, errors.New("--controlplane is required")
	}
	if c.Token == "" {
		c.Token = os.Getenv("CHECKOUT_TOKEN")
	}
	return c, nil
}

// listenAndRun runs the service on cfg.Listen until SIGINT or SIGTERM.
func listenAndRun(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	return run(ctx, cfg, lis, log)
}

// run serves the checkout API on lis, which it closes, until ctx is done.
// Shutdown is graceful: in-flight requests finish before the control plane
// client is closed.
func run(ctx context.Context, cfg config, lis net.Listener, log *slog.Logger) error {
	cp, err := client.New(ctx, client.Options{
		Address:   cfg.ControlPlane,
		Namespace: cfg.Namespace,
		Token:     cfg.Token,
		CachePath: cfg.CachePath,
		Logger:    log,
	})
	if err != nil {
		lis.Close()
		return err
	}
	defer cp.Close()

	srv := &http.Server{
		Handler:           newHandler(cp, rand.Float64),
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	log.Info("checkout listening", "addr", lis.Addr().String(), "controlplane", cfg.ControlPlane, "namespace", cfg.Namespace)

	select {
	case err := <-served:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("requests still running at the shutdown deadline; closing their connections", "error", err)
		srv.Close()
	}
	return nil
}
