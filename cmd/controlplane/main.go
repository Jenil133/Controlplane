// Command controlplane runs the control plane server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
	"github.com/Jenil133/Controlplane/internal/store/postgres"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := newLogger(cfg)
	if err := run(cfg, log); err != nil {
		log.Error("controlplane exited", "error", err)
		os.Exit(1)
	}
}

func newLogger(cfg config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func run(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer st.Close()

	notifier, closeNotifier, err := openNotifier(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer closeNotifier()

	srv := server.New(server.Options{
		Store:             st,
		Notifier:          notifier,
		Logger:            log,
		ReconcileInterval: cfg.ReconcileInterval,
	})

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Detect dead watchers behind NATs and load balancers.
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	srv.Register(grpcServer)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}

	var shuttingDown atomic.Bool
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           healthHandler(st, &shuttingDown),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 3)
	go func() {
		if err := srv.Run(ctx); err != nil {
			errc <- err
		}
	}()
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errc <- fmt.Errorf("grpc server: %w", err)
		}
	}()
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http server: %w", err)
		}
	}()
	log.Info("controlplane started", "grpc_addr", lis.Addr().String(), "http_addr", cfg.HTTPAddr, "store", cfg.Store, "redis", cfg.RedisURL != "")

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err = <-errc:
		log.Error("component failed, shutting down", "error", err)
	}

	// Fail readiness first so load balancers stop routing, then end the
	// long-lived Watch streams so GracefulStop can finish.
	shuttingDown.Store(true)
	healthServer.Shutdown()
	srv.Shutdown()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("graceful stop timed out, forcing")
		grpcServer.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Info("controlplane stopped")
	return err
}

func openStore(ctx context.Context, cfg config, log *slog.Logger) (store.Store, error) {
	if cfg.Store == "memory" {
		log.Warn("using in-memory store; all state is lost on exit")
		return memory.New(), nil
	}
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pg, err := postgres.Open(connectCtx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Migrate {
		if err := pg.Migrate(connectCtx); err != nil {
			pg.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
		log.Info("database migrations applied")
	}
	return pg, nil
}

func openNotifier(ctx context.Context, cfg config, log *slog.Logger) (notify.Notifier, func(), error) {
	if cfg.RedisURL == "" {
		log.Info("no Redis configured; change events stay inside this replica")
		return notify.NewLocal(), func() {}, nil
	}
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse redis url: %w", err)
	}
	redis.SetLogger(redisLogger{log.With("component", "redis")})
	client := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		// Not fatal: the reconciler keeps replicas converging until Redis is back.
		log.Warn("redis unreachable at startup; will keep retrying", "error", err)
	}
	return notify.NewRedis(client, cfg.RedisChannel, log), func() { client.Close() }, nil
}

// redisLogger routes go-redis internal logging through slog.
type redisLogger struct{ log *slog.Logger }

func (l redisLogger) Printf(ctx context.Context, format string, v ...any) {
	l.log.WarnContext(ctx, fmt.Sprintf(format, v...))
}

// healthHandler serves /healthz (process alive) and /readyz (able to serve).
func healthHandler(st store.Store, shuttingDown *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	return mux
}
