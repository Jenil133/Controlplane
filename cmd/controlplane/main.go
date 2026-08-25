// Command controlplane runs the control plane server: the gRPC AdminService
// and DistributionService on one port, and the JSON API, admin UI,
// Prometheus metrics and health checks on an HTTP port.
//
//	controlplane [flags]           serve until SIGINT or SIGTERM
//	controlplane migrate [flags]   apply the database migrations and exit
//
// "controlplane -h" lists the flags and their CONTROLPLANE_* environment
// variables.
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/httpapi"
	"github.com/Jenil133/Controlplane/internal/metrics"
	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/server"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
	"github.com/Jenil133/Controlplane/internal/store/postgres"
)

const (
	// connectTimeout bounds the wait for PostgreSQL at startup, after which
	// the process exits and its supervisor restarts it.
	connectTimeout = 30 * time.Second
	// readyTimeout bounds the store ping behind /readyz; the Kubernetes
	// readiness probe allows 3s for the whole request.
	readyTimeout = 2 * time.Second
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if cfg.Command == commandMigrate {
		err = migrate(ctx, cfg, log)
	} else {
		err = run(ctx, cfg, log)
	}
	stop()
	if err != nil {
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

// run serves until ctx is done and then shuts down gracefully. It returns an
// error if startup fails or a server stops on its own.
func run(ctx context.Context, cfg config, log *slog.Logger) error {
	a, err := open(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer a.close()

	// Listen only once the store is connected and migrated: the Kubernetes
	// startup probe takes an answer on /healthz to mean exactly that.
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}
	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		grpcLis.Close()
		return fmt.Errorf("listen %s: %w", cfg.HTTPAddr, err)
	}
	return a.serve(ctx, grpcLis, httpLis)
}

// migrate applies the database migrations and returns, so that a job can
// migrate once before a release rolls out instead of every replica doing it
// on startup.
func migrate(ctx context.Context, cfg config, log *slog.Logger) error {
	cfg.Migrate = true
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	st.Close()
	return nil
}

// app holds what the servers are built on.
type app struct {
	cfg           config
	log           *slog.Logger
	store         store.Store
	notifier      notify.Notifier
	closeNotifier func()
	// auth is nil when authentication is disabled.
	auth *auth.Authenticator
}

// open loads the API tokens, then connects the store and the notifier. The
// tokens come first so that a broken tokens file fails at once instead of
// after waiting for the database.
func open(ctx context.Context, cfg config, log *slog.Logger) (*app, error) {
	authn, err := loadAuth(cfg, log)
	if err != nil {
		return nil, err
	}
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return nil, err
	}
	notifier, closeNotifier, err := openNotifier(ctx, cfg, log)
	if err != nil {
		st.Close()
		return nil, err
	}
	return &app{cfg: cfg, log: log, store: st, notifier: notifier, closeNotifier: closeNotifier, auth: authn}, nil
}

// close releases the notifier and the store once serve has returned.
func (a *app) close() {
	a.closeNotifier()
	a.store.Close()
}

// serve runs the gRPC server on grpcLis and the HTTP server on httpLis until
// ctx is done or one of them fails, then shuts down gracefully. It returns
// once the servers have stopped and change propagation has stopped too or
// has had the shutdown timeout to do so. The error is the failure that ended
// serving, if any.
func (a *app) serve(ctx context.Context, grpcLis, httpLis net.Listener) error {
	m := metrics.New()
	srv := server.New(server.Options{
		Store:             a.store,
		Notifier:          a.notifier,
		Logger:            a.log,
		ReconcileInterval: a.cfg.ReconcileInterval,
		RolloutInterval:   a.cfg.RolloutInterval,
		Observer:          m,
	})
	grpcServer := a.newGRPCServer(m)
	srv.Register(grpcServer)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	var shuttingDown atomic.Bool
	httpServer := &http.Server{
		Handler:           a.httpHandler(srv, m, &shuttingDown),
		ReadHeaderTimeout: 5 * time.Second,
		// Also bounds trickled request bodies; an API body is at most 1 MiB.
		ReadTimeout: 30 * time.Second,
		ErrorLog:    slog.NewLogLogger(a.log.Handler(), slog.LevelWarn),
	}

	errc := make(chan error, 3)
	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := srv.Run(runCtx); err != nil {
			errc <- fmt.Errorf("change propagation: %w", err)
		}
	}()
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			errc <- fmt.Errorf("grpc server: %w", err)
		}
	})
	wg.Go(func() {
		if err := httpServer.Serve(httpLis); !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http server: %w", err)
		}
	})
	a.log.Info("controlplane started",
		"grpc_addr", grpcLis.Addr().String(), "http_addr", httpLis.Addr().String(),
		"store", a.cfg.Store, "redis", a.cfg.RedisURL != "", "auth", a.auth != nil,
		"rollout_interval", a.cfg.RolloutInterval)

	var err error
	select {
	case <-ctx.Done():
		a.log.Info("shutting down")
	case err = <-errc:
		a.log.Error("component failed, shutting down", "error", err)
	}

	// Fail readiness first so load balancers stop routing here, then end the
	// long-lived Watch streams, with UNAVAILABLE so that clients reconnect to
	// another replica; GracefulStop would wait for them forever.
	shuttingDown.Store(true)
	healthServer.Shutdown()
	srv.Shutdown()
	stopRun()
	// Change propagation stops at once, except that go-redis waits without a
	// deadline for Redis to confirm a subscription: a Redis lost to a network
	// partition must not hold up the exit. Its grace period runs alongside
	// the servers' so that shutdown still fits twice the shutdown timeout.
	runGrace := time.NewTimer(a.cfg.ShutdownTimeout)
	defer runGrace.Stop()
	a.stopGRPC(grpcServer)
	a.stopHTTP(httpServer)
	wg.Wait()
	select {
	case <-runDone:
	case <-runGrace.C:
		a.log.Warn("change propagation did not stop in time; exiting without it")
	}
	a.log.Info("controlplane stopped")
	return err
}

// newGRPCServer returns a gRPC server that counts every call and, with auth
// enabled, authorizes it. Metrics come first so that rejected calls are
// counted too.
func (a *app) newGRPCServer(m *metrics.Metrics) *grpc.Server {
	unary := []grpc.UnaryServerInterceptor{m.UnaryServerInterceptor()}
	stream := []grpc.StreamServerInterceptor{m.StreamServerInterceptor()}
	if a.auth != nil {
		unary = append(unary, a.auth.UnaryServerInterceptor())
		stream = append(stream, a.auth.StreamServerInterceptor())
	}
	return grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Detect dead watchers behind NATs and load balancers.
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	)
}

// httpHandler serves the probes and metrics, which never need a token, and
// hands every other path to the JSON API and admin UI.
func (a *app) httpHandler(srv *server.Server, m *metrics.Metrics, shuttingDown *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	// Liveness: the process is up and serving HTTP.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	// Readiness: able to serve requests, i.e. not shutting down and the store
	// answers.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := a.store.Ping(ctx); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.Handle("GET /metrics", m.Handler())
	mux.Handle("/", httpapi.New(httpapi.Options{
		Admin:        srv.AdminServer(),
		Distribution: srv.DistributionServer(),
		Auth:         a.auth,
		Logger:       a.log,
	}))
	return mux
}

// stopGRPC lets in-flight calls finish for up to the shutdown timeout, then
// closes the remaining connections.
func (a *app) stopGRPC(g *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(stopped)
	}()
	timer := time.NewTimer(a.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		a.log.Warn("gRPC graceful stop timed out, closing remaining connections")
		g.Stop()
		<-stopped
	}
}

// stopHTTP is stopGRPC for the HTTP server, with a timeout of its own.
func (a *app) stopHTTP(h *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		a.log.Warn("HTTP graceful shutdown timed out, closing remaining connections", "error", err)
		_ = h.Close()
	}
}

// loadAuth returns the authenticator for the configured tokens file, or nil
// when there is none. A file that cannot be loaded is an error, never a
// reason to run without authentication.
func loadAuth(cfg config, log *slog.Logger) (*auth.Authenticator, error) {
	if cfg.AuthTokensFile == "" {
		log.Warn("AUTHENTICATION IS DISABLED: every caller can read and change everything; set --auth-tokens-file (" + envPrefix + "AUTH_TOKENS_FILE) to require API tokens")
		return nil, nil
	}
	authn, err := auth.LoadFile(cfg.AuthTokensFile)
	if err != nil {
		return nil, err
	}
	log.Info("authentication enabled", "tokens_file", cfg.AuthTokensFile)
	return authn, nil
}

func openStore(ctx context.Context, cfg config, log *slog.Logger) (store.Store, error) {
	if cfg.Store == "memory" {
		log.Warn("using in-memory store; all state is lost on exit")
		return memory.New(), nil
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	pg, err := postgres.Open(connectCtx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Migrate {
		// Not bounded like connecting: a migration may take a while on a big
		// table, and replicas starting together wait for each other's.
		if err := pg.Migrate(ctx); err != nil {
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
