// Package server implements the AdminService and DistributionService gRPC
// APIs on top of a store, a hub and a notifier.
//
// Write path: AdminService commits the change (new namespace revision), then
// publishes {namespace, revision} on the notifier and refreshes the local hub.
// Every replica, including this one, hears the event and refreshes its hub,
// which pushes the new snapshot down every Watch stream for that namespace.
// A periodic reconciler compares cached revisions against the store so a
// replica that missed an event still converges.
//
// Flag rollouts are read-modify-write cycles with a compare-and-swap on the
// revision read, so every replica can run the rollout controller that
// advances due stages: when several race, exactly one write wins and the
// others skip. Controller advances propagate like any other write.
package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/hub"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/rollout"
	"github.com/Jenil133/Controlplane/internal/store"
)

// ActorHeader is the gRPC metadata key naming who made a change.
const ActorHeader = "x-controlplane-actor"

const (
	defaultActor             = "anonymous"
	defaultReconcileInterval = 10 * time.Second
	publishTimeout           = 2 * time.Second
	refreshTimeout           = 5 * time.Second
	maxResubscribeBackoff    = 30 * time.Second
)

// Options configures a Server.
type Options struct {
	Store    store.Store
	Notifier notify.Notifier
	Logger   *slog.Logger
	// ReconcileInterval is how often cached revisions are checked against the
	// store. Defaults to 10s.
	ReconcileInterval time.Duration
	// RolloutInterval is how often due rollout stages are advanced. Zero
	// disables the rollout controller on this replica.
	RolloutInterval time.Duration
	// Observer receives events for metrics. Defaults to NopObserver.
	Observer Observer
}

// Server holds the state shared by both gRPC services.
type Server struct {
	store             store.Store
	notifier          notify.Notifier
	log               *slog.Logger
	hub               *hub.Hub
	reconcileInterval time.Duration
	rolloutInterval   time.Duration
	observer          Observer
	admin             *adminService
	distribution      *distributionService

	// now times rollout transitions, both the ones users request and the
	// controller's. Tests replace it to move rollouts along on demand.
	now func() time.Time
}

// New builds a Server. Call Register to expose it and Run to start
// cross-replica change propagation.
func New(opts Options) *Server {
	s := &Server{
		store:             opts.Store,
		notifier:          opts.Notifier,
		log:               opts.Logger,
		reconcileInterval: opts.ReconcileInterval,
		rolloutInterval:   opts.RolloutInterval,
		observer:          opts.Observer,
		now:               func() time.Time { return time.Now().UTC() },
	}
	if s.notifier == nil {
		s.notifier = notify.NewLocal()
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.reconcileInterval <= 0 {
		s.reconcileInterval = defaultReconcileInterval
	}
	if s.observer == nil {
		s.observer = NopObserver{}
	}
	s.hub = hub.New(s.loadSnapshot)
	s.admin = &adminService{Server: s}
	s.distribution = &distributionService{Server: s}
	return s
}

// Register adds both services to g.
func (s *Server) Register(g grpc.ServiceRegistrar) {
	cpv1.RegisterAdminServiceServer(g, s.admin)
	cpv1.RegisterDistributionServiceServer(g, s.distribution)
}

// AdminServer returns the AdminService implementation, for in-process
// callers such as the HTTP API.
func (s *Server) AdminServer() cpv1.AdminServiceServer { return s.admin }

// DistributionServer returns the DistributionService implementation.
func (s *Server) DistributionServer() cpv1.DistributionServiceServer { return s.distribution }

// Run receives change events from other replicas, reconciles periodically
// and, when Options.RolloutInterval is set, advances due rollout stages,
// until ctx is done. It returns once all of that has stopped.
func (s *Server) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() { s.runNotifier(ctx) })
	if s.rolloutInterval > 0 {
		controller := rollout.NewController(rollout.ControllerConfig{
			Store:    s.store,
			Interval: s.rolloutInterval,
			Now:      s.now,
			Logger:   s.log,
			OnChange: func(ctx context.Context, namespace string, revision int64) {
				s.changed(ctx, namespace, revision, SourceRollout)
			},
		})
		wg.Go(func() { _ = controller.Run(ctx) })
	}

	ticker := time.NewTicker(s.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.reconcile(ctx)
		}
	}
}

// Shutdown ends every Watch stream with UNAVAILABLE so clients reconnect to
// another replica. It must be called before grpc.Server.GracefulStop, which
// otherwise waits forever on the long-lived streams.
func (s *Server) Shutdown() {
	s.hub.Close()
}

// Watchers returns the number of open Watch streams on this replica.
func (s *Server) Watchers() int {
	return s.hub.Watchers()
}

func (s *Server) runNotifier(ctx context.Context) {
	backoff := time.Second
	for {
		err := s.notifier.Run(ctx, func(ev notify.Event) {
			// Loading a snapshot can take a while; never stall the event loop.
			go s.refresh(context.WithoutCancel(ctx), ev.Namespace, ev.Revision, SourceNotifier)
		})
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("change subscription lost; relying on reconciler until it is back", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxResubscribeBackoff)
	}
}

func (s *Server) refresh(ctx context.Context, namespace string, revision int64, source ChangeSource) {
	s.observer.ChangeReceived(namespace, source)
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	if err := s.hub.Notify(ctx, namespace, revision); err != nil {
		s.log.Warn("refresh watchers failed", "namespace", namespace, "revision", revision, "error", err)
	}
}

func (s *Server) reconcile(ctx context.Context) {
	watched := s.hub.Revisions()
	if len(watched) == 0 {
		return
	}
	namespaces, err := s.store.ListNamespaces(ctx)
	if err != nil {
		s.log.Warn("reconcile: list namespaces failed", "error", err)
		return
	}
	for _, ns := range namespaces {
		if cached, ok := watched[ns.Name]; ok && ns.Revision > cached {
			s.log.Info("reconcile: watchers behind store, refreshing", "namespace", ns.Name, "cached", cached, "current", ns.Revision)
			s.refresh(ctx, ns.Name, ns.Revision, SourceReconcile)
		}
	}
}

// changed is called after a committed write. Publishing is asynchronous so a
// slow or unreachable Redis never delays the write; events may then arrive
// out of order, which is harmless because the hub ignores older revisions.
func (s *Server) changed(ctx context.Context, namespace string, revision int64, source ChangeSource) {
	ctx = context.WithoutCancel(ctx)
	go s.publish(ctx, namespace, revision)
	// Local watchers do not wait for the round trip through the notifier.
	s.refresh(ctx, namespace, revision, source)
}

func (s *Server) publish(ctx context.Context, namespace string, revision int64) {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := s.notifier.Publish(ctx, notify.Event{Namespace: namespace, Revision: revision}); err != nil {
		s.observer.PublishFailed(namespace)
		s.log.Warn("publish change failed; other replicas will catch up on reconcile", "namespace", namespace, "revision", revision, "error", err)
	}
}

func (s *Server) loadSnapshot(ctx context.Context, namespace string) (*cpv1.Snapshot, error) {
	snap, err := s.store.Snapshot(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return SnapshotToProto(snap)
}

// toStatus maps domain errors to gRPC status errors.
func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, model.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, model.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, model.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, model.ErrConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, model.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, hub.ErrClosed):
		return status.Error(codes.Unavailable, "server is shutting down, reconnect")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		s.log.Error("internal error", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// actor returns who is making the request: the authenticated principal when
// auth is enabled, otherwise the ActorHeader metadata (trusted as given).
func actor(ctx context.Context) (string, error) {
	if p, ok := auth.PrincipalFrom(ctx); ok {
		return p.Name, nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get(ActorHeader)
	if len(vals) == 0 || vals[0] == "" {
		return defaultActor, nil
	}
	if len(vals[0]) > model.MaxActorLen {
		return "", model.Invalidf("%s is longer than %d characters", ActorHeader, model.MaxActorLen)
	}
	return vals[0], nil
}
