package server

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/notify"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

// propagationBudget is the hot-reload target: a committed write must reach
// every watcher within this time.
const propagationBudget = time.Second

type replica struct {
	srv   *Server
	admin cpv1.AdminServiceClient
	dist  cpv1.DistributionServiceClient
}

type replicaOptions struct {
	notifier  notify.Notifier
	reconcile time.Duration
}

// startReplica runs a Server over an in-memory gRPC connection.
func startReplica(t *testing.T, st store.Store, opts replicaOptions) *replica {
	t.Helper()
	srv := New(Options{
		Store:             st,
		Notifier:          opts.notifier,
		Logger:            slog.New(slog.DiscardHandler),
		ReconcileInterval: opts.reconcile,
	})
	g := grpc.NewServer()
	srv.Register(g)

	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Run(ctx) }()
	go func() { _ = g.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Shutdown()
		g.Stop()
		cancel()
	})
	return &replica{srv: srv, admin: cpv1.NewAdminServiceClient(conn), dist: cpv1.NewDistributionServiceClient(conn)}
}

func ctxAs(t *testing.T, who string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	if who != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, ActorHeader, who)
	}
	return ctx
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("got %v (%v), want code %v", got, err, code)
	}
}

func mustValue(t *testing.T, v any) *structpb.Value {
	t.Helper()
	pv, err := structpb.NewValue(v)
	if err != nil {
		t.Fatal(err)
	}
	return pv
}

func createNamespace(t *testing.T, r *replica, name string) {
	t.Helper()
	if _, err := r.admin.CreateNamespace(ctxAs(t, "test"), &cpv1.CreateNamespaceRequest{Name: name}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
}

func putConfig(t *testing.T, r *replica, ns, key string, v any) int64 {
	t.Helper()
	resp, err := r.admin.PutConfig(ctxAs(t, "test"), &cpv1.PutConfigRequest{Namespace: ns, Key: key, Value: mustValue(t, v)})
	if err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	return resp.GetConfig().GetRevision()
}

type watchStream = grpc.ServerStreamingClient[cpv1.WatchResponse]

func watch(t *testing.T, r *replica, ns string, known int64) watchStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := r.dist.Watch(ctx, &cpv1.WatchRequest{Namespace: ns, KnownRevision: known, ClientId: t.Name()})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	return stream
}

// recv waits for the next snapshot, failing after timeout.
func recv(t *testing.T, s watchStream, timeout time.Duration) *cpv1.Snapshot {
	t.Helper()
	type result struct {
		resp *cpv1.WatchResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := s.Recv()
		ch <- result{resp, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Recv: %v", r.err)
		}
		return r.resp.GetSnapshot()
	case <-time.After(timeout):
		t.Fatalf("no snapshot within %v", timeout)
		return nil
	}
}

func TestAdminErrors(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	ctx := ctxAs(t, "alice")

	_, err := r.admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: "Bad Name"})
	wantCode(t, err, codes.InvalidArgument)

	createNamespace(t, r, "svc")
	_, err = r.admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: "svc"})
	wantCode(t, err, codes.AlreadyExists)

	_, err = r.admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "missing"})
	wantCode(t, err, codes.NotFound)

	_, err = r.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: "missing", Key: "k", Value: mustValue(t, 1)})
	wantCode(t, err, codes.NotFound)

	_, err = r.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: "svc", Key: "k"})
	wantCode(t, err, codes.InvalidArgument)

	_, err = r.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: "svc", Key: "bad key", Value: mustValue(t, 1)})
	wantCode(t, err, codes.InvalidArgument)

	_, err = r.admin.PutExperiment(ctx, &cpv1.PutExperimentRequest{
		Namespace: "svc", Key: "exp",
		Variants: []*cpv1.Variant{{Name: "a", Weight: 1}, {Name: "a", Weight: 1}},
	})
	wantCode(t, err, codes.InvalidArgument)

	_, err = r.admin.DeleteFlag(ctx, &cpv1.DeleteFlagRequest{Namespace: "svc", Key: "missing"})
	wantCode(t, err, codes.NotFound)

	_, err = r.admin.PutFlag(ctxAs(t, strings.Repeat("x", 200)), &cpv1.PutFlagRequest{Namespace: "svc", Key: "f"})
	wantCode(t, err, codes.InvalidArgument)

	_, err = r.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "missing"})
	wantCode(t, err, codes.NotFound)

	// None of the failed writes may have produced a revision.
	ns, err := r.admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if ns.GetNamespace().GetRevision() != 1 {
		t.Fatalf("revision = %d after failed writes, want 1", ns.GetNamespace().GetRevision())
	}
}

func TestWritesShowUpInSnapshot(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "checkout/prod")
	ctx := ctxAs(t, "alice")

	value := mustValue(t, map[string]any{"timeout_ms": 250.0, "hosts": []any{"a", "b"}})
	if _, err := r.admin.PutConfig(ctx, &cpv1.PutConfigRequest{Namespace: "checkout/prod", Key: "upstream", Value: value, Description: "d"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "checkout/prod", Key: "new-cart", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	payload := mustValue(t, map[string]any{"color": "green"})
	exp, err := r.admin.PutExperiment(ctx, &cpv1.PutExperimentRequest{
		Namespace: "checkout/prod", Key: "button", Enabled: true,
		Variants: []*cpv1.Variant{{Name: "control", Weight: 50}, {Name: "green", Weight: 50, Payload: payload}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if exp.GetExperiment().GetSalt() != "button" {
		t.Fatalf("salt = %q, want default %q", exp.GetExperiment().GetSalt(), "button")
	}

	resp, err := r.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "checkout/prod"})
	if err != nil {
		t.Fatal(err)
	}
	snap := resp.GetSnapshot()
	if snap.GetRevision() != 4 || len(snap.GetConfigs()) != 1 || len(snap.GetFlags()) != 1 || len(snap.GetExperiments()) != 1 {
		t.Fatalf("snapshot = %v", snap)
	}
	c := snap.GetConfigs()[0]
	if !proto.Equal(c.GetValue(), value) || c.GetUpdatedBy() != "alice" || c.GetRevision() != 2 || c.GetUpdatedAt() == nil {
		t.Fatalf("config = %v", c)
	}
	if f := snap.GetFlags()[0]; !f.GetEnabled() || f.GetUpdatedBy() != "alice" {
		t.Fatalf("flag = %v", f)
	}
	if v := snap.GetExperiments()[0].GetVariants(); len(v) != 2 || !proto.Equal(v[1].GetPayload(), payload) || v[0].GetPayload() != nil {
		t.Fatalf("variants = %v", v)
	}

	// Anonymous writes are attributed to "anonymous".
	if _, err := r.admin.PutFlag(ctxAs(t, ""), &cpv1.PutFlagRequest{Namespace: "checkout/prod", Key: "f2"}); err != nil {
		t.Fatal(err)
	}
	resp, _ = r.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "checkout/prod"})
	if by := resp.GetSnapshot().GetFlags()[0].GetUpdatedBy(); by != defaultActor {
		t.Fatalf("anonymous write attributed to %q", by)
	}
}

func TestWatchPushesEveryChangeWithinBudget(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	stream := watch(t, r, "svc", 0)

	if got := recv(t, stream, propagationBudget).GetRevision(); got != 1 {
		t.Fatalf("initial snapshot revision = %d, want 1", got)
	}
	for i := range 20 {
		start := time.Now()
		rev := putConfig(t, r, "svc", "counter", float64(i))
		snap := recv(t, stream, propagationBudget)
		if snap.GetRevision() != rev {
			t.Fatalf("pushed revision %d, want %d", snap.GetRevision(), rev)
		}
		if got := snap.GetConfigs()[0].GetValue().GetNumberValue(); got != float64(i) {
			t.Fatalf("pushed value %v, want %d", got, i)
		}
		if elapsed := time.Since(start); elapsed > propagationBudget {
			t.Fatalf("write->push took %v, budget %v", elapsed, propagationBudget)
		}
	}
	if r.srv.Watchers() != 1 {
		t.Fatalf("Watchers = %d, want 1", r.srv.Watchers())
	}
}

func TestWatchSkipsSnapshotClientAlreadyHas(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	rev := putConfig(t, r, "svc", "k", "v1")

	stream := watch(t, r, "svc", rev)
	next := putConfig(t, r, "svc", "k", "v2")
	if got := recv(t, stream, propagationBudget).GetRevision(); got != next {
		t.Fatalf("first pushed revision = %d, want %d (the one the client lacks)", got, next)
	}
}

func TestWatchUnknownNamespace(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	stream := watch(t, r, "missing", 0)
	_, err := stream.Recv()
	wantCode(t, err, codes.NotFound)
}

func TestShutdownEndsWatchWithUnavailable(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)

	r.srv.Shutdown()
	_, err := stream.Recv()
	wantCode(t, err, codes.Unavailable)
}

// TestCrossReplicaPropagationOverRedis writes on one replica and expects a
// watcher on another replica to receive it through Redis pub/sub.
func TestCrossReplicaPropagationOverRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	newNotifier := func() notify.Notifier {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })
		return notify.NewRedis(c, "", slog.New(slog.DiscardHandler))
	}
	shared := memory.New()
	// A long reconcile interval proves delivery came from pub/sub.
	a := startReplica(t, shared, replicaOptions{notifier: newNotifier(), reconcile: time.Hour})
	b := startReplica(t, shared, replicaOptions{notifier: newNotifier(), reconcile: time.Hour})

	createNamespace(t, a, "svc")
	stream := watch(t, b, "svc", 0)
	recv(t, stream, propagationBudget)

	// Wait until both replicas are subscribed to Redis.
	deadline := time.Now().Add(2 * time.Second)
	for mr.PubSubNumSub(notify.DefaultChannel)[notify.DefaultChannel] < 2 {
		if time.Now().After(deadline) {
			t.Fatal("replicas never subscribed to Redis")
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i := range 10 {
		start := time.Now()
		rev := putConfig(t, a, "svc", "k", float64(i))
		snap := recv(t, stream, propagationBudget)
		if snap.GetRevision() != rev {
			t.Fatalf("replica b pushed revision %d, want %d", snap.GetRevision(), rev)
		}
		if elapsed := time.Since(start); elapsed > propagationBudget {
			t.Fatalf("cross-replica propagation took %v, budget %v", elapsed, propagationBudget)
		}
	}
}

// TestReconcilerHealsMissedEvents isolates replica b from replica a's events;
// b must still converge through the periodic reconciler.
func TestReconcilerHealsMissedEvents(t *testing.T) {
	shared := memory.New()
	a := startReplica(t, shared, replicaOptions{notifier: notify.NewLocal()})
	b := startReplica(t, shared, replicaOptions{notifier: notify.NewLocal(), reconcile: 50 * time.Millisecond})

	createNamespace(t, a, "svc")
	stream := watch(t, b, "svc", 0)
	recv(t, stream, propagationBudget)

	rev := putConfig(t, a, "svc", "k", "v")
	if got := recv(t, stream, propagationBudget).GetRevision(); got != rev {
		t.Fatalf("reconciled revision = %d, want %d", got, rev)
	}
}

// stuckNotifier models an unreachable Redis: Publish hangs until its deadline.
type stuckNotifier struct{ *notify.Local }

func (*stuckNotifier) Publish(ctx context.Context, _ notify.Event) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestSlowNotifierDoesNotDelayWrites(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{notifier: &stuckNotifier{Local: notify.NewLocal()}})
	createNamespace(t, r, "svc")
	stream := watch(t, r, "svc", 0)
	recv(t, stream, propagationBudget)

	start := time.Now()
	rev := putConfig(t, r, "svc", "k", "v")
	if elapsed := time.Since(start); elapsed > publishTimeout/2 {
		t.Fatalf("write took %v with a stuck notifier; publishing must not block writes", elapsed)
	}
	// Watchers on the same replica are still served immediately.
	if got := recv(t, stream, propagationBudget).GetRevision(); got != rev {
		t.Fatalf("local watcher got revision %d, want %d", got, rev)
	}
}
