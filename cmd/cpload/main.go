// Command cpload load-tests change propagation. It holds many Watch streams
// open against one or more control plane replicas, writes a config key at a
// fixed interval and reports how long each change took to reach the
// watchers.
//
// Send and receive times are both taken from this process's monotonic clock,
// so the numbers are free of clock skew between hosts. Watchers may skip
// revisions (the server's per-watcher mailboxes are latest-wins), so two
// figures are reported:
//
//   - latency: for every snapshot a watcher received for one of the writes,
//     its receive time minus that write's send time;
//   - convergence: for every write, the time from sending it until every
//     watcher held its revision or a later one.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/model"
)

const usage = `cpload - measure how fast changes reach watchers

Usage:
  cpload [flags]

cpload creates --namespace if it is missing, opens --watchers Watch streams
spread round-robin over --addrs (sharing --conns-per-addr connections per
address), waits until every watcher holds the current snapshot, then writes
the config key loadtest.seq --writes times, one write per --interval, again
round-robin over the addresses. It prints a report and exits 1 when a
watcher or a write fails, a write never reaches every watcher, the run hits
--timeout, or the convergence p99 exceeds --slo.

Flags:
  --addrs list          comma-separated gRPC addresses (default localhost:9090)
  --namespace name      namespace to write and watch (default loadtest)
  --watchers n          Watch streams to open (default 1000)
  --writes n            writes to perform (default 50)
  --interval duration   time between writes, 0 for back to back (default 100ms)
  --conns-per-addr n    connections per address, shared by its watchers (default 4)
  --token string        bearer token (env CPLOAD_TOKEN)
  --slo duration        budget for the convergence p99 (default 1s)
  --timeout duration    limit for the whole run (default 60s)
  --json                print the report as JSON
`

const (
	// seqKey is the config key every write updates.
	seqKey = "loadtest.seq"
	// actorHeader must match server.ActorHeader.
	actorHeader = "x-controlplane-actor"
	// actor is recorded on writes when the server does not authenticate
	// callers; with auth enabled the token's name is recorded instead.
	actor = "cpload"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// cli runs cpload with args and returns the process exit code: 0 when the
// run passed, 1 when it failed or could not start, 2 for bad usage. The
// report goes to stdout, progress and errors to stderr.
func cli(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "cpload: %v\nsee 'cpload --help'\n", err)
		return 2
	}
	rep, err := run(ctx, cfg, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "cpload:", err)
		return 1
	}
	if cfg.JSON {
		err = rep.writeJSON(stdout)
	} else {
		err = rep.writeText(stdout)
	}
	if err != nil {
		fmt.Fprintln(stderr, "cpload:", err)
		return 1
	}
	if !rep.Passed() {
		return 1
	}
	return 0
}

type config struct {
	Addrs        []string
	Namespace    string
	Watchers     int
	Writes       int
	Interval     time.Duration
	ConnsPerAddr int
	Token        string
	SLO          time.Duration
	Timeout      time.Duration
	JSON         bool
}

func parseConfig(args []string) (config, error) {
	var (
		c     config
		addrs string
	)
	fs := flag.NewFlagSet("cpload", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&addrs, "addrs", "localhost:9090", "")
	fs.StringVar(&c.Namespace, "namespace", "loadtest", "")
	fs.IntVar(&c.Watchers, "watchers", 1000, "")
	fs.IntVar(&c.Writes, "writes", 50, "")
	fs.DurationVar(&c.Interval, "interval", 100*time.Millisecond, "")
	fs.IntVar(&c.ConnsPerAddr, "conns-per-addr", 4, "")
	// The environment keeps the token out of process listings.
	fs.StringVar(&c.Token, "token", os.Getenv("CPLOAD_TOKEN"), "")
	fs.DurationVar(&c.SLO, "slo", time.Second, "")
	fs.DurationVar(&c.Timeout, "timeout", 60*time.Second, "")
	fs.BoolVar(&c.JSON, "json", false, "")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	for _, a := range strings.Split(addrs, ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.Addrs = append(c.Addrs, a)
		}
	}

	switch {
	case len(c.Addrs) == 0:
		return config{}, errors.New("--addrs needs at least one address")
	case c.Watchers < 1:
		return config{}, errors.New("--watchers must be at least 1")
	case c.Writes < 1:
		return config{}, errors.New("--writes must be at least 1")
	case c.Interval < 0:
		return config{}, errors.New("--interval must not be negative")
	case c.ConnsPerAddr < 1:
		return config{}, errors.New("--conns-per-addr must be at least 1")
	case c.SLO <= 0:
		return config{}, errors.New("--slo must be positive")
	case c.Timeout <= 0:
		return config{}, errors.New("--timeout must be positive")
	// (writes-1) * interval > timeout, without overflowing: the run could
	// never finish, so say so now instead of after --timeout.
	case c.Interval > 0 && int64(c.Writes-1) > int64(c.Timeout/c.Interval):
		return config{}, fmt.Errorf("--timeout %v is shorter than %d writes take at --interval %v", c.Timeout, c.Writes, c.Interval)
	}
	if err := model.ValidateNamespaceName(c.Namespace); err != nil {
		return config{}, fmt.Errorf("--namespace: %w", err)
	}
	return c, nil
}

// run performs one load test, writing progress lines to progress. It returns
// an error only when the test cannot start: the namespace cannot be read or
// created, or a watcher fails before every watcher holds its first snapshot.
// Everything that goes wrong later is recorded in the report, which then
// fails. Every goroutine and stream run starts has ended when it returns.
func run(ctx context.Context, cfg config, progress io.Writer) (*report, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, cfg.Timeout, fmt.Errorf("timed out after %v", cfg.Timeout))
	defer cancel()

	conns, err := dial(cfg)
	if err != nil {
		return nil, err
	}
	defer closeAll(conns)

	created, err := ensureNamespace(writeContext(ctx), cpv1.NewAdminServiceClient(conns[0].cc), cfg.Namespace)
	if err != nil {
		return nil, fmt.Errorf("namespace %s: %w", cfg.Namespace, err)
	}
	if created {
		fmt.Fprintf(progress, "cpload: created namespace %s\n", cfg.Namespace)
	}

	lt := &loadTest{cfg: cfg, conns: conns, start: time.Now(), track: newTracker(cfg.Watchers)}
	lt.startWatchers(ctx)
	defer lt.stopWatchers()

	// Every namespace revision is >= 1, so this waits for initial snapshots.
	if err := lt.waitAll(ctx, 1); err != nil {
		ready, _, _ := lt.track.started()
		return nil, fmt.Errorf("waiting for initial snapshots (%d of %d watchers ready): %w", ready, cfg.Watchers, err)
	}
	if _, failed, first := lt.track.started(); failed > 0 {
		return nil, fmt.Errorf("%d of %d watchers failed to start: %w", failed, cfg.Watchers, first)
	}
	fmt.Fprintf(progress, "cpload: %d watchers ready after %v\n", cfg.Watchers, time.Since(lt.start).Round(time.Millisecond))

	writing := time.Now()
	stopped := lt.writeAll(ctx)
	fmt.Fprintf(progress, "cpload: %d of %d writes committed in %v\n", len(lt.writes), cfg.Writes, time.Since(writing).Round(time.Millisecond))
	// Revisions only grow, so once every watcher holds the last write's
	// revision, every write has converged.
	if stopped == nil && len(lt.writes) > 0 {
		stopped = lt.waitAll(ctx, lt.writes[len(lt.writes)-1].revision)
	}
	lt.stopWatchers()
	return newReport(cfg, lt.writes, lt.writeErrs, lt.watchers, stopped), nil
}

// writeContext marks calls as made by cpload for the audit log.
func writeContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, actorHeader, actor)
}

// conn is one client connection, shared by many watchers.
type conn struct {
	addr string
	cc   *grpc.ClientConn
}

// dial opens cfg.ConnsPerAddr connections to every address, ordered so that
// conns[i%len(conns)] goes round-robin over the addresses first and over each
// address's connections second, and conns[:len(cfg.Addrs)] holds one
// connection per address. Connections are established on first use.
func dial(cfg config) ([]conn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if cfg.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(auth.TokenCredentials(cfg.Token, false)))
	}
	conns := make([]conn, 0, len(cfg.Addrs)*cfg.ConnsPerAddr)
	for range cfg.ConnsPerAddr {
		for _, addr := range cfg.Addrs {
			cc, err := grpc.NewClient(addr, opts...)
			if err != nil {
				closeAll(conns)
				return nil, fmt.Errorf("dial %s: %w", addr, err)
			}
			conns = append(conns, conn{addr: addr, cc: cc})
		}
	}
	return conns, nil
}

func closeAll(conns []conn) {
	for _, c := range conns {
		c.cc.Close()
	}
}

// ensureNamespace creates the namespace unless it exists. It looks first so
// that a token without the admin role works against an existing namespace.
func ensureNamespace(ctx context.Context, admin cpv1.AdminServiceClient, name string) (created bool, err error) {
	_, err = admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: name})
	if status.Code(err) != codes.NotFound {
		return false, err
	}
	_, err = admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: name, Description: "created by cpload"})
	if status.Code(err) == codes.AlreadyExists {
		return false, nil // someone else created it meanwhile
	}
	return err == nil, err
}

// write is one committed PutConfig.
type write struct {
	revision int64
	sent     time.Duration // since loadTest.start, taken just before sending
}

// delivery is one snapshot received by a watcher.
type delivery struct {
	revision int64
	at       time.Duration // since loadTest.start
}

// watcher is one Watch stream. Its fields belong to its goroutine until
// loadTest.stopWatchers returns.
type watcher struct {
	addr       string
	deliveries []delivery // in receive order, so in increasing revision order
	err        error      // why the stream ended before cpload stopped it
}

type loadTest struct {
	cfg   config
	conns []conn
	// start is the origin of every recorded time. It carries a monotonic
	// clock reading, so durations measured from it ignore wall clock jumps.
	start time.Time
	track *tracker

	watchers []*watcher
	wg       sync.WaitGroup
	cancel   context.CancelFunc // ends every Watch stream

	writes    []write  // in send order, so in increasing revision order
	writeErrs []string // one message per failed write
}

// startWatchers opens one Watch stream per watcher, each in its own
// goroutine, round-robin over the connections. The streams last until
// stopWatchers, whatever happens to ctx.
func (lt *loadTest) startWatchers(ctx context.Context) {
	// ctx's deadline must not reach the streams: gRPC sends it to the
	// server, whose timer can end a stream a moment before ctx reports it
	// expired here, and the watcher would then count as failed.
	ctx, lt.cancel = context.WithCancel(context.WithoutCancel(ctx))
	lt.watchers = make([]*watcher, lt.cfg.Watchers)
	for i := range lt.watchers {
		c := lt.conns[i%len(lt.conns)]
		w := &watcher{addr: c.addr, deliveries: make([]delivery, 0, lt.cfg.Writes+1)}
		lt.watchers[i] = w
		client := cpv1.NewDistributionServiceClient(c.cc)
		lt.wg.Go(func() { lt.watch(ctx, i, w, client) })
	}
}

// stopWatchers ends every Watch stream and waits for the watcher goroutines
// to exit. Safe to call more than once.
func (lt *loadTest) stopWatchers() {
	lt.cancel()
	lt.wg.Wait()
}

func (lt *loadTest) watch(ctx context.Context, i int, w *watcher, client cpv1.DistributionServiceClient) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // ends the stream on every way out
	err := lt.receive(ctx, i, w, client)
	if ctx.Err() != nil {
		return // stopped by run: not a failure
	}
	w.err = fmt.Errorf("watch %s: %w", w.addr, err)
	lt.track.fail(i, w.err)
}

// receive records the arrival time of every snapshot until the stream fails.
func (lt *loadTest) receive(ctx context.Context, i int, w *watcher, client cpv1.DistributionServiceClient) error {
	stream, err := client.Watch(ctx, &cpv1.WatchRequest{Namespace: lt.cfg.Namespace, ClientId: fmt.Sprintf("cpload-%d", i)})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		at := time.Since(lt.start)
		if err != nil {
			return err
		}
		rev := resp.GetSnapshot().GetRevision()
		// The server promises increasing revisions; the convergence
		// computation relies on it, so a violation is a watcher failure.
		if n := len(w.deliveries); n > 0 && rev <= w.deliveries[n-1].revision {
			return fmt.Errorf("revision %d arrived after revision %d", rev, w.deliveries[n-1].revision)
		}
		w.deliveries = append(w.deliveries, delivery{revision: rev, at: at})
		lt.track.observe(i, rev)
	}
}

// waitAll blocks until every watcher whose stream is still open holds rev
// or a later revision, or ctx ends.
func (lt *loadTest) waitAll(ctx context.Context, rev int64) error {
	select {
	case <-lt.track.await(rev):
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// writeAll performs the writes one at a time, round-robin over the
// addresses, starting one every interval. Waiting for each write before
// sending the next keeps send order and revision order the same. A failed
// write is recorded and the next one still made. It returns why ctx ended if
// that stopped the writes early.
func (lt *loadTest) writeAll(ctx context.Context) error {
	// As for the watchers, the server must not see ctx's deadline, or a
	// write cut off by it could look like a failed write instead of the
	// end of the run. Writes are cancelled once ctx is done instead.
	wctx, cancel := context.WithCancel(writeContext(context.WithoutCancel(ctx)))
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()

	var tick <-chan time.Time
	if lt.cfg.Interval > 0 {
		// A ticker drops ticks while a write runs long, so a slow write is
		// not followed by a burst of catch-up writes.
		t := time.NewTicker(lt.cfg.Interval)
		defer t.Stop()
		tick = t.C
	}
	for i := range lt.cfg.Writes {
		if i > 0 && tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
			}
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		c := lt.conns[i%len(lt.cfg.Addrs)]
		sent := time.Since(lt.start)
		resp, err := cpv1.NewAdminServiceClient(c.cc).PutConfig(wctx, &cpv1.PutConfigRequest{
			Namespace: lt.cfg.Namespace,
			Key:       seqKey,
			Value:     structpb.NewNumberValue(float64(i + 1)),
		})
		switch {
		case err == nil:
			lt.writes = append(lt.writes, write{revision: resp.GetConfig().GetRevision(), sent: sent})
		case ctx.Err() != nil:
			// Whether it committed is unknown; the run is over either way.
			return context.Cause(ctx)
		default:
			lt.writeErrs = append(lt.writeErrs, fmt.Sprintf("write %s: %v", c.addr, err))
		}
	}
	return nil
}

// tracker follows which revision every watcher holds, so run can wait for
// all of them to reach one without polling.
type tracker struct {
	mu       sync.Mutex
	latest   []int64 // highest revision each watcher holds, 0 before its first
	dead     []bool  // the watcher's stream failed
	firstErr error

	target  int64
	behind  int           // live watchers with latest < target
	reached chan struct{} // closed once behind drops to zero
}

func newTracker(n int) *tracker {
	return &tracker{latest: make([]int64, n), dead: make([]bool, n)}
}

// await returns a channel that is closed once every live watcher holds rev
// or a later revision. A watcher that fails stops counting, so a broken
// stream cannot hold up the run; its error is reported instead.
func (t *tracker) await(rev int64) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.target, t.behind, t.reached = rev, 0, make(chan struct{})
	for i, latest := range t.latest {
		if latest < rev && !t.dead[i] {
			t.behind++
		}
	}
	if t.behind == 0 {
		close(t.reached)
	}
	return t.reached
}

// observe records that watcher i received revision rev.
func (t *tracker) observe(i int, rev int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.latest[i] < t.target && rev >= t.target {
		t.settle()
	}
	t.latest[i] = rev
}

// fail records that watcher i's stream ended with err.
func (t *tracker) fail(i int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.latest[i] < t.target {
		t.settle() // it never will reach the target
	}
	t.dead[i] = true
	if t.firstErr == nil {
		t.firstErr = err
	}
}

// settle takes one watcher off the list await is waiting for.
func (t *tracker) settle() {
	t.behind--
	if t.behind == 0 {
		close(t.reached)
	}
}

// started returns how many watchers hold a snapshot, how many failed, and
// the first failure.
func (t *tracker) started() (ready, failed int, firstErr error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, latest := range t.latest {
		switch {
		case t.dead[i]:
			failed++
		case latest > 0:
			ready++
		}
	}
	return ready, failed, t.firstErr
}
