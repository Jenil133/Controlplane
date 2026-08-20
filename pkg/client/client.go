// Package client is the Go SDK of the control plane. A Client keeps a local
// copy of one namespace, always the latest revision the control plane has
// pushed, and answers configuration, feature flag, experiment and traffic
// policy questions from memory, so none of them costs a network round trip.
//
// # Hot reload
//
// New opens a Watch stream in the background and returns at once. The server
// sends the namespace's snapshot when the stream opens and a new one after
// every committed change, and the client swaps each in atomically: reads load
// an immutable, precompiled state through an atomic pointer, so they never
// lock or block, and every call sees one complete revision. Flags and
// experiments are compiled with package eval, so every service evaluating the
// same revision makes the same decisions. Rate limits and circuit breakers are
// reconfigured in place and keep their tokens and state (packages ratelimit
// and breaker). OnChange listeners run after each swap. Snapshots of another
// namespace, or no newer than the one in use, are ignored: a client never goes
// back in time.
//
// If the stream fails, the client keeps serving the snapshot it has and
// reconnects with exponential backoff and full jitter between
// Options.MinBackoff and Options.MaxBackoff. It tells the server which
// revision it holds, so an unchanged namespace is not sent again.
//
// # Last-known-good cache
//
// With Options.CachePath set, every snapshot received from the server is
// written to that file as protojson, atomically (temporary file, fsync,
// rename, directory fsync) and readable only by its owner. New loads the file
// before contacting the server, so a service that starts while the control
// plane is unreachable still runs with the configuration it last saw, and
// catches up once the server is back. A cache file that cannot be read, does
// not parse or holds another namespace is ignored. Failing to write the cache
// is logged and otherwise harmless.
//
// # Status
//
// Status reports the State of the connection:
//
//   - StateConnecting: the first attempt to open the stream is in progress.
//   - StateLive: a stream is open and changes arrive as they are committed.
//   - StateDisconnected: the stream failed and the client is waiting to
//     reconnect, or the client was closed. Status.LastError says why.
//
// and the Source of the snapshot in use: SourceNone before the first one
// (WaitReady waits for it), SourceCache when it was loaded from the cache
// file, SourceServer once the control plane has sent one. A client that
// started from the cache reports SourceCache, while live, until the namespace
// changes, because the server does not resend the revision the client holds.
// Whatever the state, the client answers from the snapshot it has; before the
// first one, flags are off, experiments enroll nobody, config getters return
// their defaults and no rate limit or circuit breaker applies.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/ratelimit"
)

const (
	defaultMinBackoff = 200 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	// Without pings, a silent stream over a dead connection would look live
	// until TCP gave up, which can take many minutes. The server permits a
	// ping every 10s.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second
	// minConnectTimeout is gRPC's default, which WithConnectParams needs
	// restated.
	minConnectTimeout = 20 * time.Second
	// maxSnapshotBytes bounds the snapshots the client accepts. Namespaces
	// have no size limit and a config value may be 256 KiB, so gRPC's default
	// of 4 MiB is easily exceeded.
	maxSnapshotBytes = 64 << 20
)

var (
	// ErrClosed is returned by WaitReady once the client is closed, and is
	// the Status().LastError of a closed client.
	ErrClosed = errors.New("client: closed")
	// ErrNotFound is returned, wrapped, by Decode for a key without a
	// config.
	ErrNotFound = errors.New("client: config not found")
)

// Options configure a Client. Namespace and exactly one of Address and Conn
// are required.
type Options struct {
	// Address is the control plane's gRPC target, e.g. "controlplane:9090";
	// any target grpc.NewClient accepts. The client owns the connection it
	// dials and closes it on Close.
	Address string
	// Conn is an existing connection to use instead of Address. It stays the
	// caller's: Close does not close it, and DialOptions do not apply (the
	// Watch calls still accept snapshots of up to 64 MiB).
	Conn *grpc.ClientConn
	// Namespace is the namespace to serve, e.g. "checkout/prod".
	Namespace string
	// Token, if set, is sent as "authorization: Bearer <token>" with every
	// call, over plaintext connections too; dial with TLS transport
	// credentials to keep it confidential.
	Token string
	// ClientID names this instance in the server's logs. Defaults to
	// "<hostname>-<pid>".
	ClientID string
	// CachePath is the file that keeps the last snapshot received from the
	// server; its directory is created if missing. Empty disables the cache.
	CachePath string
	// DialOptions are applied after the client's defaults when dialing
	// Address, so they win: insecure transport credentials (pass TLS
	// credentials here to use TLS), keepalive pings every 30s, gRPC's
	// reconnect backoff capped at MaxBackoff, and received messages of up to
	// 64 MiB.
	DialOptions []grpc.DialOption
	// Logger receives the client's logs. Defaults to slog.Default().
	Logger *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between attempts to watch the
	// namespace: a random duration between MinBackoff and a ceiling that
	// starts at twice MinBackoff, doubles with every failed attempt in a row
	// and stops at MaxBackoff. A stream that delivered a snapshot or stayed
	// open for 10s resets the ceiling. They default to 200ms and 30s; a
	// MaxBackoff below MinBackoff means MinBackoff.
	MinBackoff, MaxBackoff time.Duration
}

// State says whether a client is receiving changes.
type State int

// Connection states; see the package documentation.
const (
	StateConnecting State = iota
	StateLive
	StateDisconnected
)

// String returns "connecting", "live" or "disconnected".
func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateLive:
		return "live"
	case StateDisconnected:
		return "disconnected"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// MarshalText encodes the state as its String, so that JSON status pages and
// logs show the name rather than a number.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Source says where the snapshot in use came from.
type Source int

// Snapshot sources; see the package documentation.
const (
	SourceNone Source = iota
	SourceCache
	SourceServer
)

// String returns "none", "cache" or "server".
func (s Source) String() string {
	switch s {
	case SourceNone:
		return "none"
	case SourceCache:
		return "cache"
	case SourceServer:
		return "server"
	default:
		return fmt.Sprintf("Source(%d)", int(s))
	}
}

// MarshalText encodes the source as its String.
func (s Source) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Status describes a client's connection and the snapshot it serves.
type Status struct {
	State  State
	Source Source
	// Revision is the revision of the snapshot in use, 0 before the first.
	Revision int64
	// LastUpdate is when the client started serving that snapshot, zero
	// before the first.
	LastUpdate time.Time
	// LastError is what ended the latest attempt to watch the namespace, or
	// ErrClosed after Close; nil while no attempt has failed. It is kept
	// after the client reconnects: State tells whether the client is live.
	LastError error
}

// link is the connection half of a Status. It is replaced, never modified.
type link struct {
	state State
	err   error
}

// listener is one OnChange registration.
type listener struct {
	fn      func(old, new *cpv1.Snapshot)
	removed atomic.Bool
}

// Client serves one namespace from memory and keeps it current. It is safe
// for concurrent use. Create one with New.
type Client struct {
	namespace  string
	clientID   string
	cachePath  string
	minBackoff time.Duration
	maxBackoff time.Duration
	// randN draws backoff jitter, uniformly from [0, n).
	randN func(n int64) int64
	log   *slog.Logger

	conn     *grpc.ClientConn
	ownsConn bool
	dist     cpv1.DistributionServiceClient
	callOpts []grpc.CallOption

	// state is the snapshot in use, compiled; nil before the first. Only
	// one goroutine at a time stores it: New, then the watch goroutine.
	state    atomic.Pointer[state]
	link     atomic.Pointer[link]
	limits   *ratelimit.Set
	breakers *breaker.Set

	mu sync.Mutex
	// listeners is copied on write, so it can be iterated without mu.
	listeners []*listener

	ready  chan struct{} // closed when the first snapshot is in use
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the watch goroutine has exited

	closeOnce sync.Once
	closeErr  error
}

// New creates a client for opts.Namespace and starts watching the namespace
// in the background. It returns at once, without waiting for the server:
// when opts.CachePath holds a snapshot of the namespace, the client serves it
// from the start; otherwise WaitReady waits for the first snapshot.
//
// ctx only matters to New: canceling it later does not affect the client,
// but its values, such as outgoing gRPC metadata, are passed on to the Watch
// calls. Close the client when done with it.
func New(ctx context.Context, opts Options) (*Client, error) {
	return newClient(ctx, opts, rand.Int64N)
}

// newClient is New with the backoff jitter source made replaceable.
func newClient(ctx context.Context, opts Options, randN func(int64) int64) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := model.ValidateNamespaceName(opts.Namespace); err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	if (opts.Address == "") == (opts.Conn == nil) {
		return nil, errors.New("client: set exactly one of Options.Address and Options.Conn")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		namespace:  opts.Namespace,
		clientID:   opts.ClientID,
		cachePath:  opts.CachePath,
		minBackoff: opts.MinBackoff,
		maxBackoff: opts.MaxBackoff,
		randN:      randN,
		log:        logger.With("namespace", opts.Namespace),
		limits:     ratelimit.NewSet(),
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
	}
	if c.clientID == "" {
		c.clientID = defaultClientID()
	}
	if c.minBackoff <= 0 {
		c.minBackoff = defaultMinBackoff
	}
	if c.maxBackoff <= 0 {
		c.maxBackoff = defaultMaxBackoff
	}
	c.maxBackoff = max(c.maxBackoff, c.minBackoff)
	c.breakers = breaker.NewSet(breaker.WithSetStateChange(c.logBreakerChange))

	if opts.Conn != nil {
		c.conn = opts.Conn
		// The dial defaults do not reach a caller's connection, and gRPC's
		// 4 MiB limit would make a large namespace unwatchable.
		c.callOpts = append(c.callOpts, grpc.MaxCallRecvMsgSize(maxSnapshotBytes))
	} else {
		conn, err := grpc.NewClient(opts.Address, append(c.dialDefaults(), opts.DialOptions...)...)
		if err != nil {
			return nil, fmt.Errorf("client: %w", err)
		}
		c.conn, c.ownsConn = conn, true
	}
	c.dist = cpv1.NewDistributionServiceClient(c.conn)
	if opts.Token != "" {
		// A call option rather than a dial option, so that it also applies
		// to a caller's Conn.
		c.callOpts = append(c.callOpts, grpc.PerRPCCredentials(auth.TokenCredentials(opts.Token, false)))
	}

	c.link.Store(&link{state: StateConnecting})
	c.ctx, c.cancel = context.WithCancel(context.WithoutCancel(ctx))
	if c.cachePath != "" {
		c.loadCache()
	}
	go c.run()
	return c, nil
}

func (c *Client) dialDefaults() []grpc.DialOption {
	// gRPC's own reconnect backoff grows to two minutes, which would keep
	// the connection down long after MaxBackoff once the server is back.
	bo := backoff.DefaultConfig
	bo.BaseDelay = min(bo.BaseDelay, c.maxBackoff)
	bo.MaxDelay = c.maxBackoff
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout}),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bo, MinConnectTimeout: minConnectTimeout}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxSnapshotBytes)),
	}
}

func defaultClientID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func (c *Client) logBreakerChange(key string, from, to breaker.State) {
	level := slog.LevelInfo
	if to == breaker.Open {
		level = slog.LevelWarn
	}
	c.log.Log(context.Background(), level, "circuit breaker changed state", "key", key, "from", from, "to", to)
}

// Close stops watching, waits until the watch goroutine has finished
// (including a cache write or OnChange listener in progress) and closes the
// connection the client dialed. The client keeps answering from the last
// snapshot it had. Close is idempotent; later calls wait for the first and
// return its result. It must not be called from an OnChange listener, which
// runs on the goroutine Close waits for.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		<-c.done
		c.link.Store(&link{state: StateDisconnected, err: ErrClosed})
		if c.ownsConn {
			c.closeErr = c.conn.Close()
		}
	})
	return c.closeErr
}

// WaitReady blocks until the client has a snapshot, from the cache or the
// server. It returns ErrClosed if the client is closed first, and ctx's error,
// annotated with the last watch error, if ctx ends first.
func (c *Client) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
	case <-c.ctx.Done():
	}
	// The first snapshot may have arrived at the same time.
	select {
	case <-c.ready:
		return nil
	default:
	}
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	if err := c.link.Load().err; err != nil {
		return fmt.Errorf("client: no snapshot yet: %w (last watch error: %v)", ctx.Err(), err)
	}
	return fmt.Errorf("client: no snapshot yet: %w", ctx.Err())
}

// Status returns the client's connection state and what it serves.
func (c *Client) Status() Status {
	l := c.link.Load()
	s := Status{State: l.state, LastError: l.err}
	if st := c.state.Load(); st != nil {
		s.Source, s.Revision, s.LastUpdate = st.source, st.snap.GetRevision(), st.installed
	}
	return s
}

// Snapshot returns the snapshot in use, or nil before the first. It is
// shared and must not be modified.
func (c *Client) Snapshot() *cpv1.Snapshot {
	return c.current().snap
}

// Revision returns the revision of the snapshot in use, 0 before the first.
func (c *Client) Revision() int64 {
	return c.current().snap.GetRevision()
}

// OnChange registers fn to be called after every change of the snapshot in
// use, with the previous snapshot (nil for the first) and the new one, both
// shared and not to be modified. Listeners run one at a time, in
// registration order, on the watch goroutine, once the new snapshot is
// already served; a slow listener delays the next update. A panicking
// listener is logged and skipped. A snapshot loaded from the cache by New is
// in use before any listener can be registered: read Snapshot after
// registering to start from the current state.
//
// remove unregisters fn: once remove returns, fn is not called again, apart
// from a call already under way. Listeners may call remove, their own or
// another's.
func (c *Client) OnChange(fn func(old, new *cpv1.Snapshot)) (remove func()) {
	if fn == nil {
		panic("client: OnChange with a nil listener")
	}
	l := &listener{fn: fn}
	c.mu.Lock()
	c.listeners = append(slices.Clip(c.listeners), l)
	c.mu.Unlock()
	return func() {
		if l.removed.Swap(true) {
			return
		}
		c.mu.Lock()
		c.listeners = slices.DeleteFunc(slices.Clone(c.listeners), func(x *listener) bool { return x == l })
		c.mu.Unlock()
	}
}

// notify passes a change to the listeners.
func (c *Client) notify(prev, next *cpv1.Snapshot) {
	c.mu.Lock()
	listeners := c.listeners
	c.mu.Unlock()
	for _, l := range listeners {
		if !l.removed.Load() {
			c.callListener(l.fn, prev, next)
		}
	}
}

func (c *Client) callListener(fn func(old, new *cpv1.Snapshot), prev, next *cpv1.Snapshot) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("control plane OnChange listener panicked", "revision", next.GetRevision(), "panic", r, "stack", string(debug.Stack()))
		}
	}()
	fn(prev, next)
}
