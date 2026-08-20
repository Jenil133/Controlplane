package client

import (
	"errors"
	"io"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/eval"
)

// healthyAfter is how long a stream that delivered nothing must have stayed
// open to count as healthy. Streams of an unchanged namespace are silent,
// since the server only sends revisions newer than known_revision; without
// this, each server restart would push such clients' backoff further up,
// until every reconnect waited MaxBackoff.
const healthyAfter = 10 * time.Second

// errStreamEnded is the LastError of a stream the server ended without an
// error status.
var errStreamEnded = errors.New("client: watch stream ended by the server")

// state is a snapshot compiled for reading. It is never modified once
// stored in Client.state.
type state struct {
	snap        *cpv1.Snapshot
	source      Source
	installed   time.Time
	configs     map[string]*structpb.Value
	flags       map[string]*eval.Flag
	experiments map[string]*eval.Experiment
}

// noState stands in for the state before the first snapshot: its nil maps
// make every lookup miss.
var noState = &state{}

func (c *Client) current() *state {
	if st := c.state.Load(); st != nil {
		return st
	}
	return noState
}

// run watches the namespace until the client is closed.
func (c *Client) run() {
	defer close(c.done)
	failures := 0
	for {
		healthy, err := c.watch()
		if c.ctx.Err() != nil {
			return
		}
		if healthy {
			failures = 0
		}
		failures++
		delay := retryDelay(failures, c.minBackoff, c.maxBackoff, c.randN)
		c.link.Store(&link{state: StateDisconnected, err: err})
		c.log.Warn("control plane watch failed; reconnecting", "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// watch runs one Watch stream until it fails. healthy reports whether the
// stream delivered a snapshot or stayed open for healthyAfter.
func (c *Client) watch() (healthy bool, err error) {
	known := c.Revision()
	req := &cpv1.WatchRequest{Namespace: c.namespace, KnownRevision: known, ClientId: c.clientID}
	stream, err := c.dist.Watch(c.ctx, req, c.callOpts...)
	if err != nil {
		return false, err
	}
	// The stream counts as live as soon as it is open: when the client
	// already holds the current revision, the server sends nothing until the
	// next change. A server that rejects the stream does so right away.
	opened := time.Now()
	c.link.Store(&link{state: StateLive, err: c.link.Load().err})
	c.log.Info("watching control plane namespace", "known_revision", known)
	delivered := false
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errStreamEnded
			}
			return delivered || time.Since(opened) >= healthyAfter, err
		}
		delivered = true
		c.install(resp.GetSnapshot(), SourceServer)
	}
}

// retryDelay returns how long to wait before the next Watch attempt after
// failures (at least 1) unhealthy streams in a row: "full jitter", a uniformly
// random duration between lo and lo·2^failures, capped at hi.
func retryDelay(failures int, lo, hi time.Duration, randN func(int64) int64) time.Duration {
	ceiling := lo
	for range failures {
		if ceiling >= hi/2 {
			ceiling = hi
			break
		}
		ceiling *= 2
	}
	return lo + time.Duration(randN(int64(ceiling-lo)+1))
}

// install starts serving snap if it belongs to the client's namespace and is
// newer than the snapshot in use. Calls must not overlap: New makes the
// first, the watch goroutine all later ones.
func (c *Client) install(snap *cpv1.Snapshot, src Source) {
	cur := c.state.Load()
	switch {
	case snap.GetNamespace() != c.namespace:
		c.log.Warn("ignoring control plane snapshot of another namespace", "snapshot_namespace", snap.GetNamespace(), "source", src)
		return
	case snap.GetRevision() < 1:
		c.log.Warn("ignoring control plane snapshot without a revision", "source", src)
		return
	case cur != nil && snap.GetRevision() <= cur.snap.GetRevision():
		if snap.GetRevision() < cur.snap.GetRevision() {
			c.log.Warn("ignoring control plane snapshot older than the one in use",
				"revision", snap.GetRevision(), "current_revision", cur.snap.GetRevision(), "source", src)
		}
		return
	}

	next := compile(snap, src, time.Now())
	c.limits.Update(rateLimitPolicies(snap.GetRateLimits()))
	c.breakers.Update(breakerPolicies(snap.GetCircuitBreakers()))
	c.state.Store(next)
	var prev *cpv1.Snapshot
	if cur == nil {
		close(c.ready)
	} else {
		prev = cur.snap
	}
	c.log.Info("control plane snapshot applied", "revision", snap.GetRevision(), "source", src)

	if src == SourceServer && c.cachePath != "" {
		if err := writeCache(c.cachePath, snap); err != nil {
			c.log.Warn("cannot write control plane snapshot cache", "path", c.cachePath, "error", err)
		}
	}
	c.notify(prev, snap)
}

func compile(snap *cpv1.Snapshot, src Source, now time.Time) *state {
	st := &state{
		snap:        snap,
		source:      src,
		installed:   now,
		configs:     make(map[string]*structpb.Value, len(snap.GetConfigs())),
		flags:       make(map[string]*eval.Flag, len(snap.GetFlags())),
		experiments: make(map[string]*eval.Experiment, len(snap.GetExperiments())),
	}
	for _, cfg := range snap.GetConfigs() {
		st.configs[cfg.GetKey()] = cfg.GetValue()
	}
	for _, f := range snap.GetFlags() {
		st.flags[f.GetKey()] = eval.CompileFlag(f)
	}
	for _, e := range snap.GetExperiments() {
		st.experiments[e.GetKey()] = eval.CompileExperiment(e)
	}
	return st
}
