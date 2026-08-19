package server

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/store"
	"github.com/Jenil133/Controlplane/internal/store/memory"
)

func TestPutFlagMergesWithStoredFlag(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")

	type want struct {
		enabled     bool
		description string
		percent     float64
		salt        string
		allowlist   []string
	}
	check := func(step string, f *cpv1.Flag, w want) {
		t.Helper()
		if f.GetEnabled() != w.enabled || f.GetDescription() != w.description || f.GetRolloutPercent() != w.percent ||
			f.GetSalt() != w.salt || !slices.Equal(f.GetAllowlist(), w.allowlist) || f.GetRollout() != nil {
			t.Fatalf("%s: flag = %v, want %+v", step, f, w)
		}
	}

	// A new flag is fully rolled out and salted with its key.
	f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Enabled: true, Description: "d1", Allowlist: []string{"u2", "u1"}})
	check("create", f, want{true, "d1", 100, "new-cart", []string{"u2", "u1"}})

	f = putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Enabled: true, RolloutPercent: proto.Float64(12.5), Salt: "s1"})
	check("set percent and salt", f, want{true, "", 12.5, "s1", nil})

	// Unset percent and salt keep the stored values; the rest is replaced.
	f = putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Description: "d2", Allowlist: []string{"u3"}})
	check("keep percent and salt", f, want{false, "d2", 12.5, "s1", []string{"u3"}})

	// Zero is a percentage like any other, not "unset".
	f = putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Enabled: true, RolloutPercent: proto.Float64(0), ExpectedRevision: f.GetRevision()})
	check("explicit expected revision", f, want{true, "", 0, "s1", nil})

	resp, err := r.dist.GetSnapshot(ctxAs(t, "test"), &cpv1.GetSnapshotRequest{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotFlag(t, resp.GetSnapshot(), "new-cart"); !proto.Equal(got, f) {
		t.Fatalf("snapshot flag = %v, want %v", got, f)
	}

	rev := namespaceRevision(t, r, "svc")
	for _, tt := range []struct {
		name string
		req  *cpv1.PutFlagRequest
		code codes.Code
	}{
		{"stale expected revision", &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", ExpectedRevision: f.GetRevision() - 1}, codes.Aborted},
		{"expected revision of a missing flag", &cpv1.PutFlagRequest{Namespace: "svc", Key: "other", ExpectedRevision: 2}, codes.Aborted},
		{"percent above 100", &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", RolloutPercent: proto.Float64(100.5)}, codes.InvalidArgument},
		{"percent with three decimals", &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", RolloutPercent: proto.Float64(12.345)}, codes.InvalidArgument},
		{"duplicate allowlist entry", &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Allowlist: []string{"u1", "u1"}}, codes.InvalidArgument},
		{"empty allowlist entry", &cpv1.PutFlagRequest{Namespace: "svc", Key: "new-cart", Allowlist: []string{""}}, codes.InvalidArgument},
		{"invalid key", &cpv1.PutFlagRequest{Namespace: "svc", Key: "bad key"}, codes.InvalidArgument},
		{"missing namespace", &cpv1.PutFlagRequest{Namespace: "missing", Key: "f"}, codes.NotFound},
		{"missing namespace with expected revision", &cpv1.PutFlagRequest{Namespace: "missing", Key: "f", ExpectedRevision: 1}, codes.NotFound},
	} {
		_, err := r.admin.PutFlag(ctxAs(t, "test"), tt.req)
		if got := status.Code(err); got != tt.code {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.code)
		}
	}
	if got := namespaceRevision(t, r, "svc"); got != rev {
		t.Fatalf("namespace revision = %d after failed writes, want %d", got, rev)
	}
}

func TestPutFlagLeavesRolloutPercentToTheRollout(t *testing.T) {
	r := startReplica(t, memory.New(), replicaOptions{})
	createNamespace(t, r, "svc")
	ctx := ctxAs(t, "alice")
	putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	started, err := r.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: "svc", Flag: "f", Stages: []*cpv1.RolloutStage{{Percent: 10}, {Percent: 50}, {Percent: 100}}})
	if err != nil {
		t.Fatal(err)
	}
	plan := started.GetFlag().GetRollout()

	for _, state := range []string{"active", "paused"} {
		if state == "paused" {
			if _, err := r.admin.PauseRollout(ctx, &cpv1.PauseRolloutRequest{Namespace: "svc", Flag: "f"}); err != nil {
				t.Fatal(err)
			}
			plan.State = cpv1.RolloutState_ROLLOUT_STATE_PAUSED
		}
		_, err := r.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true, RolloutPercent: proto.Float64(30)})
		wantCode(t, err, codes.FailedPrecondition)

		// Restating the current percentage, or leaving it out, is fine, and
		// the plan survives the edit.
		for _, percent := range []*float64{proto.Float64(10), nil} {
			f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Description: state, RolloutPercent: percent})
			if f.GetRolloutPercent() != 10 || f.GetDescription() != state || f.GetEnabled() || !proto.Equal(f.GetRollout(), plan) {
				t.Fatalf("%s rollout: PutFlag returned %v, want percent 10 and plan %v", state, f, plan)
			}
		}
	}

	// Once the rollout is over the percentage is the user's again; the plan
	// stays as a record.
	aborted, err := r.admin.AbortRollout(ctx, &cpv1.AbortRolloutRequest{Namespace: "svc", Flag: "f"})
	if err != nil {
		t.Fatal(err)
	}
	f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true, RolloutPercent: proto.Float64(30)})
	if f.GetRolloutPercent() != 30 || !proto.Equal(f.GetRollout(), aborted.GetFlag().GetRollout()) {
		t.Fatalf("after abort: PutFlag returned %v", f)
	}
}

// racingStore lets another writer change a flag between the server's read
// and its write, the next races times the server reads one.
type racingStore struct {
	store.Store
	mu    sync.Mutex
	races int
	reads int
}

func (s *racingStore) GetFlag(ctx context.Context, namespace, key string) (model.Flag, error) {
	f, err := s.Store.GetFlag(ctx, namespace, key)
	s.mu.Lock()
	s.reads++
	race := err == nil && s.races > 0
	if race {
		s.races--
	}
	s.mu.Unlock()
	if race {
		other := f.Clone()
		other.Description = "concurrent edit"
		if _, err := s.Store.PutFlag(ctx, namespace, other, store.WriteOptions{Actor: "other"}); err != nil {
			return model.Flag{}, err
		}
	}
	return f, err
}

// arm makes the next n reads race and resets the read count.
func (s *racingStore) arm(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.races, s.reads = n, 0
}

func (s *racingStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func TestFlagWritesRetryLostRaces(t *testing.T) {
	st := &racingStore{Store: memory.New()}
	r := startReplica(t, st, replicaOptions{})
	createNamespace(t, r, "svc")
	f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
	ctx := ctxAs(t, "alice")

	tests := []struct {
		name      string
		races     int
		call      func() error
		wantCode  codes.Code
		wantReads int
	}{
		{
			name:  "PutFlag wins on its last retry",
			races: casRetries,
			call: func() error {
				resp, err := r.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true, Description: "mine"})
				if err == nil && resp.GetFlag().GetDescription() != "mine" {
					t.Errorf("description = %q, want the request's", resp.GetFlag().GetDescription())
				}
				return err
			},
			wantCode:  codes.OK,
			wantReads: casRetries + 1,
		},
		{
			name:  "PutFlag gives up",
			races: casRetries + 1,
			call: func() error {
				_, err := r.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true})
				return err
			},
			wantCode:  codes.Aborted,
			wantReads: casRetries + 1,
		},
		{
			name:  "PutFlag with an expected revision never retries",
			races: 1,
			call: func() error {
				resp, err := r.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: "svc"})
				if err != nil {
					return err
				}
				f = snapshotFlag(t, resp.GetSnapshot(), "f")
				_, err = r.admin.PutFlag(ctx, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", ExpectedRevision: f.GetRevision()})
				return err
			},
			wantCode:  codes.Aborted,
			wantReads: 1,
		},
		{
			name:  "rollout transition wins on retry",
			races: 1,
			call: func() error {
				_, err := r.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: "svc", Flag: "f", Stages: []*cpv1.RolloutStage{{Percent: 5}, {Percent: 100}}})
				return err
			},
			wantCode:  codes.OK,
			wantReads: 2,
		},
		{
			name:  "rollout transition gives up",
			races: casRetries + 1,
			call: func() error {
				_, err := r.admin.AdvanceRollout(ctx, &cpv1.AdvanceRolloutRequest{Namespace: "svc", Flag: "f"})
				return err
			},
			wantCode:  codes.Aborted,
			wantReads: casRetries + 1,
		},
	}
	for _, tt := range tests {
		st.arm(tt.races)
		err := tt.call()
		if got := status.Code(err); got != tt.wantCode {
			t.Errorf("%s: got %v (%v), want %v", tt.name, got, err, tt.wantCode)
		}
		if got := st.readCount(); got != tt.wantReads {
			t.Errorf("%s: flag read %d times, want %d", tt.name, got, tt.wantReads)
		}
	}
}

// creationRaceStore runs a hook once, after the first read that finds no
// flag, as another creator in the same process would between that read and
// the write it leads to.
type creationRaceStore struct {
	store.Store
	fired atomic.Bool
	race  func()
}

func (s *creationRaceStore) GetFlag(ctx context.Context, namespace, key string) (model.Flag, error) {
	f, err := s.Store.GetFlag(ctx, namespace, key)
	if errors.Is(err, model.ErrNotFound) && s.fired.CompareAndSwap(false, true) {
		s.race()
	}
	return f, err
}

// TestConcurrentFlagCreationKeepsTheFirstWriter checks that a creator that
// lost the race to another one merges with the flag that now exists instead
// of overwriting it with new-flag defaults. The store cannot express
// "create only", so this relies on PutFlag re-reading under its creation lock.
func TestConcurrentFlagCreationKeepsTheFirstWriter(t *testing.T) {
	st := &creationRaceStore{Store: memory.New()}
	r := startReplica(t, st, replicaOptions{})
	createNamespace(t, r, "svc")
	st.race = func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Enabled: true, RolloutPercent: proto.Float64(5), Salt: "bob"})
			if f.GetRolloutPercent() != 5 {
				t.Errorf("competing creator got percent %v", f.GetRolloutPercent())
			}
		}()
		<-done
	}

	f := putFlag(t, r, &cpv1.PutFlagRequest{Namespace: "svc", Key: "f", Description: "alice"})
	if f.GetRolloutPercent() != 5 || f.GetSalt() != "bob" || f.GetDescription() != "alice" || f.GetEnabled() {
		t.Fatalf("flag = %v, want the competing creator's percent and salt with alice's other fields", f)
	}
}
