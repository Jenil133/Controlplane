package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDiff(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	from := Snapshot{
		Configs: []Config{
			{Key: "same", Value: json.RawMessage(`{"a":1,"b":[1,2]}`), Revision: 2, UpdatedAt: t0, UpdatedBy: "alice"},
			{Key: "changed", Value: json.RawMessage(`1`), Revision: 3},
			{Key: "removed", Value: json.RawMessage(`true`), Revision: 4},
		},
		Flags: []Flag{{Key: "f", Enabled: true, RolloutPercent: 10, Salt: "f", Revision: 5}},
	}
	to := Snapshot{
		Configs: []Config{
			// Same content, different formatting and metadata: not a change.
			{Key: "same", Value: json.RawMessage(`{"b": [1, 2], "a": 1}`), Revision: 9, UpdatedAt: t0.Add(time.Hour), UpdatedBy: "bob"},
			{Key: "changed", Value: json.RawMessage(`2`), Revision: 9},
			{Key: "added", Value: json.RawMessage(`"x"`), Revision: 9},
		},
		Flags:      []Flag{{Key: "f", Enabled: true, RolloutPercent: 50, Salt: "f", Revision: 9}},
		RateLimits: []RateLimit{{Key: "api", Enabled: true, RequestsPerSecond: 5, Burst: 10}},
	}

	changes, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}
	type summary struct {
		entity, key string
		typ         ChangeType
	}
	want := []summary{
		{EntityConfig, "added", ChangeAdded},
		{EntityConfig, "changed", ChangeModified},
		{EntityConfig, "removed", ChangeRemoved},
		{EntityFlag, "f", ChangeModified},
		{EntityRateLimit, "api", ChangeAdded},
	}
	if len(changes) != len(want) {
		t.Fatalf("got %d changes, want %d: %+v", len(changes), len(want), changes)
	}
	for i, c := range changes {
		if (summary{c.EntityType, c.Key, c.Type}) != want[i] {
			t.Errorf("change %d = %s %s %s, want %+v", i, c.EntityType, c.Key, c.Type, want[i])
		}
		switch c.Type {
		case ChangeAdded:
			if c.Before != nil || c.After == nil {
				t.Errorf("added change %s has before=%s after=%s", c.Key, c.Before, c.After)
			}
		case ChangeRemoved:
			if c.Before == nil || c.After != nil {
				t.Errorf("removed change %s has before=%s after=%s", c.Key, c.Before, c.After)
			}
		case ChangeModified:
			if c.Before == nil || c.After == nil {
				t.Errorf("modified change %s missing a side", c.Key)
			}
		}
	}

	var flag Flag
	if err := json.Unmarshal(changes[3].After, &flag); err != nil || flag.RolloutPercent != 50 {
		t.Fatalf("flag after image = %s (%v)", changes[3].After, err)
	}

	same, err := Diff(to, to)
	if err != nil || len(same) != 0 {
		t.Fatalf("Diff(x, x) = %+v, %v; want no changes", same, err)
	}
}

func TestPrepareRollbackPausesActiveRollouts(t *testing.T) {
	now := time.Now()
	plan := func(s RolloutState) *RolloutPlan {
		return &RolloutPlan{Stages: []RolloutStage{{Percent: 10}, {Percent: 100}}, State: s, StartedAt: now, StageStartedAt: now}
	}
	target := Snapshot{Flags: []Flag{
		{Key: "active", Rollout: plan(RolloutActive)},
		{Key: "done", Rollout: plan(RolloutCompleted)},
		{Key: "none"},
	}}
	out := PrepareRollback(target)
	if out.Flags[0].Rollout.State != RolloutPaused {
		t.Fatalf("active rollout restored as %s, want paused", out.Flags[0].Rollout.State)
	}
	if out.Flags[1].Rollout.State != RolloutCompleted || out.Flags[2].Rollout != nil {
		t.Fatalf("other flags changed: %+v", out.Flags)
	}
	if target.Flags[0].Rollout.State != RolloutActive {
		t.Fatal("PrepareRollback modified its input")
	}
}

func TestSnapshotCloneIsDeep(t *testing.T) {
	s := Snapshot{
		Configs: []Config{{Key: "c", Value: json.RawMessage(`[1]`)}},
		Flags:   []Flag{{Key: "f", Allowlist: []string{"u1"}, Rollout: &RolloutPlan{Stages: []RolloutStage{{Percent: 5}}}}},
	}
	c := s.Clone()
	c.Configs[0].Value[1] = '2'
	c.Flags[0].Allowlist[0] = "u2"
	c.Flags[0].Rollout.Stages[0].Percent = 50
	c.Flags[0].Rollout.State = RolloutAborted
	if string(s.Configs[0].Value) != `[1]` || s.Flags[0].Allowlist[0] != "u1" ||
		s.Flags[0].Rollout.Stages[0].Percent != 5 || s.Flags[0].Rollout.State != "" {
		t.Fatalf("Clone is shallow: %+v", s)
	}
}

func TestCanonicalNumber(t *testing.T) {
	same := [][]string{
		{"0", "-0", "0.0", "0e5", "0.000"},
		{"1", "1.0", "1e0", "10e-1", "0.1e1"},
		{"100", "1e2", "1E2", "1e+2", "10e1", "100.00"},
		{"0.5", "5e-1", "0.50", "50e-2"},
		{"-12.5", "-1.25e1", "-125e-1"},
		{"1e400", "10e399", "0.1e401"},
	}
	for _, group := range same {
		want := canonicalNumber(group[0])
		for _, n := range group[1:] {
			if got := canonicalNumber(n); got != want {
				t.Errorf("canonicalNumber(%q) = %q, want %q (same value as %q)", n, got, want, group[0])
			}
		}
	}
	distinct := [][2]string{
		{"9007199254740993", "9007199254740992"}, // differ above 2^53
		{"1", "-1"},
		{"1e400", "1e401"},
		{"0.1", "0.01"},
		{"12", "120"},
	}
	for _, p := range distinct {
		if canonicalNumber(p[0]) == canonicalNumber(p[1]) {
			t.Errorf("canonicalNumber(%q) == canonicalNumber(%q), want different", p[0], p[1])
		}
	}
	if got := canonicalNumber("1e99999999999999999999"); got != "1e99999999999999999999" {
		t.Errorf("out-of-range exponent = %q, want the input unchanged", got)
	}
}

// TestDiffKeepsNumberIdentity covers values float64 cannot tell apart: a
// rollback must see them as different, and must not fail on them.
func TestDiffKeepsNumberIdentity(t *testing.T) {
	snap := func(v string) Snapshot {
		return Snapshot{Configs: []Config{{Key: "c", Value: json.RawMessage(v)}}}
	}
	for _, tt := range []struct {
		name, from, to string
		changes        int
	}{
		{"beyond float64 range, equal", `{"max":1e400}`, `{"max": 1e400}`, 0},
		{"beyond float64 range, different", `{"max":1e400}`, `{"max":1e401}`, 1},
		{"above 2^53, different", `{"id":9007199254740993}`, `{"id":9007199254740992}`, 1},
		{"formatting only", `{"x":1.0,"y":[100]}`, `{"y":[1e2],"x":1}`, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := Diff(snap(tt.from), snap(tt.to))
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != tt.changes {
				t.Fatalf("got %d changes, want %d: %+v", len(changes), tt.changes, changes)
			}
		})
	}
}
