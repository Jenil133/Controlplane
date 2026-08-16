package model

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateNamespaceName(t *testing.T) {
	valid := []string{"a", "checkout", "checkout/prod", "team-a.svc_1", "0x"}
	for _, name := range valid {
		if err := ValidateNamespaceName(name); err != nil {
			t.Errorf("ValidateNamespaceName(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{"", "Checkout", "-a", "a-", "a b", "a/", "/a", strings.Repeat("a", MaxNamespaceLen+1)}
	for _, name := range invalid {
		if err := ValidateNamespaceName(name); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidateNamespaceName(%q) = %v, want ErrInvalid", name, err)
		}
	}
}

func TestValidateKey(t *testing.T) {
	valid := []string{"a", "db.timeout_ms", "Payments/RetryPolicy", "x-1"}
	for _, key := range valid {
		if err := ValidateKey(key); err != nil {
			t.Errorf("ValidateKey(%q) = %v, want nil", key, err)
		}
	}
	invalid := []string{"", ".a", "a.", "a b", "a:b", strings.Repeat("k", MaxKeyLen+1)}
	for _, key := range invalid {
		if err := ValidateKey(key); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidateKey(%q) = %v, want ErrInvalid", key, err)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"object", Config{Key: "k", Value: json.RawMessage(`{"a":1}`)}, false},
		{"scalar", Config{Key: "k", Value: json.RawMessage(`42`)}, false},
		{"missing value", Config{Key: "k"}, true},
		{"bad json", Config{Key: "k", Value: json.RawMessage(`{"a":`)}, true},
		{"too large", Config{Key: "k", Value: json.RawMessage(`"` + strings.Repeat("x", MaxValueBytes) + `"`)}, true},
		{"long description", Config{Key: "k", Value: json.RawMessage(`1`), Description: strings.Repeat("d", MaxDescriptionLen+1)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestExperimentValidate(t *testing.T) {
	base := func() Experiment {
		return Experiment{
			Key: "checkout-button",
			Variants: []Variant{
				{Name: "control", Weight: 50},
				{Name: "treatment", Weight: 50, Payload: json.RawMessage(`{"color":"green"}`)},
			},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Experiment)
		wantErr bool
	}{
		{"valid", func(*Experiment) {}, false},
		{"single variant", func(e *Experiment) { e.Variants = e.Variants[:1] }, false},
		{"no variants", func(e *Experiment) { e.Variants = nil }, true},
		{"duplicate variant", func(e *Experiment) { e.Variants[1].Name = "control" }, true},
		{"bad variant name", func(e *Experiment) { e.Variants[0].Name = "con trol" }, true},
		{"zero weights", func(e *Experiment) { e.Variants[0].Weight, e.Variants[1].Weight = 0, 0 }, true},
		{"weights overflow", func(e *Experiment) { e.Variants[0].Weight = MaxTotalWeight }, true},
		{"bad payload", func(e *Experiment) { e.Variants[1].Payload = json.RawMessage(`{`) }, true},
		{"too many variants", func(e *Experiment) {
			e.Variants = nil
			for i := 0; i <= MaxVariants; i++ {
				e.Variants = append(e.Variants, Variant{Name: "v" + strings.Repeat("x", i), Weight: 1})
			}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			tt.mutate(&e)
			e.Normalize()
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExperimentNormalizeDefaultsSalt(t *testing.T) {
	e := Experiment{Key: "exp"}
	e.Normalize()
	if e.Salt != "exp" {
		t.Fatalf("Salt = %q, want %q", e.Salt, "exp")
	}
	e = Experiment{Key: "exp", Salt: "custom"}
	e.Normalize()
	if e.Salt != "custom" {
		t.Fatalf("Salt = %q, want %q", e.Salt, "custom")
	}
}

func TestCloneIsDeep(t *testing.T) {
	c := Config{Key: "k", Value: json.RawMessage(`[1]`)}
	cc := c.Clone()
	cc.Value[1] = '2'
	if string(c.Value) != `[1]` {
		t.Fatalf("Config.Clone shares Value: original now %s", c.Value)
	}

	e := Experiment{Key: "e", Variants: []Variant{{Name: "a", Weight: 1, Payload: json.RawMessage(`[1]`)}}}
	ec := e.Clone()
	ec.Variants[0].Name = "b"
	ec.Variants[0].Payload[1] = '2'
	if e.Variants[0].Name != "a" || string(e.Variants[0].Payload) != `[1]` {
		t.Fatalf("Experiment.Clone is shallow: original now %+v", e.Variants[0])
	}
}

func TestFlagValidate(t *testing.T) {
	now := time.Now()
	base := func() Flag {
		f := Flag{Key: "new-cart", Enabled: true, RolloutPercent: 25.5, Allowlist: []string{"u1", "u2"}}
		f.Normalize()
		return f
	}
	plan := func(state RolloutState, stage int) *RolloutPlan {
		return &RolloutPlan{
			Stages:       []RolloutStage{{Percent: 1, Duration: time.Hour}, {Percent: 25.5}, {Percent: 100}},
			CurrentStage: stage, State: state, StartedAt: now, StageStartedAt: now,
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Flag)
		wantErr bool
	}{
		{"valid", func(*Flag) {}, false},
		{"zero percent", func(f *Flag) { f.RolloutPercent = 0 }, false},
		{"percent above 100", func(f *Flag) { f.RolloutPercent = 100.01 }, true},
		{"negative percent", func(f *Flag) { f.RolloutPercent = -1 }, true},
		{"three decimals", func(f *Flag) { f.RolloutPercent = 12.345 }, true},
		{"empty allowlist entry", func(f *Flag) { f.Allowlist = []string{""} }, true},
		{"duplicate allowlist entry", func(f *Flag) { f.Allowlist = []string{"a", "a"} }, true},
		{"active plan matches percent", func(f *Flag) { f.Rollout = plan(RolloutActive, 1) }, false},
		{"active plan disagrees with percent", func(f *Flag) { f.Rollout = plan(RolloutActive, 0) }, true},
		{"completed plan may disagree", func(f *Flag) { f.Rollout = plan(RolloutCompleted, 2) }, false},
		{"stage out of range", func(f *Flag) { f.Rollout = plan(RolloutAborted, 3) }, true},
		{"unknown state", func(f *Flag) { f.Rollout = plan("weird", 1) }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := base()
			tt.mutate(&f)
			if err := f.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateRolloutStages(t *testing.T) {
	ok := [][]RolloutStage{
		{{Percent: 100}},
		{{Percent: 1, Duration: time.Hour}, {Percent: 1}, {Percent: 50, Duration: time.Minute}, {Percent: 100}},
	}
	for _, stages := range ok {
		if err := ValidateRolloutStages(stages); err != nil {
			t.Errorf("ValidateRolloutStages(%v) = %v", stages, err)
		}
	}
	bad := [][]RolloutStage{
		nil,
		{{Percent: 50}, {Percent: 10}},
		{{Percent: 0}},
		{{Percent: 10, Duration: -time.Second}},
		{{Percent: 10, Duration: MaxStageDuration + 1}},
		{{Percent: 101}},
	}
	for _, stages := range bad {
		if err := ValidateRolloutStages(stages); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidateRolloutStages(%v) = %v, want ErrInvalid", stages, err)
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	rl := RateLimit{Key: "checkout", Enabled: true, RequestsPerSecond: 0.5, Burst: 1}
	if err := rl.Validate(); err != nil {
		t.Fatalf("valid rate limit: %v", err)
	}
	for _, mutate := range []func(*RateLimit){
		func(r *RateLimit) { r.RequestsPerSecond = 0 },
		func(r *RateLimit) { r.RequestsPerSecond = MaxRequestsPerSecond + 1 },
		func(r *RateLimit) { r.Burst = 0 },
	} {
		r := rl
		mutate(&r)
		if err := r.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("rate limit %+v: Validate() = %v, want ErrInvalid", r, err)
		}
	}

	cb := CircuitBreaker{
		Key: "payments", Enabled: true, FailureRateThreshold: 0.5, MinRequests: 20,
		Window: 10 * time.Second, OpenDuration: 30 * time.Second, HalfOpenMaxRequests: 3,
	}
	if err := cb.Validate(); err != nil {
		t.Fatalf("valid breaker: %v", err)
	}
	for _, mutate := range []func(*CircuitBreaker){
		func(c *CircuitBreaker) { c.FailureRateThreshold = 0 },
		func(c *CircuitBreaker) { c.FailureRateThreshold = 1.1 },
		func(c *CircuitBreaker) { c.MinRequests = 0 },
		func(c *CircuitBreaker) { c.Window = 500 * time.Millisecond },
		func(c *CircuitBreaker) { c.OpenDuration = 2 * time.Hour },
		func(c *CircuitBreaker) { c.HalfOpenMaxRequests = 0 },
	} {
		c := cb
		mutate(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("breaker %+v: Validate() = %v, want ErrInvalid", c, err)
		}
	}
}
