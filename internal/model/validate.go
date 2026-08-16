package model

import (
	"encoding/json"
	"math"
	"regexp"
	"time"
)

// Limits enforced on every write.
const (
	MaxNamespaceLen   = 128
	MaxKeyLen         = 256
	MaxVariantNameLen = 64
	MaxDescriptionLen = 1024
	MaxSaltLen        = 256
	MaxValueBytes     = 256 << 10
	MaxVariants       = 32
	MaxTotalWeight    = 1_000_000
	MaxActorLen       = 128

	MaxAllowlist     = 1000
	MaxUnitIDLen     = 256
	MaxRolloutStages = 20
	MaxStageDuration = 30 * 24 * time.Hour

	MaxRequestsPerSecond = 1_000_000
	MaxBurst             = 1_000_000
	MaxMinRequests       = 1_000_000
	MinBreakerWindow     = time.Second
	MaxBreakerWindow     = time.Hour
	MinOpenDuration      = 100 * time.Millisecond
	MaxOpenDuration      = time.Hour
	MaxHalfOpenRequests  = 1000
)

var (
	namespacePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._/-]*[a-z0-9])?$`)
	keyPattern       = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._/-]*[A-Za-z0-9])?$`)
	variantPattern   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`)
)

// ValidateNamespaceName checks a namespace name such as "checkout/prod".
func ValidateNamespaceName(name string) error {
	if name == "" {
		return Invalidf("namespace is required")
	}
	if len(name) > MaxNamespaceLen {
		return Invalidf("namespace %q is longer than %d characters", name, MaxNamespaceLen)
	}
	if !namespacePattern.MatchString(name) {
		return Invalidf("namespace %q must be lowercase letters, digits and . _ / - and start and end with a letter or digit", name)
	}
	return nil
}

// ValidateKey checks a config, flag or experiment key.
func ValidateKey(key string) error {
	if key == "" {
		return Invalidf("key is required")
	}
	if len(key) > MaxKeyLen {
		return Invalidf("key %q is longer than %d characters", key, MaxKeyLen)
	}
	if !keyPattern.MatchString(key) {
		return Invalidf("key %q must be letters, digits and . _ / - and start and end with a letter or digit", key)
	}
	return nil
}

func validateDescription(d string) error {
	if len(d) > MaxDescriptionLen {
		return Invalidf("description is longer than %d characters", MaxDescriptionLen)
	}
	return nil
}

func validateJSON(field string, v json.RawMessage) error {
	if len(v) > MaxValueBytes {
		return Invalidf("%s is larger than %d bytes", field, MaxValueBytes)
	}
	if !json.Valid(v) {
		return Invalidf("%s is not valid JSON", field)
	}
	return nil
}

// Validate checks a namespace before creation.
func (n Namespace) Validate() error {
	if err := ValidateNamespaceName(n.Name); err != nil {
		return err
	}
	return validateDescription(n.Description)
}

// Validate checks a config before it is written.
func (c Config) Validate() error {
	if err := ValidateKey(c.Key); err != nil {
		return err
	}
	if len(c.Value) == 0 {
		return Invalidf("config %q: value is required", c.Key)
	}
	if err := validateJSON("value", c.Value); err != nil {
		return err
	}
	return validateDescription(c.Description)
}

// ValidatePercent checks a rollout percentage: 0 to 100 with at most two
// decimal places, matching the 10,000 buckets units are hashed into.
func ValidatePercent(field string, p float64) error {
	if math.IsNaN(p) || p < 0 || p > 100 {
		return Invalidf("%s must be between 0 and 100, got %v", field, p)
	}
	if math.Abs(p*100-math.Round(p*100)) > 1e-6 {
		return Invalidf("%s may have at most two decimal places, got %v", field, p)
	}
	return nil
}

// Validate checks a flag before it is written. Call Normalize first.
func (f Flag) Validate() error {
	if err := ValidateKey(f.Key); err != nil {
		return err
	}
	if err := validateDescription(f.Description); err != nil {
		return err
	}
	if f.Salt == "" || len(f.Salt) > MaxSaltLen {
		return Invalidf("flag %q: salt must be 1 to %d characters", f.Key, MaxSaltLen)
	}
	if err := ValidatePercent("rollout_percent", f.RolloutPercent); err != nil {
		return err
	}
	if len(f.Allowlist) > MaxAllowlist {
		return Invalidf("flag %q: allowlist has more than %d entries", f.Key, MaxAllowlist)
	}
	seen := make(map[string]bool, len(f.Allowlist))
	for _, unit := range f.Allowlist {
		if unit == "" || len(unit) > MaxUnitIDLen {
			return Invalidf("flag %q: allowlist entries must be 1 to %d characters", f.Key, MaxUnitIDLen)
		}
		if seen[unit] {
			return Invalidf("flag %q: duplicate allowlist entry %q", f.Key, unit)
		}
		seen[unit] = true
	}
	if f.Rollout != nil {
		if err := f.Rollout.Validate(); err != nil {
			return Invalidf("flag %q: rollout: %v", f.Key, err)
		}
		if p := f.Rollout.Stages[f.Rollout.CurrentStage].Percent; f.Rollout.State.Owns() && f.RolloutPercent != p {
			return Invalidf("flag %q: rollout_percent %v differs from the %s rollout stage (%v)", f.Key, f.RolloutPercent, f.Rollout.State, p)
		}
	}
	return nil
}

// ValidateRolloutStages checks the stages of a new rollout plan.
func ValidateRolloutStages(stages []RolloutStage) error {
	if len(stages) == 0 || len(stages) > MaxRolloutStages {
		return Invalidf("a rollout needs 1 to %d stages", MaxRolloutStages)
	}
	for i, st := range stages {
		if err := ValidatePercent("stage percent", st.Percent); err != nil {
			return err
		}
		if i > 0 && st.Percent < stages[i-1].Percent {
			return Invalidf("stage percentages must not decrease (stage %d: %v after %v)", i+1, st.Percent, stages[i-1].Percent)
		}
		if st.Duration < 0 || st.Duration > MaxStageDuration {
			return Invalidf("stage %d duration must be between 0 and %v", i+1, MaxStageDuration)
		}
	}
	if stages[len(stages)-1].Percent == 0 {
		return Invalidf("the final rollout stage must be above 0%%")
	}
	return nil
}

// Validate checks a stored rollout plan.
func (p RolloutPlan) Validate() error {
	if err := ValidateRolloutStages(p.Stages); err != nil {
		return err
	}
	if p.CurrentStage < 0 || p.CurrentStage >= len(p.Stages) {
		return Invalidf("current stage %d out of range", p.CurrentStage)
	}
	switch p.State {
	case RolloutActive, RolloutPaused, RolloutCompleted, RolloutAborted:
	default:
		return Invalidf("unknown rollout state %q", p.State)
	}
	if p.StartedAt.IsZero() || p.StageStartedAt.IsZero() {
		return Invalidf("rollout timestamps are required")
	}
	return nil
}

// Validate checks a rate limit before it is written.
func (r RateLimit) Validate() error {
	if err := ValidateKey(r.Key); err != nil {
		return err
	}
	if err := validateDescription(r.Description); err != nil {
		return err
	}
	if math.IsNaN(r.RequestsPerSecond) || r.RequestsPerSecond <= 0 || r.RequestsPerSecond > MaxRequestsPerSecond {
		return Invalidf("rate limit %q: requests_per_second must be above 0 and at most %d", r.Key, MaxRequestsPerSecond)
	}
	if r.Burst < 1 || r.Burst > MaxBurst {
		return Invalidf("rate limit %q: burst must be 1 to %d", r.Key, MaxBurst)
	}
	return nil
}

// Validate checks a circuit breaker before it is written.
func (c CircuitBreaker) Validate() error {
	if err := ValidateKey(c.Key); err != nil {
		return err
	}
	if err := validateDescription(c.Description); err != nil {
		return err
	}
	if math.IsNaN(c.FailureRateThreshold) || c.FailureRateThreshold <= 0 || c.FailureRateThreshold > 1 {
		return Invalidf("circuit breaker %q: failure_rate_threshold must be above 0 and at most 1", c.Key)
	}
	if c.MinRequests < 1 || c.MinRequests > MaxMinRequests {
		return Invalidf("circuit breaker %q: min_requests must be 1 to %d", c.Key, MaxMinRequests)
	}
	if c.Window < MinBreakerWindow || c.Window > MaxBreakerWindow {
		return Invalidf("circuit breaker %q: window must be between %v and %v", c.Key, MinBreakerWindow, MaxBreakerWindow)
	}
	if c.OpenDuration < MinOpenDuration || c.OpenDuration > MaxOpenDuration {
		return Invalidf("circuit breaker %q: open_duration must be between %v and %v", c.Key, MinOpenDuration, MaxOpenDuration)
	}
	if c.HalfOpenMaxRequests < 1 || c.HalfOpenMaxRequests > MaxHalfOpenRequests {
		return Invalidf("circuit breaker %q: half_open_max_requests must be 1 to %d", c.Key, MaxHalfOpenRequests)
	}
	return nil
}

// Validate checks an experiment before it is written. Call Normalize first.
func (e Experiment) Validate() error {
	if err := ValidateKey(e.Key); err != nil {
		return err
	}
	if err := validateDescription(e.Description); err != nil {
		return err
	}
	if e.Salt == "" || len(e.Salt) > MaxSaltLen {
		return Invalidf("experiment %q: salt must be 1 to %d characters", e.Key, MaxSaltLen)
	}
	if len(e.Variants) == 0 {
		return Invalidf("experiment %q: at least one variant is required", e.Key)
	}
	if len(e.Variants) > MaxVariants {
		return Invalidf("experiment %q: at most %d variants are allowed", e.Key, MaxVariants)
	}
	seen := make(map[string]bool, len(e.Variants))
	var total uint64
	for _, v := range e.Variants {
		if v.Name == "" || len(v.Name) > MaxVariantNameLen || !variantPattern.MatchString(v.Name) {
			return Invalidf("experiment %q: variant name %q must be 1 to %d letters, digits and . _ -", e.Key, v.Name, MaxVariantNameLen)
		}
		if seen[v.Name] {
			return Invalidf("experiment %q: duplicate variant %q", e.Key, v.Name)
		}
		seen[v.Name] = true
		if len(v.Payload) > 0 {
			if err := validateJSON("variant "+v.Name+" payload", v.Payload); err != nil {
				return err
			}
		}
		total += uint64(v.Weight)
	}
	if total == 0 {
		return Invalidf("experiment %q: variant weights must not all be zero", e.Key)
	}
	if total > MaxTotalWeight {
		return Invalidf("experiment %q: variant weights add up to more than %d", e.Key, MaxTotalWeight)
	}
	return nil
}
