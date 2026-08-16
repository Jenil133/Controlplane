// Package model holds the control plane's domain types and their validation
// rules. It has no dependencies on storage or transport.
//
// The JSON encoding of these types (snake_case) is what the revision history
// and the audit log store, so field tags are part of the storage format.
package model

import (
	"encoding/json"
	"slices"
	"time"
)

// Entity types, as recorded in the audit log and in diffs.
const (
	EntityNamespace      = "namespace"
	EntityConfig         = "config"
	EntityFlag           = "flag"
	EntityExperiment     = "experiment"
	EntityRateLimit      = "rate_limit"
	EntityCircuitBreaker = "circuit_breaker"
)

// Audit actions.
const (
	ActionCreate         = "create"
	ActionUpdate         = "update"
	ActionDelete         = "delete"
	ActionRollback       = "rollback"
	ActionRolloutStart   = "rollout.start"
	ActionRolloutAdvance = "rollout.advance"
	ActionRolloutPause   = "rollout.pause"
	ActionRolloutResume  = "rollout.resume"
	ActionRolloutAbort   = "rollout.abort"
)

// Namespace groups the configuration consumed by one service or environment.
// Revision starts at 1 when the namespace is created and grows by one on every
// write inside it.
type Namespace struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Revision    int64     `json:"revision"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CreatedBy   string    `json:"created_by,omitempty"`
}

// Config is an arbitrary JSON value addressed by key.
type Config struct {
	Key         string          `json:"key"`
	Value       json.RawMessage `json:"value"`
	Description string          `json:"description,omitempty"`
	Revision    int64           `json:"revision"`
	UpdatedAt   time.Time       `json:"updated_at"`
	UpdatedBy   string          `json:"updated_by"`
}

// Flag is a feature flag with an optional percentage rollout.
type Flag struct {
	Key         string `json:"key"`
	Enabled     bool   `json:"enabled"`
	Description string `json:"description,omitempty"`
	// RolloutPercent is the share of units, 0 to 100 in steps of 0.01, that
	// see the flag on.
	RolloutPercent float64 `json:"rollout_percent"`
	// Salt seeds rollout bucketing. Defaults to Key.
	Salt string `json:"salt"`
	// Allowlist holds units that always see the flag on while it is enabled.
	Allowlist []string `json:"allowlist,omitempty"`
	// Rollout is the staged rollout plan, if one was ever started.
	Rollout   *RolloutPlan `json:"rollout,omitempty"`
	Revision  int64        `json:"revision"`
	UpdatedAt time.Time    `json:"updated_at"`
	UpdatedBy string       `json:"updated_by"`
}

// RolloutState is the lifecycle state of a rollout plan.
type RolloutState string

const (
	RolloutActive    RolloutState = "active"
	RolloutPaused    RolloutState = "paused"
	RolloutCompleted RolloutState = "completed"
	RolloutAborted   RolloutState = "aborted"
)

// Owns reports whether a plan in this state controls the flag's
// RolloutPercent, i.e. manual percentage changes are refused.
func (s RolloutState) Owns() bool {
	return s == RolloutActive || s == RolloutPaused
}

// RolloutStage is one step of a staged rollout.
type RolloutStage struct {
	Percent float64 `json:"percent"`
	// Duration is the minimum time at this stage before the controller
	// advances automatically. Zero means manual advance only.
	Duration time.Duration `json:"duration"`
}

// RolloutPlan steps a flag's RolloutPercent through increasing stages.
type RolloutPlan struct {
	Stages         []RolloutStage `json:"stages"`
	CurrentStage   int            `json:"current_stage"`
	State          RolloutState   `json:"state"`
	StartedAt      time.Time      `json:"started_at"`
	StageStartedAt time.Time      `json:"stage_started_at"`
	StartedBy      string         `json:"started_by"`
}

// RolloutRef identifies a flag with an active rollout.
type RolloutRef struct {
	Namespace string
	Key       string
}

// Variant is one arm of an experiment.
type Variant struct {
	Name    string          `json:"name"`
	Weight  uint32          `json:"weight"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Experiment is an A/B test definition.
type Experiment struct {
	Key         string    `json:"key"`
	Enabled     bool      `json:"enabled"`
	Description string    `json:"description,omitempty"`
	Salt        string    `json:"salt"`
	Variants    []Variant `json:"variants"`
	Revision    int64     `json:"revision"`
	UpdatedAt   time.Time `json:"updated_at"`
	UpdatedBy   string    `json:"updated_by"`
}

// RateLimit is a token-bucket limit enforced by every client instance.
type RateLimit struct {
	Key               string    `json:"key"`
	Enabled           bool      `json:"enabled"`
	Description       string    `json:"description,omitempty"`
	RequestsPerSecond float64   `json:"requests_per_second"`
	Burst             uint32    `json:"burst"`
	Revision          int64     `json:"revision"`
	UpdatedAt         time.Time `json:"updated_at"`
	UpdatedBy         string    `json:"updated_by"`
}

// CircuitBreaker opens when the failure rate over a rolling window reaches
// FailureRateThreshold.
type CircuitBreaker struct {
	Key                  string        `json:"key"`
	Enabled              bool          `json:"enabled"`
	Description          string        `json:"description,omitempty"`
	FailureRateThreshold float64       `json:"failure_rate_threshold"`
	MinRequests          uint32        `json:"min_requests"`
	Window               time.Duration `json:"window"`
	OpenDuration         time.Duration `json:"open_duration"`
	HalfOpenMaxRequests  uint32        `json:"half_open_max_requests"`
	Revision             int64         `json:"revision"`
	UpdatedAt            time.Time     `json:"updated_at"`
	UpdatedBy            string        `json:"updated_by"`
}

// Snapshot is the complete state of a namespace at one revision. Every list
// is sorted by key.
type Snapshot struct {
	Namespace       string           `json:"namespace"`
	Revision        int64            `json:"revision"`
	UpdatedAt       time.Time        `json:"updated_at"`
	Configs         []Config         `json:"configs"`
	Flags           []Flag           `json:"flags"`
	Experiments     []Experiment     `json:"experiments"`
	RateLimits      []RateLimit      `json:"rate_limits"`
	CircuitBreakers []CircuitBreaker `json:"circuit_breakers"`
}

// Revision is one entry in a namespace's history.
type Revision struct {
	Namespace string
	Revision  int64
	Actor     string
	CreatedAt time.Time
	Summary   string
	// Snapshot is the full state at this revision. Only populated by
	// Store.GetRevision.
	Snapshot Snapshot
}

// AuditEvent records one change to one entity.
type AuditEvent struct {
	ID         int64
	Namespace  string
	Revision   int64
	Actor      string
	Time       time.Time
	Action     string
	EntityType string
	EntityKey  string
	// Before and After are the entity's JSON encoding; nil when it did not
	// exist on that side of the change.
	Before  json.RawMessage
	After   json.RawMessage
	Message string
}

// AuditFilter selects audit events. Zero fields do not filter.
type AuditFilter struct {
	Namespace  string
	EntityType string
	EntityKey  string
	Actor      string
	// Since is inclusive, Until exclusive.
	Since time.Time
	Until time.Time
	// BeforeID returns only events with a smaller ID (for paging).
	BeforeID int64
	// Limit caps the result; stores use 50 when it is <= 0 and never return
	// more than 500.
	Limit int
}

// Normalize fills in defaults before validation.
func (f *Flag) Normalize() {
	if f.Salt == "" {
		f.Salt = f.Key
	}
}

// Normalize fills in defaults before validation.
func (e *Experiment) Normalize() {
	if e.Salt == "" {
		e.Salt = e.Key
	}
}

// Clone returns a deep copy.
func (c Config) Clone() Config {
	c.Value = slices.Clone(c.Value)
	return c
}

// Clone returns a deep copy.
func (f Flag) Clone() Flag {
	f.Allowlist = slices.Clone(f.Allowlist)
	if f.Rollout != nil {
		p := *f.Rollout
		p.Stages = slices.Clone(p.Stages)
		f.Rollout = &p
	}
	return f
}

// Clone returns a deep copy.
func (e Experiment) Clone() Experiment {
	if e.Variants != nil {
		vs := make([]Variant, len(e.Variants))
		for i, v := range e.Variants {
			v.Payload = slices.Clone(v.Payload)
			vs[i] = v
		}
		e.Variants = vs
	}
	return e
}

// Clone returns a deep copy.
func (s Snapshot) Clone() Snapshot {
	out := s
	out.Configs = cloneEach(s.Configs, Config.Clone)
	out.Flags = cloneEach(s.Flags, Flag.Clone)
	out.Experiments = cloneEach(s.Experiments, Experiment.Clone)
	out.RateLimits = slices.Clone(s.RateLimits)
	out.CircuitBreakers = slices.Clone(s.CircuitBreakers)
	return out
}

func cloneEach[T any](in []T, clone func(T) T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, v := range in {
		out[i] = clone(v)
	}
	return out
}
