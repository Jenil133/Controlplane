// Package model holds the control plane's domain types and their validation
// rules. It has no dependencies on storage or transport.
package model

import (
	"encoding/json"
	"slices"
	"time"
)

// Namespace groups the configuration consumed by one service or environment.
// Revision starts at 1 when the namespace is created and grows by one on every
// write inside it.
type Namespace struct {
	Name        string
	Description string
	Revision    int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Config is an arbitrary JSON value addressed by key.
type Config struct {
	Key         string
	Value       json.RawMessage
	Description string
	Revision    int64
	UpdatedAt   time.Time
	UpdatedBy   string
}

// Flag is a boolean feature flag.
type Flag struct {
	Key         string
	Enabled     bool
	Description string
	Revision    int64
	UpdatedAt   time.Time
	UpdatedBy   string
}

// Variant is one arm of an experiment.
type Variant struct {
	Name    string          `json:"name"`
	Weight  uint32          `json:"weight"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Experiment is an A/B test definition.
type Experiment struct {
	Key         string
	Enabled     bool
	Description string
	Salt        string
	Variants    []Variant
	Revision    int64
	UpdatedAt   time.Time
	UpdatedBy   string
}

// Snapshot is the complete state of a namespace at one revision. Entries are
// sorted by key.
type Snapshot struct {
	Namespace   string
	Revision    int64
	UpdatedAt   time.Time
	Configs     []Config
	Flags       []Flag
	Experiments []Experiment
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
