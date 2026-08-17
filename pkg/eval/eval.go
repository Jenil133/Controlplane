// Package eval evaluates feature flags and experiments from snapshot protos
// locally and deterministically, on top of package bucketing.
//
// A flag is evaluated for a unit u (user ID, account ID, ...) in this order:
//
//  1. flag disabled → off
//  2. u in the allowlist → on
//  3. u == "" → on only if rollout_percent >= 100
//  4. otherwise on iff bucket(salt, u) < threshold(rollout_percent)
//
// An experiment enrolls u unless it is disabled, u is empty or its weights
// sum to 0; an enrolled unit gets the variant bucketing.Choose picks. In both
// cases salt is the entity's salt, or its key when the salt is empty, so every
// service evaluating the same snapshot reaches the same answer.
//
// Compile a flag or experiment once per snapshot and evaluate the compiled
// value: it is immutable, safe for concurrent use, and evaluating it does not
// allocate for typical salt and unit ID lengths.
package eval

import (
	"slices"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/bucketing"
)

// Flag is a flag compiled for evaluation. The zero Flag, like a nil *Flag, is
// off for every unit.
type Flag struct {
	allowlist map[string]struct{}
	salt      string
	threshold uint32
	// emptyUnitOn is the answer for an empty unit ID, which is never hashed:
	// it would put every anonymous caller in the same bucket.
	emptyUnitOn bool
}

// CompileFlag prepares f for evaluation; the result does not refer to f. A nil
// or disabled flag compiles to a Flag that is always off.
func CompileFlag(f *cpv1.Flag) *Flag {
	if !f.GetEnabled() {
		// Rule 1 overrides everything, the allowlist included.
		return &Flag{}
	}
	salt := f.GetSalt()
	if salt == "" {
		salt = f.GetKey()
	}
	p := f.GetRolloutPercent()
	c := &Flag{salt: salt, threshold: bucketing.Threshold(p), emptyUnitOn: p >= 100}
	if list := f.GetAllowlist(); len(list) > 0 {
		c.allowlist = make(map[string]struct{}, len(list))
		for _, u := range list {
			c.allowlist[u] = struct{}{}
		}
	}
	return c
}

// Enabled reports whether the flag is on for unitID.
func (f *Flag) Enabled(unitID string) bool {
	if f == nil {
		return false
	}
	if _, ok := f.allowlist[unitID]; ok {
		return true
	}
	if unitID == "" {
		return f.emptyUnitOn
	}
	// 0% and 100% are the most common states and need no hash: no bucket is
	// below 0 and every bucket is below Buckets.
	switch f.threshold {
	case 0:
		return false
	case bucketing.Buckets:
		return true
	}
	return bucketing.Bucket(f.salt, unitID) < f.threshold
}

// Experiment is an experiment compiled for assignment. The zero Experiment,
// like a nil *Experiment, enrolls nobody.
type Experiment struct {
	salt     string
	weights  []uint32
	variants []*cpv1.Variant
}

// CompileExperiment prepares e for assignment. Everything assignment depends
// on is copied, but the variants Assign returns are e's own messages, so they
// must not be modified. A nil or disabled experiment enrolls nobody.
func CompileExperiment(e *cpv1.Experiment) *Experiment {
	if !e.GetEnabled() {
		return &Experiment{}
	}
	salt := e.GetSalt()
	if salt == "" {
		salt = e.GetKey()
	}
	vs := e.GetVariants()
	c := &Experiment{salt: salt, weights: make([]uint32, len(vs)), variants: slices.Clone(vs)}
	for i, v := range vs {
		c.weights[i] = v.GetWeight()
	}
	return c
}

// Assign returns the variant unitID is assigned to, or false when unitID is
// not enrolled: the experiment is disabled, unitID is empty or the weights
// sum to 0. A variant with weight 0 is never returned.
func (e *Experiment) Assign(unitID string) (*cpv1.Variant, bool) {
	if e == nil || unitID == "" {
		return nil, false
	}
	i := bucketing.Choose(e.salt, unitID, e.weights)
	if i < 0 {
		return nil, false
	}
	return e.variants[i], true
}

// FlagEnabled evaluates f once: CompileFlag(f).Enabled(unitID). Compile flags
// that are evaluated repeatedly.
func FlagEnabled(f *cpv1.Flag, unitID string) bool {
	return CompileFlag(f).Enabled(unitID)
}

// Variant assigns unitID once: CompileExperiment(e).Assign(unitID). Compile
// experiments that are assigned repeatedly.
func Variant(e *cpv1.Experiment, unitID string) (*cpv1.Variant, bool) {
	return CompileExperiment(e).Assign(unitID)
}
