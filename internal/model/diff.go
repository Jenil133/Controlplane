package model

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ChangeType says how an entry differs between two snapshots.
type ChangeType string

const (
	ChangeAdded    ChangeType = "added"
	ChangeModified ChangeType = "modified"
	ChangeRemoved  ChangeType = "removed"
)

// Change describes how one entry differs between two snapshots.
type Change struct {
	EntityType string
	Key        string
	Type       ChangeType
	// Before and After are the entry's JSON encoding; nil on the side where
	// the entry does not exist.
	Before json.RawMessage
	After  json.RawMessage
}

// entityOrder is the order in which Diff reports entity types.
var entityOrder = []string{EntityConfig, EntityFlag, EntityExperiment, EntityRateLimit, EntityCircuitBreaker}

type diffEntry struct {
	full    json.RawMessage // as stored, including metadata
	content []byte          // canonical JSON without revision and update metadata
}

// Diff returns the changes that turn from into to, ordered by entity type
// (config, flag, experiment, rate_limit, circuit_breaker) and then key.
// Entries are compared by content: revision, updated_at and updated_by are
// ignored, and JSON values are compared semantically.
func Diff(from, to Snapshot) ([]Change, error) {
	before, err := diffEntries(from)
	if err != nil {
		return nil, err
	}
	after, err := diffEntries(to)
	if err != nil {
		return nil, err
	}
	var changes []Change
	for _, entity := range entityOrder {
		b, a := before[entity], after[entity]
		keys := make([]string, 0, len(b)+len(a))
		for k := range b {
			keys = append(keys, k)
		}
		for k := range a {
			if _, ok := b[k]; !ok {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			old, hadOld := b[k]
			cur, hasCur := a[k]
			switch {
			case !hadOld:
				changes = append(changes, Change{EntityType: entity, Key: k, Type: ChangeAdded, After: cur.full})
			case !hasCur:
				changes = append(changes, Change{EntityType: entity, Key: k, Type: ChangeRemoved, Before: old.full})
			case !bytes.Equal(old.content, cur.content):
				changes = append(changes, Change{EntityType: entity, Key: k, Type: ChangeModified, Before: old.full, After: cur.full})
			}
		}
	}
	return changes, nil
}

func diffEntries(s Snapshot) (map[string]map[string]diffEntry, error) {
	out := make(map[string]map[string]diffEntry, len(entityOrder))
	add := func(entity, key string, full, stripped any) error {
		f, err := json.Marshal(full)
		if err != nil {
			return fmt.Errorf("encode %s %q: %w", entity, key, err)
		}
		c, err := canonicalJSON(stripped)
		if err != nil {
			return fmt.Errorf("encode %s %q: %w", entity, key, err)
		}
		if out[entity] == nil {
			out[entity] = make(map[string]diffEntry)
		}
		out[entity][key] = diffEntry{full: f, content: c}
		return nil
	}
	for _, c := range s.Configs {
		stripped := c
		stripped.Revision, stripped.UpdatedAt, stripped.UpdatedBy = 0, time.Time{}, ""
		if err := add(EntityConfig, c.Key, c, stripped); err != nil {
			return nil, err
		}
	}
	for _, f := range s.Flags {
		stripped := f
		stripped.Revision, stripped.UpdatedAt, stripped.UpdatedBy = 0, time.Time{}, ""
		if err := add(EntityFlag, f.Key, f, stripped); err != nil {
			return nil, err
		}
	}
	for _, e := range s.Experiments {
		stripped := e
		stripped.Revision, stripped.UpdatedAt, stripped.UpdatedBy = 0, time.Time{}, ""
		if err := add(EntityExperiment, e.Key, e, stripped); err != nil {
			return nil, err
		}
	}
	for _, r := range s.RateLimits {
		stripped := r
		stripped.Revision, stripped.UpdatedAt, stripped.UpdatedBy = 0, time.Time{}, ""
		if err := add(EntityRateLimit, r.Key, r, stripped); err != nil {
			return nil, err
		}
	}
	for _, cb := range s.CircuitBreakers {
		stripped := cb
		stripped.Revision, stripped.UpdatedAt, stripped.UpdatedBy = 0, time.Time{}, ""
		if err := add(EntityCircuitBreaker, cb.Key, cb, stripped); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// canonicalJSON encodes v with object keys sorted, no insignificant
// whitespace and numbers in a canonical exact form, so semantically equal
// documents compare equal byte-wise even when embedded raw JSON was formatted
// differently (PostgreSQL's jsonb reorders keys and adds spaces). The output
// is only a comparison key: numbers are never rounded through float64, so
// integers above 2^53 and values beyond float64 range keep their identity.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	return json.Marshal(canonicalNumbers(generic))
}

// canonicalNumbers rewrites every json.Number in a decoded document.
func canonicalNumbers(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, e := range v {
			v[k] = canonicalNumbers(e)
		}
	case []any:
		for i, e := range v {
			v[i] = canonicalNumbers(e)
		}
	case json.Number:
		return json.Number(canonicalNumber(string(v)))
	}
	return v
}

// canonicalNumber returns the JSON number n as sign, integer digits without
// leading or trailing zeros, and a decimal exponent ("12e3" for 12000, 1.2e4
// and 12E3), with zero, -0 and 0.0 all "0". It works on the text, so its cost
// is linear in len(n) however large the exponent is. A number whose exponent
// does not fit an int64 is returned unchanged.
func canonicalNumber(n string) string {
	sign := ""
	if strings.HasPrefix(n, "-") {
		sign, n = "-", n[1:]
	}
	mantissa, exp := n, int64(0)
	if i := strings.IndexAny(n, "eE"); i >= 0 {
		e, err := strconv.ParseInt(strings.TrimPrefix(n[i+1:], "+"), 10, 64)
		if err != nil {
			return sign + n
		}
		mantissa, exp = n[:i], e
	}
	intPart, fracPart, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return "0"
	}
	trimmed := strings.TrimRight(digits, "0")
	exp += int64(len(digits)-len(trimmed)) - int64(len(fracPart))
	return sign + trimmed + "e" + strconv.FormatInt(exp, 10)
}

// EncodeEntry returns the JSON encoding of an entity as stored in the audit
// log and revision history.
func EncodeEntry(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

// PrepareRollback returns the copy of a historical snapshot that a rollback
// restores. Rollouts that were ACTIVE come back PAUSED: their stage timers are
// stale and must not fire the moment the rollback lands.
func PrepareRollback(target Snapshot) Snapshot {
	out := target.Clone()
	for i := range out.Flags {
		if p := out.Flags[i].Rollout; p != nil && p.State == RolloutActive {
			p.State = RolloutPaused
		}
	}
	return out
}

// SortSnapshot sorts every list in s by key, in place.
func SortSnapshot(s *Snapshot) {
	slices.SortFunc(s.Configs, func(a, b Config) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(s.Flags, func(a, b Flag) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(s.Experiments, func(a, b Experiment) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(s.RateLimits, func(a, b RateLimit) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(s.CircuitBreakers, func(a, b CircuitBreaker) int { return cmp.Compare(a.Key, b.Key) })
}
