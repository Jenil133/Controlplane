package model

import (
	"encoding/json"
	"regexp"
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

// Validate checks a flag before it is written.
func (f Flag) Validate() error {
	if err := ValidateKey(f.Key); err != nil {
		return err
	}
	return validateDescription(f.Description)
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
