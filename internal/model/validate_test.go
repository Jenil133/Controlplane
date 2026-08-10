package model

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
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
