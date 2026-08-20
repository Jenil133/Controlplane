package client

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestTypedGetters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, _ := e.connect(e.options(), snap(1,
			config(t, "text", "hello"),
			config(t, "int", 42),
			config(t, "negative", -7),
			config(t, "fraction", 2.5),
			config(t, "huge", 1e300),
			config(t, "true", true),
			config(t, "timeout", "1m30s"),
			config(t, "not-a-duration", "soon"),
			config(t, "null", nil),
			config(t, "object", map[string]any{"a": 1}),
		))

		strs := []struct {
			key  string
			want string
		}{{"text", "hello"}, {"timeout", "1m30s"}, {"int", "def"}, {"null", "def"}, {"missing", "def"}}
		for _, tc := range strs {
			if got := c.String(tc.key, "def"); got != tc.want {
				t.Errorf("String(%q) = %q, want %q", tc.key, got, tc.want)
			}
		}
		ints := []struct {
			key  string
			want int64
		}{{"int", 42}, {"negative", -7}, {"fraction", -1}, {"huge", -1}, {"text", -1}, {"missing", -1}}
		for _, tc := range ints {
			if got := c.Int(tc.key, -1); got != tc.want {
				t.Errorf("Int(%q) = %d, want %d", tc.key, got, tc.want)
			}
		}
		floats := []struct {
			key  string
			want float64
		}{{"fraction", 2.5}, {"int", 42}, {"huge", 1e300}, {"true", -1}, {"missing", -1}}
		for _, tc := range floats {
			if got := c.Float(tc.key, -1); got != tc.want {
				t.Errorf("Float(%q) = %v, want %v", tc.key, got, tc.want)
			}
		}
		bools := []struct {
			key  string
			def  bool
			want bool
		}{{"true", false, true}, {"text", true, true}, {"int", false, false}, {"missing", true, true}}
		for _, tc := range bools {
			if got := c.Bool(tc.key, tc.def); got != tc.want {
				t.Errorf("Bool(%q, %v) = %v, want %v", tc.key, tc.def, got, tc.want)
			}
		}
		durations := []struct {
			key  string
			want time.Duration
		}{{"timeout", 90 * time.Second}, {"not-a-duration", time.Second}, {"int", time.Second}, {"missing", time.Second}}
		for _, tc := range durations {
			if got := c.Duration(tc.key, time.Second); got != tc.want {
				t.Errorf("Duration(%q) = %v, want %v", tc.key, got, tc.want)
			}
		}

		v, ok := c.Config("object")
		if !ok || v.GetStructValue().GetFields()["a"].GetNumberValue() != 1 {
			t.Errorf("Config(object) = %v, %v", v, ok)
		}
		if v, ok := c.Config("missing"); ok || v != nil {
			t.Errorf("Config(missing) = %v, %v", v, ok)
		}
	})
}

func TestDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c, _ := e.connect(e.options(), snap(1,
			config(t, "payments", map[string]any{
				"endpoint":   "https://payments.internal",
				"retries":    3,
				"timeout_ms": 1500,
				"regions":    []any{"eu", "us"},
			}),
			config(t, "text", "hello"),
		))

		var p struct {
			Endpoint  string   `json:"endpoint"`
			Retries   int      `json:"retries"`
			TimeoutMS int64    `json:"timeout_ms"`
			Regions   []string `json:"regions"`
		}
		if err := c.Decode("payments", &p); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if p.Endpoint != "https://payments.internal" || p.Retries != 3 || p.TimeoutMS != 1500 || len(p.Regions) != 2 {
			t.Fatalf("Decode = %+v", p)
		}

		var s string
		if err := c.Decode("text", &s); err != nil || s != "hello" {
			t.Fatalf("Decode(text) = %q, %v", s, err)
		}
		if err := c.Decode("missing", &s); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Decode(missing) = %v, want ErrNotFound", err)
		}
		var n int
		if err := c.Decode("text", &n); err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("Decode of a string into an int = %v, want a decoding error", err)
		}
	})
}

func TestReadsBeforeFirstSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		c := e.start(e.options())
		e.accept()
		synctest.Wait()

		if c.Snapshot() != nil || c.Revision() != 0 {
			t.Fatalf("Snapshot() = %v, Revision() = %d before the first snapshot", c.Snapshot(), c.Revision())
		}
		if _, ok := c.Config("any"); ok {
			t.Error("Config found a key")
		}
		if c.String("k", "def") != "def" || c.Int("k", 7) != 7 || c.Float("k", 0.5) != 0.5 || !c.Bool("k", true) || c.Duration("k", time.Second) != time.Second {
			t.Error("getters did not return their defaults")
		}
		if err := c.Decode("k", new(any)); !errors.Is(err, ErrNotFound) {
			t.Errorf("Decode = %v, want ErrNotFound", err)
		}
		if c.IsEnabled("new-cart", "alice") {
			t.Error("a flag is on")
		}
		if a, ok := c.Variant("cta-color", "alice"); ok {
			t.Errorf("enrolled in %+v", a)
		}
		for range 100 {
			if !c.Allow("checkout") {
				t.Fatal("rate limited without a policy")
			}
		}
		if _, ok := c.Breaker("payments"); ok {
			t.Error("a circuit breaker exists")
		}
		calls := 0
		for range 100 {
			_ = c.Do("payments", func() error { calls++; return errors.New("down") })
		}
		if calls != 100 {
			t.Errorf("Do called fn %d times out of 100 without a policy", calls)
		}
	})
}
