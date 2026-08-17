package eval

import (
	"math"
	"strconv"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/bucketing"
)

func unit(i int) string { return "user-" + strconv.Itoa(i) }

func TestFlagRules(t *testing.T) {
	tests := []struct {
		name string
		flag *cpv1.Flag
		unit string
		want bool
	}{
		{"nil flag", nil, "user-1", false},
		{"disabled at 100%", &cpv1.Flag{Key: "f", RolloutPercent: 100}, "user-1", false},
		{"disabled beats allowlist", &cpv1.Flag{Key: "f", RolloutPercent: 100, Allowlist: []string{"vip"}}, "vip", false},
		{"allowlist beats 0%", &cpv1.Flag{Key: "f", Enabled: true, Allowlist: []string{"vip"}}, "vip", true},
		{"not allowlisted at 0%", &cpv1.Flag{Key: "f", Enabled: true, Allowlist: []string{"vip"}}, "user-1", false},
		{"100% is on", &cpv1.Flag{Key: "f", Enabled: true, RolloutPercent: 100}, "user-1", true},
		{"empty unit at 100%", &cpv1.Flag{Key: "f", Enabled: true, RolloutPercent: 100}, "", true},
		{"empty unit at 99.99%", &cpv1.Flag{Key: "f", Enabled: true, RolloutPercent: 99.99}, "", false},
		{"empty unit at 0%", &cpv1.Flag{Key: "f", Enabled: true}, "", false},
		// Rule 2 comes before rule 3. Validation never stores an empty
		// entry, but evaluation must not depend on that.
		{"allowlisted empty unit", &cpv1.Flag{Key: "f", Enabled: true, Allowlist: []string{""}}, "", true},
		// Bucket("new-cart", "user-42") is 7601: on from 76.02% up.
		{"bucket below threshold", &cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 76.02}, "user-42", true},
		{"bucket at threshold", &cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 76.01}, "user-42", false},
		// Rule 4 compares with round(p*100), never with p*100 itself, which
		// is 7.000000000000001 at 0.07% in binary floating point: an SDK that
		// skips the rounding turns on user-6936, whose bucket is 7. These two
		// answers pin the rounding for every SDK.
		{"0.07% excludes bucket 7", &cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 0.07}, "user-6936", false},
		{"0.08% includes bucket 7", &cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 0.08}, "user-6936", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompileFlag(tt.flag).Enabled(tt.unit); got != tt.want {
				t.Errorf("CompileFlag(%v).Enabled(%q) = %v, want %v", tt.flag, tt.unit, got, tt.want)
			}
			if got := FlagEnabled(tt.flag, tt.unit); got != tt.want {
				t.Errorf("FlagEnabled(%v, %q) = %v, want %v", tt.flag, tt.unit, got, tt.want)
			}
		})
	}
}

// TestGoldenDecisions freezes end-to-end answers, computed independently from
// the spec: like the golden buckets, every SDK must reproduce them.
func TestGoldenDecisions(t *testing.T) {
	units := []string{"user-1", "user-2", "user-3", "user-4", "user-5", "user-6", "user-7", "user-8"}
	flags := []struct {
		flag *cpv1.Flag
		want string // one digit per unit
	}{
		{&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 25}, "00000010"},
		{&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 50}, "11100010"},
		{&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 50, Allowlist: []string{"user-8"}}, "11100011"},
		{&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 50, Salt: "spring-2026"}, "00101010"},
		{&cpv1.Flag{Key: "dark-mode", Enabled: true, RolloutPercent: 1}, "00000000"},
		{&cpv1.Flag{Key: "dark-mode", Enabled: true, RolloutPercent: 99.99}, "11111111"},
	}
	for _, tt := range flags {
		f := CompileFlag(tt.flag)
		got := make([]byte, len(units))
		for i, u := range units {
			got[i] = '0'
			if f.Enabled(u) {
				got[i] = '1'
			}
		}
		if string(got) != tt.want {
			t.Errorf("flag %v: decisions %s, want %s", tt.flag, got, tt.want)
		}
	}

	e := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 50}, {Name: "green", Weight: 30}, {Name: "blue", Weight: 20},
	}})
	want := []string{"control", "control", "green", "green", "green", "control", "blue", "control"}
	for i, u := range units {
		if v, ok := e.Assign(u); !ok || v.GetName() != want[i] {
			t.Errorf("Assign(%q) = %q, %v; want %q", u, v.GetName(), ok, want[i])
		}
	}
}

// TestFlagMatchesBucketing checks rule 4 against package bucketing across
// percentages, with a salt that differs from the key.
func TestFlagMatchesBucketing(t *testing.T) {
	for _, p := range []float64{0, 0.01, 1, 12.34, 50, 99.99, 100} {
		f := CompileFlag(&cpv1.Flag{Key: "new-cart", Salt: "spring-2026", Enabled: true, RolloutPercent: p})
		for i := range 2000 {
			u := unit(i)
			if got, want := f.Enabled(u), bucketing.InRollout("spring-2026", u, p); got != want {
				t.Fatalf("at %v%%: Enabled(%s) = %v, InRollout = %v", p, u, got, want)
			}
		}
	}
}

func TestSaltDefaultsToKey(t *testing.T) {
	byKey := CompileFlag(&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 50})
	bySalt := CompileFlag(&cpv1.Flag{Key: "renamed", Salt: "new-cart", Enabled: true, RolloutPercent: 50})
	other := CompileFlag(&cpv1.Flag{Key: "new-cart", Salt: "other", Enabled: true, RolloutPercent: 50})

	variants := []*cpv1.Variant{{Name: "a", Weight: 1}, {Name: "b", Weight: 1}}
	expByKey := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: variants})
	expBySalt := CompileExperiment(&cpv1.Experiment{Key: "renamed", Salt: "cta-color", Enabled: true, Variants: variants})

	differ := 0
	for i := range 1000 {
		u := unit(i)
		if byKey.Enabled(u) != bySalt.Enabled(u) {
			t.Fatalf("flag: empty salt and salt == key disagree for %s", u)
		}
		if byKey.Enabled(u) != other.Enabled(u) {
			differ++
		}
		a, _ := expByKey.Assign(u)
		b, _ := expBySalt.Assign(u)
		if a.GetName() != b.GetName() {
			t.Fatalf("experiment: empty salt and salt == key disagree for %s", u)
		}
	}
	if differ == 0 {
		t.Fatal("a different salt picked exactly the same units")
	}
}

func TestCompiledFlagIgnoresLaterEdits(t *testing.T) {
	pf := &cpv1.Flag{Key: "f", Enabled: true, Allowlist: []string{"vip"}}
	f := CompileFlag(pf)
	pf.Enabled, pf.RolloutPercent, pf.Allowlist[0] = false, 100, "someone-else"
	if !f.Enabled("vip") || f.Enabled("someone-else") || f.Enabled("user-1") {
		t.Fatal("compiled flag changed when its proto was edited")
	}
}

func TestNilAndZeroValues(t *testing.T) {
	for _, f := range []*Flag{nil, new(Flag)} {
		if f.Enabled("user-1") || f.Enabled("") {
			t.Errorf("Flag %v is on", f)
		}
	}
	for _, e := range []*Experiment{nil, new(Experiment)} {
		if v, ok := e.Assign("user-1"); ok || v != nil {
			t.Errorf("Experiment %v assigned %v", e, v)
		}
	}
}

func TestExperimentEnrollment(t *testing.T) {
	variants := func() []*cpv1.Variant {
		return []*cpv1.Variant{{Name: "control", Weight: 1}, {Name: "treatment", Weight: 1}}
	}
	tests := []struct {
		name string
		exp  *cpv1.Experiment
		unit string
		want bool
	}{
		{"nil experiment", nil, "user-1", false},
		{"disabled", &cpv1.Experiment{Key: "e", Variants: variants()}, "user-1", false},
		{"empty unit", &cpv1.Experiment{Key: "e", Enabled: true, Variants: variants()}, "", false},
		{"no variants", &cpv1.Experiment{Key: "e", Enabled: true}, "user-1", false},
		{"zero weights", &cpv1.Experiment{Key: "e", Enabled: true, Variants: []*cpv1.Variant{{Name: "a"}, {Name: "b"}}}, "user-1", false},
		{"nil variant entries", &cpv1.Experiment{Key: "e", Enabled: true, Variants: []*cpv1.Variant{nil, nil}}, "user-1", false},
		{"enrolled", &cpv1.Experiment{Key: "e", Enabled: true, Variants: variants()}, "user-1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := CompileExperiment(tt.exp).Assign(tt.unit)
			if ok != tt.want || (v != nil) != tt.want {
				t.Errorf("Assign(%q) = %v, %v; want enrolled %v", tt.unit, v, ok, tt.want)
			}
			v, ok = Variant(tt.exp, tt.unit)
			if ok != tt.want || (v != nil) != tt.want {
				t.Errorf("Variant(%q) = %v, %v; want enrolled %v", tt.unit, v, ok, tt.want)
			}
		})
	}
}

// TestAssignMatchesChoose checks that Assign returns the experiment's own
// variant message at the index bucketing.Choose picks, payload included.
func TestAssignMatchesChoose(t *testing.T) {
	payload := structpb.NewStringValue("green")
	pe := &cpv1.Experiment{Key: "cta-color", Salt: "spring-2026", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 5}, {Name: "off", Weight: 0}, {Name: "green", Weight: 3, Payload: payload}, {Name: "blue", Weight: 2},
	}}
	weights := []uint32{5, 0, 3, 2}
	e := CompileExperiment(pe)
	for i := range 2000 {
		u := unit(i)
		v, ok := e.Assign(u)
		if want := pe.Variants[bucketing.Choose("spring-2026", u, weights)]; !ok || v != want {
			t.Fatalf("Assign(%s) = %v, %v; want %v", u, v, ok, want)
		}
		if v.GetName() == "green" && v.GetPayload() != payload {
			t.Fatalf("Assign(%s) lost the variant payload", u)
		}
	}
}

// TestAssignmentIsStable checks that independently built copies of one
// experiment, as two SDK instances would hold, agree on every unit.
func TestAssignmentIsStable(t *testing.T) {
	build := func() *cpv1.Experiment {
		return &cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
			{Name: "control", Weight: 50}, {Name: "green", Weight: 30}, {Name: "blue", Weight: 20},
		}}
	}
	a, b, oneShot := CompileExperiment(build()), CompileExperiment(build()), build()
	for i := range 2000 {
		u := unit(i)
		va, _ := a.Assign(u)
		vb, _ := b.Assign(u)
		vc, _ := Variant(oneShot, u)
		if va.GetName() != vb.GetName() || va.GetName() != vc.GetName() {
			t.Fatalf("%s got %q, %q and %q from three copies", u, va.GetName(), vb.GetName(), vc.GetName())
		}
	}
}

func TestAssignmentProportions(t *testing.T) {
	e := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 50}, {Name: "off", Weight: 0}, {Name: "green", Weight: 30}, {Name: "blue", Weight: 20},
	}})
	const units = 50_000
	counts := make(map[string]int)
	for i := range units {
		v, ok := e.Assign(unit(i))
		if !ok {
			t.Fatalf("%s not enrolled", unit(i))
		}
		counts[v.GetName()]++
	}
	for name, share := range map[string]float64{"control": 0.5, "off": 0, "green": 0.3, "blue": 0.2} {
		if got := float64(counts[name]) / units; math.Abs(got-share) > 0.01 || (share == 0 && counts[name] != 0) {
			t.Errorf("variant %s got %.4f of units, want %.2f", name, got, share)
		}
	}
}

func TestCompiledExperimentIgnoresLaterEdits(t *testing.T) {
	pe := &cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 1}, {Name: "green", Weight: 1},
	}}
	e := CompileExperiment(pe)
	before := make([]string, 500)
	for i := range before {
		v, _ := e.Assign(unit(i))
		before[i] = v.GetName()
	}
	pe.Enabled = false
	pe.Variants[0].Weight = 0
	pe.Variants[0], pe.Variants[1] = pe.Variants[1], pe.Variants[0]
	for i, want := range before {
		if v, ok := e.Assign(unit(i)); !ok || v.GetName() != want {
			t.Fatalf("%s moved from %s to %s after the proto was edited", unit(i), want, v.GetName())
		}
	}
}

func TestCompiledEvaluationDoesNotAllocate(t *testing.T) {
	f := CompileFlag(&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 25, Allowlist: []string{"vip"}})
	e := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 50}, {Name: "green", Weight: 50},
	}})
	const u = "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31"
	if n := testing.AllocsPerRun(100, func() { f.Enabled(u) }); n != 0 {
		t.Errorf("Flag.Enabled allocates %v times per call", n)
	}
	if n := testing.AllocsPerRun(100, func() { e.Assign(u) }); n != 0 {
		t.Errorf("Experiment.Assign allocates %v times per call", n)
	}
}

// TestConcurrentEvaluation shares compiled values between goroutines, as the
// SDK does; it is meaningful under -race.
func TestConcurrentEvaluation(t *testing.T) {
	f := CompileFlag(&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 50, Allowlist: []string{"user-3"}})
	e := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 1}, {Name: "green", Weight: 1},
	}})
	const units = 500
	wantFlag := make([]bool, units)
	wantVariant := make([]string, units)
	for i := range units {
		wantFlag[i] = f.Enabled(unit(i))
		v, _ := e.Assign(unit(i))
		wantVariant[i] = v.GetName()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range units {
				v, _ := e.Assign(unit(i))
				if f.Enabled(unit(i)) != wantFlag[i] || v.GetName() != wantVariant[i] {
					t.Errorf("%s evaluated differently under concurrency", unit(i))
					return
				}
			}
		})
	}
	wg.Wait()
}

func BenchmarkFlagEnabled(b *testing.B) {
	allowlist := make([]string, 100)
	for i := range allowlist {
		allowlist[i] = "vip-" + strconv.Itoa(i)
	}
	partial := CompileFlag(&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 25, Allowlist: allowlist})
	full := CompileFlag(&cpv1.Flag{Key: "new-cart", Enabled: true, RolloutPercent: 100})
	const u = "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31"
	b.Run("rollout", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			partial.Enabled(u)
		}
	})
	b.Run("allowlisted", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			partial.Enabled("vip-42")
		}
	})
	b.Run("100%", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			full.Enabled(u)
		}
	})
}

func BenchmarkExperimentAssign(b *testing.B) {
	e := CompileExperiment(&cpv1.Experiment{Key: "cta-color", Enabled: true, Variants: []*cpv1.Variant{
		{Name: "control", Weight: 50}, {Name: "green", Weight: 30}, {Name: "blue", Weight: 20},
	}})
	b.ReportAllocs()
	for b.Loop() {
		e.Assign("7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31")
	}
}
