package client

import (
	"fmt"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/eval"
)

func TestFlagsAndVariantsAgreeAcrossClients(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEnv(t)
		rollout := flag("new-cart", true, 30, "vip-1")
		experiment := &cpv1.Experiment{Key: "cta-color", Enabled: true, Salt: "cta-2026", Variants: []*cpv1.Variant{
			{Name: "blue", Weight: 1},
			{Name: "green", Weight: 2, Payload: structpb.NewStringValue("#0a0")},
			{Name: "red", Weight: 0},
		}}
		s := snap(1, rollout, experiment)
		// Each client gets its own copy, as it would from the server.
		a, _ := e.connect(e.options(), proto.Clone(s).(*cpv1.Snapshot))
		b, _ := e.connect(e.options(), proto.Clone(s).(*cpv1.Snapshot))

		on, enrolled := 0, map[string]int{}
		const units = 2000
		for i := range units {
			u := fmt.Sprintf("user-%d", i)
			got := a.IsEnabled("new-cart", u)
			if got != b.IsEnabled("new-cart", u) || got != eval.FlagEnabled(rollout, u) {
				t.Fatalf("new-cart for %s: clients %v and %v, eval %v", u, got, b.IsEnabled("new-cart", u), eval.FlagEnabled(rollout, u))
			}
			if got {
				on++
			}

			va, okA := a.Variant("cta-color", u)
			vb, okB := b.Variant("cta-color", u)
			ve, okE := eval.Variant(experiment, u)
			if !okA || !okB || !okE || va.Variant != vb.Variant || va.Variant != ve.GetName() {
				t.Fatalf("cta-color for %s: clients %+v/%v and %+v/%v, eval %v/%v", u, va, okA, vb, okB, ve.GetName(), okE)
			}
			if va.Experiment != "cta-color" || !proto.Equal(va.Payload, ve.GetPayload()) {
				t.Fatalf("assignment %+v, want experiment cta-color with payload %v", va, ve.GetPayload())
			}
			enrolled[va.Variant]++
		}
		// Loose bounds: the exact split is bucketing's business, this only
		// catches a client evaluating something else entirely.
		if on < units*25/100 || on > units*35/100 {
			t.Errorf("new-cart on for %d of %d units at 30%%", on, units)
		}
		if enrolled["red"] != 0 || enrolled["green"] < enrolled["blue"] {
			t.Errorf("variant counts %v, want no red and green about twice blue", enrolled)
		}

		if !a.IsEnabled("new-cart", "vip-1") {
			t.Error("allowlisted unit not enabled")
		}
		if a.IsEnabled("missing-flag", "user-1") {
			t.Error("a missing flag is on")
		}
		if _, ok := a.Variant("cta-color", ""); ok {
			t.Error("empty unit enrolled")
		}
		if _, ok := a.Variant("missing-experiment", "user-1"); ok {
			t.Error("enrolled in a missing experiment")
		}
	})
}
