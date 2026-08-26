package main

import (
	"encoding/json"
	"slices"
	"strconv"
	"testing"

	"google.golang.org/grpc/codes"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/eval"
)

func TestEval(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "76.02", "--allow", "vip")
	s.cpctl(t, "flag", "put", "shop", "dark-mode", "--enabled", "--rollout", "50")
	s.cpctl(t, "flag", "put", "shop", "killed", "--rollout", "100", "--allow", "vip")
	s.cpctl(t, "experiment", "put", "shop", "cta", "--enabled", "--variants", "blue=50,green=50",
		"--payload", `green={"weight":2,"color":"green"}`)
	s.cpctl(t, "experiment", "put", "shop", "paused", "--variants", "a=1")

	// pkg/eval's golden answers: user-42 is in bucket 7601 of new-cart, so it
	// is on at 76.02%; a disabled flag is off even for allowlisted units.
	out := s.cpctl(t, "eval", "shop", "user-42")
	wantLines(t, out, `shop at revision 6, unit "user-42"`, "TYPE KEY RESULT PAYLOAD",
		"flag new-cart on", "flag killed off", "experiment paused not enrolled")
	wantLines(t, s.cpctl(t, "eval", "shop", "vip"), "flag new-cart on", "flag killed off")
	wantLines(t, s.cpctl(t, "eval", "shop", ""), `shop at revision 6, unit ""`, "flag new-cart off", "experiment cta not enrolled")

	// Every answer is the one the SDK computes from the same snapshot.
	snap := decode(t, s.cpctl(t, "snapshot", "shop"), &cpv1.Snapshot{})
	seen := make(map[string]bool)
	for i := range 40 {
		unit := "user-" + strconv.Itoa(i)
		var want []string
		for _, f := range snap.GetFlags() {
			result := "off"
			if eval.FlagEnabled(f, unit) {
				result = "on"
			}
			want = append(want, "flag "+f.GetKey()+" "+result)
		}
		for _, e := range snap.GetExperiments() {
			v, ok := eval.Variant(e, unit)
			switch {
			case !ok:
				want = append(want, "experiment "+e.GetKey()+" not enrolled")
			case v.GetPayload() != nil:
				payload, err := json.Marshal(v.GetPayload().AsInterface())
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, "experiment "+e.GetKey()+" "+v.GetName()+" "+string(payload))
			default:
				want = append(want, "experiment "+e.GetKey()+" "+v.GetName())
			}
			seen[v.GetName()] = true
		}
		if got := lines(s.cpctl(t, "eval", "shop", unit))[2:]; !slices.Equal(got, want) {
			t.Fatalf("eval for %s:\n%q\nwant\n%q", unit, got, want)
		}
	}
	// Both arms came up, so the payload column was checked too.
	if !seen["blue"] || !seen["green"] {
		t.Fatalf("40 units did not reach both variants: %v", seen)
	}

	s.fail(t, codes.NotFound, "eval", "nope", "user-1")
	s.fail(t, codes.Unknown, "eval", "shop")
}
