package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/eval"
)

// evaluate shows how every flag and experiment in a namespace evaluates for
// one unit. It evaluates the snapshot locally with package eval, the code
// the SDK runs, so the answers are exactly what a service would get.
func (c *cli) evaluate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eval", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	pos, err := parseArgs(fs, args, 2)
	if err != nil {
		return err
	}
	unit := pos[1]
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: pos[0]})
	if err != nil {
		return err
	}
	snap := resp.GetSnapshot()

	fmt.Fprintf(c.out, "%s at revision %d, unit %q\n", snap.GetNamespace(), snap.GetRevision(), unit)
	tw := newTable(c.out)
	fmt.Fprintln(tw, "TYPE\tKEY\tRESULT\tPAYLOAD")
	for _, f := range snap.GetFlags() {
		result := "off"
		if eval.FlagEnabled(f, unit) {
			result = "on"
		}
		fmt.Fprintf(tw, "flag\t%s\t%s\t\n", f.GetKey(), result)
	}
	for _, e := range snap.GetExperiments() {
		v, ok := eval.Variant(e, unit)
		if !ok {
			fmt.Fprintf(tw, "experiment\t%s\tnot enrolled\t\n", e.GetKey())
			continue
		}
		var payload []byte
		if v.GetPayload() != nil {
			// One line, keys sorted: protojson varies its whitespace.
			if payload, err = json.Marshal(v.GetPayload().AsInterface()); err != nil {
				return err
			}
		}
		fmt.Fprintf(tw, "experiment\t%s\t%s\t%s\n", e.GetKey(), v.GetName(), payload)
	}
	return tw.Flush()
}
