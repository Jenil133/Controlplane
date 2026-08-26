package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// rollout runs the staged rollout commands. Every one of them prints the
// flag's rollout as "rollout status" shows it.
func (c *cli) rollout(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("rollout: missing subcommand (start, advance, pause, resume, abort, status); %w", errUsage)
	}
	switch args[0] {
	case "start", "advance", "pause", "resume", "abort", "status":
	default:
		return fmt.Errorf("rollout: unknown subcommand %q; %w", args[0], errUsage)
	}
	fs := flag.NewFlagSet("rollout "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var stagesSpec *string
	if args[0] == "start" {
		stagesSpec = fs.String("stages", "", "")
	}
	pos, err := parseArgs(fs, args[1:], 2)
	if err != nil {
		return err
	}
	ns, key := pos[0], pos[1]
	ctx, cancel := c.call(ctx)
	defer cancel()

	var f *cpv1.Flag
	switch args[0] {
	case "start":
		stages, err := parseStages(*stagesSpec)
		if err != nil {
			return err
		}
		resp, err := c.admin.StartRollout(ctx, &cpv1.StartRolloutRequest{Namespace: ns, Flag: key, Stages: stages})
		if err != nil {
			return err
		}
		f = resp.GetFlag()
	case "advance":
		resp, err := c.admin.AdvanceRollout(ctx, &cpv1.AdvanceRolloutRequest{Namespace: ns, Flag: key})
		if err != nil {
			return err
		}
		f = resp.GetFlag()
	case "pause":
		resp, err := c.admin.PauseRollout(ctx, &cpv1.PauseRolloutRequest{Namespace: ns, Flag: key})
		if err != nil {
			return err
		}
		f = resp.GetFlag()
	case "resume":
		resp, err := c.admin.ResumeRollout(ctx, &cpv1.ResumeRolloutRequest{Namespace: ns, Flag: key})
		if err != nil {
			return err
		}
		f = resp.GetFlag()
	case "abort":
		resp, err := c.admin.AbortRollout(ctx, &cpv1.AbortRolloutRequest{Namespace: ns, Flag: key})
		if err != nil {
			return err
		}
		f = resp.GetFlag()
	case "status":
		// There is no single-flag read; the snapshot is what clients see.
		resp, err := c.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: ns})
		if err != nil {
			return err
		}
		flags := resp.GetSnapshot().GetFlags()
		i := slices.IndexFunc(flags, func(fl *cpv1.Flag) bool { return fl.GetKey() == key })
		if i < 0 {
			return fmt.Errorf("flag %q not found in namespace %s", key, ns)
		}
		f = flags[i]
	}
	return c.printRollout(ns, f)
}

// printRollout describes a flag's rollout plan, one property per line.
func (c *cli) printRollout(namespace string, f *cpv1.Flag) error {
	tw := newTable(c.out)
	fmt.Fprintf(tw, "flag\t%s in %s (revision %d)\n", f.GetKey(), namespace, f.GetRevision())
	p := f.GetRollout()
	if p == nil {
		fmt.Fprintf(tw, "state\tno rollout\n")
	} else {
		state := strings.ToLower(strings.TrimPrefix(p.GetState().String(), "ROLLOUT_STATE_"))
		fmt.Fprintf(tw, "state\t%s, stage %d/%d\n", state, p.GetCurrentStage()+1, len(p.GetStages()))
	}
	fmt.Fprintf(tw, "rollout percent\t%s%%\n", formatPercent(f.GetRolloutPercent()))
	if p != nil {
		fmt.Fprintf(tw, "stages\t%s\n", formatStages(p.GetStages()))
		fmt.Fprintf(tw, "started\t%s by %s\n", formatTime(p.GetStartedAt()), clean(p.GetStartedBy()))
		if next := nextStage(p); next != "" {
			fmt.Fprintf(tw, "next stage\t%s\n", next)
		}
	}
	return tw.Flush()
}

// nextStage says when an unfinished plan moves on, or returns "" when it
// will not.
func nextStage(p *cpv1.RolloutPlan) string {
	stages, i := p.GetStages(), int(p.GetCurrentStage())
	if i < 0 || i+1 >= len(stages) {
		return ""
	}
	next := formatPercent(stages[i+1].GetPercent()) + "%"
	switch p.GetState() {
	case cpv1.RolloutState_ROLLOUT_STATE_ACTIVE:
		wait := stages[i].GetDuration().AsDuration()
		if wait <= 0 {
			return next + " when advanced manually"
		}
		return next + " due " + p.GetStageStartedAt().AsTime().Add(wait).Local().Format(time.DateTime)
	case cpv1.RolloutState_ROLLOUT_STATE_PAUSED:
		// Resuming restarts the stage timer, so no time can be given yet.
		return next + " after resume or manual advance"
	default:
		return ""
	}
}

// parseStages reads rollout stages written as "1:10m,5:10m,25:30m,100":
// percent[:duration] with a Go duration, where a stage without a duration
// only advances manually. Like the admin UI it ignores blank stages and
// accepts "25%" for 25.
func parseStages(spec string) ([]*cpv1.RolloutStage, error) {
	var stages []*cpv1.RolloutStage
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		percentText, durationText, timed := strings.Cut(part, ":")
		percent, err := parsePercent(percentText)
		if err != nil {
			return nil, fmt.Errorf("--stages: %q is not percent or percent:duration, e.g. 5:10m", part)
		}
		st := &cpv1.RolloutStage{Percent: percent}
		if timed {
			d, err := time.ParseDuration(strings.TrimSpace(durationText))
			if err != nil {
				return nil, fmt.Errorf("--stages: stage %q: %w", part, err)
			}
			st.Duration = durationpb.New(d)
		}
		stages = append(stages, st)
	}
	if len(stages) == 0 {
		return nil, errors.New("--stages is required, e.g. --stages 1:10m,5:10m,25:30m,100")
	}
	return stages, nil
}

// formatStages writes stages in the syntax parseStages reads.
func formatStages(stages []*cpv1.RolloutStage) string {
	parts := make([]string, len(stages))
	for i, st := range stages {
		parts[i] = formatPercent(st.GetPercent())
		if d := st.GetDuration().AsDuration(); d != 0 {
			parts[i] += ":" + formatDuration(d)
		}
	}
	return strings.Join(parts, ",")
}

// formatDuration prints d in Go syntax without zero trailing units, e.g. "10m"
// rather than "10m0s" and "1h" rather than "1h0m0s".
func formatDuration(d time.Duration) string {
	s := d.String()
	if t, ok := strings.CutSuffix(s, "m0s"); ok {
		s = t + "m"
	}
	if t, ok := strings.CutSuffix(s, "h0m"); ok {
		s = t + "h"
	}
	return s
}
