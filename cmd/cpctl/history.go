package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

func (c *cli) history(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := int32Flag(fs, "limit")
	before := fs.Int64("before", 0, "")
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.admin.ListRevisions(ctx, &cpv1.ListRevisionsRequest{Namespace: pos[0], PageSize: *limit, BeforeRevision: *before})
	if err != nil {
		return err
	}
	tw := newTable(c.out)
	fmt.Fprintln(tw, "REVISION\tCREATED\tACTOR\tSUMMARY")
	for _, r := range resp.GetRevisions() {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", r.GetRevision(), formatTime(r.GetCreatedAt()), clean(r.GetActor()), clean(r.GetSummary()))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if next := resp.GetNextBeforeRevision(); next != 0 {
		fmt.Fprintf(c.out, "next page: --before %d\n", next)
	}
	return nil
}

// revision prints one revision, including the full namespace state at it.
func (c *cli) revision(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("revision", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	pos, err := parseArgs(fs, args, 2)
	if err != nil {
		return err
	}
	rev, err := parseRevision(pos[1])
	if err != nil {
		return err
	}
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.admin.GetRevision(ctx, &cpv1.GetRevisionRequest{Namespace: pos[0], Revision: rev})
	if err != nil {
		return err
	}
	return c.print(resp.GetRevision())
}

func (c *cli) diff(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("diff", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "")
	pos, err := parseArgsRange(fs, args, 2, 3)
	if err != nil {
		return err
	}
	req := &cpv1.DiffRevisionsRequest{Namespace: pos[0]}
	if req.FromRevision, err = parseRevision(pos[1]); err != nil {
		return err
	}
	if len(pos) == 3 {
		if req.ToRevision, err = parseRevision(pos[2]); err != nil {
			return err
		}
	}
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.admin.DiffRevisions(ctx, req)
	if err != nil {
		return err
	}
	if *asJSON {
		return c.print(resp)
	}
	c.printChanges(resp.GetChanges())
	return nil
}

// rollback previews a rollback, or applies it with --yes. Both are pinned to
// one namespace revision, the base: the preview is the diff from the base to
// the target, and the rollback is sent with the base as its expected
// revision, so it fails rather than discard changes made after the base.
// The base is --expected-revision, as a preview prints it, or else the
// current revision.
func (c *cli) rollback(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rollback", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	yes := fs.Bool("yes", false, "")
	expected := fs.Int64("expected-revision", 0, "")
	pos, err := parseArgs(fs, args, 2)
	if err != nil {
		return err
	}
	ns := pos[0]
	target, err := parseRevision(pos[1])
	if err != nil {
		return err
	}
	ctx, cancel := c.call(ctx)
	defer cancel()

	base := *expected
	if base == 0 {
		resp, err := c.admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: ns})
		if err != nil {
			return err
		}
		base = resp.GetNamespace().GetRevision()
	}

	if *yes {
		resp, err := c.admin.Rollback(ctx, &cpv1.RollbackRequest{Namespace: ns, ToRevision: target, ExpectedRevision: base})
		if err != nil {
			return err
		}
		if len(resp.GetChanges()) == 0 {
			fmt.Fprintf(c.out, "%s already matches revision %d; nothing was changed (still at revision %d)\n", ns, target, resp.GetRevision())
			return nil
		}
		fmt.Fprintf(c.out, "rolled back %s to revision %d; now at revision %d:\n", ns, target, resp.GetRevision())
		c.printChanges(resp.GetChanges())
		return nil
	}

	diff, err := c.admin.DiffRevisions(ctx, &cpv1.DiffRevisionsRequest{Namespace: ns, FromRevision: base, ToRevision: target})
	if err != nil {
		return err
	}
	// A rollback brings back rollouts that were active at the target paused,
	// because their stage timers are stale. The diff cannot show that, and
	// such a flag changes even when it is otherwise the same as now.
	old, err := c.admin.GetRevision(ctx, &cpv1.GetRevisionRequest{Namespace: ns, Revision: target})
	if err != nil {
		return err
	}
	var paused []string
	for _, f := range old.GetRevision().GetSnapshot().GetFlags() {
		if f.GetRollout().GetState() == cpv1.RolloutState_ROLLOUT_STATE_ACTIVE {
			paused = append(paused, f.GetKey())
		}
	}
	if len(diff.GetChanges())+len(paused) == 0 {
		fmt.Fprintf(c.out, "%s at revision %d already matches revision %d; nothing to roll back\n", ns, base, target)
		return nil
	}
	fmt.Fprintf(c.out, "rolling back %s from revision %d to revision %d would apply:\n", ns, base, target)
	c.printChanges(diff.GetChanges())
	for _, key := range paused {
		fmt.Fprintf(c.out, "the rollout of flag %s, active at revision %d, comes back paused\n", key, target)
	}
	fmt.Fprintf(c.out, "nothing was changed; to apply exactly this, run again with --yes --expected-revision %d\n", base)
	return nil
}

// printChanges writes one line per change: "+ config foo" for an added entry,
// "~ flag bar" for a modified one, "- rate_limit baz" for a removed one.
func (c *cli) printChanges(changes []*cpv1.Change) {
	for _, ch := range changes {
		mark := "?"
		switch ch.GetType() {
		case cpv1.ChangeType_CHANGE_TYPE_ADDED:
			mark = "+"
		case cpv1.ChangeType_CHANGE_TYPE_MODIFIED:
			mark = "~"
		case cpv1.ChangeType_CHANGE_TYPE_REMOVED:
			mark = "-"
		}
		fmt.Fprintf(c.out, "%s %s %s\n", mark, ch.GetEntityType(), ch.GetKey())
	}
}

func (c *cli) audit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	entityType := fs.String("type", "", "")
	key := fs.String("key", "", "")
	actor := fs.String("actor", "", "")
	since := fs.String("since", "", "")
	until := fs.String("until", "", "")
	limit := int32Flag(fs, "limit")
	pageToken := fs.String("page-token", "", "")
	asJSON := fs.Bool("json", false, "")
	pos, err := parseArgsRange(fs, args, 0, 1)
	if err != nil {
		return err
	}
	req := &cpv1.ListAuditEventsRequest{
		EntityType: *entityType,
		EntityKey:  *key,
		Actor:      *actor,
		PageSize:   *limit,
		PageToken:  *pageToken,
	}
	if len(pos) == 1 {
		req.Namespace = pos[0]
	}
	now := time.Now()
	if req.Since, err = timeFlag("since", *since, now); err != nil {
		return err
	}
	if req.Until, err = timeFlag("until", *until, now); err != nil {
		return err
	}
	if req.Since != nil && req.Until != nil && req.Since.AsTime().After(req.Until.AsTime()) {
		return fmt.Errorf("audit: --since is later than --until; %w", errUsage)
	}
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.admin.ListAuditEvents(ctx, req)
	if err != nil {
		return err
	}
	if *asJSON {
		return c.print(resp)
	}
	tw := newTable(c.out)
	fmt.Fprintln(tw, "ID\tTIME\tNAMESPACE\tREVISION\tACTOR\tACTION\tENTITY\tMESSAGE")
	for _, e := range resp.GetEvents() {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\t%s\t%s %s\t%s\n", e.GetId(), formatTime(e.GetTime()), clean(e.GetNamespace()),
			e.GetRevision(), clean(e.GetActor()), e.GetAction(), clean(e.GetEntityType()), clean(e.GetEntityKey()), clean(e.GetMessage()))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if next := resp.GetNextPageToken(); next != "" {
		fmt.Fprintf(c.out, "next page: --page-token %s\n", next)
	}
	return nil
}

// parseRevision reads a revision number. History starts at revision 1, and a
// zero would mean "current state" to DiffRevisions, so it is refused here.
func parseRevision(s string) (int64, error) {
	rev, err := strconv.ParseInt(s, 10, 64)
	if err != nil || rev < 1 {
		return 0, fmt.Errorf("revision %q must be a number of at least 1", s)
	}
	return rev, nil
}

// timeFlag reads the value of --since or --until: an RFC 3339 time or a Go
// duration meaning that long before now. Empty means unset.
func timeFlag(name, value string, now time.Time) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		ts := timestamppb.New(t)
		if err := ts.CheckValid(); err != nil {
			return nil, fmt.Errorf("--%s: %q: %w", name, value, err)
		}
		return ts, nil
	}
	if d, err := time.ParseDuration(value); err == nil {
		if d < 0 {
			return nil, fmt.Errorf("--%s: %q is a time in the future; give the duration ago without a minus sign", name, value)
		}
		return timestamppb.New(now.Add(-d)), nil
	}
	return nil, fmt.Errorf("--%s: %q is neither an RFC 3339 time (2006-01-02T15:04:05Z) nor a duration ago (24h)", name, value)
}
