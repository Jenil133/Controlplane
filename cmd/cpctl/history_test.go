package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

func TestParseRevision(t *testing.T) {
	if rev, err := parseRevision("42"); err != nil || rev != 42 {
		t.Fatalf("parseRevision(42) = %d, %v", rev, err)
	}
	for _, bad := range []string{"0", "-1", "", "three", "1.5"} {
		if _, err := parseRevision(bad); err == nil {
			t.Errorf("parseRevision(%q) succeeded", bad)
		}
	}
}

func TestTimeFlag(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Time{
		"2026-10-01T08:30:00Z":        time.Date(2026, 10, 1, 8, 30, 0, 0, time.UTC),
		"2026-10-01T10:30:00.5+02:00": time.Date(2026, 10, 1, 8, 30, 0, 5e8, time.UTC),
		"24h":                         now.Add(-24 * time.Hour),
		"90m":                         now.Add(-90 * time.Minute),
	} {
		ts, err := timeFlag("since", value, now)
		if err != nil || !ts.AsTime().Equal(want) {
			t.Errorf("timeFlag(%q) = %v, %v; want %v", value, ts.AsTime(), err, want)
		}
	}
	if ts, err := timeFlag("since", "", now); ts != nil || err != nil {
		t.Errorf("timeFlag(\"\") = %v, %v; want unset", ts, err)
	}
	for _, bad := range []string{"yesterday", "2026-10-01", "10"} {
		if _, err := timeFlag("until", bad, now); err == nil || !strings.Contains(err.Error(), "--until") {
			t.Errorf("timeFlag(%q) error = %v", bad, err)
		}
	}
}

func TestHistoryDiffRollback(t *testing.T) {
	s := startServer(t, nil)
	cpctl := func(args ...string) string {
		t.Helper()
		return s.cpctl(t, append([]string{"--actor", "alice"}, args...)...)
	}
	cpctl("ns", "create", "shop")                                               // 1
	cpctl("config", "put", "shop", "retry", "3")                                // 2
	cpctl("flag", "put", "shop", "dark", "--enabled")                           // 3
	cpctl("ratelimit", "put", "shop", "checkout", "--rps", "5", "--burst", "5") // 4
	cpctl("config", "put", "shop", "retry", "5")                                // 5
	cpctl("flag", "delete", "shop", "dark")                                     // 6

	out := cpctl("history", "shop")
	if ls := lines(out); len(ls) != 7 || ls[0] != "REVISION CREATED ACTOR SUMMARY" {
		t.Fatalf("history:\n%s", out)
	}
	wantMatch(t, out, `6 `+timeRE+` alice delete flag dark`)
	wantMatch(t, out, `4 `+timeRE+` alice create rate_limit checkout`)
	wantMatch(t, out, `1 `+timeRE+` alice create namespace`)

	// Pages follow the hint each page prints.
	for _, page := range []struct {
		args []string
		revs []string
		next string
	}{
		{[]string{"--limit", "2"}, []string{"6", "5"}, "next page: --before 5"},
		{[]string{"--limit", "2", "--before", "5"}, []string{"4", "3"}, "next page: --before 3"},
		{[]string{"--limit", "2", "--before", "3"}, []string{"2", "1"}, ""},
	} {
		ls := lines(cpctl(append([]string{"history", "shop"}, page.args...)...))
		var revs []string
		for _, l := range ls[1:] {
			if !strings.HasPrefix(l, "next page:") {
				revs = append(revs, strings.Fields(l)[0])
			}
		}
		if !slices.Equal(revs, page.revs) || (page.next != "") != (ls[len(ls)-1] == page.next) {
			t.Fatalf("history %v:\n%s", page.args, strings.Join(ls, "\n"))
		}
	}

	rev := decode(t, cpctl("revision", "shop", "3"), &cpv1.Revision{})
	if rev.GetRevision() != 3 || rev.GetSummary() != "create flag dark" || rev.GetActor() != "alice" ||
		len(rev.GetSnapshot().GetFlags()) != 1 || rev.GetSnapshot().GetConfigs()[0].GetValue().GetNumberValue() != 3 {
		t.Fatalf("revision 3 = %v", rev)
	}

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"3"}, "~ config retry\n- flag dark\n+ rate_limit checkout\n"},
		{[]string{"3", "5"}, "~ config retry\n+ rate_limit checkout\n"},
		{[]string{"5", "3"}, "~ config retry\n- rate_limit checkout\n"},
		{[]string{"6", "6"}, ""},
	} {
		if got := cpctl(append([]string{"diff", "shop"}, tc.args...)...); got != tc.want {
			t.Errorf("diff %v = %q, want %q", tc.args, got, tc.want)
		}
	}
	d := decode(t, cpctl("diff", "shop", "2", "3", "--json"), &cpv1.DiffRevisionsResponse{})
	if ch := d.GetChanges(); len(ch) != 1 || ch[0].GetType() != cpv1.ChangeType_CHANGE_TYPE_ADDED || ch[0].GetBefore() != nil ||
		ch[0].GetAfter().GetStructValue().GetFields()["key"].GetStringValue() != "dark" {
		t.Fatalf("diff --json = %v", d)
	}

	// The preview diffs the current revision against the target and
	// changes nothing.
	want := "rolling back shop from revision 6 to revision 3 would apply:\n" +
		"~ config retry\n+ flag dark\n- rate_limit checkout\n" +
		"nothing was changed; to apply exactly this, run again with --yes --expected-revision 6\n"
	if got := cpctl("rollback", "shop", "3"); got != want {
		t.Fatalf("rollback preview = %q, want %q", got, want)
	}
	if ns := decode(t, cpctl("ns", "get", "shop"), &cpv1.Namespace{}); ns.GetRevision() != 6 {
		t.Fatalf("preview changed the namespace: %v", ns)
	}

	// A write after the preview makes applying it fail rather than
	// silently undo that write.
	cpctl("config", "put", "shop", "timeout", "30") // 7
	s.fail(t, codes.Aborted, "rollback", "shop", "3", "--yes", "--expected-revision", "6")
	if ns := decode(t, cpctl("ns", "get", "shop"), &cpv1.Namespace{}); ns.GetRevision() != 7 {
		t.Fatalf("failed rollback changed the namespace: %v", ns)
	}

	// --yes alone pins the revision it reads.
	want = "rolled back shop to revision 3; now at revision 8:\n" +
		"~ config retry\n- config timeout\n+ flag dark\n- rate_limit checkout\n"
	if got := cpctl("rollback", "shop", "3", "--yes"); got != want {
		t.Fatalf("rollback --yes = %q, want %q", got, want)
	}
	if got := cpctl("diff", "shop", "3"); got != "" {
		t.Fatalf("diff against the restored revision: %q", got)
	}
	wantMatch(t, cpctl("history", "shop", "--limit", "1"), `8 `+timeRE+` alice rollback to revision 3`)

	if got := cpctl("rollback", "shop", "3"); got != "shop at revision 8 already matches revision 3; nothing to roll back\n" {
		t.Fatalf("no-op preview = %q", got)
	}
	if got := cpctl("rollback", "shop", "3", "--yes"); got != "shop already matches revision 3; nothing was changed (still at revision 8)\n" {
		t.Fatalf("no-op rollback = %q", got)
	}

	for _, tc := range []struct {
		code codes.Code
		args []string
	}{
		{codes.Unknown, []string{"rollback", "shop", "0"}},
		{codes.Unknown, []string{"rollback", "shop", "three"}},
		{codes.Unknown, []string{"rollback", "shop"}},
		{codes.NotFound, []string{"rollback", "shop", "99"}},
		{codes.NotFound, []string{"rollback", "shop", "99", "--yes"}},
		{codes.NotFound, []string{"rollback", "nope", "1"}},
		{codes.NotFound, []string{"revision", "shop", "99"}},
		{codes.Unknown, []string{"revision", "shop"}},
		{codes.NotFound, []string{"diff", "shop", "99"}},
		{codes.Unknown, []string{"diff", "shop"}},
		{codes.Unknown, []string{"diff", "shop", "1", "2", "3"}},
		{codes.NotFound, []string{"history", "nope"}},
		{codes.InvalidArgument, []string{"history", "shop", "--limit", "-1"}},
		{codes.Unknown, []string{"history", "shop", "--limit", "many"}},
	} {
		s.fail(t, tc.code, tc.args...)
	}
}

// TestRollbackPreviewPausedRollouts checks that the preview tells what the
// diff alone cannot: a rollout active at the target comes back paused.
func TestRollbackPreviewPausedRollouts(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")                                           // 1
	s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "0") // 2
	s.cpctl(t, "rollout", "start", "shop", "new-cart", "--stages", "1:1h,100")   // 3
	s.cpctl(t, "config", "put", "shop", "payments.latency_ms", "5000")           // 4

	want := "rolling back shop from revision 4 to revision 3 would apply:\n" +
		"- config payments.latency_ms\n" +
		"the rollout of flag new-cart, active at revision 3, comes back paused\n" +
		"nothing was changed; to apply exactly this, run again with --yes --expected-revision 4\n"
	if got := s.cpctl(t, "rollback", "shop", "3"); got != want {
		t.Fatalf("preview = %q, want %q", got, want)
	}
	// Even a namespace that matches the target is not left alone.
	if got := s.cpctl(t, "rollback", "shop", "4"); !strings.Contains(got, "the rollout of flag new-cart, active at revision 4, comes back paused\n") {
		t.Fatalf("preview of a rollback to the current revision = %q", got)
	}

	want = "rolled back shop to revision 3; now at revision 5:\n- config payments.latency_ms\n~ flag new-cart\n"
	if got := s.cpctl(t, "rollback", "shop", "3", "--yes", "--expected-revision", "4"); got != want {
		t.Fatalf("rollback = %q, want %q", got, want)
	}
	wantLines(t, s.cpctl(t, "rollout", "status", "shop", "new-cart"), "state paused, stage 1/2", "rollout percent 1%")
	if got := s.cpctl(t, "rollback", "shop", "3", "--yes"); got != "shop already matches revision 3; nothing was changed (still at revision 5)\n" {
		t.Fatalf("repeated rollback = %q", got)
	}
}

func TestAudit(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "--actor", "alice", "ns", "create", "shop")
	s.cpctl(t, "--actor", "bob", "flag", "put", "shop", "dark", "--enabled")
	s.cpctl(t, "--actor", "alice", "config", "put", "shop", "retry", "3")
	s.cpctl(t, "--actor", "bob", "flag", "put", "shop", "dark")
	s.cpctl(t, "--actor", "carol", "ns", "create", "other")

	// rows returns the table rows of audit output, newest first, as
	// "<namespace> <revision> <actor> <action> <entity> <key>" (dropping the
	// ID and time) plus the next-page hint, if any.
	rows := func(args ...string) (rows []string, next string) {
		t.Helper()
		ls := lines(s.cpctl(t, append([]string{"audit"}, args...)...))
		if ls[0] != "ID TIME NAMESPACE REVISION ACTOR ACTION ENTITY MESSAGE" {
			t.Fatalf("audit %v header: %q", args, ls[0])
		}
		for _, l := range ls[1:] {
			if hint, ok := strings.CutPrefix(l, "next page: "); ok {
				next = hint
				continue
			}
			if l != "" {
				rows = append(rows, strings.Join(strings.Fields(l)[3:], " "))
			}
		}
		return rows, next
	}
	check := func(want []string, args ...string) {
		t.Helper()
		if got, next := rows(args...); !slices.Equal(got, want) || next != "" {
			t.Fatalf("audit %v = %q (next %q), want %q", args, got, next, want)
		}
	}

	shop := []string{
		"shop 4 bob update flag dark",
		"shop 3 alice create config retry",
		"shop 2 bob create flag dark",
		"shop 1 alice create namespace shop",
	}
	check(shop, "shop")
	check([]string{"other 1 carol create namespace other"}, "--actor", "carol")
	check([]string{"shop 4 bob update flag dark", "shop 2 bob create flag dark"}, "shop", "--type", "flag", "--actor", "bob")
	check([]string{"shop 3 alice create config retry"}, "--type", "config", "--key", "retry")
	check(nil, "--until", "1h")
	check(nil, "--since", "2999-01-01T00:00:00Z")
	if got, _ := rows("--since", "1h", "--until", "2999-01-01T00:00:00Z"); len(got) != 5 {
		t.Fatalf("audit within the last hour: %q", got)
	}

	// Paging follows the hint each full page prints.
	first, next := rows("shop", "--limit", "3")
	token, ok := strings.CutPrefix(next, "--page-token ")
	if !slices.Equal(first, shop[:3]) || !ok {
		t.Fatalf("first page: %q, next %q", first, next)
	}
	check(shop[3:], "shop", "--limit", "3", "--page-token", token)

	// Every filter at once, starting after the newest flag event.
	_, next = rows("shop", "--type", "flag", "--limit", "1")
	token, _ = strings.CutPrefix(next, "--page-token ")
	check([]string{"shop 2 bob create flag dark"}, "shop", "--type", "flag", "--key", "dark", "--actor", "bob",
		"--since", "1h", "--until", "2999-01-01T00:00:00Z", "--page-token", token)

	// --json has the before and after images the table leaves out.
	resp := decode(t, s.cpctl(t, "audit", "shop", "--type", "flag", "--json"), &cpv1.ListAuditEventsResponse{})
	if ev := resp.GetEvents(); len(ev) != 2 || ev[0].GetAction() != "update" ||
		!ev[0].GetBefore().GetStructValue().GetFields()["enabled"].GetBoolValue() ||
		ev[0].GetAfter().GetStructValue().GetFields()["enabled"].GetBoolValue() || ev[1].GetBefore() != nil {
		t.Fatalf("audit --json = %v", resp)
	}

	s.fail(t, codes.Unknown, "audit", "--since", "yesterday")
	s.fail(t, codes.InvalidArgument, "audit", "--type", "widget")
	s.fail(t, codes.InvalidArgument, "audit", "--page-token", "abc")
	s.fail(t, codes.InvalidArgument, "audit", "--limit", "-5")
	s.fail(t, codes.Unknown, "audit", "shop", "other")
}
