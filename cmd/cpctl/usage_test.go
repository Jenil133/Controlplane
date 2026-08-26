package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// TestCommandHelp checks that --help and "help" describe the command asked
// about, succeed, and print the same text wherever they are given.
func TestCommandHelp(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want []string // lines that must be in the output
		not  string   // text that must not be
	}{
		{[]string{"help", "flag"}, []string{"  flag put <namespace> <key> [--enabled] [--rollout percent] [--salt s] [--allow u1,u2]...", "  flag delete <namespace> <key>"}, "config put"},
		{[]string{"flag", "put", "--help"}, []string{"  flag delete <namespace> <key>"}, "experiment"},
		{[]string{"flag", "-h"}, []string{"  flag delete <namespace> <key>"}, "ns create"},
		{[]string{"flag", "help"}, []string{"  flag delete <namespace> <key>"}, "ns create"},
		{[]string{"rollout", "start", "x", "y", "--help"}, []string{"  rollout status <namespace> <flag>", "  rollout start <namespace> <flag> --stages 1:10m,5:10m,25:30m,100"}, "history"},
		{[]string{"namespace", "--help"}, []string{"  ns list"}, "flag put"},
		{[]string{"exp", "--help"}, []string{"  experiment delete <namespace> <key>"}, "flag put"},
		{[]string{"history", "--help"}, []string{"  history <namespace> [--limit n] [--before revision]"}, "revision <"},
		{[]string{"audit", "-h"}, []string{"        [--limit n] [--page-token t] [--json]"}, "history"},
		{[]string{"token", "generate", "--help"}, []string{"  token generate --name name --role reader|editor|admin"}, "audit"},
		{[]string{"--addr", "127.0.0.1:1", "eval", "--help"}, []string{"  eval <namespace> <unit>"}, "watch"},
	} {
		var out bytes.Buffer
		if err := run(context.Background(), tc.args, &out); err != nil {
			t.Errorf("cpctl %v: %v", tc.args, err)
			continue
		}
		got := lines0(out.String())
		for _, w := range tc.want {
			if !containsString(got, w) {
				t.Errorf("cpctl %v lacks the line %q:\n%s", tc.args, w, out.String())
			}
		}
		if strings.Contains(out.String(), tc.not) {
			t.Errorf("cpctl %v mentions %q:\n%s", tc.args, tc.not, out.String())
		}
	}
	if err := run(context.Background(), []string{"help", "frobnicate"}, &bytes.Buffer{}); !errors.Is(err, errUsage) {
		t.Errorf("help for an unknown command: %v", err)
	}
	// Every command the usage text describes has help of its own.
	for _, cmd := range []string{
		"ns", "config", "flag", "experiment", "ratelimit", "breaker", "rollout", "history", "revision", "diff",
		"rollback", "audit", "snapshot", "watch", "eval", "token",
	} {
		var out bytes.Buffer
		if err := printCommandUsage(&out, cmd); err != nil || !strings.Contains(out.String(), "  "+cmd+" ") {
			t.Errorf("printCommandUsage(%q) = %q, %v", cmd, out.String(), err)
		}
	}
}

func lines0(s string) []string { return strings.Split(s, "\n") }

func containsString(list []string, s string) bool {
	for _, l := range list {
		if l == s {
			return true
		}
	}
	return false
}

// TestErrorReporting pins what main prints and returns: the name of the gRPC
// status code, and exit status 2 for command lines that cpctl rejects.
func TestErrorReporting(t *testing.T) {
	s := startServer(t, nil)
	for _, tc := range []struct {
		args []string
		text string
		exit int
	}{
		{[]string{"ns", "get", "nope"}, "NotFound: ", 1},
		{[]string{"config", "put", "nope", "k", "1"}, "NotFound: ", 1},
		{[]string{"rollout", "status", "nope", "f"}, "NotFound: ", 1},
		{[]string{"nosuch"}, `unknown command "nosuch"`, 2},
		{[]string{"ns"}, "ns: missing subcommand", 2},
		{[]string{"ns", "get"}, "ns get: expected 1 argument(s), got 0", 2},
		{[]string{"ns", "get", "a", "--bogus"}, "ns get: flag provided but not defined: -bogus", 2},
		{[]string{"flag", "put", "a", "b", "--rollout", "most"}, `invalid value "most" for flag -rollout`, 2},
		{[]string{"--timeout", "-1s", "ns", "list"}, "--timeout must not be negative", 2},
		{[]string{"--nope", "ns", "list"}, "flag provided but not defined: -nope", 2},
		{[]string{"--timeout", "soon", "ns", "list"}, "invalid value", 2},
		{[]string{"diff", "ns", "zero"}, `revision "zero" must be a number of at least 1`, 1},
		{[]string{"rollout", "start", "a", "b"}, "--stages is required", 1},
	} {
		_, err := s.run(tc.args...)
		if err == nil {
			t.Errorf("cpctl %v succeeded", tc.args)
			continue
		}
		if text := errorText(err); !strings.Contains(text, tc.text) || strings.Contains(text, "rpc error") {
			t.Errorf("cpctl %v: error text %q, want %q", tc.args, text, tc.text)
		}
		if got := exitCode(err); got != tc.exit {
			t.Errorf("cpctl %v: exit code %d, want %d (%v)", tc.args, got, tc.exit, err)
		}
	}
	// A status error keeps its code name when something wraps it.
	wrapped := fmt.Errorf("context: %w", status.Error(codes.PermissionDenied, "no"))
	if got := errorText(wrapped); !strings.HasPrefix(got, "PermissionDenied: ") {
		t.Errorf("errorText(wrapped) = %q", got)
	}
}

// TestDashValues covers values that start with a dash.
func TestDashValues(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	_, err := s.run("config", "put", "shop", "offset", "-5")
	if err == nil || !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "put -- before a value that starts with a dash") {
		t.Fatalf("config put with -5: %v", err)
	}
	for value, want := range map[string]float64{"-5": -5, "-0.5": -0.5} {
		out := s.cpctl(t, "config", "put", "shop", "offset", "--", value)
		if c := decode(t, out, &cpv1.Config{}); c.GetValue().GetNumberValue() != want {
			t.Errorf("value %s stored as %v", value, c)
		}
		// Flags still work before the --, and the dashes after it are data.
		out = s.cpctl(t, "config", "put", "--description", "d", "shop", "offset", "--", value)
		if c := decode(t, out, &cpv1.Config{}); c.GetValue().GetNumberValue() != want || c.GetDescription() != "d" {
			t.Errorf("value %s with a flag stored as %v", value, c)
		}
	}
	out := s.cpctl(t, "config", "put", "shop", "text", "--", "--enabled")
	if c := decode(t, out, &cpv1.Config{}); c.GetValue().GetStringValue() != "--enabled" {
		t.Errorf("dashed string stored as %v", c)
	}
	// A flag's own value may start with a dash.
	if f := decode(t, s.cpctl(t, "flag", "put", "shop", "f", "--description", "-x-", "--salt", "-s"), &cpv1.Flag{}); f.GetDescription() != "-x-" || f.GetSalt() != "-s" {
		t.Errorf("flag = %v", f)
	}
}

func TestFiniteNumbers(t *testing.T) {
	for _, bad := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf", "infinity", "NaN%"} {
		if _, err := parsePercent(bad); err == nil {
			t.Errorf("parsePercent(%q) succeeded", bad)
		}
		if _, err := parseStages(bad + ":1m"); err == nil {
			t.Errorf("parseStages(%q) succeeded", bad)
		}
	}
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	for _, args := range [][]string{
		{"flag", "put", "shop", "f", "--rollout", "NaN"},
		{"ratelimit", "put", "shop", "r", "--rps", "Inf", "--burst", "1"},
		{"breaker", "put", "shop", "b", "--failure-rate", "NaN"},
		{"rollout", "start", "shop", "f", "--stages", "NaN"},
	} {
		// Unknown is the code of an error found before calling the server.
		if _, err := s.run(args...); err == nil || status.Code(err) != codes.Unknown {
			t.Errorf("cpctl %v: %v", args, err)
		}
	}
	if ns := decode(t, s.cpctl(t, "ns", "get", "shop"), &cpv1.Namespace{}); ns.GetRevision() != 1 {
		t.Errorf("a rejected command changed the namespace: %v", ns)
	}
}

func TestRolloutFlagsAreScoped(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	s.cpctl(t, "flag", "put", "shop", "f", "--enabled", "--rollout", "0")
	s.cpctl(t, "rollout", "start", "shop", "f", "--stages", "10,100")
	// --stages belongs to start; anywhere else it would be silently ignored.
	for _, sub := range []string{"advance", "pause", "resume", "abort", "status"} {
		if _, err := s.run("rollout", sub, "shop", "f", "--stages", "5"); !errors.Is(err, errUsage) {
			t.Errorf("rollout %s --stages: %v", sub, err)
		}
	}
	wantLines(t, s.cpctl(t, "rollout", "status", "shop", "f"), "state active, stage 1/2")
}

func TestTimeoutFlag(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	// 0 means no deadline, not a deadline that has already passed.
	if out := s.cpctl(t, "--timeout", "0", "ns", "get", "shop"); !strings.Contains(out, `"name": "shop"`) {
		t.Fatalf("--timeout 0: %s", out)
	}
	if out := s.cpctl(t, "--timeout", "30s", "ns", "get", "shop"); !strings.Contains(out, `"name": "shop"`) {
		t.Fatalf("--timeout 30s: %s", out)
	}
	if _, err := s.run("--timeout", "1ns", "ns", "list"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("--timeout 1ns: %v", err)
	}
}

func TestTimeFlagBounds(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for _, bad := range []string{"-1h", "-5m30s", "+-1h", "1d", "1h ago", "0x10", "NaN", "9999-99-99T00:00:00Z", "0000-01-01T00:00:00Z"} {
		if ts, err := timeFlag("since", bad, now); err == nil {
			t.Errorf("timeFlag(%q) = %v", bad, ts.AsTime())
		}
	}
	for _, ok := range []string{"0", "0s", "1ns", "87600h", "0001-01-01T00:00:00Z", "9999-12-31T23:59:59Z"} {
		ts, err := timeFlag("since", ok, now)
		if err != nil {
			t.Errorf("timeFlag(%q): %v", ok, err)
			continue
		}
		if err := ts.CheckValid(); err != nil {
			t.Errorf("timeFlag(%q) is not a valid timestamp: %v", ok, err)
		}
	}
	s := startServer(t, nil)
	_, err := s.run("audit", "--since", "1h", "--until", "2h")
	if !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "--since is later than --until") {
		t.Fatalf("audit with since after until: %v", err)
	}
	if _, err := s.run("audit", "--since", "-1h"); err == nil || !strings.Contains(err.Error(), "--since") {
		t.Fatalf("audit --since -1h: %v", err)
	}
}

// TestRevisions64 checks that revisions are 64-bit end to end.
func TestRevisions64(t *testing.T) {
	const max = math.MaxInt64
	if rev, err := parseRevision(strconv.FormatInt(max, 10)); err != nil || rev != max {
		t.Fatalf("parseRevision(max) = %d, %v", rev, err)
	}
	for _, bad := range []string{"9223372036854775808", "99999999999999999999", "+0", "0x10", "1e3", " 7", "7 "} {
		if _, err := parseRevision(bad); err == nil {
			t.Errorf("parseRevision(%q) succeeded", bad)
		}
	}
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	big := strconv.FormatInt(max, 10)
	// 2^31 and 2^32 would be truncated by a 32-bit conversion.
	for _, n := range []string{"2147483649", "4294967297", big} {
		s.fail(t, codes.Aborted, "config", "put", "shop", "k", "1", "--expected-revision", n)
		s.fail(t, codes.NotFound, "revision", "shop", n)
		s.fail(t, codes.NotFound, "diff", "shop", "1", n)
		s.fail(t, codes.NotFound, "rollback", "shop", n)
		s.fail(t, codes.Aborted, "rollback", "shop", "1", "--yes", "--expected-revision", n)
	}
	if out := s.cpctl(t, "history", "shop", "--before", big); !strings.Contains(out, "create namespace") {
		t.Fatalf("history --before max:\n%s", out)
	}
}

func TestClean(t *testing.T) {
	for in, want := range map[string]string{
		"plain":          "plain",
		"ünïcode ✓":      "ünïcode ✓",
		"a\nb\tc":        "a?b?c",
		"\x1b[31mred":    "?[31mred",
		"nul\x00byte":    "nul?byte",
		"\u0085next":     "?next",
		"\u202eoverride": "\u202eoverride", // not a control character; left alone
	} {
		if got := clean(in); got != want {
			t.Errorf("clean(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTableTrimsTrailingSpace(t *testing.T) {
	var out bytes.Buffer
	tb := newTable(&out)
	fmt.Fprintln(tb, "TYPE\tKEY\tRESULT\tPAYLOAD")
	fmt.Fprintln(tb, "flag\tlong-key-name\ton\t")
	fmt.Fprintln(tb, "experiment\tk\tblue\t{\"a\":1}")
	if err := tb.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "TYPE        KEY            RESULT  PAYLOAD\n" +
		"flag        long-key-name  on\n" +
		"experiment  k              blue    {\"a\":1}\n"
	if out.String() != want {
		t.Fatalf("table:\n%q\nwant\n%q", out.String(), want)
	}
}

// TestOutputLayout runs every table command
// against a server and checks the shared layout rules.
func TestOutputLayout(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")
	s.cpctl(t, "flag", "put", "shop", "f", "--enabled")
	s.cpctl(t, "experiment", "put", "shop", "e", "--variants", "a=1,b=1", "--enabled", "--payload", `a={"k":[1,2]}`)
	s.cpctl(t, "rollout", "start", "shop", "f", "--stages", "10:1h,100")
	for _, args := range [][]string{
		{"ns", "list"}, {"history", "shop"}, {"audit", "shop"}, {"eval", "shop", "u1"}, {"eval", "shop", "u2"},
		{"rollout", "status", "shop", "f"}, {"diff", "shop", "1"}, {"rollback", "shop", "3"},
	} {
		out := s.cpctl(t, args...)
		if out == "" || !strings.HasSuffix(out, "\n") {
			t.Errorf("cpctl %v output %q", args, out)
		}
		for _, l := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
			if strings.TrimRight(l, " ") != l {
				t.Errorf("cpctl %v: line %q ends in spaces", args, l)
			}
			for _, r := range l {
				if unicode.IsControl(r) {
					t.Errorf("cpctl %v: line %q has a control character", args, l)
				}
			}
		}
	}
}
