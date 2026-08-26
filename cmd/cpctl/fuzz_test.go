package main

import (
	"bytes"
	"flag"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

// The parsers below take text typed by people, so they get both a table of
// hostile inputs and a fuzz target. The fuzz targets run their seeds as
// ordinary tests; "go test -fuzz FuzzParseStages" explores further. What
// they check is that nothing panics and that what is accepted is sane.

var hostile = []string{
	"", " ", ",", ",,,", ":", "::", ":::", "=", "==", "=,=", ",=", "a=", "=a", "-", "--", "-0", "+", "%", "%%", "1%%",
	"NaN", "Inf", "-Inf", "+Inf", "nan:1m", "1:NaN", "1e400", "-1e400", "1e-400", "0x1p3", "1_0", "1,0", "١٢٣",
	"1:1", "1:-1m", "1:+1m", "1:1m:1m", "1:106751d", "1:2562047h47m16.854775807s", "1:2562047h47m16.854775808s",
	"1:9223372036854775807ns", "1:9223372036854775808ns", "1:1e3s", "1:.5s", "1:5.s", "1:1µs", "1:1us", "1:1 m",
	"100:0", "0:0", "0.001", "99.999", "100.01", "-1", "-0.0001", "1:0s,2:0s,3:0s",
	"a=1,a=1", "a=4294967295", "a=4294967296", "a=-1", "a=+1", "a=1.5", "a=0x10", "a=1_0", "a= 1", " a = 1 ", "a=1,,b=2",
	"a=1,b", "\x00", "a\x00=1", "a=\x00", "\xff\xfe", "a=\xff", "é=1", "a=1\n", "\n", "\t", "{", "[", `"`, `"a`, "null",
	`{"a":`, `{"a":1}`, `"x"`, "1e999999999", "9" + strings.Repeat("9", 400), strings.Repeat("1,", 5000),
	strings.Repeat("a=1,", 5000), strings.Repeat("(", 10000), strings.Repeat("[", 100000),
	"2026-10-03T12:00:00Z", "2026-10-03T12:00:00+25:00", "2026-13-01T00:00:00Z", "0000-01-01T00:00:00Z",
	"10000-01-01T00:00:00Z", "1h", "-1h", "1h1h", "1.5h", "1d", "0", "00", "1ns", "9223372036854775807",
	"9223372036854775808", "2562048h", "-2562048h", "87600h", "1h-1m",
}

func TestParsersSurviveHostileInput(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for _, in := range hostile {
		checkParsers(t, in, now)
	}
}

func checkParsers(t *testing.T, in string, now time.Time) {
	t.Helper()
	if stages, err := parseStages(in); err == nil {
		if len(stages) == 0 {
			t.Errorf("parseStages(%.40q) accepted no stages", in)
		}
		for _, st := range stages {
			if p := st.GetPercent(); math.IsNaN(p) || math.IsInf(p, 0) {
				t.Errorf("parseStages(%.40q) gave percent %v", in, p)
			}
			if st.GetDuration() != nil && st.GetDuration().CheckValid() != nil {
				t.Errorf("parseStages(%.40q) gave an invalid duration", in)
			}
		}
		// What is shown can be typed back and means the same (to the two
		// decimals a percentage is shown with).
		again, err := parseStages(formatStages(stages))
		if err != nil || len(again) != len(stages) {
			t.Errorf("parseStages(%.40q) does not round-trip: %v", in, err)
		}
		for i := range again {
			if formatPercent(again[i].GetPercent()) != formatPercent(stages[i].GetPercent()) ||
				again[i].GetDuration().AsDuration() != stages[i].GetDuration().AsDuration() {
				t.Errorf("parseStages(%.40q): stage %d changed in a round trip", in, i)
			}
		}
	}
	if vs, err := parseVariants(in, keyValues{}); err == nil && len(vs) == 0 {
		t.Errorf("parseVariants(%.40q) accepted no variants", in)
	}
	if _, err := parseVariants("a=1", keyValues{"a": in}); err != nil {
		t.Errorf("a payload of %.40q was refused: %v", in, err)
	}
	if v, err := parseValue(in); err != nil || v == nil {
		t.Errorf("parseValue(%.40q) = %v, %v: any text must be accepted", in, v, err)
	}
	if p, err := parsePercent(in); err == nil && (math.IsNaN(p) || math.IsInf(p, 0)) {
		t.Errorf("parsePercent(%.40q) = %v", in, p)
	}
	if ts, err := timeFlag("since", in, now); err == nil && ts != nil {
		if err := ts.CheckValid(); err != nil {
			t.Errorf("timeFlag(%.40q) gave an invalid timestamp: %v", in, err)
		}
		if ts.AsTime().After(now.AddDate(8000, 0, 0)) {
			t.Errorf("timeFlag(%.40q) = %v", in, ts.AsTime())
		}
	}
	if _, err := parseRevision(in); err == nil {
		if n, _ := parseRevision(in); n < 1 {
			t.Errorf("parseRevision(%.40q) = %d", in, n)
		}
	}
	var l stringList
	_ = l.Set(in)
	_ = keyValues{}.Set(in)
	_ = clean(in)
	if d, err := time.ParseDuration(in); err == nil {
		if back, err := time.ParseDuration(formatDuration(d)); err != nil || back != d {
			t.Errorf("formatDuration(%v) = %q does not parse back", d, formatDuration(d))
		}
	}
}

func FuzzParsers(f *testing.F) {
	for _, in := range hostile {
		f.Add(in)
	}
	for _, in := range []string{"1:10m,5:10m,25:30m,100", "control=50,treatment=50", "2026-10-01T08:30:00Z", "24h"} {
		f.Add(in)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, in string) { checkParsers(t, in, now) })
}

// FuzzParseArgs feeds argument vectors to the flag-interleaving parser of
// the commands with the most flags.
func FuzzParseArgs(f *testing.F) {
	for _, in := range []string{
		"ns\x00key\x00--enabled", "--\x00--\x00--", "--description\x00--\x00x", "-enabled=false\x00a", "--rollout\x001\x00a\x00b",
		"a\x00--allow\x00", "\x00\x00", "-", "--=", "--enabled=", "-h", "--help\x00a",
	} {
		f.Add(in, 0, 3)
	}
	f.Fuzz(func(t *testing.T, joined string, lo, hi int) {
		fs := flag.NewFlagSet("fuzz", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		fs.String("description", "", "")
		fs.Bool("enabled", false, "")
		fs.Func("rollout", "", func(s string) error { _, err := parsePercent(s); return err })
		fs.Var(new(stringList), "allow", "")
		args := strings.Split(joined, "\x00")
		pos, err := parseArgsRange(fs, args, lo, hi)
		if err == nil && (len(pos) < lo || len(pos) > hi) {
			t.Errorf("parseArgsRange(%q, %d, %d) = %q", args, lo, hi, pos)
		}
		if len(pos) > len(args) {
			t.Errorf("more positional arguments (%q) than arguments (%q)", pos, args)
		}
	})
}

// FuzzRun runs whole command lines against an unreachable server with a tiny
// timeout, checking that nothing panics and that a failure prints nothing.
func FuzzRun(f *testing.F) {
	for _, in := range []string{
		"flag\x00put\x00a\x00b\x00--rollout\x00x", "rollout\x00start\x00a\x00b\x00--stages\x00,", "audit\x00--since\x00-1",
		"breaker\x00put\x00a\x00b\x00--window\x00x", "diff\x00a\x00--", "token\x00generate\x00--name\x00x",
		"help\x00a", "--timeout\x00-1\x00ns", "eval\x00a", "rollback\x00a\x000", "history\x00a\x00--limit\x00x",
	} {
		f.Add(in)
	}
	f.Fuzz(func(t *testing.T, joined string) {
		for _, skip := range []string{"watch", "token", "addr", "timeout", "help", "-h"} {
			if strings.Contains(joined, skip) {
				return // these print on success, or would change how the run is bounded
			}
		}
		args := append([]string{"--addr", "127.0.0.1:1", "--timeout", "1ns"}, strings.Split(joined, "\x00")...)
		var out bytes.Buffer
		if err := run(t.Context(), args, &out); err != nil {
			_ = errorText(err)
			_ = exitCode(err)
			if out.Len() != 0 {
				t.Errorf("cpctl %q failed (%v) but printed %q", args, err, out.String())
			}
		}
	})
}
