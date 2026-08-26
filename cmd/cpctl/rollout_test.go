package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestParseStages(t *testing.T) {
	stages, err := parseStages(" 1:10m, 5:1h30m ,,25%:0,100")
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		percent  float64
		duration time.Duration
		timed    bool
	}{{1, 10 * time.Minute, true}, {5, 90 * time.Minute, true}, {25, 0, true}, {100, 0, false}}
	if len(stages) != len(want) {
		t.Fatalf("stages = %v", stages)
	}
	for i, w := range want {
		st := stages[i]
		if st.GetPercent() != w.percent || st.GetDuration().AsDuration() != w.duration || (st.GetDuration() != nil) != w.timed {
			t.Errorf("stage %d = %v, want %+v", i, st, w)
		}
	}
	for _, bad := range []string{"", " , ", "x", "1:10", "1:10m:5", ":10m", "5:soon", "%:1m"} {
		if _, err := parseStages(bad); err == nil {
			t.Errorf("parseStages(%q) succeeded", bad)
		}
	}
}

func TestFormatStagesRoundTrip(t *testing.T) {
	for _, spec := range []string{"1:10m,5:10m,25:30m,100", "0.5:1h30m,50:24h,100", "100", "1:1m30s,2:500ms,3:1h0m30s,4"} {
		stages, err := parseStages(spec)
		if err != nil {
			t.Fatalf("parseStages(%q): %v", spec, err)
		}
		if got := formatStages(stages); got != spec {
			t.Errorf("formatStages(parseStages(%q)) = %q", spec, got)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0:                                  "0s",
		500 * time.Millisecond:             "500ms",
		30 * time.Second:                   "30s",
		90 * time.Second:                   "1m30s",
		10 * time.Minute:                   "10m",
		time.Hour:                          "1h",
		90 * time.Minute:                   "1h30m",
		time.Hour + 30*time.Second:         "1h0m30s",
		24 * time.Hour:                     "24h",
		time.Minute + 500*time.Millisecond: "1m0.5s",
	} {
		if got := formatDuration(d); got != want {
			t.Errorf("formatDuration(%v) = %q, want %q", d, got, want)
		}
		if parsed, err := time.ParseDuration(formatDuration(d)); err != nil || parsed != d {
			t.Errorf("formatDuration(%v) does not parse back: %v, %v", d, parsed, err)
		}
	}
}

const timeRE = `\d{4}-\d\d-\d\d \d\d:\d\d:\d\d`

func TestRolloutCommands(t *testing.T) {
	s := startServer(t, nil)
	s.cpctl(t, "ns", "create", "shop")                                           // revision 1
	s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "0") // revision 2

	out := s.cpctl(t, "--actor", "alice", "rollout", "start", "shop", "new-cart", "--stages", "1:10m, 5:1h30m, 25, 100")
	wantLines(t, out,
		"flag new-cart in shop (revision 3)",
		"state active, stage 1/4",
		"rollout percent 1%",
		"stages 1:10m,5:1h30m,25,100")
	wantMatch(t, out, `started `+timeRE+` by alice`)
	wantMatch(t, out, `next stage 5% due `+timeRE)

	// The plan owns the percentage while it runs.
	s.fail(t, codes.FailedPrecondition, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "50")
	s.fail(t, codes.FailedPrecondition, "rollout", "start", "shop", "new-cart", "--stages", "100")
	s.fail(t, codes.FailedPrecondition, "rollout", "resume", "shop", "new-cart")

	wantLines(t, s.cpctl(t, "rollout", "advance", "shop", "new-cart"), "state active, stage 2/4", "rollout percent 5%")
	wantLines(t, s.cpctl(t, "rollout", "pause", "shop", "new-cart"),
		"state paused, stage 2/4", "rollout percent 5%", "next stage 25% after resume or manual advance")
	out = s.cpctl(t, "rollout", "resume", "shop", "new-cart")
	wantLines(t, out, "flag new-cart in shop (revision 6)", "state active, stage 2/4")
	wantMatch(t, out, `next stage 25% due `+timeRE)
	out = s.cpctl(t, "rollout", "advance", "shop", "new-cart")
	wantLines(t, out, "state active, stage 3/4", "rollout percent 25%", "next stage 100% when advanced manually")
	if status := s.cpctl(t, "rollout", "status", "shop", "new-cart"); status != out {
		t.Fatalf("rollout status:\n%s\ndiffers from what advance printed:\n%s", status, out)
	}

	out = s.cpctl(t, "rollout", "abort", "shop", "new-cart")
	wantLines(t, out, "state aborted, stage 3/4", "rollout percent 0%")
	if strings.Contains(out, "next stage") {
		t.Fatalf("aborted rollout has a next stage:\n%s", out)
	}
	s.fail(t, codes.FailedPrecondition, "rollout", "advance", "shop", "new-cart")

	// A finished plan can be replaced; a single stage completes at once and
	// hands the percentage back.
	wantLines(t, s.cpctl(t, "rollout", "start", "shop", "new-cart", "--stages", "100%"),
		"state completed, stage 1/1", "rollout percent 100%", "stages 100")
	s.cpctl(t, "flag", "put", "shop", "new-cart", "--enabled", "--rollout", "50") // revision 10

	s.cpctl(t, "flag", "put", "shop", "plain", "--enabled") // revision 11
	if out := s.cpctl(t, "rollout", "status", "shop", "plain"); out != "flag             plain in shop (revision 11)\nstate            no rollout\nrollout percent  100%\n" {
		t.Fatalf("status of a flag without a rollout:\n%s", out)
	}
	s.fail(t, codes.Unknown, "rollout", "status", "shop", "missing")
	s.fail(t, codes.NotFound, "rollout", "advance", "shop", "missing")
	s.fail(t, codes.NotFound, "rollout", "status", "nope", "plain")
	s.fail(t, codes.Unknown, "rollout", "start", "shop", "plain")
	s.fail(t, codes.Unknown, "rollout", "start", "shop", "plain", "--stages", "5:soon")
	s.fail(t, codes.InvalidArgument, "rollout", "start", "shop", "plain", "--stages", "50,10")
	s.fail(t, codes.Unknown, "rollout", "skip", "shop", "plain")
	s.fail(t, codes.Unknown, "rollout", "status", "shop")
	s.fail(t, codes.Unknown, "rollout")
}
