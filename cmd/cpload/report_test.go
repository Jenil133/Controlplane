package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	// seq returns the sorted samples 1ns, 2ns, ..., n ns.
	seq := func(n int) []time.Duration {
		s := make([]time.Duration, n)
		for i := range s {
			s[i] = time.Duration(i + 1)
		}
		return s
	}
	tests := []struct {
		n, p int
		want time.Duration
	}{
		{1, 50, 1},
		{1, 99, 1},
		{2, 50, 1},
		{2, 90, 2},
		{10, 1, 1},
		{10, 50, 5},
		{10, 90, 9},
		{10, 99, 10},
		{10, 100, 10},
		{100, 50, 50},
		{100, 99, 99},
		{101, 50, 51},
		{101, 99, 100},
		{1000, 99, 990},
	}
	for _, tt := range tests {
		if got := percentile(seq(tt.n), tt.p); got != tt.want {
			t.Errorf("percentile(1..%d, %d) = %d, want %d (nearest rank)", tt.n, tt.p, got, tt.want)
		}
	}
}

func TestSummarize(t *testing.T) {
	if got := summarize(nil); got != (stats{}) {
		t.Fatalf("summarize(nil) = %+v, want zero", got)
	}
	samples := []time.Duration{9, 2, 60, 5, 30, 3, 10}
	want := stats{Count: 7, P50: 9, P90: 60, P99: 60, Max: 60}
	if got := summarize(samples); got != want {
		t.Fatalf("summarize = %+v, want %+v", got, want)
	}
}

const ms1 = time.Millisecond

// sampleWrites and sampleWatchers describe a run with three writes and three
// watchers that each start at revision 4; watcher 1 skips revision 5 and
// watcher 2 skips revision 6.
var (
	sampleWrites = []write{
		{revision: 5, sent: 10 * ms1},
		{revision: 6, sent: 110 * ms1},
		{revision: 7, sent: 210 * ms1},
	}
	sampleWatchers = [][]delivery{
		{{4, 1 * ms1}, {5, 12 * ms1}, {6, 113 * ms1}, {7, 215 * ms1}},
		{{4, 2 * ms1}, {6, 120 * ms1}, {7, 240 * ms1}},
		{{4, 3 * ms1}, {5, 19 * ms1}, {7, 270 * ms1}},
	}
)

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name     string
		watchers [][]delivery
		want     analysis
	}{
		{
			name:     "all converged",
			watchers: sampleWatchers,
			want: analysis{
				// The initial snapshots (revision 4) are no write's delivery.
				latency: []time.Duration{2 * ms1, 3 * ms1, 5 * ms1, 10 * ms1, 30 * ms1, 9 * ms1, 60 * ms1},
				// Revision 5 waits for watcher 1 to reach 6 at 120ms, revision
				// 6 for watcher 2 to reach 7 at 270ms.
				convergence: []time.Duration{110 * ms1, 160 * ms1, 60 * ms1},
				skipped:     2,
			},
		},
		{
			name:     "watcher 2 stops at revision 5",
			watchers: [][]delivery{sampleWatchers[0], sampleWatchers[1], sampleWatchers[2][:2]},
			want: analysis{
				latency:     []time.Duration{2 * ms1, 3 * ms1, 5 * ms1, 10 * ms1, 30 * ms1, 9 * ms1},
				convergence: []time.Duration{110 * ms1},
				skipped:     1,
				unconverged: 2,
			},
		},
		{
			name:     "nobody got past the initial snapshot",
			watchers: [][]delivery{{{4, 1 * ms1}}, {{4, 2 * ms1}}},
			want:     analysis{unconverged: 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := analyze(sampleWrites, tt.watchers); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("analyze =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}
}

func sampleConfig() config {
	return config{Addrs: []string{"a:9090", "b:9090"}, Namespace: "loadtest", ConnsPerAddr: 4, SLO: time.Second}
}

func sampleReport(cfg config, writeErrs []string, watchErrs []error, stopped error) *report {
	watchers := make([]*watcher, len(sampleWatchers))
	for i, ds := range sampleWatchers {
		watchers[i] = &watcher{addr: "a:9090", deliveries: ds}
		if i < len(watchErrs) {
			watchers[i].err = watchErrs[i]
		}
	}
	return newReport(cfg, sampleWrites, writeErrs, watchers, stopped)
}

func TestNewReport(t *testing.T) {
	r := sampleReport(sampleConfig(), nil, nil, nil)
	if !r.Passed() || len(r.Failures) != 0 || len(r.Errors) != 0 {
		t.Fatalf("clean run did not pass: failures %v, errors %v", r.Failures, r.Errors)
	}
	if r.Watchers != 3 || r.Writes != 3 || r.Deliveries != 7 || r.Skipped != 2 || r.Unconverged != 0 {
		t.Fatalf("report = %+v", r)
	}
	if want := (stats{Count: 7, P50: 9 * ms1, P90: 60 * ms1, P99: 60 * ms1, Max: 60 * ms1}); r.Latency != want {
		t.Fatalf("latency = %+v, want %+v", r.Latency, want)
	}
	if want := (stats{Count: 3, P50: 110 * ms1, P90: 160 * ms1, P99: 160 * ms1, Max: 160 * ms1}); r.Convergence != want {
		t.Fatalf("convergence = %+v, want %+v", r.Convergence, want)
	}

	tests := []struct {
		name      string
		slo       time.Duration
		writeErrs []string
		watchErrs []error
		stopped   error
		failures  []string
		errors    []errorCount
	}{
		{
			name:     "convergence p99 over the slo",
			slo:      100 * ms1,
			failures: []string{"convergence p99 160.00ms exceeds slo 100ms"},
		},
		{
			name:     "convergence p99 exactly at the slo",
			slo:      160 * ms1,
			failures: []string{},
		},
		{
			name:      "watch and write errors",
			slo:       time.Second,
			writeErrs: []string{"write b:9090: boom"},
			watchErrs: []error{errors.New("watch a:9090: gone"), errors.New("watch a:9090: gone")},
			failures:  []string{"2 watch error(s)", "1 write error(s)"},
			errors:    []errorCount{{"watch a:9090: gone", 2}, {"write b:9090: boom", 1}},
		},
		{
			name:     "stopped early",
			slo:      time.Second,
			stopped:  errors.New("timed out after 1m0s"),
			failures: []string{"run stopped early: timed out after 1m0s"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sampleConfig()
			cfg.SLO = tt.slo
			r := sampleReport(cfg, tt.writeErrs, tt.watchErrs, tt.stopped)
			if !reflect.DeepEqual(r.Failures, tt.failures) {
				t.Errorf("failures = %q, want %q", r.Failures, tt.failures)
			}
			if r.Passed() != (len(tt.failures) == 0) {
				t.Errorf("Passed = %v with failures %q", r.Passed(), r.Failures)
			}
			if tt.errors == nil {
				tt.errors = []errorCount{}
			}
			if !reflect.DeepEqual(r.Errors, tt.errors) {
				t.Errorf("errors = %v, want %v", r.Errors, tt.errors)
			}
		})
	}
}

func TestWriteText(t *testing.T) {
	var out bytes.Buffer
	if err := sampleReport(sampleConfig(), nil, nil, nil).writeText(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"addresses     a:9090, b:9090 (4 connections each)\n",
		"watchers      3 (0 failed)\n",
		"writes        3 committed (0 failed)\n",
		"deliveries    7 (2 revisions skipped)\n",
		"latency       p50 9.00ms  p90 60.00ms  p99 60.00ms  max 60.00ms\n",
		"convergence   p50 110.00ms  p90 160.00ms  p99 160.00ms  max 160.00ms (3 of 3 writes)\n",
		"errors        none\n",
		"result        PASS (convergence p99 160.00ms, slo 1s)\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, out.String())
		}
	}

	out.Reset()
	failed := sampleReport(sampleConfig(), []string{"write b:9090: boom"}, []error{errors.New("watch a:9090: gone")}, nil)
	if err := failed.writeText(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"errors        1x watch a:9090: gone\n              1x write b:9090: boom\n",
		"result        FAIL: 1 watch error(s); 1 write error(s)\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, out.String())
		}
	}
}

func TestWriteJSON(t *testing.T) {
	cfg := sampleConfig()
	cfg.SLO = 100 * ms1
	var out bytes.Buffer
	if err := sampleReport(cfg, nil, nil, nil).writeJSON(&out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Namespace    string    `json:"namespace"`
		Addrs        []string  `json:"addrs"`
		ConnsPerAddr int       `json:"conns_per_addr"`
		Watchers     int       `json:"watchers"`
		Writes       int       `json:"writes"`
		WriteErrors  *int      `json:"write_errors"`
		WatchErrors  *int      `json:"watch_errors"`
		Deliveries   int       `json:"deliveries"`
		Skipped      int       `json:"skipped"`
		Unconverged  *int      `json:"unconverged"`
		Latency      jsonStats `json:"latency"`
		Convergence  jsonStats `json:"convergence"`
		SLO          float64   `json:"slo_ms"`
		Errors       []any     `json:"errors"`
		Passed       *bool     `json:"passed"`
		Failures     []string  `json:"failures"`
	}
	dec := json.NewDecoder(&out)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Namespace != "loadtest" || len(got.Addrs) != 2 || got.ConnsPerAddr != 4 || got.Watchers != 3 || got.Writes != 3 ||
		got.Deliveries != 7 || got.Skipped != 2 || got.SLO != 100 {
		t.Fatalf("decoded %+v", got)
	}
	if got.WriteErrors == nil || *got.WriteErrors != 0 || got.WatchErrors == nil || *got.WatchErrors != 0 ||
		got.Unconverged == nil || *got.Unconverged != 0 {
		t.Fatalf("error counts missing or wrong: %+v", got)
	}
	if want := (jsonStats{Count: 7, P50: 9, P90: 60, P99: 60, Max: 60}); got.Latency != want {
		t.Fatalf("latency = %+v, want %+v", got.Latency, want)
	}
	if want := (jsonStats{Count: 3, P50: 110, P90: 160, P99: 160, Max: 160}); got.Convergence != want {
		t.Fatalf("convergence = %+v, want %+v", got.Convergence, want)
	}
	if got.Passed == nil || *got.Passed || len(got.Failures) != 1 || got.Errors == nil || len(got.Errors) != 0 {
		t.Fatalf("passed %v, failures %q, errors %v; want a failed run with an empty error list", got.Passed, got.Failures, got.Errors)
	}
}

func TestMillisecondsRoundToTheMicrosecond(t *testing.T) {
	if got := ms(1234567 * time.Nanosecond); got != 1.235 {
		t.Fatalf("ms(1.234567ms) = %v, want 1.235", got)
	}
	if got := millis(1234567 * time.Nanosecond); got != "1.23ms" {
		t.Fatalf("millis(1.234567ms) = %q, want 1.23ms", got)
	}
}
