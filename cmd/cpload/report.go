package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"
)

// report is the outcome of one run.
type report struct {
	Namespace    string
	Addrs        []string
	ConnsPerAddr int
	Watchers     int
	Writes       int // committed writes
	WriteErrors  int
	WatchErrors  int
	// Deliveries counts the snapshots watchers received for the writes'
	// revisions: one latency sample each.
	Deliveries int
	// Skipped counts (watcher, write) pairs where the watcher went straight
	// from an older revision to a newer one than the write's.
	Skipped int
	// Unconverged counts writes that some watcher never caught up with.
	Unconverged int
	Latency     stats
	Convergence stats
	SLO         time.Duration
	Errors      []errorCount
	// Failures says why the run failed; empty when it passed.
	Failures []string
}

// errorCount is one distinct error message and how often it occurred.
type errorCount struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

// stats summarizes a set of durations.
type stats struct {
	Count              int
	P50, P90, P99, Max time.Duration
}

// Passed reports whether the run met every requirement.
func (r *report) Passed() bool { return len(r.Failures) == 0 }

// newReport turns what a run recorded into a report. stopped is why the run
// ended early, or nil.
func newReport(cfg config, writes []write, writeErrs []string, watchers []*watcher, stopped error) *report {
	r := &report{
		Namespace:    cfg.Namespace,
		Addrs:        cfg.Addrs,
		ConnsPerAddr: cfg.ConnsPerAddr,
		Watchers:     len(watchers),
		Writes:       len(writes),
		WriteErrors:  len(writeErrs),
		SLO:          cfg.SLO,
	}
	deliveries := make([][]delivery, len(watchers))
	errs := slices.Clone(writeErrs)
	for i, w := range watchers {
		deliveries[i] = w.deliveries
		if w.err != nil {
			r.WatchErrors++
			errs = append(errs, w.err.Error())
		}
	}
	r.Errors = tally(errs)

	a := analyze(writes, deliveries)
	r.Deliveries, r.Skipped, r.Unconverged = len(a.latency), a.skipped, a.unconverged
	r.Latency, r.Convergence = summarize(a.latency), summarize(a.convergence)

	r.Failures = []string{}
	fail := func(format string, args ...any) { r.Failures = append(r.Failures, fmt.Sprintf(format, args...)) }
	if stopped != nil {
		fail("run stopped early: %v", stopped)
	}
	if r.WatchErrors > 0 {
		fail("%d watch error(s)", r.WatchErrors)
	}
	if r.WriteErrors > 0 {
		fail("%d write error(s)", r.WriteErrors)
	}
	if r.Unconverged > 0 {
		fail("%d write(s) never reached every watcher", r.Unconverged)
	}
	if r.Convergence.Count > 0 && r.Convergence.P99 > r.SLO {
		fail("convergence p99 %s exceeds slo %v", millis(r.Convergence.P99), r.SLO)
	}
	return r
}

// analysis holds the raw samples of a run.
type analysis struct {
	latency     []time.Duration // one per delivery of a write's revision
	convergence []time.Duration // one per write every watcher caught up with
	skipped     int
	unconverged int
}

// analyze computes per-delivery latencies and per-write convergence times.
// writes and every watcher's deliveries must be in increasing revision order.
func analyze(writes []write, watchers [][]delivery) analysis {
	sent := make(map[int64]time.Duration, len(writes))
	for _, w := range writes {
		sent[w.revision] = w.sent
	}
	var a analysis
	// For write j: when the slowest watcher so far first held its revision
	// or a later one, and how many watchers ever did.
	caughtUp := make([]time.Duration, len(writes))
	reached := make([]int, len(writes))
	for _, ds := range watchers {
		for _, d := range ds {
			if s, ok := sent[d.revision]; ok {
				a.latency = append(a.latency, d.at-s)
			}
		}
		// Both lists are sorted by revision, so one merge pass finds, for
		// every write, the first delivery at or after its revision.
		k := 0
		for j, w := range writes {
			for k < len(ds) && ds[k].revision < w.revision {
				k++
			}
			if k == len(ds) {
				break // this watcher never got this far
			}
			reached[j]++
			caughtUp[j] = max(caughtUp[j], ds[k].at)
			if ds[k].revision != w.revision {
				a.skipped++
			}
		}
	}
	for j, w := range writes {
		if reached[j] < len(watchers) {
			a.unconverged++
			continue
		}
		a.convergence = append(a.convergence, caughtUp[j]-w.sent)
	}
	return a
}

// summarize sorts samples in place and returns their percentiles.
func summarize(samples []time.Duration) stats {
	if len(samples) == 0 {
		return stats{}
	}
	slices.Sort(samples)
	return stats{
		Count: len(samples),
		P50:   percentile(samples, 50),
		P90:   percentile(samples, 90),
		P99:   percentile(samples, 99),
		Max:   samples[len(samples)-1],
	}
}

// percentile returns the nearest-rank p-th percentile, 1 <= p <= 100, of a
// non-empty sorted slice: the smallest sample that at least p% of the
// samples do not exceed.
func percentile(sorted []time.Duration, p int) time.Duration {
	rank := (p*len(sorted) + 99) / 100 // ceil(p/100 * n) in integer math
	return sorted[rank-1]
}

// tally counts identical messages, most frequent first.
func tally(msgs []string) []errorCount {
	counts := make(map[string]int)
	for _, m := range msgs {
		counts[m]++
	}
	out := make([]errorCount, 0, len(counts))
	for m, n := range counts {
		out = append(out, errorCount{Message: m, Count: n})
	}
	slices.SortFunc(out, func(a, b errorCount) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), strings.Compare(a.Message, b.Message))
	})
	return out
}

// millis formats d in milliseconds, so that all figures share one unit.
func millis(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

func (s stats) String() string {
	if s.Count == 0 {
		return "no samples"
	}
	return fmt.Sprintf("p50 %s  p90 %s  p99 %s  max %s", millis(s.P50), millis(s.P90), millis(s.P99), millis(s.Max))
}

func (r *report) writeText(w io.Writer) error {
	var b strings.Builder
	line := func(label, format string, args ...any) {
		fmt.Fprintf(&b, "%-13s %s\n", label, fmt.Sprintf(format, args...))
	}
	line("namespace", "%s", r.Namespace)
	line("addresses", "%s (%d connections each)", strings.Join(r.Addrs, ", "), r.ConnsPerAddr)
	line("watchers", "%d (%d failed)", r.Watchers, r.WatchErrors)
	line("writes", "%d committed (%d failed)", r.Writes, r.WriteErrors)
	line("deliveries", "%d (%d revisions skipped)", r.Deliveries, r.Skipped)
	line("latency", "%v", r.Latency)
	line("convergence", "%v (%d of %d writes)", r.Convergence, r.Convergence.Count, r.Writes)
	if len(r.Errors) == 0 {
		line("errors", "none")
	}
	for i, e := range r.Errors {
		label := ""
		if i == 0 {
			label = "errors"
		}
		line(label, "%dx %s", e.Count, e.Message)
	}
	if r.Passed() {
		line("result", "PASS (convergence p99 %s, slo %v)", millis(r.Convergence.P99), r.SLO)
	} else {
		line("result", "FAIL: %s", strings.Join(r.Failures, "; "))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// jsonStats is stats in milliseconds, rounded to the microsecond.
type jsonStats struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func toJSONStats(s stats) jsonStats {
	return jsonStats{Count: s.Count, P50: ms(s.P50), P90: ms(s.P90), P99: ms(s.P99), Max: ms(s.Max)}
}

func ms(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}

func (r *report) writeJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		Namespace    string       `json:"namespace"`
		Addrs        []string     `json:"addrs"`
		ConnsPerAddr int          `json:"conns_per_addr"`
		Watchers     int          `json:"watchers"`
		Writes       int          `json:"writes"`
		WriteErrors  int          `json:"write_errors"`
		WatchErrors  int          `json:"watch_errors"`
		Deliveries   int          `json:"deliveries"`
		Skipped      int          `json:"skipped"`
		Unconverged  int          `json:"unconverged"`
		Latency      jsonStats    `json:"latency"`
		Convergence  jsonStats    `json:"convergence"`
		SLO          float64      `json:"slo_ms"`
		Errors       []errorCount `json:"errors"`
		Passed       bool         `json:"passed"`
		Failures     []string     `json:"failures"`
	}{
		Namespace:    r.Namespace,
		Addrs:        r.Addrs,
		ConnsPerAddr: r.ConnsPerAddr,
		Watchers:     r.Watchers,
		Writes:       r.Writes,
		WriteErrors:  r.WriteErrors,
		WatchErrors:  r.WatchErrors,
		Deliveries:   r.Deliveries,
		Skipped:      r.Skipped,
		Unconverged:  r.Unconverged,
		Latency:      toJSONStats(r.Latency),
		Convergence:  toJSONStats(r.Convergence),
		SLO:          ms(r.SLO),
		Errors:       r.Errors,
		Passed:       r.Passed(),
		Failures:     r.Failures,
	})
}
