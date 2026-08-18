package breaker

import (
	"testing"
	"time"
)

func TestWindowRollsWholeSlots(t *testing.T) {
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return start.Add(d) }
	var w window
	w.reset(10*time.Second, start) // 1s slots

	w.add(at(0), false)
	w.add(at(999*time.Millisecond), true) // late in the first slot
	w.add(at(5*time.Second), false)
	w.add(at(9*time.Second), true)

	steps := []struct {
		at   time.Duration
		want tally
	}{
		{9999 * time.Millisecond, tally{requests: 4, failures: 2}},
		// The whole first slot goes at 10s, only 9.001s after its last outcome.
		{10 * time.Second, tally{requests: 2, failures: 1}},
		{15 * time.Second, tally{requests: 1}},
		{19 * time.Second, tally{}},
	}
	for _, s := range steps {
		if got := w.sum(at(s.at)); got != s.want {
			t.Fatalf("sum at +%v = %+v, want %+v", s.at, got, s.want)
		}
	}
}

func TestWindowJumpsAndBackwardClock(t *testing.T) {
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	var w window
	w.reset(time.Second, start) // 100ms slots

	w.add(start.Add(250*time.Millisecond), false)
	// Jumping far ahead empties every slot.
	if got := w.sum(start.Add(24 * time.Hour)); got != (tally{}) {
		t.Fatalf("sum after a day = %+v, want empty", got)
	}

	// A clock that steps back records into the newest slot, which then
	// expires as usual.
	w.add(start.Add(time.Hour), false)
	if got := w.sum(start.Add(24*time.Hour + 900*time.Millisecond)); got != (tally{requests: 1, failures: 1}) {
		t.Fatalf("sum after a backward step = %+v, want one failure", got)
	}
	if got := w.sum(start.Add(24*time.Hour + time.Second)); got != (tally{}) {
		t.Fatalf("sum one window later = %+v, want empty", got)
	}
}

func TestWindowResetChangesSlotLength(t *testing.T) {
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	var w window
	w.reset(time.Second, start)
	w.add(start, false)
	w.reset(time.Minute, start.Add(time.Second))
	if got := w.sum(start.Add(time.Second)); got != (tally{}) {
		t.Fatalf("sum after reset = %+v, want empty", got)
	}
	w.add(start.Add(time.Second), true)
	if got := w.sum(start.Add(time.Minute)); got != (tally{requests: 1}) {
		t.Fatalf("sum 59s into a 1m window = %+v, want the outcome kept", got)
	}
}
