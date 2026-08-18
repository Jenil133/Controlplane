package breaker

import "time"

// window counts call outcomes over a rolling period kept as windowSlots
// slots of equal length: slot number n covers [start+n*slot, start+(n+1)*slot)
// and only the windowSlots newest slot numbers are kept. Expiring whole slots
// keeps memory and work per call constant whatever the traffic, at the cost
// of outcomes expiring up to one slot early.
type window struct {
	slot  time.Duration
	start time.Time
	head  int64 // number of the newest slot
	slots [windowSlots]tally
}

// tally counts outcomes.
type tally struct {
	requests, failures uint64
}

// reset empties the window and makes it cover length, starting at now.
func (w *window) reset(length time.Duration, now time.Time) {
	*w = window{slot: length / windowSlots, start: now}
}

// roll moves the window forward to now, emptying the slots it leaves
// behind. A clock that went backwards stays in the newest slot.
func (w *window) roll(now time.Time) {
	n := int64(now.Sub(w.start) / w.slot)
	if n <= w.head {
		return
	}
	if n-w.head >= windowSlots {
		w.slots = [windowSlots]tally{}
	} else {
		for i := w.head + 1; i <= n; i++ {
			w.slots[i%windowSlots] = tally{}
		}
	}
	w.head = n
}

// add records one outcome at now.
func (w *window) add(now time.Time, success bool) {
	w.roll(now)
	t := &w.slots[w.head%windowSlots]
	t.requests++
	if !success {
		t.failures++
	}
}

// sum returns the outcomes recorded in the window that ends at now.
func (w *window) sum(now time.Time) tally {
	w.roll(now)
	var total tally
	for _, t := range w.slots {
		total.requests += t.requests
		total.failures += t.failures
	}
	return total
}
