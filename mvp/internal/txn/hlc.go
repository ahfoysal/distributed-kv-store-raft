package txn

import (
	"sync"
	"time"
)

// HLC is a hybrid logical clock. Timestamps are 64-bit with the upper 48 bits
// holding physical time in milliseconds and the lower 16 bits holding a
// monotonic counter to disambiguate events within the same millisecond. This
// keeps timestamps monotonic across process restarts (because physical time
// moves forward) while still allowing a rapid burst of txns to get distinct
// strictly-increasing timestamps.
type HLC struct {
	mu     sync.Mutex
	lastPT uint64 // ms
	lastLC uint64 // logical counter
}

func NewHLC() *HLC { return &HLC{} }

// Now returns a new HLC timestamp, strictly greater than every previously
// issued timestamp from this clock.
func (h *HLC) Now() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := uint64(time.Now().UnixMilli())
	if pt > h.lastPT {
		h.lastPT = pt
		h.lastLC = 0
	} else {
		h.lastLC++
	}
	return (h.lastPT << 16) | (h.lastLC & 0xFFFF)
}

// Update advances the clock past an externally-observed timestamp. Used when
// restoring from a snapshot: we must never hand out a timestamp that was
// already committed.
func (h *HLC) Update(ts uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := ts >> 16
	lc := ts & 0xFFFF
	if pt > h.lastPT {
		h.lastPT = pt
		h.lastLC = lc
	} else if pt == h.lastPT && lc > h.lastLC {
		h.lastLC = lc
	}
}
