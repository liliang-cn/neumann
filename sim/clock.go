package sim

import (
	"slices"
	"sync"
	"time"
)

// Clock is a fake clock that moves only when told to (Advance, Set). It
// satisfies any interface{ Now() time.Time }, and runs timers too: a timer set
// with AfterFunc fires when the clock reaches it, on the goroutine that moved
// the clock, in the order the timers fall due. A timer that is already due
// when it is set fires at once on a goroutine of its own, as time.AfterFunc
// does.
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	seq    int
	timers []*fakeTimer
}

type fakeTimer struct {
	at  time.Time
	seq int
	f   func()
}

// NewClock returns a clock reading start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc runs f once the clock has advanced by d. stop cancels it and
// reports whether it had not fired yet.
func (c *Clock) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	if d <= 0 {
		go f()
		return func() bool { return false }
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	tm := &fakeTimer{at: c.now.Add(d), seq: c.seq, f: f}
	c.timers = append(c.timers, tm)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		i := slices.Index(c.timers, tm)
		if i < 0 {
			return false
		}
		c.timers = slices.Delete(c.timers, i, i+1)
		return true
	}
}

// Advance moves the clock forward by d and runs every timer that falls due,
// returning after they have run.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.set(c.now.Add(d))
}

// Set moves the clock to t, forward or back, and runs every timer that is due
// at t, returning after they have run.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	c.set(t)
}

// set moves the clock with c.mu held, releases it, and runs the due timers.
func (c *Clock) set(t time.Time) {
	c.now = t
	var due []*fakeTimer
	c.timers = slices.DeleteFunc(c.timers, func(tm *fakeTimer) bool {
		if tm.at.After(c.now) {
			return false
		}
		due = append(due, tm)
		return true
	})
	c.mu.Unlock()
	slices.SortFunc(due, func(a, b *fakeTimer) int {
		if a.at.Equal(b.at) {
			return a.seq - b.seq
		}
		return a.at.Compare(b.at)
	})
	for _, tm := range due {
		tm.f()
	}
}

// Timers is how many timers are set and have not fired.
func (c *Clock) Timers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
