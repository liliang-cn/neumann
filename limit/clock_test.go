package limit_test

import (
	"sync"
	"time"
)

// fakeClock is a manual clock: time moves only when a test says so, and a
// timer fires only when time is moved past it.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), ch: ch})
	return ch
}

// Pending is the number of timers that have not fired yet.
func (c *fakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.fireLocked()
}

// AdvanceToNext moves time to the earliest pending timer and fires it. It
// reports false when no timer is pending.
func (c *fakeClock) AdvanceToNext() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers) == 0 {
		return false
	}
	next := c.timers[0].at
	for _, t := range c.timers[1:] {
		if t.at.Before(next) {
			next = t.at
		}
	}
	if next.After(c.now) {
		c.now = next
	}
	c.fireLocked()
	return true
}

func (c *fakeClock) fireLocked() {
	keep := c.timers[:0]
	for _, t := range c.timers {
		if t.at.After(c.now) {
			keep = append(keep, t)
			continue
		}
		t.ch <- c.now
	}
	c.timers = keep
}
