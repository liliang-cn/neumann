package sched_test

import (
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func backoff(id string, at time.Time) *task.TCB {
	tc := waiting(id, task.WaitBackoff)
	tc.WakeAt = at
	return tc
}

// wakes records the ids OnWake was called with.
type wakes struct {
	mu  sync.Mutex
	ids []string
}

func (w *wakes) add(id string) { w.mu.Lock(); w.ids = append(w.ids, id); w.mu.Unlock() }
func (w *wakes) get() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.ids...)
}

// A task in backoff gets a wake-up timer when it enters the wait set; the
// timer fires at WakeAt, not before, and leaves the task where it is: the
// wake handler checks it out and commits it Ready.
func TestBackoffTimerFiresAtWakeAt(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	var w wakes
	s.OnWake(w.add)
	s.Add(backoff("b", t0.Add(4*time.Second)))
	clk.Advance(3 * time.Second)
	if got := w.get(); len(got) != 0 {
		t.Fatalf("woke early: %v", got)
	}
	clk.Advance(time.Second)
	if got := w.get(); len(got) != 1 || got[0] != "b" {
		t.Fatalf("wakes %v", got)
	}
	if _, wt, _ := s.Counts(); wt != 1 {
		t.Fatal("the timer must not move the task by itself")
	}
}

// Whoever takes the task out of the wait set first stops its timer: a cancel,
// a checkout, a stop.
func TestBackoffTimerStopsWhenTheTaskLeaves(t *testing.T) {
	for _, leave := range []string{"cancel", "checkout", "close"} {
		clk := sim.NewClock(t0)
		s := sched.New(clk, 0)
		var w wakes
		s.OnWake(w.add)
		s.Add(backoff("b", t0.Add(time.Second)))
		switch leave {
		case "cancel":
			s.Cancel("b")
		case "checkout":
			s.Checkout("b")
		case "close":
			s.Close()
		}
		if clk.Timers() != 0 {
			t.Fatalf("%s: %d timers left", leave, clk.Timers())
		}
		clk.Advance(time.Minute)
		if got := w.get(); len(got) != 0 {
			t.Fatalf("%s: woke %v", leave, got)
		}
	}
}

// WaitingFor lists the tasks of a class in one kind of wait, oldest first.
func TestWaitingForListsOneWaitOfOneClass(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	a := waiting("a", task.WaitGroup)
	a.ReadySince = t0.Add(2 * time.Second)
	b := waiting("b", task.WaitGroup)
	b.ReadySince = t0.Add(time.Second)
	c := waiting("c", task.WaitGroup)
	c.Class = "other"
	s.Add(a)
	s.Add(b)
	s.Add(c)
	s.Add(waiting("d", task.WaitReconcile))
	got := s.WaitingFor("default", task.WaitGroup)
	if len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Fatalf("WaitingFor %v", got)
	}
}
