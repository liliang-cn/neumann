package sched_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// NextIdle gives up after idle on the scheduler's clock, and only then: a
// task that arrives first is taken, and the idle timer is stopped.
func TestNextIdleTimesOutOnTheClock(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	type res struct {
		t        *task.TCB
		ok, idle bool
	}
	wait := func() chan res {
		ch := make(chan res, 1)
		go func() {
			t, ok, idle := s.NextIdle(context.Background(), "default", 30*time.Second)
			ch <- res{t, ok, idle}
		}()
		for clk.Timers() == 0 {
			time.Sleep(time.Millisecond)
		}
		return ch
	}

	ch := wait()
	clk.Advance(29 * time.Second)
	select {
	case r := <-ch:
		t.Fatalf("returned before its idle time: %+v", r)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Second)
	if r := <-ch; r.ok || !r.idle || r.t != nil {
		t.Fatalf("after 30s idle: %+v", r)
	}

	ch = wait()
	s.Add(tcb("a", 0, 0))
	if r := <-ch; !r.ok || r.idle || r.t.ID != "a" {
		t.Fatalf("with work: %+v", r)
	}
	if n := clk.Timers(); n != 0 {
		t.Fatalf("%d idle timers left behind", n)
	}

	ch = wait()
	s.Close()
	if r := <-ch; r.ok || r.idle {
		t.Fatalf("after Close: %+v", r)
	}
}

// OnReady runs once per task that joins a ready queue, with its class and
// with no scheduler lock held; a task joining the wait set does not run it.
func TestOnReadyRunsForReadyTasks(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	var mu sync.Mutex
	var got []string
	s.OnReady(func(class string) {
		n := s.ReadyLen(class) // would deadlock under the lock
		mu.Lock()
		got = append(got, class)
		mu.Unlock()
		if n == 0 {
			t.Errorf("OnReady(%s) with an empty queue", class)
		}
	})
	s.Add(tcb("a", 0, 0))
	b := tcb("b", 0, 0)
	b.Class = "big"
	s.Add(b)
	w := tcb("w", 0, 0)
	w.State, w.Wait = task.Waiting, task.WaitGroup
	s.Add(w)
	if len(got) != 2 || got[0] != "default" || got[1] != "big" {
		t.Fatalf("OnReady calls %v", got)
	}
	if s.ReadyLen("default") != 1 || s.ReadyLen("big") != 1 || s.ReadyLen("none") != 0 {
		t.Fatal("ReadyLen")
	}
}

// Take finds a task in a ready queue or the wait set, when ok says so, and
// never one held elsewhere.
func TestTakeFromEitherPlace(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	s.Add(tcb("r", 0, 0))
	w := tcb("w", 1, time.Second)
	w.State, w.Wait = task.Waiting, task.WaitGroup
	s.Add(w)
	no := func(*task.TCB) bool { return false }
	yes := func(*task.TCB) bool { return true }
	if _, ok := s.Take("r", no); ok {
		t.Fatal("took a task ok refused")
	}
	q := s.Queues()["default"]
	if q.Ready != 1 || q.Waiting != 1 || q.ForGroup != 1 || !q.Oldest.Equal(t0) {
		t.Fatalf("queues %+v", q)
	}
	for _, id := range []string{"r", "w"} {
		got, ok := s.Take(id, yes)
		if !ok || got.ID != id {
			t.Fatalf("Take(%s) = %v, %v", id, got, ok)
		}
		if _, ok := s.Take(id, yes); ok {
			t.Fatalf("took %s twice", id)
		}
	}
	if r, w, h := s.Counts(); r != 0 || w != 0 || h != 2 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
	if len(s.Queues()) != 0 {
		t.Fatalf("queues %+v", s.Queues())
	}
}
