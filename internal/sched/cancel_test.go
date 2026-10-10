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

func waiting(id string, w task.WaitReason) *task.TCB {
	return &task.TCB{ID: id, State: task.Waiting, Wait: w, Class: "default"}
}

func TestCancelTakesATaskNobodyIsWorkingOn(t *testing.T) {
	for _, tc := range []*task.TCB{
		tcb("ready", 0, 0),
		waiting("reconcile", task.WaitReconcile),
		waiting("backoff", task.WaitBackoff),
		waiting("group", task.WaitGroup),
		{ID: "suspended", State: task.Suspended, Class: "default"},
	} {
		s := sched.New(&clock{now: t0}, 0)
		s.Add(tc)
		ctx := s.Context(tc.ID)
		got, ok := s.Cancel(tc.ID)
		if !ok || got != tc {
			t.Fatalf("%s: Cancel did not hand the task over", tc.ID)
		}
		if ctx.Err() == nil {
			t.Fatalf("%s: its context was not cancelled", tc.ID)
		}
		if r, w, h := s.Counts(); r != 0 || w != 0 || h != 1 {
			t.Fatalf("%s: counts %d/%d/%d, want it held", tc.ID, r, w, h)
		}
		nctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		if _, ok := s.Next(nctx, "default"); ok {
			t.Fatalf("%s: still in the ready queue", tc.ID)
		}
		cancel()
		got.State = task.Cancelled
		if !s.Requeue(got) {
			t.Fatalf("%s: terminal requeue refused", tc.ID)
		}
		if r, w, h := s.Counts(); r+w+h != 0 {
			t.Fatalf("%s: left behind %d/%d/%d", tc.ID, r, w, h)
		}
	}
}

// A task waiting on I/O belongs to its tool batch: Cancel only signals it,
// and the batch finalises it when it checks the task out.
func TestCancelLeavesAnIOWaitToItsBatch(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	s.Add(waiting("io", task.WaitIO))
	ctx := s.Context("io")
	if _, ok := s.Cancel("io"); ok {
		t.Fatal("Cancel took a task whose batch is in flight")
	}
	if ctx.Err() == nil || !s.Cancelled("io") {
		t.Fatal("the batch was not told")
	}
	got, ok := s.Checkout("io")
	if !ok {
		t.Fatal("the batch cannot check its task out")
	}
	got.State, got.Wait = task.Ready, task.WaitNone
	if s.Requeue(got) {
		t.Fatal("a cancelled task was queued again as ready")
	}
	if _, _, h := s.Counts(); h != 1 {
		t.Fatal("refused requeue must leave the task with its holder")
	}
	got.State = task.Cancelled
	if !s.Requeue(got) {
		t.Fatal("terminal requeue refused")
	}
}

// A task on a vCore: Cancel signals, and the vCore's own hand-back is refused
// so that it finalises the task itself.
func TestCancelSignalsAHeldTask(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	s.Add(tcb("a", 0, 0))
	held, _ := s.Next(context.Background(), "default")
	ctx := s.Context("a")
	if _, ok := s.Cancel("a"); ok {
		t.Fatal("Cancel took a task off its vCore")
	}
	if ctx.Err() == nil {
		t.Fatal("context not cancelled")
	}
	held.State, held.Wait = task.Waiting, task.WaitIO
	if s.Requeue(held) {
		t.Fatal("hand-back of a cancelled task accepted")
	}
	held.State = task.Cancelled
	if !s.Requeue(held) {
		t.Fatal("terminal requeue refused")
	}
	if s.Cancelled("a") {
		t.Fatal("a finished task keeps its mark")
	}
}

func TestCancelOfAnUnknownTaskDoesNothing(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	if _, ok := s.Cancel("ghost"); ok || s.Cancelled("ghost") {
		t.Fatal("an unknown task was marked")
	}
	s.Add(tcb("ghost", 0, 0)) // added after: not cancelled
	if _, ok := s.Next(context.Background(), "default"); !ok {
		t.Fatal("task lost")
	}
}

func TestContextEndsWithTheBase(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	base, cancel := context.WithCancel(context.Background())
	s.SetContext(base)
	s.Add(tcb("a", 0, 0))
	ctx := s.Context("a")
	cancel()
	if ctx.Err() == nil {
		t.Fatal("task context outlived the base")
	}
}

type fired struct {
	mu  sync.Mutex
	ids []string
	ch  chan string
}

func (f *fired) fn(id string) {
	f.mu.Lock()
	f.ids = append(f.ids, id)
	f.mu.Unlock()
	f.ch <- id
}

func newFired() *fired { return &fired{ch: make(chan string, 16)} }

func (f *fired) wait(t *testing.T, want string) {
	t.Helper()
	select {
	case id := <-f.ch:
		if id != want {
			t.Fatalf("deadline fired for %s, want %s", id, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("deadline of %s never fired", want)
	}
}

func (f *fired) none(t *testing.T) {
	t.Helper()
	select {
	case id := <-f.ch:
		t.Fatalf("deadline fired for %s", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func withDeadline(tc *task.TCB, at time.Time) *task.TCB {
	tc.Budget.Deadline = at
	return tc
}

// design §9.4: the timer runs whatever state the task is in, including on a
// vCore and in the hands of a tool batch.
func TestDeadlineFiresInEveryState(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	f := newFired()
	s.OnDeadline(f.fn)

	s.Add(withDeadline(tcb("ready", 0, 0), t0.Add(1*time.Second)))
	s.Add(withDeadline(waiting("io", task.WaitIO), t0.Add(2*time.Second)))
	s.Add(withDeadline(waiting("reconcile", task.WaitReconcile), t0.Add(3*time.Second)))
	s.Add(withDeadline(&task.TCB{ID: "suspended", State: task.Suspended, Class: "default"}, t0.Add(4*time.Second)))
	s.Add(withDeadline(tcb("running", 0, time.Hour), t0.Add(5*time.Second)))
	if _, ok := s.Checkout("io"); !ok { // a tool batch holds it now
		t.Fatal("checkout")
	}

	clk.Advance(time.Second)
	f.wait(t, "ready")
	clk.Advance(time.Second)
	f.wait(t, "io")
	clk.Advance(time.Second)
	f.wait(t, "reconcile")
	clk.Advance(time.Second)
	f.wait(t, "suspended")
	held, _ := s.Next(context.Background(), "default") // "ready" (ReadySince t0) first
	held2, _ := s.Next(context.Background(), "default")
	if held.ID != "ready" || held2.ID != "running" {
		t.Fatalf("got %s, %s", held.ID, held2.ID)
	}
	clk.Advance(time.Second)
	f.wait(t, "running")
	f.none(t)
}

func TestPastDeadlineFiresAtOnce(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	f := newFired()
	s.OnDeadline(f.fn)
	s.Add(withDeadline(tcb("late", 0, 0), t0.Add(-time.Minute)))
	f.wait(t, "late")
}

func TestDeadlineStopsWhenTheTaskEnds(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	f := newFired()
	s.OnDeadline(f.fn)
	s.Add(withDeadline(tcb("a", 0, 0), t0.Add(time.Second)))
	a, _ := s.Next(context.Background(), "default")
	a.State = task.Done
	s.Requeue(a)
	if clk.Timers() != 0 {
		t.Fatal("timer still set for a finished task")
	}
	clk.Advance(time.Minute)
	f.none(t)
}

// The timer is armed once: a task going round the queues keeps its one timer.
func TestDeadlineIsArmedOnce(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	s.OnDeadline(func(string) {})
	s.Add(withDeadline(tcb("a", 0, 0), t0.Add(time.Second)))
	for range 3 {
		a, _ := s.Next(context.Background(), "default")
		s.Requeue(a)
	}
	if n := clk.Timers(); n != 1 {
		t.Fatalf("%d timers", n)
	}
}

func TestForgetDropsTimerAndContext(t *testing.T) {
	clk := sim.NewClock(t0)
	s := sched.New(clk, 0)
	s.OnDeadline(func(string) { t.Error("fired for a forgotten task") })
	s.Add(withDeadline(tcb("a", 0, 0), t0.Add(time.Second)))
	s.Next(context.Background(), "default")
	ctx := s.Context("a")
	s.Forget("a")
	if ctx.Err() == nil || clk.Timers() != 0 {
		t.Fatal("forgotten task keeps its context or timer")
	}
	clk.Advance(time.Minute)
}
