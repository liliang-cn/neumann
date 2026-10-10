package sched_test

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/task"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

var t0 = time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)

func tcb(id string, prio int, at time.Duration) *task.TCB {
	return &task.TCB{ID: id, State: task.Ready, Class: "default", Priority: prio, ReadySince: t0.Add(at)}
}

func next(t *testing.T, s *sched.Sched) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, ok := s.Next(ctx, "default")
	if !ok {
		t.Fatal("Next returned nothing")
	}
	return got.ID
}

func order(t *testing.T, s *sched.Sched, want ...string) {
	t.Helper()
	for _, w := range want {
		if got := next(t, s); got != w {
			t.Fatalf("got %s, want %s", got, w)
		}
	}
}

func TestFIFOWithinPriority(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(5 * time.Second)}, 30*time.Second)
	s.Add(tcb("c", 0, 3*time.Second))
	s.Add(tcb("a", 0, 1*time.Second))
	s.Add(tcb("b", 0, 2*time.Second))
	order(t, s, "a", "b", "c")
}

func TestInteractiveBeforeBackground(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(10 * time.Second)}, 30*time.Second)
	s.Add(tcb("bg", 1, 0))
	s.Add(tcb("fg", 0, 5*time.Second))
	order(t, s, "fg", "bg")
}

func TestAgingPromotesBackground(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(31 * time.Second)}, 30*time.Second)
	s.Add(tcb("bg", 1, 0))
	s.Add(tcb("fg", 0, 30*time.Second))
	order(t, s, "bg", "fg")
}

func TestNextWaitsForWork(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	got := make(chan string, 1)
	go func() {
		if tc, ok := s.Next(context.Background(), "default"); ok {
			got <- tc.ID
		}
	}()
	select {
	case id := <-got:
		t.Fatalf("Next returned %s before any task was added", id)
	case <-time.After(20 * time.Millisecond):
	}
	s.Add(&task.TCB{ID: "x", State: task.Ready, Class: "default", ReadySince: time.Now()})
	select {
	case id := <-got:
		if id != "x" {
			t.Fatalf("got %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not wake up")
	}
}

func TestNextStopsOnCancelAndClose(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	done := make(chan bool, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, ok := s.Next(ctx, "default"); done <- ok }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Next returned a task after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("Next ignored cancel")
	}
	go func() { _, ok := s.Next(context.Background(), "default"); done <- ok }()
	time.Sleep(10 * time.Millisecond)
	s.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Next returned a task after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Next ignored Close")
	}
}

func TestCheckoutOnlyFindsWaitingTasks(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	w := &task.TCB{ID: "w", State: task.Waiting, Wait: task.WaitIO, Class: "default"}
	s.Requeue(w)
	if got, ok := s.Checkout("w"); !ok || got != w {
		t.Fatal("waiting task not checked out")
	}
	if _, ok := s.Checkout("w"); ok {
		t.Fatal("checked out twice")
	}
	s.Add(tcb("r", 0, 0))
	if _, ok := s.Checkout("r"); ok {
		t.Fatal("checked out a ready task")
	}
}

func TestClassesAreSeparate(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	o := tcb("o", 0, 0)
	o.Class = "other"
	s.Add(o)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, ok := s.Next(ctx, "default"); ok {
		t.Fatal("default class got a task of another class")
	}
	if got, ok := s.Next(context.Background(), "other"); !ok || got.ID != "o" {
		t.Fatal("other class did not get its task")
	}
}

func TestCountsAndTerminalTasksLeave(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	s.Add(tcb("a", 0, 0))
	s.Add(tcb("b", 0, time.Second))
	s.Requeue(&task.TCB{ID: "w", State: task.Waiting, Class: "default"})
	got, _ := s.Next(context.Background(), "default")
	if r, w, o := s.Counts(); r != 1 || w != 1 || o != 1 {
		t.Fatalf("counts %d/%d/%d", r, w, o)
	}
	got.State = task.Done
	s.Requeue(got)
	if r, w, o := s.Counts(); r != 1 || w != 1 || o != 0 {
		t.Fatalf("after done: %d/%d/%d", r, w, o)
	}
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	f()
}

func TestDuplicatePlacementPanics(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	a := tcb("a", 0, 0)
	s.Add(a)
	mustPanic(t, "second Add", func() { s.Add(a) })
	r := tcb("r", 0, 0)
	s.Add(r)
	mustPanic(t, "ready task requeued as waiting", func() {
		w := &task.TCB{ID: "r", State: task.Waiting, Class: "default"}
		s.Requeue(w)
	})
	w := &task.TCB{ID: "w", State: task.Waiting, Class: "default"}
	s.Requeue(w)
	mustPanic(t, "waiting task requeued again", func() { s.Requeue(w) })
}

func TestRequeueRunningPanics(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	mustPanic(t, "Requeue of Running", func() {
		s.Requeue(&task.TCB{ID: "x", State: task.Running, Class: "default"})
	})
}

func TestRequeueAfterCloseStillLands(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	s.Close()
	s.Requeue(tcb("a", 0, 0))
	if r, _, _ := s.Counts(); r != 1 {
		t.Fatalf("ready %d, want 1", r)
	}
	if _, ok := s.Next(context.Background(), "default"); ok {
		t.Fatal("Next returned a task after Close")
	}
}

func TestEqualReadySinceKeepsInsertionOrder(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(time.Second)}, 30*time.Second)
	for _, id := range []string{"a", "b", "c", "d"} {
		s.Add(tcb(id, 0, 0))
	}
	order(t, s, "a", "b", "c", "d")
}

func TestNilClockDefaultsToReal(t *testing.T) {
	s := sched.New(nil, 0)
	s.Add(tcb("a", 1, 0))
	order(t, s, "a")
}
