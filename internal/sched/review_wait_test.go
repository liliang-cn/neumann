package sched

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/task"
)

// Three operators approve three held calls of one round at once. A holds the
// task; B and C wait. A hands it back still Waiting(Approval) (calls remain
// held). Whichever of B and C wakes first takes it; the other sees it held
// again with the hand-back count moved, and must wait again: the task is
// still awaiting approval and will be handed back in that state.
func TestConcurrentApprovalsAllGetTheTask(t *testing.T) {
	s := New(RealClock{}, 0)
	awaiting := func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == task.WaitApproval }
	s.Add(&task.TCB{ID: "t1", State: task.Waiting, Wait: task.WaitApproval, Class: "default"})
	a, ok := s.CheckoutIf("t1", awaiting)
	if !ok {
		t.Fatal("A")
	}
	res := make(chan bool, 2)
	for range 2 {
		go func() {
			got, ok := s.CheckoutWait(context.Background(), "t1", s.Backs("t1"), awaiting)
			if ok {
				time.Sleep(10 * time.Millisecond) // decide its call
				s.Requeue(got)                    // still awaiting: one call left
			}
			res <- ok
		}()
	}
	time.Sleep(20 * time.Millisecond)
	s.Requeue(a) // A decided its call; still awaiting
	r1, r2 := <-res, <-res
	t.Logf("B ok=%v C ok=%v", r1, r2)
	if !r1 || !r2 {
		t.Errorf("a concurrent approval gave up while the task was still awaiting approval")
	}
}

// Five concurrent approvals; the last hand-back leaves the round decided
// (Ready), so the ones still waiting must give up rather than hang.
func TestFiveApprovalsThenReadyAllReturn(t *testing.T) {
	s := New(RealClock{}, 0)
	awaiting := func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == task.WaitApproval }
	s.Add(&task.TCB{ID: "t1", State: task.Waiting, Wait: task.WaitApproval, Class: "default"})
	a, _ := s.CheckoutIf("t1", awaiting)
	res := make(chan bool, 4)
	left := 3 // calls still held after A's
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	for range 4 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, ok := s.CheckoutWait(ctx, "t1", s.Backs("t1"), awaiting)
			if ok {
				<-mu
				left--
				if left == 0 {
					got.State, got.Wait = task.Ready, task.WaitNone
				}
				mu <- struct{}{}
				s.Requeue(got)
			}
			res <- ok
		}()
	}
	time.Sleep(10 * time.Millisecond)
	s.Requeue(a)
	oks := 0
	start := time.Now()
	for range 4 {
		if <-res {
			oks++
		}
	}
	if oks != 3 || time.Since(start) > 2*time.Second {
		t.Errorf("oks=%d after %v; want 3 promptly", oks, time.Since(start))
	}
}

// The task was handed back Ready and taken by a vCore after the caller took
// its snapshot (and read a stale state from the store), before it entered
// CheckoutWait: it gives up at once instead of waiting for the vCore.
func TestEntryAfterAReadyHandBackGivesUp(t *testing.T) {
	s := New(RealClock{}, 0)
	awaiting := func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == task.WaitApproval }
	s.Add(&task.TCB{ID: "t1", State: task.Waiting, Wait: task.WaitApproval, Class: "default"})
	a, _ := s.CheckoutIf("t1", awaiting)
	since := s.Backs("t1") // Approve's snapshot, before its stale read of the store
	a.State, a.Wait = task.Ready, task.WaitNone
	s.Requeue(a)
	if _, ok := s.Next(context.Background(), "default"); !ok {
		t.Fatal("vCore")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, ok := s.CheckoutWait(ctx, "t1", since, awaiting)
	t.Logf("ok=%v after %v", ok, time.Since(start))
	if time.Since(start) > 200*time.Millisecond {
		t.Errorf("waited for the vCore's instruction (until ctx ended)")
	}
}
