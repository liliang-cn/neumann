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
// again with the hand-back count moved, and gives up, though the task is
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
			got, ok := s.CheckoutWait(context.Background(), "t1", awaiting)
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
