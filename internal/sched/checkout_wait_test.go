package sched

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/task"
)

// An operator waiting for a task to be handed back gives up as soon as it is
// handed back in a state it cannot take, even when another holder takes the
// task again before the operator wakes: it does not wait through that
// holder's whole instruction.
func TestCheckoutWaitGivesUpOnceTheTaskIsHandedBackElsewhere(t *testing.T) {
	s := New(RealClock{}, 0)
	tc := &task.TCB{ID: "t1", State: task.Waiting, Wait: task.WaitApproval, Class: "default"}
	s.Add(tc)
	held, ok := s.Checkout("t1") // another operator holds it
	if !ok {
		t.Fatal("checkout")
	}
	awaiting := func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == task.WaitApproval }
	got := make(chan bool, 1)
	go func() {
		_, ok := s.CheckoutWait(context.Background(), "t1", awaiting)
		got <- ok
	}()
	time.Sleep(20 * time.Millisecond) // the waiter is asleep on the condition
	// The holder hands it back Ready and a vCore takes it, before the waiter
	// gets the lock back.
	s.mu.Lock()
	held.State, held.Wait = task.Ready, task.WaitNone
	s.requeueLocked(held)
	if s.pickLocked("default") == nil {
		s.mu.Unlock()
		t.Fatal("the vCore found nothing")
	}
	s.mu.Unlock()
	select {
	case ok := <-got:
		if ok {
			t.Fatal("CheckoutWait took a task that is no longer waiting for approval")
		}
	case <-time.After(time.Second):
		t.Fatal("CheckoutWait went on waiting while the task ran another instruction")
	}
}
