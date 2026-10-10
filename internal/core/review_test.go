package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// (A) Env.Cancel commits its interrupt, then (a moment later) Sched.Cancel.
// A vCore whose model call returned in that window commits and dispatches a
// side-effect call that the committed cancel should have prevented.
func TestReviewSideEffectDispatchedAfterCancelCommitted(t *testing.T) {
	var runs atomic.Int32
	entered := make(chan struct{}, 1)
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "pay"}, func(context.Context, bus.DeviceCall) (string, error) {
		runs.Add(1)
		entered <- struct{}{}
		return "paid", nil
	})
	interruptIn := make(chan struct{})
	var e *Env
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		go e.Cancel(bg, "t1", ReasonCancelled)
		<-interruptIn // the cancel is durable before the model answers
		return sim.CallTools("dev_pay"), nil
	}, dev)
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if err := inner(ctx, c); err != nil {
			return err
		}
		if len(c.NewInterrupts) > 0 && c.NewInterrupts[0].Kind == mem.InterruptCancel {
			close(interruptIn)
			select { // Env.Cancel is slow to reach Sched.Cancel (scheduling, group commit return)
			case <-entered:
			case <-time.After(2 * time.Second):
			}
		}
		return nil
	}
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	waitFor(t, "terminal", func() bool { return load(t, store).State.Terminal() })
	e.WaitBatches()
	got := load(t, store)
	t.Logf("task %s %q, device ran %d, record %+v", got.State, got.StopReason, runs.Load(), callsOf(t, store)["c-1"])
	if runs.Load() != 0 {
		t.Errorf("BUG: side-effect call ran %d times although the cancel was committed before the call was planned", runs.Load())
	}
}

// (B) The vCore re-reads pending interrupts after a final answer, sees the
// committed cancel, and finishes Done over it.
func TestReviewFinishIgnoresCancelItReads(t *testing.T) {
	var e *Env
	var store *mem.Memory
	e, store, _ = newTestEnv(t, func(model.Request) (model.Response, error) {
		// Env.Cancel's first half: committed, Sched.Cancel not reached yet.
		if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled}}}); err != nil {
			t.Error(err)
		}
		return sim.Reply("answer"), nil
	})
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	got := load(t, store)
	t.Logf("task %s %q", got.State, got.StopReason)
	if got.State != task.Cancelled {
		t.Errorf("BUG-ish: committed cancel was read at line model.go:111 and ignored; task ended %s", got.State)
	}
}

// (C) A recovered re-send of a dispatched Idempotent call (it may have run
// before the crash) is cancelled before the bus sends it again: it is
// recorded "cancelled" (never reached its device), with no reconcile_needed.
func TestReviewRecoveredResendMarkedNeverRan(t *testing.T) {
	var runs atomic.Int32
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "set", Effect: bus.Idempotent}, func(context.Context, bus.DeviceCall) (string, error) {
		runs.Add(1)
		return "set", nil
	})
	e, store, _ := newTestEnv(t, reply, dev)
	crashSite(t, store, mem.CallRecord{Tool: "dev_set", Status: mem.CallDispatched, Attempt: 1, Effect: task.Idempotent, Device: "dev"})
	tc := load(t, store)
	batch, err := e.Recover(bg, tc)
	if err != nil || len(batch) != 1 {
		t.Fatalf("batch %v err %v", batch, err)
	}
	e.Sched.Add(tc)
	// e.g. the deadline timer armed by Add fires before Start dispatches
	if err := e.Cancel(bg, "t1", ReasonDeadline); err != nil {
		t.Fatal(err)
	}
	e.Dispatch("t1", nil, nil, batch)
	e.WaitBatches()
	r := callsOf(t, store)["c-1"]
	t.Logf("task %s, record %s attempt %d, runs this process %d, reconcile_needed events %d",
		load(t, store).State, r.Status, r.Attempt, runs.Load(), len(eventsOfKind(store, "reconcile_needed")))
	if r.Status == mem.CallCancelled {
		t.Errorf("BUG: call whose attempt 1 was sent before the crash is recorded cancelled (never reached its device)")
	}
}
