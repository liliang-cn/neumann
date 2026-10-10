package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// A batch whose task's deadline passed (clock-expired; the timer has not
// committed a "deadline" cancel yet) finalises with fallback "deadline". A
// user Cancel committed between cancelSet's read of the interrupts and the
// final commit makes the store demand StopReason "cancelled": the final
// commit is refused, finalise Forgets the task, and nobody finalises it.
func TestReview2DeadlineFinaliseRefusedByUserCancelWedges(t *testing.T) {
	clk := sim.NewClock(time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC))
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "slow", Effect: bus.ReadOnly}, func(context.Context, bus.DeviceCall) (string, error) {
		clk.Advance(2 * time.Hour) // the deadline passes while the tool runs
		return "ok", nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_slow"), nil }, dev)
	e.Clock = clk
	e.Sched = sched.New(clk, sched.DefaultAging) // no OnDeadline handler: the timer commits nothing
	inner := e.Commit
	var refused error
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && c.Task.State == task.Failed && c.Task.StopReason == ReasonDeadline {
			// Env.Cancel by a user lands now: commit, then signal.
			if err := inner(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled}}}); err != nil {
				t.Fatal(err)
			}
			if _, ok := e.Sched.Cancel("t1"); ok {
				t.Error("Sched.Cancel took a task the batch holds")
			}
			err := inner(ctx, c)
			refused = err
			return err
		}
		return inner(ctx, c)
	}
	tc := newTask(t, e, store, "q")
	tc.Budget.Deadline = clk.Now().Add(time.Hour)
	tc.Version = 2
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	e.Sched.Add(tc)
	e.step(bg, take(t, e), 0)
	e.WaitBatches()
	got := load(t, store)
	r, w, h := e.Sched.Counts()
	t.Logf("final commit err: %v; stored task %s/%s; sched ready=%d waiting=%d held=%d", refused, got.State, got.Wait, r, w, h)
	if errors.Is(refused, mem.ErrCancelPending) && !got.State.Terminal() && r+w+h == 0 {
		t.Errorf("BUG: committed cancel left the task %s and unscheduled until a restart", got.State)
	}
}
