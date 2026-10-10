package core

import (
	"context"
	"fmt"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Stop reasons of a cancelled task; a cancel interrupt carries one as its
// Text.
const (
	ReasonCancelled = "cancelled" // an outside Cancel: the task ends Cancelled
	ReasonDeadline  = "deadline"  // its deadline passed: the task ends Failed
)

// Cancel commits a cancel for a task, then signals it (design §9.5): the
// task's context ends, so a model call or tool call in flight for it stops at
// once. A task nobody is at work on (ready, held for reconciliation,
// suspended) is finalised here before Cancel returns; any other is finalised
// by its owner, the vCore or tool batch holding it, when it notices. The store
// refuses a cancel for an unknown task (mem.ErrNotFound) or a finished one
// (mem.ErrFinished).
//
// A task that commits its last state while the cancel is being committed ends
// as it ended; the cancel is then applied with no effect.
func (e *Env) Cancel(ctx context.Context, id, reason string) error {
	in := mem.Interrupt{TaskID: id, Kind: mem.InterruptCancel, Text: reason}
	if err := e.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
		return err
	}
	if t, ok := e.Sched.Cancel(id); ok {
		e.finalise(context.WithoutCancel(ctx), t, mem.CommitSet{}, nil)
	}
	return nil
}

// cancelReason returns the reason of the first pending cancel, if any.
func cancelReason(pending []mem.Interrupt) (string, bool) {
	for _, in := range pending {
		if in.Kind == mem.InterruptCancel {
			if in.Text == "" {
				return ReasonCancelled, true
			}
			return in.Text, true
		}
	}
	return "", false
}

// stopping reports whether t must not run another instruction: a cancel is
// pending, or its deadline has passed (its timer may not have run yet, and a
// task that has run out of time must not start more work).
func (e *Env) stopping(t *task.TCB, pending []mem.Interrupt) (reason string, ok bool) {
	if reason, ok := cancelReason(pending); ok {
		return reason, true
	}
	if d := t.Budget.Deadline; !d.IsZero() && !e.Clock.Now().Before(d) {
		return ReasonDeadline, true
	}
	return "", false
}

// fate is what the owner of a cancelled task knows about one of its
// dispatched calls when it writes the final commit.
type fate struct {
	notRun   bool             // it never reached its device
	result   *task.ToolResult // it came back before the final commit
	recorded bool             // and its own commit landed
}

// cancelSet completes c into the final commit of a cancelled task. The caller
// owns t, whose Version is the stored one. The reason is the first pending
// cancel's, or fallback when none is pending.
//
//   - planned calls become cancelled: they were never dispatched;
//   - a dispatched call with its result in hand is done; one known never to
//     have reached its device is cancelled; any other ran, or may have, and is
//     unknown, with a reconcile_needed event marked cancelled: the record is
//     left for an operator, and the task does not wait for it;
//   - the task ends Cancelled, or Failed for a deadline, with the cancel's
//     reason as StopReason. The store reports each undelivered steer as
//     steer_dropped in the same commit.
func (e *Env) cancelSet(ctx context.Context, t *task.TCB, c mem.CommitSet, fates map[string]fate, fallback string) (mem.CommitSet, error) {
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return c, fmt.Errorf("read calls: %w", err)
	}
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return c, fmt.Errorf("read interrupts: %w", err)
	}
	reason, ok := cancelReason(pending)
	if !ok {
		reason = fallback
	}
	for _, r := range recs {
		f, known := fates[r.CallID]
		switch r.Status {
		case mem.CallPlanned:
			r.Status = mem.CallCancelled
		case mem.CallDispatched:
			switch {
			case known && f.result != nil:
				r.Status, r.Result = mem.CallDone, f.result
				if !f.recorded {
					c.Events = append(c.Events, proto.NewEvent(proto.EvToolResult, t.ID,
						map[string]any{"call_id": r.CallID, "is_error": f.result.IsError}))
				}
			case known && f.notRun:
				r.Status = mem.CallCancelled
			default:
				r.Status = mem.CallUnknown
				c.Events = append(c.Events, proto.NewEvent(proto.EvReconcileNeeded, t.ID, map[string]any{
					"call_id": r.CallID, "tool": r.Tool, "device": r.Device, "args": string(r.Args), "key": r.Key,
					"cancelled": true,
				}))
			}
		default:
			continue
		}
		c.Calls = upsert(c.Calls, r)
	}
	t.Wait = task.WaitNone
	t.StopReason = reason
	t.Version++
	if reason == ReasonDeadline {
		t.State = task.Failed
		c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID,
			map[string]string{"error": "deadline exceeded", "stop_reason": reason}))
	} else {
		t.State = task.Cancelled
		c.Events = append(c.Events, proto.NewEvent(proto.EvCancelled, t.ID, map[string]string{"stop_reason": reason}))
	}
	c.Task = t
	return c, nil
}

// finalise commits the final state of a cancelled task the caller owns and
// gives it up. It reports whether that commit landed; if it did not, the task
// is no longer scheduled, and recovery finishes the cancel after a restart.
func (e *Env) finalise(ctx context.Context, t *task.TCB, c mem.CommitSet, fates map[string]fate) bool {
	c, err := e.cancelSet(ctx, t, c, fates, ReasonCancelled)
	if err == nil {
		err = e.Commit(ctx, c)
	}
	if err != nil {
		e.Logf("neumann: final commit of cancelled task %s failed; it is no longer scheduled: %v", t.ID, err)
		e.Sched.Forget(t.ID)
		return false
	}
	e.Sched.Requeue(t) // terminal: forgotten
	return true
}

// handBack returns t to the scheduler after its holder committed t's state.
// When the task was cancelled while it was held, the scheduler refuses it and
// handBack finalises it instead; unsent are calls that the holder's commit
// recorded as dispatched but that never reached the bus. It reports whether
// t went back as committed: false means the holder must not act on that
// commit (dispatch nothing).
func (e *Env) handBack(ctx context.Context, t *task.TCB, unsent []mem.CallRecord) bool {
	if e.Sched.Requeue(t) {
		return true
	}
	fates := make(map[string]fate, len(unsent))
	for _, r := range unsent {
		fates[r.CallID] = fate{notRun: true}
	}
	e.finalise(context.WithoutCancel(ctx), t, mem.CommitSet{}, fates)
	return false
}

// HandBack is handBack for the machine's own holders (an operator's Resolve).
func (e *Env) HandBack(ctx context.Context, t *task.TCB, unsent []mem.CallRecord) bool {
	return e.handBack(ctx, t, unsent)
}

// lateFor records what a device returns after its task was cancelled and its
// call given up on: the call record becomes late with the result, a tool_late
// event is published, and the registers never see it. It waits for the
// task's final commit, so the late record is never overwritten by it.
func (e *Env) lateFor(taskID string, r mem.CallRecord, finalised <-chan struct{}) func(task.ToolResult) {
	return func(res task.ToolResult) {
		select {
		case <-finalised:
		case <-e.halt().Done():
			return
		}
		if e.halt().Err() != nil {
			return
		}
		r.Status, r.Result = mem.CallLate, &res
		ev := proto.NewEvent(proto.EvToolLate, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError})
		err := e.Commit(context.WithoutCancel(e.halt()), mem.CommitSet{Calls: []mem.CallRecord{r}, Events: []proto.Event{ev}})
		if err != nil && e.halt().Err() == nil {
			e.Logf("neumann: commit of late result %s/%s failed: %v", taskID, r.CallID, err)
		}
	}
}
