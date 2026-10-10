package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

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
// by its owner, the vCore or tool batch holding it. The store refuses a
// cancel for an unknown task (mem.ErrNotFound) or a finished one
// (mem.ErrFinished).
//
// The commit decides, not the signal: once the cancel is committed, the store
// refuses any later commit that would let the task go on
// (mem.ErrCancelPending), so an owner that has not been signalled yet
// finalises the task when its own commit is refused. A task whose own final
// commit landed first ends as it ended; the cancel is then applied with no
// effect.
func (e *Env) Cancel(ctx context.Context, id, reason string) error {
	in := mem.Interrupt{TaskID: id, Kind: mem.InterruptCancel, Text: reason}
	if err := e.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
		return err
	}
	if t, ok := e.Sched.Cancel(id); ok {
		e.finalise(context.WithoutCancel(ctx), t, mem.CommitSet{}, nil, ReasonCancelled)
	}
	return nil
}

// Finalise is the final commit of a cancelled task that the machine's own
// holder (an operator's Resolve) has in hand, after the store refused its
// commit with mem.ErrCancelPending.
func (e *Env) Finalise(ctx context.Context, t *task.TCB) {
	e.finalise(context.WithoutCancel(ctx), t, mem.CommitSet{}, nil, ReasonCancelled)
}

// cancelReason returns the reason of the first pending cancel, if any.
func cancelReason(pending []mem.Interrupt) (string, bool) {
	for _, in := range pending {
		if in.Kind == mem.InterruptCancel {
			return mem.CancelReason(in), true
		}
	}
	return "", false
}

// expired reports whether a deadline has passed.
func (e *Env) expired(deadline time.Time) bool {
	return !deadline.IsZero() && !e.Clock.Now().Before(deadline)
}

// stopping reports whether t must not run another instruction: a cancel is
// pending, or its deadline has passed (its timer may not have run yet, and a
// task that has run out of time must not start more work).
func (e *Env) stopping(t *task.TCB, pending []mem.Interrupt) (reason string, ok bool) {
	if reason, ok := cancelReason(pending); ok {
		return reason, true
	}
	if e.expired(t.Budget.Deadline) {
		return ReasonDeadline, true
	}
	return "", false
}

// Send is one call a tool batch hands to the bus. SentBefore: an earlier send
// of the call may have reached its device (a re-send after a restart under
// the same key, or a retry after an attempt whose outcome was unknown), so a
// send that is cancelled before it goes out does not make the call one that
// never ran.
type Send struct {
	mem.CallRecord
	SentBefore bool
}

// sends are the first sends of freshly dispatched records: only a retry
// (attempt 2 or later) has been sent before.
func sends(batch []mem.CallRecord) []Send {
	out := make([]Send, len(batch))
	for i, r := range batch {
		out[i] = Send{CallRecord: r, SentBefore: r.Attempt > 1}
	}
	return out
}

// fate is what the owner of a cancelled task knows about one of its
// dispatched calls when it writes the final commit.
type fate struct {
	notSent    bool             // this send never reached the device
	sentBefore bool             // an earlier send may have
	result     *task.ToolResult // it came back before the final commit
	recorded   bool             // and its own commit landed
}

// errCannotFinalise marks a final commit that could not be assembled (the
// store could not be read). The task is given up rather than failed: its
// cancel is committed, and recovery finishes it.
var errCannotFinalise = errors.New("cannot assemble the final commit of a cancelled task")

// cancelSet completes c into the final commit of a cancelled task. The caller
// owns t, whose Version is the stored one. The reason is the first pending
// cancel's (the store accepts no other), or fallback when none is pending.
// Records already in c are kept as they are.
//
//   - a planned call is cancelled, never dispatched; but one with an earlier
//     attempt (an operator's retry) is unknown, since that attempt may have
//     run;
//   - a dispatched call with its result in hand is done; one whose send is
//     known not to have reached its device, and that was never sent before,
//     is cancelled; any other ran, or may have, and is unknown, with a
//     reconcile_needed event marked cancelled: the record is left for an
//     operator, and the task does not wait for it;
//   - the task ends Cancelled, or Failed for a deadline, with the reason as
//     StopReason. The store reports each undelivered steer as steer_dropped
//     in the same commit.
func (e *Env) cancelSet(ctx context.Context, t *task.TCB, c mem.CommitSet, fates map[string]fate, fallback string) (mem.CommitSet, error) {
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return c, fmt.Errorf("%w: read calls: %v", errCannotFinalise, err)
	}
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return c, fmt.Errorf("%w: read interrupts: %v", errCannotFinalise, err)
	}
	reason, ok := cancelReason(pending)
	if !ok {
		reason = fallback
	}
	unknown := func(r mem.CallRecord) mem.CallRecord {
		r.Status = mem.CallUnknown
		c.Events = append(c.Events, proto.NewEvent(proto.EvReconcileNeeded, t.ID, map[string]any{
			"call_id": r.CallID, "tool": r.Tool, "device": r.Device, "args": string(r.Args), "key": r.Key,
			"cancelled": true,
		}))
		return r
	}
	for _, r := range recs {
		if slices.ContainsFunc(c.Calls, func(o mem.CallRecord) bool { return o.CallID == r.CallID }) {
			continue
		}
		f, known := fates[r.CallID]
		switch r.Status {
		case mem.CallPlanned:
			if r.Attempt > 0 {
				r = unknown(r)
			} else {
				r.Status = mem.CallCancelled
			}
		case mem.CallDispatched:
			switch {
			case known && f.result != nil:
				r.Status, r.Result = mem.CallDone, f.result
				if !f.recorded {
					c.Events = append(c.Events, proto.NewEvent(proto.EvToolResult, t.ID,
						map[string]any{"call_id": r.CallID, "is_error": f.result.IsError}))
				}
			case known && f.notSent && !f.sentBefore:
				r.Status = mem.CallCancelled
			default:
				r = unknown(r)
			}
		default:
			continue
		}
		c.Calls = append(c.Calls, r)
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
// is no longer scheduled, and recovery finishes the committed cancel after a
// restart.
func (e *Env) finalise(ctx context.Context, t *task.TCB, c mem.CommitSet, fates map[string]fate, fallback string) bool {
	e.startCommitted(t.ID) // forget any failed starts
	c, err := e.cancelSet(ctx, t, c, fates, fallback)
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

// cancelInstead finalises t after the store refused c (mem.ErrCancelPending):
// a cancel of t was committed before it. t is still the caller's, but
// nothing c describes happened. The final commit starts from the stored task
// and keeps only what the step learned for certain: confirmed usage and
// unconfirmed requests, the call ids it handed out, results that came back,
// and its step and usage events. A call it was about to dispatch for the
// first time is recorded cancelled; one already in the call log is settled
// from what the log says.
func (e *Env) cancelInstead(ctx context.Context, t *task.TCB, c mem.CommitSet) {
	ctx = context.WithoutCancel(ctx)
	stored, err := e.Store.Load(ctx, t.ID)
	var recs []mem.CallRecord
	if err == nil {
		recs, err = e.Store.Calls(ctx, t.ID)
	}
	if err != nil {
		e.Logf("neumann: task %s was cancelled, and reading it back failed; it is no longer scheduled: %v", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	stored.Used, stored.Unconfirmed, stored.CallSeq = t.Used, t.Unconfirmed, t.CallSeq
	logged := func(id string) bool {
		return slices.ContainsFunc(recs, func(r mem.CallRecord) bool { return r.CallID == id })
	}
	var f mem.CommitSet
	for _, ev := range c.Events {
		switch ev.Kind {
		case proto.EvStep, proto.EvUsage, proto.EvToolResult:
			f.Events = append(f.Events, ev)
		}
	}
	for _, r := range c.Calls {
		switch {
		case r.Status == mem.CallDispatched && !logged(r.CallID):
			r.Status, r.Attempt, r.Key = mem.CallCancelled, 0, ""
		case r.Status == mem.CallDispatched:
			continue // settled by cancelSet from the log
		}
		f.Calls = append(f.Calls, r)
	}
	e.finalise(ctx, stored, f, nil, ReasonCancelled)
}

// handBack returns t to the scheduler after its holder committed t's state.
// When the task was cancelled while it was held, the scheduler refuses it and
// handBack finalises it instead; unsent are calls that the holder's commit
// recorded as dispatched but that never reached the bus. It reports whether
// t went back as committed: false means the holder must not act on that
// commit (dispatch nothing).
func (e *Env) handBack(ctx context.Context, t *task.TCB, unsent []Send) bool {
	if e.Sched.Requeue(t) {
		return true
	}
	fates := make(map[string]fate, len(unsent))
	for _, r := range unsent {
		fates[r.CallID] = fate{notSent: true, sentBefore: r.SentBefore}
	}
	e.finalise(context.WithoutCancel(ctx), t, mem.CommitSet{}, fates, ReasonCancelled)
	return false
}

// HandBack is handBack for the machine's own holders (an operator's Resolve).
func (e *Env) HandBack(ctx context.Context, t *task.TCB, unsent []Send) bool {
	return e.handBack(ctx, t, unsent)
}

// lateFor records what a device returns after its task was cancelled and its
// call given up on: the call record becomes late with the result, a tool_late
// event is published, and the registers never see it. It waits until the
// batch is over, and records only when the task's final commit landed, so
// that commit never overwrites it and nothing is written for a task whose
// final state is not on disk. Once CloseLate has run, nothing more is
// recorded.
func (e *Env) lateFor(taskID string, r mem.CallRecord, over <-chan struct{}, landed *atomic.Bool) func(task.ToolResult) {
	return func(res task.ToolResult) {
		e.lateMu.Lock()
		if e.lateClosed {
			e.lateMu.Unlock()
			return
		}
		e.lates.Add(1)
		e.lateMu.Unlock()
		defer e.lates.Done()
		<-over
		if !landed.Load() || e.halt().Err() != nil {
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

// CloseLate stops recording late results and waits for the ones already
// being recorded. Shutdown calls it after WaitBatches, before the store is
// handed back to its owner.
func (e *Env) CloseLate() {
	e.lateMu.Lock()
	e.lateClosed = true
	e.lateMu.Unlock()
	e.lates.Wait()
}
