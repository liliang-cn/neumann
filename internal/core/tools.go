package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execTools commits the planned calls as dispatched and the task as waiting.
// The caller dispatches the returned batch only after that commit succeeds.
// A model step dispatches its own calls, and recovery dispatches through
// settle; this instruction covers a task left Ready at StepTools with planned
// calls, which no current path produces but a store may still hold.
func (e *Env) execTools(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []Send, error) {
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return mem.CommitSet{Events: []proto.Event{
			proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepTools.String(), Core: core}),
		}}, nil, fmt.Errorf("read calls: %w", err)
	}
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, nil, fmt.Errorf("read interrupts: %w", err)
	}
	if reason, ok := e.stopping(t, pending); ok {
		c, err := e.cancelSet(ctx, t, mem.CommitSet{}, nil, reason)
		return c, nil, err
	}
	c := mem.CommitSet{Events: []proto.Event{
		proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepTools.String(), Core: core}),
	}}
	var batch []mem.CallRecord
	for _, r := range recs {
		if r.Status == mem.CallPlanned {
			batch = append(batch, dispatched(r))
		}
	}
	if len(batch) == 0 {
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil, nil
	}
	c.Calls = batch
	waitIO(t)
	c.Task = t
	return c, sends(batch), nil
}

// dispatched marks a planned call as handed to the bus: one more attempt,
// under the idempotency key task/call/attempt.
func dispatched(r mem.CallRecord) mem.CallRecord {
	r.Status = mem.CallDispatched
	r.Attempt++
	r.Key = fmt.Sprintf("%s/%s/%d", r.TaskID, r.CallID, r.Attempt)
	return r
}

// waitIO parks t until its dispatched calls complete.
func waitIO(t *task.TCB) {
	t.State = task.Waiting
	t.Wait = task.WaitIO
	t.PC = task.StepTools
	t.Version++
}

// dispatch runs a committed batch off the vCore. Each result is committed as
// it arrives; when all are in, the task is checked out of the wait set, given
// its tool message, committed as ready, and requeued. The calls run under the
// task's context: when the task is cancelled, the batch finalises it instead.
// Once the task's deadline has passed, nothing more is sent: a call not yet
// handed to the bus is not sent, as if cancelled before it went out.
func (e *Env) dispatch(taskID string, allow, deny []string, batch []Send) {
	e.dispatchMu.Lock()
	if e.dispatchClosed {
		e.dispatchMu.Unlock()
		// Shutting down: the calls stay dispatched in the call log, and
		// recovery sends them when the machine starts again.
		e.Logf("neumann: not dispatching %d calls of task %s: dispatch is closed", len(batch), taskID)
		return
	}
	e.batches.Add(1)
	e.dispatchMu.Unlock()
	tctx, release := e.taskContext(taskID)
	deadline := e.Sched.Deadline(taskID)
	go func() {
		defer e.batches.Done()
		defer release()
		commitCtx := context.WithoutCancel(e.halt())
		over := make(chan struct{}) // closed once the batch has handed its task on
		var landed atomic.Bool      // a cancel's final commit landed: late results may be recorded
		defer close(over)
		b := &outcomes{
			results:  make([]task.ToolResult, len(batch)),
			got:      make([]bool, len(batch)),
			recorded: make([]bool, len(batch)),
			notSent:  make([]bool, len(batch)),
		}
		var wg sync.WaitGroup
		for i, s := range batch {
			r := s.CallRecord
			wg.Add(1)
			go func() {
				defer wg.Done()
				if e.expired(deadline) {
					b.notSent[i] = true // out of time: never handed to the bus
					return
				}
				res, how := e.Bus.Run(tctx, r.Tool, r.Args, r.Key, allow, deny, e.lateFor(taskID, r, over, &landed))
				if e.halt().Err() != nil {
					return // halted: the record stays dispatched for recovery
				}
				switch how {
				case bus.NotRun:
					b.notSent[i] = true // cancelled before it reached the device
					return
				case bus.Abandoned:
					return // cancelled while it ran: its outcome is unknown
				}
				b.results[i], b.got[i] = res, true
				r.Status = mem.CallDone
				r.Result = &res
				ev := proto.NewEvent(proto.EvToolResult, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError})
				if err := e.Commit(commitCtx, mem.CommitSet{Calls: []mem.CallRecord{r}, Events: []proto.Event{ev}}); err != nil {
					e.Logf("neumann: commit of result %s/%s failed: %v", taskID, r.CallID, err)
				} else {
					b.recorded[i] = true
				}
			}()
		}
		wg.Wait()
		landed.Store(e.complete(commitCtx, taskID, deadline, batch, b))
	}()
}

// outcomes is what a tool batch learned about each of its calls.
type outcomes struct {
	results  []task.ToolResult
	got      []bool // a result came back
	recorded []bool // ...and its own commit landed
	notSent  []bool // this send never reached the device
}

// complete hands the batch's task on: with its round finished, or, when the
// task was cancelled or ran out of time meanwhile, finalised. It reports
// whether a cancel's final commit landed.
func (e *Env) complete(ctx context.Context, taskID string, deadline time.Time, batch []Send, b *outcomes) bool {
	t, ok := e.Sched.Checkout(taskID)
	if !ok {
		// Ownership invariant: a dispatched batch's task is in the wait set
		// until this call checks it out. Anything else is a bug.
		panic(fmt.Sprintf("core: task %s was not waiting when its tools finished", taskID))
	}
	if e.halt().Err() != nil {
		e.Sched.Forget(taskID) // records stay dispatched; recovery resumes them
		return false
	}
	if e.Sched.Cancelled(taskID) || e.expired(deadline) {
		fates := make(map[string]fate, len(batch))
		for i, s := range batch {
			switch {
			case b.got[i]:
				res := b.results[i]
				fates[s.CallID] = fate{result: &res, recorded: b.recorded[i]}
			case b.notSent[i]:
				fates[s.CallID] = fate{notSent: true, sentBefore: s.SentBefore}
			}
		}
		fallback := ReasonCancelled
		if e.expired(deadline) {
			fallback = ReasonDeadline
		}
		return e.finalise(ctx, t, mem.CommitSet{}, fates, fallback)
	}
	if i := slices.Index(b.got, false); i >= 0 {
		// Only a cancel, a deadline or a halt leaves a call without a result.
		panic(fmt.Sprintf("core: call %s/%s has no result and its task was not cancelled", taskID, batch[i].CallID))
	}
	// The final transition carries the finished records too (an idempotent
	// upsert of what the per-result commits recorded), so the model can never
	// see a result that is not in the call log.
	// The round's other calls (already done, or reported unknown) come from
	// the call log: after a restart, only part of a round may be in flight.
	recs, err := e.Store.Calls(ctx, taskID)
	if err != nil {
		e.Logf("neumann: read calls of task %s after tools failed; it is no longer scheduled: %v", taskID, err)
		e.Sched.Forget(taskID)
		return false
	}
	rs := round(t, recs)
	done := make([]mem.CallRecord, len(batch))
	var events []proto.Event
	for i, s := range batch {
		r := s.CallRecord
		res := b.results[i]
		r.Status = mem.CallDone
		r.Result = &res
		done[i] = r
		if !b.recorded[i] {
			// Its own commit failed, so its tool_result event was never
			// published; it goes out with the transition that records it.
			events = append(events, proto.NewEvent(proto.EvToolResult, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError}))
		}
	}
	for _, d := range done {
		rs = upsert(rs, d)
	}
	slices.SortFunc(rs, func(a, b mem.CallRecord) int { return a.Index - b.Index })
	t.Regs = append(t.Regs, toolMessage(rs))
	e.ready(t, task.StepModel)
	c := mem.CommitSet{Task: t, Calls: done, Events: events}
	if err := e.Commit(ctx, c); err != nil {
		if errors.Is(err, mem.ErrCancelPending) {
			e.cancelInstead(ctx, t, c) // cancelled before the round was answered
			return false
		}
		e.Logf("neumann: commit after tools for task %s failed; it is no longer scheduled: %v", taskID, err)
		e.Sched.Forget(taskID)
		return false
	}
	e.handBack(ctx, t, nil)
	return false
}

// toolMessage is the tool message for a finished round, built from its call
// records in order: each record must carry its result. Recovery rebuilds a
// round from the call log with the same function.
func toolMessage(records []mem.CallRecord) task.Message {
	msg := task.Message{Role: task.RoleTool}
	for _, r := range records {
		if r.Result == nil {
			// A round is assembled only when every call has a result; a
			// missing one would reach the model as an empty success.
			panic(fmt.Sprintf("core: call %s/%s has no result (status %s)", r.TaskID, r.CallID, r.Status))
		}
		res := *r.Result
		msg.Parts = append(msg.Parts, task.Part{
			Kind: task.PartToolResult, CallID: r.CallID, Text: res.Content, IsError: res.IsError,
		})
	}
	return msg
}
