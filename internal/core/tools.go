package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execTools commits the planned calls as dispatched and the task as waiting.
// The caller dispatches the returned batch only after that commit succeeds.
// A model step dispatches its own calls; this instruction is for a task whose
// calls are planned but were never dispatched (phase-2 recovery resumes there).
func (e *Env) execTools(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []mem.CallRecord, error) {
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return mem.CommitSet{Events: []proto.Event{
			proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepTools.String(), Core: core}),
		}}, nil, fmt.Errorf("read calls: %w", err)
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
	return c, batch, nil
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
// its tool message, committed as ready, and requeued.
func (e *Env) dispatch(taskID string, allow, deny []string, batch []mem.CallRecord) {
	e.batches.Add(1)
	go func() {
		defer e.batches.Done()
		ctx := e.halt() // phase 3 adds the task's own cancel context
		commitCtx := context.WithoutCancel(ctx)
		results := make([]task.ToolResult, len(batch))
		recorded := make([]bool, len(batch)) // per-result commit succeeded
		var wg sync.WaitGroup
		for i, r := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := e.Bus.Call(ctx, r.Tool, r.Args, r.Key, allow, deny)
				if ctx.Err() != nil {
					return // halted: the record stays dispatched for recovery
				}
				results[i] = res
				r.Status = mem.CallDone
				r.Result = &res
				ev := proto.NewEvent(proto.EvToolResult, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError})
				if err := e.Commit(commitCtx, mem.CommitSet{Calls: []mem.CallRecord{r}, Events: []proto.Event{ev}}); err != nil {
					e.Logf("neumann: commit of result %s/%s failed: %v", taskID, r.CallID, err)
				} else {
					recorded[i] = true
				}
			}()
		}
		wg.Wait()
		e.complete(commitCtx, taskID, batch, results, recorded)
	}()
}

func (e *Env) complete(ctx context.Context, taskID string, batch []mem.CallRecord, results []task.ToolResult, recorded []bool) {
	t, ok := e.Sched.Checkout(taskID)
	if !ok {
		// Ownership invariant: a dispatched batch's task is in the wait set
		// until this call checks it out. Anything else is a bug.
		panic(fmt.Sprintf("core: task %s was not waiting when its tools finished", taskID))
	}
	if e.halt().Err() != nil {
		e.Sched.Forget(taskID) // records stay dispatched; recovery resumes them
		return
	}
	// The final transition carries the finished records too (an idempotent
	// upsert of what the per-result commits recorded), so the model can never
	// see a result that is not in the call log.
	done := make([]mem.CallRecord, len(batch))
	var events []proto.Event
	for i, r := range batch {
		res := results[i]
		r.Status = mem.CallDone
		r.Result = &res
		done[i] = r
		if !recorded[i] {
			// Its own commit failed, so its tool_result event was never
			// published; it goes out with the transition that records it.
			events = append(events, proto.NewEvent(proto.EvToolResult, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError}))
		}
	}
	t.Regs = append(t.Regs, toolMessage(done))
	e.ready(t, task.StepModel)
	if err := e.Commit(ctx, mem.CommitSet{Task: t, Calls: done, Events: events}); err != nil {
		e.Logf("neumann: commit after tools for task %s failed; it is no longer scheduled: %v", taskID, err)
		e.Sched.Forget(taskID)
		return
	}
	e.Sched.Requeue(t)
}

// toolMessage is the tool message for a finished round, built from its call
// records in order: each record must carry its result. Recovery rebuilds a
// round from the call log with the same function.
func toolMessage(records []mem.CallRecord) task.Message {
	msg := task.Message{Role: task.RoleTool}
	for _, r := range records {
		var res task.ToolResult
		if r.Result != nil {
			res = *r.Result
		}
		msg.Parts = append(msg.Parts, task.Part{
			Kind: task.PartToolResult, CallID: r.CallID, Text: res.Content, IsError: res.IsError,
		})
	}
	return msg
}
