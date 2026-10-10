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
		if r.Status != mem.CallPlanned {
			continue
		}
		r.Status = mem.CallDispatched
		r.Attempt++
		r.Key = fmt.Sprintf("%s/%s/%d", r.TaskID, r.CallID, r.Attempt)
		batch = append(batch, r)
	}
	if len(batch) == 0 {
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil, nil
	}
	c.Calls = batch
	t.State = task.Waiting
	t.Wait = task.WaitIO
	t.Version++
	c.Task = t
	return c, batch, nil
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
				}
			}()
		}
		wg.Wait()
		e.complete(commitCtx, taskID, batch, results)
	}()
}

func (e *Env) complete(ctx context.Context, taskID string, batch []mem.CallRecord, results []task.ToolResult) {
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
	msg := task.Message{Role: task.RoleTool}
	for i, r := range batch {
		res := results[i]
		r.Status = mem.CallDone
		r.Result = &res
		done[i] = r
		msg.Parts = append(msg.Parts, task.Part{
			Kind: task.PartToolResult, CallID: r.CallID, Text: results[i].Content, IsError: results[i].IsError,
		})
	}
	t.Regs = append(t.Regs, msg)
	e.ready(t, task.StepModel)
	if err := e.Commit(ctx, mem.CommitSet{Task: t, Calls: done}); err != nil {
		e.Logf("neumann: commit after tools for task %s failed; it is no longer scheduled: %v", taskID, err)
		e.Sched.Forget(taskID)
		return
	}
	e.Sched.Requeue(t)
}
