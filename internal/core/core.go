// Package core runs vCores: the loop that fetches a ready task, executes one
// instruction, commits, and only then acts on what it committed (design §5).
package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/task"
)

// Env is what every vCore shares. The machine builds one.
type Env struct {
	Store mem.Store
	Sched *sched.Sched
	Bus   *bus.Bus
	Model model.Adapter
	Clock sched.Clock

	// Commit is the only write path: commit to the store and, on success, let
	// subscribers see the events. Nothing a commit describes may happen before
	// Commit returns nil.
	Commit func(ctx context.Context, c mem.CommitSet) error
	Logf   func(format string, args ...any)

	// Halt interrupts work that is already running: model calls, tool calls.
	// A commit that already passed its halt check may still land; nothing is
	// committed for work that Halt interrupted, and that task keeps its last
	// committed state. nil means context.Background(), never halted.
	Halt context.Context

	// SteerFormat renders a steer as the user message the model sees. nil
	// sends the text as it is.
	SteerFormat func(text string) string

	// Delta receives a model step's output as it is generated, when the
	// adapter can stream. It is called on the vCore's goroutine and must not
	// block. nil: calls are not streamed.
	Delta func(proto.Delta)

	batches sync.WaitGroup
}

func (e *Env) frameSteer(text string) string {
	if e.SteerFormat == nil {
		return text
	}
	return e.SteerFormat(text)
}

type stepData struct {
	PC   string `json:"pc"`
	Core int    `json:"core"`
}

func (e *Env) halt() context.Context {
	if e.Halt == nil {
		return context.Background()
	}
	return e.Halt
}

// RunVCore is one vCore. It returns when the scheduler closes or ctx ends,
// and only ever between two instructions. Cancelling ctx stops fetching; an
// instruction already started runs to its commit, so a shutdown never loses
// or fails the task it was holding. Cancelling Halt interrupts running
// instructions instead: their results are discarded, not committed.
func (e *Env) RunVCore(ctx context.Context, class string, core int) {
	for {
		t, ok := e.Sched.Next(ctx, class)
		if !ok {
			return
		}
		e.step(e.halt(), t, core)
	}
}

// WaitBatches waits for every dispatched tool batch to hand its task back.
// Call it only after every vCore has returned.
func (e *Env) WaitBatches() { e.batches.Wait() }

func (e *Env) step(ctx context.Context, t *task.TCB, core int) {
	var (
		c     mem.CommitSet
		batch []mem.CallRecord
		err   error
	)
	if e.halt().Err() != nil {
		e.Sched.Forget(t.ID)
		return
	}
	switch t.PC {
	case task.StepModel:
		c, batch, err = e.execModel(ctx, t, core)
	case task.StepTools:
		c, batch, err = e.execTools(ctx, t, core)
	default:
		err = fmt.Errorf("instruction %s is not implemented", t.PC)
	}
	if e.halt().Err() != nil {
		// Halted mid-instruction: the result is not committed, so the task
		// keeps its last committed state for recovery to resume from.
		e.Sched.Forget(t.ID)
		return
	}
	if err != nil {
		c, batch = e.fail(t, c, err.Error()), nil
	}
	// A commit that has started is allowed to finish.
	if err := e.Commit(context.WithoutCancel(ctx), c); err != nil {
		e.Logf("neumann: commit for task %s failed; it is no longer scheduled: %v", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	id := t.ID
	allow := append([]string(nil), t.Allow...)
	deny := append([]string(nil), t.Deny...)
	e.Sched.Requeue(t) // t now belongs to the scheduler
	if len(batch) > 0 && e.halt().Err() == nil {
		e.dispatch(id, allow, deny, batch)
	}
}

// ready makes t runnable at its next instruction.
func (e *Env) ready(t *task.TCB, pc task.PC) {
	t.State = task.Ready
	t.Wait = task.WaitNone
	t.PC = pc
	t.ReadySince = e.Clock.Now()
	t.Version++
}

func (e *Env) finish(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Done
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events,
		proto.NewEvent(proto.EvAnswer, t.ID, map[string]string{"text": t.LastAnswer()}),
		proto.NewEvent(proto.EvDone, t.ID, map[string]string{"stop_reason": reason}))
	return c
}

func (e *Env) fail(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Failed
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID, map[string]string{"error": reason}))
	return c
}
