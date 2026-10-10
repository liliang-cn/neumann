package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execModel is one model instruction: apply pending steers, call the model
// once, and describe the result as a commit set. When the model asks for
// tools, the same commit records the calls as dispatched and the task as
// waiting on I/O, and the returned batch is dispatched right after it: a tool
// round does not queue for a vCore a second time just to hand calls over.
func (e *Env) execModel(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []mem.CallRecord, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, nil, fmt.Errorf("read interrupts: %w", err)
	}
	base := len(t.Regs)
	var c mem.CommitSet
	if t.Budget.MaxSteps > 0 && t.Used.Steps >= t.Budget.MaxSteps {
		// Not an instruction: no model call happens, so no step event.
		return e.finish(t, c, "budget"), nil, nil
	}
	c.Events = append(c.Events, proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepModel.String(), Core: core}))
	c.Applied = applySteers(t, pending, e.frameSteer)

	resp, err := e.callModel(ctx, model.Request{System: t.System, Tools: t.Tools, Messages: t.Regs})
	t.Used.Steps++
	if err != nil {
		// Steers applied for this call were never answered: take them back so
		// the terminal commit reports them dropped.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		reason := "model: " + err.Error()
		var p *panicError
		if errors.As(err, &p) {
			reason = err.Error()
		}
		return e.fail(t, c, reason), nil, nil
	}
	t.Used.Add(resp.Usage)
	c.Events = append(c.Events, proto.NewEvent(proto.EvUsage, t.ID, resp.Usage))

	msg := resp.Message
	msg.Parts = slices.Clone(msg.Parts) // the adapter's slice is not ours to write
	msg.Role = task.RoleAssistant
	if len(msg.ToolCalls()) > 0 {
		var batch []mem.CallRecord
		for i := range msg.Parts {
			p := &msg.Parts[i]
			if p.Kind != task.PartToolCall {
				continue
			}
			t.CallSeq++
			p.CallID = fmt.Sprintf("c-%d", t.CallSeq)
			batch = append(batch, dispatched(mem.CallRecord{
				TaskID: t.ID, CallID: p.CallID, Index: t.CallSeq, Tool: p.Name, Args: p.Args, Status: mem.CallPlanned,
			}))
			// Args go into the event as a string: a model can emit malformed
			// JSON, and marshalling it as raw JSON would fail.
			c.Events = append(c.Events, proto.NewEvent(proto.EvToolCall, t.ID,
				map[string]any{"call_id": p.CallID, "tool": p.Name, "args": string(p.Args)}))
		}
		t.Regs = append(t.Regs, msg)
		waitIO(t)
		c.Calls = batch
		c.Task = t
		return c, batch, nil
	}

	t.Regs = append(t.Regs, msg)
	// A steer committed while the model was answering turns this answer into
	// a turn: the task goes on instead of finishing over it. The steer stays
	// pending: the next model step applies it at its start and sends it, or
	// that step's terminal commit reports it dropped.
	late, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return c, nil, fmt.Errorf("read interrupts: %w", err)
	}
	if late = withoutApplied(late, c.Applied); hasSteer(late) {
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil, nil
	}
	return e.finish(t, c, "end_turn"), nil, nil
}

// panicError is a model adapter's panic, recovered into a failure of its task.
type panicError struct{ v any }

func (p *panicError) Error() string { return fmt.Sprintf("model panicked: %v", p.v) }

// callModel calls the adapter; a panic in it fails the task, not the process.
func (e *Env) callModel(ctx context.Context, req model.Request) (resp model.Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp, err = model.Response{}, &panicError{v: r}
		}
	}()
	return e.Model.Call(ctx, req)
}

// applySteers appends each pending steer as a user message, rendered by
// frame, and returns the interrupt sequence numbers it consumed. Other kinds
// stay pending.
func applySteers(t *task.TCB, pending []mem.Interrupt, frame func(string) string) []int64 {
	var seqs []int64
	for _, in := range pending {
		if in.Kind != mem.InterruptSteer {
			continue
		}
		t.Regs = append(t.Regs, task.Text(task.RoleUser, frame(in.Text)))
		seqs = append(seqs, in.Seq)
	}
	return seqs
}

func withoutApplied(list []mem.Interrupt, applied []int64) []mem.Interrupt {
	var out []mem.Interrupt
	for _, in := range list {
		if !slices.Contains(applied, in.Seq) {
			out = append(out, in)
		}
	}
	return out
}

func hasSteer(list []mem.Interrupt) bool {
	return slices.ContainsFunc(list, func(in mem.Interrupt) bool { return in.Kind == mem.InterruptSteer })
}
