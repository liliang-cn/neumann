package core

import (
	"context"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execModel is one model instruction: apply pending steers, call the model
// once, and describe the result as a commit set.
func (e *Env) execModel(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, fmt.Errorf("read interrupts: %w", err)
	}
	base := len(t.Regs)
	c := mem.CommitSet{Events: []proto.Event{
		proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepModel.String(), Core: core}),
	}}
	if t.Budget.MaxSteps > 0 && t.Used.Steps >= t.Budget.MaxSteps {
		return e.finish(t, c, "budget"), nil
	}
	c.Applied = applySteers(t, pending)

	resp, err := e.Model.Call(ctx, model.Request{System: t.System, Tools: t.Tools, Messages: t.Regs})
	t.Used.Steps++
	if err != nil {
		// Steers applied for this call were never answered: take them back so
		// the terminal commit reports them dropped.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		return e.fail(t, c, "model: "+err.Error()), nil
	}
	t.Used.Add(resp.Usage)
	c.Events = append(c.Events, proto.NewEvent(proto.EvUsage, t.ID, resp.Usage))

	msg := resp.Message
	msg.Role = task.RoleAssistant
	if len(msg.ToolCalls()) > 0 {
		for i := range msg.Parts {
			p := &msg.Parts[i]
			if p.Kind != task.PartToolCall {
				continue
			}
			t.CallSeq++
			p.CallID = fmt.Sprintf("c-%d", t.CallSeq)
			c.Calls = append(c.Calls, mem.CallRecord{
				TaskID: t.ID, CallID: p.CallID, Index: t.CallSeq, Tool: p.Name, Args: p.Args, Status: mem.CallPlanned,
			})
			// Args go into the event as a string: a model can emit malformed
			// JSON, and marshalling it as raw JSON would fail.
			c.Events = append(c.Events, proto.NewEvent(proto.EvToolCall, t.ID,
				map[string]any{"call_id": p.CallID, "tool": p.Name, "args": string(p.Args)}))
		}
		t.Regs = append(t.Regs, msg)
		e.ready(t, task.StepTools)
		c.Task = t
		return c, nil
	}

	t.Regs = append(t.Regs, msg)
	// A steer committed while the model was answering turns this answer into
	// a turn: the task goes on instead of finishing over it.
	late, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return c, fmt.Errorf("read interrupts: %w", err)
	}
	if late = withoutApplied(late, c.Applied); hasSteer(late) {
		c.Applied = append(c.Applied, applySteers(t, late)...)
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil
	}
	return e.finish(t, c, "end_turn"), nil
}

// applySteers appends each pending steer as a user message and returns the
// interrupt sequence numbers it consumed. Other kinds stay pending.
func applySteers(t *task.TCB, pending []mem.Interrupt) []int64 {
	var seqs []int64
	for _, in := range pending {
		if in.Kind != mem.InterruptSteer {
			continue
		}
		t.Regs = append(t.Regs, task.Text(task.RoleUser, in.Text))
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
