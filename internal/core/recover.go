package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// unknownReport is the result a model is given for a call whose outcome a
// restart made unknown, when its tool allows reporting that (ReportUnknown).
const unknownReport = "This call was sent, but the machine restarted before its result came back, " +
	"so whether it took effect is unknown. Check with a read-only tool before assuming it did or did not."

// ErrNotReconciling is returned by Resolve for a call that is not waiting
// for an operator.
var ErrNotReconciling = errors.New("not waiting for reconciliation")

// Outcome is an operator's answer for a call whose outcome is unknown.
type Outcome struct {
	Retry   bool   // send the call again (a new attempt, a new idempotency key)
	Result  string // otherwise: what the call returned, given to the model
	IsError bool   // the call failed
}

// round returns the calls of t's current tool round, in call order: the tool
// calls of the last assistant turn, when no tool message has answered it yet.
// It reads only the end of the registers, so compaction removing older turns
// cannot make an old call look current.
func round(t *task.TCB, recs []mem.CallRecord) []mem.CallRecord {
	var want map[string]bool
	for i := len(t.Regs) - 1; i >= 0; i-- {
		m := t.Regs[i]
		if m.Role == task.RoleTool {
			return nil // the last round is answered
		}
		if m.Role == task.RoleAssistant {
			want = map[string]bool{}
			for _, p := range m.ToolCalls() {
				want[p.CallID] = true
			}
			break
		}
	}
	var out []mem.CallRecord
	for _, r := range recs {
		if want[r.CallID] {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b mem.CallRecord) int { return a.Index - b.Index })
	return out
}

// Recover decides how an unfinished task resumes after a restart, commits
// that decision, and returns the calls to dispatch. The caller owns t, hands
// it to the scheduler after Recover returns nil, and only then dispatches.
//
//   - Running: a model request may have reached the provider. Counted as
//     unconfirmed; the step is done again.
//   - Waiting on I/O: the current round is settled call by call (settle).
//   - Anything else (Ready, waiting for reconciliation) resumes as it is.
//
// A task with a committed cancel is finalised instead, whatever its state.
func (e *Env) Recover(ctx context.Context, t *task.TCB) ([]Send, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return nil, err
	}
	if reason, ok := cancelReason(pending); ok {
		// A cancel was committed and its final commit was not: finish it.
		// Nothing is sent again; whatever was in flight is unknown.
		from := t.State.String()
		if t.State == task.Waiting {
			from += "_" + t.Wait.String()
		}
		if t.State == task.Running {
			t.Unconfirmed++ // a request may have been sent
		}
		ev := proto.NewEvent(proto.EvRecovered, t.ID, map[string]any{"from": from, "cancel": reason})
		c, err := e.cancelSet(ctx, t, mem.CommitSet{Events: []proto.Event{ev}}, nil, ReasonCancelled)
		if err != nil {
			return nil, err
		}
		return nil, e.Commit(ctx, c)
	}
	switch {
	case t.State == task.Running:
		t.Unconfirmed++
		e.ready(t, task.StepModel)
		ev := proto.NewEvent(proto.EvRecovered, t.ID, map[string]any{"from": "running", "unconfirmed": t.Unconfirmed})
		return nil, e.Commit(ctx, mem.CommitSet{Task: t, Events: []proto.Event{ev}})
	case t.State == task.Waiting && t.Wait == task.WaitIO:
		recs, err := e.Store.Calls(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		rs := round(t, recs)
		if err := checkRound(t, rs); err != nil {
			return nil, err
		}
		c, batch := e.settle(t, rs, "restart")
		if err := e.Commit(ctx, c); err != nil {
			return nil, err
		}
		return batch, nil
	case t.State == task.Waiting && t.Wait == task.WaitReconcile:
		// Still held from an earlier run. Announced again: an operator
		// reading events from now on must learn that it waits for them.
		recs, err := e.Store.Calls(ctx, t.ID)
		if err != nil {
			return nil, err
		}
		var held []map[string]any
		for _, r := range round(t, recs) {
			if r.Status == mem.CallUnknown && r.Result == nil {
				held = append(held, map[string]any{"call_id": r.CallID, "tool": r.Tool, "device": r.Device, "args": string(r.Args), "key": r.Key})
			}
		}
		ev := proto.NewEvent(proto.EvRecovered, t.ID, map[string]any{"from": "reconcile", "held": held})
		return nil, e.Commit(ctx, mem.CommitSet{Events: []proto.Event{ev}})
	}
	return nil, nil
}

// checkRound refuses a round whose call log this version cannot settle, so a
// damaged or foreign record stops Start with an error naming the task rather
// than a panic on every start.
func checkRound(t *task.TCB, rs []mem.CallRecord) error {
	for _, r := range rs {
		switch r.Status {
		case mem.CallPlanned, mem.CallDispatched:
		case mem.CallDone, mem.CallUnknown:
			if r.Status == mem.CallDone && r.Result == nil {
				return fmt.Errorf("task %s: call %s is done but has no result", t.ID, r.CallID)
			}
		default:
			return fmt.Errorf("task %s: call %s has status %q, which recovery cannot settle", t.ID, r.CallID, r.Status)
		}
	}
	return nil
}

// Resolve applies an operator's outcome to a call held for reconciliation,
// settles the round again, and returns the calls to dispatch. The caller
// owns t (checked out of the wait set) and requeues it after a nil error;
// on error t is unchanged.
func (e *Env) Resolve(ctx context.Context, t *task.TCB, callID string, o Outcome) ([]Send, error) {
	if t.State != task.Waiting || t.Wait != task.WaitReconcile {
		return nil, fmt.Errorf("task %s: %w", t.ID, ErrNotReconciling)
	}
	if o.Retry && (o.Result != "" || o.IsError) {
		return nil, errors.New("an outcome is either Retry or a result, not both")
	}
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	rs := round(t, recs)
	i := slices.IndexFunc(rs, func(r mem.CallRecord) bool { return r.CallID == callID })
	if i < 0 || rs[i].Status != mem.CallUnknown || rs[i].Result != nil {
		return nil, fmt.Errorf("call %s/%s: %w", t.ID, callID, ErrNotReconciling)
	}
	var ev proto.Event
	if o.Retry {
		rs[i].Status = mem.CallPlanned // settle dispatches it as a new attempt
		ev = proto.NewEvent(proto.EvResolved, t.ID, map[string]any{"call_id": callID, "retry": true})
	} else {
		rs[i].Status = mem.CallDone
		rs[i].Result = &task.ToolResult{Content: o.Result, IsError: o.IsError}
		ev = proto.NewEvent(proto.EvToolResult, t.ID, map[string]any{"call_id": callID, "is_error": o.IsError, "resolved": true})
	}
	clone := t.Clone() // settle mutates; keep t intact if the commit fails
	c, batch := e.settle(clone, rs, "resolve")
	if !slices.ContainsFunc(c.Calls, func(r mem.CallRecord) bool { return r.CallID == callID }) {
		c.Calls = append(c.Calls, rs[i]) // settle wrote it already if it sent it
	}
	c.Events = append([]proto.Event{ev}, c.Events...)
	if err := e.Commit(ctx, c); err != nil {
		return nil, err
	}
	*t = *clone
	return batch, nil
}

// settle decides what happens to a round whose calls were interrupted:
//
//   - done, or unknown with a reported result: keeps its result;
//   - planned: never sent; dispatched now as a new attempt;
//   - dispatched, ReadOnly or Idempotent: sent again under the same key;
//   - dispatched, SideEffect: its outcome is unknown. With ReportUnknown the
//     model is told so; otherwise the call is held for an operator.
//
// Any held call parks the task in Waiting(Reconcile) and nothing is sent.
// Otherwise the calls to send go out and the task waits on I/O; with nothing
// to send, the round's tool message is assembled and the task is ready.
func (e *Env) settle(t *task.TCB, rs []mem.CallRecord, why string) (mem.CommitSet, []Send) {
	var c mem.CommitSet
	var send []Send
	held := 0
	for i := range rs {
		r := &rs[i]
		switch r.Status {
		case mem.CallDone:
		case mem.CallUnknown:
			if r.Result == nil {
				held++
			}
		case mem.CallPlanned:
			send = append(send, Send{CallRecord: *r})
		case mem.CallDispatched:
			switch {
			case r.Effect == task.ReadOnly || r.Effect == task.Idempotent:
				// Sent again under the same key: it may have run already.
				send = append(send, Send{CallRecord: *r, SentBefore: true})
			case r.ReportUnknown:
				r.Status = mem.CallUnknown
				r.Result = &task.ToolResult{Content: unknownReport, IsError: true}
				c.Calls = upsert(c.Calls, *r)
				c.Events = append(c.Events, proto.NewEvent(proto.EvToolResult, t.ID,
					map[string]any{"call_id": r.CallID, "is_error": true, "unknown": true}))
			default:
				r.Status = mem.CallUnknown
				held++
				c.Calls = upsert(c.Calls, *r)
				c.Events = append(c.Events, proto.NewEvent(proto.EvReconcileNeeded, t.ID,
					map[string]any{"call_id": r.CallID, "tool": r.Tool, "device": r.Device, "args": string(r.Args), "key": r.Key}))
			}
		}
	}
	summary := map[string]any{"from": "waiting_io", "why": why, "send": len(send), "held": held}
	switch {
	case held > 0:
		t.State, t.Wait, t.PC = task.Waiting, task.WaitReconcile, task.StepTools
		t.Version++
		send = nil
	case len(send) > 0:
		for i, r := range send {
			if r.Status == mem.CallPlanned {
				d := dispatched(r.CallRecord)
				// A retry after an unknown attempt: that attempt may have run.
				send[i] = Send{CallRecord: d, SentBefore: d.Attempt > 1}
				c.Calls = upsert(c.Calls, d)
			}
		}
		waitIO(t)
	default:
		t.Regs = append(t.Regs, toolMessage(rs))
		e.ready(t, task.StepModel)
	}
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvRecovered, t.ID, summary))
	return c, send
}

// upsert replaces the record with r's CallID in list, or appends r.
func upsert(list []mem.CallRecord, r mem.CallRecord) []mem.CallRecord {
	for i := range list {
		if list[i].CallID == r.CallID {
			list[i] = r
			return list
		}
	}
	return append(list, r)
}

// Dispatch sends a committed batch of t's calls, exactly as a model step's
// own batch is sent. The caller has already handed t to the scheduler.
func (e *Env) Dispatch(taskID string, allow, deny []string, batch []Send) {
	e.dispatch(taskID, allow, deny, batch)
}
