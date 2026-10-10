package core

import (
	"context"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execTools (a task Ready at StepTools with planned calls in its log): the
// policy holds a call, the commit is refused by a cancel committed behind
// the vCore, and cancelInstead hands the logged planned record to finalise
// as it is; cancelSet skips records already in c, so the cancelled task's
// final commit leaves the call planned.
func TestCancelInsteadSettlesALoggedPlannedCall(t *testing.T) {
	e, store, _ := newTestEnv(t, reply, echoDev())
	e.Policy = func(context.Context, ToolCallInfo) Decision { return Decision{Verdict: RequireApproval, Reason: "x"} }
	tc := newTask(t, e, store, "q")
	asst := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-1", Name: "dev_echo", Args: []byte(`{}`)}}}
	tc.Regs = append(tc.Regs, asst)
	tc.PC, tc.CallSeq, tc.Version = task.StepTools, 1, 2
	rec := mem.CallRecord{TaskID: "t1", CallID: "c-1", Index: 1, Tool: "dev_echo", Args: []byte(`{}`), Status: mem.CallPlanned, Device: "dev"}
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc, Calls: []mem.CallRecord{rec}}); err != nil {
		t.Fatal(err)
	}
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && c.Task.Wait == task.WaitApproval {
			cancelBehind(t, store) // lands between execTools' read and its commit
		}
		return inner(ctx, c)
	}
	e.Sched.Add(tc)
	e.step(bg, take(t, e), 0)
	got := load(t, store)
	r := callsOf(t, store)["c-1"]
	t.Logf("task %s %q; call %s policy=%q approval=%q", got.State, got.StopReason, r.Status, r.Policy, r.Approval)
	if got.State.Terminal() && (r.Status == mem.CallPlanned || r.Status == mem.CallDispatched) {
		t.Errorf("a finished task left call %s %s", r.CallID, r.Status)
	}
}

// The same refused commit with the policy denying the logged call: the final
// commit keeps the denial the call was given, and its tool_denied event with
// it.
func TestCancelInsteadKeepsADenialWithItsEvent(t *testing.T) {
	e, store, _ := newTestEnv(t, reply, echoDev())
	e.Policy = func(context.Context, ToolCallInfo) Decision { return Decision{Verdict: Deny, Reason: "no"} }
	tc := newTask(t, e, store, "q")
	asst := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-1", Name: "dev_echo", Args: []byte(`{}`)}}}
	tc.Regs = append(tc.Regs, asst)
	tc.PC, tc.CallSeq, tc.Version = task.StepTools, 1, 2
	rec := mem.CallRecord{TaskID: "t1", CallID: "c-1", Index: 1, Tool: "dev_echo", Args: []byte(`{}`), Status: mem.CallPlanned, Device: "dev"}
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc, Calls: []mem.CallRecord{rec}}); err != nil {
		t.Fatal(err)
	}
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && !c.Task.State.Terminal() && len(c.Calls) > 0 {
			cancelBehind(t, store)
		}
		return inner(ctx, c)
	}
	e.Sched.Add(tc)
	e.step(bg, take(t, e), 0)
	if got := load(t, store); got.State != task.Cancelled {
		t.Fatalf("task %s", got.State)
	}
	r := callsOf(t, store)["c-1"]
	if r.Status != mem.CallDone || r.Policy != mem.PolicyDeny || r.Result == nil || !r.Result.IsError {
		t.Fatalf("c-1 %+v", r)
	}
	evs, _ := store.Events(bg, 0, 0)
	n := 0
	for _, ev := range evs {
		if ev.Kind == proto.EvToolDenied {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d tool_denied events for the denied call", n)
	}
}
