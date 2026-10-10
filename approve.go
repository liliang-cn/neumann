package neumann

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/internal/core"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/task"
)

// ToolPolicy decides each tool call before its first dispatch is committed
// (design §9.7): Config.ToolPolicy. It is called once per call, on the
// goroutine that owns the task, inside the instruction whose commit records
// the call; the decision is committed with the call, so a restart never asks
// again. It must be fast and free of side effects: it runs while the task
// holds its vCore, and a commit that fails discards what it decided. A panic
// in it denies the call, with the panic as the reason.
//
// It has no timeout. A policy that blocks stalls its task's vCore, and so
// every task of that group behind it; and since Start decides planned calls
// that were never decided (a store written before a policy was configured)
// while it recovers, a policy that blocks there stalls Start itself. Do not
// call out to a person or a slow service from it: return RequireApproval
// and decide with Approve instead.
type ToolPolicy = core.Policy

// ToolCallInfo is what a ToolPolicy is shown of a call: the task and call
// ids, the tool's full name, its device and declared effect, the arguments
// as the model wrote them (a copy, possibly invalid JSON), the task's class,
// and the attempt it would be dispatched as (1 for a new call).
type ToolCallInfo = core.ToolCallInfo

// Decision is a ToolPolicy's answer; the zero value allows the call. Use
// Allow, Deny, RewriteArgs and RequireApproval to build one.
type Decision = core.Decision

// Verdict is the kind of a Decision.
type Verdict = core.Verdict

const (
	VerdictAllow           = core.Allow
	VerdictDeny            = core.Deny
	VerdictRewrite         = core.Rewrite
	VerdictRequireApproval = core.RequireApproval
)

// Allow lets the call be dispatched as the model wrote it.
func Allow() Decision { return Decision{Verdict: core.Allow} }

// Deny refuses the call: it is never executed, its call record is done with
// an error result that tells the model the reason, and a tool_denied event
// records it.
func Deny(reason string) Decision { return Decision{Verdict: core.Deny, Reason: reason} }

// RewriteArgs dispatches the call with args instead of the model's (a
// tool_rewritten event records both). args must be a JSON object; anything
// else (invalid JSON, null, an array) denies the call.
func RewriteArgs(args []byte) Decision {
	return Decision{Verdict: core.Rewrite, Args: slices.Clone(args)}
}

// RequireApproval holds the call for an operator: its task waits in
// Waiting(Approval) with the whole round undispatched, an approval_needed
// event names the call, and Machine.Approve decides it.
func RequireApproval(reason string) Decision {
	return Decision{Verdict: core.RequireApproval, Reason: reason}
}

// ApprovalDecision is an operator's answer to a held call; the zero value
// allows it. Verdict ApproveDeny refuses it with Reason, ApproveEdit runs it
// with Args (a JSON object) instead of the model's.
type ApprovalDecision = core.ApprovalDecision

// ApprovalVerdict is the kind of an ApprovalDecision.
type ApprovalVerdict = core.ApprovalVerdict

const (
	ApproveAllow = core.ApproveAllow
	ApproveDeny  = core.ApproveDeny
	ApproveEdit  = core.ApproveEdit
)

// ErrNotAwaitingApproval is returned by Approve for a task that is not
// waiting for approval, or a call of it that is not held (already decided,
// or never held).
var ErrNotAwaitingApproval = core.ErrNotAwaitingApproval

// Approve answers a call held for approval (an approval_needed event names
// it). It returns once the decision is committed. When it was the last held
// call of its round, the round goes on in the same commit: the calls let
// through are dispatched, in call order with the rest, and the denied ones
// reach the model as error results. A cancel, a deadline or Stop while the
// round waits sends nothing: its calls are recorded cancelled.
//
// Approve returns ErrNotAwaitingApproval when the task is not waiting for
// approval or the call is not held, ErrNotFound for an unknown task, and an
// error for a decision that cannot be applied (an edit whose arguments are
// not a JSON object).
func (m *Machine) Approve(ctx context.Context, taskID, callID string, d ApprovalDecision) error {
	if err := d.Check(); err != nil {
		return fmt.Errorf("neumann: approve %s/%s: %w", taskID, callID, err)
	}
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	awaiting := func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == task.WaitApproval }
	since := m.sched.Backs(taskID) // before the store is read: a hand-back after it is seen
	t, ok := m.sched.CheckoutIf(taskID, awaiting)
	if !ok {
		stored, err := m.store.Load(ctx, taskID)
		if err != nil {
			return err
		}
		if awaiting(stored) {
			// Committed as waiting, and not handed back yet: by the vCore
			// whose commit announced approval_needed, or by an operator
			// deciding another call of the round. Either lets go at once.
			t, ok = m.sched.CheckoutWait(ctx, taskID, since, awaiting)
		}
		if !ok {
			return fmt.Errorf("neumann: task %s: %w", taskID, ErrNotAwaitingApproval)
		}
	}
	batch, err := m.env.Approve(ctx, t, callID, d)
	if errors.Is(err, mem.ErrCancelPending) {
		// Cancelled first: the decision is not recorded, and the task ends.
		m.env.Finalise(ctx, t)
		return fmt.Errorf("neumann: task %s was cancelled before the decision was recorded: %w", taskID, ErrNotAwaitingApproval)
	}
	allow, deny := slices.Clone(t.Allow), slices.Clone(t.Deny)
	// t is unchanged when err != nil. Cancelled while the operator held it:
	// finalised instead, and the batch is never sent.
	queued := m.env.HandBack(ctx, t, batch)
	if err != nil {
		return err
	}
	if queued && len(batch) > 0 {
		m.env.Dispatch(taskID, allow, deny, batch)
	}
	return nil
}
