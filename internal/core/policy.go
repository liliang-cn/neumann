package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Verdict is what the tool policy decides for one call.
type Verdict int

const (
	Allow           Verdict = iota // dispatch the call as the model wrote it
	Deny                           // never execute it; the model gets an error result with the reason
	Rewrite                        // dispatch it with Decision.Args instead (valid JSON only)
	RequireApproval                // hold the call's round until an operator decides (Approve)
)

var verdictNames = [...]string{"allow", "deny", "rewrite", "require_approval"}

func (v Verdict) String() string {
	if v < 0 || int(v) >= len(verdictNames) {
		return fmt.Sprintf("verdict(%d)", int(v))
	}
	return verdictNames[v]
}

// Decision is the tool policy's answer for one call. The zero value allows
// it. Reason goes to the model (Deny) or to the operator (RequireApproval).
type Decision struct {
	Verdict Verdict
	Reason  string
	Args    json.RawMessage // Rewrite: the arguments the call is dispatched with
}

// ToolCallInfo is what the tool policy is shown of a call about to be
// dispatched for the first time.
type ToolCallInfo struct {
	TaskID  string
	CallID  string
	Tool    string // full name, as the model sees it: <device>_<tool>
	Device  string // empty for a tool no device declares
	Effect  task.Effect
	Args    json.RawMessage // a copy; may be invalid JSON, as the model wrote it
	Class   string          // the task's execution group
	Attempt int             // the attempt it would be dispatched as: 1 for a new call
}

// Policy decides each tool call before its first dispatch is committed
// (design §9.7). It runs on the goroutine that owns the task, inside the
// instruction that commits the call, so it must be fast and free of side
// effects; a panic in it denies the call.
type Policy func(ctx context.Context, info ToolCallInfo) Decision

// ApprovalVerdict is an operator's answer to a held call.
type ApprovalVerdict int

const (
	ApproveAllow ApprovalVerdict = iota // run the call as it is
	ApproveDeny                         // never run it; the model gets an error result with the reason
	ApproveEdit                         // run it with ApprovalDecision.Args instead (valid JSON only)
)

var approvalNames = [...]string{"allow", "deny", "edit"}

func (v ApprovalVerdict) String() string {
	if v < 0 || int(v) >= len(approvalNames) {
		return fmt.Sprintf("approval(%d)", int(v))
	}
	return approvalNames[v]
}

// MarshalText writes the verdict by name: "allow", "deny" or "edit".
func (v ApprovalVerdict) MarshalText() ([]byte, error) {
	if v < 0 || int(v) >= len(approvalNames) {
		return nil, fmt.Errorf("invalid approval verdict %d", int(v))
	}
	return []byte(approvalNames[v]), nil
}

func (v *ApprovalVerdict) UnmarshalText(b []byte) error {
	for i, n := range approvalNames {
		if n == string(b) {
			*v = ApprovalVerdict(i)
			return nil
		}
	}
	return fmt.Errorf("unknown approval verdict %q: allow, deny or edit", b)
}

// ApprovalDecision is an operator's answer to a call held for approval. The
// zero value allows it.
type ApprovalDecision struct {
	Verdict ApprovalVerdict `json:"verdict"`
	Reason  string          `json:"reason,omitempty"` // deny: what the model is told
	Args    json.RawMessage `json:"args,omitempty"`   // edit: the arguments the call runs with
}

// Check refuses a decision that cannot be applied: an unknown verdict, an
// edit without valid JSON arguments, or arguments on anything but an edit.
func (d ApprovalDecision) Check() error {
	switch d.Verdict {
	case ApproveAllow, ApproveDeny:
		if d.Args != nil {
			return fmt.Errorf("an approval that does not edit carries no arguments (verdict %s)", d.Verdict)
		}
	case ApproveEdit:
		if len(d.Args) == 0 || !json.Valid(d.Args) {
			return errors.New("an edit needs arguments that are valid JSON")
		}
	default:
		return fmt.Errorf("unknown approval verdict %d", int(d.Verdict))
	}
	return nil
}

// ErrNotAwaitingApproval is returned by Approve for a task or call that is
// not held for an operator's approval.
var ErrNotAwaitingApproval = errors.New("not awaiting approval")

// Who refused a call, in tool_denied events.
const (
	byPolicy   = "policy"
	byOperator = "operator"
)

// deniedText is the result a model is given for a call that was refused
// before it ran.
func deniedText(reason string) string {
	return "This call was denied and not executed. Reason: " + reason
}

// decide asks the tool policy about every call of rs that is planned, has
// never been sent, and has no decision yet, and applies each answer to its
// record in place; the caller commits the records with the returned events.
// A call the task's page table refuses is not asked about: the bus refuses
// it when it is dispatched. Without a policy nothing is recorded.
//
//   - Allow: recorded, the call goes on as it is.
//   - Deny: the call is done with an error result that gives the reason; a
//     tool_denied event records it. It is never sent.
//   - Rewrite: Args are replaced (the model's are kept in OrigArgs) and a
//     tool_rewritten event records both; arguments that are not valid JSON
//     deny the call instead.
//   - RequireApproval: the call stays planned with Approval pending, and an
//     approval_needed event announces it.
//
// The policy runs on the caller's goroutine, which owns t; a panic in it
// denies the call.
func (e *Env) decide(ctx context.Context, t *task.TCB, rs []mem.CallRecord) []proto.Event {
	if e.Policy == nil {
		return nil
	}
	var evs []proto.Event
	for i := range rs {
		r := &rs[i]
		if r.Status != mem.CallPlanned || r.Attempt > 0 || r.Policy != "" || !bus.Permitted(r.Tool, t.Allow, t.Deny) {
			continue
		}
		d := e.ask(ctx, t, *r)
		switch d.Verdict {
		case Allow:
			r.Policy = mem.PolicyAllow
		case Rewrite:
			if len(d.Args) == 0 || !json.Valid(d.Args) {
				evs = append(evs, deny(t, r, mem.PolicyDeny, "the tool policy rewrote the arguments to invalid JSON", byPolicy))
				continue
			}
			r.Policy = mem.PolicyRewrite
			r.OrigArgs, r.Args = r.Args, bytes.Clone(d.Args)
			evs = append(evs, proto.NewEvent(proto.EvToolRewritten, t.ID, map[string]any{
				"call_id": r.CallID, "tool": r.Tool, "original": string(r.OrigArgs), "args": string(r.Args),
			}))
		case Deny:
			reason := d.Reason
			if reason == "" {
				reason = "denied by the tool policy"
			}
			evs = append(evs, deny(t, r, mem.PolicyDeny, reason, byPolicy))
		case RequireApproval:
			r.Policy, r.Approval, r.Reason = mem.PolicyApproval, mem.ApprovalPending, d.Reason
			evs = append(evs, approvalNeeded(t.ID, *r, false))
		default:
			evs = append(evs, deny(t, r, mem.PolicyDeny, fmt.Sprintf("the tool policy answered %s", d.Verdict), byPolicy))
		}
	}
	return evs
}

// ask runs the policy for one call, under the same guard as the adapter's
// hooks: a panic is a denial that names it.
func (e *Env) ask(ctx context.Context, t *task.TCB, r mem.CallRecord) (d Decision) {
	info := ToolCallInfo{
		TaskID: t.ID, CallID: r.CallID, Tool: r.Tool, Device: r.Device, Effect: r.Effect,
		Args: bytes.Clone(r.Args), Class: t.Class, Attempt: r.Attempt + 1,
	}
	defer func() {
		if v := recover(); v != nil {
			d = Decision{Verdict: Deny, Reason: fmt.Sprintf("the tool policy panicked: %v", v)}
		}
	}()
	return e.Policy(ctx, info)
}

// deny makes r a call refused before it ran: done, never sent, with an error
// result that tells the model why. policy is the record's Policy (the
// policy's own denial), or empty for an operator's (Approval denied).
func deny(t *task.TCB, r *mem.CallRecord, policy, reason, by string) proto.Event {
	if policy != "" {
		r.Policy = policy
	} else {
		r.Approval = mem.ApprovalDenied
	}
	r.Status, r.Reason = mem.CallDone, reason
	r.Result = &task.ToolResult{Content: deniedText(reason), IsError: true}
	return proto.NewEvent(proto.EvToolDenied, t.ID, map[string]any{"call_id": r.CallID, "tool": r.Tool, "reason": reason, "by": by})
}

// approvalNeeded announces a call held for an operator; again marks the
// announcement a restart makes.
func approvalNeeded(taskID string, r mem.CallRecord, again bool) proto.Event {
	d := map[string]any{
		"call_id": r.CallID, "tool": r.Tool, "device": r.Device, "effect": r.Effect.String(),
		"args": string(r.Args), "reason": r.Reason,
	}
	if again {
		d["recovered"] = true
	}
	return proto.NewEvent(proto.EvApprovalNeeded, taskID, d)
}

// pendingApproval reports whether r is held for an operator.
func pendingApproval(r mem.CallRecord) bool {
	return r.Status == mem.CallPlanned && r.Approval == mem.ApprovalPending
}

// awaitApproval parks t until every held call of its round is decided.
func awaitApproval(t *task.TCB) {
	t.State, t.Wait, t.PC = task.Waiting, task.WaitApproval, task.StepTools
	t.Version++
}

// startRound decides what a fresh round does once the policy has spoken
// (rs are its records, in call order, updated in place): while any call is
// held for approval, the whole round waits, nothing dispatched (design
// §9.7); otherwise every planned call is dispatched and the task waits on
// I/O; a round whose every call was denied is answered at once and the task
// is ready for its next model step.
func (e *Env) startRound(t *task.TCB, rs []mem.CallRecord) []mem.CallRecord {
	if slices.ContainsFunc(rs, pendingApproval) {
		awaitApproval(t)
		return nil
	}
	var batch []mem.CallRecord
	for i := range rs {
		if rs[i].Status == mem.CallPlanned {
			rs[i] = dispatched(rs[i])
			batch = append(batch, rs[i])
		}
	}
	if len(batch) == 0 {
		t.Regs = append(t.Regs, toolMessage(rs))
		e.ready(t, task.StepModel)
		return nil
	}
	waitIO(t)
	return batch
}

// Approve applies an operator's decision to a call held for approval and,
// once no call of the round is held any more, lets the round go on in the
// same commit: the planned calls are dispatched (returned, for the caller to
// send after handing t back) and the denied ones answered. The caller owns t
// (checked out of the wait set) and hands it back after a nil error; on
// error t is unchanged.
func (e *Env) Approve(ctx context.Context, t *task.TCB, callID string, d ApprovalDecision) ([]Send, error) {
	if t.State != task.Waiting || t.Wait != task.WaitApproval {
		return nil, fmt.Errorf("task %s: %w", t.ID, ErrNotAwaitingApproval)
	}
	if err := d.Check(); err != nil {
		return nil, err
	}
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	rs := round(t, recs)
	i := slices.IndexFunc(rs, func(r mem.CallRecord) bool { return r.CallID == callID })
	if i < 0 || !pendingApproval(rs[i]) {
		return nil, fmt.Errorf("call %s/%s: %w", t.ID, callID, ErrNotAwaitingApproval)
	}
	r := &rs[i]
	var ev proto.Event
	switch d.Verdict {
	case ApproveAllow:
		r.Approval = mem.ApprovalAllowed
		ev = proto.NewEvent(proto.EvApproved, t.ID, map[string]any{"call_id": callID})
	case ApproveEdit:
		orig := r.Args
		if r.OrigArgs == nil {
			r.OrigArgs = r.Args
		}
		r.Args, r.Approval = bytes.Clone(d.Args), mem.ApprovalEdited
		ev = proto.NewEvent(proto.EvApproved, t.ID, map[string]any{"call_id": callID, "original": string(orig), "args": string(r.Args)})
	case ApproveDeny:
		reason := d.Reason
		if reason == "" {
			reason = "denied by an operator"
		}
		ev = deny(t, r, "", reason, byOperator)
	}
	clone := t.Clone() // settled in place; t stays intact if the commit fails
	c, batch, _ := e.settleRound(clone, rs)
	if !slices.ContainsFunc(c.Calls, func(o mem.CallRecord) bool { return o.CallID == callID }) {
		c.Calls = append(c.Calls, *r) // settleRound wrote it already if it sent it
	}
	c.Events = append([]proto.Event{ev}, c.Events...)
	if err := e.Commit(ctx, c); err != nil {
		return nil, err
	}
	*t = *clone
	return batch, nil
}
