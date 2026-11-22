// Package mem is main memory: the one place task state is committed. Every
// state change in the machine is a CommitSet applied by Store.Commit, and
// nothing a commit describes may happen before Commit returns (design §4).
package mem

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// CallStatus is where a call is in its life. A cancelled task's final commit
// leaves no call planned or dispatched: one that never reached its device is
// cancelled, one that did (or may have) is unknown, except a ReadOnly one,
// declared to leave nothing to check, which is cancelled too; a result that arrives after
// that commit is kept on the record as late and never reaches the model.
type CallStatus string

const (
	CallPlanned    CallStatus = "planned"
	CallDispatched CallStatus = "dispatched"
	CallDone       CallStatus = "done"
	CallUnknown    CallStatus = "unknown"
	CallCancelled  CallStatus = "cancelled"
	CallLate       CallStatus = "late"
)

// CallRecord is the write-ahead log entry of one tool call.
type CallRecord struct {
	TaskID  string          `json:"task_id"`
	CallID  string          `json:"call_id"`
	Index   int             `json:"index"` // order of the call within its task
	Tool    string          `json:"tool"`  // full name as the model sees it
	Args    json.RawMessage `json:"args,omitempty"`
	Status  CallStatus      `json:"status"`
	Attempt int             `json:"attempt"`
	Key     string          `json:"key,omitempty"` // idempotency key: task/call/attempt
	// Device, Effect and ReportUnknown are recorded when the call is planned, from
	// the tool's declaration at that moment: recovery after a restart decides
	// by them, not by whatever the bus declares by then.
	Device        string           `json:"device"`
	Effect        task.Effect      `json:"effect"`
	ReportUnknown bool             `json:"report_unknown,omitempty"`
	Result        *task.ToolResult `json:"result,omitempty"`

	// The tool policy's decision (Policy*), taken once, before the call is
	// first dispatched, and committed with it: recovery never asks again. A
	// call held for an operator (PolicyApproval) stays planned while Approval
	// is ApprovalPending. Reason is why the call was denied or held. OrigArgs
	// are the model's arguments when the policy or an operator replaced them
	// (Args are what the device is sent).
	Policy   string          `json:"policy,omitempty"`
	Approval string          `json:"approval,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	OrigArgs json.RawMessage `json:"orig_args,omitempty"`
}

// The tool policy's decisions (CallRecord.Policy); empty: no policy was
// configured when the call was planned.
const (
	PolicyAllow    = "allow"
	PolicyDeny     = "deny"
	PolicyRewrite  = "rewrite"
	PolicyApproval = "approval"
)

// An operator's answer to a call the policy held (CallRecord.Approval).
const (
	ApprovalPending = "pending"
	ApprovalAllowed = "allowed"
	ApprovalDenied  = "denied"
	ApprovalEdited  = "edited"
)

// Interrupt kinds. A steer's Text is the message; a cancel's Text is its
// reason, the StopReason the task ends with ("cancelled" or "deadline"). A
// suspend and a resume carry no text (design §9.9): the holder of the task
// applies them at its next instruction boundary, in seq order, and only the
// last of them counts. Unlike a cancel, neither refuses any commit; a
// terminal commit applies them silently, as any interrupt but a steer.
const (
	InterruptSteer   = "steer"
	InterruptCancel  = "cancel"
	InterruptSuspend = "suspend"
	InterruptResume  = "resume"
)

// CancelReason is the StopReason that the final commit of the cancel in must
// carry: its Text, or "cancelled" when it has none.
func CancelReason(in Interrupt) string {
	if in.Text == "" {
		return "cancelled"
	}
	return in.Text
}

// Interrupt is an outside input to a task, durable before it is applied.
// From names a steer's sender ("operator", "task:<id>", ...); empty is
// unspecified. Key, when set, is the steer's idempotency key: the machine
// commits a steer whose Key the task already has only once. A store must
// keep both as committed: the machine finds a key by reading the task's
// interrupts back, so a store that loses Key sends a keyed steer twice after
// a restart (and a bus/tasks send with it), and one that loses From delivers
// a message as the user's. Interrupts written before From and Key existed
// read back with both empty.
type Interrupt struct {
	TaskID  string `json:"task_id"`
	Seq     int64  `json:"seq"` // assigned by the store
	Kind    string `json:"kind"`
	Text    string `json:"text,omitempty"`
	From    string `json:"from,omitempty"`
	Key     string `json:"key,omitempty"`
	Applied bool   `json:"applied"`
}

// SteerEventData is what a steer_dropped event says about the steer it
// reports; From only when the steer has one, so a steer without a sender is
// reported exactly as before senders existed.
func SteerEventData(in Interrupt) map[string]any {
	d := map[string]any{"seq": in.Seq, "text": in.Text}
	if in.From != "" {
		d["from"] = in.From
	}
	return d
}

// CommitSet is one atomic state change.
//
//   - Task, when set, is the new TCB. NewTask requires Version 1 and an
//     unused ID; otherwise Version must be the stored Version + 1.
//   - Calls are upserted by (TaskID, CallID).
//   - Calls must belong to a stored task or to the task created by this commit.
//     Calls on terminal tasks are allowed (late results are recorded there).
//   - NewInterrupts are checked against the state before this commit: they are
//     rejected for unknown (ErrNotFound) or finished (ErrFinished) tasks, so an
//     interrupt for a task created in the same commit is ErrNotFound.
//   - Applied marks interrupts of Task as applied; it requires Task, and every
//     seq must be a currently pending interrupt of that task.
//   - When Task is terminal, every interrupt still unapplied is marked applied,
//     and a steer_dropped event (SteerEventData) is added for each steer
//     among them, in the same commit, after Events.
//   - While a task has a pending cancel (committed before this commit), the
//     only Task the store accepts for it is the cancel's final commit:
//     terminal, with StopReason CancelReason of the first pending cancel. No
//     call of the task may be recorded as dispatched. Either is refused with
//     ErrCancelPending, so the cancel committed first wins over work that
//     comes after it, whatever the holder of the task has seen. Results, late
//     results and further interrupts are still recorded.
//   - Archive appends messages to Task's archive, in order: the messages a
//     compaction swapped out of its registers (design §5.3). It requires
//     Task, and goes in or is refused with the rest of the commit.
//   - Events get their Seq and TaskSeq; Commit returns them. Both are dense:
//     Seq counts 1, 2, 3… across all events and TaskSeq 1, 2, 3… per task,
//     with no gap left by a rejected commit. They are dense as committed:
//     an EventPruner can later delete events, leaving gaps in what Events
//     reads, but a Seq is never given out twice.
type CommitSet struct {
	Task          *task.TCB
	NewTask       bool
	Calls         []CallRecord
	NewInterrupts []Interrupt
	Applied       []int64
	Archive       []task.Message
	Events        []proto.Event
}

var (
	ErrVersion  = errors.New("mem: version conflict")
	ErrNotFound = errors.New("mem: task not found")
	ErrFinished = errors.New("mem: task already finished")
	ErrExists   = errors.New("mem: task already exists")
	// ErrCancelPending refuses a commit that would let a task go on after a
	// cancel of it was committed: its owner must finalise it instead.
	ErrCancelPending = errors.New("mem: task has a pending cancel")
)

// Store is main memory. Commit applies a CommitSet atomically and durably
// (as durable as the implementation is) and returns its events with their
// sequence numbers and times; the read methods return committed state only.
// Every implementation must pass mem/storetest. NewMemory is the in-process
// one for tests; mem/sqlite is the durable one.
type Store interface {
	Commit(ctx context.Context, c CommitSet) ([]proto.Event, error)
	Load(ctx context.Context, id string) (*task.TCB, error)
	Unfinished(ctx context.Context) ([]*task.TCB, error)
	Calls(ctx context.Context, taskID string) ([]CallRecord, error)
	Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error)
	Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error)
	// Archive is everything Archive commits appended for a task, oldest
	// first; empty for a task with none, or none at all.
	Archive(ctx context.Context, taskID string) ([]task.Message, error)
}

// EventPruner is a Store that can delete old events, so that the event log
// does not grow without bound (Config.EventRetention of package neumann,
// design §4.4.2). It is optional: a Store without it keeps every event, and
// a machine asked for retention over such a store refuses to start.
//
// An event is eligible as of a time before when all of these hold:
//
//   - its Time is before before;
//   - it is not the newest event in the log (so a Seq is never handed out
//     again, and the log's head stays readable);
//   - it has no TaskID (a machine-level event), or its task is not
//     unfinished (terminal, or not in the store at all) and every event of
//     that task meets the two conditions above.
//
// So the events of an unfinished task are never pruned, however old, and a
// finished task's events become eligible together, once its last one is
// old enough (a bounded prune may still take them over more than one
// call). An event committed for a task after its events went (a late
// result's tool_late, say) is pruned on its own when it is old. Event times
// never decrease with Seq (CommitTime), and a store may rely on it.
//
// Seqs are dense as committed, so pruned events are the gaps a reader
// finds: a reader that has read everything up to n has missed nothing as
// long as the next event it reads is n+1. The newest event stays, so a gap
// is always followed by an event that shows it.
//
// Every implementation must pass the retention cases of mem/storetest, which
// run whenever the Store under test is an EventPruner.
type EventPruner interface {
	// PruneEvents deletes eligible events, lowest Seq first, at most limit
	// of them (limit <= 0: no bound), and returns how many it deleted. It
	// never deletes anything else, and the events it keeps read back
	// exactly as before, Seq and TaskSeq unchanged. Each call is atomic and
	// should be short: the caller bounds the work with limit and calls
	// again while it returns limit.
	PruneEvents(ctx context.Context, before time.Time, limit int) (int, error)
}
