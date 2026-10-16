package proto

import (
	"encoding/json"
	"time"

	"github.com/liliang-cn/neumann/task"
)

// Instruction kinds.
const (
	KindSubmit = "submit"
	KindSteer  = "steer"
	KindCancel = "cancel"
)

// Instruction is everything the machine accepts from outside.
type Instruction struct {
	Kind     string       `json:"kind"`
	TaskID   string       `json:"task_id,omitempty"`
	Input    string       `json:"input,omitempty"`
	Class    string       `json:"class,omitempty"`
	PinClass bool         `json:"pin_class,omitempty"` // never moved to a fallback group
	Priority int          `json:"priority,omitempty"`  // 0 interactive; larger is less urgent (submit, priority)
	Budget   *task.Budget `json:"budget,omitempty"`
	Allow    []string     `json:"allow,omitempty"`
	Deny     []string     `json:"deny,omitempty"`
	From     string       `json:"from,omitempty"` // steer: its sender ("operator", "task:<id>", ...); empty: unspecified
}

// Event kinds.
const (
	EvAccepted     = "accepted"
	EvStep         = "step"
	EvToolCall     = "tool_call"
	EvToolResult   = "tool_result"
	EvToolLate     = "tool_late" // a result that arrived after its task was cancelled
	EvUsage        = "usage"
	EvAnswer       = "answer"
	EvDone         = "done"
	EvFailed       = "failed"
	EvCancelled    = "cancelled"
	EvSteerDropped = "steer_dropped"
	// A steer that has a sender reached the model: {seq, from}, committed
	// with the model step that sent it, ahead of that step's events. A steer
	// without one is delivered silently; steer_dropped carries from too when
	// the steer has one.
	EvSteerDelivered = "steer_delivered"

	// A model call was preempted for a more urgent task of its group:
	// {by, kept, ran_ms, n}; the task is Ready again and its next call
	// continues the kept text (design §6.4).
	EvPreempted = "preempted"
	// An operator changed a task's priority: {priority, was} (design §6.4).
	EvPrioritySet = "priority_set"

	// Recovery after a restart, and an operator's answer for a call whose
	// outcome became unknown.
	EvRecovered       = "recovered"
	EvReconcileNeeded = "reconcile_needed"
	EvResolved        = "resolved"

	// A model call failed or was cut off and the task will try again:
	// {class, attempt, delay_ms, wait, error} (design §11, §13).
	EvModelRetry = "model_retry"

	// A model request is about to go out: committed with the task's Running
	// state, so its time is when the call began and a provider's latency is
	// the time from it to the step that carries the result ({pc, core}).
	EvModelCall = "model_call"

	// Machine events, with no task: a model call waited for room in its
	// limiters ({task, class, waited_ms}); an execution group's breaker opened
	// ({class, cause}) or closed ({class}) (design §6.2, §7.2).
	EvThrottled = "throttled"
	EvGroupDown = "group_down"
	EvGroupUp   = "group_up"

	// A vCore started ({class, core, reason, live}: reason demand) or
	// retired (reason overload, idle, manual or shutdown); live is the
	// group's vCore count after the change (design §6.1).
	EvCoreUp   = "core_up"
	EvCoreDown = "core_down"

	// A task moved to another execution group: {from, to, reason}, reason
	// group_down or manual (design §6.3). migrate_refused: its group is down
	// and the fallback cannot take it ({from, to, check, detail}); the task
	// waits for its group.
	EvMigrated       = "migrated"
	EvMigrateRefused = "migrate_refused"

	// The tool policy and human approval (design §9.7). tool_denied: a call
	// was refused and never executed; the model gets an error result
	// ({call_id, tool, reason, by}, by "policy" or "operator"); it takes the
	// place of tool_result. tool_rewritten: the policy replaced a call's
	// arguments ({call_id, tool, original, args}). approval_needed: a call is
	// held for an operator ({call_id, tool, device, effect, args, reason};
	// recovered: true when a restart announces it again). approved: an
	// operator let a held call run ({call_id}; an edit adds edited: true,
	// original and args).
	EvToolDenied     = "tool_denied"
	EvToolRewritten  = "tool_rewritten"
	EvApprovalNeeded = "approval_needed"
	EvApproved       = "approved"

	// The answer check (design §9.8) rejected a final answer: {reason,
	// feedback, attempt}, attempt 0 for the task's first checked answer. The
	// answer and the feedback, as a user message, are in the registers and
	// the model answers again. With final: true no retry was left: the
	// task finishes with that answer anyway, and its answer and done events
	// carry check_failed: true ({reason, attempt, final}, no feedback). When
	// a check is configured, the answer and done events of an answer it saw
	// carry checked: true; done adds check_reason (the check's reason, if
	// any) or check_error (it timed out or panicked, and the answer passed).
	// A final rejection for want of a model step in the task's budget adds
	// budget: true. The step event of a checked answer carries check_ms,
	// how long the check took after the call answered.
	EvAnswerRejected = "answer_rejected"

	// A compaction swapped the oldest rounds of a task's registers out into
	// one summary message (design §5.3): {rounds, messages_before,
	// messages_after, tokens_before_est, tokens_after_est, chunk, chunks}:
	// chunk i of the pass's n (n as projected when the chunk was cut).
	EvCompacted = "compacted"
)

// Event is something that happened, published only after it was committed.
// Seq is global and TaskSeq is per task; the store assigns both.
//
// Time is when the event was committed, in UTC, also assigned by the store:
// one instant for every event of a commit set, never earlier than the set
// committed before it, and stored durably, so a replay reads the same value.
// It is zero for events a store wrote before it kept times; a consumer must
// treat zero as unknown.
type Event struct {
	Kind    string          `json:"kind"`
	TaskID  string          `json:"task_id,omitempty"`
	Seq     int64           `json:"seq"`
	TaskSeq int64           `json:"task_seq"`
	Time    time.Time       `json:"time,omitzero"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// NewEvent builds an uncommitted event; data is marshalled to JSON.
func NewEvent(kind, taskID string, data any) Event {
	ev := Event{Kind: kind, TaskID: taskID}
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			panic("proto: marshal event data: " + err.Error())
		}
		ev.Data = b
	}
	return ev
}

// Terminal reports whether the event ends its task.
func (e Event) Terminal() bool {
	return e.Kind == EvDone || e.Kind == EvFailed || e.Kind == EvCancelled
}

// Delta is a piece of a model step's output while it is being generated,
// pushed live to subscribers. Deltas are never committed and carry no Seq: a
// subscriber that falls behind loses some, and the step's committed answer
// event is the record. Step is the task's model-step number (1-based).
type Delta struct {
	TaskID    string `json:"task_id"`
	Step      int    `json:"step"`
	Text      string `json:"text,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}
