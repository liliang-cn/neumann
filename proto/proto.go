// Package proto is the machine's wire format: instructions in, events out.
package proto

import (
	"encoding/json"

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
	Priority int          `json:"priority,omitempty"`
	Budget   *task.Budget `json:"budget,omitempty"`
	Allow    []string     `json:"allow,omitempty"`
	Deny     []string     `json:"deny,omitempty"`
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

	// Recovery after a restart, and an operator's answer for a call whose
	// outcome became unknown.
	EvRecovered       = "recovered"
	EvReconcileNeeded = "reconcile_needed"
	EvResolved        = "resolved"

	// A model call failed or was cut off and the task will try again:
	// {class, attempt, delay_ms, wait, error} (design §11, §13).
	EvModelRetry = "model_retry"

	// Machine events, with no task: a model call waited for room in its
	// limiters ({task, waited_ms}); an execution group's breaker opened
	// ({class, cause}) or closed ({class}) (design §6.2, §7.2).
	EvThrottled = "throttled"
	EvGroupDown = "group_down"
	EvGroupUp   = "group_up"
)

// Event is something that happened, published only after it was committed.
// Seq is global and TaskSeq is per task; the store assigns both.
type Event struct {
	Kind    string          `json:"kind"`
	TaskID  string          `json:"task_id,omitempty"`
	Seq     int64           `json:"seq"`
	TaskSeq int64           `json:"task_seq"`
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
