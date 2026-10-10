// Package mem is main memory: the one place task state is committed. Every
// state change in the machine is a CommitSet applied by Store.Commit, and
// nothing a commit describes may happen before Commit returns (design §4).
package mem

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

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
	TaskID  string           `json:"task_id"`
	CallID  string           `json:"call_id"`
	Index   int              `json:"index"` // order of the call within its task
	Tool    string           `json:"tool"`  // full name as the model sees it
	Args    json.RawMessage  `json:"args,omitempty"`
	Status  CallStatus       `json:"status"`
	Attempt int              `json:"attempt"`
	Key     string           `json:"key,omitempty"` // idempotency key: task/call/attempt
	Result  *task.ToolResult `json:"result,omitempty"`
}

const InterruptSteer = "steer"

// Interrupt is an outside input to a task, durable before it is applied.
type Interrupt struct {
	TaskID  string `json:"task_id"`
	Seq     int64  `json:"seq"` // assigned by the store
	Kind    string `json:"kind"`
	Text    string `json:"text,omitempty"`
	Applied bool   `json:"applied"`
}

// CommitSet is one atomic state change.
//
//   - Task, when set, is the new TCB. NewTask requires Version 1 and an
//     unused ID; otherwise Version must be the stored Version + 1.
//   - Calls are upserted by (TaskID, CallID).
//   - NewInterrupts are rejected for unknown (ErrNotFound) or finished
//     (ErrFinished) tasks.
//   - Applied marks interrupts of Task as applied; it requires Task.
//   - When Task is terminal, every interrupt still unapplied is marked applied
//     and a steer_dropped event is added for it, in the same commit.
//   - Events get their Seq and TaskSeq; Commit returns them.
type CommitSet struct {
	Task          *task.TCB
	NewTask       bool
	Calls         []CallRecord
	NewInterrupts []Interrupt
	Applied       []int64
	Events        []proto.Event
}

var (
	ErrVersion  = errors.New("mem: version conflict")
	ErrNotFound = errors.New("mem: task not found")
	ErrFinished = errors.New("mem: task already finished")
	ErrExists   = errors.New("mem: task already exists")
)

type Store interface {
	Commit(ctx context.Context, c CommitSet) ([]proto.Event, error)
	Load(ctx context.Context, id string) (*task.TCB, error)
	Unfinished(ctx context.Context) ([]*task.TCB, error)
	Calls(ctx context.Context, taskID string) ([]CallRecord, error)
	Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error)
	Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error)
}
