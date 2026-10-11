package mem

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Memory is an in-process Store. It is not durable; it exists for phase 1 and
// for tests, and it implements exactly the semantics the SQLite store will.
type Memory struct {
	// Now is the clock that stamps event times (proto.Event.Time); nil is
	// time.Now. Set it before the first Commit.
	Now func() time.Time

	mu         sync.Mutex
	stamped    time.Time // the time of the last commit set with events
	tasks      map[string]*task.TCB
	calls      map[string][]CallRecord
	interrupts map[string][]Interrupt
	archive    map[string][]task.Message
	events     []proto.Event
	taskSeq    map[string]int64
	intSeq     int64
}

var _ Store = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		tasks:      map[string]*task.TCB{},
		calls:      map[string][]CallRecord{},
		interrupts: map[string][]Interrupt{},
		archive:    map[string][]task.Message{},
		taskSeq:    map[string]int64{},
	}
}

func (m *Memory) Commit(ctx context.Context, c CommitSet) ([]proto.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validate(c); err != nil {
		return nil, err
	}

	events := make([]proto.Event, len(c.Events))
	for i, e := range c.Events {
		events[i] = cloneEvent(e)
	}
	if c.Task != nil {
		m.tasks[c.Task.ID] = c.Task.Clone()
	}
	for _, r := range c.Calls {
		m.upsertCall(r)
	}
	if len(c.Archive) > 0 {
		m.archive[c.Task.ID] = append(m.archive[c.Task.ID], task.CloneMessages(c.Archive)...)
	}
	for _, in := range c.NewInterrupts {
		m.intSeq++
		in.Seq, in.Applied = m.intSeq, false
		m.interrupts[in.TaskID] = append(m.interrupts[in.TaskID], in)
	}
	if c.Task != nil {
		applied := make(map[int64]bool, len(c.Applied))
		for _, s := range c.Applied {
			applied[s] = true
		}
		list := m.interrupts[c.Task.ID]
		for i := range list {
			switch {
			case list[i].Applied:
			case applied[list[i].Seq]:
				list[i].Applied = true
			case c.Task.State.Terminal():
				list[i].Applied = true
				if list[i].Kind == InterruptSteer {
					events = append(events, proto.NewEvent(proto.EvSteerDropped, c.Task.ID, SteerEventData(list[i])))
				}
			}
		}
	}
	if len(events) > 0 {
		now := time.Now
		if m.Now != nil {
			now = m.Now
		}
		m.stamped = CommitTime(now(), m.stamped)
	}
	for i := range events {
		events[i].Time = m.stamped
		m.taskSeq[events[i].TaskID]++
		events[i].Seq = int64(len(m.events)) + 1
		events[i].TaskSeq = m.taskSeq[events[i].TaskID]
		m.events = append(m.events, cloneEvent(events[i]))
	}
	out := make([]proto.Event, len(events))
	for i, e := range events {
		out[i] = cloneEvent(e)
	}
	return out, nil
}

func cloneEvent(e proto.Event) proto.Event {
	e.Data = bytes.Clone(e.Data)
	return e
}

// validate checks a commit completely before anything is applied, so that a
// commit applies whole or not at all.
func (m *Memory) validate(c CommitSet) error {
	if c.NewTask && c.Task == nil {
		return errors.New("mem: NewTask requires Task")
	}
	if len(c.Applied) > 0 && c.Task == nil {
		return errors.New("mem: Applied requires Task")
	}
	if len(c.Archive) > 0 && c.Task == nil {
		return errors.New("mem: Archive requires Task")
	}
	if t := c.Task; t != nil {
		old, exists := m.tasks[t.ID]
		switch {
		case c.NewTask && exists:
			return fmt.Errorf("%w: %s", ErrExists, t.ID)
		case c.NewTask && t.Version != 1:
			return fmt.Errorf("%w: new task %s must have version 1, got %d", ErrVersion, t.ID, t.Version)
		case !c.NewTask && !exists:
			return fmt.Errorf("%w: %s", ErrNotFound, t.ID)
		case !c.NewTask && t.Version != old.Version+1:
			return fmt.Errorf("%w: task %s is at %d, commit carries %d", ErrVersion, t.ID, old.Version, t.Version)
		}
	}
	if t := c.Task; t != nil && !c.NewTask {
		if in, ok := m.pendingCancel(t.ID); ok && !(t.State.Terminal() && t.StopReason == CancelReason(in)) {
			return fmt.Errorf("%w: %s", ErrCancelPending, t.ID)
		}
	}
	for _, r := range c.Calls {
		if _, ok := m.tasks[r.TaskID]; !ok && !(c.NewTask && c.Task.ID == r.TaskID) {
			return fmt.Errorf("%w: %s", ErrNotFound, r.TaskID)
		}
		if _, ok := m.pendingCancel(r.TaskID); ok && r.Status == CallDispatched {
			return fmt.Errorf("%w: %s: call %s cannot be dispatched", ErrCancelPending, r.TaskID, r.CallID)
		}
	}
	for _, seq := range c.Applied {
		pending := false
		for _, in := range m.interrupts[c.Task.ID] {
			if in.Seq == seq && !in.Applied {
				pending = true
				break
			}
		}
		if !pending {
			return fmt.Errorf("mem: interrupt %d is not pending for task %s", seq, c.Task.ID)
		}
	}
	for _, in := range c.NewInterrupts {
		t, ok := m.tasks[in.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, in.TaskID)
		}
		if t.State.Terminal() {
			return fmt.Errorf("%w: %s", ErrFinished, in.TaskID)
		}
	}
	return nil
}

// pendingCancel returns the first unapplied cancel interrupt of a task.
func (m *Memory) pendingCancel(id string) (Interrupt, bool) {
	for _, in := range m.interrupts[id] {
		if !in.Applied && in.Kind == InterruptCancel {
			return in, true
		}
	}
	return Interrupt{}, false
}

func (m *Memory) upsertCall(r CallRecord) {
	r = cloneCall(r)
	list := m.calls[r.TaskID]
	for i := range list {
		if list[i].CallID == r.CallID {
			list[i] = r
			return
		}
	}
	m.calls[r.TaskID] = append(list, r)
}

func cloneCall(r CallRecord) CallRecord {
	r.Args = append(json.RawMessage(nil), r.Args...)
	if r.OrigArgs != nil {
		r.OrigArgs = append(json.RawMessage(nil), r.OrigArgs...)
	}
	if r.Result != nil {
		res := *r.Result
		r.Result = &res
	}
	return r
}

func (m *Memory) Load(ctx context.Context, id string) (*task.TCB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return t.Clone(), nil
}

func (m *Memory) Unfinished(ctx context.Context) ([]*task.TCB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*task.TCB
	for _, t := range m.tasks {
		if !t.State.Terminal() {
			out = append(out, t.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) Calls(ctx context.Context, taskID string) ([]CallRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CallRecord, 0, len(m.calls[taskID]))
	for _, r := range m.calls[taskID] {
		out = append(out, cloneCall(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (m *Memory) Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Interrupt
	for _, in := range m.interrupts[taskID] {
		if unappliedOnly && in.Applied {
			continue
		}
		out = append(out, in)
	}
	return out, nil
}

func (m *Memory) Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if afterSeq < 0 {
		afterSeq = 0
	}
	n := int64(len(m.events))
	if afterSeq >= n {
		return nil, nil
	}
	end := n
	if limit > 0 && afterSeq+int64(limit) < end {
		end = afterSeq + int64(limit)
	}
	out := make([]proto.Event, 0, end-afterSeq)
	for _, e := range m.events[afterSeq:end] {
		out = append(out, cloneEvent(e))
	}
	return out, nil
}

func (m *Memory) Archive(ctx context.Context, taskID string) ([]task.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return task.CloneMessages(m.archive[taskID]), nil
}
