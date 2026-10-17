package app

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liliang-cn/neumann/proto"
)

// Tracker follows the machine's committed event log: it keeps every event
// by task, lets callers wait for a task's end, and hands each event to
// listeners in order. One goroutine reads the log.
type Tracker struct {
	mu      sync.Mutex
	byTask  map[string][]proto.Event
	machine []proto.Event
	end     map[string]proto.Event
	waiters map[string][]chan struct{}
	subs    []func(proto.Event)
	last    int64
	closed  chan struct{}
}

// Track starts following the log after seq from.
func (a *App) Track(ctx context.Context, from int64) *Tracker {
	t := &Tracker{byTask: map[string][]proto.Event{}, end: map[string]proto.Event{}, waiters: map[string][]chan struct{}{}, last: from, closed: make(chan struct{})}
	evs := a.M.Events(ctx, from)
	go func() {
		defer close(t.closed)
		for ev := range evs {
			t.add(ev)
		}
	}()
	return t
}

func (t *Tracker) add(ev proto.Event) {
	t.mu.Lock()
	t.last = ev.Seq
	if ev.TaskID == "" {
		t.machine = append(t.machine, ev)
	} else {
		t.byTask[ev.TaskID] = append(t.byTask[ev.TaskID], ev)
		if ev.Terminal() {
			if _, ok := t.end[ev.TaskID]; !ok {
				t.end[ev.TaskID] = ev
				for _, w := range t.waiters[ev.TaskID] {
					close(w)
				}
				delete(t.waiters, ev.TaskID)
			}
		}
	}
	subs := t.subs
	t.mu.Unlock()
	for _, f := range subs {
		f(ev)
	}
}

// Subscribe adds a listener for every event read from now on. It runs on
// the tracker's goroutine: keep it quick.
func (t *Tracker) Subscribe(f func(proto.Event)) {
	t.mu.Lock()
	t.subs = append(t.subs, f)
	t.mu.Unlock()
}

// Last is the Seq of the last event read.
func (t *Tracker) Last() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// Events returns the events of a task read so far.
func (t *Tracker) Events(id string) []proto.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]proto.Event(nil), t.byTask[id]...)
}

// Machine returns the machine events (no task) read so far.
func (t *Tracker) Machine() []proto.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]proto.Event(nil), t.machine...)
}

// Ended returns a task's terminal event, if read.
func (t *Tracker) Ended(id string) (proto.Event, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ev, ok := t.end[id]
	return ev, ok
}

// Wait blocks until the task's terminal event has been read, or ctx ends.
func (t *Tracker) Wait(ctx context.Context, id string) (proto.Event, error) {
	t.mu.Lock()
	if ev, ok := t.end[id]; ok {
		t.mu.Unlock()
		return ev, nil
	}
	w := make(chan struct{})
	t.waiters[id] = append(t.waiters[id], w)
	t.mu.Unlock()
	select {
	case <-w:
		ev, _ := t.Ended(id)
		return ev, nil
	case <-ctx.Done():
		return proto.Event{}, ctx.Err()
	case <-t.closed:
		if ev, ok := t.Ended(id); ok {
			return ev, nil
		}
		return proto.Event{}, context.Canceled
	}
}

// Field reads one field of an event's data.
func Field[T any](ev proto.Event, name string) (T, bool) {
	var zero T
	var m map[string]json.RawMessage
	if json.Unmarshal(ev.Data, &m) != nil {
		return zero, false
	}
	raw, ok := m[name]
	if !ok {
		return zero, false
	}
	var v T
	if json.Unmarshal(raw, &v) != nil {
		return zero, false
	}
	return v, true
}
