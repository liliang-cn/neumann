package main

import (
	"testing"
	"time"

	"github.com/liliang-cn/neumann/proto"
)

// A round cut off by a cancel is timed up to the task's end, also when its
// only call is read-only and so has no reconcile_needed to close it.
func TestComputeTimesARoundTheCancelCutOff(t *testing.T) {
	t0 := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	at := func(sec int, kind string, data any) proto.Event {
		ev := proto.NewEvent(kind, "a", data)
		ev.Time = t0.Add(time.Duration(sec) * time.Second)
		return ev
	}
	evs := []proto.Event{
		at(0, proto.EvAccepted, map[string]any{"class": "default"}),
		at(1, proto.EvModelCall, map[string]any{"pc": "model"}),
		at(2, proto.EvStep, map[string]any{"pc": "model"}),
		at(2, proto.EvToolCall, map[string]any{"call_id": "c-1", "tool": "git_log"}),
		at(5, proto.EvCancelled, map[string]any{"stop_reason": "cancelled"}),
		at(6, proto.EvToolLate, map[string]any{"call_id": "c-1"}),
	}
	m := Compute(evs, "", false, nil, nil)
	if m.ToolRounds != 1 || m.ToolMS != 3000 || m.Reconcile != 0 || m.Outcome != proto.EvCancelled {
		t.Fatalf("rounds %d, tool ms %v, reconcile %d, outcome %q", m.ToolRounds, m.ToolMS, m.Reconcile, m.Outcome)
	}
}
