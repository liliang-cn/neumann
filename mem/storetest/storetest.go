// Package storetest is the conformance suite every mem.Store must pass: the
// in-memory store and the SQLite store run the same cases, so they cannot
// drift apart in the semantics the machine relies on (design §4).
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

// Factory returns a fresh, empty store for one case.
type Factory func(t *testing.T) mem.Store

// Run runs every case against stores made by newStore.
func Run(t *testing.T, newStore Factory) {
	cases := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"NewTaskThenLoad", testNewTaskThenLoad},
		{"VersionMustAdvanceByOne", testVersionMustAdvanceByOne},
		{"StoreKeepsItsOwnCopy", testStoreKeepsItsOwnCopy},
		{"EventsGetSequenceNumbers", testEventsGetSequenceNumbers},
		{"InterruptsAreAppliedWithTheTask", testInterruptsAreAppliedWithTheTask},
		{"AppliedNeedsATask", testAppliedNeedsATask},
		{"InterruptForUnknownOrFinishedTask", testInterruptForUnknownOrFinishedTask},
		{"TerminalCommitDropsUnappliedInterrupts", testTerminalCommitDropsUnappliedInterrupts},
		{"CallsUpsertInIndexOrder", testCallsUpsertInIndexOrder},
		{"Unfinished", testUnfinished},
		{"EventDataIsCopied", testEventDataIsCopied},
		{"CallsForUnknownTaskRejected", testCallsForUnknownTaskRejected},
		{"NewTaskWithoutTask", testNewTaskWithoutTask},
		{"CommitIsAtomic", testCommitIsAtomic},
		{"TerminalWithAppliedDropsOnlyTheRest", testTerminalWithAppliedDropsOnlyTheRest},
		{"NonTerminalAppliedLeavesOthersPending", testNonTerminalAppliedLeavesOthersPending},
		{"TaskSeqInterleaves", testTaskSeqInterleaves},
		{"SameVersionIsAConflict", testSameVersionIsAConflict},
		{"AppliedMustBePending", testAppliedMustBePending},
		{"NonSteerInterruptDroppedSilently", testNonSteerInterruptDroppedSilently},
		{"FullTaskRoundTrip", testFullTaskRoundTrip},
		{"FullCallRecordRoundTrip", testFullCallRecordRoundTrip},
		{"UnfinishedCoversEveryNonTerminalState", testUnfinishedCoversEveryNonTerminalState},
		{"CancelInterruptIsAppliedSilently", testCancelInterruptIsAppliedSilently},
		{"CancelledAndLateCallsRoundTrip", testCancelledAndLateCallsRoundTrip},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newStore) })
	}
}

func newTask(id string) *task.TCB {
	return &task.TCB{ID: id, State: task.Ready, Class: "default", Regs: []task.Message{task.Text(task.RoleUser, "q")}, Version: 1}
}

func mustCommit(t *testing.T, s mem.Store, c mem.CommitSet) []proto.Event {
	t.Helper()
	evs, err := s.Commit(ctx, c)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	return evs
}

func steer(id, text string) mem.CommitSet {
	return mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: id, Kind: mem.InterruptSteer, Text: text}}}
}

func testNewTaskThenLoad(t *testing.T, newStore Factory) {
	s := newStore(t)
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Regs[0].TextContent() != "q" {
		t.Fatalf("loaded %+v", got)
	}
	if _, err := s.Commit(ctx, mem.CommitSet{Task: newTask("a"), NewTask: true}); !errors.Is(err, mem.ErrExists) {
		t.Fatalf("second create: %v, want ErrExists", err)
	}
	if _, err := s.Load(ctx, "nope"); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("load missing: %v, want ErrNotFound", err)
	}
}

func testVersionMustAdvanceByOne(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.Version = 3
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc}); !errors.Is(err, mem.ErrVersion) {
		t.Fatalf("got %v, want ErrVersion", err)
	}
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc})
}

func testStoreKeepsItsOwnCopy(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.Regs[0].Parts[0].Text = "mutated after commit"
	got, _ := s.Load(ctx, "a")
	if got.Regs[0].TextContent() != "q" {
		t.Fatalf("store saw a mutation made after commit")
	}
	got.Regs[0].Parts[0].Text = "mutated after load"
	again, _ := s.Load(ctx, "a")
	if again.Regs[0].TextContent() != "q" {
		t.Fatalf("store saw a mutation made after load")
	}
}

func testEventsGetSequenceNumbers(t *testing.T, newStore Factory) {
	s := newStore(t)
	evs := mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true, Events: []proto.Event{
		proto.NewEvent(proto.EvAccepted, "a", nil),
		proto.NewEvent(proto.EvStep, "a", nil),
	}})
	if evs[0].Seq != 1 || evs[0].TaskSeq != 1 || evs[1].Seq != 2 || evs[1].TaskSeq != 2 {
		t.Fatalf("got %+v", evs)
	}
	evs = mustCommit(t, s, mem.CommitSet{Task: newTask("b"), NewTask: true, Events: []proto.Event{
		proto.NewEvent(proto.EvAccepted, "b", nil),
	}})
	if evs[0].Seq != 3 || evs[0].TaskSeq != 1 {
		t.Fatalf("got %+v", evs)
	}
	check := func(after int64, limit, want int, firstSeq int64) {
		t.Helper()
		got, err := s.Events(ctx, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != want || (want > 0 && got[0].Seq != firstSeq) {
			t.Fatalf("Events(%d,%d) = %+v", after, limit, got)
		}
	}
	check(0, 0, 3, 1)
	check(2, 0, 1, 3)
	check(0, 2, 2, 1)
	check(3, 0, 0, 0)
}

func testInterruptsAreAppliedWithTheTask(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "one"))
	pending, _ := s.Interrupts(ctx, "a", true)
	if len(pending) != 1 || pending[0].Seq != 1 || pending[0].Text != "one" {
		t.Fatalf("pending %+v", pending)
	}
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{pending[0].Seq}})
	if pending, _ = s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
	if all, _ := s.Interrupts(ctx, "a", false); len(all) != 1 || !all[0].Applied {
		t.Fatalf("all %+v", all)
	}
}

func testAppliedNeedsATask(t *testing.T, newStore Factory) {
	s := newStore(t)
	if _, err := s.Commit(ctx, mem.CommitSet{Applied: []int64{1}}); err == nil {
		t.Fatal("Applied without Task was accepted")
	}
}

func testInterruptForUnknownOrFinishedTask(t *testing.T, newStore Factory) {
	s := newStore(t)
	if _, err := s.Commit(ctx, steer("nope", "x")); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.State, tc.Version = task.Done, 2
	mustCommit(t, s, mem.CommitSet{Task: tc})
	if _, err := s.Commit(ctx, steer("a", "x")); !errors.Is(err, mem.ErrFinished) {
		t.Fatalf("got %v, want ErrFinished", err)
	}
}

func testTerminalCommitDropsUnappliedInterrupts(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "late"))
	tc.State, tc.Version = task.Done, 2
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Events: []proto.Event{proto.NewEvent(proto.EvDone, "a", nil)}})
	if len(evs) != 2 || evs[0].Kind != proto.EvDone || evs[1].Kind != proto.EvSteerDropped {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
}

func testCallsUpsertInIndexOrder(t *testing.T, newStore Factory) {
	s := newStore(t)
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{
		{TaskID: "a", CallID: "c-2", Index: 2, Status: mem.CallPlanned},
		{TaskID: "a", CallID: "c-1", Index: 1, Status: mem.CallPlanned},
	}})
	mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{
		{TaskID: "a", CallID: "c-1", Index: 1, Status: mem.CallDone, Result: &task.ToolResult{Content: "x"}},
	}})
	calls, err := s.Calls(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].CallID != "c-1" || calls[0].Status != mem.CallDone ||
		calls[0].Result.Content != "x" || calls[1].Status != mem.CallPlanned {
		t.Fatalf("calls %+v", calls)
	}
}

func testUnfinished(t *testing.T, newStore Factory) {
	s := newStore(t)
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	done := newTask("b")
	mustCommit(t, s, mem.CommitSet{Task: done, NewTask: true})
	done.State, done.Version = task.Done, 2
	mustCommit(t, s, mem.CommitSet{Task: done})
	got, _ := s.Unfinished(ctx)
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("unfinished %+v", got)
	}
}

func testEventDataIsCopied(t *testing.T, newStore Factory) {
	s := newStore(t)
	data := []byte(`{"k":"v"}`)
	ev := proto.NewEvent(proto.EvStep, "a", nil)
	ev.Data = data
	ret := mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true, Events: []proto.Event{ev}})
	data[2] = 'X'
	ret[0].Data[2] = 'Y'
	got, _ := s.Events(ctx, 0, 0)
	if string(got[0].Data) != `{"k":"v"}` {
		t.Fatalf("store aliased commit data: %s", got[0].Data)
	}
	got[0].Data[2] = 'Z'
	again, _ := s.Events(ctx, 0, 0)
	if string(again[0].Data) != `{"k":"v"}` {
		t.Fatalf("store aliased read data: %s", again[0].Data)
	}
}

func testCallsForUnknownTaskRejected(t *testing.T, newStore Factory) {
	s := newStore(t)
	_, err := s.Commit(ctx, mem.CommitSet{Calls: []mem.CallRecord{{TaskID: "nope", CallID: "c", Index: 1}}})
	if !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if calls, _ := s.Calls(ctx, "nope"); len(calls) != 0 {
		t.Fatalf("stored %+v", calls)
	}
	// a call for the task created in the same commit is fine
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true, Calls: []mem.CallRecord{{TaskID: "a", CallID: "c", Index: 1}}})
}

func testNewTaskWithoutTask(t *testing.T, newStore Factory) {
	s := newStore(t)
	if _, err := s.Commit(ctx, mem.CommitSet{NewTask: true}); err == nil {
		t.Fatal("NewTask without Task was accepted")
	}
}

func testCommitIsAtomic(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true, Events: []proto.Event{proto.NewEvent(proto.EvAccepted, "a", nil)}})
	tc.Version = 2
	_, err := s.Commit(ctx, mem.CommitSet{
		Task:          tc,
		Calls:         []mem.CallRecord{{TaskID: "a", CallID: "c", Index: 1}},
		Events:        []proto.Event{proto.NewEvent(proto.EvStep, "a", nil)},
		NewInterrupts: []mem.Interrupt{{TaskID: "ghost", Kind: mem.InterruptSteer}},
	})
	if !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if got, _ := s.Load(ctx, "a"); got.Version != 1 {
		t.Fatalf("version %d, want 1", got.Version)
	}
	if calls, _ := s.Calls(ctx, "a"); len(calls) != 0 {
		t.Fatalf("calls %+v", calls)
	}
	if evs, _ := s.Events(ctx, 0, 0); len(evs) != 1 {
		t.Fatalf("events %+v", evs)
	}
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Events: []proto.Event{proto.NewEvent(proto.EvStep, "a", nil)}})
	if evs[0].Seq != 2 || evs[0].TaskSeq != 2 {
		t.Fatalf("gap after failed commit: %+v", evs[0])
	}
}

func twoSteers(t *testing.T, newStore Factory) (mem.Store, *task.TCB) {
	t.Helper()
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "one"))
	mustCommit(t, s, steer("a", "two"))
	return s, tc
}

func testTerminalWithAppliedDropsOnlyTheRest(t *testing.T, newStore Factory) {
	s, tc := twoSteers(t, newStore)
	tc.State, tc.Version = task.Done, 2
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{1}})
	if len(evs) != 1 || evs[0].Kind != proto.EvSteerDropped || !strings.Contains(string(evs[0].Data), `"seq":2`) {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
}

func testNonTerminalAppliedLeavesOthersPending(t *testing.T, newStore Factory) {
	s, tc := twoSteers(t, newStore)
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{1}})
	pending, _ := s.Interrupts(ctx, "a", true)
	if len(pending) != 1 || pending[0].Seq != 2 {
		t.Fatalf("pending %+v", pending)
	}
}

func testTaskSeqInterleaves(t *testing.T, newStore Factory) {
	s := newStore(t)
	e := func(id string) mem.CommitSet {
		return mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvStep, id, nil)}}
	}
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	mustCommit(t, s, mem.CommitSet{Task: newTask("b"), NewTask: true})
	var got []proto.Event
	for _, id := range []string{"a", "b", "a"} {
		got = append(got, mustCommit(t, s, e(id))...)
	}
	want := [][2]int64{{1, 1}, {2, 1}, {3, 2}}
	for i, w := range want {
		if got[i].Seq != w[0] || got[i].TaskSeq != w[1] {
			t.Fatalf("event %d = seq %d taskseq %d, want %v", i, got[i].Seq, got[i].TaskSeq, w)
		}
	}
}

func testSameVersionIsAConflict(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc}); !errors.Is(err, mem.ErrVersion) {
		t.Fatalf("got %v, want ErrVersion", err)
	}
}

func testAppliedMustBePending(t *testing.T, newStore Factory) {
	s, tc := twoSteers(t, newStore)
	tc.Version = 2
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, Applied: []int64{99}}); err == nil {
		t.Fatal("unknown seq accepted")
	}
	mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{1}})
	tc.Version = 3
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, Applied: []int64{1}}); err == nil {
		t.Fatal("already-applied seq accepted")
	}
}

func testNonSteerInterruptDroppedSilently(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "a", Kind: "other"}}})
	tc.State, tc.Version = task.Done, 2
	if evs := mustCommit(t, s, mem.CommitSet{Task: tc}); len(evs) != 0 {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("pending %+v", pending)
	}
}

// fullTask sets every field recovery reads, with non-empty slices (a store
// may keep an empty slice as nil) and a UTC time without a monotonic reading.
func fullTask(id string) *task.TCB {
	return &task.TCB{
		ID: id, State: task.Running, PC: task.StepTools, Wait: task.WaitIO, Class: "big", Priority: 1,
		ReadySince: time.Date(2026, 10, 10, 8, 30, 0, 0, time.UTC),
		System:     "sys",
		Tools:      []task.ToolSpec{{Name: "dev_x", Description: "x", Schema: json.RawMessage(`{"type":"object"}`)}},
		Regs: []task.Message{
			task.Text(task.RoleUser, "q"),
			{Role: task.RoleAssistant, Parts: []task.Part{
				{Kind: task.PartReasoning, Text: "r", Provider: "p", Opaque: json.RawMessage(`{ "sig": "a<b" }`)},
				{Kind: task.PartToolCall, CallID: "c-1", ProviderCallID: "call_9", Name: "dev_x", Args: json.RawMessage(`{"a":`), Provider: "p", Opaque: json.RawMessage(`{"id":"call_9"}`)},
			}},
			{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "boom", IsError: true}}},
		},
		Allow: []string{"dev_"}, Deny: []string{"dev_y"},
		Budget:      task.Budget{MaxSteps: 9, Deadline: time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)},
		Used:        task.Usage{Steps: 3, PromptTokens: 100, CompletionTokens: 20, ReasoningTokens: 5, CachedTokens: 64},
		CallSeq:     1,
		Unconfirmed: 2,
		StopReason:  "",
		Version:     1,
	}
}

func testFullTaskRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t)
	want := fullTask("a")
	mustCommit(t, s, mem.CommitSet{Task: want, NewTask: true})
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the task:\n got %+v\nwant %+v", got, want)
	}
	un, _ := s.Unfinished(ctx)
	if len(un) != 1 || !reflect.DeepEqual(un[0], want) {
		t.Fatalf("Unfinished changed the task: %+v", un)
	}
}

func testFullCallRecordRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t)
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	want := []mem.CallRecord{
		{TaskID: "a", CallID: "c-1", Index: 1, Tool: "dev_x", Args: json.RawMessage(`{"a":`), Status: mem.CallUnknown,
			Attempt: 2, Key: "a/c-1/2", Device: "dev", Effect: task.SideEffect, ReportUnknown: true,
			Result: &task.ToolResult{Content: "?", IsError: true}},
		{TaskID: "a", CallID: "c-2", Index: 2, Tool: "dev_y", Args: json.RawMessage(`{"b":1}`), Status: mem.CallDispatched,
			Attempt: 1, Key: "a/c-2/1", Device: "dev", Effect: task.Idempotent},
	}
	mustCommit(t, s, mem.CommitSet{Calls: want})
	got, err := s.Calls(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the calls:\n got %+v\nwant %+v", got, want)
	}
}

func testUnfinishedCoversEveryNonTerminalState(t *testing.T, newStore Factory) {
	s := newStore(t)
	states := []task.State{task.Ready, task.Running, task.Waiting, task.Suspended, task.Done, task.Failed, task.Cancelled}
	for i, st := range states {
		tc := newTask(fmt.Sprintf("t%d", i))
		tc.State = st
		mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	}
	un, _ := s.Unfinished(ctx)
	var got []task.State
	for _, tc := range un {
		got = append(got, tc.State)
	}
	if !reflect.DeepEqual(got, []task.State{task.Ready, task.Running, task.Waiting, task.Suspended}) {
		t.Fatalf("unfinished states %v", got)
	}
}

// A cancel interrupt carries its reason in Text, survives until the terminal
// commit, and is then applied without a steer_dropped event.
func testCancelInterruptIsAppliedSilently(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "s"))
	mustCommit(t, s, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "a", Kind: mem.InterruptCancel, Text: "deadline"}}})
	pending, _ := s.Interrupts(ctx, "a", true)
	if len(pending) != 2 || pending[1].Kind != mem.InterruptCancel || pending[1].Text != "deadline" {
		t.Fatalf("pending %+v", pending)
	}
	tc.State, tc.StopReason, tc.Version = task.Cancelled, "cancelled", 2
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Events: []proto.Event{proto.NewEvent(proto.EvCancelled, "a", nil)}})
	if len(evs) != 2 || evs[0].Kind != proto.EvCancelled || evs[1].Kind != proto.EvSteerDropped {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending %+v", pending)
	}
	if _, err := s.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "a", Kind: mem.InterruptCancel}}}); !errors.Is(err, mem.ErrFinished) {
		t.Fatalf("cancel of a finished task: %v, want ErrFinished", err)
	}
}

// A cancelled task's calls end cancelled or unknown in its final commit, and a
// result that arrives after it is recorded as late on the terminal task.
func testCancelledAndLateCallsRoundTrip(t *testing.T, newStore Factory) {
	s := newStore(t)
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	planned := mem.CallRecord{TaskID: "a", CallID: "c-1", Index: 1, Tool: "dev_x", Status: mem.CallPlanned, Device: "dev"}
	sent := mem.CallRecord{TaskID: "a", CallID: "c-2", Index: 2, Tool: "dev_y", Status: mem.CallDispatched, Attempt: 1, Key: "a/c-2/1", Device: "dev"}
	mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{planned, sent}})
	planned.Status, sent.Status = mem.CallCancelled, mem.CallUnknown
	tc.State, tc.Version = task.Cancelled, 2
	mustCommit(t, s, mem.CommitSet{Task: tc, Calls: []mem.CallRecord{planned, sent}})
	sent.Status, sent.Result = mem.CallLate, &task.ToolResult{Content: "paid"}
	evs := mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{sent}, Events: []proto.Event{proto.NewEvent(proto.EvToolLate, "a", nil)}})
	if len(evs) != 1 || evs[0].Kind != proto.EvToolLate {
		t.Fatalf("events %+v", evs)
	}
	got, _ := s.Calls(ctx, "a")
	if !reflect.DeepEqual(got, []mem.CallRecord{planned, sent}) {
		t.Fatalf("calls %+v", got)
	}
}
