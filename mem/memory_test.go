package mem_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func newTask(id string) *task.TCB {
	return &task.TCB{ID: id, State: task.Ready, Class: "default", Regs: []task.Message{task.Text(task.RoleUser, "q")}, Version: 1}
}

func mustCommit(t *testing.T, s *mem.Memory, c mem.CommitSet) []proto.Event {
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

func TestNewTaskThenLoad(t *testing.T) {
	s := mem.NewMemory()
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

func TestVersionMustAdvanceByOne(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.Version = 3
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc}); !errors.Is(err, mem.ErrVersion) {
		t.Fatalf("got %v, want ErrVersion", err)
	}
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc})
}

func TestStoreKeepsItsOwnCopy(t *testing.T) {
	s := mem.NewMemory()
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

func TestEventsGetSequenceNumbers(t *testing.T) {
	s := mem.NewMemory()
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

func TestInterruptsAreAppliedWithTheTask(t *testing.T) {
	s := mem.NewMemory()
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

func TestAppliedNeedsATask(t *testing.T) {
	s := mem.NewMemory()
	if _, err := s.Commit(ctx, mem.CommitSet{Applied: []int64{1}}); err == nil {
		t.Fatal("Applied without Task was accepted")
	}
}

func TestInterruptForUnknownOrFinishedTask(t *testing.T) {
	s := mem.NewMemory()
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

func TestTerminalCommitDropsUnappliedInterrupts(t *testing.T) {
	s := mem.NewMemory()
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

func TestCallsUpsertInIndexOrder(t *testing.T) {
	s := mem.NewMemory()
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

func TestUnfinished(t *testing.T) {
	s := mem.NewMemory()
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

func TestEventDataIsCopied(t *testing.T) {
	s := mem.NewMemory()
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

func TestCallsForUnknownTaskRejected(t *testing.T) {
	s := mem.NewMemory()
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

func TestNewTaskWithoutTask(t *testing.T) {
	s := mem.NewMemory()
	if _, err := s.Commit(ctx, mem.CommitSet{NewTask: true}); err == nil {
		t.Fatal("NewTask without Task was accepted")
	}
}

func TestCommitIsAtomic(t *testing.T) {
	s := mem.NewMemory()
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

func twoSteers(t *testing.T) (*mem.Memory, *task.TCB) {
	t.Helper()
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "one"))
	mustCommit(t, s, steer("a", "two"))
	return s, tc
}

func TestTerminalWithAppliedDropsOnlyTheRest(t *testing.T) {
	s, tc := twoSteers(t)
	tc.State, tc.Version = task.Done, 2
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{1}})
	if len(evs) != 1 || evs[0].Kind != proto.EvSteerDropped || !strings.Contains(string(evs[0].Data), `"seq":2`) {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
}

func TestNonTerminalAppliedLeavesOthersPending(t *testing.T) {
	s, tc := twoSteers(t)
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{1}})
	pending, _ := s.Interrupts(ctx, "a", true)
	if len(pending) != 1 || pending[0].Seq != 2 {
		t.Fatalf("pending %+v", pending)
	}
}

func TestTaskSeqInterleaves(t *testing.T) {
	s := mem.NewMemory()
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

func TestSameVersionIsAConflict(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc}); !errors.Is(err, mem.ErrVersion) {
		t.Fatalf("got %v, want ErrVersion", err)
	}
}

func TestAppliedMustBePending(t *testing.T) {
	s, tc := twoSteers(t)
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

func TestNonSteerInterruptDroppedSilently(t *testing.T) {
	s := mem.NewMemory()
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
