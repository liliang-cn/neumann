package storetest

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// The retention cases run only for a Store that is a mem.EventPruner: one
// without it keeps every event, which is what a Store has always done.

func pruner(t *testing.T, s mem.Store) mem.EventPruner {
	t.Helper()
	p, ok := s.(mem.EventPruner)
	if !ok {
		t.Skip("store is not a mem.EventPruner")
	}
	return p
}

// later waits until the wall clock is past at, so that every commit from
// now on is stamped after every event stamped at or before at. The cases
// draw the line between old and young events with it; a store whose clock
// does not follow the wall clock cannot run them.
func later(t *testing.T, at time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !time.Now().After(at) {
		if time.Now().After(deadline) {
			t.Fatalf("the clock did not pass %v", at)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func ev(kind, id string) proto.Event { return proto.NewEvent(kind, id, nil) }

func finish(tc *task.TCB) *task.TCB {
	tc = tc.Clone()
	tc.Version++
	tc.State, tc.StopReason = task.Done, "end_turn"
	return tc
}

func seqs(evs []proto.Event) []int64 {
	out := make([]int64, len(evs))
	for i, e := range evs {
		out[i] = e.Seq
	}
	return out
}

func mustPrune(t *testing.T, p mem.EventPruner, before time.Time, limit int) int {
	t.Helper()
	n, err := p.PruneEvents(ctx, before, limit)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	return n
}

// retentionLog commits a log with every kind of event retention tells apart
// and returns it, with the time that divides its old events from its young
// ones and the seqs of the events eligible as of that time.
//
//	seq  task   kind      why
//	1    done   accepted  pruned: done, every event of it old
//	2    run    accepted  kept: run is unfinished
//	3    done   done      pruned
//	4    ""     core_up   pruned: machine-level and old
//	5    late   accepted  kept: late has a young event (8)
//	6    late   done      kept
//	7    ghost  step      pruned: no such task, and old
//	8    run    step      kept: unfinished
//	--- old above, young below ---
//	9    late   tool_late kept: young
//	10   ""     core_up   kept: young
//	11   young  accepted  kept: young
//	12   young  done      kept: young, and the newest event
func retentionLog(t *testing.T, s mem.Store) (log []proto.Event, before time.Time, eligible []int64) {
	t.Helper()
	run, done, late, young := newTask("run"), newTask("done"), newTask("late"), newTask("young")
	commit := func(c mem.CommitSet) { log = append(log, mustCommit(t, s, c)...) }
	commit(mem.CommitSet{Task: done, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "done")}})
	commit(mem.CommitSet{Task: run, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "run")}})
	commit(mem.CommitSet{Task: finish(done), Events: []proto.Event{ev(proto.EvDone, "done")}})
	commit(mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	commit(mem.CommitSet{Task: late, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "late")}})
	commit(mem.CommitSet{Task: finish(late), Events: []proto.Event{ev(proto.EvDone, "late")}})
	commit(mem.CommitSet{Events: []proto.Event{ev(proto.EvStep, "ghost")}})
	commit(mem.CommitSet{Events: []proto.Event{ev(proto.EvStep, "run")}})
	later(t, log[len(log)-1].Time)
	commit(mem.CommitSet{Events: []proto.Event{ev(proto.EvToolLate, "late")}})
	commit(mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	commit(mem.CommitSet{Task: young, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "young")}})
	commit(mem.CommitSet{Task: finish(young), Events: []proto.Event{ev(proto.EvDone, "young")}})
	if len(log) != 12 {
		t.Fatalf("committed %d events, want 12", len(log))
	}
	return log, log[8].Time, []int64{1, 3, 4, 7}
}

func without(log []proto.Event, gone []int64) []proto.Event {
	return slices.DeleteFunc(slices.Clone(log), func(e proto.Event) bool { return slices.Contains(gone, e.Seq) })
}

// sameEvents fails unless got is want, field for field.
func sameEvents(t *testing.T, what string, got, want []proto.Event) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: seqs %v, want %v", what, seqs(got), seqs(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Seq != w.Seq || g.TaskSeq != w.TaskSeq || g.TaskID != w.TaskID || g.Kind != w.Kind || !g.Time.Equal(w.Time) {
			t.Fatalf("%s: event %d = %+v, want %+v", what, i, g, w)
		}
	}
}

func testPruneOnlyEligibleEvents(t *testing.T, newStore Factory) {
	s := newStore(t)
	p := pruner(t, s)
	log, before, eligible := retentionLog(t, s)
	if n := mustPrune(t, p, before, 0); n != len(eligible) {
		t.Fatalf("pruned %d events, want %d", n, len(eligible))
	}
	kept := without(log, eligible)
	all, err := s.Events(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sameEvents(t, "after prune", all, kept)
	// Again, with nothing new eligible: nothing changes.
	if n := mustPrune(t, p, before, 0); n != 0 {
		t.Fatalf("second prune deleted %d", n)
	}
	all, _ = s.Events(ctx, 0, 0)
	sameEvents(t, "after a second prune", all, kept)
	// Reads go on by seq across the gaps.
	read := func(after int64, limit int, want ...int64) {
		t.Helper()
		got, err := s.Events(ctx, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(seqs(got), want) {
			t.Fatalf("Events(%d, %d) = seqs %v, want %v", after, limit, seqs(got), want)
		}
	}
	read(0, 2, 2, 5)
	read(2, 2, 5, 6)
	read(3, 1, 5) // after a pruned seq: the next kept one
	read(6, 2, 8, 9)
	read(7, 0, 8, 9, 10, 11, 12)
	read(12, 0)
}

// A bounded prune deletes the oldest eligible events first, and pruning in
// bounded steps ends where one unbounded prune does.
func testPruneOldestFirstWithinLimit(t *testing.T, newStore Factory) {
	s := newStore(t)
	p := pruner(t, s)
	log, before, eligible := retentionLog(t, s)
	for i := range eligible {
		if n := mustPrune(t, p, before, 1); n != 1 {
			t.Fatalf("step %d pruned %d, want 1", i, n)
		}
		all, _ := s.Events(ctx, 0, 0)
		sameEvents(t, fmt.Sprintf("step %d", i), all, without(log, eligible[:i+1]))
	}
	if n := mustPrune(t, p, before, 1); n != 0 {
		t.Fatalf("pruned %d once nothing was eligible", n)
	}
}

// The newest event is never pruned, so a Seq is never handed out twice,
// and TaskSeq goes on where it was.
func testPruneKeepsTheNewestEvent(t *testing.T, newStore Factory) {
	s := newStore(t)
	p := pruner(t, s)
	a := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: a, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "a")}})
	mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	last := mustCommit(t, s, mem.CommitSet{Task: finish(a), Events: []proto.Event{ev(proto.EvDone, "a")}})
	later(t, last[0].Time)
	future := time.Now().Add(time.Hour)
	// a's events go together or not at all: its newest is the log's newest,
	// so none of them goes, and the machine-level event alone does.
	if n := mustPrune(t, p, future, 0); n != 1 {
		t.Fatalf("pruned %d, want 1 (the core_up)", n)
	}
	mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	if n := mustPrune(t, p, future, 0); n != 2 {
		t.Fatalf("pruned %d, want a's two events", n)
	}
	all, _ := s.Events(ctx, 0, 0)
	if !slices.Equal(seqs(all), []int64{4}) {
		t.Fatalf("left seqs %v, want [4]", seqs(all))
	}
	if n := mustPrune(t, p, future, 0); n != 0 {
		t.Fatalf("pruned the newest event (%d)", n)
	}
	next := mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, ""), ev(proto.EvStep, "a")}})
	if next[0].Seq != 5 || next[0].TaskSeq != 3 || next[1].Seq != 6 || next[1].TaskSeq != 3 {
		t.Fatalf("committed after pruning: %+v, want seqs 5, 6 and task seqs 3, 3", next)
	}
}

// An unfinished task's events are kept however old they are; once it
// finishes they go together, when its last one is old enough.
func testPruneWaitsForTheTaskToFinish(t *testing.T, newStore Factory) {
	s := newStore(t)
	p := pruner(t, s)
	run := newTask("run")
	mustCommit(t, s, mem.CommitSet{Task: run, NewTask: true, Events: []proto.Event{ev(proto.EvAccepted, "run")}})
	old := mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvStep, "run"), ev(proto.EvStep, "run")}})
	later(t, old[1].Time)
	up := mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	if n := mustPrune(t, p, time.Now().Add(time.Hour), 0); n != 0 {
		t.Fatalf("pruned %d events of an unfinished task", n)
	}
	later(t, up[0].Time)
	end := mustCommit(t, s, mem.CommitSet{Task: finish(run), Events: []proto.Event{ev(proto.EvDone, "run")}})
	if n := mustPrune(t, p, end[0].Time, 0); n != 1 {
		t.Fatalf("pruned %d as of the task's end, want only the core_up", n)
	}
	mustCommit(t, s, mem.CommitSet{Events: []proto.Event{ev(proto.EvCoreUp, "")}})
	later(t, end[0].Time)
	if n := mustPrune(t, p, time.Now(), 0); n != 4 {
		t.Fatalf("pruned %d once the task's last event was old, want its 4", n)
	}
	all, _ := s.Events(ctx, 0, 0)
	if !slices.Equal(seqs(all), []int64{6}) {
		t.Fatalf("left seqs %v, want [6]", seqs(all))
	}
}

func testPruneNothing(t *testing.T, newStore Factory) {
	s := newStore(t)
	p := pruner(t, s)
	if n := mustPrune(t, p, time.Now().Add(time.Hour), 0); n != 0 {
		t.Fatalf("pruned %d from an empty log", n)
	}
	log, _, _ := retentionLog(t, s)
	if n := mustPrune(t, p, log[0].Time, 0); n != 0 {
		t.Fatalf("pruned %d as of the first event's time", n)
	}
	all, _ := s.Events(ctx, 0, 0)
	sameEvents(t, "unpruned", all, log)
}
