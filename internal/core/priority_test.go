package core

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func prio(seq int64, p string) mem.Interrupt {
	return mem.Interrupt{Seq: seq, Kind: mem.InterruptPriority, Text: p}
}

// landPriority (design §6.4): the last pending priority decides and every
// one of them is applied with it, whatever the task is doing; a
// priority_set {priority, was} only when the task's priority changes (a
// change undone before it landed says nothing); a terminal commit is left
// to the store; a number that does not parse is applied and ignored.
func TestLandPriority(t *testing.T) {
	steer := mem.Interrupt{Seq: 9, Kind: mem.InterruptSteer, Text: "hi"}
	sus := mem.Interrupt{Seq: 8, Kind: mem.InterruptSuspend}
	ready := task.TCB{State: task.Ready, Priority: 2}
	io := task.TCB{State: task.Waiting, Wait: task.WaitIO, Priority: 2}
	for _, c := range []struct {
		name     string
		t        task.TCB
		pending  []mem.Interrupt
		priority int
		applied  string
		events   string
	}{
		{"nothing pending", ready, []mem.Interrupt{steer, sus}, 2, "", ""},
		{"one change", ready, []mem.Interrupt{prio(1, "0"), steer}, 0, "1", `priority_set {"priority":0,"was":2}`},
		{"the last wins", ready, []mem.Interrupt{prio(1, "5"), steer, prio(2, "0")}, 0, "1,2", `priority_set {"priority":0,"was":2}`},
		{"a change undone", ready, []mem.Interrupt{prio(1, "5"), prio(2, "2")}, 2, "1,2", ""},
		{"while its tools run", io, []mem.Interrupt{prio(1, "7")}, 7, "1", `priority_set {"priority":7,"was":2}`},
		{"not a number", ready, []mem.Interrupt{prio(1, "x"), prio(2, "-1")}, 2, "1,2", ""},
		{"finished", task.TCB{State: task.Done, Priority: 2}, []mem.Interrupt{prio(1, "0")}, 2, "", ""},
	} {
		e := &Env{Clock: sched.RealClock{}, Logf: t.Logf}
		tc := c.t
		got := e.landPriority(&tc, mem.CommitSet{Task: &tc, Applied: []int64{99}}, c.pending)
		var applied, events []string
		for _, s := range got.Applied[1:] {
			applied = append(applied, strconv.FormatInt(s, 10))
		}
		for _, ev := range got.Events {
			events = append(events, ev.Kind+" "+string(ev.Data))
		}
		if tc.Priority != c.priority || strings.Join(applied, ",") != c.applied || strings.Join(events, ";") != c.events {
			t.Errorf("%s: priority %d, applied %v, events %v", c.name, tc.Priority, applied, events)
		}
		if got.Applied[0] != 99 {
			t.Errorf("%s: the commit's own applied interrupts were lost", c.name)
		}
	}
}

// Every landing of a suspend lands the task's pending priorities too, in
// the same commit, priority_set first: a suspend that waits for the task's
// tools does not hold a priority back.
func TestLandSuspendLandsPriorities(t *testing.T) {
	e := &Env{Clock: sched.RealClock{}, Logf: t.Logf}
	sus := mem.Interrupt{Seq: 1, Kind: mem.InterruptSuspend}
	tc := task.TCB{State: task.Ready, Priority: 1}
	got := e.landSuspend(&tc, mem.CommitSet{Task: &tc}, []mem.Interrupt{sus, prio(2, "3")}, "x", "", "")
	var kinds []string
	for _, ev := range got.Events {
		kinds = append(kinds, ev.Kind)
	}
	if tc.State != task.Suspended || tc.Priority != 3 || len(got.Applied) != 2 || strings.Join(kinds, ",") != "priority_set,suspended" {
		t.Fatalf("%s, priority %d, applied %v, events %v", tc.State, tc.Priority, got.Applied, kinds)
	}
	io := task.TCB{State: task.Waiting, Wait: task.WaitIO, Priority: 1}
	got = e.landSuspend(&io, mem.CommitSet{Task: &io}, []mem.Interrupt{sus, prio(2, "3")}, "x", "", "")
	if io.State != task.Waiting || io.Priority != 3 || len(got.Applied) != 1 || got.Applied[0] != 2 {
		t.Fatalf("while tools run: %s, priority %d, applied %v", io.State, io.Priority, got.Applied)
	}
}

func addInterrupt(t *testing.T, store *mem.Memory, in mem.Interrupt) {
	t.Helper()
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
		t.Fatal(err)
	}
}

func pendingKinds(t *testing.T, store *mem.Memory) string {
	t.Helper()
	pending, err := store.Interrupts(bg, "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, in := range pending {
		out = append(out, in.Kind+":"+in.Text)
	}
	return strings.Join(out, ",")
}

// PriorityInterrupt commits a priority for a task its caller does not hold
// (design §6.4): the number goes into the interrupt, nothing about the task
// changes, and its holder lands it. The same priority as the one the task
// will have once what is pending lands commits nothing; a finished task is
// ErrFinished, an unknown one ErrNotFound.
func TestPriorityInterrupt(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	if err := e.PriorityInterrupt(bg, "t1", 0); err != nil || pendingKinds(t, store) != "" {
		t.Fatalf("its own priority: %v, pending %s", err, pendingKinds(t, store))
	}
	if err := e.PriorityInterrupt(bg, "t1", 3); err != nil {
		t.Fatal(err)
	}
	if err := e.PriorityInterrupt(bg, "t1", 3); err != nil {
		t.Fatal(err)
	}
	if err := e.PriorityInterrupt(bg, "t1", 0); err != nil {
		t.Fatal(err)
	}
	if k := pendingKinds(t, store); k != "priority:3,priority:0" {
		t.Fatalf("pending %s", k)
	}
	if got := load(t, store); got.Priority != 0 || got.Version != 1 || kinds(store) != "" {
		t.Fatalf("the task changed: %+v, events %s", got, kinds(store))
	}
	tc.State, tc.StopReason, tc.Version = task.Done, "end_turn", 2
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	if err := e.PriorityInterrupt(bg, "t1", 4); !errors.Is(err, mem.ErrFinished) {
		t.Fatalf("finished: %v", err)
	}
	if err := e.PriorityInterrupt(bg, "nope", 4); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
}

// A task nobody is at work on when its priority interrupt is committed (it
// went back to the scheduler after its caller found it busy) is taken and
// landed by PriorityInterrupt itself; one that is held is marked, and its
// holder's hand-back lands it.
func TestPriorityInterruptLandsAnIdleTask(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	e.Sched.Add(tc)
	if err := e.PriorityInterrupt(bg, "t1", 2); err != nil {
		t.Fatal(err)
	}
	if got := load(t, store); got.Priority != 2 || pendingKinds(t, store) != "" || kinds(store) != proto.EvPrioritySet {
		t.Fatalf("ready task: priority %d, pending %s, events %s", got.Priority, pendingKinds(t, store), kinds(store))
	}
	held := take(t, e)
	if err := e.PriorityInterrupt(bg, "t1", 4); err != nil {
		t.Fatal(err)
	}
	if !e.Sched.Nudged("t1") || pendingKinds(t, store) != "priority:4" {
		t.Fatalf("held task: nudged %v, pending %s", e.Sched.Nudged("t1"), pendingKinds(t, store))
	}
	if !e.handBack(bg, held, nil) {
		t.Fatal("not handed back")
	}
	if got := load(t, store); got.Priority != 4 || pendingKinds(t, store) != "" {
		t.Fatalf("after the hand-back: priority %d, pending %s", got.Priority, pendingKinds(t, store))
	}
}

// SetPriority on a task its caller holds applies any priority still
// pending for it as well: they came before the caller's, which wins.
func TestSetPriorityAppliesPendingOnes(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	addInterrupt(t, store, mem.PriorityInterrupt("t1", 5))
	if err := e.SetPriority(bg, tc, 1); err != nil {
		t.Fatal(err)
	}
	if got := load(t, store); got.Priority != 1 || tc.Priority != 1 || pendingKinds(t, store) != "" {
		t.Fatalf("priority %d / %d, pending %s", got.Priority, tc.Priority, pendingKinds(t, store))
	}
	// The task's own priority with one pending: applied, nothing said.
	addInterrupt(t, store, mem.PriorityInterrupt("t1", 5))
	if err := e.SetPriority(bg, tc, 1); err != nil {
		t.Fatal(err)
	}
	if got := load(t, store); got.Priority != 1 || pendingKinds(t, store) != "" || kinds(store) != proto.EvPrioritySet {
		t.Fatalf("priority %d, pending %s, events %s", got.Priority, pendingKinds(t, store), kinds(store))
	}
	// Nothing pending, the same priority: nothing committed.
	v := tc.Version
	if err := e.SetPriority(bg, tc, 1); err != nil || load(t, store).Version != v {
		t.Fatalf("same priority: %v, version %d", err, load(t, store).Version)
	}
}

// An instruction that starts with a priority pending lands it in its own
// commit: the model step's here, which dispatches a tool round.
func TestPriorityPendingAtTheStartLandsInTheStep(t *testing.T) {
	e, store, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo"), nil
		}
		return sim.Reply("done"), nil
	}, echoDev())
	tc := newTask(t, e, store, "q")
	addInterrupt(t, store, mem.PriorityInterrupt("t1", 3))
	e.Sched.Add(tc)
	e.step(bg, take(t, e), 0)
	e.WaitBatches()
	got := load(t, store)
	if got.Priority != 3 || pendingKinds(t, store) != "" {
		t.Fatalf("priority %d, pending %s", got.Priority, pendingKinds(t, store))
	}
	if k := kinds(store); k != "model_call,step,usage,tool_call,priority_set,tool_result" {
		t.Fatalf("events %s", k)
	}
}

// Recovery lands a pending priority wherever the task stopped: with the
// commit that settles it, or in a commit of its own for a task that
// otherwise resumes as it is.
func TestRecoverLandsAPriority(t *testing.T) {
	for _, c := range []struct {
		name  string
		state task.State
		wait  task.WaitReason
	}{
		{"running", task.Running, task.WaitNone},
		{"ready", task.Ready, task.WaitNone},
		{"suspended", task.Suspended, task.WaitNone},
		{"backoff", task.Waiting, task.WaitBackoff},
		{"approval", task.Waiting, task.WaitApproval},
		{"reconcile", task.Waiting, task.WaitReconcile},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, store, _ := newTestEnv(t, reply)
			tc := newTask(t, e, store, "q")
			tc.State, tc.Wait, tc.Version = c.state, c.wait, 2
			if _, err := store.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
				t.Fatal(err)
			}
			addInterrupt(t, store, mem.PriorityInterrupt("t1", 6))
			if _, err := e.Recover(bg, load(t, store)); err != nil {
				t.Fatal(err)
			}
			if got := load(t, store); got.Priority != 6 || pendingKinds(t, store) != "" || strings.Count(kinds(store), proto.EvPrioritySet) != 1 {
				t.Fatalf("priority %d, pending %s, events %s", got.Priority, pendingKinds(t, store), kinds(store))
			}
		})
	}
}

// A priority committed while a holder has the task out, which it then hands
// back into a wait for an operator (an approval, reconciliation), lands at
// that hand-back: such a wait refuses no hand-back (only a suspend waits for
// it to end), so the hand-back lands it rather than leaving it until the
// operator acts. A suspend pending alongside still waits for the operator,
// marked for them, and the hand-back does not spin on it.
func TestPriorityLandsAtAHandBackIntoAnOperatorWait(t *testing.T) {
	for _, c := range []struct {
		name    string
		wait    task.WaitReason
		suspend bool
	}{
		{"approval", task.WaitApproval, false},
		{"reconcile", task.WaitReconcile, false},
		{"approval with a suspend pending", task.WaitApproval, true},
		{"reconcile with a suspend pending", task.WaitReconcile, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			e, store, _ := newTestEnv(t, reply)
			e.Sched.Add(newTask(t, e, store, "q"))
			held := take(t, e)
			held.State, held.Wait = task.Waiting, c.wait
			held.Version++
			if _, err := store.Commit(bg, mem.CommitSet{Task: held}); err != nil {
				t.Fatal(err)
			}
			if c.suspend {
				if err := e.Suspend(bg, "t1"); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.PriorityInterrupt(bg, "t1", 7); err != nil {
				t.Fatal(err)
			}
			done := make(chan bool, 1)
			go func() { done <- e.handBack(bg, held, nil) }()
			select {
			case ok := <-done:
				if !ok {
					t.Fatal("not handed back")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the hand-back did not return")
			}
			got := load(t, store)
			want := ""
			if c.suspend {
				want = "suspend:"
			}
			if got.Priority != 7 || got.State != task.Waiting || got.Wait != c.wait || pendingKinds(t, store) != want {
				t.Fatalf("%s/%s at priority %d, pending %s", got.State, got.Wait, got.Priority, pendingKinds(t, store))
			}
			if e.Sched.Nudged("t1") != c.suspend {
				t.Fatalf("nudged %v", e.Sched.Nudged("t1"))
			}
			if n := strings.Count(kinds(store), proto.EvPrioritySet); n != 1 {
				t.Fatalf("%d priority_set events: %s", n, kinds(store))
			}
		})
	}
}

// heldInApproval is a task taken out of the scheduler and committed
// waiting for approval, with priority 7 committed for it while it is held.
func heldInApproval(t *testing.T, e *Env, store *mem.Memory) *task.TCB {
	t.Helper()
	e.Sched.Add(newTask(t, e, store, "q"))
	held := take(t, e)
	held.State, held.Wait = task.Waiting, task.WaitApproval
	held.Version++
	if _, err := store.Commit(bg, mem.CommitSet{Task: held}); err != nil {
		t.Fatal(err)
	}
	if err := e.PriorityInterrupt(bg, "t1", 7); err != nil {
		t.Fatal(err)
	}
	return held
}

// landWaiting ends where it cannot land: a cancel committed first
// finalises the task instead; a task someone else holds again is left to
// their hand-back; a landing that fails hands the task back still marked,
// for its next boundary.
func TestLandWaitingExits(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		e, store, _ := newTestEnv(t, reply)
		held := heldInApproval(t, e, store)
		// Committed, its owner not told yet (the gap inside Cancel).
		addInterrupt(t, store, mem.Interrupt{TaskID: "t1", Kind: mem.InterruptCancel, Text: "cancelled"})
		if !e.handBack(bg, held, nil) {
			t.Fatal("not handed back")
		}
		if got := load(t, store); got.State != task.Cancelled || got.Priority != 0 || pendingKinds(t, store) != "" {
			t.Fatalf("%s at priority %d, pending %s", got.State, got.Priority, pendingKinds(t, store))
		}
		if _, _, held := e.Sched.Counts(); held != 0 {
			t.Fatalf("%d held", held)
		}
	})
	t.Run("held by someone else", func(t *testing.T) {
		e, store, _ := newTestEnv(t, reply)
		heldInApproval(t, e, store) // still out of the scheduler
		done := make(chan struct{})
		go func() { e.landWaiting(bg, "t1"); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("landWaiting did not return")
		}
		if got := load(t, store); got.Priority != 0 || pendingKinds(t, store) != "priority:7" || !e.Sched.Nudged("t1") {
			t.Fatalf("priority %d, pending %s, nudged %v", got.Priority, pendingKinds(t, store), e.Sched.Nudged("t1"))
		}
	})
	t.Run("landing fails", func(t *testing.T) {
		e, store, _ := newTestEnv(t, reply)
		held := heldInApproval(t, e, store)
		fs := &failAfter{Store: store}
		fs.left.Store(1) // landWaiting's own read; landHeld's fails
		e.Store = fs
		if !e.handBack(bg, held, nil) {
			t.Fatal("not handed back")
		}
		e.Store = store
		if got := load(t, store); got.Priority != 0 || pendingKinds(t, store) != "priority:7" || !e.Sched.Nudged("t1") {
			t.Fatalf("priority %d, pending %s, nudged %v", got.Priority, pendingKinds(t, store), e.Sched.Nudged("t1"))
		}
		if _, waiting, held := e.Sched.Counts(); waiting != 1 || held != 0 {
			t.Fatalf("waiting %d, held %d", waiting, held)
		}
	})
}

// failAfter is a store whose interrupt reads fail once left runs out.
type failAfter struct {
	mem.Store
	left atomic.Int32
}

func (s *failAfter) Interrupts(ctx context.Context, id string, unapplied bool) ([]mem.Interrupt, error) {
	if s.left.Add(-1) < 0 {
		return nil, errors.New("disk on fire")
	}
	return s.Store.Interrupts(ctx, id, unapplied)
}

// failingInterrupts is a store whose interrupt reads fail.
type failingInterrupts struct{ mem.Store }

func (failingInterrupts) Interrupts(context.Context, string, bool) ([]mem.Interrupt, error) {
	return nil, errors.New("disk on fire")
}

// A landing that cannot read the task's interrupts leaves it marked: what
// it was nudged for (a suspend a priority's taker found pending, its mark
// cleared by the take) lands at a later boundary instead of never.
func TestLandHeldKeepsTheMarkWhenItCannotRead(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	e.Sched.Add(newTask(t, e, store, "q"))
	held := take(t, e)
	e.Store = failingInterrupts{store}
	if err := e.landHeld(bg, held); err == nil {
		t.Fatal("a failed read landed")
	}
	if !e.Sched.Nudged("t1") {
		t.Fatal("the mark was lost")
	}
}
