package neumann_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// setPriorityNow calls SetPriority and fails the test if it does not
// return at once: on a busy task it must not wait for the instruction.
func setPriorityNow(t *testing.T, m *neumann.Machine, id string, p int) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- m.SetPriority(bg, id, p) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetPriority(%s, %d): %v", id, p, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("SetPriority(%s, %d) waited for the task's instruction", id, p)
	}
}

func pendingOf(t *testing.T, s mem.Store, id string) string {
	t.Helper()
	pending, err := s.Interrupts(bg, id, true)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, in := range pending {
		out = append(out, in.Kind+":"+in.Text)
	}
	return strings.Join(out, ",")
}

// SetPriority on a task whose model call is in flight (design §6.4)
// returns as soon as the change is committed, without touching the call;
// the task's next boundary, the model step's commit, applies it: the last
// of several changes wins, one priority_set {priority, was} records it,
// and a change to what is already pending commits nothing.
func TestSetPriorityDuringAModelCall(t *testing.T) {
	entered := make(chan struct{}, 1)
	gate, release := newGate()
	defer release()
	store := mem.NewMemory()
	m, _ := start(t, neumann.Config{Store: store, Cores: 1, Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			entered <- struct{}{}
			<-gate
			return sim.CallTools("dev_echo"), nil
		}
		return sim.Reply("done"), nil
	})
	submit(t, m, proto.Instruction{TaskID: "A", Input: "A", Priority: 2})
	<-entered
	setPriorityNow(t, m, "A", 2) // its own: nothing
	if p := pendingOf(t, store, "A"); p != "" {
		t.Fatalf("its own priority committed %s", p)
	}
	setPriorityNow(t, m, "A", 5)
	setPriorityNow(t, m, "A", 0)
	setPriorityNow(t, m, "A", 0) // what is pending already: nothing
	if p := pendingOf(t, store, "A"); p != "priority:5,priority:0" {
		t.Fatalf("pending %s", p)
	}
	if a := loadTask(t, m, "A"); a.Priority != 2 || a.State != task.Running || len(eventData(t, m, "A", proto.EvPrioritySet)) != 0 {
		t.Fatalf("A before its boundary: %s, priority %d", a.State, a.Priority)
	}
	release()
	got := waitDone(t, m, "A")
	if got.Priority != 0 || pendingOf(t, store, "A") != "" {
		t.Fatalf("A ended at priority %d, pending %s", got.Priority, pendingOf(t, store, "A"))
	}
	if ps := one(t, eventData(t, m, "A", proto.EvPrioritySet), proto.EvPrioritySet); ps["priority"] != float64(0) || ps["was"] != float64(2) {
		t.Fatalf("priority_set %v", ps)
	}
	// Landed with the model step: right after its tool_call, before the
	// round's result.
	if k := kinds(t, m, "A"); !strings.Contains(k, "step,usage,tool_call,priority_set,tool_result") {
		t.Fatalf("A's events %s", k)
	}
}

// SetPriority on a task waiting for its tools lands at the end of the
// round, and the ready queue orders the task by it from then on: made
// interactive, it goes ahead of a background task that has waited longer.
func TestSetPriorityDuringAToolCall(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	blocking := make(chan struct{}, 1)
	blocker, unblock := newGate()
	defer unblock()
	store := mem.NewMemory()
	m, sm := start(t, neumann.Config{Store: store, Cores: 1, Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		switch sim.FirstUser(req) {
		case "A":
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("dev_slow"), nil
			}
		case "B":
			blocking <- struct{}{}
			<-blocker
		}
		return sim.Reply("done"), nil
	})
	submit(t, m, proto.Instruction{TaskID: "A", Input: "A", Priority: 2})
	<-entered
	submit(t, m, proto.Instruction{TaskID: "B", Input: "B"})
	<-blocking // B holds the one vCore
	submit(t, m, proto.Instruction{TaskID: "C", Input: "C", Priority: 1})
	setPriorityNow(t, m, "A", 0)
	if a := loadTask(t, m, "A"); a.Priority != 2 || a.Wait != task.WaitIO {
		t.Fatalf("A before its round ended: %s/%s, priority %d", a.State, a.Wait, a.Priority)
	}
	release()
	waitFor(t, func() bool { a := loadTask(t, m, "A"); return a.State == task.Ready && a.Priority == 0 })
	if k := kinds(t, m, "A"); !strings.HasSuffix(k, "tool_result,priority_set") {
		t.Fatalf("A's events %s", k)
	}
	if q := m.Queue().Classes[0]; q.Ready != 2 || q.Background != 1 {
		t.Fatalf("queue %+v", q)
	}
	unblock()
	for _, id := range []string{"A", "B", "C"} {
		waitFor(t, func() bool { return loadTask(t, m, id).State.Terminal() })
	}
	var order []string
	for _, req := range sm.Requests() {
		order = append(order, sim.FirstUser(req))
	}
	if o := strings.Join(order, ","); o != "A,B,A,C" {
		t.Fatalf("model calls in order %s", o)
	}
	if n := len(eventData(t, m, "A", proto.EvPrioritySet)); n != 1 {
		t.Fatalf("%d priority_set events", n)
	}
}

// A task made less urgent while its call runs is judged by the priority
// the call was sent with until its boundary (design §6.4): a more urgent
// task than its new priority does not cut the call off; at the boundary
// the task is queued behind it.
func TestSetPriorityLessUrgentDoesNotPreemptTheCall(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "A" && sim.ToolRounds(req) == 0 {
			r := sim.CallTools("dev_echo")
			r.Message.Parts = append([]task.Part{{Kind: task.PartText, Text: story("A")}}, r.Message.Parts...)
			return r, nil
		}
		return storyFn(req)
	})
	m := preemptMachine(t, neumann.Config{Devices: []bus.Device{echoDev()}}, lm, clk)
	submitAs(t, m, "A", 0)
	onAir(t, lm, "A", 1)
	submitAs(t, m, "C", 1)
	setPriorityNow(t, m, "A", 3)
	clk.Advance(5 * time.Second) // A's call is past its quantum
	if cut, _ := lm.cut(t, "A"); cut {
		t.Fatal("A's call was cut off before its boundary")
	}
	drive(t, m, clk, time.Minute, "A", "C")
	if n := len(preemptions(t, m, "A")); n != 0 {
		t.Fatalf("A preempted %d times", n)
	}
	if ps := one(t, eventData(t, m, "A", proto.EvPrioritySet), proto.EvPrioritySet); ps["priority"] != float64(3) || ps["was"] != float64(0) {
		t.Fatalf("priority_set %v", ps)
	}
	var order []string
	for _, req := range lm.Requests() {
		order = append(order, sim.FirstUser(req))
	}
	if o := strings.Join(order, ","); o != "A,C,A" {
		t.Fatalf("model calls in order %s", o)
	}
}

// A priority pending on a task that then ends is dropped: a cancel's final
// commit applies it, nothing reports it, and the task keeps its priority.
func TestSetPriorityThenCancel(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	store := mem.NewMemory()
	m, _ := start(t, neumann.Config{Store: store, Devices: []bus.Device{gateDev(entered, gate)}}, toolsThenDone)
	id := submit(t, m, proto.Instruction{Input: "x", Priority: 2})
	<-entered
	setPriorityNow(t, m, id, 0)
	if err := m.Cancel(bg, id); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	if got.State != task.Cancelled || got.Priority != 2 || pendingOf(t, store, id) != "" {
		t.Fatalf("%s at priority %d, pending %s", got.State, got.Priority, pendingOf(t, store, id))
	}
	if n := len(eventData(t, m, id, proto.EvPrioritySet)); n != 0 {
		t.Fatalf("%d priority_set events", n)
	}
	if err := m.SetPriority(bg, id, 1); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("finished: %v", err)
	}
}

// A priority committed and not landed survives a restart: recovery lands
// it, with the commit that settles a task caught mid call, or in one of its
// own for a task that resumes as it is.
func TestSetPriorityPendingSurvivesARestart(t *testing.T) {
	store := mem.NewMemory()
	reg := []task.Message{task.Text(task.RoleUser, "x")}
	for _, tc := range []*task.TCB{
		{ID: "R", State: task.Running, Class: neumann.DefaultClass, Priority: 2, Regs: reg, Version: 1},
		{ID: "Q", State: task.Ready, Class: neumann.DefaultClass, Priority: 2, Regs: reg, Version: 1},
		{ID: "S", State: task.Suspended, Class: neumann.DefaultClass, Priority: 2, Regs: reg, Version: 1},
	} {
		if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{mem.PriorityInterrupt(tc.ID, 0)}}); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := start(t, neumann.Config{Store: store}, func(model.Request) (model.Response, error) { return sim.Reply("done"), nil })
	for _, id := range []string{"R", "Q", "S"} {
		if ps := one(t, eventData(t, m, id, proto.EvPrioritySet), proto.EvPrioritySet); ps["priority"] != float64(0) || ps["was"] != float64(2) {
			t.Fatalf("%s: priority_set %v", id, ps)
		}
		if p := pendingOf(t, store, id); p != "" {
			t.Fatalf("%s: pending %s", id, p)
		}
	}
	if s := loadTask(t, m, "S"); s.State != task.Suspended || s.Priority != 0 {
		t.Fatalf("S after the restart: %s, priority %d", s.State, s.Priority)
	}
	if k := kinds(t, m, "R"); !strings.HasPrefix(k, "recovered,priority_set") {
		t.Fatalf("R's events %s", k)
	}
}
