package neumann_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// localMachine is preemptMachine with the group's preemption as cfg says
// (off unless cfg.Preempt).
func localMachine(t *testing.T, cfg neumann.Config, lm *local, clk *sim.Clock) *neumann.Machine {
	t.Helper()
	cfg.Model, cfg.Clock = lm, clk
	if cfg.Cores == 0 {
		cfg.Cores = 1
	}
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
	}
	cfg.SteerFormat = func(s string) string { return s }
	cfg.BackoffJitter = func(d time.Duration) time.Duration { return d }
	return startWith(t, cfg)
}

func suspend(t *testing.T, m *neumann.Machine, id string) {
	t.Helper()
	if err := m.Suspend(bg, id); err != nil {
		t.Fatalf("suspend %s: %v", id, err)
	}
}

func resume(t *testing.T, m *neumann.Machine, id string) {
	t.Helper()
	if err := m.ResumeTask(bg, id); err != nil {
		t.Fatalf("resume %s: %v", id, err)
	}
}

// waitSuspended waits until id is committed Suspended.
func waitSuspended(t *testing.T, m *neumann.Machine, id string) *task.TCB {
	t.Helper()
	var tc *task.TCB
	waitFor(t, func() bool { tc = loadTask(t, m, id); return tc.State == task.Suspended })
	return tc
}

// suspendedWas checks id's one suspended event: what the task was doing,
// and what it kept.
func suspendedWas(t *testing.T, m *neumann.Machine, id, was string, kept, keptThink int) {
	t.Helper()
	d := one(t, eventData(t, m, id, proto.EvSuspended), proto.EvSuspended)
	if d["was"] != was || d["kept"] != float64(kept) || d["kept_think"] != float64(keptThink) {
		t.Fatalf("suspended %v, want was %s, kept %d / %d", d, was, kept, keptThink)
	}
}

func kinds(t *testing.T, m *neumann.Machine, id string) string {
	t.Helper()
	return strings.Join(kindsOf(t, m, id), ",")
}

// A ready task is suspended by the suspender itself, before Suspend
// returns (design §9.9): it leaves the ready queue, Status and Queue count
// it, and no model call is made for it until it is resumed; a second
// suspend commits nothing. Resumed, it is ready from now and runs. The
// errors: an unknown task, a finished one, a resume of a task that is not
// suspended.
func TestSuspendReadyTask(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := localMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "A", 0)
	onAir(t, lm, "A", 1)
	submitAs(t, m, "B", 0)

	suspend(t, m, "B")
	if tc := loadTask(t, m, "B"); tc.State != task.Suspended || tc.Wait != task.WaitNone {
		t.Fatalf("B right after Suspend: %s/%s", tc.State, tc.Wait)
	}
	suspendedWas(t, m, "B", "ready", 0, 0)
	if st := m.Status(); st.Suspended != 1 || st.Ready != 0 || st.Groups[0].Suspended != 1 {
		t.Fatalf("status %+v", st)
	}
	if q := m.Queue(); q.Classes[0].Suspended != 1 || q.Classes[0].Waiting != 1 {
		t.Fatalf("queue %+v", q)
	}
	suspend(t, m, "B") // already suspended: nothing committed
	if k := kinds(t, m, "B"); k != "accepted,suspended" {
		t.Fatalf("B's events %s", k)
	}
	if err := m.Suspend(bg, "nope"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("suspend of an unknown task: %v", err)
	}
	if err := m.ResumeTask(bg, "nope"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("resume of an unknown task: %v", err)
	}
	if err := m.ResumeTask(bg, "A"); !errors.Is(err, neumann.ErrNotSuspended) {
		t.Fatalf("resume of a running task: %v", err)
	}

	drive(t, m, clk, time.Minute, "A")
	if n := callsOf(lm, "B"); n != 0 {
		t.Fatalf("a suspended task called its model %d times", n)
	}
	clk.Advance(time.Minute)
	resume(t, m, "B")
	if d := one(t, eventData(t, m, "B", proto.EvResumed), proto.EvResumed); d["pending"] != false {
		t.Fatalf("resumed %v", d)
	}
	drive(t, m, clk, time.Minute, "B")
	b := loadTask(t, m, "B")
	if b.State != task.Done || b.LastAnswer() != story("B") {
		t.Fatalf("B ended %s %q", b.State, b.LastAnswer())
	}
	if k := kinds(t, m, "B"); k != "accepted,suspended,resumed,model_call,step,usage,answer,done" {
		t.Fatalf("B's events %s", k)
	}
	if err := m.Suspend(bg, "B"); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("suspend of a finished task: %v", err)
	}
	if err := m.ResumeTask(bg, "B"); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("resume of a finished task: %v", err)
	}
	if st := m.Status(); st.Suspended != 0 {
		t.Fatalf("status %+v", st)
	}
}

// A task backing off, or waiting for its group, is suspended at once
// (was backoff, group): its wake-up time is dropped, so the clock passing it
// does not run it, and neither does its group coming back. Resumed, it
// runs at once.
func TestSuspendBackoffAndGroupWait(t *testing.T) {
	for _, c := range []struct {
		name string
		fail error
		wait task.WaitReason
		was  string
	}{
		{"backoff", sim.Transient(), task.WaitBackoff, "backoff"},
		{"group", sim.QuotaExhausted(), task.WaitGroup, "group"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			var mu sync.Mutex
			calls := 0
			sm := &sim.Model{Fn: func(model.Request) (model.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				if calls++; calls == 1 {
					return model.Response{}, c.fail
				}
				return sim.Reply("done"), nil
			}}
			m := startWith(t, neumann.Config{Model: sm, Clock: clk, BackoffJitter: func(d time.Duration) time.Duration { return d }})
			id := submit(t, m, proto.Instruction{Input: "x"})
			waitFor(t, func() bool { tc := loadTask(t, m, id); return tc.State == task.Waiting && tc.Wait == c.wait })
			suspend(t, m, id)
			tc := loadTask(t, m, id)
			if tc.State != task.Suspended || !tc.WakeAt.IsZero() {
				t.Fatalf("after Suspend: %s/%s, wake %v", tc.State, tc.Wait, tc.WakeAt)
			}
			suspendedWas(t, m, id, c.was, 0, 0)
			clk.Advance(time.Hour)
			if err := m.GroupReset(bg, neumann.DefaultClass); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			if tc := loadTask(t, m, id); tc.State != task.Suspended || sm.Calls() != 1 {
				t.Fatalf("a suspended task woke: %s, %d calls", tc.State, sm.Calls())
			}
			resume(t, m, id)
			if got := waitDone(t, m, id); got.State != task.Done || sm.Calls() != 2 {
				t.Fatalf("ended %s after %d calls", got.State, sm.Calls())
			}
		})
	}
}

// A model call in flight is cut off by a suspend, as by a preemption
// (design §9.9), whether or not the group preempts: the commit carries the
// call's step and the suspended event (was model, the characters kept), no
// usage; the text it streamed is kept for the call after the resume to
// continue; nothing is counted (no step, no retry, nothing unconfirmed, no
// preemption).
func TestSuspendInterruptsTheModelCall(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := localMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	if cut, cause := lm.cut(t, "B"); !cut || !strings.Contains(cause.Error(), "suspended") {
		t.Fatalf("B's call after Suspend: cut %v, cause %v", cut, cause)
	}
	tc := waitSuspended(t, m, "B")
	kept := firstPiece(story("B"))
	suspendedWas(t, m, "B", "model", len([]rune(kept)), 0)
	if tc.Partial != kept || tc.Preempts != 0 || tc.Used.Steps != 0 || tc.Unconfirmed != 0 || tc.Retries != 0 {
		t.Fatalf("B suspended: %+v", tc)
	}
	if k := kinds(t, m, "B"); k != "accepted,model_call,step,suspended" {
		t.Fatalf("B's events %s", k)
	}
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "B")
	b := loadTask(t, m, "B")
	if b.State != task.Done || b.LastAnswer() != story("B") || b.Partial != "" || b.Used.Steps != 1 {
		t.Fatalf("B ended %s %q: %+v", b.State, b.LastAnswer(), b)
	}
	reqs := lm.Requests()
	if len(reqs) != 2 || sim.Prefill(reqs[1]) != kept {
		t.Fatalf("%d requests, the second ending %q", len(reqs), sim.Prefill(reqs[len(reqs)-1]))
	}
	if k := kinds(t, m, "B"); k != "accepted,model_call,step,suspended,resumed,model_call,step,usage,answer,done" {
		t.Fatalf("B's events %s", k)
	}
	if steps := eventData(t, m, "B", proto.EvStep); steps[1]["prefill"] != float64(len([]rune(kept))) {
		t.Fatalf("steps %v", steps)
	}
}

// A call cut off while it is still thinking keeps its thinking, and the
// call after the resume goes on thinking from it (design §6.4, §9.9).
func TestSuspendWhileThinkingKeepsTheThought(t *testing.T) {
	clk := sim.NewClock(clock0)
	thinking := "Let me think about the machine and its queue, slowly and carefully, before I answer."
	lm := newLocal(clk, model.Caps{Tools: true, Prefill: true, ThinkPrefill: true}, func(req model.Request) (model.Response, error) {
		return sim.ContinueThink(req, thinking, story(sim.FirstUser(req))), nil
	})
	m := localMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	tc := waitSuspended(t, m, "B")
	all := []rune(thinking + story("B"))
	first := string(all[:len(all)/4])
	suspendedWas(t, m, "B", "model", 0, len([]rune(first)))
	if tc.Partial != "" || tc.PartialThink != first {
		t.Fatalf("B kept %q / %q", tc.Partial, tc.PartialThink)
	}
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "B")
	reqs := lm.Requests()
	if b := loadTask(t, m, "B"); b.LastAnswer() != story("B") || sim.PrefillThink(reqs[len(reqs)-1]) != first {
		t.Fatalf("B answered %q, its last request thinking from %q", b.LastAnswer(), sim.PrefillThink(reqs[len(reqs)-1]))
	}
}

// toolsThenDone asks for dev_slow once, then answers.
func toolsThenDone(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) == 0 {
		return sim.CallTools("dev_slow"), nil
	}
	return sim.Reply("done"), nil
}

// Tools are never cut off (they may have effects): a task suspended while
// its round runs lands once the results are in (was tools), with its tool
// message; the next model step waits for the resume.
func TestSuspendWaitsForTools(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	m, sm := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, toolsThenDone)
	id := submit(t, m, proto.Instruction{Input: "x"})
	<-entered
	suspend(t, m, id)
	if tc := loadTask(t, m, id); tc.State != task.Waiting || tc.Wait != task.WaitIO {
		t.Fatalf("a task waiting for its tools after Suspend: %s/%s", tc.State, tc.Wait)
	}
	release()
	tc := waitSuspended(t, m, id)
	suspendedWas(t, m, id, "tools", 0, 0)
	if last := tc.Regs[len(tc.Regs)-1]; last.Role != task.RoleTool || last.Parts[0].Text != "slow done" {
		t.Fatalf("registers end %+v", last)
	}
	if st := callStatus(t, m, id); st["c-1"] != mem.CallDone {
		t.Fatalf("calls %v", st)
	}
	time.Sleep(10 * time.Millisecond)
	if sm.Calls() != 1 {
		t.Fatalf("%d model calls while suspended", sm.Calls())
	}
	resume(t, m, id)
	if got := waitDone(t, m, id); got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("ended %s %q", got.State, got.LastAnswer())
	}
}

// A round held for approval waits for the operator as before (design
// §9.9): a decision that answers the round (a denial) suspends the task in
// its commit (was approval); one that sends the round lets it run, and the
// task is suspended once its results are in (was tools).
func TestSuspendDuringApproval(t *testing.T) {
	for _, c := range []struct {
		name string
		d    neumann.ApprovalDecision
		was  string
	}{
		{"deny", neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "no"}, "approval"},
		{"allow", neumann.ApprovalDecision{}, "tools"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()},
				ToolPolicy: func(context.Context, neumann.ToolCallInfo) neumann.Decision { return neumann.RequireApproval("check") }},
				func(req model.Request) (model.Response, error) {
					if sim.ToolRounds(req) == 0 {
						return sim.CallTools("dev_echo"), nil
					}
					return sim.Reply("done"), nil
				})
			id := submit(t, m, proto.Instruction{Input: "x"})
			waitFor(t, func() bool { tc := loadTask(t, m, id); return tc.Wait == task.WaitApproval })
			suspend(t, m, id)
			if tc := loadTask(t, m, id); tc.State != task.Waiting || tc.Wait != task.WaitApproval {
				t.Fatalf("after Suspend: %s/%s", tc.State, tc.Wait)
			}
			if err := m.Approve(bg, id, "c-1", c.d); err != nil {
				t.Fatal(err)
			}
			waitSuspended(t, m, id)
			suspendedWas(t, m, id, c.was, 0, 0)
			resume(t, m, id)
			if got := waitDone(t, m, id); got.State != task.Done {
				t.Fatalf("ended %s", got.State)
			}
		})
	}
}

// A compaction call is not cut off: the suspend lands with its result, the
// summary in place (design §9.9).
func TestSuspendDuringCompaction(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			once.Do(func() { close(entered); <-gate })
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered
	suspend(t, m, id)
	close(gate)
	waitSuspended(t, m, id)
	suspendedWas(t, m, id, "model", 0, 0)
	ks := kindsOf(t, m, id)
	if n := len(ks); ks[n-2] != proto.EvCompacted || ks[n-1] != proto.EvSuspended {
		t.Fatalf("events end %v", ks[n-2:])
	}
	resume(t, m, id)
	if got := waitDone(t, m, id); got.State != task.Done || got.LastAnswer() != "done after 20" {
		t.Fatalf("ended %s %q", got.State, got.LastAnswer())
	}
}

// A task waiting for room in its limiters is cut off there: nothing is
// sent, and the suspend lands (was model). Resumed, it waits for room
// again.
func TestSuspendWhileWaitingForRoom(t *testing.T) {
	clk := sim.NewClock(clock0)
	sm := &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("done"), nil }}
	m := startWith(t, neumann.Config{Model: sm, Clock: clk,
		Limiters: []limit.Config{{Name: "gw", Limits: map[string]limit.Rate{"requests": {Per: time.Minute, Limit: 1}}}},
		Limits:   []string{"gw"}})
	a := submit(t, m, proto.Instruction{Input: "a"})
	waitDone(t, m, a)
	b := submit(t, m, proto.Instruction{Input: "b"})
	// Queued in the limiter, not merely held: a vCore holds the task before
	// its step reads the interrupts, and a suspend landing then is "ready".
	waitFor(t, func() bool { st := m.Status(); return st.Held == 1 && st.Limiters[0].Waiting == 1 })
	suspend(t, m, b)
	waitSuspended(t, m, b)
	suspendedWas(t, m, b, "model", 0, 0)
	if k := kinds(t, m, b); k != "accepted,suspended" || sm.Calls() != 1 {
		t.Fatalf("b's events %s; %d calls", k, sm.Calls())
	}
	resume(t, m, b)
	waitFor(t, func() bool { return m.Status().Held == 1 })
	clk.Advance(time.Minute)
	if got := waitDone(t, m, b); got.State != task.Done || sm.Calls() != 2 {
		t.Fatalf("b ended %s after %d calls", got.State, sm.Calls())
	}
}

// A resume before the suspend lands cancels it (design §9.9): the task
// goes on where it would have landed, and a resumed event {pending: true}
// rides on that commit, after its own events; no suspended event. A second
// resume finds nothing to resume.
func TestResumeBeforeSuspendLands(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	store := mem.NewMemory()
	m, _ := start(t, neumann.Config{Store: store, Devices: []bus.Device{gateDev(entered, gate)}}, toolsThenDone)
	id := submit(t, m, proto.Instruction{Input: "x"})
	<-entered
	suspend(t, m, id)
	resume(t, m, id)
	if err := m.ResumeTask(bg, id); !errors.Is(err, neumann.ErrNotSuspended) {
		t.Fatalf("a second resume: %v", err)
	}
	release()
	if got := waitDone(t, m, id); got.State != task.Done {
		t.Fatalf("ended %s", got.State)
	}
	if d := one(t, eventData(t, m, id, proto.EvResumed), proto.EvResumed); d["pending"] != true {
		t.Fatalf("resumed %v", d)
	}
	if k := kinds(t, m, id); k != "accepted,model_call,step,usage,tool_call,tool_result,resumed,model_call,step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
	ints, _ := store.Interrupts(bg, id, false)
	for _, in := range ints {
		if !in.Applied {
			t.Fatalf("interrupt left pending: %+v", in)
		}
	}
}

// Suspends and resumes that pile up before one lands count in seq order:
// the last decides, and every one is applied where it lands.
func TestSuspendResumeSuspendInOrder(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	store := mem.NewMemory()
	m, _ := start(t, neumann.Config{Store: store, Devices: []bus.Device{gateDev(entered, gate)}}, toolsThenDone)
	id := submit(t, m, proto.Instruction{Input: "x"})
	<-entered
	suspend(t, m, id)
	resume(t, m, id)
	suspend(t, m, id)
	release()
	waitSuspended(t, m, id)
	suspendedWas(t, m, id, "tools", 0, 0)
	if n := len(eventData(t, m, id, proto.EvResumed)); n != 0 {
		t.Fatalf("%d resumed events", n)
	}
	ints, _ := store.Interrupts(bg, id, false)
	if len(ints) != 3 || !ints[0].Applied || !ints[1].Applied || !ints[2].Applied {
		t.Fatalf("interrupts %+v", ints)
	}
	resume(t, m, id)
	if got := waitDone(t, m, id); got.State != task.Done {
		t.Fatalf("ended %s", got.State)
	}
}

// A suspended task's deadline fires, and a cancel ends it (design §9.4,
// §9.5): the canceller takes it from the wait set; its steers are reported
// dropped.
func TestDeadlineAndCancelWhileSuspended(t *testing.T) {
	for _, c := range []struct {
		name  string
		end   task.State
		why   string
		budge bool
	}{
		{"cancel", task.Cancelled, "cancelled", false},
		{"deadline", task.Failed, "deadline", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			lm := newLocal(clk, prefillCaps, nil)
			m := localMachine(t, neumann.Config{}, lm, clk)
			in := proto.Instruction{TaskID: "B", Input: "B"}
			if c.budge {
				in.Budget = inAnHour(clk)
			}
			submit(t, m, in)
			onAir(t, lm, "B", 1)
			suspend(t, m, "B")
			waitSuspended(t, m, "B")
			if err := m.Steer(bg, "B", "hello"); err != nil {
				t.Fatal(err)
			}
			if c.budge {
				clk.Advance(time.Hour)
			} else if err := m.Cancel(bg, "B"); err != nil {
				t.Fatal(err)
			}
			got := waitDone(t, m, "B")
			if got.State != c.end || got.StopReason != c.why || len(eventData(t, m, "B", proto.EvSteerDropped)) != 1 {
				t.Fatalf("ended %s %q; events %s", got.State, got.StopReason, kinds(t, m, "B"))
			}
			if st := m.Status(); st.Suspended != 0 || st.Unfinished != 0 {
				t.Fatalf("status %+v", st)
			}
		})
	}
}

// A steer to a suspended task waits in the interrupt table: no model call,
// and the first step after the resume delivers it.
func TestSteerWhileSuspended(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := localMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	waitSuspended(t, m, "B")
	if err := m.Steer(bg, "B", "also this"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if n := callsOf(lm, "B"); n != 1 {
		t.Fatalf("%d calls while suspended", n)
	}
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "B")
	reqs := lm.Requests()
	texts := sim.UserTexts(reqs[1])
	if texts[len(texts)-1] != "also this" {
		t.Fatalf("the first request after the resume ends %q", texts[len(texts)-1])
	}
	if b := loadTask(t, m, "B"); b.LastAnswer() != "B heard: also this" {
		t.Fatalf("B answered %q", b.LastAnswer())
	}
}

// An operator may change a suspended task's priority and class (design
// §9.9): each is committed, the task stays suspended, and after the resume
// it runs in its new class.
func TestSetPriorityAndClassWhileSuspended(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	other := &sim.Model{Name: "other", Fn: func(model.Request) (model.Response, error) { return sim.Reply("from b"), nil }}
	m := startWith(t, neumann.Config{Clock: clk, SteerFormat: func(s string) string { return s },
		Groups: []neumann.GroupConfig{{Class: "a", Model: lm, Max: 1}, {Class: "b", Model: other}}})
	submit(t, m, proto.Instruction{TaskID: "B", Input: "B", Class: "a"})
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	waitSuspended(t, m, "B")
	if err := m.SetPriority(bg, "B", 3); err != nil {
		t.Fatal(err)
	}
	if err := m.SetClass(bg, "B", "b"); err != nil {
		t.Fatal(err)
	}
	tc := loadTask(t, m, "B")
	if tc.State != task.Suspended || tc.Priority != 3 || tc.Class != "b" {
		t.Fatalf("B after the changes: %s, priority %d, class %s", tc.State, tc.Priority, tc.Class)
	}
	if q := m.Queue(); q.Classes[1].Suspended != 1 || q.Classes[0].Suspended != 0 {
		t.Fatalf("queue %+v", q)
	}
	resume(t, m, "B")
	if got := waitDone(t, m, "B"); got.State != task.Done || got.LastAnswer() != "from b" || other.Calls() != 1 {
		t.Fatalf("B ended %s %q", got.State, got.LastAnswer())
	}
}

// A resumed task is ready like any other: in a group that preempts, a less
// urgent call is cut off for it (design §9.9, §6.4). A task suspended while
// it was ready preempted nothing.
func TestResumeTriggersPreemption(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	suspend(t, m, "A")
	clk.Advance(2 * time.Second)
	if cut, _ := lm.cut(t, "B"); cut {
		t.Fatal("B was cut off for a suspended task")
	}
	resume(t, m, "A")
	if p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted); p["by"] != "A" {
		t.Fatalf("preempted %v", p)
	}
	drive(t, m, clk, time.Minute, "A", "B")
}

// A suspend and a preemption of the same call: whichever cut it off first
// is the call's, and the task ends suspended either way, preempted once
// (the preempted commit, then the suspend: folded into it or landed while
// it was ready) or not at all. The vCore goes to the urgent task.
func TestSuspendAPreemptedCall(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	suspend(t, m, "B")
	tc := waitSuspended(t, m, "B")
	if len(preemptions(t, m, "B")) != 1 || tc.Preempts != 1 || tc.Partial != firstPiece(story("B")) {
		t.Fatalf("B: %d preempted events, %+v", len(preemptions(t, m, "B")), tc)
	}
	one(t, eventData(t, m, "B", proto.EvSuspended), proto.EvSuspended)
	drive(t, m, clk, time.Minute, "A")
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "B")
	if b := loadTask(t, m, "B"); b.LastAnswer() != story("B") {
		t.Fatalf("B answered %q", b.LastAnswer())
	}
}

// The urgent task a preemption freed a vCore for is suspended before the
// vCore takes it: the vCore takes the next ready task instead (or, if it
// took the urgent one first, its call is cut off at once).
func TestSuspendTheTaskAVCoreWasFreedFor(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	suspend(t, m, "A")
	waitSuspended(t, m, "A")
	drive(t, m, clk, time.Minute, "B")
	resume(t, m, "A")
	drive(t, m, clk, time.Minute, "A")
	if a := loadTask(t, m, "A"); a.LastAnswer() != story("A") {
		t.Fatalf("A answered %q", a.LastAnswer())
	}
}

// A suspend during the answer check lands with the check's rejection
// (was check); one whose check passes is moot: the task ends with its
// answer.
func TestSuspendDuringTheCheck(t *testing.T) {
	for _, reject := range []bool{true, false} {
		t.Run(map[bool]string{true: "rejected", false: "passed"}[reject], func(t *testing.T) {
			in, out := make(chan struct{}, 2), make(chan struct{})
			var once sync.Once
			check := func(_ context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
				if info.Attempt == 0 {
					in <- struct{}{}
					<-out
					return neumann.AnswerVerdict{Reject: reject, Reason: "draft", Feedback: "again"}
				}
				return neumann.AnswerVerdict{}
			}
			m, _ := start(t, neumann.Config{AnswerCheck: check, AnswerCheckTimeout: time.Minute}, draftThenFinalAs("final"))
			id := submit(t, m, proto.Instruction{Input: "x"})
			<-in
			suspend(t, m, id)
			once.Do(func() { close(out) })
			if !reject {
				if got := waitDone(t, m, id); got.State != task.Done || got.LastAnswer() != "draft" {
					t.Fatalf("ended %s %q", got.State, got.LastAnswer())
				}
				if n := len(eventData(t, m, id, proto.EvSuspended)); n != 0 {
					t.Fatalf("%d suspended events", n)
				}
				return
			}
			waitSuspended(t, m, id)
			suspendedWas(t, m, id, "check", 0, 0)
			resume(t, m, id)
			if got := waitDone(t, m, id); got.State != task.Done || got.LastAnswer() != "final" {
				t.Fatalf("ended %s %q", got.State, got.LastAnswer())
			}
		})
	}
}

// A suspended task is still suspended after a restart, announced again
// (recovered, from suspended), and runs once resumed. Recovery lands what
// was committed and not landed (design §4.5, §9.9): a suspend of a task
// caught mid call (Running) suspends it instead of redoing the call, a
// resume of a suspended task makes it ready, and a suspend a resume
// cancelled is applied with a resumed {pending: true}.
func TestSuspendedSurvivesARestart(t *testing.T) {
	store := mem.NewMemory()
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m1, err := neumann.New(neumann.Config{Model: lm, Clock: clk, Cores: 1, Store: store, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	submitAs(t, m1, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m1, "B")
	waitSuspended(t, m1, "B")
	stop(t, m1)

	// What a crash leaves: a suspend committed for a task mid call, a resume
	// for a suspended one, a suspend and its resume for a ready one.
	reg := []task.Message{task.Text(task.RoleUser, "x")}
	for _, tc := range []*task.TCB{
		{ID: "R", State: task.Running, Class: neumann.DefaultClass, Regs: reg, Version: 1},
		{ID: "S", State: task.Suspended, Class: neumann.DefaultClass, Regs: []task.Message{task.Text(task.RoleUser, "S")}, Version: 1},
		{ID: "Q", State: task.Ready, Class: neumann.DefaultClass, Regs: []task.Message{task.Text(task.RoleUser, "Q")}, Version: 1},
	} {
		if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, in := range []mem.Interrupt{
		{TaskID: "R", Kind: mem.InterruptSuspend},
		{TaskID: "S", Kind: mem.InterruptResume},
		{TaskID: "Q", Kind: mem.InterruptSuspend}, {TaskID: "Q", Kind: mem.InterruptResume},
	} {
		if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
			t.Fatal(err)
		}
	}

	m2 := localMachine(t, neumann.Config{Store: store}, lm, clk)
	if tc := loadTask(t, m2, "B"); tc.State != task.Suspended || tc.Partial != firstPiece(story("B")) {
		t.Fatalf("B after the restart: %+v", tc)
	}
	if d := eventData(t, m2, "B", proto.EvRecovered); len(d) != 1 || d[0]["from"] != "suspended" {
		t.Fatalf("recovered %v", d)
	}
	r := loadTask(t, m2, "R")
	if r.State != task.Suspended || r.Unconfirmed != 1 {
		t.Fatalf("R after the restart: %s, unconfirmed %d", r.State, r.Unconfirmed)
	}
	if k := kinds(t, m2, "R"); k != "recovered,suspended" {
		t.Fatalf("R's events %s", k)
	}
	suspendedWas(t, m2, "R", "model", 0, 0)
	drive(t, m2, clk, time.Minute, "S", "Q")
	if d := one(t, eventData(t, m2, "S", proto.EvResumed), proto.EvResumed); d["pending"] != false {
		t.Fatalf("S resumed %v", d)
	}
	if d := one(t, eventData(t, m2, "Q", proto.EvResumed), proto.EvResumed); d["pending"] != true {
		t.Fatalf("Q resumed %v", d)
	}
	if n := callsOf(lm, "B"); n != 1 {
		t.Fatalf("B called its model %d times while suspended", n)
	}
	resume(t, m2, "B")
	resume(t, m2, "R")
	drive(t, m2, clk, time.Minute, "B", "R")
	if b := loadTask(t, m2, "B"); b.LastAnswer() != story("B") {
		t.Fatalf("B answered %q", b.LastAnswer())
	}
}
