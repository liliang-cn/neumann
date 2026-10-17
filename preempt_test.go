package neumann_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// story is what the scripted local model answers a task named name.
func story(name string) string {
	return name + ": once upon a time there was a machine that kept its queue"
}

// storyFn answers each task its story, continuing a prefill (only the rest
// comes back), and answers a steer by saying what it heard.
func storyFn(req model.Request) (model.Response, error) {
	name := sim.FirstUser(req)
	if n := len(req.Messages); n > 1 && req.Messages[n-1].Role == task.RoleUser {
		return sim.Reply(name + " heard: " + req.Messages[n-1].TextContent()), nil
	}
	return sim.Continue(req, story(name)), nil
}

// firstPiece is the first of the four pieces the paced model streams of s.
func firstPiece(s string) string {
	r := []rune(s)
	return string(r[:len(r)/4])
}

// local is a scripted local backend: it streams in four pieces a minute
// apart on the machine's clock, so a call stays in flight until the test
// moves the clock, and it records each task's latest call context, so a test
// can tell at once whether a call was cut off. onCut, when set, runs when a
// call of a task returns cut off by its context. hold, when set and true
// for a task's n-th call, keeps that call in flight with nothing streamed
// (and nothing recorded in Requests) until its context ends.
type local struct {
	*sim.Model
	mu     sync.Mutex
	ctxs   map[string]context.Context
	calls  map[string]int // calls per task
	pieces map[string]int // pieces its latest call has streamed
	onCut  func(name string)
	hold   func(name string, n int) bool
}

func (l *local) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	name := sim.FirstUser(req)
	l.mu.Lock()
	l.ctxs[name] = ctx
	l.calls[name]++
	l.pieces[name] = 0
	held := l.hold != nil && l.hold(name, l.calls[name])
	l.mu.Unlock()
	if held {
		<-ctx.Done()
		return model.Response{}, ctx.Err()
	}
	resp, err := l.Model.Stream(ctx, req, func(d model.Delta) {
		emit(d)
		l.mu.Lock()
		l.pieces[name]++
		l.mu.Unlock()
	})
	if err != nil && ctx.Err() != nil && l.onCut != nil {
		l.onCut(name)
	}
	return resp, err
}

// callsOf is how many calls name has made.
func callsOf(l *local, name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[name]
}

// cut reports whether name's latest call has been cut off, and why.
func (l *local) cut(t *testing.T, name string) (bool, error) {
	t.Helper()
	l.mu.Lock()
	ctx := l.ctxs[name]
	l.mu.Unlock()
	if ctx == nil {
		t.Fatalf("%s never called the model", name)
	}
	return ctx.Err() != nil, context.Cause(ctx)
}

func newLocal(clk *sim.Clock, caps model.Caps, fn modelFn) *local {
	if fn == nil {
		fn = storyFn
	}
	return &local{Model: &sim.Model{Fn: fn, Info: &caps, Clock: clk, Every: time.Minute, Pieces: 4},
		ctxs: map[string]context.Context{}, calls: map[string]int{}, pieces: map[string]int{}}
}

var prefillCaps = model.Caps{Tools: true, Prefill: true}

// preemptMachine runs cfg (one group, Preempt on, one vCore unless cfg says
// otherwise) on a fake clock over lm.
func preemptMachine(t *testing.T, cfg neumann.Config, lm *local, clk *sim.Clock) *neumann.Machine {
	t.Helper()
	cfg.Model, cfg.Clock, cfg.Preempt = lm, clk, true
	if cfg.Cores == 0 {
		cfg.Cores = 1
	}
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
	}
	cfg.SteerFormat = func(s string) string { return s }
	if cfg.BackoffJitter == nil {
		cfg.BackoffJitter = func(d time.Duration) time.Duration { return d }
	}
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		if err := m.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
	return m
}

// onAir waits until name's k-th model call has streamed its first piece:
// with nothing moving the clock, it waits in flight for the next one.
func onAir(t *testing.T, lm *local, name string, k int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		lm.mu.Lock()
		calls, pieces := lm.calls[name], lm.pieces[name]
		lm.mu.Unlock()
		if calls == k && pieces > 0 {
			return
		}
		if calls > k || time.Now().After(deadline) {
			t.Fatalf("%s: call %d with %d pieces, waited for call %d on air", name, calls, pieces, k)
		}
		time.Sleep(time.Millisecond)
	}
}

// drive moves the clock in steps until every task in ids has ended.
func drive(t *testing.T, m *neumann.Machine, clk *sim.Clock, step time.Duration, ids ...string) {
	t.Helper()
	driveUntil(t, clk, step, func() bool {
		for _, id := range ids {
			if tc, err := m.Load(bg, id); err != nil || !tc.State.Terminal() {
				return false
			}
		}
		return true
	})
}

func driveUntil(t *testing.T, clk *sim.Clock, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("the condition never held while the clock moved")
		}
		clk.Advance(step)
		time.Sleep(time.Millisecond)
	}
}

func submitAs(t *testing.T, m *neumann.Machine, id string, prio int) string {
	t.Helper()
	return submit(t, m, proto.Instruction{TaskID: id, Input: id, Priority: prio})
}

func preemptions(t *testing.T, m *neumann.Machine, id string) []map[string]any {
	t.Helper()
	return eventData(t, m, id, proto.EvPreempted)
}

// waitPreempted waits until id has n preempted events and returns them.
func waitPreempted(t *testing.T, m *neumann.Machine, id string, n int) []map[string]any {
	t.Helper()
	var got []map[string]any
	waitFor(t, func() bool { got = preemptions(t, m, id); return len(got) >= n })
	return got
}

// A call cut off for an interactive task keeps what it had streamed, and
// the task's next call asks the model to continue it (design §6.4): the
// request ends with that text as an assistant message, the model sends only
// the rest, and the answer is the two joined. The preempted commit carries
// the step and preempted events, no usage; the call counts as no step, no
// retry and nothing unconfirmed. The continued call's step says how much it
// continued (prefill) and when its first piece came (first_ms).
func TestPreemptedCallIsContinued(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)

	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	if cut, _ := lm.cut(t, "B"); cut {
		t.Fatal("B was cut off before its quantum")
	}
	clk.Advance(2 * time.Second)
	if cut, cause := lm.cut(t, "B"); !cut || !strings.Contains(cause.Error(), "preempted") {
		t.Fatalf("B's call after its quantum: cut %v, cause %v", cut, cause)
	}
	p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted)
	kept := firstPiece(story("B"))
	if p["by"] != "A" || p["kept"] != float64(len([]rune(kept))) || p["n"] != float64(1) || p["ran_ms"] != float64(2000) {
		t.Fatalf("preempted %v", p)
	}
	tc := loadTask(t, m, "B")
	if tc.State != task.Ready || tc.Partial != kept || tc.Preempts != 1 || tc.Used.Steps != 0 || tc.Unconfirmed != 0 || tc.Retries != 0 || !tc.ReadySince.Equal(clk.Now()) {
		t.Fatalf("B after the preemption: %+v", tc)
	}
	drive(t, m, clk, time.Minute, "A", "B")

	b := loadTask(t, m, "B")
	if b.State != task.Done || b.LastAnswer() != story("B") || b.Partial != "" || b.Preempts != 0 || b.Used.Steps != 1 || b.Unconfirmed != 0 {
		t.Fatalf("B ended %s %q: %+v", b.State, b.LastAnswer(), b)
	}
	if a := loadTask(t, m, "A"); a.LastAnswer() != story("A") {
		t.Fatalf("A answered %q", a.LastAnswer())
	}
	var continued []model.Request
	for _, r := range lm.Requests() {
		if sim.FirstUser(r) == "B" {
			continued = append(continued, r)
		}
	}
	if len(continued) != 2 || sim.Prefill(continued[0]) != "" || sim.Prefill(continued[1]) != kept {
		t.Fatalf("B's requests: %d, the second ending %q", len(continued), sim.Prefill(continued[len(continued)-1]))
	}
	var kinds []string
	for _, ev := range eventsOf(t, m, "B") {
		kinds = append(kinds, ev.Kind)
	}
	if k := strings.Join(kinds, ","); k != "accepted,model_call,step,preempted,model_call,step,usage,answer,done" {
		t.Fatalf("B's events %s", k)
	}
	steps := eventData(t, m, "B", proto.EvStep)
	if _, ok := steps[0]["prefill"]; ok || steps[1]["prefill"] != float64(len([]rune(kept))) {
		t.Fatalf("steps %v", steps)
	}
	if steps[0]["first_ms"] != float64(0) || steps[1]["first_ms"] != float64(0) {
		t.Fatalf("steps without first_ms: %v", steps)
	}
}

// Of the calls past their quantum, the least urgent task's is cut off; among
// equally urgent ones, the call that has run the shortest (design §6.4).
func TestPreemptTakesTheLeastUrgentCall(t *testing.T) {
	for _, c := range []struct {
		name         string
		first, later int
		victim       string
	}{
		{"least urgent", 2, 1, "B1"},
		{"shortest of equals", 1, 1, "B2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			lm := newLocal(clk, prefillCaps, nil)
			m := preemptMachine(t, neumann.Config{Cores: 2}, lm, clk)
			submitAs(t, m, "B1", c.first)
			onAir(t, lm, "B1", 1)
			clk.Advance(time.Second)
			submitAs(t, m, "B2", c.later)
			onAir(t, lm, "B2", 1)
			clk.Advance(5 * time.Second) // both past their quantum
			submitAs(t, m, "A", 0)
			other := map[string]string{"B1": "B2", "B2": "B1"}[c.victim]
			if cut, _ := lm.cut(t, c.victim); !cut {
				t.Fatalf("%s was not cut off", c.victim)
			}
			if cut, _ := lm.cut(t, other); cut {
				t.Fatalf("%s was cut off too", other)
			}
			p := one(t, waitPreempted(t, m, c.victim, 1), proto.EvPreempted)
			if p["by"] != "A" {
				t.Fatalf("preempted %v", p)
			}
			drive(t, m, clk, time.Minute, "A", "B1", "B2")
			if len(preemptions(t, m, other)) != 0 {
				t.Fatalf("%s was preempted", other)
			}
			for _, id := range []string{"A", "B1", "B2"} {
				if tc := loadTask(t, m, id); tc.LastAnswer() != story(id) {
					t.Fatalf("%s answered %q", id, tc.LastAnswer())
				}
			}
		})
	}
}

// A call is not cut off before it has run PreemptAfter: the check arms a
// timer for the moment it has, and cuts it off then (design §6.4).
func TestPreemptWaitsForTheQuantum(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{PreemptAfter: 3 * time.Second}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	clk.Advance(500 * time.Millisecond)
	submitAs(t, m, "A", 0)
	for _, step := range []time.Duration{time.Second, time.Second, 400 * time.Millisecond} {
		clk.Advance(step)
		if cut, _ := lm.cut(t, "B"); cut {
			t.Fatalf("B was cut off %v after it started", clk.Now().Sub(clock0))
		}
	}
	clk.Advance(100 * time.Millisecond)
	if cut, _ := lm.cut(t, "B"); !cut {
		t.Fatal("B was not cut off once its quantum was over")
	}
	if p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted); p["ran_ms"] != float64(3000) {
		t.Fatalf("preempted %v", p)
	}
	drive(t, m, clk, time.Minute, "A", "B")
}

// A step is preempted at most MaxPreempts times: after that, its call runs
// to the end whoever is waiting (design §6.4). An answer resets the count.
func TestPreemptLimitPerStep(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{MaxPreempts: 2}, lm, clk)
	submitAs(t, m, "B", 1)
	for i, a := range []string{"A1", "A2"} {
		onAir(t, lm, "B", i+1) // B's call (the first, or a continuation) is in flight
		submitAs(t, m, a, 0)
		clk.Advance(2 * time.Second)
		if cut, _ := lm.cut(t, "B"); !cut {
			t.Fatalf("B was not cut off for %s", a)
		}
		waitPreempted(t, m, "B", i+1)
		// A runs to its answer; then B continues.
		driveUntil(t, clk, time.Minute, func() bool { return loadTask(t, m, a).State == task.Done })
	}
	onAir(t, lm, "B", 3)
	submitAs(t, m, "A3", 0)
	clk.Advance(10 * time.Second)
	if cut, _ := lm.cut(t, "B"); cut {
		t.Fatal("B was cut off a third time")
	}
	drive(t, m, clk, time.Minute, "A3", "B")
	ps := preemptions(t, m, "B")
	if len(ps) != 2 || ps[0]["n"] != float64(1) || ps[1]["n"] != float64(2) {
		t.Fatalf("preempted %v", ps)
	}
	b := loadTask(t, m, "B")
	if b.LastAnswer() != story("B") || b.Preempts != 0 || b.Partial != "" {
		t.Fatalf("B ended %q, %+v", b.LastAnswer(), b)
	}
	// B's last call continued what both earlier ones had streamed.
	var last model.Request
	for _, r := range lm.Requests() {
		if sim.FirstUser(r) == "B" {
			last = r
		}
	}
	if p := sim.Prefill(last); len(p) <= len(firstPiece(story("B"))) || !strings.HasPrefix(story("B"), p) {
		t.Fatalf("B's last call continued %q", p)
	}
	if loadTask(t, m, "A3").LastAnswer() != story("A3") {
		t.Fatal("A3 did not finish")
	}
}

// Aging decides only the order tasks are taken in: a background task that
// has waited past AgingAfter competes as interactive in the queue, but does
// not cut off another background task's call (design §6.4). An interactive
// task does.
func TestAgedTaskDoesNotPreempt(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{AgingAfter: 30 * time.Second}, lm, clk)
	submitAs(t, m, "B1", 1)
	onAir(t, lm, "B1", 1)
	submitAs(t, m, "B2", 1)
	for range 4 {
		clk.Advance(10 * time.Second) // B2 ages past the threshold
		if cut, _ := lm.cut(t, "B1"); cut {
			t.Fatal("an aged background task cut off another")
		}
	}
	submitAs(t, m, "A", 0)
	if cut, _ := lm.cut(t, "B1"); !cut {
		t.Fatal("an interactive task did not cut B1 off")
	}
	if p := one(t, waitPreempted(t, m, "B1", 1), proto.EvPreempted); p["by"] != "A" {
		t.Fatalf("preempted %v", p)
	}
	// The vCore freed for A goes to A, though aged B2 has waited longer.
	onAir(t, lm, "A", 1)
	if n := callsOf(lm, "B2"); n != 0 {
		t.Fatalf("B2 made %d calls before A", n)
	}
	drive(t, m, clk, time.Minute, "A", "B1", "B2")
	for _, id := range []string{"A", "B1", "B2"} {
		if tc := loadTask(t, m, id); tc.LastAnswer() != story(id) {
			t.Fatalf("%s answered %q", id, tc.LastAnswer())
		}
	}
}

// An adapter that cannot continue a prefill keeps nothing: the preempted
// event says kept 0, Partial stays empty, and the next call starts the
// answer afresh (design §6.4).
func TestPreemptWithoutPrefillRestarts(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, model.Caps{Tools: true}, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	if p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted); p["kept"] != float64(0) {
		t.Fatalf("preempted %v", p)
	}
	if tc := loadTask(t, m, "B"); tc.Partial != "" || tc.Preempts != 1 {
		t.Fatalf("B after the preemption: %+v", tc)
	}
	drive(t, m, clk, time.Minute, "A", "B")
	for _, r := range lm.Requests() {
		if p := sim.Prefill(r); p != "" {
			t.Fatalf("a request ends with %q", p)
		}
	}
	if b := loadTask(t, m, "B"); b.LastAnswer() != story("B") || b.Used.Steps != 1 {
		t.Fatalf("B ended %q, %+v", b.LastAnswer(), b)
	}
}

// A steer for a preempted task is delivered as an interruption of its
// answer: the text the call had becomes a whole assistant turn in the
// registers, the steer follows it, and nothing is continued (design §6.4).
func TestPreemptedPartialThenSteer(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	waitPreempted(t, m, "B", 1)
	onAir(t, lm, "A", 1) // A's call is in flight; B is ready
	if err := m.Steer(bg, "B", "also mention the cat"); err != nil {
		t.Fatal(err)
	}
	drive(t, m, clk, time.Minute, "A", "B")

	b := loadTask(t, m, "B")
	kept := firstPiece(story("B"))
	n := len(b.Regs)
	if b.State != task.Done || n != 4 || b.Regs[1].Role != task.RoleAssistant || b.Regs[1].TextContent() != kept ||
		b.Regs[2].TextContent() != "also mention the cat" || b.LastAnswer() != "B heard: also mention the cat" || b.Partial != "" {
		t.Fatalf("B ended %s with registers %+v", b.State, b.Regs)
	}
	var last model.Request
	for _, r := range lm.Requests() {
		if sim.FirstUser(r) == "B" {
			last = r
		}
	}
	if k := len(last.Messages); k != 3 || last.Messages[1].TextContent() != kept || last.Messages[2].Role != task.RoleUser {
		t.Fatalf("B's last request %+v", last.Messages)
	}
	if _, ok := eventData(t, m, "B", proto.EvStep)[1]["prefill"]; ok {
		t.Fatal("the call after the steer continued a prefill")
	}
}

// A cancel and a preemption of the same call: the cancel wins, whether its
// signal reached the vCore before it settled the call, or only its commit
// had landed (the store then refuses the preempted commit). The task ends
// Cancelled with no preempted event; the urgent task still runs.
func TestCancelBeatsPreempt(t *testing.T) {
	for _, signalled := range []bool{true, false} {
		t.Run(map[bool]string{true: "signalled", false: "committed"}[signalled], func(t *testing.T) {
			clk := sim.NewClock(clock0)
			lm := newLocal(clk, prefillCaps, nil)
			store := mem.NewMemory()
			var m *neumann.Machine
			lm.onCut = func(name string) {
				if name != "B" {
					return
				}
				if signalled {
					if err := m.Cancel(bg, "B"); err != nil {
						t.Errorf("cancel: %v", err)
					}
					return
				}
				in := mem.Interrupt{TaskID: "B", Kind: mem.InterruptCancel, Text: "cancelled"}
				if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
					t.Errorf("commit the cancel: %v", err)
				}
			}
			m = preemptMachine(t, neumann.Config{Store: store}, lm, clk)
			submitAs(t, m, "B", 1)
			onAir(t, lm, "B", 1)
			submitAs(t, m, "A", 0)
			clk.Advance(2 * time.Second)
			drive(t, m, clk, time.Minute, "A", "B")
			b := loadTask(t, m, "B")
			if b.State != task.Cancelled || len(preemptions(t, m, "B")) != 0 {
				t.Fatalf("B ended %s %q with %d preemptions", b.State, b.StopReason, len(preemptions(t, m, "B")))
			}
			if a := loadTask(t, m, "A"); a.LastAnswer() != story("A") {
				t.Fatalf("A answered %q", a.LastAnswer())
			}
		})
	}
}

// A stream that breaks off (a lost response) keeps what came, committed with
// the backoff, and the retry continues it (design §6.4): the call was
// unconfirmed and a retry like any other, and the answer is whole.
func TestLostStreamIsContinued(t *testing.T) {
	full := "Once upon a time there was a stream that broke"
	clk := sim.NewClock(clock0)
	var calls atomic.Int32
	sm := &sim.Model{Info: &prefillCaps, Fn: func(req model.Request) (model.Response, error) {
		if calls.Add(1) == 1 {
			return sim.Reply("Once upon a time"), sim.Lost()
		}
		return sim.Continue(req, full), nil
	}}
	m, err := neumann.New(neumann.Config{Model: sm, Clock: clk, Logf: t.Logf, BackoffJitter: func(d time.Duration) time.Duration { return d }})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	id := submit(t, m, proto.Instruction{Input: "tell"})
	waitFor(t, func() bool { tc := loadTask(t, m, id); return tc.State == task.Waiting && tc.Wait == task.WaitBackoff })
	if tc := loadTask(t, m, id); tc.Partial != "Once upon a time" || tc.Unconfirmed != 1 || tc.Retries != 1 || tc.Used.Steps != 0 {
		t.Fatalf("after the lost stream: %+v", tc)
	}
	clk.Advance(time.Second)
	got := waitDone(t, m, id)
	if got.LastAnswer() != full || got.Partial != "" || got.Retries != 0 || got.Unconfirmed != 1 || got.Used.Steps != 1 {
		t.Fatalf("ended %q: %+v", got.LastAnswer(), got)
	}
	reqs := sm.Requests()
	if len(reqs) != 2 || sim.Prefill(reqs[1]) != "Once upon a time" {
		t.Fatalf("%d requests, the retry continued %q", len(reqs), sim.Prefill(reqs[len(reqs)-1]))
	}
	if steps := eventData(t, m, id, proto.EvStep); steps[1]["prefill"] != float64(len("Once upon a time")) {
		t.Fatalf("steps %v", steps)
	}
}

// Without Prefill, a broken stream's text is dropped: the retry starts over.
func TestLostStreamWithoutPrefillStartsOver(t *testing.T) {
	var calls atomic.Int32
	clk := sim.NewClock(clock0)
	sm := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		if calls.Add(1) == 1 {
			return sim.Reply("Once upon"), sim.Lost()
		}
		return sim.Reply("whole"), nil
	}}
	m, err := neumann.New(neumann.Config{Model: sm, Clock: clk, Logf: t.Logf, BackoffJitter: func(d time.Duration) time.Duration { return d }})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	id := submit(t, m, proto.Instruction{Input: "tell"})
	waitFor(t, func() bool { return loadTask(t, m, id).State == task.Waiting })
	if tc := loadTask(t, m, id); tc.Partial != "" {
		t.Fatalf("kept %q", tc.Partial)
	}
	clk.Advance(time.Second)
	if got := waitDone(t, m, id); got.LastAnswer() != "whole" || sim.Prefill(sm.Requests()[1]) != "" {
		t.Fatalf("ended %q", got.LastAnswer())
	}
}

// SetPriority (design §6.4): a ready task and a waiting one change priority
// in place (priority_set {priority, was}); one on a vCore is busy, a
// finished one finished, an unknown one not found; the task's own priority
// commits nothing. Made more urgent than a call in flight past its quantum,
// a ready task cuts it off at once.
func TestSetPriority(t *testing.T) {
	clk := sim.NewClock(clock0)
	var dCalls atomic.Int32
	lm := newLocal(clk, prefillCaps, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "D" && dCalls.Add(1) == 1 {
			return model.Response{}, sim.Transient()
		}
		return storyFn(req)
	})
	m := preemptMachine(t, neumann.Config{BackoffBase: time.Hour, BackoffMax: time.Hour}, lm, clk)

	submitAs(t, m, "D", 1)
	waitFor(t, func() bool { return loadTask(t, m, "D").Wait == task.WaitBackoff })
	wake := loadTask(t, m, "D").WakeAt
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "C", 1)

	if err := m.SetPriority(bg, "B", 0); !errors.Is(err, neumann.ErrTaskBusy) {
		t.Fatalf("on a vCore: %v", err)
	}
	if err := m.SetPriority(bg, "nope", 0); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	if err := m.SetPriority(bg, "C", -1); err == nil {
		t.Fatal("a negative priority was accepted")
	}
	if err := m.SetPriority(bg, "C", 1); err != nil || len(eventData(t, m, "C", proto.EvPrioritySet)) != 0 {
		t.Fatalf("the same priority: %v", err)
	}
	if err := m.SetPriority(bg, "D", 3); err != nil {
		t.Fatalf("waiting: %v", err)
	}
	if d := loadTask(t, m, "D"); d.Priority != 3 || d.State != task.Waiting || d.Wait != task.WaitBackoff || !d.WakeAt.Equal(wake) {
		t.Fatalf("D after SetPriority: %+v", d)
	}
	if q := m.Queue().Classes[0]; q.Ready != 1 || q.Background != 1 || q.Waiting != 1 {
		t.Fatalf("queue %+v", q)
	}

	clk.Advance(5 * time.Second) // B is past its quantum, but C is no more urgent
	if cut, _ := lm.cut(t, "B"); cut {
		t.Fatal("B was cut off for a task as urgent as it")
	}
	if err := m.SetPriority(bg, "C", 0); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if cut, _ := lm.cut(t, "B"); !cut {
		t.Fatal("C made urgent did not cut B off")
	}
	if p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted); p["by"] != "C" {
		t.Fatalf("preempted %v", p)
	}
	if ps := one(t, eventData(t, m, "C", proto.EvPrioritySet), proto.EvPrioritySet); ps["priority"] != float64(0) || ps["was"] != float64(1) {
		t.Fatalf("priority_set %v", ps)
	}
	if c := loadTask(t, m, "C"); c.Priority != 0 || !c.ReadySince.Equal(clock0) {
		t.Fatalf("C after SetPriority: %+v", c)
	}

	drive(t, m, clk, 10*time.Minute, "B", "C", "D")
	if err := m.SetPriority(bg, "B", 2); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("finished: %v", err)
	}
	for _, id := range []string{"B", "C", "D"} {
		if tc := loadTask(t, m, id); tc.LastAnswer() != story(id) {
			t.Fatalf("%s answered %q", id, tc.LastAnswer())
		}
	}
}

// thinker is a local backend that thinks and is cut off: it streams its
// thinking as reasoning deltas and comes back finish "length" with no parts
// and no reasoning tokens (as Ollama does), until a request turns thinking
// off or carries a cap of at least answerAt.
type thinker struct {
	caps     model.Caps
	answerAt int
	mu       sync.Mutex
	reqs     []model.Request
}

func (th *thinker) Provider() string { return "thinker" }
func (th *thinker) Caps() model.Caps { return th.caps }
func (th *thinker) MaxOut() int      { return 4096 }
func (th *thinker) requests() []model.Request {
	th.mu.Lock()
	defer th.mu.Unlock()
	return slices.Clone(th.reqs)
}

func (th *thinker) Call(ctx context.Context, req model.Request) (model.Response, error) {
	return th.Stream(ctx, req, func(model.Delta) {})
}

func (th *thinker) Stream(_ context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	th.mu.Lock()
	th.reqs = append(th.reqs, req)
	th.mu.Unlock()
	if th.answerAt > 0 && req.MaxOut >= th.answerAt || req.NoThink && th.answerAt == 0 {
		emit(model.Delta{Text: "the answer"})
		return sim.Reply("the answer"), nil
	}
	if !req.NoThink {
		emit(model.Delta{Reasoning: "let me think about it, again and again"})
	}
	return model.Response{Message: task.Message{Role: task.RoleAssistant}, FinishReason: "length", Usage: task.Usage{PromptTokens: 10, CompletionTokens: 64}}, nil
}

// A turn cut off with nothing in it but thinking, on an adapter that can
// turn thinking off, is asked again first with thinking off at the same cap
// (design §13); only if that is cut off too is the cap raised fourfold.
// NoThink lasts until a call is answered without being cut off. Without
// ThinkOff the cap is raised at once.
func TestThinkingOnlyTruncationTurnsThinkingOff(t *testing.T) {
	type sent struct {
		noThink bool
		maxOut  int
	}
	for _, c := range []struct {
		name string
		th   *thinker
		want []sent
	}{
		{"thinking off answers", &thinker{caps: model.Caps{Tools: true, ThinkOff: true}}, []sent{{false, 0}, {true, 0}}},
		{"thinking off still cut off", &thinker{caps: model.Caps{Tools: true, ThinkOff: true}, answerAt: 16384}, []sent{{false, 0}, {true, 0}, {true, 16384}}},
		{"no ThinkOff", &thinker{caps: model.Caps{Tools: true}, answerAt: 16384}, []sent{{false, 0}, {false, 16384}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, err := neumann.New(neumann.Config{Model: c.th, Logf: t.Logf})
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Start(bg); err != nil {
				t.Fatal(err)
			}
			defer stop(t, m)
			id := submit(t, m, proto.Instruction{Input: "think"})
			got := waitDone(t, m, id)
			if got.State != task.Done || got.LastAnswer() != "the answer" || got.NoThink || got.Truncations != 0 {
				t.Fatalf("ended %s %q: %+v", got.State, got.LastAnswer(), got)
			}
			var reqs []sent
			for _, r := range c.th.requests() {
				reqs = append(reqs, sent{r.NoThink, r.MaxOut})
			}
			if !slices.Equal(reqs, c.want) {
				t.Fatalf("requests %v, want %v", reqs, c.want)
			}
			rs := retries(t, m, id)
			if c.th.caps.ThinkOff {
				d := eventData(t, m, id, proto.EvModelRetry)[0]
				if d["no_think"] != true || d["max_out"] != float64(4096) || d["class"] != "truncated" {
					t.Fatalf("model_retry %v", d)
				}
				rs = rs[1:]
			}
			for _, r := range rs {
				if r.Class != "truncated" || r.MaxOut != 16384 {
					t.Fatalf("model_retry %+v", r)
				}
			}
			if got.Used.Steps != len(c.want) {
				t.Fatalf("%d steps for %d calls", got.Used.Steps, len(c.want))
			}
		})
	}
}

// Caps.Parallel caps a group's window (design §6): Max, the shorthand's
// Cores and SetCores are all held to it, and a Min above it is a
// configuration error. One request at a time reaches a backend that serves
// one; the rest wait in the machine's queue.
func TestParallelCapsTheWindow(t *testing.T) {
	gate, release := newGate()
	defer release()
	var inFlight, most atomic.Int32
	one := &sim.Model{Info: &model.Caps{Tools: true, Parallel: 1}, Fn: func(req model.Request) (model.Response, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		<-gate
		return sim.Reply("ok"), nil
	}}
	m, err := neumann.New(neumann.Config{Logf: t.Logf, Groups: []neumann.GroupConfig{{Class: "local", Model: one, Initial: 4}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	if g := m.Status().Groups[0]; g.Max != 1 || g.Target != 1 {
		t.Fatalf("group %+v", g)
	}
	if err := m.SetCores("local", 4); err != nil {
		t.Fatal(err)
	}
	if g := m.Status().Groups[0]; g.Target != 1 {
		t.Fatalf("after SetCores(4): %+v", g)
	}
	var ids []string
	for range 4 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "go", Class: "local"}))
	}
	waitFor(t, func() bool { return inFlight.Load() == 1 && m.Status().Ready == 3 })
	if g := m.Status().Groups[0]; g.Live != 1 {
		t.Fatalf("group %+v", g)
	}
	release()
	for _, id := range ids {
		waitDone(t, m, id)
	}
	if most.Load() != 1 {
		t.Fatalf("%d requests in flight at once", most.Load())
	}

	two := &sim.Model{Info: &model.Caps{Tools: true, Parallel: 2}, Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }}
	m2, err := neumann.New(neumann.Config{Model: two, Cores: 4, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if g := m2.Status().Groups[0]; g.Target != 2 || g.Max != 2 {
		t.Fatalf("shorthand: %+v", g)
	}
	m2.Stop(bg)
	if _, err := neumann.New(neumann.Config{Groups: []neumann.GroupConfig{{Class: "x", Model: two, Min: 3}}}); err == nil || !strings.Contains(err.Error(), "Parallel") {
		t.Fatalf("Min above Parallel: %v", err)
	}
	if _, err := neumann.New(neumann.Config{Groups: []neumann.GroupConfig{{Class: "x", Model: two}}, Preempt: true}); err == nil {
		t.Fatal("the shorthand's Preempt was accepted with Groups")
	}
	if _, err := neumann.New(neumann.Config{Groups: []neumann.GroupConfig{{Class: "x", Model: two, PreemptAfter: -time.Second}}}); err == nil {
		t.Fatal("a negative PreemptAfter was accepted")
	}
}

// A call taken ahead of a more urgent ready task (by aging) is checked once
// it is in flight, and cut off after its quantum: no other check would see
// it (design §6.4).
func TestPreemptACallTakenAheadByAging(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{AgingAfter: 30 * time.Second}, lm, clk)
	submitAs(t, m, "X", 0)
	onAir(t, lm, "X", 1)
	submitAs(t, m, "V", 1)
	clk.Advance(40 * time.Second) // V ages
	submitAs(t, m, "H", 0)        // X is as urgent as H: not a victim
	driveUntil(t, clk, time.Minute, func() bool { return loadTask(t, m, "X").State == task.Done })
	onAir(t, lm, "V", 1) // the aged V took the vCore ahead of H
	if tc := loadTask(t, m, "H"); tc.State != task.Ready {
		t.Fatalf("H %s", tc.State)
	}
	clk.Advance(time.Second)
	if cut, _ := lm.cut(t, "V"); cut {
		t.Fatal("V was cut off before its quantum")
	}
	clk.Advance(time.Second)
	if cut, _ := lm.cut(t, "V"); !cut {
		t.Fatal("V ran its quantum with H ready and no vCore free, and was not cut off")
	}
	if p := one(t, waitPreempted(t, m, "V", 1), proto.EvPreempted); p["by"] != "H" {
		t.Fatalf("preempted %v", p)
	}
	onAir(t, lm, "H", 1)
	drive(t, m, clk, time.Minute, "H", "V")
}

// A call that waited for room in a limiter while the urgent task arrived is
// checked once it is in flight, and cut off after its quantum.
func TestPreemptACallThatWaitedForRoom(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{
		Limits:   []string{"rpm"},
		Limiters: []limit.Config{{Name: "rpm", Limits: map[string]limit.Rate{"requests": {Per: 10 * time.Minute, Limit: 1}}}},
	}, lm, clk)
	submitAs(t, m, "X", 0)
	onAir(t, lm, "X", 1)
	submitAs(t, m, "V", 1)
	driveUntil(t, clk, time.Minute, func() bool { return loadTask(t, m, "X").State == task.Done })
	waitFor(t, func() bool { st := m.Status(); return st.Held == 1 && st.Ready == 0 }) // V waits for room on the vCore
	submitAs(t, m, "H", 0)
	if callsOf(lm, "V") != 0 {
		t.Fatal("V was sent without room")
	}
	if p := one(t, func() []map[string]any {
		var got []map[string]any
		driveUntil(t, clk, time.Second, func() bool { got = preemptions(t, m, "V"); return len(got) > 0 })
		return got
	}(), proto.EvPreempted); p["by"] != "H" || p["ran_ms"] != float64(2000) {
		t.Fatalf("preempted %v", p)
	}
	drive(t, m, clk, time.Minute, "H", "V")
}

// The vCore a preemption frees goes to the task it was freed for, not to an
// aged background task that would win the queue (design §6.4).
func TestFreedVCoreGoesToTheUrgentTask(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{AgingAfter: 30 * time.Second}, lm, clk)
	submitAs(t, m, "V", 1)
	onAir(t, lm, "V", 1)
	submitAs(t, m, "Y", 2)
	clk.Advance(40 * time.Second) // Y ages; V is past its quantum
	submitAs(t, m, "H", 0)
	waitPreempted(t, m, "V", 1)
	onAir(t, lm, "H", 1)
	if n := callsOf(lm, "Y"); n != 0 {
		t.Fatalf("aged Y made %d calls on the vCore freed for H", n)
	}
	drive(t, m, clk, time.Minute, "H", "V", "Y")
	// V was cut off once, for H. (Y, once it runs, is less urgent than V
	// and may be cut off for it in turn.)
	if p := one(t, preemptions(t, m, "V"), proto.EvPreempted); p["by"] != "H" {
		t.Fatalf("V preempted %v", p)
	}
	for _, p := range preemptions(t, m, "Y") {
		if p["by"] != "V" {
			t.Fatalf("Y preempted %v", p)
		}
	}
}

// The half-open breaker's probe is never preempted: cut off, it would never
// be reported, and the group would stay down until the breaker gave it up.
// The urgent task runs once the probe has brought the group back.
func TestProbeIsNeverPreempted(t *testing.T) {
	clk := sim.NewClock(clock0)
	var vCalls atomic.Int32
	lm := newLocal(clk, prefillCaps, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "V" && vCalls.Add(1) == 1 {
			return model.Response{}, sim.Transient()
		}
		return storyFn(req)
	})
	m, err := neumann.New(neumann.Config{
		Clock: clk, Logf: t.Logf, SteerFormat: func(s string) string { return s },
		BackoffJitter: func(d time.Duration) time.Duration { return d },
		Groups: []neumann.GroupConfig{{Class: "default", Model: lm, Initial: 1, Max: 1, Preempt: true,
			BreakerThreshold: 1, BreakerOpenFor: 30 * time.Second}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	submitAs(t, m, "V", 1)
	waitFor(t, func() bool { return vCalls.Load() == 1 })
	driveUntil(t, clk, time.Second, func() bool { return callsOf(lm, "V") == 2 }) // the probe
	onAir(t, lm, "V", 2)
	if st := m.Status().Groups[0]; st.Breaker != "half_open" {
		t.Fatalf("breaker %s", st.Breaker)
	}
	submitAs(t, m, "H", 0)
	clk.Advance(10 * time.Second)
	if cut, _ := lm.cut(t, "V"); cut {
		t.Fatal("the probe was cut off")
	}
	drive(t, m, clk, time.Minute, "H", "V")
	if n := len(preemptions(t, m, "V")); n != 0 {
		t.Fatalf("the probe was preempted %d times", n)
	}
	if st := m.Status().Groups[0]; st.Breaker != "closed" {
		t.Fatalf("breaker %s after the probe answered", st.Breaker)
	}
}

// A continued call that comes back cut off with nothing new in it, only
// thinking, is a truncation with nothing: the prefix it continued is not an
// answer by itself (design §6.4, §13). It is asked again with thinking off,
// still continuing the kept text, and the task ends with the whole answer.
func TestThinkingOnlyContinuationIsRedone(t *testing.T) {
	caps := model.Caps{Tools: true, Prefill: true, ThinkOff: true}
	var calls atomic.Int32
	sm := &sim.Model{Info: &caps, Fn: func(req model.Request) (model.Response, error) {
		switch calls.Add(1) {
		case 1:
			return sim.Reply("Once upon a"), sim.Lost()
		case 2:
			return sim.Truncated(), nil // only thinking, cut off at the cap
		}
		return sim.Continue(req, "Once upon a time it ended."), nil
	}}
	clk := sim.NewClock(clock0)
	m, err := neumann.New(neumann.Config{Model: sm, Clock: clk, Logf: t.Logf, BackoffJitter: func(d time.Duration) time.Duration { return d }})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	id := submit(t, m, proto.Instruction{Input: "tell"})
	waitFor(t, func() bool { return loadTask(t, m, id).Partial != "" })
	clk.Advance(time.Second)
	got := waitDone(t, m, id)
	if got.LastAnswer() != "Once upon a time it ended." || got.Partial != "" || got.NoThink {
		t.Fatalf("ended %s with %q: %+v", got.State, got.LastAnswer(), got)
	}
	reqs := sm.Requests()
	if len(reqs) != 3 || reqs[1].NoThink || !reqs[2].NoThink || sim.Prefill(reqs[1]) != "Once upon a" || sim.Prefill(reqs[2]) != "Once upon a" {
		t.Fatalf("%d requests", len(reqs))
	}
}

// A call cut off while the model is still thinking keeps the thinking, when
// the adapter can continue a reasoning prefix (Caps.ThinkPrefill): the next
// call ends with an assistant message holding only that reasoning, the model
// goes on thinking from it, and the answer's reasoning is the two joined
// (design §6.4). Once the answer's text has begun, only the text is kept,
// as for any preemption.
func TestPreemptedThinkingIsContinued(t *testing.T) {
	for _, c := range []struct {
		name, thinking string
		textKept       bool
	}{
		{"while thinking", "Let me think about the machine and its queue, slowly and carefully, before I answer.", false},
		{"after the text began", "Hm.", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			lm := newLocal(clk, model.Caps{Tools: true, Prefill: true, ThinkPrefill: true}, func(req model.Request) (model.Response, error) {
				return sim.ContinueThink(req, c.thinking, story(sim.FirstUser(req))), nil
			})
			m := preemptMachine(t, neumann.Config{}, lm, clk)
			submitAs(t, m, "B", 1)
			onAir(t, lm, "B", 1)
			submitAs(t, m, "A", 0)
			clk.Advance(2 * time.Second)
			p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted)
			all := []rune(c.thinking + story("B"))
			first := string(all[:len(all)/4])
			tc := loadTask(t, m, "B")
			if c.textKept {
				text := strings.TrimPrefix(first, c.thinking)
				if p["kept"] != float64(len([]rune(text))) || p["kept_think"] != float64(0) || tc.Partial != text || tc.PartialThink != "" {
					t.Fatalf("preempted %v; B %q / %q", p, tc.Partial, tc.PartialThink)
				}
			} else if p["kept"] != float64(0) || p["kept_think"] != float64(len([]rune(first))) || tc.Partial != "" || tc.PartialThink != first {
				t.Fatalf("preempted %v; B %q / %q", p, tc.Partial, tc.PartialThink)
			}
			drive(t, m, clk, time.Minute, "A", "B")

			b := loadTask(t, m, "B")
			if b.LastAnswer() != story("B") || b.Partial != "" || b.PartialThink != "" {
				t.Fatalf("B ended %q: %+v", b.LastAnswer(), b)
			}
			var last model.Request
			for _, r := range lm.Requests() {
				if sim.FirstUser(r) == "B" {
					last = r
				}
			}
			steps := eventData(t, m, "B", proto.EvStep)
			if c.textKept {
				if sim.PrefillThink(last) != "" || sim.Prefill(last) == "" || steps[1]["prefill"] == nil {
					t.Fatalf("the continuation ends %+v; steps %v", last.Messages[len(last.Messages)-1], steps)
				}
				return
			}
			if sim.PrefillThink(last) != first || steps[1]["prefill_think"] != float64(len([]rune(first))) {
				t.Fatalf("the continuation went on thinking from %q; steps %v", sim.PrefillThink(last), steps)
			}
			msg := b.Regs[len(b.Regs)-1]
			if msg.Parts[0].Kind != task.PartReasoning || msg.Parts[0].Text != c.thinking {
				t.Fatalf("the answer's reasoning %+v", msg.Parts)
			}
		})
	}
}

// A continued thought that comes back with reasoning alone and finish
// "stop" (the model stopped thinking without answering) is not taken as an
// answer, and not sent again as it was: like a thought cut off at the cap,
// it is asked again with thinking off and the kept thinking dropped
// (design §6.4, §13).
func TestAThoughtThatStopsIsRedoneWithoutThinking(t *testing.T) {
	clk := sim.NewClock(clock0)
	var calls atomic.Int32
	lm := newLocal(clk, model.Caps{Tools: true, Prefill: true, ThinkPrefill: true, ThinkOff: true}, func(req model.Request) (model.Response, error) {
		switch {
		case sim.FirstUser(req) != "B":
			return storyFn(req)
		case req.NoThink:
			return sim.Reply("B, briefly."), nil
		case calls.Add(1) == 1:
			return sim.Think("Let me think about the machine and its queue, slowly and at length.", story("B")), nil
		}
		r := sim.Think(" and so on.", "") // stopped without an answer
		r.Message.Parts = r.Message.Parts[:1]
		return r, nil
	})
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	if p := one(t, waitPreempted(t, m, "B", 1), proto.EvPreempted); p["kept_think"] == float64(0) {
		t.Fatalf("preempted %v", p)
	}
	drive(t, m, clk, time.Minute, "A", "B")
	b := loadTask(t, m, "B")
	if b.LastAnswer() != "B, briefly." || b.PartialThink != "" || b.NoThink {
		t.Fatalf("B ended %q: %+v", b.LastAnswer(), b)
	}
	var bs []model.Request
	for _, r := range lm.Requests() {
		if sim.FirstUser(r) == "B" {
			bs = append(bs, r)
		}
	}
	if len(bs) != 3 || sim.PrefillThink(bs[1]) == "" || !bs[2].NoThink || sim.PrefillThink(bs[2]) != "" {
		t.Fatalf("%d requests for B", len(bs))
	}
	if d := eventData(t, m, "B", proto.EvModelRetry); len(d) != 1 || d[0]["no_think"] != true {
		t.Fatalf("model_retry %v", d)
	}
}
