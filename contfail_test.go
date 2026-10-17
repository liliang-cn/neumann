package neumann_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// emptyContinuation is what Ollama made of a continued thought (2026-10-17,
// qwen3.5:9b-mlx): an empty response, which the OpenAI-compatible adapter
// reports as a transient error.
func emptyContinuation() error {
	return &model.Error{Class: model.Transient, Provider: "sim", Msg: "response has neither text nor tool calls"}
}

// clockMachine runs cfg over sm on clk, backoff without jitter.
func clockMachine(t *testing.T, cfg neumann.Config, sm model.Adapter, clk *sim.Clock) *neumann.Machine {
	t.Helper()
	cfg.Model, cfg.Clock = sm, clk
	cfg.BackoffJitter = func(d time.Duration) time.Duration { return d }
	return startWith(t, cfg)
}

// waitRetries waits until id is committed waiting out its n-th backoff.
func waitRetries(t *testing.T, m *neumann.Machine, id string, n int) *task.TCB {
	t.Helper()
	var tc *task.TCB
	waitFor(t, func() bool {
		tc = loadTask(t, m, id)
		return tc.State == task.Waiting && tc.Wait == task.WaitBackoff && tc.Retries == n
	})
	return tc
}

// A stream that breaks off mid-thought keeps the thought (design §6.4), and
// the retry goes on thinking from it. A backend that answers the continued
// thought with nothing fails that call; sent again as it was, the request
// failed the same way until the task ran out of retries (seen live: seven
// identical retries). The failed continuation drops the thought in the
// commit that records the failure (model_retry says how much), and the next
// call is a fresh one, thinking left as it was.
func TestFailedThoughtContinuationStartsOver(t *testing.T) {
	thought := "Let me look at window.go first"
	var withPrefix, without atomic.Int32
	sm := &sim.Model{Info: &model.Caps{Tools: true, Prefill: true, ThinkPrefill: true, ThinkOff: true}, Fn: func(req model.Request) (model.Response, error) {
		switch {
		case sim.PrefillThink(req) != "":
			withPrefix.Add(1)
			return model.Response{}, emptyContinuation()
		case without.Add(1) == 1:
			return sim.Think(thought, ""), sim.Lost() // the stream breaks off mid-thought
		}
		return sim.Reply("fixed"), nil
	}}
	clk := sim.NewClock(clock0)
	m := clockMachine(t, neumann.Config{}, sm, clk)
	id := submit(t, m, proto.Instruction{Input: "fix it"})
	if tc := waitRetries(t, m, id, 1); tc.PartialThink != thought || tc.Partial != "" {
		t.Fatalf("after the broken stream: %+v", tc)
	}
	clk.Advance(time.Second)
	if tc := waitRetries(t, m, id, 2); tc.PartialThink != "" || tc.Partial != "" || tc.NoThink {
		t.Fatalf("after the failed continuation: %+v", tc)
	}
	clk.Advance(2 * time.Second)
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "fixed" || got.Retries != 0 || got.PartialThink != "" {
		t.Fatalf("ended %s %q: %+v", got.State, got.LastAnswer(), got)
	}
	if withPrefix.Load() != 1 || without.Load() != 2 {
		t.Fatalf("calls continuing the thought %d, fresh %d", withPrefix.Load(), without.Load())
	}
	reqs := sm.Requests()
	if len(reqs) != 3 || sim.PrefillThink(reqs[1]) != thought || sim.PrefillThink(reqs[2]) != "" || reqs[2].NoThink {
		t.Fatalf("%d requests", len(reqs))
	}
	steps := eventData(t, m, id, proto.EvStep)
	if _, ok := steps[2]["prefill_think"]; len(steps) != 3 || steps[1]["prefill_think"] != float64(len(thought)) || ok {
		t.Fatalf("steps %v", steps)
	}
	retries := eventData(t, m, id, proto.EvModelRetry)
	if len(retries) != 2 || retries[1]["dropped_think"] != float64(len(thought)) || retries[1]["attempt"] != float64(2) {
		t.Fatalf("model_retry %v", retries)
	}
	if _, ok := retries[0]["dropped_think"]; ok {
		t.Fatalf("the broken stream dropped something: %v", retries[0])
	}
}

// The same for a kept text: a continuation that keeps failing drops its
// prefix with the first failure, and the commit that records it holds the
// task without it, so a machine restarted from that commit makes a fresh
// call too.
func TestFailedContinuationStartsOver(t *testing.T) {
	kept := "Once upon a time"
	var calls atomic.Int32
	sm := &sim.Model{Info: &prefillCaps, Fn: func(req model.Request) (model.Response, error) {
		switch {
		case sim.Prefill(req) != "":
			return model.Response{}, emptyContinuation()
		case calls.Add(1) == 1:
			return sim.Reply(kept), sim.Lost()
		}
		return sim.Reply("A whole story."), nil
	}}
	store := mem.NewMemory()
	clk := sim.NewClock(clock0)
	m1 := clockMachine(t, neumann.Config{Store: store}, sm, clk)
	id := submit(t, m1, proto.Instruction{Input: "tell"})
	if tc := waitRetries(t, m1, id, 1); tc.Partial != kept || tc.Unconfirmed != 1 {
		t.Fatalf("after the broken stream: %+v", tc)
	}
	clk.Advance(time.Second)
	if tc := waitRetries(t, m1, id, 2); tc.Partial != "" || tc.PartialThink != "" || tc.Unconfirmed != 1 {
		t.Fatalf("after the failed continuation: %+v", tc)
	}
	if d := eventData(t, m1, id, proto.EvModelRetry); len(d) != 2 || d[1]["dropped"] != float64(len(kept)) || d[1]["class"] != "transient" {
		t.Fatalf("model_retry %v", d)
	}
	stop(t, m1)

	m2 := clockMachine(t, neumann.Config{Store: store}, sm, clk)
	clk.Advance(2 * time.Second)
	got := waitDone(t, m2, id)
	if got.State != task.Done || got.LastAnswer() != "A whole story." || got.Partial != "" || got.Retries != 0 {
		t.Fatalf("ended %s %q: %+v", got.State, got.LastAnswer(), got)
	}
	reqs := sm.Requests()
	if len(reqs) != 3 || sim.Prefill(reqs[1]) != kept || sim.Prefill(reqs[2]) != "" {
		t.Fatalf("%d requests, the last continuing %q", len(reqs), sim.Prefill(reqs[len(reqs)-1]))
	}
}

// A continuation whose own stream breaks off after it added to the prefix
// is not a request the backend refuses: what it added is kept with the
// prefix (design §6.4), and the retry continues the longer text.
func TestBrokenContinuationKeepsWhatItAdded(t *testing.T) {
	full := "Once upon a time there was a stream that broke twice"
	var calls atomic.Int32
	sm := &sim.Model{Info: &prefillCaps, Fn: func(req model.Request) (model.Response, error) {
		switch calls.Add(1) {
		case 1:
			return sim.Reply("Once upon"), sim.Lost()
		case 2:
			return sim.Reply(" a time"), sim.Lost()
		}
		return sim.Continue(req, full), nil
	}}
	clk := sim.NewClock(clock0)
	m := clockMachine(t, neumann.Config{}, sm, clk)
	id := submit(t, m, proto.Instruction{Input: "tell"})
	waitRetries(t, m, id, 1)
	clk.Advance(time.Second)
	if tc := waitRetries(t, m, id, 2); tc.Partial != "Once upon a time" || tc.Unconfirmed != 2 {
		t.Fatalf("after the second broken stream: %+v", tc)
	}
	clk.Advance(2 * time.Second)
	if got := waitDone(t, m, id); got.LastAnswer() != full {
		t.Fatalf("answered %q", got.LastAnswer())
	}
	reqs := sm.Requests()
	if len(reqs) != 3 || sim.Prefill(reqs[1]) != "Once upon" || sim.Prefill(reqs[2]) != "Once upon a time" {
		t.Fatalf("%d requests", len(reqs))
	}
	for _, d := range eventData(t, m, id, proto.EvModelRetry) {
		if _, ok := d["dropped"]; ok {
			t.Fatalf("model_retry %v", d)
		}
	}
}

// A continued call cut off by the machine itself, by a preemption or a
// suspend, is not a failure of the continuation: the prefix stays, with
// what the call added (design §6.4, §9.9), and the call after it continues
// the whole of it.
func TestCutOffContinuationKeepsItsPrefix(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	m := preemptMachine(t, neumann.Config{}, lm, clk)
	whole := story("B")
	// onCall waits, moving the clock a second at a time, until B's k-th
	// call has streamed its first piece.
	onCall := func(k int) {
		t.Helper()
		driveUntil(t, clk, time.Second, func() bool {
			lm.mu.Lock()
			defer lm.mu.Unlock()
			return lm.calls["B"] == k && lm.pieces["B"] > 0
		})
	}
	partial := func(was string) string {
		t.Helper()
		p := loadTask(t, m, "B").Partial
		if len(p) <= len(was) || !strings.HasPrefix(p, was) || !strings.HasPrefix(whole, p) {
			t.Fatalf("B kept %q after %q", p, was)
		}
		return p
	}

	submitAs(t, m, "B", 1)
	onAir(t, lm, "B", 1)
	submitAs(t, m, "A", 0)
	clk.Advance(2 * time.Second)
	waitPreempted(t, m, "B", 1)
	p1 := partial("")

	// The continued call is preempted in its turn.
	onCall(2)
	submitAs(t, m, "C", 0)
	clk.Advance(2 * time.Second)
	if p := waitPreempted(t, m, "B", 2)[1]; p["by"] != "C" || p["n"] != float64(2) {
		t.Fatalf("second preemption %v", p)
	}
	p2 := partial(p1)

	// And the next one suspended.
	onCall(3)
	suspend(t, m, "B")
	tc := waitSuspended(t, m, "B")
	p3 := partial(p2)
	suspendedWas(t, m, "B", "model", len([]rune(p3))-len([]rune(p2)), 0)
	if tc.Retries != 0 || tc.Unconfirmed != 0 {
		t.Fatalf("B suspended: %+v", tc)
	}
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "A", "B", "C")
	if b := loadTask(t, m, "B"); b.State != task.Done || b.LastAnswer() != whole || b.Partial != "" {
		t.Fatalf("B ended %s %q", b.State, b.LastAnswer())
	}
	var bs []model.Request
	for _, r := range lm.Requests() {
		if sim.FirstUser(r) == "B" {
			bs = append(bs, r)
		}
	}
	if len(bs) != 4 || sim.Prefill(bs[1]) != p1 || sim.Prefill(bs[2]) != p2 || sim.Prefill(bs[3]) != p3 {
		t.Fatalf("%d requests for B", len(bs))
	}
	if d := eventData(t, m, "B", proto.EvModelRetry); len(d) != 0 {
		t.Fatalf("model_retry %v", d)
	}
}
