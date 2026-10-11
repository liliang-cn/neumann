package neumann_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// checker is an answer check that notes what it is shown and answers by fn.
type checker struct {
	mu    sync.Mutex
	shown []neumann.AnswerInfo
	fn    func(ctx context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict
}

func (c *checker) check(ctx context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
	c.mu.Lock()
	c.shown = append(c.shown, info)
	c.mu.Unlock()
	return c.fn(ctx, info)
}

func (c *checker) attempts() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for _, in := range c.shown {
		out = append(out, in.Attempt)
	}
	return out
}

// rejectDrafts rejects the answer "draft" and passes any other.
func rejectDrafts(_ context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
	if info.Answer == "draft" {
		return neumann.AnswerVerdict{Reject: true, Reason: "a draft", Feedback: "Give the final figure."}
	}
	return neumann.AnswerVerdict{Reason: "fine"}
}

// draftThenFinal answers "draft" until it has been given feedback, then
// "final".
func draftThenFinal(req model.Request) (model.Response, error) { return draftThenFinalAs("final")(req) }

// draftThenFinalAs answers "draft" until it has been given feedback, then
// final.
func draftThenFinalAs(final string) modelFn {
	return func(req model.Request) (model.Response, error) {
		if slices.ContainsFunc(req.Messages, func(m task.Message) bool { return m.Feedback }) {
			return sim.Reply(final), nil
		}
		return sim.Reply("draft"), nil
	}
}

// eventData is the data of id's events of kind, in order.
func eventData(t *testing.T, m *neumann.Machine, id, kind string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind != kind {
			continue
		}
		var d map[string]any
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	return out
}

func one(t *testing.T, list []map[string]any, kind string) map[string]any {
	t.Helper()
	if len(list) != 1 {
		t.Fatalf("%d %s events: %v", len(list), kind, list)
	}
	return list[0]
}

func TestAnswerCheckRejectsThenPasses(t *testing.T) {
	c := &checker{fn: rejectDrafts}
	m, sm := start(t, neumann.Config{AnswerCheck: c.check}, draftThenFinal)
	id := submit(t, m, proto.Instruction{Input: "what is the total?"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != "final" {
		t.Fatalf("ended %s %q with %q", got.State, got.StopReason, got.LastAnswer())
	}
	if !slices.Equal(c.attempts(), []int{0, 1}) {
		t.Fatalf("attempts %v", c.attempts())
	}
	first := c.shown[0]
	if first.TaskID != id || first.Class != neumann.DefaultClass || first.Request != "what is the total?" || first.Answer != "draft" || c.shown[1].Answer != "final" {
		t.Fatalf("shown %+v", c.shown)
	}
	if got.Rejections != 1 || got.Used.Steps != 2 || sm.Calls() != 2 {
		t.Fatalf("rejections %d, steps %d, calls %d", got.Rejections, got.Used.Steps, sm.Calls())
	}
	want := []struct {
		role     task.Role
		text     string
		feedback bool
	}{{task.RoleUser, "what is the total?", false}, {task.RoleAssistant, "draft", false}, {task.RoleUser, "Give the final figure.", true}, {task.RoleAssistant, "final", false}}
	h, err := m.History(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, regs := range [][]task.Message{got.Regs, h} {
		if len(regs) != len(want) {
			t.Fatalf("messages %+v", regs)
		}
		for i, w := range want {
			if regs[i].Role != w.role || regs[i].TextContent() != w.text || regs[i].Feedback != w.feedback {
				t.Fatalf("message %d = %+v, want %+v", i, regs[i], w)
			}
		}
	}
	rej := one(t, eventData(t, m, id, proto.EvAnswerRejected), proto.EvAnswerRejected)
	if rej["reason"] != "a draft" || rej["feedback"] != "Give the final figure." || rej["attempt"] != 0.0 || rej["final"] != nil {
		t.Fatalf("answer_rejected %v", rej)
	}
	ans := one(t, eventData(t, m, id, proto.EvAnswer), proto.EvAnswer)
	done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone)
	if ans["text"] != "final" || ans["checked"] != true || ans["check_failed"] != nil {
		t.Fatalf("answer %v", ans)
	}
	if done["stop_reason"] != "end_turn" || done["checked"] != true || done["check_reason"] != "fine" || done["check_failed"] != nil || done["check_error"] != nil {
		t.Fatalf("done %v", done)
	}
	kinds := kindsOf(t, m, id)
	if i := slices.Index(kinds, proto.EvAnswerRejected); i < 0 || kinds[i-1] != proto.EvUsage || kinds[i+1] != proto.EvStep && kinds[i+1] != proto.EvModelCall {
		t.Fatalf("events %v", kinds)
	}
}

// Without a check, nothing about checks is recorded: the answer and done
// events are as they always were.
func TestNoAnswerCheckRecordsNothing(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("hi"), nil })
	id := submit(t, m, proto.Instruction{Input: "x"})
	waitDone(t, m, id)
	for _, ev := range eventsOf(t, m, id) {
		switch ev.Kind {
		case proto.EvAnswer:
			if string(ev.Data) != `{"text":"hi"}` {
				t.Fatalf("answer %s", ev.Data)
			}
		case proto.EvDone:
			if string(ev.Data) != `{"stop_reason":"end_turn"}` {
				t.Fatalf("done %s", ev.Data)
			}
		case proto.EvStep:
			if strings.Contains(string(ev.Data), "check_ms") {
				t.Fatalf("step %s", ev.Data)
			}
		case proto.EvAnswerRejected:
			t.Fatal("answer_rejected without a check")
		}
	}
}

func TestAnswerCheckRetriesRunOut(t *testing.T) {
	for _, tc := range []struct {
		name    string
		retries int
		calls   int
	}{{"default", 0, 2}, {"none", -1, 1}, {"three", 3, 4}} {
		t.Run(tc.name, func(t *testing.T) {
			c := &checker{fn: func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict {
				return neumann.AnswerVerdict{Reject: true, Reason: "never good enough"}
			}}
			var n atomic.Int32
			m, sm := start(t, neumann.Config{AnswerCheck: c.check, MaxAnswerRetries: tc.retries}, func(model.Request) (model.Response, error) {
				return sim.Reply("answer " + string(rune('0'+n.Add(1)))), nil
			})
			id := submit(t, m, proto.Instruction{Input: "q"})
			got := waitDone(t, m, id)
			last := "answer " + string(rune('0'+tc.calls))
			if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != last || sm.Calls() != tc.calls {
				t.Fatalf("ended %s %q with %q after %d calls", got.State, got.StopReason, got.LastAnswer(), sm.Calls())
			}
			if got.Rejections != tc.calls {
				t.Fatalf("rejections %d", got.Rejections)
			}
			want := make([]int, tc.calls)
			for i := range want {
				want[i] = i
			}
			if !slices.Equal(c.attempts(), want) {
				t.Fatalf("attempts %v", c.attempts())
			}
			rej := eventData(t, m, id, proto.EvAnswerRejected)
			if len(rej) != tc.calls {
				t.Fatalf("answer_rejected %v", rej)
			}
			for i, d := range rej {
				final := i == tc.calls-1
				if d["attempt"] != float64(i) || (d["final"] == true) != final || (d["feedback"] != nil) == final {
					t.Fatalf("answer_rejected %d: %v", i, d)
				}
				if !final && d["feedback"] != "Your answer was not accepted. Reason: never good enough. Revise it and answer again." {
					t.Fatalf("default feedback %v", d["feedback"])
				}
			}
			feedback := 0
			for _, msg := range got.Regs {
				if msg.Feedback {
					feedback++
				}
			}
			if feedback != tc.calls-1 {
				t.Fatalf("%d feedback messages", feedback)
			}
			ans := one(t, eventData(t, m, id, proto.EvAnswer), proto.EvAnswer)
			done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone)
			if ans["text"] != last || ans["checked"] != true || ans["check_failed"] != true {
				t.Fatalf("answer %v", ans)
			}
			if done["checked"] != true || done["check_failed"] != true || done["check_reason"] != "never good enough" {
				t.Fatalf("done %v", done)
			}
		})
	}
}

// Each answer is a model call, so the step budget bounds a loop of
// rejections too: a rejection with no step left for another answer is
// final, in the same commit, as one past the retries is: the task ends
// end_turn with that answer marked check_failed, and no feedback is left
// that nobody answers.
func TestAnswerCheckLoopIsBoundedByTheBudget(t *testing.T) {
	for _, steps := range []int{1, 2} {
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			c := &checker{fn: func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict {
				return neumann.AnswerVerdict{Reject: true, Reason: "no"}
			}}
			m, sm := start(t, neumann.Config{AnswerCheck: c.check, MaxAnswerRetries: 5}, func(model.Request) (model.Response, error) {
				return sim.Reply("draft"), nil
			})
			id := submit(t, m, proto.Instruction{Input: "q", Budget: &task.Budget{MaxSteps: steps}})
			got := waitDone(t, m, id)
			if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != "draft" || sm.Calls() != steps || len(c.attempts()) != steps {
				t.Fatalf("ended %s %q with %q after %d calls, %d checks", got.State, got.StopReason, got.LastAnswer(), sm.Calls(), len(c.attempts()))
			}
			if last := got.Regs[len(got.Regs)-1]; last.Role != task.RoleAssistant || got.Rejections != steps {
				t.Fatalf("last register %+v, rejections %d", last, got.Rejections)
			}
			rej := eventData(t, m, id, proto.EvAnswerRejected)
			if len(rej) != steps || rej[steps-1]["final"] != true || rej[steps-1]["budget"] != true || rej[steps-1]["feedback"] != nil {
				t.Fatalf("answer_rejected %v", rej)
			}
			for _, d := range rej[:steps-1] {
				if d["final"] != nil || d["budget"] != nil {
					t.Fatalf("answer_rejected %v", rej)
				}
			}
			ans := one(t, eventData(t, m, id, proto.EvAnswer), proto.EvAnswer)
			done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone)
			if ans["text"] != "draft" || ans["check_failed"] != true || done["check_failed"] != true || done["check_reason"] != "no" {
				t.Fatalf("answer %v, done %v", ans, done)
			}
			// One commit: the final rejection and the end of the task.
			evs := eventsOf(t, m, id)
			k := slices.IndexFunc(evs, func(ev proto.Event) bool { return ev.Kind == proto.EvDone })
			if evs[k-2].Kind != proto.EvAnswerRejected || evs[k-2].Seq != evs[k].Seq-2 {
				t.Fatalf("events %v", kindsOf(t, m, id))
			}
		})
	}
}

// A check that never answers is given up on after its timeout, whether it
// honours its context or not, and the answer passes: the vCore is never
// held longer than that.
func TestAnswerCheckTimeoutPasses(t *testing.T) {
	const timeout = 100 * time.Millisecond
	stuck := make(chan struct{})
	defer close(stuck)
	for _, tc := range []struct {
		name string
		fn   func(ctx context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict
	}{
		{"honours its context", func(ctx context.Context, _ neumann.AnswerInfo) neumann.AnswerVerdict {
			if d, ok := ctx.Deadline(); !ok || time.Until(d) > timeout {
				t.Errorf("the check's context has no deadline within %v", timeout)
			}
			<-ctx.Done()
			return neumann.AnswerVerdict{Reject: true, Reason: "too late"}
		}},
		{"ignores it", func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict {
			<-stuck
			return neumann.AnswerVerdict{Reject: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &checker{fn: tc.fn}
			m, _ := start(t, neumann.Config{AnswerCheck: c.check, AnswerCheckTimeout: timeout, Cores: 1},
				func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
			begin := time.Now()
			var ids []string
			for range 3 {
				ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
			}
			for _, id := range ids {
				got := waitDone(t, m, id)
				if got.State != task.Done || got.LastAnswer() != "ok" || got.Rejections != 0 {
					t.Fatalf("ended %s with %q, %d rejections", got.State, got.LastAnswer(), got.Rejections)
				}
				done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone)
				if done["checked"] != true || done["check_error"] != "check timed out" || done["check_failed"] != nil {
					t.Fatalf("done %v", done)
				}
				// The step records the time the check took after the call.
				if step := one(t, eventData(t, m, id, proto.EvStep), proto.EvStep); step["check_ms"].(float64) < float64(timeout.Milliseconds()) {
					t.Fatalf("step %v", step)
				}
			}
			// One vCore, three checks one after the other: each held it for
			// its timeout, not longer.
			if d := time.Since(begin); d < 3*timeout || d > 3*timeout+2*time.Second {
				t.Fatalf("three timed-out checks on one vCore took %v", d)
			}
		})
	}
}

func TestAnswerCheckPanicPasses(t *testing.T) {
	c := &checker{fn: func(_ context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
		if info.Request == "boom" {
			panic("boom")
		}
		return neumann.AnswerVerdict{}
	}}
	m, _ := start(t, neumann.Config{AnswerCheck: c.check}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	id := submit(t, m, proto.Instruction{Input: "boom"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "ok" {
		t.Fatalf("ended %s with %q", got.State, got.LastAnswer())
	}
	if done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone); done["check_error"] != "check panicked: boom" {
		t.Fatalf("done %v", done)
	}
	// The machine goes on.
	id = submit(t, m, proto.Instruction{Input: "calm"})
	if got := waitDone(t, m, id); got.State != task.Done {
		t.Fatalf("next task ended %s", got.State)
	}
	if done := one(t, eventData(t, m, id, proto.EvDone), proto.EvDone); done["checked"] != true || done["check_error"] != nil {
		t.Fatalf("done %v", done)
	}
}

// A steer that arrives while the model answers makes the answer a turn, as
// without a check: the check is not asked about it, and the next answer is
// the first it sees.
func TestSteerDuringTheCallSkipsTheCheck(t *testing.T) {
	entered := make(chan struct{}, 1)
	gate, release := newGate()
	defer release()
	var calls atomic.Int32
	c := &checker{fn: rejectDrafts}
	m, _ := start(t, neumann.Config{AnswerCheck: c.check}, func(model.Request) (model.Response, error) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-gate
			return sim.Reply("draft"), nil
		}
		return sim.Reply("second"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A"})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("model call never started")
	}
	if err := m.Steer(bg, id, "late steer"); err != nil {
		t.Fatalf("steer: %v", err)
	}
	release()
	got := waitDone(t, m, id)
	if got.LastAnswer() != "second" || len(got.Regs) != 4 || got.Regs[2].TextContent() != "late steer" || got.Rejections != 0 {
		t.Fatalf("regs %+v, rejections %d", got.Regs, got.Rejections)
	}
	if len(c.shown) != 1 || c.shown[0].Answer != "second" || c.shown[0].Attempt != 0 {
		t.Fatalf("shown %+v", c.shown)
	}
	if n := len(eventData(t, m, id, proto.EvAnswerRejected)); n != 0 {
		t.Fatalf("%d answer_rejected", n)
	}
}

// A steer that arrives while the check runs counts as if it had arrived
// during the call: the verdict is dropped, the answer is a turn, and the
// steer is delivered with the next model step.
func TestSteerDuringTheCheckWins(t *testing.T) {
	var m *neumann.Machine
	var steered atomic.Bool
	c := &checker{}
	c.fn = func(_ context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
		if !steered.Swap(true) {
			if err := m.Steer(bg, info.TaskID, "and in euros"); err != nil {
				t.Errorf("steer: %v", err)
			}
			return neumann.AnswerVerdict{Reject: true, Reason: "dropped"}
		}
		return neumann.AnswerVerdict{}
	}
	var calls atomic.Int32
	m, _ = start(t, neumann.Config{AnswerCheck: c.check}, func(req model.Request) (model.Response, error) {
		if calls.Add(1) == 1 {
			return sim.Reply("10 dollars"), nil
		}
		return sim.Reply("9 euros"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "price?"})
	got := waitDone(t, m, id)
	if got.LastAnswer() != "9 euros" || got.Rejections != 0 || len(got.Regs) != 4 || got.Regs[2].TextContent() != "and in euros" || got.Regs[2].Feedback {
		t.Fatalf("regs %+v, rejections %d", got.Regs, got.Rejections)
	}
	if !slices.Equal(c.attempts(), []int{0, 0}) {
		t.Fatalf("attempts %v", c.attempts())
	}
	if n := len(eventData(t, m, id, proto.EvAnswerRejected)); n != 0 {
		t.Fatalf("%d answer_rejected", n)
	}
}

// cancelOnCommit commits a cancel of a task right before the first commit
// that match picks: the cancel lands first, as if Cancel had raced it.
type cancelOnCommit struct {
	mem.Store
	match func(mem.CommitSet) bool
	once  sync.Once
	hit   atomic.Bool
}

func (s *cancelOnCommit) Commit(ctx context.Context, c mem.CommitSet) ([]proto.Event, error) {
	if c.Task != nil && s.match(c) {
		s.once.Do(func() {
			s.hit.Store(true)
			s.Store.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: c.Task.ID, Kind: mem.InterruptCancel, Text: "cancelled"}}})
		})
	}
	return s.Store.Commit(ctx, c)
}

func hasEvent(c mem.CommitSet, kind string) bool {
	return slices.ContainsFunc(c.Events, func(ev proto.Event) bool { return ev.Kind == kind })
}

// A cancel committed before the commit of a verdict wins (design §4.1 rule
// 4): the task ends cancelled, and nothing of the verdict is kept: no
// answer_rejected, no feedback, no rejection counted, no checked answer.
func TestCancelBeatsTheVerdict(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fn    func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict
		match func(mem.CommitSet) bool
	}{
		{"reject", rejectDrafts, func(c mem.CommitSet) bool { return hasEvent(c, proto.EvAnswerRejected) }},
		{"final reject", func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict {
			return neumann.AnswerVerdict{Reject: true}
		}, func(c mem.CommitSet) bool { return hasEvent(c, proto.EvDone) }},
		{"pass", func(context.Context, neumann.AnswerInfo) neumann.AnswerVerdict { return neumann.AnswerVerdict{} },
			func(c mem.CommitSet) bool { return hasEvent(c, proto.EvDone) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &cancelOnCommit{Store: mem.NewMemory(), match: tc.match}
			c := &checker{fn: tc.fn}
			retries := 0
			if tc.name == "final reject" {
				retries = -1
			}
			m, sm := start(t, neumann.Config{Store: s, AnswerCheck: c.check, MaxAnswerRetries: retries}, draftThenFinal)
			id := submit(t, m, proto.Instruction{Input: "q"})
			got := waitDone(t, m, id)
			if !s.hit.Load() || got.State != task.Cancelled || got.StopReason != "cancelled" {
				t.Fatalf("hit %v, ended %s %q", s.hit.Load(), got.State, got.StopReason)
			}
			if got.Rejections != 0 || len(got.Regs) != 1 || got.Used.Steps != 1 || sm.Calls() != 1 {
				t.Fatalf("rejections %d, regs %+v, steps %d, calls %d", got.Rejections, got.Regs, got.Used.Steps, sm.Calls())
			}
			for _, k := range []string{proto.EvAnswerRejected, proto.EvAnswer, proto.EvDone} {
				if n := len(eventData(t, m, id, k)); n != 0 {
					t.Fatalf("%d %s events after the cancel won", n, k)
				}
			}
			if n := len(eventData(t, m, id, proto.EvUsage)); n != 1 {
				t.Fatalf("%d usage events: the answered call's usage must be kept", n)
			}
		})
	}
}

// A cancel while the check runs ends the check at once (its context) and
// the task: nothing about the check is recorded.
func TestCancelDuringTheCheck(t *testing.T) {
	var m *neumann.Machine
	c := &checker{fn: func(ctx context.Context, info neumann.AnswerInfo) neumann.AnswerVerdict {
		if err := m.Cancel(bg, info.TaskID); err != nil {
			t.Errorf("cancel: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Error("the check's context did not end with its task")
		}
		return neumann.AnswerVerdict{Reject: true}
	}}
	m, _ = start(t, neumann.Config{AnswerCheck: c.check, AnswerCheckTimeout: time.Minute}, draftThenFinal)
	id := submit(t, m, proto.Instruction{Input: "q"})
	begin := time.Now()
	got := waitDone(t, m, id)
	if got.State != task.Cancelled || got.Rejections != 0 || len(got.Regs) != 1 || time.Since(begin) > 3*time.Second {
		t.Fatalf("ended %s, rejections %d, regs %+v after %v", got.State, got.Rejections, got.Regs, time.Since(begin))
	}
	for _, k := range []string{proto.EvAnswerRejected, proto.EvAnswer, proto.EvDone} {
		if n := len(eventData(t, m, id, k)); n != 0 {
			t.Fatalf("%d %s events", n, k)
		}
	}
}

// crashAfterKind is a process that dies right after the first commit
// carrying an event of kind lands: the commit is durable, its caller never
// hears so, and nothing after it is recorded.
type crashAfterKind struct {
	mem.Store
	kind    string
	crashed atomic.Bool
}

func (s *crashAfterKind) Commit(ctx context.Context, c mem.CommitSet) ([]proto.Event, error) {
	if s.crashed.Load() {
		return nil, errCrashed
	}
	evs, err := s.Store.Commit(ctx, c)
	if err == nil && hasEvent(c, s.kind) {
		s.crashed.Store(true)
		return nil, errCrashed
	}
	return evs, err
}

// A rejection survives a restart: Rejections is committed with the
// feedback, so the restarted machine's check is told the attempt, and the
// model answers from the feedback.
func TestRejectionsSurviveARestart(t *testing.T) {
	w := newWorld()
	path := filepath.Join(t.TempDir(), "neumann.db")
	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	c1 := &checker{fn: rejectDrafts}
	w.check = c1.check
	cs := &crashAfterKind{Store: s1, kind: proto.EvAnswerRejected}
	m1 := newMachine(t, cs, w, draftThenFinal)
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := m1.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, TaskID: "t1", Input: "q"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, cs.crashed.Load)
	stop(t, m1)
	s1.Close()

	s2, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	tc, err := s2.Load(bg, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if tc.State != task.Ready || tc.Rejections != 1 || !tc.Regs[len(tc.Regs)-1].Feedback {
		t.Fatalf("after the crash: %s, rejections %d, regs %+v", tc.State, tc.Rejections, tc.Regs)
	}
	c2 := &checker{fn: rejectDrafts}
	w.check = c2.check
	m2 := newMachine(t, s2, w, draftThenFinal)
	if err := m2.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m2)
	got := waitDone(t, m2, "t1")
	if got.State != task.Done || got.LastAnswer() != "final" || got.Rejections != 1 || !slices.Equal(c2.attempts(), []int{1}) {
		t.Fatalf("ended %s with %q, rejections %d, attempts %v", got.State, got.LastAnswer(), got.Rejections, c2.attempts())
	}
}

// A rejection is committed, then the request for another answer is lost (a
// hard Stop mid-call): the restart counts it unconfirmed, finds no step
// left, and ends the task on its budget with the rejected answer, marked
// check_failed.
func TestBudgetEndAfterALostRetryIsMarked(t *testing.T) {
	store := mem.NewMemory()
	block := make(chan struct{})
	var n atomic.Int32
	fn := func(req model.Request) (model.Response, error) {
		if n.Add(1) == 1 {
			return sim.Reply("draft"), nil
		}
		<-block
		return sim.Reply("final"), nil
	}
	c := &checker{fn: rejectDrafts}
	m1, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: fn}, Store: store, AnswerCheck: c.check, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	id := submit(t, m1, proto.Instruction{Input: "q", Budget: &task.Budget{MaxSteps: 2}})
	waitFor(t, func() bool { return n.Load() == 2 })
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	m1.Stop(ctx) // runs out: the call is halted, nothing committed for it
	cancel()
	close(block)

	m2 := startWith(t, neumann.Config{Model: &sim.Model{Fn: fn}, Store: store, AnswerCheck: c.check})
	got := waitDone(t, m2, id)
	if got.State != task.Done || got.StopReason != "budget" || got.LastAnswer() != "draft" || got.Unconfirmed != 1 || got.Rejections != 1 {
		t.Fatalf("ended %s %q with %q, unconfirmed %d, rejections %d", got.State, got.StopReason, got.LastAnswer(), got.Unconfirmed, got.Rejections)
	}
	ans := one(t, eventData(t, m2, id, proto.EvAnswer), proto.EvAnswer)
	done := one(t, eventData(t, m2, id, proto.EvDone), proto.EvDone)
	if ans["text"] != "draft" || ans["checked"] != true || ans["check_failed"] != true || done["checked"] != true || done["check_failed"] != true {
		t.Fatalf("answer %v, done %v", ans, done)
	}
	if !slices.Equal(c.attempts(), []int{0}) {
		t.Fatalf("attempts %v", c.attempts())
	}
}
