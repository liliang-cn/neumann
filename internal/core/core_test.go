package core

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

type modelFn = func(model.Request) (model.Response, error)

func newTestEnv(t *testing.T, fn modelFn, devs ...bus.Device) (*Env, *mem.Memory, *sim.Model) {
	t.Helper()
	store := mem.NewMemory()
	b, err := bus.New(bg, 4, devs...)
	if err != nil {
		t.Fatal(err)
	}
	sm := &sim.Model{Fn: fn}
	e := &Env{
		Store: store,
		Sched: sched.New(sched.RealClock{}, sched.DefaultAging),
		Bus:   b,
		Model: sm,
		Clock: sched.RealClock{},
		Logf:  t.Logf,
	}
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		_, err := store.Commit(ctx, c)
		return err
	}
	return e, store, sm
}

func newTask(t *testing.T, e *Env, store *mem.Memory, input string) *task.TCB {
	t.Helper()
	tc := &task.TCB{
		ID: "t1", State: task.Ready, PC: task.StepModel, Class: "default",
		ReadySince: time.Now(), Tools: e.Bus.Specs(nil, nil),
		Regs: []task.Message{task.Text(task.RoleUser, input)}, Version: 1,
	}
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
		t.Fatal(err)
	}
	return tc
}

// take fetches the task back from the scheduler, as a vCore would.
func take(t *testing.T, e *Env) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	tc, ok := e.Sched.Next(ctx, "default")
	if !ok {
		t.Fatal("no ready task")
	}
	return tc
}

func load(t *testing.T, store *mem.Memory) *task.TCB {
	t.Helper()
	tc, err := store.Load(bg, "t1")
	if err != nil {
		t.Fatal(err)
	}
	return tc
}

func kinds(store *mem.Memory) string {
	evs, _ := store.Events(bg, 0, 0)
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Kind)
	}
	return strings.Join(out, ",")
}

func echoDev() bus.Device {
	return builtin.New("dev").Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		return string(c.Args), nil
	})
}

func TestModelStepAnswers(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("hello"), nil })
	e.step(bg, newTask(t, e, store, "hi"), 0)
	got := load(t, store)
	if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != "hello" || got.Version != 3 {
		t.Fatalf("task %+v", got)
	}
	if k := kinds(store); k != "step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
}

func TestModelStepDispatchesToolCalls(t *testing.T) {
	gate := make(chan struct{})
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "hold"}, func(context.Context, bus.DeviceCall) (string, error) {
		<-gate
		return "held", nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_hold", "dev_hold"), nil
	}, dev)
	e.step(bg, newTask(t, e, store, "go"), 0)
	got := load(t, store)
	if got.State != task.Waiting || got.Wait != task.WaitIO || got.PC != task.StepTools {
		t.Fatalf("task %s/%s/%s", got.State, got.Wait, got.PC)
	}
	calls := got.Regs[1].ToolCalls()
	if len(calls) != 2 || calls[0].CallID != "c-1" || calls[0].ProviderCallID != "p-0" || calls[1].CallID != "c-2" {
		t.Fatalf("calls %+v", calls)
	}
	recs, _ := store.Calls(bg, "t1")
	if len(recs) != 2 || recs[0].Status != mem.CallDispatched || recs[0].Attempt != 1 ||
		recs[0].Key != "t1/c-1/1" || recs[1].Index != 2 {
		t.Fatalf("records %+v", recs)
	}
	// One instruction: the model step itself hands the calls to the bus.
	if k := kinds(store); k != "step,usage,tool_call,tool_call" {
		t.Fatalf("events %s", k)
	}
	close(gate)
	e.WaitBatches()
	if got := load(t, store); got.State != task.Ready || got.PC != task.StepModel {
		t.Fatalf("after tools %s/%s", got.State, got.PC)
	}
}

func TestToolsAreCommittedBeforeDispatch(t *testing.T) {
	var store *mem.Memory
	var mu sync.Mutex
	var seen []mem.CallStatus
	var taskStates []string
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "check"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		parts := strings.Split(c.Key, "/") // task/call/attempt
		recs, err := store.Calls(ctx, parts[0])
		if err != nil {
			return "", err
		}
		if st, err := store.Load(ctx, parts[0]); err == nil {
			mu.Lock()
			taskStates = append(taskStates, st.State.String()+"/"+st.Wait.String())
			mu.Unlock()
		}
		for _, r := range recs {
			if r.CallID == parts[1] {
				mu.Lock()
				seen = append(seen, r.Status)
				mu.Unlock()
			}
		}
		return "checked", nil
	})
	e, s, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_check", "dev_check"), nil
		}
		return sim.Reply("done"), nil
	}, dev)
	store = s
	e.step(bg, newTask(t, e, s, "go"), 0) // model: commit dispatched, then dispatch
	e.WaitBatches()

	got := load(t, s)
	if got.State != task.Ready || got.PC != task.StepModel {
		t.Fatalf("after tools: %s/%s", got.State, got.PC)
	}
	last := got.Regs[len(got.Regs)-1]
	if last.Role != task.RoleTool || len(last.Parts) != 2 || last.Parts[0].CallID != "c-1" || last.Parts[0].Text != "checked" {
		t.Fatalf("tool message %+v", last)
	}
	if !slices.Equal(seen, []mem.CallStatus{mem.CallDispatched, mem.CallDispatched}) {
		t.Fatalf("device saw statuses %v, want dispatched twice", seen)
	}
	if !slices.Equal(taskStates, []string{"waiting/io", "waiting/io"}) {
		t.Fatalf("task states seen by the device: %v", taskStates)
	}
	recs, _ := s.Calls(bg, "t1")
	if recs[0].Status != mem.CallDone || recs[0].Attempt != 1 || recs[0].Key != "t1/c-1/1" {
		t.Fatalf("record %+v", recs[0])
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, s); got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("final %+v", got)
	}
}

func TestSteerIsAppliedBeforeTheModelCall(t *testing.T) {
	var saw []string
	e, store, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		saw = sim.UserTexts(req)
		return sim.Reply("ok"), nil
	})
	tc := newTask(t, e, store, "q")
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: "also this"}}}); err != nil {
		t.Fatal(err)
	}
	e.step(bg, tc, 0)
	if !slices.Equal(saw, []string{"q", "also this"}) {
		t.Fatalf("model saw %v", saw)
	}
	if pending, _ := store.Interrupts(bg, "t1", true); len(pending) != 0 {
		t.Fatalf("still pending %+v", pending)
	}
}

// lateSteerEnv scripts a first model call during which a steer is committed;
// second decides what the next call does.
func lateSteerEnv(t *testing.T, second modelFn) (*Env, *mem.Memory, *[][]string) {
	t.Helper()
	var store *mem.Memory
	var seen [][]string
	calls := 0
	e, s, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		calls++
		seen = append(seen, sim.UserTexts(req))
		if calls == 1 {
			if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: "one more"}}}); err != nil {
				return model.Response{}, err
			}
			return sim.Reply("first"), nil
		}
		return second(req)
	})
	store = s
	return e, s, &seen
}

func droppedCount(t *testing.T, store *mem.Memory, text string) int {
	t.Helper()
	evs, _ := store.Events(bg, 0, 0)
	n := 0
	for _, ev := range evs {
		if ev.Kind == "steer_dropped" && strings.Contains(string(ev.Data), text) {
			n++
		}
	}
	return n
}

func TestSteerDuringFinalCallContinuesTheTask(t *testing.T) {
	e, s, seen := lateSteerEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("second"), nil })
	e.step(bg, newTask(t, e, s, "q"), 0)
	got := load(t, s)
	if got.State != task.Ready || got.PC != task.StepModel || len(got.Regs) != 2 ||
		got.Regs[0].TextContent() != "q" || got.Regs[1].TextContent() != "first" {
		t.Fatalf("after first call: %+v", got)
	}
	if p, _ := s.Interrupts(bg, "t1", true); len(p) != 1 || p[0].Text != "one more" {
		t.Fatalf("steer should still be pending: %+v", p)
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, s); got.State != task.Done || got.LastAnswer() != "second" {
		t.Fatalf("final %+v", got)
	}
	if want := []string{"q", "one more"}; !slices.Equal((*seen)[1], want) {
		t.Fatalf("second call saw %v", (*seen)[1])
	}
	if n := droppedCount(t, s, "one more"); n != 0 {
		t.Fatalf("%d steer_dropped events", n)
	}
}

func TestLateSteerIsDroppedWhenTheNextStepHitsTheBudget(t *testing.T) {
	e, s, _ := lateSteerEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("second"), nil })
	tc := newTask(t, e, s, "q")
	tc.Budget.MaxSteps = 1
	e.step(bg, tc, 0)
	e.step(bg, take(t, e), 0)
	got := load(t, s)
	if got.State != task.Done || got.StopReason != "budget" || hasUserText(got, "one more") {
		t.Fatalf("task %+v", got)
	}
	if n := droppedCount(t, s, "one more"); n != 1 {
		t.Fatalf("%d steer_dropped events, want 1", n)
	}
}

func TestLateSteerIsDroppedWhenTheNextCallFails(t *testing.T) {
	e, s, _ := lateSteerEnv(t, func(model.Request) (model.Response, error) { return model.Response{}, sim.Permanent() })
	e.step(bg, newTask(t, e, s, "q"), 0)
	e.step(bg, take(t, e), 0)
	got := load(t, s)
	if got.State != task.Failed || hasUserText(got, "one more") {
		t.Fatalf("task %+v", got)
	}
	if n := droppedCount(t, s, "one more"); n != 1 {
		t.Fatalf("%d steer_dropped events, want 1", n)
	}
}

func hasUserText(tc *task.TCB, text string) bool {
	for _, m := range tc.Regs {
		if m.Role == task.RoleUser && m.TextContent() == text {
			return true
		}
	}
	return false
}

func TestBudgetStopIsNotAModelStep(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	tc := newTask(t, e, store, "q")
	tc.Budget.MaxSteps = 1
	tc.Used.Steps = 1
	e.step(bg, tc, 0)
	if got := load(t, store); got.StopReason != "budget" {
		t.Fatalf("task %+v", got)
	}
	if hasKind(store, "step") {
		t.Fatalf("a budget stop counted as an instruction: %s", kinds(store))
	}
}

func TestModelStepDoesNotModifyTheAdapterResponse(t *testing.T) {
	shared := sim.CallTools("dev_echo")
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return shared, nil }, echoDev())
	e.step(bg, newTask(t, e, store, "go"), 0)
	if got := load(t, store).Regs[1].ToolCalls(); len(got) != 1 || got[0].CallID != "c-1" {
		t.Fatalf("task calls %+v", got)
	}
	if id := shared.Message.Parts[0].CallID; id != "" {
		t.Fatalf("adapter response was modified: CallID %q", id)
	}
}

func TestModelPanicFailsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { panic("boom") })
	e.step(bg, newTask(t, e, store, "q"), 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "model_error" || !strings.Contains(failedError(store), "model panicked: boom") {
		t.Fatalf("task %+v, error %q", got, failedError(store))
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
}

func TestCompleteWithoutCheckoutPanics(t *testing.T) {
	e, _, _ := newTestEnv(t, nil)
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "ghost") {
			t.Fatalf("recovered %v", r)
		}
	}()
	e.complete(bg, "ghost", nil, nil)
}

func TestBudgetEndsTheTask(t *testing.T) {
	e, store, sm := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_echo"), nil
	}, echoDev())
	tc := newTask(t, e, store, "loop")
	tc.Budget.MaxSteps = 2
	for range 10 {
		e.step(bg, tc, 0)
		if load(t, store).State.Terminal() {
			break
		}
		e.WaitBatches()
		tc = take(t, e)
	}
	got := load(t, store)
	if got.State != task.Done || got.StopReason != "budget" || sm.Calls() != 2 {
		t.Fatalf("state %s reason %q model calls %d", got.State, got.StopReason, sm.Calls())
	}
}

// A permanent rejection fails the task at once (design §11): StopReason
// model_error, the provider's error in the failed event.
func TestModelErrorFailsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return model.Response{}, sim.Permanent()
	})
	e.step(bg, newTask(t, e, store, "q"), 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "model_error" || !strings.Contains(failedError(store), "scripted permanent") {
		t.Fatalf("task %+v, error %q", got, failedError(store))
	}
	if k := kinds(store); k != "step,failed" {
		t.Fatalf("events %s", k)
	}
}

func TestCompletionCommitsResultsWithTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		return sim.CallTools("dev_echo", "dev_echo"), nil
	}, echoDev())
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task == nil && len(c.Calls) > 0 {
			return errors.New("injected: per-result commit lost")
		}
		return inner(ctx, c)
	}
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.WaitBatches()
	got := load(t, store)
	if got.State != task.Ready || got.PC != task.StepModel {
		t.Fatalf("task %s/%s", got.State, got.PC)
	}
	msg := got.Regs[len(got.Regs)-1]
	recs, _ := store.Calls(bg, "t1")
	if len(recs) != 2 || len(msg.Parts) != 2 {
		t.Fatalf("records %+v message %+v", recs, msg)
	}
	for i, r := range recs {
		if r.Status != mem.CallDone || r.Result == nil || r.Result.Content != msg.Parts[i].Text {
			t.Fatalf("record %d %+v does not match tool message %+v", i, r, msg.Parts[i])
		}
	}
}

func TestRunVCoreFinishesTheInstructionAfterCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		close(started)
		<-release
		return sim.Reply("late"), nil
	})
	e.Sched.Add(newTask(t, e, store, "q"))
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { e.RunVCore(ctx, "default", 0); close(done) }()
	<-started
	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunVCore did not return")
	}
	got := load(t, store)
	if got.State != task.Done || got.LastAnswer() != "late" {
		t.Fatalf("task %+v", got)
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
}

// failedError is the error text of the failed event, if any.
func failedError(store *mem.Memory) string {
	evs, _ := store.Events(bg, 0, 0)
	for _, ev := range evs {
		if ev.Kind == "failed" {
			var d map[string]string
			json.Unmarshal(ev.Data, &d)
			return d["error"]
		}
	}
	return ""
}

func hasKind(store *mem.Memory, kind string) bool {
	return slices.Contains(strings.Split(kinds(store), ","), kind)
}

func addSteer(t *testing.T, store *mem.Memory, text string) {
	t.Helper()
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: text}}}); err != nil {
		t.Fatal(err)
	}
}

func TestModelErrorDropsTheSteersItApplied(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return model.Response{}, sim.Permanent()
	})
	tc := newTask(t, e, store, "q")
	addSteer(t, store, "also this")
	e.step(bg, tc, 0)
	got := load(t, store)
	if got.State != task.Failed || len(got.Regs) != 1 {
		t.Fatalf("task %s regs %+v", got.State, got.Regs)
	}
	if !hasKind(store, "steer_dropped") {
		t.Fatalf("events %s", kinds(store))
	}
}

func TestToolResultsKeepCallOrder(t *testing.T) {
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "slow"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		if strings.Contains(string(c.Args), `"n":0`) {
			time.Sleep(80 * time.Millisecond)
		}
		return "r" + string(c.Args), nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_slow", "dev_slow"), nil
	}, dev)
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.step(bg, take(t, e), 0)
	e.WaitBatches()
	msg := load(t, store).Regs[2]
	if len(msg.Parts) != 2 || msg.Parts[0].CallID != "c-1" || msg.Parts[1].CallID != "c-2" ||
		msg.Parts[0].Text != `r{"n":0}` || msg.Parts[1].Text != `r{"n":1}` {
		t.Fatalf("tool message %+v", msg)
	}
}

func TestSteerDuringToolTurnIsAppliedAfterTheToolMessage(t *testing.T) {
	var store *mem.Memory
	e, s, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			addSteer(t, store, "mid")
			return sim.CallTools("dev_echo"), nil
		}
		return sim.Reply("done"), nil
	}, echoDev())
	store = s
	e.step(bg, newTask(t, e, s, "go"), 0)
	e.WaitBatches()
	if p, _ := s.Interrupts(bg, "t1", true); len(p) != 1 {
		t.Fatalf("pending after tools %+v", p)
	}
	e.step(bg, take(t, e), 0)
	got := load(t, s)
	if len(got.Regs) < 5 || got.Regs[2].Role != task.RoleTool || got.Regs[3].TextContent() != "mid" || got.State != task.Done {
		t.Fatalf("task %s regs %+v", got.State, got.Regs)
	}
}

func TestBudgetDropsPendingSteers(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	tc := newTask(t, e, store, "q")
	tc.Budget.MaxSteps = 1
	tc.Used.Steps = 1
	addSteer(t, store, "late")
	e.step(bg, tc, 0)
	if got := load(t, store); got.StopReason != "budget" || !hasKind(store, "steer_dropped") {
		t.Fatalf("task %+v events %s", got, kinds(store))
	}
}

func TestOnlySteersAreConsumed(t *testing.T) {
	var store *mem.Memory
	e, s, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: "other"}}}); err != nil {
			return model.Response{}, err
		}
		return sim.Reply("a"), nil
	})
	store = s
	e.step(bg, newTask(t, e, s, "q"), 0)
	if got := load(t, s); got.State != task.Done {
		t.Fatalf("a non-steer interrupt kept the task going: %s", got.State)
	}
}

func TestStepCommitFailureForgetsTheTaskAndDispatchesNothing(t *testing.T) {
	ran := 0
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, func(context.Context, bus.DeviceCall) (string, error) {
		ran++
		return "", nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_x"), nil }, dev)
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && c.Task.State == task.Waiting {
			return errors.New("injected")
		}
		return inner(ctx, c)
	}
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.WaitBatches()
	if r, w, h := e.Sched.Counts(); r+w+h != 0 || ran != 0 {
		t.Fatalf("counts %d %d %d, device ran %d times", r, w, h, ran)
	}
}

func TestCompleteCommitFailureForgetsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_echo"), nil }, echoDev())
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && c.Task.State == task.Ready && c.Task.PC == task.StepModel && len(c.Calls) > 0 {
			return errors.New("injected")
		}
		return inner(ctx, c)
	}
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.WaitBatches()
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
}

func TestHaltDuringModelCallCommitsNothing(t *testing.T) {
	halt, cancel := context.WithCancel(bg)
	defer cancel()
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		cancel() // the machine gave up while the call was in flight
		return sim.Reply("too late"), nil
	})
	e.Halt = halt
	before := newTask(t, e, store, "q").Version
	e.Sched.Add(load(t, store))
	e.step(bg, take(t, e), 0)
	got := load(t, store)
	// Only the start of the step is committed: the task is Running, which tells
	// recovery a request may have reached the provider. Nothing of the
	// interrupted call itself is committed.
	if got.Version != before+1 || got.State != task.Running || len(got.Regs) != 1 || hasKind(store, "step") {
		t.Fatalf("a halted step committed: %+v events %s", got, kinds(store))
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
}

func TestHaltDuringToolCallSkipsResultCommits(t *testing.T) {
	halt, cancel := context.WithCancel(bg)
	defer cancel()
	started := make(chan struct{})
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "wait"}, func(ctx context.Context, _ bus.DeviceCall) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_wait"), nil }, dev)
	e.Halt = halt
	e.step(bg, newTask(t, e, store, "go"), 0)
	<-started
	cancel()
	e.WaitBatches()
	if got := load(t, store); got.State != task.Waiting {
		t.Fatalf("task %+v", got)
	}
	if hasKind(store, "tool_result") {
		t.Fatalf("result committed after halt: %s", kinds(store))
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("counts %d %d %d", r, w, h)
	}
}

func TestRunningIsCommittedBeforeTheModelCallWithoutSteers(t *testing.T) {
	var store *mem.Memory
	var atCall *task.TCB
	e, s, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		atCall = load(t, store)
		return sim.Reply("ok"), nil
	})
	store = s
	tc := newTask(t, e, s, "q")
	addSteer(t, s, "later")
	e.step(bg, tc, 0)
	if atCall.State != task.Running || len(atCall.Regs) != 1 {
		t.Fatalf("stored during the call: state %s, %d regs (the steer must not be in the start commit)", atCall.State, len(atCall.Regs))
	}
	if p, _ := s.Interrupts(bg, "t1", true); len(p) != 0 {
		t.Fatalf("steer still pending after the step: %+v", p)
	}
}

func TestPlannedCallsRecordTheirEffect(t *testing.T) {
	dev := builtin.New("dev").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly}, func(context.Context, bus.DeviceCall) (string, error) { return "r", nil }).
		Add(bus.ToolDef{Name: "pay", ReportUnknown: true}, func(context.Context, bus.DeviceCall) (string, error) { return "p", nil })
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_read", "dev_pay"), nil
	}, dev)
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.WaitBatches()
	recs, _ := store.Calls(bg, "t1")
	if recs[0].Effect != task.ReadOnly || recs[0].ReportUnknown || recs[1].Effect != task.SideEffect || !recs[1].ReportUnknown {
		t.Fatalf("records %+v", recs)
	}
}

func TestLostResultEventIsPublishedWithTheCompletion(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_echo"), nil }, echoDev())
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task == nil && len(c.Calls) > 0 {
			return errors.New("injected: per-result commit lost")
		}
		return inner(ctx, c)
	}
	e.step(bg, newTask(t, e, store, "go"), 0)
	e.WaitBatches()
	evs, _ := store.Events(bg, 0, 0)
	n := 0
	for _, ev := range evs {
		if ev.Kind == "tool_result" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("tool_result events %d, want 1: %s", n, kinds(store))
	}
}
