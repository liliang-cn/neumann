package core

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sched"
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
	if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != "hello" || got.Version != 2 {
		t.Fatalf("task %+v", got)
	}
	if k := kinds(store); k != "step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
}

func TestModelStepPlansToolCalls(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_echo", "dev_echo"), nil
	}, echoDev())
	e.step(bg, newTask(t, e, store, "go"), 0)
	got := load(t, store)
	if got.State != task.Ready || got.PC != task.StepTools {
		t.Fatalf("task %s/%s", got.State, got.PC)
	}
	calls := got.Regs[1].ToolCalls()
	if len(calls) != 2 || calls[0].CallID != "c-1" || calls[0].ProviderCallID != "p-0" || calls[1].CallID != "c-2" {
		t.Fatalf("calls %+v", calls)
	}
	recs, _ := store.Calls(bg, "t1")
	if len(recs) != 2 || recs[0].Status != mem.CallPlanned || recs[1].Index != 2 {
		t.Fatalf("records %+v", recs)
	}
}

func TestToolsAreCommittedBeforeDispatch(t *testing.T) {
	var store *mem.Memory
	var mu sync.Mutex
	var seen []mem.CallStatus
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "check"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		parts := strings.Split(c.Key, "/") // task/call/attempt
		recs, err := store.Calls(ctx, parts[0])
		if err != nil {
			return "", err
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
	e.step(bg, newTask(t, e, s, "go"), 0) // model: plan
	e.step(bg, take(t, e), 0)             // tools: commit dispatched, then dispatch
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

func TestSteerDuringFinalCallContinuesTheTask(t *testing.T) {
	var store *mem.Memory
	calls := 0
	e, s, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		calls++
		if calls == 1 {
			if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: "one more"}}}); err != nil {
				return model.Response{}, err
			}
			return sim.Reply("first"), nil
		}
		return sim.Reply("second"), nil
	})
	store = s
	e.step(bg, newTask(t, e, s, "q"), 0)
	got := load(t, s)
	if got.State != task.Ready || got.PC != task.StepModel || len(got.Regs) != 3 ||
		got.Regs[1].TextContent() != "first" || got.Regs[2].TextContent() != "one more" {
		t.Fatalf("after first call: %+v", got)
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, s); got.State != task.Done || got.LastAnswer() != "second" {
		t.Fatalf("final %+v", got)
	}
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

func TestModelErrorFailsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return model.Response{}, errors.New("provider down")
	})
	e.step(bg, newTask(t, e, store, "q"), 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "model: provider down" {
		t.Fatalf("task %+v", got)
	}
	if k := kinds(store); k != "step,failed" {
		t.Fatalf("events %s", k)
	}
}
