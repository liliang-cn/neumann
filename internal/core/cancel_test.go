package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// ctxModel is an adapter that sees its context, unlike sim.Model's Fn.
type ctxModel struct {
	fn    func(ctx context.Context, req model.Request) (model.Response, error)
	calls atomic.Int32
}

func (m *ctxModel) Provider() string { return "ctx" }
func (m *ctxModel) Call(ctx context.Context, req model.Request) (model.Response, error) {
	m.calls.Add(1)
	return m.fn(ctx, req)
}

func callsOf(t *testing.T, store *mem.Memory) map[string]mem.CallRecord {
	t.Helper()
	recs, err := store.Calls(bg, "t1")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]mem.CallRecord{}
	for _, r := range recs {
		out[r.CallID] = r
	}
	return out
}

func eventsOfKind(store *mem.Memory, kind string) []proto.Event {
	evs, _ := store.Events(bg, 0, 0)
	var out []proto.Event
	for _, ev := range evs {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// waitFor polls the store until ok holds.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestCancelPendingAtStartRunsNoInstruction(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	e.Sched.Add(tc)
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	got := load(t, store)
	if got.State != task.Cancelled || got.StopReason != "cancelled" || sm.Calls() != 0 {
		t.Fatalf("task %s %q after %d model calls", got.State, got.StopReason, sm.Calls())
	}
	if k := kinds(store); k != "cancelled" {
		t.Fatalf("events %s", k)
	}
}

// A vCore that takes a task whose cancel is committed but not yet signalled
// (the canceller is between its commit and the signal) finalises it.
func TestVCoreFinalisesACommittedCancel(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	if err := e.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonDeadline}}}); err != nil {
		t.Fatal(err)
	}
	e.step(bg, tc, 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "deadline" || sm.Calls() != 0 {
		t.Fatalf("task %s %q after %d model calls", got.State, got.StopReason, sm.Calls())
	}
	if k := kinds(store); k != "failed" {
		t.Fatalf("events %s", k)
	}
}

func TestCancelStopsAModelCall(t *testing.T) {
	entered := make(chan struct{})
	cm := &ctxModel{fn: func(ctx context.Context, _ model.Request) (model.Response, error) {
		close(entered)
		<-ctx.Done()
		return model.Response{}, ctx.Err()
	}}
	e, store, _ := newTestEnv(t, reply)
	e.Model = cm
	tc := newTask(t, e, store, "q")
	e.Sched.Add(tc)
	held := take(t, e)
	done := make(chan struct{})
	go func() { e.step(bg, held, 0); close(done) }()
	<-entered
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the model call was not interrupted")
	}
	got := load(t, store)
	if got.State != task.Cancelled || got.Unconfirmed != 1 || got.Used.Steps != 1 {
		t.Fatalf("task %s unconfirmed=%d steps=%d", got.State, got.Unconfirmed, got.Used.Steps)
	}
	if k := kinds(store); k != "step,cancelled" {
		t.Fatalf("events %s", k)
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("scheduler still holds it: %d/%d/%d", r, w, h)
	}
}

// design §18 test 11: the model asked for tools, but the task was cancelled
// before they were dispatched. They are recorded cancelled and never run.
func TestCancelledPlannedCallsNeverRun(t *testing.T) {
	var runs atomic.Int32
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "pay"}, func(context.Context, bus.DeviceCall) (string, error) {
		runs.Add(1)
		return "paid", nil
	})
	var e *Env
	cm := &ctxModel{fn: func(context.Context, model.Request) (model.Response, error) {
		if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
			t.Error(err)
		}
		return sim.CallTools("dev_pay", "dev_pay"), nil
	}}
	e, store, _ := newTestEnv(t, reply, dev)
	e.Model = cm
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	e.WaitBatches()
	got := load(t, store)
	if got.State != task.Cancelled || got.Used.PromptTokens == 0 {
		t.Fatalf("task %s used %+v", got.State, got.Used)
	}
	recs := callsOf(t, store)
	if len(recs) != 2 || recs["c-1"].Status != mem.CallCancelled || recs["c-2"].Status != mem.CallCancelled ||
		recs["c-1"].Attempt != 0 || recs["c-1"].Key != "" {
		t.Fatalf("records %+v", recs)
	}
	if runs.Load() != 0 {
		t.Fatalf("a cancelled call ran %d times", runs.Load())
	}
}

// The cancel lands after the step committed its calls as dispatched and
// before it handed the task back: the scheduler refuses the task, and the
// vCore finalises it without sending the calls.
func TestCancelBetweenCommitAndDispatch(t *testing.T) {
	var runs atomic.Int32
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "pay"}, func(context.Context, bus.DeviceCall) (string, error) {
		runs.Add(1)
		return "paid", nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_pay"), nil }, dev)
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if err := inner(ctx, c); err != nil {
			return err
		}
		if c.Task != nil && c.Task.State == task.Waiting {
			if err := inner(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled}}}); err != nil {
				t.Fatal(err)
			}
			if _, ok := e.Sched.Cancel("t1"); ok {
				t.Error("Cancel took a task its vCore holds")
			}
		}
		return nil
	}
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	e.WaitBatches()
	got := load(t, store)
	if got.State != task.Cancelled {
		t.Fatalf("task %s", got.State)
	}
	if r := callsOf(t, store)["c-1"]; r.Status != mem.CallCancelled || runs.Load() != 0 {
		t.Fatalf("record %+v, device ran %d", r, runs.Load())
	}
}

// design §18 test 11: an in-flight call sees its context cancelled; a call
// still queued for the device never runs. The one that ran has an unknown
// outcome: it is recorded unknown with reconcile_needed, and the task does not
// wait for it.
func TestCancelWhileToolsInFlight(t *testing.T) {
	entered := make(chan string, 2)
	var sawCancel atomic.Int32
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "hold"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		entered <- c.Key
		<-ctx.Done()
		sawCancel.Add(1)
		return "", ctx.Err()
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_hold", "dev_hold"), nil })
	b, err := bus.New(bg, 1, dev) // one slot: the second call queues
	if err != nil {
		t.Fatal(err)
	}
	e.Bus = b
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	ran := <-entered
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	e.WaitBatches()
	got := load(t, store)
	if got.State != task.Cancelled || sawCancel.Load() != 1 {
		t.Fatalf("task %s, device saw cancel %d times", got.State, sawCancel.Load())
	}
	for _, r := range callsOf(t, store) {
		want := mem.CallCancelled
		if r.Key == ran {
			want = mem.CallUnknown
		}
		if r.Status != want {
			t.Fatalf("call %s (key %s, ran %s): %s, want %s", r.CallID, r.Key, ran, r.Status, want)
		}
	}
	if n := len(eventsOfKind(store, proto.EvReconcileNeeded)); n != 1 {
		t.Fatalf("%d reconcile_needed events", n)
	}
	for _, msg := range got.Regs {
		if msg.Role == task.RoleTool {
			t.Fatal("a cancelled round reached the registers")
		}
	}
}

// design §18 test 11: a result that arrives after the cancel is stored on its
// call record as late and never enters the registers.
func TestLateResultIsRecordedLate(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "deaf"}, func(context.Context, bus.DeviceCall) (string, error) {
		close(entered)
		<-gate // ignores its context
		return "paid anyway", nil
	})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_deaf"), nil }, dev)
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	<-entered
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the cancel", func() bool { return load(t, store).State == task.Cancelled })
	if r := callsOf(t, store)["c-1"]; r.Status != mem.CallUnknown || r.Result != nil {
		t.Fatalf("at cancel: %+v", r)
	}
	close(gate)
	waitFor(t, "the late result", func() bool { return callsOf(t, store)["c-1"].Status == mem.CallLate })
	r := callsOf(t, store)["c-1"]
	if r.Result == nil || r.Result.Content != "paid anyway" {
		t.Fatalf("late record %+v", r)
	}
	late := eventsOfKind(store, proto.EvToolLate)
	if len(late) != 1 || !strings.Contains(string(late[0].Data), `"call_id":"c-1"`) {
		t.Fatalf("tool_late events %+v", late)
	}
	got := load(t, store)
	if got.State != task.Cancelled {
		t.Fatalf("task %+v", got)
	}
	for _, msg := range got.Regs {
		if msg.Role == task.RoleTool {
			t.Fatal("a late result reached the registers")
		}
	}
	e.WaitBatches()
}

// A result that is back before the cancel is final keeps its result: it is
// done, not unknown.
func TestResultsInHandAtCancelStayDone(t *testing.T) {
	hold := make(chan struct{})
	entered := make(chan struct{})
	dev := builtin.New("dev").
		Add(bus.ToolDef{Name: "fast", Effect: bus.ReadOnly}, func(context.Context, bus.DeviceCall) (string, error) { return "fast", nil }).
		Add(bus.ToolDef{Name: "slow"}, func(ctx context.Context, _ bus.DeviceCall) (string, error) {
			close(entered)
			<-hold
			return "", ctx.Err()
		})
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.CallTools("dev_fast", "dev_slow"), nil }, dev)
	e.Sched.Add(newTask(t, e, store, "q"))
	e.step(bg, take(t, e), 0)
	<-entered
	waitFor(t, "the fast result", func() bool { return callsOf(t, store)["c-1"].Status == mem.CallDone })
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	close(hold)
	e.WaitBatches()
	recs := callsOf(t, store)
	if recs["c-1"].Status != mem.CallDone || recs["c-1"].Result.Content != "fast" || recs["c-2"].Status != mem.CallUnknown {
		t.Fatalf("records %+v", recs)
	}
}

// A task held for reconciliation has no owner at work: the canceller
// finalises it, and the held call stays unknown for the operator.
func TestCancelOfAHeldTask(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store, mem.CallRecord{Tool: "dev_pay", Status: mem.CallDispatched, Attempt: 1})
	tc := load(t, store)
	if _, err := e.Recover(bg, tc); err != nil {
		t.Fatal(err)
	}
	e.Sched.Add(tc)
	if err := e.Cancel(bg, "t1", ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	got := load(t, store)
	if got.State != task.Cancelled {
		t.Fatalf("task %s/%s", got.State, got.Wait)
	}
	if r := callsOf(t, store)["c-1"]; r.Status != mem.CallUnknown || runs.Load() != 0 {
		t.Fatalf("record %+v runs %d", r, runs.Load())
	}
	if n := len(eventsOfKind(store, proto.EvReconcileNeeded)); n != 1 {
		t.Fatalf("%d reconcile_needed events, want the one from recovery only", n)
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 {
		t.Fatalf("scheduler still holds it: %d/%d/%d", r, w, h)
	}
}

// Recovery finishes a cancel whose interrupt was committed but whose final
// commit was not: planned calls never run, dispatched ones are unknown.
func TestRecoverFinishesACommittedCancel(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store,
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDone, Attempt: 1, Effect: task.ReadOnly, Result: &task.ToolResult{Content: "kept"}},
		mem.CallRecord{Tool: "dev_pay", Status: mem.CallDispatched, Attempt: 1},
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDispatched, Attempt: 1, Effect: task.ReadOnly},
		mem.CallRecord{Tool: "dev_pay", Status: mem.CallPlanned},
	)
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{
		{TaskID: "t1", Kind: mem.InterruptSteer, Text: "also"},
		{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled},
	}}); err != nil {
		t.Fatal(err)
	}
	batch, err := e.Recover(bg, load(t, store))
	if err != nil || len(batch) != 0 {
		t.Fatalf("batch %+v err %v", batch, err)
	}
	got := load(t, store)
	if got.State != task.Cancelled || got.StopReason != "cancelled" {
		t.Fatalf("task %s %q", got.State, got.StopReason)
	}
	recs := callsOf(t, store)
	want := map[string]mem.CallStatus{"c-1": mem.CallDone, "c-2": mem.CallUnknown, "c-3": mem.CallUnknown, "c-4": mem.CallCancelled}
	for id, st := range want {
		if recs[id].Status != st {
			t.Fatalf("%s: %s, want %s", id, recs[id].Status, st)
		}
	}
	if runs.Load() != 0 {
		t.Fatalf("devices ran %d times", runs.Load())
	}
	if n := len(eventsOfKind(store, proto.EvReconcileNeeded)); n != 2 {
		t.Fatalf("%d reconcile_needed, want one per unknown call", n)
	}
	if n := len(eventsOfKind(store, proto.EvSteerDropped)); n != 1 {
		t.Fatalf("%d steer_dropped", n)
	}
}

func TestRecoverRunningWithACommittedCancel(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	tc.State, tc.Version = task.Running, 2
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonDeadline}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Recover(bg, load(t, store)); err != nil {
		t.Fatal(err)
	}
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "deadline" || got.Unconfirmed != 1 {
		t.Fatalf("task %s %q unconfirmed %d", got.State, got.StopReason, got.Unconfirmed)
	}
}

func TestDeadlineFailsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	e.Sched.Add(newTask(t, e, store, "q"))
	if err := e.Cancel(bg, "t1", ReasonDeadline); err != nil {
		t.Fatal(err)
	}
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "deadline" {
		t.Fatalf("task %s %q", got.State, got.StopReason)
	}
	evs := eventsOfKind(store, proto.EvFailed)
	var d map[string]string
	if len(evs) != 1 || json.Unmarshal(evs[0].Data, &d) != nil || d["stop_reason"] != "deadline" {
		t.Fatalf("failed events %+v", evs)
	}
}

func TestCancelRefusesFinishedAndUnknownTasks(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	if err := e.Cancel(bg, "nope", ReasonCancelled); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	e.step(bg, newTask(t, e, store, "q"), 0)
	if err := e.Cancel(bg, "t1", ReasonCancelled); !errors.Is(err, mem.ErrFinished) {
		t.Fatalf("finished: %v", err)
	}
	if k := kinds(store); strings.Contains(k, "cancelled") {
		t.Fatalf("events %s", k)
	}
}

// Cancelling twice is harmless: the second one finds the task finished, or
// (racing the first) is applied by the same final commit.
func TestCancelTwice(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	e.Sched.Add(newTask(t, e, store, "q"))
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = e.Cancel(bg, "t1", ReasonCancelled) }()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil && !errors.Is(err, mem.ErrFinished) {
			t.Fatal(err)
		}
	}
	if n := len(eventsOfKind(store, proto.EvCancelled)); n != 1 {
		t.Fatalf("%d cancelled events", n)
	}
}

// A task whose deadline has passed starts no instruction, even if its timer
// has not run yet.
func TestExpiredDeadlineStartsNothing(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	tc.Budget.Deadline = time.Now().Add(-time.Second)
	e.step(bg, tc, 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "deadline" || sm.Calls() != 0 {
		t.Fatalf("task %s %q after %d model calls", got.State, got.StopReason, sm.Calls())
	}
}
