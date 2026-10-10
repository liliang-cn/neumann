package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// crashSite commits a task caught mid tool round: the assistant asked for
// one call per record, and the records are as given.
func crashSite(t *testing.T, store *mem.Memory, recs ...mem.CallRecord) *task.TCB {
	t.Helper()
	asst := task.Message{Role: task.RoleAssistant}
	for i := range recs {
		recs[i].TaskID = "t1"
		recs[i].CallID = fmt.Sprintf("c-%d", i+1)
		recs[i].Index = i + 1
		if recs[i].Key == "" && recs[i].Attempt > 0 {
			recs[i].Key = fmt.Sprintf("t1/c-%d/%d", i+1, recs[i].Attempt)
		}
		asst.Parts = append(asst.Parts, task.Part{Kind: task.PartToolCall, CallID: recs[i].CallID, Name: recs[i].Tool, Args: recs[i].Args})
	}
	tc := &task.TCB{
		ID: "t1", State: task.Waiting, Wait: task.WaitIO, PC: task.StepTools, Class: "default",
		ReadySince: time.Now(), Regs: []task.Message{task.Text(task.RoleUser, "q"), asst},
		CallSeq: len(recs), Version: 1,
	}
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true, Calls: recs}); err != nil {
		t.Fatal(err)
	}
	return tc
}

func recoverDevices(runs *atomic.Int32, keys chan<- string) bus.Device {
	f := func(_ context.Context, c bus.DeviceCall) (string, error) {
		runs.Add(1)
		if keys != nil {
			keys <- c.Key
		}
		return "fresh " + c.Tool, nil
	}
	return builtin.New("dev").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly}, f).
		Add(bus.ToolDef{Name: "pay"}, f).
		Add(bus.ToolDef{Name: "note", ReportUnknown: true}, f)
}

func reply(model.Request) (model.Response, error) { return sim.Reply("done"), nil }

func TestRecoverRunningCountsUnconfirmed(t *testing.T) {
	e, store, _ := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	tc.State, tc.Version = task.Running, 2
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	batch, err := e.Recover(bg, load(t, store))
	if err != nil || batch != nil {
		t.Fatalf("batch %v err %v", batch, err)
	}
	got := load(t, store)
	if got.State != task.Ready || got.PC != task.StepModel || got.Unconfirmed != 1 || !hasKind(store, "recovered") {
		t.Fatalf("task %+v", got)
	}
}

func TestRecoverSettlesEachKindOfCall(t *testing.T) {
	var runs atomic.Int32
	keys := make(chan string, 8)
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, keys))
	done := &task.ToolResult{Content: "kept"}
	crashSite(t, store,
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDone, Attempt: 1, Effect: task.ReadOnly, Result: done},
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDispatched, Attempt: 1, Effect: task.ReadOnly},
		mem.CallRecord{Tool: "dev_read", Status: mem.CallPlanned, Effect: task.ReadOnly},
		mem.CallRecord{Tool: "dev_note", Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect, ReportUnknown: true},
	)
	tc := load(t, store)
	batch, err := e.Recover(bg, tc)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Key != "t1/c-2/1" || batch[1].Key != "t1/c-3/1" {
		t.Fatalf("batch %+v", batch)
	}
	stored := load(t, store)
	if stored.State != task.Waiting || stored.Wait != task.WaitIO {
		t.Fatalf("task %+v", stored)
	}
	e.Sched.Add(tc)
	e.Dispatch("t1", nil, nil, batch)
	e.WaitBatches()
	got := load(t, store)
	if got.State != task.Ready || got.PC != task.StepModel {
		t.Fatalf("after the round: %s/%s", got.State, got.PC)
	}
	msg := got.Regs[len(got.Regs)-1]
	texts := []string{msg.Parts[0].Text, msg.Parts[1].Text, msg.Parts[2].Text, msg.Parts[3].Text}
	if texts[0] != "kept" || texts[1] != "fresh read" || texts[2] != "fresh read" || !strings.Contains(texts[3], "unknown") || !msg.Parts[3].IsError {
		t.Fatalf("tool message %q", texts)
	}
	if runs.Load() != 2 {
		t.Fatalf("devices ran %d times, want 2 (the done and the side-effect calls must not run)", runs.Load())
	}
}

func TestRecoverHoldsAnUnknownSideEffect(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store,
		mem.CallRecord{Tool: "dev_pay", Args: json.RawMessage(`{"amount":5}`), Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect},
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDispatched, Attempt: 1, Effect: task.ReadOnly},
	)
	tc := load(t, store)
	batch, err := e.Recover(bg, tc)
	if err != nil || len(batch) != 0 {
		t.Fatalf("held round must send nothing: batch %v err %v", batch, err)
	}
	got := load(t, store)
	if got.State != task.Waiting || got.Wait != task.WaitReconcile || !hasKind(store, "reconcile_needed") {
		t.Fatalf("task %+v events %s", got, kinds(store))
	}
	recs, _ := store.Calls(bg, "t1")
	if recs[0].Status != mem.CallUnknown || recs[0].Result != nil || recs[1].Status != mem.CallDispatched {
		t.Fatalf("records %+v", recs)
	}

	// The operator says the payment went through: the read is sent again
	// and the round completes.
	e.Sched.Add(tc)
	held, _ := e.Sched.CheckoutIf("t1", func(*task.TCB) bool { return true })
	batch, err = e.Resolve(bg, held, "c-1", Outcome{Result: "paid"})
	if err != nil || len(batch) != 1 || batch[0].CallID != "c-2" {
		t.Fatalf("batch %+v err %v", batch, err)
	}
	e.Sched.Requeue(held)
	e.Dispatch("t1", nil, nil, batch)
	e.WaitBatches()
	got = load(t, store)
	msg := got.Regs[len(got.Regs)-1]
	if got.State != task.Ready || msg.Parts[0].Text != "paid" || msg.Parts[1].Text != "fresh read" || runs.Load() != 1 {
		t.Fatalf("task %s message %+v runs %d", got.State, msg, runs.Load())
	}
}

func TestResolveRetrySendsANewAttempt(t *testing.T) {
	var runs atomic.Int32
	keys := make(chan string, 4)
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, keys))
	crashSite(t, store, mem.CallRecord{Tool: "dev_pay", Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect})
	tc := load(t, store)
	if _, err := e.Recover(bg, tc); err != nil {
		t.Fatal(err)
	}
	e.Sched.Add(tc)
	held, _ := e.Sched.CheckoutIf("t1", func(*task.TCB) bool { return true })
	batch, err := e.Resolve(bg, held, "c-1", Outcome{Retry: true})
	if err != nil || len(batch) != 1 || batch[0].Attempt != 2 || batch[0].Key != "t1/c-1/2" {
		t.Fatalf("batch %+v err %v", batch, err)
	}
	e.Sched.Requeue(held)
	e.Dispatch("t1", nil, nil, batch)
	e.WaitBatches()
	if k := <-keys; k != "t1/c-1/2" {
		t.Fatalf("device saw key %s", k)
	}
	if got := load(t, store); got.State != task.Ready {
		t.Fatalf("task %+v", got)
	}
}

func TestResolveRefusesWhatIsNotHeld(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store,
		mem.CallRecord{Tool: "dev_pay", Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect},
		mem.CallRecord{Tool: "dev_read", Status: mem.CallDone, Attempt: 1, Effect: task.ReadOnly, Result: &task.ToolResult{Content: "r"}},
	)
	tc := load(t, store)
	// Not held yet: the task is still waiting on I/O.
	if _, err := e.Resolve(bg, tc, "c-1", Outcome{Result: "x"}); !errors.Is(err, ErrNotReconciling) {
		t.Fatalf("got %v", err)
	}
	if _, err := e.Recover(bg, tc); err != nil {
		t.Fatal(err)
	}
	before := *tc
	if _, err := e.Resolve(bg, tc, "c-2", Outcome{Result: "x"}); !errors.Is(err, ErrNotReconciling) {
		t.Fatalf("a call that has its result: %v", err)
	}
	if _, err := e.Resolve(bg, tc, "c-9", Outcome{Result: "x"}); !errors.Is(err, ErrNotReconciling) {
		t.Fatalf("an unknown call: %v", err)
	}
	if tc.Version != before.Version || tc.State != before.State {
		t.Fatalf("a refused Resolve changed the task: %+v", tc)
	}
}

func TestStartCommitFailureIsRetried(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	inner := e.Commit
	failed := false
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if !failed && c.Task != nil && c.Task.State == task.Running {
			failed = true
			return errors.New("database is locked")
		}
		return inner(ctx, c)
	}
	e.step(bg, newTask(t, e, store, "q"), 0)
	if sm.Calls() != 0 {
		t.Fatal("the model was called although the start was not recorded")
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, store); got.State != task.Done || sm.Calls() != 1 {
		t.Fatalf("task %+v, model calls %d", got, sm.Calls())
	}
}

func TestStartCommitGivesUpAfterRetries(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	inner := e.Commit
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		if c.Task != nil && c.Task.State == task.Running {
			return errors.New("disk gone")
		}
		return inner(ctx, c)
	}
	tc := newTask(t, e, store, "q")
	for range maxStartRetries + 1 {
		e.step(bg, tc, 0)
		var ok bool
		ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
		tc, ok = e.Sched.Next(ctx, "default")
		cancel()
		if !ok {
			break
		}
	}
	if r, w, h := e.Sched.Counts(); r+w+h != 0 || sm.Calls() != 0 {
		t.Fatalf("counts %d %d %d, model calls %d", r, w, h, sm.Calls())
	}
}

func TestBudgetCountsUnconfirmedRequests(t *testing.T) {
	e, store, sm := newTestEnv(t, reply)
	tc := newTask(t, e, store, "q")
	tc.Budget.MaxSteps, tc.Unconfirmed = 2, 2
	e.step(bg, tc, 0)
	if got := load(t, store); got.State != task.Done || got.StopReason != "budget" || sm.Calls() != 0 {
		t.Fatalf("task %+v, model calls %d", got, sm.Calls())
	}
}

func TestRoundIsOnlyTheLastTurn(t *testing.T) {
	call := func(id string) task.Part { return task.Part{Kind: task.PartToolCall, CallID: id, Name: "dev_read"} }
	tc := &task.TCB{ID: "t1", Regs: []task.Message{
		task.Text(task.RoleUser, "q"),
		{Role: task.RoleAssistant, Parts: []task.Part{call("c-1")}},
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "r"}}},
		{Role: task.RoleAssistant, Parts: []task.Part{call("c-3"), call("c-2")}},
	}}
	recs := []mem.CallRecord{
		{CallID: "c-1", Index: 1, Status: mem.CallDone},
		{CallID: "c-3", Index: 3, Status: mem.CallPlanned},
		{CallID: "c-2", Index: 2, Status: mem.CallDispatched},
	}
	rs := round(tc, recs)
	if len(rs) != 2 || rs[0].CallID != "c-2" || rs[1].CallID != "c-3" {
		t.Fatalf("round %+v", rs)
	}
	tc.Regs = tc.Regs[:3] // the last round is answered
	if rs := round(tc, recs); rs != nil {
		t.Fatalf("an answered round is not current: %+v", rs)
	}
}

func TestRecoverRefusesACallLogItCannotSettle(t *testing.T) {
	for name, rec := range map[string]mem.CallRecord{
		"done without a result": {Tool: "dev_read", Status: mem.CallDone, Attempt: 1, Effect: task.ReadOnly},
		"foreign status":        {Tool: "dev_read", Status: mem.CallStatus("cancelled"), Attempt: 1, Effect: task.ReadOnly},
	} {
		t.Run(name, func(t *testing.T) {
			var runs atomic.Int32
			e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
			crashSite(t, store, rec)
			before := load(t, store)
			batch, err := e.Recover(bg, before)
			if err == nil || !strings.Contains(err.Error(), "t1") || batch != nil {
				t.Fatalf("batch %v err %v", batch, err)
			}
			if got := load(t, store); got.Version != before.Version || got.State != task.Waiting || runs.Load() != 0 {
				t.Fatalf("a refused recovery changed the task: %+v (runs %d)", got, runs.Load())
			}
		})
	}
}

func TestHeldCallIsAnnouncedAgainOnTheNextRestart(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store, mem.CallRecord{Tool: "dev_pay", Args: json.RawMessage(`{"amount":5}`), Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect, Device: "dev"})
	if _, err := e.Recover(bg, load(t, store)); err != nil {
		t.Fatal(err)
	}
	held := load(t, store)
	batch, err := e.Recover(bg, held) // the machine restarts again before anyone resolved it
	if err != nil || batch != nil {
		t.Fatalf("batch %v err %v", batch, err)
	}
	if got := load(t, store); got.Version != held.Version || got.Wait != task.WaitReconcile {
		t.Fatalf("re-announcing must not change the task: %+v", got)
	}
	evs, _ := store.Events(bg, 0, 0)
	last := evs[len(evs)-1]
	var d struct {
		From string `json:"from"`
		Held []struct {
			CallID string `json:"call_id"`
			Device string `json:"device"`
			Key    string `json:"key"`
		} `json:"held"`
	}
	if err := json.Unmarshal(last.Data, &d); err != nil {
		t.Fatal(err)
	}
	if last.Kind != "recovered" || d.From != "reconcile" || len(d.Held) != 1 || d.Held[0].CallID != "c-1" || d.Held[0].Device != "dev" || d.Held[0].Key != "t1/c-1/1" {
		t.Fatalf("event %s %s", last.Kind, last.Data)
	}
	if runs.Load() != 0 {
		t.Fatal("a held call ran")
	}
}

func TestResolveRefusesRetryWithAResult(t *testing.T) {
	var runs atomic.Int32
	e, store, _ := newTestEnv(t, reply, recoverDevices(&runs, nil))
	crashSite(t, store, mem.CallRecord{Tool: "dev_pay", Status: mem.CallDispatched, Attempt: 1, Effect: task.SideEffect})
	tc := load(t, store)
	if _, err := e.Recover(bg, tc); err != nil {
		t.Fatal(err)
	}
	before := *tc
	for _, o := range []Outcome{{Retry: true, Result: "paid"}, {Retry: true, IsError: true}} {
		if batch, err := e.Resolve(bg, tc, "c-1", o); err == nil || batch != nil {
			t.Fatalf("%+v: batch %v err %v", o, batch, err)
		}
	}
	recs, _ := store.Calls(bg, "t1")
	if tc.Version != before.Version || tc.State != before.State || recs[0].Status != mem.CallUnknown || runs.Load() != 0 {
		t.Fatalf("a refused Resolve changed something: %+v %+v", tc, recs[0])
	}
}
