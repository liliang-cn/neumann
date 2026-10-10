package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// ctxModel is an adapter that sees its context, unlike sim.Model's Fn.
type ctxModel struct {
	fn func(ctx context.Context, req model.Request) (model.Response, error)
}

func (m ctxModel) Provider() string { return "ctx" }
func (m ctxModel) Call(ctx context.Context, req model.Request) (model.Response, error) {
	return m.fn(ctx, req)
}

func startWith(t *testing.T, cfg neumann.Config) *neumann.Machine {
	t.Helper()
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
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

func callStatus(t *testing.T, m *neumann.Machine, id string) map[string]mem.CallStatus {
	t.Helper()
	recs, err := m.Calls(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]mem.CallStatus{}
	for _, r := range recs {
		out[r.CallID] = r.Status
	}
	return out
}

func kindsOf(t *testing.T, m *neumann.Machine, id string) []string {
	t.Helper()
	var out []string
	for _, ev := range eventsOf(t, m, id) {
		out = append(out, ev.Kind)
	}
	return out
}

func TestCancelValidation(t *testing.T) {
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: payModel}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(bg, "t1"); !errors.Is(err, neumann.ErrNotStarted) {
		t.Fatalf("before start: %v", err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(bg, "nope"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
	id, _ := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x"})
	waitDone(t, m, id)
	if err := m.Cancel(bg, id); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("finished task: %v", err)
	}
	stop(t, m)
	if err := m.Cancel(bg, id); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("after stop: %v", err)
	}
}

// design §9.5 and §18 test 11, through the public API: the in-flight call
// sees its context cancelled, the task ends Cancelled with the call recorded
// unknown (it ran), and an undelivered steer is reported dropped.
func TestCancelATaskWithAToolInFlight(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		return sim.Reply("too late"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered
	if err := m.Steer(bg, id, "and then"); err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(bg, id); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	if got.State != task.Cancelled || got.StopReason != "cancelled" {
		t.Fatalf("task %s %q", got.State, got.StopReason)
	}
	if st := callStatus(t, m, id); st["c-1"] != mem.CallUnknown {
		t.Fatalf("calls %v", st)
	}
	k := kindsOf(t, m, id)
	if !slices.Contains(k, proto.EvReconcileNeeded) || !slices.Contains(k, proto.EvSteerDropped) || k[len(k)-2] != proto.EvCancelled {
		t.Fatalf("events %v", k)
	}
	if st := m.Status(); st.Unfinished != 0 || st.Ready+st.Waiting+st.Held != 0 {
		t.Fatalf("status %+v", st)
	}
}

// A late result after a cancel, end to end: recorded late, never in the
// registers, reported by tool_late.
func TestLateResultThroughTheMachine(t *testing.T) {
	entered, gate := make(chan struct{}), make(chan struct{})
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "deaf"}, func(context.Context, bus.DeviceCall) (string, error) {
		close(entered)
		<-gate
		return "done anyway", nil
	})
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev}}, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_deaf"), nil
	})
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	events := m.Events(ctx, 0)
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered
	if err := m.Cancel(bg, id); err != nil {
		t.Fatal(err)
	}
	for ev := range events {
		if ev.TaskID == id && ev.Kind == proto.EvCancelled {
			close(gate)
		}
		if ev.TaskID == id && ev.Kind == proto.EvToolLate {
			break
		}
	}
	recs, _ := m.Calls(bg, id)
	if len(recs) != 1 || recs[0].Status != mem.CallLate || recs[0].Result == nil || recs[0].Result.Content != "done anyway" {
		t.Fatalf("records %+v", recs)
	}
	got, _ := m.Load(bg, id)
	for _, msg := range got.Regs {
		if msg.Role == task.RoleTool {
			t.Fatal("the late result reached the registers")
		}
	}
}

// deadlineMachine starts a machine on a fake clock. Its model answers "A"
// only when released (ignoring cancellation), blocks on its context for
// "running", calls the slow tool for "tools", and answers anything else.
func deadlineMachine(t *testing.T, store mem.Store, cores int) (*neumann.Machine, *sim.Clock, chan string, func(), chan struct{}) {
	t.Helper()
	return deadlineMachineAt(t, store, cores, clock0)
}

var clock0 = time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)

func deadlineMachineAt(t *testing.T, store mem.Store, cores int, now time.Time) (*neumann.Machine, *sim.Clock, chan string, func(), chan struct{}) {
	t.Helper()
	clk := sim.NewClock(now)
	entered := make(chan string, 4)
	gate, release := newGate()
	t.Cleanup(release)
	inModel := make(chan struct{}, 4)
	fn := func(ctx context.Context, req model.Request) (model.Response, error) {
		switch sim.FirstUser(req) {
		case "A":
			inModel <- struct{}{}
			<-gate
			return sim.Reply("A done"), nil
		case "running":
			inModel <- struct{}{}
			<-ctx.Done()
			return model.Response{}, ctx.Err()
		case "tools":
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("dev_slow"), nil
			}
		}
		return sim.Reply("ok"), nil
	}
	m := startWith(t, neumann.Config{
		Model: ctxModel{fn: fn}, Store: store, Cores: cores, Clock: clk,
		Devices: []bus.Device{gateDev(entered, gate)},
		Logf:    t.Logf,
	})
	return m, clk, entered, release, inModel
}

func failedByDeadline(t *testing.T, m *neumann.Machine, id string) {
	t.Helper()
	got := waitDone(t, m, id)
	if got.State != task.Failed || got.StopReason != "deadline" {
		t.Fatalf("task %s %q", got.State, got.StopReason)
	}
	var d map[string]string
	evs := eventsOf(t, m, id)
	for _, ev := range evs {
		if ev.Kind == proto.EvFailed {
			json.Unmarshal(ev.Data, &d)
		}
	}
	if d["stop_reason"] != "deadline" {
		t.Fatalf("failed event %v", d)
	}
}

func inAnHour(clk *sim.Clock) *task.Budget { return &task.Budget{Deadline: clk.Now().Add(time.Hour)} }

// design §18 test 10, for the states phase 3 has: the deadline fires whether
// the task is ready, waiting on tools, waiting for reconciliation, or running.
func TestDeadlineFiresInEveryState(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		m, clk, _, release, inModel := deadlineMachine(t, mem.NewMemory(), 1)
		a := submit(t, m, proto.Instruction{Input: "A"})
		<-inModel // the only core is busy
		b := submit(t, m, proto.Instruction{Input: "B", Budget: inAnHour(clk)})
		if st := m.Status(); st.Ready != 1 {
			t.Fatalf("B should be ready; status %+v", st)
		}
		clk.Advance(time.Hour)
		failedByDeadline(t, m, b)
		release()
		if got := waitDone(t, m, a); got.State != task.Done {
			t.Fatalf("A %s", got.State)
		}
	})
	t.Run("waiting_tools", func(t *testing.T) {
		m, clk, entered, _, _ := deadlineMachine(t, mem.NewMemory(), 2)
		id := submit(t, m, proto.Instruction{Input: "tools", Budget: inAnHour(clk)})
		<-entered
		clk.Advance(time.Hour)
		failedByDeadline(t, m, id)
		if st := callStatus(t, m, id); st["c-1"] != mem.CallUnknown {
			t.Fatalf("calls %v", st)
		}
	})
	t.Run("waiting_reconcile", func(t *testing.T) {
		store := mem.NewMemory()
		held := &task.TCB{
			ID: "held", State: task.Waiting, Wait: task.WaitIO, PC: task.StepTools, Class: neumann.DefaultClass,
			Regs: []task.Message{task.Text(task.RoleUser, "pay"), {Role: task.RoleAssistant, Parts: []task.Part{
				{Kind: task.PartToolCall, CallID: "c-1", Name: "dev_slow", Args: json.RawMessage(`{}`)}}}},
			Budget: task.Budget{Deadline: clock0.Add(time.Hour)}, CallSeq: 1, Version: 1,
		}
		rec := mem.CallRecord{TaskID: "held", CallID: "c-1", Index: 1, Tool: "dev_slow", Status: mem.CallDispatched, Attempt: 1, Key: "held/c-1/1", Device: "dev"}
		if _, err := store.Commit(bg, mem.CommitSet{Task: held, NewTask: true, Calls: []mem.CallRecord{rec}}); err != nil {
			t.Fatal(err)
		}
		m, clk, _, _, _ := deadlineMachine(t, store, 1)
		if got, _ := m.Load(bg, "held"); got.Wait != task.WaitReconcile {
			t.Fatalf("recovered as %s/%s", got.State, got.Wait)
		}
		clk.Advance(time.Hour)
		failedByDeadline(t, m, "held")
	})
	t.Run("running", func(t *testing.T) {
		m, clk, _, _, inModel := deadlineMachine(t, mem.NewMemory(), 1)
		id := submit(t, m, proto.Instruction{Input: "running", Budget: inAnHour(clk)})
		<-inModel
		clk.Advance(time.Hour)
		failedByDeadline(t, m, id)
		if got, _ := m.Load(bg, id); got.Unconfirmed != 1 {
			t.Fatalf("unconfirmed %d", got.Unconfirmed)
		}
	})
	t.Run("already_past", func(t *testing.T) {
		m, clk, _, _, _ := deadlineMachine(t, mem.NewMemory(), 1)
		id := submit(t, m, proto.Instruction{Input: "B", Budget: &task.Budget{Deadline: clk.Now().Add(-time.Second)}})
		failedByDeadline(t, m, id)
	})
}

// A deadline survives a restart: the new machine arms it again, and one that
// passed while the machine was down fires at once.
func TestDeadlineSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	open := func() *sqlite.Store {
		s, err := sqlite.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s1 := open()
	m1, clk, entered, _, _ := deadlineMachine(t, s1, 1)
	soon := submit(t, m1, proto.Instruction{Input: "tools", Budget: &task.Budget{Deadline: clk.Now().Add(time.Minute)}})
	later := submit(t, m1, proto.Instruction{Input: "tools", Budget: &task.Budget{Deadline: clk.Now().Add(time.Hour)}})
	<-entered
	<-entered
	// Stopped with both calls in flight: they are halted, not finished.
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	m1.Stop(ctx)
	cancel()
	s1.Close()

	// The machine comes back two minutes later: "soon" passed while it was
	// down and fails at once; "later" is armed again.
	s2 := open()
	defer s2.Close()
	m2, clk2, _, _, _ := deadlineMachineAt(t, s2, 1, clock0.Add(2*time.Minute))
	failedByDeadline(t, m2, soon)
	if got, _ := m2.Load(bg, later); got.State.Terminal() {
		t.Fatalf("later ended early: %s", got.State)
	}
	clk2.Advance(time.Hour)
	failedByDeadline(t, m2, later)
}

// design §18 test 9: Submit, Steer and Cancel that returned success are
// durable even if the process dies the moment they return.
func TestAcceptedInputSurvivesACrash(t *testing.T) {
	type run struct {
		name  string
		model func(gate <-chan struct{}, inModel chan<- struct{}) modelFn
		act   func(t *testing.T, m *neumann.Machine, inModel <-chan struct{}) string
		check func(t *testing.T, got *task.TCB, saw []string, events []proto.Event)
	}
	blockFirst := func(gate <-chan struct{}, inModel chan<- struct{}) modelFn {
		var once sync.Once
		return func(req model.Request) (model.Response, error) {
			first := false
			once.Do(func() { first = true })
			if first {
				inModel <- struct{}{}
				<-gate
			}
			return sim.Reply("done"), nil
		}
	}
	runs := []run{
		{
			name:  "submit",
			model: blockFirst,
			act: func(t *testing.T, m *neumann.Machine, _ <-chan struct{}) string {
				return submit(t, m, proto.Instruction{Input: "q"})
			},
			check: func(t *testing.T, got *task.TCB, _ []string, _ []proto.Event) {
				if got.State != task.Done {
					t.Fatalf("task %s", got.State)
				}
			},
		},
		{
			name:  "steer",
			model: blockFirst,
			act: func(t *testing.T, m *neumann.Machine, inModel <-chan struct{}) string {
				id := submit(t, m, proto.Instruction{Input: "q"})
				<-inModel
				if err := m.Steer(bg, id, "and also"); err != nil {
					t.Fatal(err)
				}
				return id
			},
			check: func(t *testing.T, got *task.TCB, saw []string, events []proto.Event) {
				if got.State != task.Done || !slices.Contains(saw, "and also") {
					t.Fatalf("task %s; the model saw %v", got.State, saw)
				}
			},
		},
		{
			name:  "cancel",
			model: blockFirst,
			act: func(t *testing.T, m *neumann.Machine, inModel <-chan struct{}) string {
				id := submit(t, m, proto.Instruction{Input: "q"})
				<-inModel // its vCore is stuck in a call that ignores the cancel
				if err := m.Cancel(bg, id); err != nil {
					t.Fatal(err)
				}
				return id
			},
			check: func(t *testing.T, got *task.TCB, _ []string, _ []proto.Event) {
				if got.State != task.Cancelled || got.Unconfirmed != 1 {
					t.Fatalf("task %s unconfirmed %d", got.State, got.Unconfirmed)
				}
			},
		},
	}
	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "neumann.db")
			s1, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			cs := &crashStore{Store: s1}
			cs.left.Store(1 << 30)
			gate, release := newGate()
			inModel := make(chan struct{}, 1)
			m1, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: r.model(gate, inModel)}, Store: cs, Logf: func(string, ...any) {},
				SteerFormat: func(s string) string { return s }})
			if err != nil {
				t.Fatal(err)
			}
			if err := m1.Start(bg); err != nil {
				t.Fatal(err)
			}
			id := r.act(t, m1, inModel)
			cs.crash() // the process dies the moment the call returned
			release()
			stop(t, m1)
			s1.Close()

			s2, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			var mu sync.Mutex
			var saw []string
			m2 := startWith(t, neumann.Config{Store: s2, SteerFormat: func(s string) string { return s },
				Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
					mu.Lock()
					saw = sim.UserTexts(req)
					mu.Unlock()
					return sim.Reply("done"), nil
				}}})
			got := waitDone(t, m2, id)
			mu.Lock()
			defer mu.Unlock()
			r.check(t, got, saw, eventsOf(t, m2, id))
		})
	}
}
