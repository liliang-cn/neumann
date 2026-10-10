package neumann_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

type modelFn = func(model.Request) (model.Response, error)

func start(t *testing.T, cfg neumann.Config, fn modelFn) (*neumann.Machine, *sim.Model) {
	t.Helper()
	sm := &sim.Model{Fn: fn}
	cfg.Model = sm
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
	}
	if cfg.SteerFormat == nil {
		// Tests compare the text the model received with the steer sent;
		// TestDefaultSteerFormat covers the framed default.
		cfg.SteerFormat = func(s string) string { return s }
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
	return m, sm
}

func submit(t *testing.T, m *neumann.Machine, in proto.Instruction) string {
	t.Helper()
	in.Kind = proto.KindSubmit
	id, err := m.Submit(bg, in)
	if err != nil {
		t.Fatalf("submit %q: %v", in.Input, err)
	}
	return id
}

func waitDone(t *testing.T, m *neumann.Machine, id string) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	for ev := range m.Events(ctx, 0) {
		if ev.TaskID == id && ev.Terminal() {
			tc, err := m.Load(bg, id)
			if err != nil {
				t.Fatalf("load %s: %v", id, err)
			}
			return tc
		}
	}
	t.Fatalf("task %s did not finish within 5s", id)
	return nil
}

func eventsOf(t *testing.T, m *neumann.Machine, id string) []proto.Event {
	t.Helper()
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []proto.Event
	for _, ev := range all {
		if ev.TaskID == id {
			out = append(out, ev)
		}
	}
	return out
}

func echoDev() bus.Device {
	return builtin.New("dev").Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		return string(c.Args), nil
	})
}

// gateDev is a device whose "slow" tool reports on entered when a call
// starts and blocks until gate is closed.
func gateDev(entered chan<- string, gate <-chan struct{}) bus.Device {
	return builtin.New("dev").
		Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			return string(c.Args), nil
		}).
		Add(bus.ToolDef{Name: "slow"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
			entered <- c.Key
			select {
			case <-gate:
				return "slow done", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
}

func newGate() (chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch) }) }
}

func TestSubmitAnswers(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("hello"), nil })
	id := submit(t, m, proto.Instruction{Input: "hi"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "hello" {
		t.Fatalf("task %+v", got)
	}
	var kinds []string
	for _, ev := range eventsOf(t, m, id) {
		kinds = append(kinds, ev.Kind)
	}
	if k := strings.Join(kinds, ","); k != "accepted,step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
}

func TestSubmitValidation(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	cases := []proto.Instruction{
		{Kind: proto.KindSubmit},
		{Kind: proto.KindSteer, Input: "x"},
		{Kind: proto.KindSubmit, Input: "x", Class: "nope"},
	}
	for _, in := range cases {
		if _, err := m.Submit(bg, in); err == nil {
			t.Errorf("Submit(%+v) succeeded", in)
		}
	}
}

func TestAdmissionIsExact(t *testing.T) { // design §18 invariant 20
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Cores: 1, MaxQueued: 4}, func(model.Request) (model.Response, error) {
		<-gate
		return sim.Reply("ok"), nil
	})
	var accepted, busy atomic.Int32
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: fmt.Sprintf("t-%d", i)})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, neumann.ErrBusy):
				var ce *neumann.CapacityError
				if !errors.As(err, &ce) || ce.Limit != 4 {
					t.Errorf("busy error without capacity details: %v", err)
				}
				busy.Add(1)
			default:
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 4 || busy.Load() != 28 {
		t.Fatalf("accepted %d busy %d, want 4 and 28", accepted.Load(), busy.Load())
	}
}

func TestSteerOnFinishedOrUnknownTask(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	id := submit(t, m, proto.Instruction{Input: "q"})
	waitDone(t, m, id)
	if err := m.Steer(bg, id, "too late"); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("steer after done: %v", err)
	}
	if err := m.Steer(bg, "no-such-task", "x"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("steer unknown: %v", err)
	}
}

func TestEventsReplayFromAnySeq(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "q"}))
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for from := range int64(len(all)) {
		ctx, cancel := context.WithTimeout(bg, time.Second)
		var got []proto.Event
		for ev := range m.Events(ctx, from) {
			got = append(got, ev)
			if len(got) == len(all)-int(from) {
				break
			}
		}
		cancel()
		if len(got) != len(all)-int(from) || len(got) == 0 || got[0].Seq != from+1 {
			t.Fatalf("from %d: got %d events %+v", from, len(got), got)
		}
	}
}

func TestSubmitCopiesPageTables(t *testing.T) {
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	allow := []string{"dev_*"}
	deny := []string{"dev_x"}
	id := submit(t, m, proto.Instruction{Input: "q", Allow: allow, Deny: deny})
	allow[0], deny[0] = "mutated", "mutated"
	got := waitDone(t, m, id)
	if !slices.Equal(got.Allow, []string{"dev_*"}) || !slices.Equal(got.Deny, []string{"dev_x"}) {
		t.Fatalf("allow %v deny %v", got.Allow, got.Deny)
	}
}

func TestSubmitAndSteerValidation(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	for _, p := range []int{-1, 2} {
		if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x", Priority: p}); err == nil {
			t.Errorf("priority %d accepted", p)
		}
	}
	id := submit(t, m, proto.Instruction{Input: "q", Priority: 1})
	if err := m.Steer(bg, id, "  \n"); err == nil {
		t.Error("blank steer accepted")
	}
}

func TestStoppedMachine(t *testing.T) {
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("x"), nil }}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(bg); err != nil {
		t.Fatalf("second stop: %v", err)
	}
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x"}); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("submit after stop: %v", err)
	}
	if err := m.Steer(bg, "t", "x"); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("steer after stop: %v", err)
	}
	if err := m.Start(bg); err == nil {
		t.Fatal("start after stop succeeded")
	}
}

func TestStopBeforeStart(t *testing.T) {
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("x"), nil }}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x"}); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("submit: %v", err)
	}
	if err := m.Start(bg); err == nil {
		t.Fatal("start after stop succeeded")
	}
	select {
	case _, ok := <-m.Events(bg, 0):
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(time.Second):
		t.Fatal("events did not end after stop")
	}
}

func toolModel(fn func() model.Response) modelFn {
	return func(req model.Request) (model.Response, error) {
		for _, msg := range req.Messages {
			if msg.Role == task.RoleTool {
				return sim.Reply("finished"), nil
			}
		}
		return fn(), nil
	}
}

func TestStopDrainsInFlightToolBatch(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	sm := &sim.Model{Fn: toolModel(func() model.Response { return sim.CallTools("dev_slow") })}
	m, err := neumann.New(neumann.Config{Model: sm, Devices: []bus.Device{gateDev(entered, gate)}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered

	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	sawResult := make(chan bool, 1)
	go func() {
		for ev := range m.Events(ctx, 0) {
			if ev.TaskID == id && ev.Kind == proto.EvToolResult {
				sawResult <- true
				return
			}
		}
		sawResult <- false
	}()
	go func() {
		// Release the tool only once Stop has marked the machine stopped.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if errors.Is(m.Steer(bg, "nope", "x"), neumann.ErrStopped) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		release()
	}()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !<-sawResult {
		t.Fatal("subscriber never saw the tool result")
	}
	got, err := m.Load(bg, id)
	// Stop closes the scheduler, so the model step after the tools is not
	// fetched: the task rests at its committed, resumable ready state.
	if err != nil || got.State != task.Ready || got.PC != task.StepModel || got.Regs[len(got.Regs)-1].Role != task.RoleTool {
		t.Fatalf("task %+v err %v", got, err)
	}
}

func TestStopTimeoutHaltsWork(t *testing.T) {
	entered := make(chan string, 1)
	gate, release := newGate()
	defer release()
	sm := &sim.Model{Fn: toolModel(func() model.Response { return sim.CallTools("dev_slow") })}
	m, err := neumann.New(neumann.Config{Model: sm, Devices: []bus.Device{gateDev(entered, gate)}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	if err := m.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop: %v", err)
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("stop took %v", d)
	}
	got, err := m.Load(bg, id)
	if err != nil || got.State != task.Waiting {
		t.Fatalf("task %+v err %v", got, err)
	}
	recs, err := m.Calls(bg, id)
	if err != nil || len(recs) != 1 || recs[0].Status != mem.CallDispatched || recs[0].Attempt != 1 || recs[0].Result != nil {
		t.Fatalf("call records %+v err %v", recs, err)
	}
	before, _ := m.EventLog(bg, 0, 0)
	time.Sleep(100 * time.Millisecond)
	after, _ := m.EventLog(bg, 0, 0)
	if len(before) != len(after) {
		t.Fatalf("events grew after stop: %d -> %d", len(before), len(after))
	}
}

func TestEventsEndAfterStop(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "q"}))
	ch := m.Events(bg, 0)
	if err := m.Stop(bg); err != nil {
		t.Fatal(err)
	}
	timeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("events stream did not end after stop")
		}
	}
}

func TestFinishedTaskFreesItsSlot(t *testing.T) {
	m, _ := start(t, neumann.Config{MaxQueued: 1}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "a"}))
	deadline := time.Now().Add(2 * time.Second)
	for m.Status().Unfinished != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "b"}); err != nil {
		t.Fatalf("second submit: %v", err)
	}
}

func TestSubmitRejectsTaskIDWithSlash(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "q", TaskID: "a/b"}); err == nil {
		t.Fatal("task id with / accepted")
	}
}

func TestDefaultSteerFormat(t *testing.T) {
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	var mu sync.Mutex
	var saw []string
	sm := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		mu.Lock()
		saw = sim.UserTexts(req)
		mu.Unlock()
		return sim.Reply("ok"), nil
	}}
	m, err := neumann.New(neumann.Config{Model: sm, Devices: []bus.Device{gateDev(entered, gate)}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		m.Stop(ctx)
	}()
	id := submit(t, m, proto.Instruction{Input: "q"})
	<-entered
	if err := m.Steer(bg, id, "also X"); err != nil {
		t.Fatal(err)
	}
	release()
	waitDone(t, m, id)
	mu.Lock()
	defer mu.Unlock()
	if len(saw) != 2 || saw[1] != neumann.DefaultSteerFormat("also X") || !strings.HasSuffix(saw[1], "also X") {
		t.Fatalf("model saw %q", saw)
	}
}

type warmModel struct {
	*sim.Model
	warmed chan struct{}
}

func (w *warmModel) Warm(context.Context) error { close(w.warmed); return nil }

func TestStartWarmsTheModelConnection(t *testing.T) {
	wm := &warmModel{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("x"), nil }}, warmed: make(chan struct{})}
	m, err := neumann.New(neumann.Config{Model: wm, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		m.Stop(ctx)
	}()
	select {
	case <-wm.warmed:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not warm the model connection")
	}
}
