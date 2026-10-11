package neumann_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/bus/tasks"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// sendTurn is an assistant turn calling tasks_send.
func sendTurn(to, text string) model.Response {
	args, _ := json.Marshal(map[string]string{"task_id": to, "text": text})
	return model.Response{Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
		{Kind: task.PartToolCall, ProviderCallID: "p-send", Name: "tasks_send", Args: args},
	}}, FinishReason: "tool_calls"}
}

// lastUser is the text of the request's last user message.
func lastUser(req model.Request) string {
	texts := sim.UserTexts(req)
	if len(texts) == 0 {
		return ""
	}
	return texts[len(texts)-1]
}

// countSends counts the tasks device's send calls, in this process.
type countSends struct {
	bus.Device
	n atomic.Int32
}

func (c *countSends) Call(ctx context.Context, call bus.DeviceCall) (task.ToolResult, error) {
	if call.Tool == "send" {
		c.n.Add(1)
	}
	return c.Device.Call(ctx, call)
}

// Two tasks talk through the tasks device: asker asks helper a question
// while helper works, helper answers it with tasks_send while asker works,
// and asker answers its user with helper's reply. Each message reaches its
// model with its sender in front, and each delivery is an event naming it.
func TestTwoTasksExchangeMessages(t *testing.T) {
	gates := map[string]chan struct{}{"asker": make(chan struct{}), "helper": make(chan struct{})}
	entered := make(chan string, 4)
	work := builtin.New("desk").Add(bus.ToolDef{Name: "work", Effect: bus.ReadOnly}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		entered <- c.Task
		select {
		case <-gates[c.Task]:
			return "worked", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	dev := tasks.New("tasks")
	var mu sync.Mutex
	saw := map[string][]string{}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev, work}}, func(req model.Request) (model.Response, error) {
		who := sim.FirstUser(req)
		mu.Lock()
		saw[who] = sim.UserTexts(req)
		mu.Unlock()
		switch who {
		case "helper":
			switch {
			case sim.ToolRounds(req) == 0:
				return sim.CallTools("desk_work"), nil
			case sim.ToolRounds(req) == 1 && strings.HasPrefix(lastUser(req), "[message from task:asker] "):
				return sendTurn("asker", "42"), nil
			}
			return sim.Reply("answered asker"), nil
		default: // asker
			switch sim.ToolRounds(req) {
			case 0:
				return sendTurn("helper", "What is 6 × 7?"), nil
			case 1:
				return sim.CallTools("desk_work"), nil
			}
			return sim.Reply("helper says: " + strings.TrimPrefix(lastUser(req), "[message from task:helper] ")), nil
		}
	})
	// Bound before any task runs; start has already started m, which is
	// fine only because nothing was submitted yet.
	dev.Bind(m)
	submit(t, m, proto.Instruction{TaskID: "helper", Input: "helper"})
	if who := <-entered; who != "helper" {
		t.Fatalf("%s started working first", who)
	}
	submit(t, m, proto.Instruction{TaskID: "asker", Input: "asker"})
	if who := <-entered; who != "asker" { // its question is sent: tasks_send came first
		t.Fatalf("%s started working second", who)
	}
	close(gates["helper"]) // helper's next step reads the question and answers it
	waitDone(t, m, "helper")
	close(gates["asker"])
	asker := waitDone(t, m, "asker")
	if asker.LastAnswer() != "helper says: 42" {
		t.Fatalf("asker answered %q", asker.LastAnswer())
	}
	mu.Lock()
	if got := strings.Join(saw["helper"], "|"); got != "helper|[message from task:asker] What is 6 × 7?" {
		t.Fatalf("helper's model saw %q", got)
	}
	if got := strings.Join(saw["asker"], "|"); got != "asker|[message from task:helper] 42" {
		t.Fatalf("asker's model saw %q", got)
	}
	mu.Unlock()
	if got := kindsData(t, m, "helper", proto.EvSteerDelivered); len(got) != 1 || got[0] != proto.EvSteerDelivered+` {"from":"task:asker","seq":1}` {
		t.Fatalf("helper's steer events %q", got)
	}
	if got := kindsData(t, m, "asker", proto.EvSteerDelivered); len(got) != 1 || got[0] != proto.EvSteerDelivered+` {"from":"task:helper","seq":2}` {
		t.Fatalf("asker's steer events %q", got)
	}
}

// dieAtSteer is a process that dies right after a steer lands: the steer is
// durable, but its sender never hears so and records nothing more.
type dieAtSteer struct {
	mem.Store
	dead atomic.Bool
}

func (d *dieAtSteer) Commit(ctx context.Context, cs mem.CommitSet) ([]proto.Event, error) {
	if d.dead.Load() {
		return nil, errCrashed
	}
	if len(cs.NewInterrupts) > 0 && cs.NewInterrupts[0].Kind == mem.InterruptSteer {
		d.dead.Store(true)
		d.Store.Commit(ctx, cs)
		return nil, errCrashed
	}
	return d.Store.Commit(ctx, cs)
}

// A tasks_send whose steer landed just before the process died is sent again
// by recovery (it is Idempotent) under the same key, and the device does not
// send twice; the steer, undelivered at the crash, reaches the other task
// after the restart with its sender in front.
func TestSendAcrossACrashIsSentOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	script := func(req model.Request) (model.Response, error) {
		switch who := sim.FirstUser(req); {
		case who == "asker" && sim.ToolRounds(req) == 0:
			return sendTurn("helper", "ping"), nil
		case who == "helper" && sim.ToolRounds(req) == 0:
			return sim.CallTools("desk_work"), nil
		}
		return sim.Reply("heard: " + lastUser(req)), nil
	}
	// boot starts a process on the database; its desk_work returns once
	// gate is closed.
	boot := func(store mem.Store, gate chan struct{}, entered chan string) (*neumann.Machine, *countSends) {
		work := builtin.New("desk").Add(bus.ToolDef{Name: "work", Effect: bus.ReadOnly}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
			entered <- c.Task
			select {
			case <-gate:
				return "worked", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
		dev := tasks.New("tasks")
		counted := &countSends{Device: dev}
		m, err := neumann.New(neumann.Config{
			Model: &sim.Model{Fn: script}, Store: store, Devices: []bus.Device{counted, work},
			SteerFormat: func(s string) string { return s }, Logf: func(string, ...any) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		dev.Bind(m)
		if err := m.Start(bg); err != nil {
			t.Fatal(err)
		}
		return m, counted
	}

	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dying := &dieAtSteer{Store: s1}
	entered := make(chan string, 4)
	m1, sends1 := boot(dying, make(chan struct{}), entered)
	submit(t, m1, proto.Instruction{TaskID: "helper", Input: "helper"})
	<-entered
	submit(t, m1, proto.Instruction{TaskID: "asker", Input: "asker"})
	deadline := time.Now().Add(5 * time.Second)
	for !dying.dead.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the steer was never committed")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	m1.Stop(ctx) // helper's work is still held: the stop halts it
	cancel()
	s1.Close()
	if n := sends1.n.Load(); n != 1 {
		t.Fatalf("the first process sent %d times", n)
	}

	s2, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	open := make(chan struct{})
	close(open)
	m2, sends2 := boot(s2, open, make(chan string, 4))
	defer stop(t, m2)
	asker := waitDone(t, m2, "asker")
	helper := waitDone(t, m2, "helper")
	if n := sends2.n.Load(); n != 1 {
		t.Fatalf("recovery sent the call %d times, want once", n)
	}
	if asker.State != task.Done {
		t.Fatalf("asker ended %s: %s", asker.State, asker.StopReason)
	}
	calls, _ := m2.Calls(bg, "asker")
	if len(calls) != 1 || calls[0].Status != mem.CallDone || calls[0].Result.IsError || calls[0].Result.Content != "sent to task helper" {
		t.Fatalf("asker's call log %+v", calls)
	}
	ints, _ := s2.Interrupts(bg, "helper", false)
	if len(ints) != 1 || ints[0].From != "task:asker" || ints[0].Key != calls[0].Key || !ints[0].Applied {
		t.Fatalf("helper's interrupts %+v (call key %s)", ints, calls[0].Key)
	}
	if helper.LastAnswer() != "heard: [message from task:asker] ping" {
		t.Fatalf("helper answered %q", helper.LastAnswer())
	}
	if got := kindsData(t, m2, "helper", proto.EvSteerDelivered, proto.EvSteerDropped); len(got) != 1 || got[0] != proto.EvSteerDelivered+` {"from":"task:asker","seq":1}` {
		t.Fatalf("helper's steer events %q", got)
	}
}
