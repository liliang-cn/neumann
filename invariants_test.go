package neumann_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// Phase-1 invariants from docs/design.md §18. Run with -race.

func TestEveryCallGetsExactlyOneResultInOrder(t *testing.T) { // invariant 1
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo", "dev_echo", "dev_echo"), nil
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "q"})
	got := waitDone(t, m, id)
	if len(got.Regs) != 4 {
		t.Fatalf("regs %+v", got.Regs)
	}
	calls := got.Regs[1].ToolCalls()
	results := got.Regs[2].Parts
	if len(calls) != 3 || len(results) != 3 {
		t.Fatalf("%d calls, %d results", len(calls), len(results))
	}
	for i := range calls {
		if results[i].Kind != task.PartToolResult || results[i].CallID != calls[i].CallID ||
			results[i].Text != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("result %d = %+v for call %+v", i, results[i], calls[i])
		}
	}
	recs, _ := m.Calls(bg, id)
	for _, r := range recs {
		if r.Status != mem.CallDone || r.Attempt != 1 {
			t.Fatalf("record %+v", r)
		}
	}
}

func TestOneInstructionPerTaskAtATime(t *testing.T) { // invariant 2
	var mu sync.Mutex
	inflight := map[string]bool{}
	m, _ := start(t, neumann.Config{Cores: 4, Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		name := sim.FirstUser(req)
		mu.Lock()
		if inflight[name] {
			t.Errorf("two instructions of %s at once", name)
		}
		inflight[name] = true
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		inflight[name] = false
		mu.Unlock()
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_echo"), nil
		}
		return sim.Reply("done " + name), nil
	})
	var ids []string
	for i := range 20 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)}))
	}
	for _, id := range ids {
		if got := waitDone(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s: %s", id, got.State, got.StopReason)
		}
	}
}

func TestSlowToolDoesNotHoldTheCore(t *testing.T) { // invariant 3
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Cores: 1, Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "A" && sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		return sim.Reply(sim.FirstUser(req) + " done"), nil
	})
	a := submit(t, m, proto.Instruction{Input: "A"})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("A's slow tool never started")
	}
	b := submit(t, m, proto.Instruction{Input: "B"})
	if got := waitDone(t, m, b); got.LastAnswer() != "B done" {
		t.Fatalf("B: %+v", got)
	}
	if st := m.Status(); st.Waiting != 1 {
		t.Fatalf("A should be waiting on I/O; status %+v", st)
	}
	release()
	if got := waitDone(t, m, a); got.LastAnswer() != "A done" {
		t.Fatalf("A: %+v", got)
	}
}

func TestSteerWhileToolInFlight(t *testing.T) { // invariant 5, delivered after the tool round
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	var mu sync.Mutex
	var saw []string
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		mu.Lock()
		saw = sim.UserTexts(req)
		mu.Unlock()
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A"})
	<-entered
	if err := m.Steer(bg, id, "also check B"); err != nil {
		t.Fatal(err)
	}
	release()
	got := waitDone(t, m, id)
	roles := []task.Role{task.RoleUser, task.RoleAssistant, task.RoleTool, task.RoleUser, task.RoleAssistant}
	if len(got.Regs) != len(roles) {
		t.Fatalf("regs %+v", got.Regs)
	}
	for i, r := range roles {
		if got.Regs[i].Role != r {
			t.Fatalf("reg %d is %s, want %s", i, got.Regs[i].Role, r)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(saw, "|") != "A|also check B" {
		t.Fatalf("model saw %v", saw)
	}
}

func TestSteerIsDeliveredOrReportedDropped(t *testing.T) { // invariant 5, racing the final answer
	m, _ := start(t, neumann.Config{Cores: 2}, func(model.Request) (model.Response, error) {
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		return sim.Reply("ok"), nil
	})
	for i := range 40 {
		steer := fmt.Sprintf("steer-%d", i)
		id := submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)})
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		err := m.Steer(bg, id, steer)
		got := waitDone(t, m, id)
		if err != nil {
			if !errors.Is(err, neumann.ErrFinished) {
				t.Fatalf("steer: %v", err)
			}
			continue
		}
		delivered := false
		for _, msg := range got.Regs {
			if msg.Role == task.RoleUser && msg.TextContent() == steer {
				delivered = true
			}
		}
		dropped := false
		for _, ev := range eventsOf(t, m, id) {
			if ev.Kind == proto.EvSteerDropped {
				dropped = true
			}
		}
		if delivered == dropped {
			t.Errorf("%s: delivered=%v dropped=%v, want exactly one", steer, delivered, dropped)
		}
	}
}

func TestModelCallsEqualModelInstructions(t *testing.T) { // invariant 6
	m, sm := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_echo", "dev_echo"), nil
		}
		return sim.Reply("ok"), nil
	})
	var ids []string
	for i := range 10 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)}))
	}
	for _, id := range ids {
		waitDone(t, m, id)
	}
	all, _ := m.EventLog(bg, 0, 0)
	steps := 0
	for _, ev := range all {
		if ev.Kind != proto.EvStep {
			continue
		}
		var d struct {
			PC string `json:"pc"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.PC == task.StepModel.String() {
			steps++
		}
	}
	if steps != sm.Calls() || steps != 30 {
		t.Fatalf("model steps %d, model calls %d, want 30 and 30", steps, sm.Calls())
	}
}

func TestDeniedToolIsHiddenAndRefused(t *testing.T) {
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo"), nil // the model names it anyway
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "q", Deny: []string{"dev_echo"}})
	got := waitDone(t, m, id)
	for _, s := range got.Tools {
		if s.Name == "dev_echo" {
			t.Fatal("denied tool was offered")
		}
	}
	res := got.Regs[2].Parts[0]
	if !res.IsError || !strings.Contains(res.Text, "not permitted") {
		t.Fatalf("result %+v", res)
	}
}
