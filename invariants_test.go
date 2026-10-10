package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
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

// Phase-1 invariants from docs/design.md §18. Run with -race.

// lagDev's "lag" tool sleeps (3-n)*15ms for args {"n":n}, so the calls of one
// batch complete in reverse of call order.
func lagDev() bus.Device {
	return builtin.New("dev").Add(bus.ToolDef{Name: "lag"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		var a struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(c.Args, &a); err != nil {
			return "", err
		}
		time.Sleep(time.Duration(3-a.N) * 15 * time.Millisecond)
		return string(c.Args), nil
	})
}

func TestEveryCallGetsExactlyOneResultInOrder(t *testing.T) { // invariant 1
	m, _ := start(t, neumann.Config{Devices: []bus.Device{lagDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_lag", "dev_lag", "dev_lag"), nil
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
	recs, err := m.Calls(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("%d call records, want 3: %+v", len(recs), recs)
	}
	for _, r := range recs {
		if r.Status != mem.CallDone || r.Attempt != 1 {
			t.Fatalf("record %+v", r)
		}
	}
}

func TestOneInstructionPerTaskAtATime(t *testing.T) { // invariant 2
	var mu sync.Mutex
	inflight := map[string]bool{}
	perTask := map[string]int{}
	m, _ := start(t, neumann.Config{Cores: 4, Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		name := sim.FirstUser(req)
		mu.Lock()
		if inflight[name] {
			t.Errorf("two instructions of %s at once", name)
		}
		inflight[name] = true
		perTask[name]++
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
	mu.Lock()
	defer mu.Unlock()
	if len(perTask) != 20 {
		t.Fatalf("%d tasks reached the model, want 20", len(perTask))
	}
	for name, n := range perTask {
		if n != 3 {
			t.Errorf("%s: %d model calls, want 3", name, n)
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
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow tool never started")
	}
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

func hasUserText(tc *task.TCB, text string) bool {
	for _, msg := range tc.Regs {
		if msg.Role == task.RoleUser && msg.TextContent() == text {
			return true
		}
	}
	return false
}

func droppedEvents(t *testing.T, m *neumann.Machine, id string) []proto.Event {
	t.Helper()
	var out []proto.Event
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvSteerDropped {
			out = append(out, ev)
		}
	}
	return out
}

func TestSteerDuringFinalAnswerIsDelivered(t *testing.T) { // invariant 5, deterministic
	entered := make(chan struct{}, 1)
	gate, release := newGate()
	defer release()
	var calls atomic.Int32
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-gate
			return sim.Reply("first"), nil
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
	if len(got.Regs) != 4 {
		t.Fatalf("regs %+v", got.Regs)
	}
	want := []struct {
		role task.Role
		text string
	}{{task.RoleUser, "A"}, {task.RoleAssistant, "first"}, {task.RoleUser, "late steer"}, {task.RoleAssistant, "second"}}
	for i, w := range want {
		if got.Regs[i].Role != w.role || got.Regs[i].TextContent() != w.text {
			t.Fatalf("reg %d = %s %q, want %s %q", i, got.Regs[i].Role, got.Regs[i].TextContent(), w.role, w.text)
		}
	}
	if got.LastAnswer() != "second" {
		t.Fatalf("last answer %q", got.LastAnswer())
	}
	if d := droppedEvents(t, m, id); len(d) != 0 {
		t.Fatalf("unexpected steer_dropped: %+v", d)
	}
}

func TestSteerLeftWhenBudgetEndsIsReportedDropped(t *testing.T) { // invariant 5, deterministic
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_slow"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A", Budget: &task.Budget{MaxSteps: 1}})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow tool never started")
	}
	if err := m.Steer(bg, id, "too late"); err != nil {
		t.Fatalf("steer: %v", err)
	}
	release()
	got := waitDone(t, m, id)
	if got.State != task.Done || got.StopReason != "budget" {
		t.Fatalf("ended %s: %s", got.State, got.StopReason)
	}
	if hasUserText(got, "too late") {
		t.Fatalf("steer was applied: %+v", got.Regs)
	}
	d := droppedEvents(t, m, id)
	if len(d) != 1 || !strings.Contains(string(d[0].Data), "too late") {
		t.Fatalf("steer_dropped events: %+v", d)
	}
}

// The randomized soak: a steer racing the end of a run is delivered, reported
// dropped, or arrives after the task finished -- never silently lost.
func TestSteerIsDeliveredOrReportedDropped(t *testing.T) { // invariant 5, soak
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	var rmu sync.Mutex
	jitter := func() time.Duration {
		rmu.Lock()
		defer rmu.Unlock()
		return time.Duration(rng.IntN(2000)) * time.Microsecond
	}
	var smu sync.Mutex
	seen := map[string][]string{} // task name -> every user text a model call received
	m, _ := start(t, neumann.Config{Cores: 2}, func(req model.Request) (model.Response, error) {
		smu.Lock()
		seen[sim.FirstUser(req)] = append(seen[sim.FirstUser(req)], sim.UserTexts(req)...)
		smu.Unlock()
		time.Sleep(jitter())
		return sim.Reply("ok"), nil
	})
	var delivered, dropped, finished int
	for i := range 40 {
		steer := fmt.Sprintf("steer-%d", i)
		id := submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)})
		time.Sleep(jitter())
		err := m.Steer(bg, id, steer)
		waitDone(t, m, id)
		if err != nil {
			if !errors.Is(err, neumann.ErrFinished) {
				t.Fatalf("steer: %v", err)
			}
			finished++
			continue
		}
		smu.Lock()
		isDelivered := slices.Contains(seen[fmt.Sprintf("task-%d", i)], steer)
		smu.Unlock()
		isDropped := len(droppedEvents(t, m, id)) > 0
		if isDelivered == isDropped {
			t.Errorf("%s: delivered=%v dropped=%v, want exactly one", steer, isDelivered, isDropped)
		}
		if isDelivered {
			delivered++
		} else {
			dropped++
		}
	}
	t.Logf("outcomes: delivered=%d dropped=%d finished-before-steer=%d", delivered, dropped, finished)
}

func TestModelCallsEqualModelInstructions(t *testing.T) { // invariant 6
	m, sm := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_echo", "dev_echo"), nil
		}
		return sim.Reply("ok"), nil
	})
	const tasks = 10
	var ids []string
	for i := range tasks {
		ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)}))
	}
	for _, id := range ids {
		if got := waitDone(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s: %s", id, got.State, got.StopReason)
		}
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
	if steps != sm.Calls() || steps != tasks*3 {
		t.Fatalf("model steps %d, model calls %d, want %d and %d", steps, sm.Calls(), tasks*3, tasks*3)
	}
}

func TestDeniedToolIsHiddenAndRefused(t *testing.T) {
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo"), nil // the model names it anyway
		}
		return sim.Reply("ok"), nil
	})
	offered := func(tc *task.TCB) bool {
		for _, s := range tc.Tools {
			if s.Name == "dev_echo" {
				return true
			}
		}
		return false
	}
	control := waitDone(t, m, submit(t, m, proto.Instruction{Input: "control"}))
	if !offered(control) {
		t.Fatalf("control task was not offered dev_echo: %+v", control.Tools)
	}
	got := waitDone(t, m, submit(t, m, proto.Instruction{Input: "q", Deny: []string{"dev_echo"}}))
	if offered(got) {
		t.Fatal("denied tool was offered")
	}
	if len(got.Regs) < 3 || len(got.Regs[2].Parts) < 1 {
		t.Fatalf("regs %+v", got.Regs)
	}
	res := got.Regs[2].Parts[0]
	if !res.IsError || !strings.Contains(res.Text, "not permitted") {
		t.Fatalf("result %+v", res)
	}
}
