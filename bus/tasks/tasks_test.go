package tasks_test

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

// world is a machine whose tasks are scripted by their input: "hold ..."
// waits in gate_hold until release; "send <id> <text>" sends text to task
// <id> with tasks_send and answers with the result; anything else answers at
// once.
type world struct {
	m       *neumann.Machine
	dev     *tasks.Device
	store   *mem.Memory
	entered chan string // the task ids that reached gate_hold
	release func()
	calls   atomic.Int32 // numbers the calls made with call
}

func newWorld(t *testing.T, cfg neumann.Config, opts ...tasks.Option) *world {
	t.Helper()
	w := &world{dev: tasks.New("tasks", opts...), store: mem.NewMemory(), entered: make(chan string, 16)}
	gate := make(chan struct{})
	var once sync.Once
	w.release = func() { once.Do(func() { close(gate) }) }
	hold := builtin.New("gate").Add(bus.ToolDef{Name: "hold", Effect: bus.ReadOnly}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		w.entered <- c.Task
		select {
		case <-gate:
			return "held", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	if len(cfg.Groups) == 0 {
		cfg.Model = &sim.Model{Fn: script}
	}
	cfg.Store = w.store
	cfg.Devices = append(cfg.Devices, w.dev, hold)
	cfg.SteerFormat = func(s string) string { return s }
	cfg.Logf = t.Logf
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.dev.Bind(m)
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
	t.Cleanup(w.release) // before the stop: it waits for the held calls
	w.m = m
	return w
}

func script(req model.Request) (model.Response, error) {
	in := sim.FirstUser(req)
	switch {
	case strings.HasPrefix(in, "hold") && sim.ToolRounds(req) == 0:
		return sim.CallTools("gate_hold"), nil
	case strings.HasPrefix(in, "send ") && sim.ToolRounds(req) == 0:
		f := strings.SplitN(in, " ", 3)
		args, _ := json.Marshal(map[string]string{"task_id": f[1], "text": f[2]})
		return model.Response{Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
			{Kind: task.PartToolCall, ProviderCallID: "p-0", Name: "tasks_send", Args: args},
		}}, FinishReason: "tool_calls"}, nil
	case sim.ToolRounds(req) > 0:
		last := req.Messages[len(req.Messages)-1]
		for _, p := range last.Parts {
			if p.Kind == task.PartToolResult {
				return sim.Reply(p.Text), nil
			}
		}
	}
	return sim.Reply("done"), nil
}

func (w *world) submit(t *testing.T, id, input string, in ...proto.Instruction) {
	t.Helper()
	var i proto.Instruction
	if len(in) > 0 {
		i = in[0]
	}
	i.Kind, i.TaskID, i.Input = proto.KindSubmit, id, input
	if _, err := w.m.Submit(bg, i); err != nil {
		t.Fatal(err)
	}
}

// held waits for n tasks to reach gate_hold.
func (w *world) held(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-w.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("a task never reached gate_hold")
		}
	}
}

func (w *world) done(t *testing.T, id string) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	for ev := range w.m.Events(ctx, 0) {
		if ev.TaskID == id && ev.Terminal() {
			tc, err := w.m.Load(bg, id)
			if err != nil {
				t.Fatal(err)
			}
			return tc
		}
	}
	t.Fatalf("task %s did not finish", id)
	return nil
}

// call runs one of the device's tools as task caller would, as a new call
// each time: key caller/c-<1000+n>/1.
func (w *world) call(t *testing.T, caller, tool string, args any) task.ToolResult {
	t.Helper()
	b, _ := json.Marshal(args)
	res, err := w.dev.Call(bg, bus.DeviceCall{Tool: tool, Args: b, Key: fmt.Sprintf("%s/c-%d/1", caller, 1000+w.calls.Add(1)), Task: caller})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	return res
}

func send(id, text string) map[string]string { return map[string]string{"task_id": id, "text": text} }

func TestSendAndList(t *testing.T) {
	w := newWorld(t, neumann.Config{}, tasks.WithMaxText(40))
	long := "hold " + strings.Repeat("a very long request that goes on ", 5)
	w.submit(t, "c", "hold c")
	w.submit(t, "u1", "hold u1\n\tthen   more")
	w.submit(t, "u2", long)
	w.submit(t, "f", "quick")
	w.done(t, "f")
	w.held(t, 3)

	res := w.call(t, "c", "list", map[string]any{})
	var list []struct{ ID, Class, State, Wait, Title string }
	if err := json.Unmarshal([]byte(res.Content), &list); err != nil || res.IsError {
		t.Fatalf("list %+v: %v", res, err)
	}
	if len(list) != 2 || list[0].ID != "u1" || list[1].ID != "u2" {
		t.Fatalf("list %+v", list)
	}
	if e := list[0]; e.Class != neumann.DefaultClass || e.State != "waiting" || e.Wait != "io" || e.Title != "hold u1 then more" {
		t.Fatalf("u1 listed as %+v", e)
	}
	if title := []rune(list[1].Title); len(title) != neumann.TitleLen || title[len(title)-1] != '…' || !strings.HasPrefix(long, string(title[:len(title)-1])) {
		t.Fatalf("u2's title %q", list[1].Title)
	}

	if res := w.call(t, "c", "send", send("u1", "hello")); res.IsError || res.Content != "sent to task u1" {
		t.Fatalf("send: %+v", res)
	}
	for _, c := range []struct {
		args any
		want string
	}{
		{send("f", "x"), "task f has finished"},
		{send("nope", "x"), "no task nope"},
		{send("c", "x"), "a task cannot send to itself"},
		{send("u1", " \n "), "text is empty"},
		{send("u1", strings.Repeat("x", 41)), "more than the limit of 40"},
		{map[string]string{"text": "x"}, "task_id is required"},
		{"not an object", "bad arguments"},
	} {
		res := w.call(t, "c", "send", c.args)
		if !res.IsError || !strings.Contains(res.Content, c.want) {
			t.Fatalf("send %v: %+v, want an error with %q", c.args, res, c.want)
		}
	}

	w.release()
	u1 := w.done(t, "u1")
	n := 0
	for _, msg := range u1.Regs {
		if msg.Role == task.RoleUser && msg.TextContent() == neumann.DefaultMessageFormat("task:c", "hello") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("u1's registers: %+v", u1.Regs)
	}
	pending, _ := w.store.Interrupts(bg, "u2", false)
	if len(pending) != 0 {
		t.Fatalf("a refused send reached u2: %+v", pending)
	}
	var delivered []string
	evs, _ := w.m.EventLog(bg, 0, 0)
	for _, ev := range evs {
		if ev.Kind == proto.EvSteerDelivered {
			delivered = append(delivered, ev.TaskID+" "+string(ev.Data))
		}
	}
	if len(delivered) != 1 || delivered[0] != `u1 {"from":"task:c","seq":1}` {
		t.Fatalf("steer_delivered events %q", delivered)
	}
}

// A call sent again under the same key does not send again.
func TestSendIsIdempotentByKey(t *testing.T) {
	w := newWorld(t, neumann.Config{})
	w.submit(t, "c", "hold c")
	w.submit(t, "u", "hold u")
	w.held(t, 2)
	b, _ := json.Marshal(send("u", "once"))
	for range 3 {
		res, err := w.dev.Call(bg, bus.DeviceCall{Tool: "send", Args: b, Key: "c/c-7/1", Task: "c"})
		if err != nil || res.IsError || res.Content != "sent to task u" {
			t.Fatalf("send: %+v, %v", res, err)
		}
	}
	if got, _ := w.store.Interrupts(bg, "u", false); len(got) != 1 || got[0].Key != "c/c-7/1" || got[0].From != "task:c" {
		t.Fatalf("interrupts %+v", got)
	}
}

func TestVisibility(t *testing.T) {
	tenant := func(id string) string { return id[:strings.IndexByte(id, '-')] }
	w := newWorld(t, neumann.Config{}, tasks.WithVisible(func(caller, other neumann.TaskInfo) bool {
		return tenant(caller.ID) == tenant(other.ID)
	}))
	w.submit(t, "a-1", "hold")
	w.submit(t, "a-2", "hold")
	w.submit(t, "b-1", "hold")
	w.held(t, 3)
	res := w.call(t, "a-1", "list", map[string]any{})
	if res.IsError || !strings.Contains(res.Content, `"id":"a-2"`) || strings.Contains(res.Content, "b-1") || strings.Contains(res.Content, `"id":"a-1"`) {
		t.Fatalf("a-1 lists %s", res.Content)
	}
	// A task one may not see is unknown, whether it exists or not.
	if res := w.call(t, "a-1", "send", send("b-1", "psst")); !res.IsError || res.Content != "no task b-1" {
		t.Fatalf("send across tenants: %+v", res)
	}
	if res := w.call(t, "a-1", "send", send("a-2", "hi")); res.IsError {
		t.Fatalf("send within a tenant: %+v", res)
	}
}

func TestUnboundOrWithoutACaller(t *testing.T) {
	dev := tasks.New("tasks")
	res, err := dev.Call(bg, bus.DeviceCall{Tool: "list", Key: "a/c-1/1", Task: "a"})
	if err != nil || !res.IsError || !strings.Contains(res.Content, "not bound") {
		t.Fatalf("unbound: %+v, %v", res, err)
	}
	w := newWorld(t, neumann.Config{})
	res, err = w.dev.Call(bg, bus.DeviceCall{Tool: "list"})
	if err != nil || !res.IsError || !strings.Contains(res.Content, "names no calling task") {
		t.Fatalf("no caller: %+v, %v", res, err)
	}
	if _, err := w.dev.Call(bg, bus.DeviceCall{Tool: "nope", Task: "a"}); err == nil {
		t.Fatal("an unknown tool was not an error")
	}
}

// The page table and the tool policy gate tasks_send as any other tool:
// a task without the tools never sees them, and a denied call sends nothing.
func TestPageTableAndPolicyGateSending(t *testing.T) {
	policy := func(_ context.Context, info neumann.ToolCallInfo) neumann.Decision {
		if info.Tool == "tasks_send" && info.TaskID == "watched" {
			return neumann.Deny("no messages from this task")
		}
		return neumann.Allow()
	}
	w := newWorld(t, neumann.Config{ToolPolicy: policy})
	w.submit(t, "target", "hold")
	w.held(t, 1)

	w.submit(t, "walled", "send target hi", proto.Instruction{Deny: []string{"tasks_"}})
	tc := w.done(t, "walled")
	for _, s := range tc.Tools {
		if strings.HasPrefix(s.Name, "tasks_") {
			t.Fatalf("a task denied tasks_ was offered %s", s.Name)
		}
	}
	if a := tc.LastAnswer(); !strings.Contains(a, "not permitted") {
		t.Fatalf("walled got %q", a)
	}

	w.submit(t, "watched", "send target hi")
	if a := w.done(t, "watched").LastAnswer(); !strings.Contains(a, "no messages from this task") {
		t.Fatalf("watched got %q", a)
	}

	w.submit(t, "free", "send target hi")
	if a := w.done(t, "free").LastAnswer(); a != "sent to task target" {
		t.Fatalf("free got %q", a)
	}
	got, _ := w.store.Interrupts(bg, "target", false)
	if len(got) != 1 || got[0].From != "task:free" {
		t.Fatalf("target's interrupts %+v", got)
	}
}

// Tasks see the tasks of their own class by default; All widens that.
func TestVisibleDefaultsToTheSameClass(t *testing.T) {
	groups := func() []neumann.GroupConfig {
		return []neumann.GroupConfig{{Class: "red", Model: &sim.Model{Fn: script}}, {Class: "blue", Model: &sim.Model{Fn: script}}}
	}
	for _, c := range []struct {
		name string
		opts []tasks.Option
		sees bool
	}{{"default", nil, false}, {"all", []tasks.Option{tasks.WithVisible(tasks.All)}, true}} {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t, neumann.Config{Groups: groups()}, c.opts...)
			w.submit(t, "r-1", "hold", proto.Instruction{Class: "red"})
			w.submit(t, "r-2", "hold", proto.Instruction{Class: "red"})
			w.submit(t, "b-1", "hold", proto.Instruction{Class: "blue"})
			w.held(t, 3)
			res := w.call(t, "r-1", "list", map[string]any{})
			if !strings.Contains(res.Content, `"id":"r-2"`) || strings.Contains(res.Content, `"id":"b-1"`) != c.sees {
				t.Fatalf("r-1 lists %s", res.Content)
			}
			res = w.call(t, "r-1", "send", send("b-1", "hi"))
			if res.IsError == c.sees || (!c.sees && res.Content != "no task b-1") {
				t.Fatalf("send across classes: %+v", res)
			}
		})
	}
}

// A call sent again under a key that is already committed is answered as
// the first time, before anything that changed since is looked at: the
// receiver has finished, a Visible hides finished tasks, a lower limit.
func TestARepeatIsAnsweredBeforeItIsChecked(t *testing.T) {
	w := newWorld(t, neumann.Config{})
	w.submit(t, "c", "hold c")
	w.submit(t, "u", "hold u")
	w.held(t, 2)
	b, _ := json.Marshal(send("u", "a longer message"))
	if res, err := w.dev.Call(bg, bus.DeviceCall{Tool: "send", Args: b, Key: "c/c-99/1", Task: "c"}); err != nil || res.IsError {
		t.Fatalf("send: %+v, %v", res, err)
	}
	w.release()
	w.done(t, "u")
	strict := tasks.New("tasks", tasks.WithMaxText(4), tasks.WithMaxSends(-1),
		tasks.WithVisible(func(_, other neumann.TaskInfo) bool { return !other.State.Terminal() }))
	strict.Bind(w.m)
	res, err := strict.Call(bg, bus.DeviceCall{Tool: "send", Args: b, Key: "c/c-99/1", Task: "c"})
	if err != nil || res.IsError || res.Content != "sent to task u" {
		t.Fatalf("the repeat: %+v, %v", res, err)
	}
	res, _ = strict.Call(bg, bus.DeviceCall{Tool: "send", Args: b, Key: "c/c-100/1", Task: "c"})
	if !res.IsError {
		t.Fatalf("a new call past the new limit: %+v", res)
	}
}

// Two tasks that answer every message stop at their quota of sends: the
// send past it is an error result, and the model stops.
func TestSendsAreBoundedPerTask(t *testing.T) {
	dev := tasks.New("tasks", tasks.WithMaxSends(3))
	m, err := neumann.New(neumann.Config{Devices: []bus.Device{dev}, Logf: t.Logf, Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		who := sim.FirstUser(req)
		other := map[string]string{"a": "b", "b": "a"}[who]
		last := req.Messages[len(req.Messages)-1]
		for _, p := range last.Parts {
			if p.Kind == task.PartToolResult && p.IsError {
				return sim.Reply("stopped: " + p.Text), nil
			}
		}
		args, _ := json.Marshal(send(other, "anything else?"))
		return model.Response{Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
			{Kind: task.PartToolCall, ProviderCallID: "p", Name: "tasks_send", Args: args},
		}}, FinishReason: "tool_calls"}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	dev.Bind(m)
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		m.Stop(ctx)
	}()
	w := &world{m: m}
	for _, id := range []string{"a", "b"} {
		if _, err := m.Submit(bg, proto.Instruction{TaskID: id, Input: id}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"a", "b"} {
		w.done(t, id)
		calls, _ := m.Calls(bg, id)
		ok := 0
		for _, c := range calls {
			if c.Result != nil && !c.Result.IsError {
				ok++
			}
		}
		if ok > 3 {
			t.Fatalf("%s sent %d messages past a quota of 3", id, ok)
		}
	}
	if a, _ := m.Load(bg, "a"); !strings.Contains(a.LastAnswer(), "the limit") && !strings.Contains(a.LastAnswer(), "finished") {
		t.Fatalf("a stopped with %q", a.LastAnswer())
	}
}
