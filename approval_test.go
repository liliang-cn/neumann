package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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

// recDev is a device whose tools a, b, c and d answer "<tool>:<args>" and
// note every call they get.
type recDev struct {
	mu  sync.Mutex
	got []string
}

func (d *recDev) device() bus.Device {
	dev := builtin.New("dev")
	for _, name := range []string{"a", "b", "c", "d"} {
		dev.Add(bus.ToolDef{Name: name}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			d.mu.Lock()
			d.got = append(d.got, name+" "+string(c.Args))
			d.mu.Unlock()
			return name + ":" + string(c.Args), nil
		})
	}
	return dev
}

func (d *recDev) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := slices.Clone(d.got)
	slices.Sort(out)
	return out
}

// oneRound asks for the named tools once, then answers "done".
func oneRound(names ...string) modelFn {
	return func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools(names...), nil
		}
		return sim.Reply("done"), nil
	}
}

// policy decides by tool name (anything unnamed is allowed) and notes every
// call it is asked about.
type policy struct {
	mu     sync.Mutex
	by     map[string]neumann.Decision
	asked  []neumann.ToolCallInfo
	panics string // a tool whose decision panics
}

func (p *policy) decide(_ context.Context, info neumann.ToolCallInfo) neumann.Decision {
	p.mu.Lock()
	p.asked = append(p.asked, info)
	p.mu.Unlock()
	if info.Tool == p.panics {
		panic("boom")
	}
	return p.by[info.Tool]
}

func (p *policy) askedAbout() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, in := range p.asked {
		out = append(out, in.TaskID+"/"+in.CallID)
	}
	return out
}

func recordsOf(t *testing.T, m *neumann.Machine, id string) map[string]mem.CallRecord {
	t.Helper()
	recs, err := m.Calls(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]mem.CallRecord{}
	for _, r := range recs {
		out[r.CallID] = r
	}
	return out
}

// dataOf is the data of id's events of kind, by call id.
func dataOf(t *testing.T, m *neumann.Machine, id, kind string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind != kind {
			continue
		}
		var d map[string]any
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		out[fmt.Sprint(d["call_id"])] = d
	}
	return out
}

// toolMessage is the task's first tool message, checked against the
// assistant turn before it: one result per call, in call order.
func toolMessage(t *testing.T, tc *task.TCB) []task.Part {
	t.Helper()
	for i, msg := range tc.Regs {
		if msg.Role != task.RoleTool {
			continue
		}
		var want []string
		for _, p := range tc.Regs[i-1].ToolCalls() {
			want = append(want, p.CallID)
		}
		var got []string
		for _, p := range msg.Parts {
			got = append(got, p.CallID)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("the tool message answers %v, the calls were %v", got, want)
		}
		return msg.Parts
	}
	t.Fatalf("no tool message in %+v", tc.Regs)
	return nil
}

// waitApproval waits until id is waiting for approval with n approval_needed
// events, and returns their data by call id.
func waitApproval(t *testing.T, m *neumann.Machine, id string, n int) map[string]map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tc, err := m.Load(bg, id)
		if err != nil {
			t.Fatal(err)
		}
		if tc.State.Terminal() {
			t.Fatalf("task ended %s %q while it should wait for approval", tc.State, tc.StopReason)
		}
		got := dataOf(t, m, id, proto.EvApprovalNeeded)
		if tc.State == task.Waiting && tc.Wait == task.WaitApproval && len(got) >= n {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("task %s never waited for %d approvals", id, n)
	return nil
}

func TestToolPolicyDecidesEachCall(t *testing.T) {
	dev := &recDev{}
	p := &policy{by: map[string]neumann.Decision{
		"dev_b": neumann.Deny("b is not for you"),
		"dev_c": neumann.RewriteArgs([]byte(`{"n":99}`)),
		"dev_d": neumann.RewriteArgs([]byte(`{"n":`)),
	}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b", "dev_c", "dev_d"))
	id := submit(t, m, proto.Instruction{TaskID: "t1", Input: "go"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("task ended %s %q", got.State, got.LastAnswer())
	}
	if c := dev.calls(); !slices.Equal(c, []string{`a {"n":0}`, `c {"n":99}`}) {
		t.Fatalf("the device ran %v", c)
	}
	parts := toolMessage(t, got)
	if parts[0].IsError || parts[0].Text != `a:{"n":0}` || parts[2].IsError || parts[2].Text != `c:{"n":99}` {
		t.Fatalf("results %+v", parts)
	}
	if !parts[1].IsError || !strings.Contains(parts[1].Text, "b is not for you") {
		t.Fatalf("the denied call's result %+v", parts[1])
	}
	if !parts[3].IsError || !strings.Contains(parts[3].Text, "not a JSON object") {
		t.Fatalf("a rewrite to invalid JSON is denied: %+v", parts[3])
	}

	recs := recordsOf(t, m, id)
	if r := recs["c-1"]; r.Policy != mem.PolicyAllow || r.Status != mem.CallDone || r.Attempt != 1 {
		t.Fatalf("c-1 %+v", r)
	}
	if r := recs["c-2"]; r.Policy != mem.PolicyDeny || r.Status != mem.CallDone || r.Attempt != 0 || r.Key != "" ||
		r.Result == nil || !r.Result.IsError || r.Reason != "b is not for you" {
		t.Fatalf("c-2 %+v", r)
	}
	if r := recs["c-3"]; r.Policy != mem.PolicyRewrite || string(r.Args) != `{"n":99}` || string(r.OrigArgs) != `{"n":2}` {
		t.Fatalf("c-3 %+v", r)
	}
	if r := recs["c-4"]; r.Policy != mem.PolicyDeny || r.Attempt != 0 || string(r.Args) != `{"n":3}` {
		t.Fatalf("c-4 %+v", r)
	}

	denied := dataOf(t, m, id, proto.EvToolDenied)
	if d := denied["c-2"]; d == nil || d["reason"] != "b is not for you" || d["by"] != "policy" || d["tool"] != "dev_b" {
		t.Fatalf("tool_denied %v", denied)
	}
	if len(denied) != 2 || denied["c-4"] == nil {
		t.Fatalf("tool_denied %v", denied)
	}
	rw := dataOf(t, m, id, proto.EvToolRewritten)
	if d := rw["c-3"]; len(rw) != 1 || d["original"] != `{"n":2}` || d["args"] != `{"n":99}` {
		t.Fatalf("tool_rewritten %v", rw)
	}
	if res := dataOf(t, m, id, proto.EvToolResult); len(res) != 2 || res["c-1"] == nil || res["c-3"] == nil {
		t.Fatalf("tool_result events %v: a denied call has tool_denied instead", res)
	}

	// Asked once per call, with what it needs to know.
	if a := p.askedAbout(); !slices.Equal(a, []string{"t1/c-1", "t1/c-2", "t1/c-3", "t1/c-4"}) {
		t.Fatalf("the policy was asked about %v", a)
	}
	info := p.asked[0]
	want := neumann.ToolCallInfo{TaskID: "t1", CallID: "c-1", Tool: "dev_a", Device: "dev", Effect: bus.SideEffect,
		Args: json.RawMessage(`{"n":0}`), Class: neumann.DefaultClass, Attempt: 1}
	if fmt.Sprint(info) != fmt.Sprint(want) {
		t.Fatalf("info %+v, want %+v", info, want)
	}
}

func TestToolPolicyPanicDenies(t *testing.T) {
	dev := &recDev{}
	p := &policy{panics: "dev_a"}
	m, sm := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a"))
	got := waitDone(t, m, submit(t, m, proto.Instruction{Input: "go"}))
	if got.State != task.Done || len(dev.calls()) != 0 || sm.Calls() != 2 {
		t.Fatalf("task %s, device %v, model calls %d", got.State, dev.calls(), sm.Calls())
	}
	parts := toolMessage(t, got)
	if !parts[0].IsError || !strings.Contains(parts[0].Text, "panicked: boom") {
		t.Fatalf("result %+v", parts[0])
	}
	if d := dataOf(t, m, got.ID, proto.EvToolDenied)["c-1"]; d == nil || !strings.Contains(fmt.Sprint(d["reason"]), "boom") {
		t.Fatalf("tool_denied %v", d)
	}
}

// Every call of the round is held for an operator: the round waits until the
// last one is decided, then the allowed and edited ones run and the denied
// one reaches the model as an error, all in call order.
func TestApprovalAllowDenyEdit(t *testing.T) {
	dev := &recDev{}
	hold := neumann.RequireApproval("money moves")
	p := &policy{by: map[string]neumann.Decision{"dev_a": hold, "dev_b": hold, "dev_c": hold}}
	m, sm := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b", "dev_c"))
	id := submit(t, m, proto.Instruction{Input: "go"})
	need := waitApproval(t, m, id, 3)
	if d := need["c-2"]; d["tool"] != "dev_b" || d["device"] != "dev" || d["effect"] != "side_effect" || d["args"] != `{"n":1}` || d["reason"] != "money moves" {
		t.Fatalf("approval_needed %v", d)
	}
	// The event is committed before the vCore hands the task back.
	waitFor(t, func() bool { return m.Status().Approval == 1 })
	if st := m.Status(); st.Groups[0].Approval != 1 {
		t.Fatalf("status counts group %d awaiting approval", st.Groups[0].Approval)
	}
	if q := m.Queue(); q.Classes[0].Approval != 1 || q.Classes[0].Waiting != 1 {
		t.Fatalf("queue %+v", q)
	}
	for _, r := range recordsOf(t, m, id) {
		if r.Status != mem.CallPlanned || r.Approval != mem.ApprovalPending || r.Policy != mem.PolicyApproval {
			t.Fatalf("held call %+v", r)
		}
	}

	if err := m.Approve(bg, id, "c-1", neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Approve(bg, id, "c-2", neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "not today"}); err != nil {
		t.Fatal(err)
	}
	if err := m.Approve(bg, id, "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotAwaitingApproval) {
		t.Fatalf("approving a decided call: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if c := dev.calls(); len(c) != 0 || sm.Calls() != 1 {
		t.Fatalf("the round went on with a call still held: device %v, model calls %d", c, sm.Calls())
	}
	if tc, _ := m.Load(bg, id); tc.Wait != task.WaitApproval {
		t.Fatalf("task %s %s with a call still held", tc.State, tc.Wait)
	}
	if err := m.Approve(bg, id, "c-3", neumann.ApprovalDecision{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`{"n":7}`)}); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("task ended %s", got.State)
	}
	if c := dev.calls(); !slices.Equal(c, []string{`a {"n":0}`, `c {"n":7}`}) {
		t.Fatalf("the device ran %v", c)
	}
	parts := toolMessage(t, got)
	if parts[0].Text != `a:{"n":0}` || !parts[1].IsError || !strings.Contains(parts[1].Text, "not today") || parts[2].Text != `c:{"n":7}` {
		t.Fatalf("results %+v", parts)
	}
	recs := recordsOf(t, m, id)
	if r := recs["c-1"]; r.Approval != mem.ApprovalAllowed || r.Status != mem.CallDone || r.Attempt != 1 {
		t.Fatalf("c-1 %+v", r)
	}
	if r := recs["c-2"]; r.Approval != mem.ApprovalDenied || r.Status != mem.CallDone || r.Attempt != 0 || r.Reason != "not today" {
		t.Fatalf("c-2 %+v", r)
	}
	if r := recs["c-3"]; r.Approval != mem.ApprovalEdited || string(r.Args) != `{"n":7}` || string(r.OrigArgs) != `{"n":2}` {
		t.Fatalf("c-3 %+v", r)
	}
	approved := dataOf(t, m, id, proto.EvApproved)
	if len(approved) != 2 || approved["c-1"] == nil || approved["c-3"]["args"] != `{"n":7}` || approved["c-3"]["edited"] != true || approved["c-3"]["original"] != `{"n":2}` {
		t.Fatalf("approved %v", approved)
	}
	if d := dataOf(t, m, id, proto.EvToolDenied)["c-2"]; d["by"] != "operator" || d["reason"] != "not today" {
		t.Fatalf("tool_denied %v", d)
	}
	if st := m.Status(); st.Approval != 0 {
		t.Fatalf("status still counts %d awaiting approval", st.Approval)
	}
}

// A round with calls the policy allowed, one it holds and one it denies:
// nothing runs until the held one is approved, and then the tool message
// still answers every call once, in call order.
func TestMixedRoundKeepsOrderAndPairing(t *testing.T) {
	dev := &recDev{}
	p := &policy{by: map[string]neumann.Decision{
		"dev_b": neumann.RequireApproval("check"),
		"dev_c": neumann.Deny("never c"),
	}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b", "dev_c", "dev_d"))
	id := submit(t, m, proto.Instruction{Input: "go"})
	need := waitApproval(t, m, id, 1)
	if len(need) != 1 || need["c-2"] == nil {
		t.Fatalf("approval_needed %v", need)
	}
	recs := recordsOf(t, m, id)
	for _, c := range []string{"c-1", "c-4"} {
		if r := recs[c]; r.Status != mem.CallPlanned || r.Policy != mem.PolicyAllow || r.Approval != "" {
			t.Fatalf("an allowed call of a held round %+v", r)
		}
	}
	if r := recs["c-3"]; r.Status != mem.CallDone || r.Policy != mem.PolicyDeny {
		t.Fatalf("a denied call of a held round %+v", r)
	}
	time.Sleep(20 * time.Millisecond)
	if c := dev.calls(); len(c) != 0 {
		t.Fatalf("calls ran before the round was approved: %v", c)
	}
	if err := m.Approve(bg, id, "c-2", neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	if c := dev.calls(); !slices.Equal(c, []string{`a {"n":0}`, `b {"n":1}`, `d {"n":3}`}) {
		t.Fatalf("the device ran %v", c)
	}
	parts := toolMessage(t, got)
	for i, want := range []string{`a:{"n":0}`, `b:{"n":1}`, "", `d:{"n":3}`} {
		if want == "" {
			if !parts[i].IsError || !strings.Contains(parts[i].Text, "never c") {
				t.Fatalf("result %d %+v", i, parts[i])
			}
			continue
		}
		if parts[i].IsError || parts[i].Text != want {
			t.Fatalf("result %d %+v, want %s", i, parts[i], want)
		}
	}
	// Each call has exactly one result event.
	results := map[string]int{}
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvToolResult || ev.Kind == proto.EvToolDenied {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(ev.Data, &d)
			results[d.CallID]++
		}
	}
	if fmt.Sprint(results) != "map[c-1:1 c-2:1 c-3:1 c-4:1]" {
		t.Fatalf("result events per call %v", results)
	}
}

func TestApproveErrors(t *testing.T) {
	p := &policy{by: map[string]neumann.Decision{"dev_a": neumann.RequireApproval("")}}
	cfg := neumann.Config{Model: &sim.Model{Fn: oneRound("dev_a")}, Devices: []bus.Device{(&recDev{}).device()}, ToolPolicy: p.decide, Logf: t.Logf}
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Approve(bg, "t1", "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotStarted) {
		t.Fatalf("before start: %v", err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	if err := m.Approve(bg, "nope", "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
	done := submit(t, m, proto.Instruction{Input: "no tools", Deny: []string{"dev_"}})
	if err := m.Approve(bg, done, "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotAwaitingApproval) && !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("a task that is not waiting: %v", err)
	}
	waitDone(t, m, done)
	if err := m.Approve(bg, done, "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotAwaitingApproval) {
		t.Fatalf("a finished task: %v", err)
	}

	id := submit(t, m, proto.Instruction{Input: "go"})
	waitApproval(t, m, id, 1)
	if err := m.Approve(bg, id, "c-99", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotAwaitingApproval) {
		t.Fatalf("an unknown call: %v", err)
	}
	for _, d := range []neumann.ApprovalDecision{
		{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`{"n":`)},
		{Verdict: neumann.ApproveEdit},
		{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`null`)},
		{Verdict: neumann.ApproveEdit, Args: json.RawMessage(` [1]`)},
		{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`"x"`)},
		{Verdict: neumann.ApproveDeny, Args: json.RawMessage(`{}`)},
		{Verdict: 9},
	} {
		if err := m.Approve(bg, id, "c-1", d); err == nil || errors.Is(err, neumann.ErrNotAwaitingApproval) {
			t.Fatalf("decision %+v: %v", d, err)
		}
	}
	// None of that disturbed the held call.
	if r := recordsOf(t, m, id)["c-1"]; r.Approval != mem.ApprovalPending {
		t.Fatalf("c-1 %+v", r)
	}
	if err := m.Approve(bg, id, "c-1", neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	waitDone(t, m, id)
	stop(t, m)
	if err := m.Approve(bg, id, "c-1", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("after stop: %v", err)
	}
}

// A cancel while the round waits for approval sends nothing: the held call
// and the allowed one waiting with it are both recorded cancelled.
func TestCancelWhileAwaitingApproval(t *testing.T) {
	dev := &recDev{}
	p := &policy{by: map[string]neumann.Decision{"dev_b": neumann.RequireApproval("check")}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b"))
	id := submit(t, m, proto.Instruction{Input: "go"})
	waitApproval(t, m, id, 1)
	if err := m.Steer(bg, id, "hurry"); err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(bg, id); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	if got.State != task.Cancelled {
		t.Fatalf("task ended %s", got.State)
	}
	for c, r := range recordsOf(t, m, id) {
		if r.Status != mem.CallCancelled || r.Attempt != 0 {
			t.Fatalf("%s %+v", c, r)
		}
	}
	if err := m.Approve(bg, id, "c-2", neumann.ApprovalDecision{}); !errors.Is(err, neumann.ErrNotAwaitingApproval) {
		t.Fatalf("approving a cancelled task: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if c := dev.calls(); len(c) != 0 {
		t.Fatalf("the device ran %v", c)
	}
	if !slices.Contains(kindsOf(t, m, id), proto.EvSteerDropped) {
		t.Fatalf("the steer was not reported dropped: %v", kindsOf(t, m, id))
	}
	if st := m.Status(); st.Approval != 0 {
		t.Fatalf("status counts %d awaiting approval", st.Approval)
	}
}

func TestDeadlineWhileAwaitingApproval(t *testing.T) {
	dev := &recDev{}
	clk := sim.NewClock(clock0)
	p := &policy{by: map[string]neumann.Decision{"dev_a": neumann.RequireApproval("check")}}
	m := startWith(t, neumann.Config{Model: &sim.Model{Fn: oneRound("dev_a")}, Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide, Clock: clk})
	id := submit(t, m, proto.Instruction{Input: "go", Budget: inAnHour(clk)})
	waitApproval(t, m, id, 1)
	clk.Advance(2 * time.Hour)
	failedByDeadline(t, m, id)
	if r := recordsOf(t, m, id)["c-1"]; r.Status != mem.CallCancelled {
		t.Fatalf("c-1 %+v", r)
	}
	if c := dev.calls(); len(c) != 0 {
		t.Fatalf("the device ran %v", c)
	}
}

// A steer that arrives while the round waits is not lost and does not jump
// the round: it reaches the model after the round's tool message.
func TestSteerWaitsForTheApprovedRound(t *testing.T) {
	p := &policy{by: map[string]neumann.Decision{"dev_a": neumann.RequireApproval("check")}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{(&recDev{}).device()}, ToolPolicy: p.decide}, oneRound("dev_a"))
	id := submit(t, m, proto.Instruction{Input: "go"})
	waitApproval(t, m, id, 1)
	if err := m.Steer(bg, id, "also this"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if tc, _ := m.Load(bg, id); tc.Wait != task.WaitApproval || hasUserText(tc, "also this") {
		t.Fatalf("the steer moved the task on: %s %s", tc.State, tc.Wait)
	}
	if err := m.Approve(bg, id, "c-1", neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, id)
	var roles []string
	for _, msg := range got.Regs {
		roles = append(roles, string(msg.Role))
	}
	if strings.Join(roles, ",") != "user,assistant,tool,user,assistant" || got.Regs[3].TextContent() != "also this" {
		t.Fatalf("registers %v", roles)
	}
}

// A restart finds the task still waiting for approval: it announces the held
// calls again, never asks the policy again about calls it decided, and an
// approval given to the new machine finishes the task.
func TestApprovalSurvivesARestart(t *testing.T) {
	store := mem.NewMemory()
	dev := &recDev{}
	p1 := &policy{by: map[string]neumann.Decision{"dev_b": neumann.RequireApproval("check")}}
	cfg := neumann.Config{Model: &sim.Model{Fn: oneRound("dev_a", "dev_b")}, Store: store, Devices: []bus.Device{dev.device()}, ToolPolicy: p1.decide, Logf: t.Logf}
	m1, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	id := submit(t, m1, proto.Instruction{Input: "go"})
	waitApproval(t, m1, id, 1)
	// Nothing is in flight: Stop does not need its deadline.
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	if err := m1.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	last, _ := store.Events(bg, 0, 0)

	p2 := &policy{}
	cfg.ToolPolicy = p2.decide
	m2 := startWith(t, cfg)
	if tc, _ := m2.Load(bg, id); tc.State != task.Waiting || tc.Wait != task.WaitApproval {
		t.Fatalf("recovered as %s %s", tc.State, tc.Wait)
	}
	var again, recovered bool
	all, _ := store.Events(bg, last[len(last)-1].Seq, 0)
	for _, ev := range all {
		var d map[string]any
		json.Unmarshal(ev.Data, &d)
		switch {
		case ev.Kind == proto.EvApprovalNeeded && d["call_id"] == "c-2" && d["recovered"] == true && d["reason"] == "check":
			again = true
		case ev.Kind == proto.EvRecovered && d["from"] == "waiting_approval":
			recovered = true
		}
	}
	if !again || !recovered {
		t.Fatalf("the restart did not announce the held call again: %+v", all)
	}
	if err := m2.Approve(bg, id, "c-2", neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m2, id)
	if got.State != task.Done || !slices.Equal(dev.calls(), []string{`a {"n":0}`, `b {"n":1}`}) {
		t.Fatalf("task %s, device %v", got.State, dev.calls())
	}
	if a := p2.askedAbout(); len(a) != 0 {
		t.Fatalf("the restarted machine asked the policy again about %v", a)
	}
	toolMessage(t, got)
}

// An operator that answers approval_needed the moment it is published, and
// two operators deciding two calls of one round at once, are never told the
// task is not waiting: the commit that announces the calls comes before the
// vCore hands the task back, and Approve waits for that hand-back.
func TestApproveRightAfterTheAnnouncement(t *testing.T) {
	p := &policy{by: map[string]neumann.Decision{"dev_a": neumann.RequireApproval(""), "dev_b": neumann.RequireApproval("")}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{(&recDev{}).device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b"))
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	events := m.Events(ctx, 0)
	const tasks = 30
	ids := map[string]bool{}
	for i := 0; i < tasks; i++ {
		ids[submit(t, m, proto.Instruction{Input: fmt.Sprint("t", i)})] = true
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2*tasks)
	pending := map[string][]string{}
	done := 0
	for ev := range events {
		if !ids[ev.TaskID] {
			continue
		}
		if ev.Kind == proto.EvApprovalNeeded {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(ev.Data, &d)
			pending[ev.TaskID] = append(pending[ev.TaskID], d.CallID)
			if len(pending[ev.TaskID]) == 2 {
				// Both calls at once, from two operators.
				for _, call := range pending[ev.TaskID] {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if err := m.Approve(bg, ev.TaskID, call, neumann.ApprovalDecision{}); err != nil {
							errs <- fmt.Errorf("%s/%s: %w", ev.TaskID, call, err)
						}
					}()
				}
			}
		}
		if ev.Terminal() {
			if ev.Kind != proto.EvDone {
				t.Fatalf("task %s ended with %s", ev.TaskID, ev.Kind)
			}
			if done++; done == tasks {
				break
			}
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if done != tasks {
		t.Fatalf("%d of %d tasks finished", done, tasks)
	}
}

// Arguments are a JSON object: a rewrite to anything else (null, an array, a
// string) denies the call, as a rewrite to invalid JSON does.
func TestRewriteArgsMustBeAnObject(t *testing.T) {
	dev := &recDev{}
	p := &policy{by: map[string]neumann.Decision{
		"dev_a": neumann.RewriteArgs([]byte(`null`)),
		"dev_b": neumann.RewriteArgs([]byte(`[1]`)),
		"dev_c": neumann.RewriteArgs([]byte(` {"n":9} `)),
	}}
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev.device()}, ToolPolicy: p.decide}, oneRound("dev_a", "dev_b", "dev_c"))
	got := waitDone(t, m, submit(t, m, proto.Instruction{Input: "go"}))
	if c := dev.calls(); !slices.Equal(c, []string{`c  {"n":9} `}) {
		t.Fatalf("the device ran %v", c)
	}
	parts := toolMessage(t, got)
	for _, i := range []int{0, 1} {
		if !parts[i].IsError || !strings.Contains(parts[i].Text, "JSON object") {
			t.Fatalf("result %d %+v", i, parts[i])
		}
	}
}
