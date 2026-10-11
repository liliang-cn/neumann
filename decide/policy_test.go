package decide_test

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
	"github.com/liliang-cn/neumann/decide"
	"github.com/liliang-cn/neumann/decide/decidetest"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// risky answers needs_approval by tool: yes for notes_delete, no otherwise.
func risky(state string, _ map[string]decide.Question) (map[string]decide.Answer, error) {
	p := 0.02
	if strings.Contains(state, "tool: notes_delete\n") {
		p = 0.97
	}
	return map[string]decide.Answer{decide.NeedsApproval: decidetest.Yes(p)}, nil
}

func call(tool string, effect task.Effect) neumann.ToolCallInfo {
	return neumann.ToolCallInfo{
		TaskID: "t1", CallID: "c-1", Tool: tool, Device: "notes", Effect: effect,
		Args: json.RawMessage(`{ "id" : "n-42" }`), Class: "default", Attempt: 1,
	}
}

// logs collects what a policy logs.
type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) logf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *logs) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.lines)
}

func TestPolicyReadOnlyIsNotAsked(t *testing.T) {
	e := &decidetest.Engine{Fn: risky}
	p := decide.Policy(e, decide.PolicyConfig{})
	if d := p(bg, call("notes_delete", bus.ReadOnly)); d.Verdict != neumann.VerdictAllow {
		t.Fatalf("%+v", d)
	}
	if n := len(e.Asked()); n != 0 {
		t.Fatalf("the engine was asked %d times about a read-only call", n)
	}
}

func TestPolicyHoldsAYes(t *testing.T) {
	e := &decidetest.Engine{Fn: risky}
	p := decide.Policy(e, decide.PolicyConfig{})
	d := p(bg, call("notes_delete", bus.SideEffect))
	if d.Verdict != neumann.VerdictRequireApproval || d.Reason != "decidetest: needs_approval yes, p=0.97" {
		t.Fatalf("%+v", d)
	}
	if d := p(bg, call("notes_save", bus.SideEffect)); d.Verdict != neumann.VerdictAllow {
		t.Fatalf("a no: %+v", d)
	}
	if d := p(bg, call("notes_delete", bus.Idempotent)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("an idempotent call is asked about too: %+v", d)
	}
	asked := e.Asked()
	if len(asked) != 3 {
		t.Fatalf("asked %d times", len(asked))
	}
	q, ok := asked[0].Questions[decide.NeedsApproval]
	if !ok || len(asked[0].Questions) != 1 || q.Instructions != decide.DefaultApprovalQuestion().Instructions {
		t.Fatalf("questions %+v", asked[0].Questions)
	}
	want := "tool: notes_delete\ndevice: notes\ndeclared effect: side_effect\ntask class: default\narguments: {\"id\":\"n-42\"}"
	if asked[0].State != want {
		t.Fatalf("state %q", asked[0].State)
	}
}

func TestPolicyThreshold(t *testing.T) {
	e := &decidetest.Engine{Answers: map[string]decide.Answer{decide.NeedsApproval: decidetest.Yes(0.6)}} // confidence 0.2
	if d := decide.Policy(e, decide.PolicyConfig{})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("threshold 0 holds any yes: %+v", d)
	}
	if d := decide.Policy(e, decide.PolicyConfig{Threshold: 0.5})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictAllow {
		t.Fatalf("a yes under the threshold: %+v", d)
	}
	if d := decide.Policy(e, decide.PolicyConfig{Threshold: 0.19})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("a yes over the threshold: %+v", d)
	}
}

func TestPolicyFallsBackOnError(t *testing.T) {
	var l logs
	e := &decidetest.Engine{Fn: func(string, map[string]decide.Question) (map[string]decide.Answer, error) {
		return nil, errors.New("decide: decidetest: 500 Internal Server Error: boom")
	}}
	p := decide.Policy(e, decide.PolicyConfig{Logf: l.logf})
	d := p(bg, call("notes_delete", bus.SideEffect))
	if d.Verdict != neumann.VerdictRequireApproval || !strings.HasPrefix(d.Reason, "decidetest: no answer (") ||
		!strings.Contains(d.Reason, "boom") || !strings.HasSuffix(d.Reason, "held because the call has side effects") {
		t.Fatalf("side effect: %+v", d)
	}
	if d := p(bg, call("notes_touch", bus.Idempotent)); d.Verdict != neumann.VerdictAllow {
		t.Fatalf("idempotent: %+v", d)
	}
	if got := l.all(); len(got) != 2 || !strings.Contains(got[1], "notes_touch call t1/c-1") || !strings.Contains(got[1], "boom") {
		t.Fatalf("logged %q", got)
	}

	// A missing answer is no answer.
	e = &decidetest.Engine{Fn: func(string, map[string]decide.Question) (map[string]decide.Answer, error) {
		return map[string]decide.Answer{}, nil
	}}
	if d := decide.Policy(e, decide.PolicyConfig{Logf: l.logf})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("missing answer: %+v", d)
	}

	// A fallback of the caller's own.
	var gotErr error
	p = decide.Policy(&decidetest.Engine{}, decide.PolicyConfig{Logf: l.logf, Fallback: func(_ neumann.ToolCallInfo, err error) neumann.Decision {
		gotErr = err
		return neumann.Deny("no engine, no calls")
	}})
	if d := p(bg, call("x", bus.Idempotent)); d.Verdict != neumann.VerdictDeny || d.Reason != "no engine, no calls" || gotErr == nil {
		t.Fatalf("%+v %v", d, gotErr)
	}
}

// TestPolicyTimeoutBoundsTheHook: the machine gives a policy no timeout, so
// the policy's own must cut a slow engine short.
func TestPolicyTimeoutBoundsTheHook(t *testing.T) {
	var l logs
	e := &decidetest.Engine{Fn: risky, Delay: time.Minute}
	p := decide.Policy(e, decide.PolicyConfig{Timeout: 50 * time.Millisecond, Logf: l.logf})
	start := time.Now()
	d := p(bg, call("notes_save", bus.SideEffect))
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the policy took %s", took)
	}
	if d.Verdict != neumann.VerdictRequireApproval || d.Reason != "decidetest: no answer (no answer within 50ms); held because the call has side effects" {
		t.Fatalf("%+v", d)
	}
	// The caller's own cancel is not reported as the policy's timeout.
	ctx, cancel := context.WithCancel(bg)
	cancel()
	d = p(ctx, call("notes_save", bus.SideEffect))
	if d.Verdict != neumann.VerdictRequireApproval || !strings.Contains(d.Reason, "context canceled") {
		t.Fatalf("%+v", d)
	}
}

func TestPolicyExtraQuestionsAndDecide(t *testing.T) {
	e := &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.NeedsApproval: decidetest.Yes(0.1),
		"pii":                decidetest.Yes(0.9),
	}}
	var seen neumann.Decision
	p := decide.Policy(e, decide.PolicyConfig{
		Question: decide.Noul("Does this call touch production?"),
		Extra:    map[string]decide.Question{"pii": decide.Noul("Do the arguments hold personal data?")},
		Decide: func(info neumann.ToolCallInfo, answers map[string]decide.Answer, d neumann.Decision) neumann.Decision {
			seen = d
			if answers["pii"].Yes {
				return neumann.Deny(fmt.Sprintf("%s would send personal data", info.Tool))
			}
			return d
		},
		Tools: map[string]string{"mail_send": "Send an email to anyone."},
	})
	info := call("mail_send", bus.SideEffect)
	info.Args = json.RawMessage(`{"to":"bob@example.com"`) // invalid JSON: shown as written
	d := p(bg, info)
	if d.Verdict != neumann.VerdictDeny || d.Reason != "mail_send would send personal data" || seen.Verdict != neumann.VerdictAllow {
		t.Fatalf("%+v (policy alone: %+v)", d, seen)
	}
	a := e.Asked()[0]
	if a.Questions[decide.NeedsApproval].Instructions != "Does this call touch production?" || a.Questions["pii"].Type != decide.TypeNoul {
		t.Fatalf("questions %+v", a.Questions)
	}
	if !strings.Contains(a.State, "about the tool: Send an email to anyone.\n") || !strings.HasSuffix(a.State, `arguments: {"to":"bob@example.com"`) {
		t.Fatalf("state %q", a.State)
	}
}

func TestCallStateClipsArguments(t *testing.T) {
	info := neumann.ToolCallInfo{Tool: "fs_write", Effect: bus.SideEffect, Args: json.RawMessage(`{"text":"` + strings.Repeat("é", 5000) + `"}`)}
	s := decide.CallState(info, "")
	if strings.Contains(s, "device:") || strings.Contains(s, "task class:") || strings.Contains(s, "about the tool:") {
		t.Fatalf("empty fields shown: %q", s[:100])
	}
	if !strings.HasSuffix(s, "more bytes)") || len(s) > 4096+200 || !strings.Contains(s, "é…") {
		t.Fatalf("clipped to %d bytes, ending %q", len(s), s[len(s)-40:])
	}
	if s := decide.CallState(neumann.ToolCallInfo{Tool: "x"}, ""); !strings.HasSuffix(s, "arguments: (none)") {
		t.Fatalf("%q", s)
	}
}

func TestPolicyPanicsOnBadConfig(t *testing.T) {
	e := &decidetest.Engine{}
	for name, f := range map[string]func(){
		"nil engine":   func() { decide.Policy(nil, decide.PolicyConfig{}) },
		"not a noul":   func() { decide.Policy(e, decide.PolicyConfig{Question: decide.Choice("?", "a", "b")}) },
		"bad question": func() { decide.Policy(e, decide.PolicyConfig{Question: decide.Noul(" ")}) },
		"name clash": func() {
			decide.Policy(e, decide.PolicyConfig{Extra: map[string]decide.Question{decide.NeedsApproval: decide.Noul("?")}})
		},
		"bad extra": func() {
			decide.Policy(e, decide.PolicyConfig{Extra: map[string]decide.Question{"x": decide.Choice("?", "a")}})
		},
		"threshold": func() { decide.Policy(e, decide.PolicyConfig{Threshold: 1}) },
	} {
		func() {
			defer func() {
				if v := recover(); v == nil || !strings.HasPrefix(fmt.Sprint(v), "decide: ") {
					t.Errorf("%s: recovered %v", name, v)
				}
			}()
			f()
		}()
	}
}

// TestPolicyInAMachine runs the policy as a machine's ToolPolicy: in one
// round, a read is let through unasked, a save is allowed, and a delete is
// held for approval with the engine's answer as its committed reason.
func TestPolicyInAMachine(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	notes := builtin.New("notes")
	for _, d := range []bus.ToolDef{
		{Name: "read", Effect: bus.ReadOnly},
		{Name: "save", Effect: bus.SideEffect},
		{Name: "delete", Effect: bus.SideEffect},
	} {
		notes.Add(d, func(_ context.Context, c bus.DeviceCall) (string, error) {
			mu.Lock()
			ran = append(ran, d.Name)
			mu.Unlock()
			return d.Name + " ok", nil
		})
	}
	e := &decidetest.Engine{Fn: risky}
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("notes_read", "notes_save", "notes_delete"), nil
			}
			return sim.Reply("done"), nil
		}},
		Devices:    []bus.Device{notes},
		ToolPolicy: decide.Policy(e, decide.PolicyConfig{Logf: t.Logf}),
		Logf:       t.Logf,
	})
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
	id, err := m.Submit(bg, proto.Instruction{TaskID: "t1", Input: "tidy my notes"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	var held []map[string]any
	for ev := range m.Events(ctx, 0) {
		if ev.TaskID != id {
			continue
		}
		if ev.Kind == proto.EvApprovalNeeded {
			var d map[string]any
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			held = append(held, d)
			tc, err := m.Load(bg, id)
			if err != nil {
				t.Fatal(err)
			}
			if tc.State != task.Waiting || tc.Wait != task.WaitApproval {
				t.Fatalf("task is %s/%s while a call is held", tc.State, tc.Wait)
			}
			mu.Lock()
			n := len(ran)
			mu.Unlock()
			if n != 0 {
				t.Fatalf("%d calls ran while the round was held", n)
			}
			recs, err := m.Calls(bg, id)
			if err != nil {
				t.Fatal(err)
			}
			byTool := map[string]mem.CallRecord{}
			for _, r := range recs {
				byTool[r.Tool] = r
			}
			if r := byTool["notes_read"]; r.Policy != mem.PolicyAllow {
				t.Fatalf("read %+v", r)
			}
			if r := byTool["notes_save"]; r.Policy != mem.PolicyAllow {
				t.Fatalf("save %+v", r)
			}
			r := byTool["notes_delete"]
			if r.Policy != mem.PolicyApproval || r.Approval != mem.ApprovalPending || r.Reason != "decidetest: needs_approval yes, p=0.97" {
				t.Fatalf("delete %+v", r)
			}
			if err := m.Approve(bg, id, fmt.Sprint(d["call_id"]), neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}); err != nil {
				t.Fatal(err)
			}
		}
		if ev.Terminal() {
			if ev.Kind != proto.EvDone {
				t.Fatalf("task ended %s %s", ev.Kind, ev.Data)
			}
			break
		}
	}
	if len(held) != 1 || held[0]["tool"] != "notes_delete" || held[0]["reason"] != "decidetest: needs_approval yes, p=0.97" {
		t.Fatalf("approval_needed %v", held)
	}
	asked := e.Asked()
	if len(asked) != 2 || !strings.Contains(asked[0].State, "tool: notes_save") || !strings.Contains(asked[1].State, "tool: notes_delete") {
		t.Fatalf("the engine was asked %d times: %+v", len(asked), asked)
	}
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(ran)
	if !slices.Equal(ran, []string{"delete", "read", "save"}) {
		t.Fatalf("ran %v", ran)
	}
}
