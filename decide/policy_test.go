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
func risky(state string, _ []decide.Ask) (map[string]decide.Answer, error) {
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
	q, ok := asked[0].Question(decide.NeedsApproval)
	if !ok || len(asked[0].Asks) != 1 || q.Instructions != decide.DefaultApprovalQuestion().Instructions {
		t.Fatalf("questions %+v", asked[0].Asks)
	}
	want := "tool: notes_delete\ndevice: notes\ndeclared effect: side_effect\ntask class: default\narguments: {\"id\":\"n-42\"}"
	if asked[0].State != want {
		t.Fatalf("state %q", asked[0].State)
	}
}

func TestPolicyThreshold(t *testing.T) {
	e := &decidetest.Engine{Answers: map[string]decide.Answer{decide.NeedsApproval: decidetest.Yes(0.6)}} // confidence 0.2
	if d := decide.Policy(e, decide.PolicyConfig{})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("the default threshold holds any yes: %+v", d)
	}
	if d := decide.Policy(e, decide.PolicyConfig{Threshold: -1})(bg, call("x", bus.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Fatalf("a negative threshold holds any yes: %+v", d)
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
	e := &decidetest.Engine{Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) {
		return nil, errors.New("decide: decidetest: 500 Internal Server Error: boom")
	}}
	p := decide.Policy(e, decide.PolicyConfig{Logf: l.logf})
	for _, effect := range []bus.Effect{bus.SideEffect, bus.Idempotent} {
		// Idempotent is not harmless: a delete by id is idempotent.
		d := p(bg, call("notes_delete", effect))
		if d.Verdict != neumann.VerdictRequireApproval || d.Reason != "decidetest: no answer (500 Internal Server Error: boom); held" {
			t.Fatalf("%s: %+v", effect, d)
		}
	}
	got := l.all()
	if len(got) != 2 || got[1] != "decide: policy for notes_delete call t1/c-1: decidetest: 500 Internal Server Error: boom" {
		t.Fatalf("logged %q", got)
	}

	// A missing answer, or one of the wrong type, is no answer.
	for name, c := range map[string]struct {
		answers map[string]decide.Answer
		reason  string
	}{
		"missing":    {map[string]decide.Answer{}, `decidetest: no answer ("needs_approval" unanswered); held`},
		"wrong type": {map[string]decide.Answer{decide.NeedsApproval: decidetest.Pick("yes", 0.9)}, `decidetest: no answer ("needs_approval" answered as a choice, asked as a noul); held`},
	} {
		e = &decidetest.Engine{Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) { return c.answers, nil }}
		d := decide.Policy(e, decide.PolicyConfig{Logf: l.logf})(bg, call("x", bus.SideEffect))
		if d.Verdict != neumann.VerdictRequireApproval || d.Reason != c.reason {
			t.Fatalf("%s: %+v", name, d)
		}
	}
	if got := l.all(); len(got) != 4 {
		t.Fatalf("logged %q", got)
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
// the policy's own must cut a slow engine short, and a stuck one too.
func TestPolicyTimeoutBoundsTheHook(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		var l logs
		e := &decidetest.Engine{Fn: risky, Delay: 500 * time.Millisecond, IgnoreContext: ignore}
		p := decide.Policy(e, decide.PolicyConfig{Timeout: 50 * time.Millisecond, Logf: l.logf})
		start := time.Now()
		d := p(bg, call("notes_save", bus.SideEffect))
		if took := time.Since(start); took > 400*time.Millisecond {
			t.Fatalf("ignore context %v: the policy took %s", ignore, took)
		}
		if d.Verdict != neumann.VerdictRequireApproval || d.Reason != "decidetest: no answer within 50ms; held" {
			t.Fatalf("ignore context %v: %+v", ignore, d)
		}
		if got := l.all(); len(got) != 1 || got[0] != "decide: policy for notes_save call t1/c-1: decidetest: no answer within 50ms" {
			t.Fatalf("logged %q", got)
		}
	}

	// The task's own end (a cancel, its deadline) is not the engine's
	// failure: the call falls back, and nothing is logged.
	var l logs
	p := decide.Policy(&decidetest.Engine{Fn: risky, Delay: time.Minute}, decide.PolicyConfig{Logf: l.logf})
	ctx, cancel := context.WithCancel(bg)
	time.AfterFunc(20*time.Millisecond, cancel)
	d := p(ctx, call("notes_save", bus.SideEffect))
	if d.Verdict != neumann.VerdictRequireApproval || !strings.Contains(d.Reason, "context canceled") {
		t.Fatalf("%+v", d)
	}
	if got := l.all(); len(got) != 0 {
		t.Fatalf("logged %q", got)
	}
}

func TestPolicyExtraQuestionsAndDecide(t *testing.T) {
	e := &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.NeedsApproval: decidetest.Yes(0.1),
		"pii":                decidetest.Yes(0.9),
		"bulk":               decidetest.Yes(0.1),
	}}
	var seen neumann.Decision
	p := decide.Policy(e, decide.PolicyConfig{
		Question: decide.Noul("Does this call touch production?"),
		Extra:    []decide.Ask{{Name: "pii", Question: decide.Noul("Do the arguments hold personal data?")}, {Name: "bulk", Question: decide.Noul("Many records?")}},
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
	if names(a.Asks) != "needs_approval,pii,bulk" || a.Asks[0].Question.Instructions != "Does this call touch production?" {
		t.Fatalf("questions %+v", a.Asks)
	}
	if !strings.Contains(a.State, "about the tool: Send an email to anyone.\n") || !strings.HasSuffix(a.State, `arguments: {"to":"bob@example.com"`) {
		t.Fatalf("state %q", a.State)
	}
}

func TestCallStateClipsArguments(t *testing.T) {
	info := neumann.ToolCallInfo{Tool: "fs_write", Effect: bus.SideEffect, Args: json.RawMessage(`{"text":"` + strings.Repeat("é", 5000) + `"}`)}
	s := decide.CallState(info, "", decide.DefaultMaxArgs)
	if strings.Contains(s, "device:") || strings.Contains(s, "task class:") || strings.Contains(s, "about the tool:") {
		t.Fatalf("empty fields shown: %q", s[:100])
	}
	if !strings.HasSuffix(s, `é"}`) || !strings.Contains(s, `arguments: {"text":"éé`) || !strings.Contains(s, "bytes cut]…") || len(s) > decide.DefaultMaxArgs+100 {
		t.Fatalf("clipped to %d bytes: %q", len(s), s)
	}
	if s := decide.CallState(info, "", -1); !strings.HasSuffix(s, `é"}`) {
		t.Fatal("a negative maxArgs cut the arguments")
	}
	if s := decide.CallState(info, "", 0); len(s) > decide.DefaultMaxArgs+100 {
		t.Fatalf("zero maxArgs is not DefaultMaxArgs: %d bytes", len(s))
	}
	if s := decide.CallState(neumann.ToolCallInfo{Tool: "x"}, "", 0); !strings.HasSuffix(s, "arguments: (none)") {
		t.Fatalf("%q", s)
	}

	// Policy cuts at DefaultMaxArgs unless told otherwise.
	e := &decidetest.Engine{Fn: risky}
	decide.Policy(e, decide.PolicyConfig{})(bg, info)
	decide.Policy(e, decide.PolicyConfig{MaxArgs: 100})(bg, info)
	if a := e.Asked(); len(a[0].State) > decide.DefaultMaxArgs+100 || len(a[1].State) > 250 {
		t.Fatalf("states of %d and %d bytes", len(a[0].State), len(a[1].State))
	}
}

// panicking is an engine that panics on every request.
func panicking() *decidetest.Engine {
	return &decidetest.Engine{Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) {
		var m map[string]decide.Answer
		m["x"] = decide.Answer{} // a nil map: panics
		return m, nil
	}}
}

// TestPolicySurvivesAPanickingEngine: the engine runs on a goroutine of its
// own, where the machine's recover cannot reach; the policy must, and hold
// the call.
func TestPolicySurvivesAPanickingEngine(t *testing.T) {
	var l logs
	d := decide.Policy(panicking(), decide.PolicyConfig{Logf: l.logf})(bg, call("notes_delete", bus.SideEffect))
	if d.Verdict != neumann.VerdictRequireApproval || d.Reason != "decidetest: no answer (decidetest panicked: assignment to entry in nil map); held" {
		t.Fatalf("%+v", d)
	}
	if got := l.all(); len(got) != 1 || !strings.Contains(got[0], "panicked") {
		t.Fatalf("logged %q", got)
	}
}

func TestCallStateKeepsEveryKey(t *testing.T) {
	note := strings.Repeat("Routine housekeeping of the build machine, as agreed. ", 13)
	cases := map[string]struct {
		args       string
		want       []string
		maxLen     int
		middleOnly bool
	}{
		"a late field": {
			args:   `{"comment":"` + note + `","z_cmd":"rm -rf / --no-preserve-root"}`,
			want:   []string{`"comment":"Routine housekeeping`, `bytes cut]…`, `"z_cmd":"rm -rf / --no-preserve-root"}`},
			maxLen: decide.DefaultMaxArgs + 40,
		},
		"nested, the longest cut most": {
			args:   `{"a":"` + strings.Repeat("x", 2000) + `","opts":{"force":true,"paths":["/etc","` + strings.Repeat("y", 300) + `"]},"n":3}`,
			want:   []string{`"opts":{"force":true,"paths":["/etc","yyy`, `,"n":3}`, `"a":"xxx`},
			maxLen: decide.DefaultMaxArgs + 40,
		},
		"short values whole": {
			args: `{"to":"bob@example.com","subject":"<hi> & bye","body":"` + strings.Repeat("z", 3000) + `"}`,
			want: []string{`"to":"bob@example.com"`, `"subject":"<hi> & bye"`, `"body":"zzz`},
		},
		"an array": {
			args:       `["` + strings.Repeat("a", 2000) + `","rm -rf /"]`,
			want:       []string{`["aaa`, `bytes cut …]`, `rm -rf /"]`},
			middleOnly: true,
		},
		"invalid JSON": {
			args:       `{"comment":"` + note + note + `", "cmd":"rm -rf /"`,
			want:       []string{`{"comment":"Routine`, `bytes cut …]`, `"cmd":"rm -rf /"`},
			middleOnly: true,
		},
	}
	for name, c := range cases {
		s := decide.CallState(neumann.ToolCallInfo{Tool: "shell_run", Args: json.RawMessage(c.args)}, "", 0)
		args := s[strings.Index(s, "arguments: ")+len("arguments: "):]
		for _, w := range c.want {
			if !strings.Contains(args, w) {
				t.Errorf("%s: lacks %s: %s", name, w, args)
			}
		}
		if limit := max(c.maxLen, decide.DefaultMaxArgs+80); len(args) > limit {
			t.Errorf("%s: %d bytes", name, len(args))
		}
		if !c.middleOnly && !json.Valid([]byte(args)) {
			t.Errorf("%s: not JSON: %s", name, args)
		}
	}

	// So many keys that they alone do not fit: the rendering is cut.
	var many []string
	for i := range 200 {
		many = append(many, fmt.Sprintf(`"key_%03d":"value"`, i))
	}
	s := decide.CallState(neumann.ToolCallInfo{Tool: "x", Args: json.RawMessage("{" + strings.Join(many, ",") + "}")}, "", 0)
	if !strings.Contains(s, `"key_000"`) || !strings.Contains(s, `"key_199":"value"}`) || !strings.Contains(s, "bytes cut …]") {
		t.Fatalf("%s", s)
	}
}

func TestPolicyPanicsOnBadConfig(t *testing.T) {
	e := &decidetest.Engine{}
	for name, f := range map[string]func(){
		"nil engine":   func() { decide.Policy(nil, decide.PolicyConfig{}) },
		"not a noul":   func() { decide.Policy(e, decide.PolicyConfig{Question: decide.Choice("?", "a", "b")}) },
		"bad question": func() { decide.Policy(e, decide.PolicyConfig{Question: decide.Noul(" ")}) },
		"name clash": func() {
			decide.Policy(e, decide.PolicyConfig{Extra: []decide.Ask{{Name: decide.NeedsApproval, Question: decide.Noul("?")}}})
		},
		"bad extra": func() {
			decide.Policy(e, decide.PolicyConfig{Extra: []decide.Ask{{Name: "x", Question: decide.Choice("?", "a")}}})
		},
		"threshold": func() { decide.Policy(e, decide.PolicyConfig{Threshold: 1}) },
		"64 extras": func() {
			var extra []decide.Ask
			for i := range decide.MaxQuestions {
				extra = append(extra, decide.Ask{Name: fmt.Sprint("x", i), Question: decide.Noul("?")})
			}
			decide.Policy(e, decide.PolicyConfig{Extra: extra})
		},
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
