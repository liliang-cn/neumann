package task

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCloneIsDeep(t *testing.T) {
	orig := &TCB{
		ID:         "t1",
		ReadySince: time.Unix(100, 0).UTC(),
		Regs:       []Message{Text(RoleUser, "hi")},
		Allow:      []string{"a"},
		Version:    3,
	}
	c := orig.Clone()
	c.Regs[0].Parts[0].Text = "changed"
	c.Allow[0] = "b"
	if orig.Regs[0].Parts[0].Text != "hi" || orig.Allow[0] != "a" {
		t.Fatalf("clone shares memory with the original")
	}
	if c.ID != "t1" || c.Version != 3 || !c.ReadySince.Equal(orig.ReadySince) {
		t.Fatalf("clone lost fields: %+v", c)
	}
}

func TestTerminal(t *testing.T) {
	want := map[State]bool{
		Ready: false, Running: false, Waiting: false, Suspended: false,
		Done: true, Failed: true, Cancelled: true,
	}
	for s, w := range want {
		if s.Terminal() != w {
			t.Errorf("%s.Terminal() = %v, want %v", s, !w, w)
		}
	}
}

func TestLastAnswer(t *testing.T) {
	calls := Message{Role: RoleAssistant, Parts: []Part{
		{Kind: PartToolCall, Name: "x", Args: json.RawMessage(`{}`)},
	}}
	tc := &TCB{Regs: []Message{
		Text(RoleUser, "q"),
		Text(RoleAssistant, "earlier"),
		calls,
		{Role: RoleTool, Parts: []Part{{Kind: PartToolResult, CallID: "c-1"}}},
	}}
	if got := tc.LastAnswer(); got != "earlier" {
		t.Fatalf("LastAnswer = %q, want earlier", got)
	}
	none := &TCB{Regs: []Message{Text(RoleUser, "q"), calls}}
	if got := none.LastAnswer(); got != "" {
		t.Fatalf("LastAnswer = %q, want empty", got)
	}
}

func TestCloneNilAndEmpty(t *testing.T) {
	if (*TCB)(nil).Clone() != nil {
		t.Fatal("nil Clone should be nil")
	}
	c := (&TCB{Allow: []string{}}).Clone()
	if c.Allow == nil {
		t.Fatal("empty non-nil Allow became nil")
	}
	if (&TCB{}).Clone().Allow != nil {
		t.Fatal("nil Allow became non-nil")
	}
}

func TestCloneKeepsRawBytes(t *testing.T) {
	bad := json.RawMessage(`{"a":`)
	odd := json.RawMessage(`{ "s": "a<b" }`)
	schema := json.RawMessage(`{ "type": "object" }`)
	orig := &TCB{
		Tools: []ToolSpec{{Name: "n", Schema: schema}},
		Deny:  []string{"d"},
		Regs: []Message{{Role: RoleAssistant, Parts: []Part{
			{Kind: PartToolCall, Args: bad},
			{Kind: PartToolCall, Args: odd},
			{Kind: PartReasoning, Opaque: odd},
		}}},
	}
	c := orig.Clone() // must not panic on truncated Args
	ps := c.Regs[0].Parts
	if string(ps[0].Args) != `{"a":` || string(ps[1].Args) != `{ "s": "a<b" }` ||
		string(ps[2].Opaque) != `{ "s": "a<b" }` || string(c.Tools[0].Schema) != `{ "type": "object" }` {
		t.Fatalf("raw bytes changed: %s %s %s %s", ps[0].Args, ps[1].Args, ps[2].Opaque, c.Tools[0].Schema)
	}
	ps[1].Args[2] = 'X'
	ps[2].Opaque[2] = 'X'
	c.Tools[0].Name = "changed"
	c.Deny[0] = "changed"
	op := orig.Regs[0].Parts
	if string(op[1].Args) != `{ "s": "a<b" }` || string(op[2].Opaque) != `{ "s": "a<b" }` ||
		orig.Tools[0].Name != "n" || orig.Deny[0] != "d" {
		t.Fatal("clone shares memory with the original")
	}
}

func TestStateNamesInJSON(t *testing.T) {
	in := &TCB{State: Done, PC: StepTools, Wait: WaitIO}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{`"state":"done"`, `"pc":"tools"`, `"wait":"io"`} {
		if !strings.Contains(string(b), w) {
			t.Errorf("missing %s in %s", w, b)
		}
	}
	var out TCB
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.State != Done || out.PC != StepTools || out.Wait != WaitIO {
		t.Fatalf("round trip: %+v", out)
	}
	if err := json.Unmarshal([]byte(`{"state":"bogus"}`), &out); err == nil {
		t.Fatal("bogus state accepted")
	}
	if err := json.Unmarshal([]byte(`{"pc":"bogus"}`), &out); err == nil {
		t.Fatal("bogus pc accepted")
	}
	if err := json.Unmarshal([]byte(`{"wait":"bogus"}`), &out); err == nil {
		t.Fatal("bogus wait accepted")
	}
	if _, err := json.Marshal(&TCB{State: State(99)}); err == nil {
		t.Fatal("out-of-range state marshalled")
	}
}
