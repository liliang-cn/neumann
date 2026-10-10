package task

import (
	"encoding/json"
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

func TestLastAnswerAndCallCount(t *testing.T) {
	tc := &TCB{Regs: []Message{
		Text(RoleUser, "q"),
		{Role: RoleAssistant, Parts: []Part{
			{Kind: PartToolCall, Name: "x", Args: json.RawMessage(`{}`)},
			{Kind: PartToolCall, Name: "y"},
		}},
		{Role: RoleTool, Parts: []Part{{Kind: PartToolResult, CallID: "c-1"}}},
		Text(RoleAssistant, "answer"),
	}}
	if got := tc.LastAnswer(); got != "answer" {
		t.Fatalf("LastAnswer = %q", got)
	}
	if got := tc.CallCount(); got != 2 {
		t.Fatalf("CallCount = %d", got)
	}
}
