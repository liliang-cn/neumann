package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestCaps(t *testing.T) {
	a := New("http://x", "k", "m")
	if got := a.Caps(); got != (model.Caps{Window: DefaultContextWindow, MaxOut: DefaultMaxTokens, Tools: true}) {
		t.Fatalf("default caps %+v", got)
	}
	a.ContextWindow, a.NoTools, a.MaxTools, a.ThinkOff = -1, true, 64, "disabled"
	if got := model.CapsOf(a); got != (model.Caps{MaxOut: DefaultMaxTokens, MaxTools: 64, ThinkOff: true}) {
		t.Fatalf("configured caps %+v", got)
	}
	b := New("http://x", "k", "m")
	b.ThinkingBudget = 2048
	if !b.Caps().ThinkOff {
		t.Fatal("a model with a thinking budget does not think without it: ThinkOff")
	}
}

// With ThinkOff set, a request with NoThink turns thinking off explicitly
// (current models think when the field is left out), and nothing else
// about the thinking configuration changes: the budget goes out as before
// on requests without NoThink.
func TestNoThinkSendsThinkOff(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}
	off := req
	off.NoThink = true
	for _, c := range []struct {
		thinkOff string
		budget   int
		req      model.Request
		want     string // the thinking field as sent; empty: none
	}{
		{"disabled", 0, off, `{"type":"disabled"}`},
		{"between_tools", 0, off, `{"type":"between_tools"}`},
		{"disabled", 2048, off, `{"type":"disabled"}`},
		{"disabled", 2048, req, `{"budget_tokens":2048,"type":"enabled"}`},
		{"disabled", 0, req, ``},
		{"", 0, off, ``}, // cannot turn it off: NoThink is ignored
	} {
		a.ThinkOff, a.ThinkingBudget = c.thinkOff, c.budget
		n := len(s.raw)
		call(t, a, c.req)
		th, ok := s.body(n)["thinking"]
		got := ""
		if ok {
			b, _ := json.Marshal(th)
			got = string(b)
		}
		if got != c.want {
			t.Fatalf("ThinkOff %q, budget %d, NoThink %v: thinking %s, want %s", c.thinkOff, c.budget, c.req.NoThink, got, c.want)
		}
	}
}

func TestNoThinkSendsNoThinking(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	a.ThinkingBudget = 2048
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}
	call(t, a, req)
	req.NoThink = true
	call(t, a, req)
	if th, ok := s.body(0)["thinking"].(map[string]any); !ok || th["budget_tokens"] != float64(2048) {
		t.Fatalf("thinking %v", s.body(0)["thinking"])
	}
	if th, ok := s.body(1)["thinking"]; ok {
		t.Fatalf("NoThink sent thinking %v", th)
	}
}
