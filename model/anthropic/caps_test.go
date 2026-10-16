package anthropic

import (
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestCaps(t *testing.T) {
	a := New("http://x", "k", "m")
	if got := a.Caps(); got != (model.Caps{Window: DefaultContextWindow, MaxOut: DefaultMaxTokens, Tools: true, ThinkOff: true}) {
		t.Fatalf("default caps %+v", got)
	}
	a.ContextWindow, a.NoTools, a.MaxTools = -1, true, 64
	if got := model.CapsOf(a); got != (model.Caps{MaxOut: DefaultMaxTokens, MaxTools: 64, ThinkOff: true}) {
		t.Fatalf("configured caps %+v", got)
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
