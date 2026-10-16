package core

import (
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// keepPartial (design §6.4): text continues text, kept text ends the
// thinking, thinking is kept only before any text; an adapter that cannot
// continue keeps nothing new, and never drops a Partial the task already
// has (a turn its pending steers interrupt is still delivered).
func TestKeepPartial(t *testing.T) {
	both := model.Caps{Prefill: true, ThinkPrefill: true}
	for _, c := range []struct {
		name                string
		partial, think      string // the task's before
		text, reasoning     string // what the call produced
		caps                model.Caps
		wantP, wantT        string
		keptText, keptThink string
	}{
		{"text continues text", "Once ", "", "upon", "", both, "Once upon", "", "upon", ""},
		{"text begun ends the thinking", "", "hm", "Once", "hm more", both, "Once", "", "Once", ""},
		{"thinking only", "", "hm", "", " more", both, "", "hm more", "", " more"},
		{"thinking without ThinkPrefill", "", "", "", "hm", model.Caps{Prefill: true}, "", "", "", ""},
		{"no prefill keeps the turn", "Once ", "", "upon", "", model.Caps{}, "Once ", "", "", ""},
		{"no prefill, no Partial", "", "", "upon", "", model.Caps{ThinkPrefill: true}, "", "", "", ""},
		{"no prefill, text begun after thinking", "", "", "upon", "hm", model.Caps{ThinkPrefill: true}, "", "hm", "", "hm"},
	} {
		tc := &task.TCB{Partial: c.partial, PartialThink: c.think}
		kt, kth := keepPartial(tc, c.text, c.reasoning, c.caps)
		if tc.Partial != c.wantP || tc.PartialThink != c.wantT || kt != c.keptText || kth != c.keptThink {
			t.Errorf("%s: Partial %q PartialThink %q, kept %q / %q", c.name, tc.Partial, tc.PartialThink, kt, kth)
		}
	}
}

func TestContinuedJoins(t *testing.T) {
	call := task.Part{Kind: task.PartToolCall, CallID: "c-1", Name: "x"}
	reason := task.Part{Kind: task.PartReasoning, Text: " more"}
	got := continued("Once ", task.Message{Role: task.RoleAssistant, Parts: []task.Part{reason, call}})
	if len(got.Parts) != 3 || got.Parts[0].Text != " more" || got.Parts[1].Kind != task.PartText || got.Parts[1].Text != "Once " {
		t.Fatalf("continued without text: %+v", got.Parts)
	}
	got = continuedThink("hm", task.Message{Role: task.RoleAssistant, Parts: []task.Part{reason, {Kind: task.PartText, Text: "A"}}})
	if got.Parts[0].Text != "hm more" || got.TextContent() != "A" {
		t.Fatalf("continuedThink: %+v", got.Parts)
	}
	got = continuedThink("hm", task.Text(task.RoleAssistant, "A"))
	if got.Parts[0].Kind != task.PartReasoning || got.Parts[0].Text != "hm" || got.TextContent() != "A" {
		t.Fatalf("continuedThink without reasoning: %+v", got.Parts)
	}
}
