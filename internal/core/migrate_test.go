package core

import (
	"encoding/json"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// A request to one provider leaves out the reasoning another produced, and a
// message left empty by that; its own reasoning, and reasoning with no
// provider, stay. Nothing is copied when there is nothing to leave out.
func TestForProvider(t *testing.T) {
	r := func(p string) task.Part {
		return task.Part{Kind: task.PartReasoning, Text: "r", Provider: p, Opaque: json.RawMessage(`{}`)}
	}
	msgs := []task.Message{
		task.Text(task.RoleUser, "q"),
		{Role: task.RoleAssistant, Parts: []task.Part{r("a"), r(""), {Kind: task.PartText, Text: "x"}}},
		{Role: task.RoleAssistant, Parts: []task.Part{r("a")}},
		{Role: task.RoleAssistant, Parts: []task.Part{r("b"), {Kind: task.PartText, Text: "y"}}},
	}
	got := forProvider(msgs, "b")
	// The two assistant turns around the one left out are joined.
	if len(got) != 2 || len(got[1].Parts) != 4 || got[1].Parts[0].Provider != "" || got[1].Parts[2].Provider != "b" {
		t.Fatalf("for b: %+v", got)
	}
	if len(msgs[1].Parts) != 3 || len(msgs[2].Parts) != 1 {
		t.Fatal("the registers were changed")
	}
	if a := forProvider(msgs[:2], "a"); &a[0] != &msgs[0] {
		t.Fatal("copied with nothing to leave out")
	}
}

// Leaving out a message whose only part was foreign reasoning does not leave
// two messages of one role side by side: the ones it separated are joined.
func TestForProviderKeepsRolesAlternating(t *testing.T) {
	msgs := []task.Message{
		task.Text(task.RoleUser, "q"),
		{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartReasoning, Provider: "a", Opaque: json.RawMessage(`{}`)}}},
		task.Text(task.RoleUser, "steer"),
		task.Text(task.RoleAssistant, "x"),
	}
	got := forProvider(msgs, "b")
	if len(got) != 2 || got[0].Role != task.RoleUser || len(got[0].Parts) != 2 || got[0].Parts[1].Text != "steer" || got[1].TextContent() != "x" {
		t.Fatalf("for b: %+v", got)
	}
	if len(msgs[0].Parts) != 1 {
		t.Fatal("the registers were changed")
	}
}

type panickyCaps struct{ *sim.Model }

func (panickyCaps) Caps() model.Caps { panic("no caps") }

// A target whose adapter panics reporting its caps refuses the move instead
// of taking the vCore down.
func TestFitsSurvivesAPanickingCaps(t *testing.T) {
	e, _, _ := newTestEnv(t, nil)
	target := &Group{Class: "x", Model: panickyCaps{&sim.Model{}}}
	err := e.fits(&task.TCB{Class: "y"}, target)
	if err == nil || err.Check != CheckCaps || err.To != "x" || err.From != "y" {
		t.Fatalf("fits: %v", err)
	}
	ok := &Group{Class: "z", Model: &sim.Model{Info: &model.Caps{Tools: true, Window: 100000}}}
	if err := e.fits(&task.TCB{Class: "y", Regs: []task.Message{task.Text(task.RoleUser, "q")}}, ok); err != nil {
		t.Fatalf("fits a small task: %v", err)
	}
}
