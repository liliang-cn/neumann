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
	if len(got) != 3 || len(got[1].Parts) != 2 || got[1].Parts[0].Provider != "" || len(got[2].Parts) != 2 {
		t.Fatalf("for b: %+v", got)
	}
	if len(msgs[1].Parts) != 3 || len(msgs[2].Parts) != 1 {
		t.Fatal("the registers were changed")
	}
	if a := forProvider(msgs[:2], "a"); &a[0] != &msgs[0] {
		t.Fatal("copied with nothing to leave out")
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
