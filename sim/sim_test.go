package sim

import (
	"context"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestHelpers(t *testing.T) {
	req := model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "first"),
		CallTools("a", "b").Message,
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1"}}},
		task.Text(task.RoleUser, "second"),
	}}
	if FirstUser(req) != "first" {
		t.Fatalf("FirstUser = %q", FirstUser(req))
	}
	if got := UserTexts(req); len(got) != 2 || got[1] != "second" {
		t.Fatalf("UserTexts = %v", got)
	}
	if ToolRounds(req) != 1 {
		t.Fatalf("ToolRounds = %d", ToolRounds(req))
	}
	calls := CallTools("a", "b").Message.ToolCalls()
	if len(calls) != 2 || calls[1].Name != "b" || string(calls[1].Args) != `{"n":1}` || calls[1].ProviderCallID != "p-1" {
		t.Fatalf("CallTools = %+v", calls)
	}
}

func TestModelCountsCalls(t *testing.T) {
	m := &Model{Fn: func(model.Request) (model.Response, error) { return Reply("ok"), nil }}
	for range 3 {
		if _, err := m.Call(context.Background(), model.Request{}); err != nil {
			t.Fatal(err)
		}
	}
	if m.Calls() != 3 {
		t.Fatalf("Calls = %d", m.Calls())
	}
}
