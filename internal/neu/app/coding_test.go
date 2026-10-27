package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func codingApp(t *testing.T, mode string, m model.Adapter) (*App, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello world\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Backend: "local", Model: "m", Parallel: 1, Root: root, Writable: "out", MaxSteps: 10, Quiet: true,
		Store: filepath.Join(root, ".neu", "state.db"), Coding: Coding{On: true, Approve: mode, ShScript: true}}
	if m == nil {
		m = &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }}
	}
	a, err := Build(context.Background(), cfg, Options{Adapter: m})
	if err != nil {
		t.Fatal(err)
	}
	return a, root
}

func info(tool, args string, effect task.Effect) neumann.ToolCallInfo {
	return neumann.ToolCallInfo{TaskID: "t", CallID: "c", Tool: tool, Args: json.RawMessage(args), Effect: effect, Attempt: 1}
}

func TestPolicyPerMode(t *testing.T) {
	calls := []struct {
		name, tool, args string
		effect           task.Effect
		ask, auto, never neumann.Verdict
	}{
		{"read", "fs_read", `{"path":"a.txt"}`, task.ReadOnly, neumann.VerdictAllow, neumann.VerdictAllow, neumann.VerdictAllow},
		{"go test", "go_test", `{"dir":"."}`, task.Idempotent, neumann.VerdictAllow, neumann.VerdictAllow, neumann.VerdictAllow},
		{"write in out/", "fs_write", `{"path":"out/r.md","content":"x"}`, task.SideEffect, neumann.VerdictAllow, neumann.VerdictAllow, neumann.VerdictAllow},
		{"edit outside", "fs_edit", `{"path":"a.txt","old_string":"hello","new_string":"bye"}`, task.SideEffect, neumann.VerdictRequireApproval, neumann.VerdictAllow, neumann.VerdictDeny},
		{"delete outside", "fs_delete", `{"path":"a.txt"}`, task.SideEffect, neumann.VerdictRequireApproval, neumann.VerdictAllow, neumann.VerdictDeny},
		{"allowlisted", "sh_run", `{"args":["ls","-la"]}`, task.SideEffect, neumann.VerdictAllow, neumann.VerdictAllow, neumann.VerdictAllow},
		{"rm", "sh_run", `{"args":["rm","a.txt"]}`, task.SideEffect, neumann.VerdictRequireApproval, neumann.VerdictAllow, neumann.VerdictDeny},
		{"pipe", "sh_run", `{"script":"cat a.txt | wc -l"}`, task.SideEffect, neumann.VerdictRequireApproval, neumann.VerdictAllow, neumann.VerdictDeny},
		{"web", "web_fetch", `{"url":"https://go.dev"}`, task.ReadOnly, neumann.VerdictAllow, neumann.VerdictAllow, neumann.VerdictAllow},
		{"other device", "mcp_deploy", `{}`, task.SideEffect, neumann.VerdictRequireApproval, neumann.VerdictAllow, neumann.VerdictDeny},
	}
	for _, mode := range []string{ApproveAsk, ApproveAuto, ApproveNever} {
		a, _ := codingApp(t, mode, nil)
		for _, c := range calls {
			want := map[string]neumann.Verdict{ApproveAsk: c.ask, ApproveAuto: c.auto, ApproveNever: c.never}[mode]
			if d := a.policy(context.Background(), info(c.tool, c.args, c.effect)); d.Verdict != want {
				t.Errorf("%s, %s: %v %q; want %v", mode, c.name, d.Verdict, d.Reason, want)
			}
		}
		a.Store.Close()
	}

	a, _ := codingApp(t, ApproveAsk, nil)
	defer a.Store.Close()
	if d := a.policy(context.Background(), info("sh_run", `{"args":["rm","a.txt"]}`, task.SideEffect)); d.Reason != "$ rm a.txt" {
		t.Errorf("held reason %q", d.Reason)
	}
	a.AllowAlways("sh_run")
	if d := a.policy(context.Background(), info("sh_run", `{"args":["rm","a.txt"]}`, task.SideEffect)); d.Verdict != neumann.VerdictAllow {
		t.Errorf("after always: %v", d.Verdict)
	}
	if d := a.policy(context.Background(), info("fs_edit", `{"path":"a.txt","old_string":"h","new_string":"j"}`, task.SideEffect)); d.Verdict != neumann.VerdictRequireApproval {
		t.Errorf("always sh_run let fs_edit through: %v", d.Verdict)
	}
	a.Cfg.Coding.NoPrompt = "stdin is not a terminal"
	if d := a.policy(context.Background(), info("fs_edit", `{"path":"a.txt","old_string":"h","new_string":"j"}`, task.SideEffect)); d.Verdict != neumann.VerdictDeny || !strings.Contains(d.Reason, "not a terminal") {
		t.Errorf("no prompt: %v %q", d.Verdict, d.Reason)
	}
}

func TestCodingKeysStayOutOfTheShell(t *testing.T) {
	t.Setenv("GW_KEY", "x")
	a, _ := codingApp(t, ApproveAuto, nil)
	defer a.Store.Close()
	a.Cfg.KeyEnv = "GW_KEY"
	keys := a.Cfg.KeyEnvs()
	for _, k := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "NEU_API_KEY", "GW_KEY"} {
		found := false
		for _, have := range keys {
			found = found || have == k
		}
		if !found {
			t.Errorf("%s not denied: %v", k, keys)
		}
	}
	if (&Coding{On: true, Approve: "maybe"}).check() == nil {
		t.Error("an unknown -approve is accepted")
	}
	if k := (Coding{On: true, Approve: "read-only"}); k.check() != nil || k.Approve != ApproveNever {
		t.Errorf("read-only → %q", k.Approve)
	}
}

// In ask mode a held fs_edit waits in Waiting(Approval) until Approve, then
// runs; a denied one reaches the model as an error result.
func TestHeldEditRunsAfterApproval(t *testing.T) {
	var step atomic.Int32
	var lastTool atomic.Value
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		if s := lastToolText(req); s != "" {
			lastTool.Store(s)
		}
		switch step.Add(1) {
		case 1:
			return toolCall("fs_edit", map[string]any{"path": "a.txt", "old_string": "hello", "new_string": "bye"}), nil
		case 2:
			return toolCall("fs_delete", map[string]any{"path": "a.txt"}), nil
		}
		return sim.Reply("done"), nil
	}}
	a, root := codingApp(t, ApproveAsk, m)
	ctx := context.Background()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	head, _ := observe.Head(ctx, a.M)
	tr := a.Track(ctx, head)
	held := make(chan Held, 4)
	tr.Subscribe(func(ev proto.Event) {
		if h, ok := HeldFrom(ev); ok {
			held <- h
		}
	})
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: "t1", Input: "edit"}); err != nil {
		t.Fatal(err)
	}
	h := <-held
	if h.Tool != "fs_edit" || h.Reason != "fs_edit a.txt" {
		t.Fatalf("held %+v", h)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "a.txt")); string(b) != "hello world\n" {
		t.Fatal("the edit ran before its approval")
	}
	now, err := a.HeldNow(ctx)
	if err != nil || len(now) != 1 || now[0].Call != h.Call {
		t.Fatalf("HeldNow %+v %v", now, err)
	}
	if err := a.M.Approve(ctx, h.Task, h.Call, neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}); err != nil {
		t.Fatal(err)
	}
	h = <-held
	if h.Tool != "fs_delete" {
		t.Fatalf("held %+v", h)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "a.txt")); string(b) != "bye world\n" {
		t.Fatalf("after approval: %q", b)
	}
	if err := a.M.Approve(ctx, h.Task, h.Call, neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "the user denied this call: keep it"}); err != nil {
		t.Fatal(err)
	}
	ev, _ := tr.Wait(ctx, "t1")
	if ev.Kind != proto.EvDone {
		t.Fatalf("ended %s", ev.Kind)
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); err != nil {
		t.Fatal("a denied delete ran")
	}
	if s, _ := lastTool.Load().(string); !strings.Contains(s, "keep it") {
		t.Fatalf("the model was not told: %q", s)
	}
}

func toolCall(name string, args any) model.Response {
	b, _ := json.Marshal(args)
	msg := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, ProviderCallID: "p-" + name, Name: name, Args: b}}}
	return model.Response{Message: msg, FinishReason: "tool_calls"}
}

func lastToolText(req model.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == task.RoleTool {
			var b strings.Builder
			for _, p := range req.Messages[i].Parts {
				b.WriteString(p.Text)
			}
			return b.String()
		}
	}
	return ""
}
