package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// helperEnv makes the test binary a stdio MCP server instead of running
// tests (the os.Args[0] helper pattern of bus/mcp's tests).
const helperEnv = "NEU_APP_MCP_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "serve" {
		fakeServer().Run(context.Background(), &sdk.StdioTransport{})
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeServer has a tool of each annotation, one that reports the
// environment it was started with, one that reports its pid, and eval,
// which collides with calc_eval when the server is called calc.
func fakeServer() *sdk.Server {
	s := sdk.NewServer(&sdk.Implementation{Name: "fake", Version: "v0"}, nil)
	obj := json.RawMessage(`{"type":"object"}`)
	text := func(s string) *sdk.CallToolResult {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: s}}}
	}
	add := func(name string, ann *sdk.ToolAnnotations, f func() string) {
		s.AddTool(&sdk.Tool{Name: name, Description: "tool " + name, Annotations: ann, InputSchema: obj},
			func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) { return text(f()), nil })
	}
	add("lookup", &sdk.ToolAnnotations{ReadOnlyHint: true}, func() string { return "found" })
	add("touch", &sdk.ToolAnnotations{IdempotentHint: true}, func() string { return "touched" })
	add("save", nil, func() string { return "saved" })
	add("eval", nil, func() string { return "42" })
	add("env", nil, func() string {
		return fmt.Sprintf("OPENAI_API_KEY=%q MY_KEY=%q MY_TOKEN=%q", os.Getenv("OPENAI_API_KEY"), os.Getenv("MY_KEY"), os.Getenv("MY_TOKEN"))
	})
	add("pid", nil, func() string { return fmt.Sprint(os.Getpid()) })
	return s
}

// fake is an [[mcp]] entry that starts the fake server.
func fake(name string, env map[string]string) MCPServer {
	e := map[string]string{helperEnv: "serve"}
	for k, v := range env {
		e[k] = v
	}
	return MCPServer{Name: name, Command: os.Args[0], Args: []string{"-test.run=^$"}, Env: e}
}

func scriptedConfig(t *testing.T) Config {
	dir := t.TempDir()
	return Config{Backend: "cloud", Model: "m", KeyEnv: "MY_KEY", Cores: 4, Root: dir, Writable: "out", MaxSteps: 20, Quiet: true,
		Store: filepath.Join(dir, ".neu", "state.db")}
}

// warnings collects Extras.Warnf's lines.
type warnings struct {
	mu    sync.Mutex
	lines []string
}

func (w *warnings) f(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lines = append(w.lines, fmt.Sprintf(format, args...))
}

func (w *warnings) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.lines...)
}

// call is a model turn calling tool with args.
func call(tool, args string) model.Response {
	return model.Response{FinishReason: "tool_calls", Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
		{Kind: task.PartToolCall, ProviderCallID: "p-" + tool, Name: tool, Args: json.RawMessage(args)},
	}}}
}

// lastResult is the text of the last tool result in req.
func lastResult(req model.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		for _, p := range req.Messages[i].Parts {
			if p.Kind == task.PartToolResult {
				return p.Text
			}
		}
	}
	return ""
}

func run(t *testing.T, a *App, id, input string) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	evs := a.M.Events(ctx, 0)
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: id, Input: input}); err != nil {
		t.Fatal(err)
	}
	for ev := range evs {
		if ev.TaskID == id && ev.Terminal() {
			tc, err := a.M.Load(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			return tc
		}
	}
	t.Fatalf("task %s did not finish", id)
	return nil
}

func TestMCPServerCheck(t *testing.T) {
	ok := MCPServer{Name: "time-1", Command: "uvx"}
	if err := ok.Check(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		s    MCPServer
		want string
	}{
		{MCPServer{Name: "a b", Command: "x"}, "server name"},
		{MCPServer{Name: "", Command: "x"}, "server name"},
		{MCPServer{Name: "a"}, "needs command"},
		{MCPServer{Name: "a", Command: "x", URL: "http://h"}, "exclusive"},
		{MCPServer{Name: "a", URL: "http://h", Args: []string{"-v"}}, "args and env"},
		{MCPServer{Name: "a", Command: "x", Headers: map[string]string{"A": "b"}}, "headers apply"},
		{MCPServer{Name: "a", Command: "x", Effect: "harmless"}, "effect \"harmless\""},
		{MCPServer{Name: "a", Command: "x", ReadOnly: []string{"t"}, SideEffect: []string{"t"}}, "in both"},
		{MCPServer{Name: "a", Command: "x", StartupTimeout: "soon"}, "startup_timeout"},
	} {
		if err := tc.s.Check(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: %v, want %q", tc.s, err, tc.want)
		}
	}
}

// MCP servers start with neu, their tools are <server>_<tool> with the
// effects the entry gives them (SideEffect unless it says otherwise), a
// server that does not start or collides is one warning, the backend's
// keys do not reach a server that does not name them, and the servers
// stop with neu.
func TestMCPServersAttach(t *testing.T) {
	t.Setenv("MY_KEY", "backend-secret")
	t.Setenv("OPENAI_API_KEY", "openai-secret")
	t.Setenv("NEU_TEST_TOKEN", "tok")
	off := false
	plain := fake("fake", map[string]string{"MY_TOKEN": "${NEU_TEST_TOKEN}"})
	plain.ReadOnly = []string{"lookup", "nosuch"}
	ann := fake("ann", map[string]string{"OPENAI_API_KEY": "$OPENAI_API_KEY"})
	ann.Effect = "annotations"
	ann.SideEffect = []string{"lookup"}
	servers := []MCPServer{
		plain, ann,
		{Name: "broken", Command: filepath.Join(t.TempDir(), "no-such-server")},
		{Name: "off", Command: "/nonexistent", Enabled: &off},
		fake("calc", nil), // calc_eval is taken
	}
	var w warnings
	pid := ""
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		switch sim.ToolRounds(req) {
		case 0:
			return call(sim.FirstUser(req)+"_env", `{}`), nil
		case 1:
			env := lastResult(req)
			return call(sim.FirstUser(req)+"_pid", `{"env":`+fmt.Sprintf("%q", env)+`}`), nil
		}
		pid = lastResult(req)
		var env string
		for _, msg := range req.Messages {
			for _, p := range msg.Parts {
				if p.Kind == task.PartToolResult && strings.HasPrefix(p.Text, "OPENAI") {
					env = p.Text
				}
			}
		}
		return sim.Reply(env), nil
	}}
	ctx := context.Background()
	a, err := Build(ctx, scriptedConfig(t), Options{Adapter: m, Extras: Extras{MCP: servers, Warnf: w.f}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			a.Close(5 * time.Second)
		}
	}()

	warns := w.all()
	slices.Sort(warns)
	if len(warns) != 3 || !strings.HasPrefix(warns[0], "mcp broken: ") || !strings.HasSuffix(warns[0], "; continuing without it") ||
		!strings.HasPrefix(warns[1], "mcp calc: tool calc_eval is already served by device calc") ||
		warns[2] != "mcp fake: no tool named nosuch (in its effect lists); ignored" {
		t.Fatalf("warnings:\n%s", strings.Join(warns, "\n"))
	}
	if got := strings.Join(a.MCPServers(), ","); got != "fake,ann" {
		t.Fatalf("servers %s", got)
	}
	want := map[string]bus.Effect{
		"fake_lookup": bus.ReadOnly, "fake_touch": bus.SideEffect, "fake_save": bus.SideEffect, "fake_env": bus.SideEffect,
		"ann_lookup": bus.SideEffect, "ann_touch": bus.Idempotent, "ann_save": bus.SideEffect,
		"fs_read": bus.ReadOnly, "calc_eval": bus.ReadOnly,
	}
	got := map[string]ToolInfo{}
	for _, ti := range a.ToolList() {
		got[ti.Name] = ti
	}
	for name, e := range want {
		ti, ok := got[name]
		if !ok || ti.Effect != e {
			t.Errorf("%s: %+v, want effect %s", name, ti, e)
		}
	}
	if ti := got["fake_lookup"]; ti.Device != "fake" || ti.Source != "mcp" {
		t.Errorf("fake_lookup %+v", ti)
	}
	if ti := got["fs_read"]; ti.Device != "fs" || ti.Source != "builtin" {
		t.Errorf("fs_read %+v", ti)
	}

	tc := run(t, a, "t1", "fake")
	if ans := tc.LastAnswer(); ans != `OPENAI_API_KEY="" MY_KEY="" MY_TOKEN="tok"` {
		t.Fatalf("fake's environment: %s (%s)", ans, tc.State)
	}
	if ans := run(t, a, "t2", "ann").LastAnswer(); ans != `OPENAI_API_KEY="openai-secret" MY_KEY="" MY_TOKEN=""` {
		t.Fatalf("ann's environment: %s", ans)
	}
	var p int
	if _, err := fmt.Sscan(pid, &p); err != nil || p <= 0 {
		t.Fatalf("pid %q", pid)
	}
	if err := a.Close(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	closed = true
	for deadline := time.Now().Add(5 * time.Second); syscall.Kill(p, 0) == nil; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("server %d still runs after Close", p)
		}
	}
}

func writeSkill(t *testing.T, root, dir, name, desc string) {
	t.Helper()
	p := filepath.Join(root, dir, "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf("---\nname: %s\ndescription: %s\n---\nInstructions of %s.\n", name, desc, desc)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Skills come from every directory, the first one winning a name; their
// catalogue is in the system prompt and skills_load returns one.
func TestSkillsFromSeveralDirs(t *testing.T) {
	project, user := t.TempDir(), t.TempDir()
	writeSkill(t, project, "release", "release", "project release")
	writeSkill(t, user, "release", "release", "user release")
	writeSkill(t, user, "review", "review", "user review")
	var system string
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		system = req.System
		if sim.ToolRounds(req) == 0 {
			return call("skills_load", `{"name":"release"}`), nil
		}
		return sim.Reply(lastResult(req)), nil
	}}
	ctx := context.Background()
	a, err := Build(ctx, scriptedConfig(t), Options{Adapter: m, Extras: Extras{SkillDirs: []string{project, user}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	var names []string
	for _, s := range a.Skills().Skills() {
		names = append(names, s.Name+"="+s.Description)
	}
	if got := strings.Join(names, ","); got != "release=project release,review=user review" {
		t.Fatalf("skills %s", got)
	}
	if p := a.Skills().Problems(); len(p) != 1 || !strings.Contains(p[0].Reason, "duplicate") {
		t.Fatalf("problems %v", p)
	}
	n := 0
	for _, ti := range a.ToolList() {
		if ti.Device == "skills" {
			n++
			if ti.Effect != bus.ReadOnly || ti.Source != "skills" {
				t.Errorf("%+v", ti)
			}
		}
	}
	if n != 3 {
		t.Fatalf("%d skills tools", n)
	}
	ans := run(t, a, "s1", "release please").LastAnswer()
	if !strings.Contains(ans, "Instructions of project release.") {
		t.Fatalf("skills_load: %s", ans)
	}
	if !strings.HasPrefix(system, System) || !strings.Contains(system, "- review: user review") {
		t.Fatalf("system prompt:\n%s", system)
	}
}

// No skill in the directories: no skills device, and the prompt is neu's.
func TestNoSkillsNoDevice(t *testing.T) {
	ctx := context.Background()
	a, err := Build(ctx, scriptedConfig(t), Options{Adapter: reply("x"), Extras: Extras{SkillDirs: []string{t.TempDir()}}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	for _, ti := range a.ToolList() {
		if ti.Device == "skills" {
			t.Fatalf("skills tool %s without skills", ti.Name)
		}
	}
	if a.Skills() == nil || len(a.Skills().Skills()) != 0 {
		t.Fatal("the empty device is kept for /skills")
	}
}

// One task sends another a message with tasks_send; it is delivered as a
// steer from task:<sender> while the other still works, and the other
// answers with it.
func TestTasksSendDeliversASteer(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	gate := builtin.New("gate").Add(bus.ToolDef{Name: "hold", Effect: bus.ReadOnly}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return "held", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		in := sim.FirstUser(req)
		switch {
		case in == "work" && sim.ToolRounds(req) == 0:
			return call("gate_hold", `{}`), nil
		case in == "work":
			for _, u := range sim.UserTexts(req) {
				if strings.Contains(u, "task:sender") {
					return sim.Reply("got: " + u), nil
				}
			}
			return sim.Reply("no message"), nil
		case in == "report" && sim.ToolRounds(req) == 0:
			return call("tasks_send", `{"task_id":"worker","text":"the count is 7"}`), nil
		}
		return sim.Reply(lastResult(req)), nil
	}}
	ctx := context.Background()
	a, err := Build(ctx, scriptedConfig(t), Options{Adapter: m, Extras: Extras{Messages: true, Devices: []bus.Device{gate}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	ectx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	evs := a.M.Events(ectx, 0)
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: "worker", Input: "work"}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if ans := run(t, a, "sender", "report").LastAnswer(); ans != "sent to task worker" {
		t.Fatalf("sender: %s", ans)
	}
	close(release)
	var from string
	for ev := range evs {
		if ev.TaskID != "worker" {
			continue
		}
		if ev.Kind == proto.EvSteerDelivered {
			from, _ = Field[string](ev, "from")
		}
		if ev.Terminal() {
			break
		}
	}
	if from != "task:sender" {
		t.Fatalf("steer_delivered from %q", from)
	}
	tc, err := a.M.Load(ctx, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if ans := tc.LastAnswer(); !strings.HasPrefix(ans, "got: ") || !strings.Contains(ans, "the count is 7") {
		t.Fatalf("worker: %s", ans)
	}
	var tools []string
	for _, ti := range a.ToolList() {
		if ti.Source == "tasks" {
			tools = append(tools, ti.Name+"="+ti.Effect.String())
		}
	}
	if got := strings.Join(tools, ","); got != "tasks_list=read_only,tasks_send=idempotent" {
		t.Fatalf("tasks tools %s", got)
	}
}
