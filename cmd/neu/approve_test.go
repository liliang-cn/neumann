package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// calls is a model turn with these tool calls, in one round.
func calls(cs ...[2]any) model.Response {
	msg := task.Message{Role: task.RoleAssistant}
	for i, c := range cs {
		b, _ := json.Marshal(c[1])
		msg.Parts = append(msg.Parts, task.Part{Kind: task.PartToolCall, ProviderCallID: "p" + string(rune('a'+i)), Name: c[0].(string), Args: b})
	}
	return model.Response{Message: msg, FinishReason: "tool_calls"}
}

func toolTexts(req model.Request) string {
	var b strings.Builder
	for _, m := range req.Messages {
		if m.Role == task.RoleTool {
			for _, p := range m.Parts {
				b.WriteString(p.Text + "\n")
			}
		}
	}
	return b.String()
}

// syncBuf is a bytes.Buffer safe for the console and the test at once.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startCoding(t *testing.T, mode string, m model.Adapter, tune func(*app.Config)) (*app.App, *app.Tracker, string) {
	t.Helper()
	root := t.TempDir()
	for _, f := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("hello "+f+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := app.Config{Backend: "local", Model: "m", Parallel: 1, Root: root, Writable: "out", MaxSteps: 10, Quiet: true,
		Store: filepath.Join(root, ".neu", "state.db"), Coding: app.Coding{On: true, Approve: mode, ShScript: true}}
	if tune != nil {
		tune(&cfg)
	}
	ctx := context.Background()
	a, err := app.Build(ctx, cfg, app.Options{Adapter: m})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(5 * time.Second) })
	head, err := observe.Head(ctx, a.M)
	if err != nil {
		t.Fatal(err)
	}
	return a, a.Track(ctx, head), root
}

// The ask flow on a scripted terminal: two calls held in one round are
// asked one after the other (y, then n with a reason), and "a" allows a
// tool for the rest of the session, so its next call is not asked.
func TestAskFlowYesNoAlways(t *testing.T) {
	var step atomic.Int32
	var seen atomic.Value
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		seen.Store(toolTexts(req))
		switch step.Add(1) {
		case 1:
			return calls([2]any{"fs_edit", map[string]any{"path": "a.txt", "old_string": "hello", "new_string": "bye"}},
				[2]any{"fs_write", map[string]any{"path": "b.txt", "content": "replaced\n"}}), nil
		case 2:
			return calls([2]any{"sh_run", map[string]any{"args": []string{"touch", "made1"}}}), nil
		case 3:
			return calls([2]any{"sh_run", map[string]any{"args": []string{"touch", "made2"}}}), nil
		}
		return sim.Reply("done"), nil
	}}
	a, tr, root := startCoding(t, app.ApproveAsk, m, nil)
	var out, errw syncBuf
	con := newConsole(&out, &errw, false)
	appr := newApprover(a, con, readerLines(strings.NewReader("y\nmaybe\nn not that file\na\n")))
	ctx := context.Background()
	if code := oneShot(ctx, a, tr, con, nil, "change things", appr, &out, &errw); code != 0 {
		t.Fatalf("exit %d\n%s", code, errw.String())
	}
	if b, _ := os.ReadFile(filepath.Join(root, "a.txt")); string(b) != "bye a.txt\n" {
		t.Errorf("a.txt %q: the approved edit did not run", b)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "b.txt")); string(b) != "hello b.txt\n" {
		t.Errorf("b.txt %q: the denied write ran", b)
	}
	for _, f := range []string{"made1", "made2"} {
		if _, err := os.Stat(filepath.Join(root, f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	e := errw.String()
	if n := strings.Count(e, "needs approval"); n != 3 {
		t.Errorf("%d prompts, want 3 (made2 is allowed by a):\n%s", n, e)
	}
	first, second := strings.Index(e, "needs approval: fs_edit a.txt"), strings.Index(e, "needs approval: fs_write b.txt")
	if first < 0 || second < first || !strings.Contains(e[first:second], "✓ allowed") {
		t.Errorf("the round's two calls were not asked one at a time:\n%s", e)
	}
	if !strings.Contains(e, "+bye a.txt") || !strings.Contains(e, "$ touch made1") || !strings.Contains(e, "answer y, n") || !strings.Contains(e, "every sh_run from now on") {
		t.Errorf("prompt text:\n%s", e)
	}
	if s, _ := seen.Load().(string); !strings.Contains(s, "the user denied this call: not that file") {
		t.Errorf("the model was not told of the denial: %q", s)
	}
}

// A one-shot run without a terminal cannot ask: a call that would be held
// is denied at once, and the model hears that -approve auto allows it.
func TestOneShotWithoutTerminalDenies(t *testing.T) {
	var step atomic.Int32
	var seen atomic.Value
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		seen.Store(toolTexts(req))
		if step.Add(1) == 1 {
			return calls([2]any{"fs_delete", map[string]any{"path": "a.txt"}}), nil
		}
		return sim.Reply("could not"), nil
	}}
	a, tr, root := startCoding(t, app.ApproveAsk, m, func(c *app.Config) { c.Coding.NoPrompt = noPrompt })
	var out, errw syncBuf
	con := newConsole(&out, &errw, false)
	if code := oneShot(context.Background(), a, tr, con, nil, "delete a.txt", newApprover(a, con, nil), &out, &errw); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); err != nil {
		t.Fatal("deleted without approval")
	}
	if s, _ := seen.Load().(string); !strings.Contains(s, "-approve auto") || !strings.Contains(s, "not a terminal") {
		t.Errorf("model told %q", s)
	}
	if !strings.Contains(errw.String(), "✗ fs_delete refused") {
		t.Errorf("stderr:\n%s", errw.String())
	}
	if stdinIsTerminal(strings.NewReader("")) {
		t.Error("a strings.Reader is a terminal")
	}
}

// A held call left by an earlier run in ask mode is decided by the mode
// of this one: -approve auto lets it run.
func TestAutoAnswersACallHeldBefore(t *testing.T) {
	m := &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		if sim.Rounds(req) == 0 {
			return calls([2]any{"fs_edit", map[string]any{"path": "a.txt", "old_string": "hello", "new_string": "bye"}}), nil
		}
		return sim.Reply("done"), nil
	}}
	a, tr, root := startCoding(t, app.ApproveAsk, m, nil)
	held := make(chan struct{}, 1)
	tr.Subscribe(func(ev proto.Event) {
		if ev.Kind == proto.EvApprovalNeeded {
			held <- struct{}{}
		}
	})
	ctx := context.Background()
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: "t1", Input: "edit"}); err != nil {
		t.Fatal(err)
	}
	<-held
	a.Cfg.Coding.Approve = app.ApproveAuto // as if restarted with -approve auto
	var errw syncBuf
	con := newConsole(io.Discard, &errw, false)
	newApprover(a, con, nil).start(ctx, tr, nil) // finds it with HeldNow
	if ev, _ := tr.Wait(ctx, "t1"); ev.Kind != proto.EvDone {
		t.Fatalf("ended %s", ev.Kind)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "a.txt")); string(b) != "bye a.txt\n" || !strings.Contains(errw.String(), "-approve auto") {
		t.Fatalf("a.txt %q; stderr %s", b, errw.String())
	}
}

func TestParseAnswer(t *testing.T) {
	for in, want := range map[string]string{"y": "allow", "YES": "allow", " a ": "allow+always", "always": "allow+always", "n": "deny:the user denied this call", "no use go_test": "deny:the user denied this call: use go_test"} {
		d, always, ok := parseAnswer(in)
		got := d.Verdict.String()
		if always {
			got += "+always"
		}
		if d.Reason != "" {
			got += ":" + d.Reason
		}
		if !ok || got != want {
			t.Errorf("%q → %s (%v); want %s", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "maybe", "e"} {
		if _, _, ok := parseAnswer(in); ok {
			t.Errorf("%q accepted", in)
		}
	}
}

// While a prompt is open nothing else is printed; what came meanwhile
// follows, in order, once it is answered.
func TestConsoleHoldsOutputDuringPrompt(t *testing.T) {
	var out, errw bytes.Buffer
	c := newConsole(&out, &errw, false)
	c.setFocus("a")
	c.delta(proto.Delta{TaskID: "a", Text: "partial"})
	release := c.hold()
	c.prompt("[b] needs approval: x\n  allow? ")
	c.delta(proto.Delta{TaskID: "a", Text: " answer"})
	c.line(&errw, "[c] → fs_read")
	if out.String() != "partial\n" || errw.String() != "[b] needs approval: x\n  allow? " {
		t.Fatalf("printed during the prompt: %q / %q", out.String(), errw.String())
	}
	release()
	if out.String() != "partial\n answer\n" || !strings.HasSuffix(errw.String(), "allow? [c] → fs_read\n") {
		t.Fatalf("after release: %q / %q", out.String(), errw.String())
	}
}

// The REPL's lines go to the REPL, except the one typed while a prompt
// waits for its answer.
func TestLineSourceRoutesAnswers(t *testing.T) {
	pr, pw := io.Pipe()
	s := readLines(pr)
	go io.WriteString(pw, "task one\n")
	if l := <-s.lines; l != "task one" {
		t.Fatalf("REPL got %q", l)
	}
	got := make(chan string)
	go func() {
		l, _ := s.answer(context.Background())
		got <- l
	}()
	for {
		s.mu.Lock()
		w := s.want
		s.mu.Unlock()
		if w != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	io.WriteString(pw, "y\n")
	if l := <-got; l != "y" {
		t.Fatalf("answer %q", l)
	}
	go io.WriteString(pw, "task two\n")
	if l := <-s.lines; l != "task two" {
		t.Fatalf("REPL got %q", l)
	}
	pw.Close()
	if _, err := s.answer(context.Background()); err != io.EOF {
		t.Fatalf("answer at EOF: %v", err)
	}
}
