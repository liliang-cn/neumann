package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func TestUsageLine(t *testing.T) {
	for _, tc := range []struct {
		u    task.Usage
		n    int
		took time.Duration
		want string
	}{
		{task.Usage{Steps: 7, PromptTokens: 41234, CachedTokens: 34300, CompletionTokens: 1100}, 0, 12345 * time.Millisecond,
			"[t1] 7 calls, 41.2k in (83% cached), 1.1k out, 12.3s"},
		{task.Usage{Steps: 1, PromptTokens: 950, CompletionTokens: 12}, 0, 0,
			"[t1] 1 call, 950 in, 12 out"},
		{task.Usage{Steps: 30, PromptTokens: 1_304_000, CompletionTokens: 9000, ReasoningTokens: 4200}, 2, 90 * time.Second,
			"[t1] 30 calls, 1.30M in, 9.0k out (4.2k reasoning), 1m30s, compacted 2×"},
	} {
		if got := usageLine("t1", tc.u, tc.n, tc.took); got != tc.want {
			t.Errorf("got  %q\nwant %q", got, tc.want)
		}
	}
}

func usageEv(id string, u task.Usage) proto.Event {
	b, _ := json.Marshal(u)
	return proto.Event{Kind: proto.EvUsage, TaskID: id, Data: b}
}

// The session's totals are the sum of every usage event, running tasks
// included.
func TestTally(t *testing.T) {
	s := newTally()
	s.add(usageEv("a", task.Usage{PromptTokens: 1000, CachedTokens: 500, CompletionTokens: 10}))
	s.add(usageEv("a", task.Usage{PromptTokens: 1000, CachedTokens: 900, CompletionTokens: 10}))
	s.add(proto.Event{Kind: proto.EvCompacted, TaskID: "a"})
	s.add(proto.Event{Kind: proto.EvDone, TaskID: "a"})
	s.add(usageEv("b", task.Usage{PromptTokens: 2000, CompletionTokens: 30, ReasoningTokens: 20}))
	s.add(proto.Event{Kind: proto.EvStep, TaskID: "b"})
	want := "[session] 3 calls, 4.0k in (35% cached), 50 out (20 reasoning), compacted 1×; 2 tasks made calls, 1 ended"
	if got := s.String(); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

// bigLog writes a log of n lines under root.
func bigLog(t *testing.T, root string, n int) {
	t.Helper()
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "2026-10-27T10:%02d:%02d level=info svc=api req=%06d path=/v1/items status=200 took=%dms\n", i/60%60, i%60, i, i%97)
	}
	if err := os.WriteFile(filepath.Join(root, "big.log"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// readPage is a turn calling fs_read on page i (50 lines) of big.log.
func readPage(i int) model.Response {
	args, _ := json.Marshal(map[string]any{"path": "big.log", "offset_line": i*50 + 1, "limit_lines": 50})
	return model.Response{
		Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{{
			Kind: task.PartToolCall, ProviderCallID: fmt.Sprintf("p-%d", i), Name: "fs_read", Args: args,
		}}},
		FinishReason: "tool_calls",
		Usage:        task.Usage{PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 20},
	}
}

// A neu task on a model whose window is small reads a big file in pages:
// the machine compacts it before its request outgrows the window, every
// request stays inside it, and the usage line on stderr says so.
func TestOneShotCompactsOnASmallWindow(t *testing.T) {
	const window, pages = 6000, 8
	root := t.TempDir()
	bigLog(t, root, pages*50)
	var maxEst int64
	m := &sim.Model{
		Info: &model.Caps{Window: window, MaxOut: 256, Tools: true},
		Fn: func(req model.Request) (model.Response, error) {
			if est := task.EstimateTokens(req.System, req.Tools, req.Messages); est > maxEst {
				maxEst = est
			}
			if sim.IsCompaction(req) {
				r := sim.Summarize(req)
				r.Usage = task.Usage{PromptTokens: 3000, CompletionTokens: 40}
				return r, nil
			}
			if n := sim.Rounds(req); n < pages {
				return readPage(n), nil
			}
			r := sim.Reply("the log has 400 lines")
			r.Usage = task.Usage{PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 20}
			return r, nil
		},
	}
	cfg := app.Config{Backend: "local", Model: "m", Root: root, Writable: "out", MaxSteps: 40, Parallel: 1, Quiet: true,
		Store: filepath.Join(root, ".neu", "state.db")}
	ctx := context.Background()
	a, err := app.Build(ctx, cfg, app.Options{Adapter: m})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	head, err := observe.Head(ctx, a.M)
	if err != nil {
		t.Fatal(err)
	}
	tr := a.Track(ctx, head)
	var stdout, stderr bytes.Buffer
	con := newConsole(&stdout, &stderr, false)
	con.watch(ctx, a.M) // the answer streams, and is not printed again at the end
	if code := oneShot(ctx, a, tr, con, "How many lines does big.log have?", newApprover(a, con, nil), &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\nstdout %s\nstderr %s", code, stdout.String(), stderr.String())
	}
	if n := strings.Count(stdout.String(), "400 lines"); n != 1 {
		t.Fatalf("answer printed %d times: %q", n, stdout.String())
	}
	if maxEst > window {
		t.Errorf("a request of ~%d tokens went to a %d-token window", maxEst, window)
	}
	out := stderr.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	last := lines[len(lines)-1]
	if !strings.Contains(out, "] compacted ") {
		t.Fatalf("no compaction in stderr:\n%s", out)
	}
	if !regexp.MustCompile(`^\[t[0-9a-f]+\] \d+ calls, [0-9.]+k in \(\d+% cached\), \d+ out, [0-9.]+m?s, compacted \d+×$`).MatchString(last) {
		t.Fatalf("usage line %q", last)
	}
	t.Logf("largest request ~%d tokens; stderr:\n%s", maxEst, out)
}
