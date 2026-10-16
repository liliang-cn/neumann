// Package local is design §17 phase 8's live check on a local backend: a
// background task is preempted while its model is still thinking, an
// interactive task runs, and the background task's next call goes on
// thinking from where it was cut off (design §6.4), ends the thought and
// answers.
//
// Skipped unless NEUMANN_ACCEPT_LIVE=1. The backend is Ollama's
// OpenAI-compatible endpoint, NEUMANN_LOCAL_URL (default
// http://127.0.0.1:11434/v1), and the model NEUMANN_LOCAL_MODEL (default
// qwen3.5:9b-mlx), configured as cmd/neumann's -local does.
//
//	NEUMANN_ACCEPT_LIVE=1 go test -v -count=1 -timeout 30m ./acceptance/local
package local

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func TestPreemptedWhileThinkingIsContinued(t *testing.T) {
	if os.Getenv("NEUMANN_ACCEPT_LIVE") != "1" {
		t.Skip("set NEUMANN_ACCEPT_LIVE=1 to run against a local backend")
	}
	a := openai.New(env("NEUMANN_LOCAL_URL", "http://127.0.0.1:11434/v1"), "ollama", env("NEUMANN_LOCAL_MODEL", "qwen3.5:9b-mlx"))
	a.Parallel, a.Prefill, a.ThinkOff = 1, true, "none"
	a.ThinkTags = [2]string{"<think>", "</think>"}
	a.Timeout, a.FirstByteTimeout, a.IdleTimeout = 0, 10*time.Minute, 2*time.Minute
	a.MaxTokens = 4096
	m, err := neumann.New(neumann.Config{Model: a, Cores: 1, Preempt: true, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer m.Stop(context.Background())
	deltas := m.Deltas(ctx)

	begin := time.Now()
	bg, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, TaskID: "bg", Priority: 1,
		Input: "What is 17 * 23? Work it out, then answer with just the number."})
	if err != nil {
		t.Fatal(err)
	}
	// Wait until bg's model has been thinking for a while, with no text yet.
	thought := 0
	for thought < 40 {
		select {
		case d := <-deltas:
			if d.TaskID != bg {
				continue
			}
			if d.Text != "" {
				t.Fatalf("bg's answer began after %d pieces of thinking: the preemption would not land in the thinking", thought)
			}
			if d.Reasoning != "" {
				thought++
			}
		case <-ctx.Done():
			t.Fatal("bg never thought")
		}
	}
	t.Logf("bg has thought %d pieces after %v; submitting fg", thought, time.Since(begin))
	fg, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, TaskID: "fg", Priority: 0, Input: "Say hello in one word."})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range deltas {
		}
	}()
	done := map[string]*task.TCB{}
	for ev := range m.Events(ctx, 0) {
		if ev.Terminal() {
			tc, err := m.Load(ctx, ev.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			done[ev.TaskID] = tc
			t.Logf("%s %s after %v: %q", ev.TaskID, tc.State, time.Since(begin), tc.LastAnswer())
		}
		if len(done) == 2 {
			break
		}
	}
	if len(done) != 2 {
		t.Fatalf("finished: %v", done)
	}
	var preempted, steps []map[string]any
	all, _ := m.EventLog(ctx, 0, 0)
	for _, ev := range all {
		var d map[string]any
		json.Unmarshal(ev.Data, &d)
		switch {
		case ev.TaskID == bg && ev.Kind == proto.EvPreempted:
			preempted = append(preempted, d)
		case ev.TaskID == bg && ev.Kind == proto.EvStep:
			steps = append(steps, d)
		}
	}
	t.Logf("bg preempted %v; steps %v", preempted, steps)
	if len(preempted) == 0 || preempted[0]["by"] != fg || preempted[0]["kept_think"].(float64) == 0 || preempted[0]["kept"].(float64) != 0 {
		t.Fatalf("bg was not preempted while thinking: %v", preempted)
	}
	if steps[1]["prefill_think"] == nil {
		t.Fatalf("bg's next call did not go on thinking: %v", steps)
	}
	b := done[bg]
	if b.State != task.Done || !strings.Contains(b.LastAnswer(), "391") {
		t.Fatalf("bg ended %s with %q", b.State, b.LastAnswer())
	}
	last := b.Regs[len(b.Regs)-1]
	if len(last.Parts) == 0 || last.Parts[0].Kind != task.PartReasoning || len(last.Parts[0].Text) == 0 {
		t.Fatalf("bg's answer has no reasoning: %+v", last.Parts)
	}
	reasoning := last.Parts[0].Text
	t.Logf("bg's reasoning, %d characters: %.300q…", len([]rune(reasoning)), reasoning)
	// Continued, not started over: the thinking opens once.
	if opening := firstLine(reasoning); opening != "" && strings.Count(reasoning, opening) > 1 {
		t.Fatalf("bg's thinking started over: %q appears %d times", opening, strings.Count(reasoning, opening))
	}
	if f := done[fg]; f.State != task.Done || f.LastAnswer() == "" {
		t.Fatalf("fg ended %s with %q", f.State, f.LastAnswer())
	}
}

// firstLine is s's first non-empty line, if it is long enough to say a
// thought opened with it.
func firstLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); len(l) >= 12 {
			return l
		}
	}
	return ""
}
