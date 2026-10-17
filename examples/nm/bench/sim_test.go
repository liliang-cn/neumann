package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func toolCall(name string, args any) model.Response {
	b, _ := json.Marshal(args)
	msg := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, ProviderCallID: "p-" + name, Name: name, Args: b}}}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: task.Usage{PromptTokens: 100, CompletionTokens: 10}}
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

var fixDirRe = regexp.MustCompile(`fixbug/\S+`)

// script plays a competent model for two tasks: quick.lines (one fs_read,
// then the count it read) and fix-bug (read, write the fix, test, answer).
func script(req model.Request) (model.Response, error) {
	q := sim.FirstUser(req)
	round := sim.ToolRounds(req)
	switch {
	case strings.Contains(q, "How many lines"):
		if round == 0 {
			return toolCall("fs_read", map[string]any{"path": "data/sales.csv", "limit_lines": 1}), nil
		}
		m := regexp.MustCompile(`of (\d+)`).FindStringSubmatch(lastToolText(req))
		if m == nil {
			return sim.Reply("I could not tell."), nil
		}
		return sim.Reply("data/sales.csv has " + m[1] + " lines."), nil
	case strings.Contains(q, "failing tests"):
		dir := strings.TrimRight(fixDirRe.FindString(q), ".,")
		switch round {
		case 0:
			return toolCall("fs_read", map[string]any{"path": dir + "/window.go"}), nil
		case 1:
			src, _ := os.ReadFile(filepath.Join(simRoot, dir, "window.go"))
			fixed := strings.Replace(string(src), "i < len(xs)-1; i++", "i < len(xs); i++", 1)
			return toolCall("fs_write", map[string]any{"path": dir + "/window.go", "content": fixed}), nil
		case 2:
			return toolCall("go_test", map[string]any{"dir": dir}), nil
		}
		if strings.Contains(lastToolText(req), "PASS (exit status 0)") {
			return sim.Reply("The second loop stopped one value early (len(xs)-1); it now runs to len(xs). Tests pass."), nil
		}
		return sim.Reply("Tests still fail."), nil
	}
	return sim.Reply("?"), nil
}

var simRoot string

func TestSoloOnAScriptedModel(t *testing.T) {
	dir, _ := workspace(t)
	simRoot = dir
	work := filepath.Dir(dir)
	b := &Bench{
		Cfg:      app.Config{Backend: "local", Model: "sim", Cores: 2, MaxSteps: 10, Root: dir, Writable: "out,fixbug", Store: filepath.Join(work, ".nm", "sim.db"), Quiet: true},
		Opt:      Options{Scenario: "solo", RunID: fmt.Sprintf("s%d", time.Now().UnixNano()%1e6), Work: work, Timeout: time.Minute},
		Testdata: testdata,
		out:      io.Discard,
		errw:     io.Discard,
		adapter:  &sim.Model{Name: "scripted", Fn: script},
	}
	b.Facts, _ = Setup(dir, "../../..", testdata)
	var err error
	if b.tasks, err = Select("quick.lines,fix-bug"); err != nil {
		t.Fatal(err)
	}
	rep := &Report{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := b.solo(ctx, rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Results) != 2 {
		t.Fatalf("results: %d", len(rep.Results))
	}
	for _, r := range rep.Results {
		if !r.Pass {
			t.Errorf("%s failed: %s (answer %q)", r.ID, r.Detail, r.Answer)
		}
		if r.WallMS <= 0 || r.QueueMS < 0 || r.ModelMS < 0 || r.Outcome != "done" {
			t.Errorf("%s metrics: %+v", r.ID, r.Metrics)
		}
	}
	if os.Getenv("NM_DUMP") != "" {
		for _, ev := range b.lastSession.Tr.Events(rep.Results[1].ID) {
			t.Logf("%d %s %s %s", ev.Seq, ev.Time.Format("05.000000"), ev.Kind, ev.Data)
		}
	}
	q, f := rep.Results[0].Metrics, rep.Results[1].Metrics
	if q.ModelCalls != 2 || q.Steps != 2 || q.ToolCalls != 1 || q.ToolRounds != 1 || q.Usage.PromptTokens == 0 {
		t.Errorf("quick.lines counts: %+v", q)
	}
	if f.ModelCalls != 4 || f.ToolCalls != 3 || f.ToolRounds != 3 || f.DeviceMS <= 0 || f.ToolMS < f.DeviceMS/2 {
		t.Errorf("fix-bug counts: %+v", f)
	}
	if f.WallMS+0.5 < f.QueueMS+f.ModelMS+f.ToolMS {
		t.Errorf("fix-bug: parts exceed the whole: %+v", f)
	}
	var buf strings.Builder
	rep.Print(&buf)
	if !strings.Contains(buf.String(), "fix-bug.1") || !strings.Contains(buf.String(), "pass") {
		t.Errorf("table:\n%s", buf.String())
	}
}

// With -backend hybrid and -route static, each task runs in its kind's
// group, and the report accounts for each class: the cloud class's tokens
// are the run's cost.
func TestHybridSoloAccountsPerClass(t *testing.T) {
	dir, _ := workspace(t)
	simRoot = dir
	work := filepath.Dir(dir)
	t.Setenv("CPA_BASE_URL", "https://cloud.example/v1")
	t.Setenv("CPA_MODEL", "cloud-model")
	t.Setenv("CPA_API_KEY", "k")
	cfg := app.Config{Backend: "hybrid", Parallel: 1, Route: app.RouteStatic, Fallback: true, MaxSteps: 10, Root: dir, Writable: "out,fixbug",
		Store: filepath.Join(work, ".nm", "hybrid.db"), Quiet: true}
	if err := cfg.Resolve(); err != nil {
		t.Fatal(err)
	}
	strong := &sim.Model{Name: "sim:strong", Fn: func(req model.Request) (model.Response, error) {
		r := sim.Reply("West, 1.00")
		r.Usage = task.Usage{PromptTokens: 1000, CachedTokens: 400, CompletionTokens: 50}
		return r, nil
	}}
	b := &Bench{
		Cfg:      cfg,
		Opt:      Options{Scenario: "solo", RunID: fmt.Sprintf("h%d", time.Now().UnixNano()%1e6), Work: work, Timeout: time.Minute},
		Testdata: testdata,
		out:      io.Discard,
		errw:     io.Discard,
		adapters: map[string]model.Adapter{app.ClassFast: &sim.Model{Name: "sim:fast", Fn: script}, app.ClassStrong: strong},
	}
	b.Facts, _ = Setup(dir, "../../..", testdata)
	var err error
	if b.tasks, err = Select("quick.lines,csv-stats"); err != nil {
		t.Fatal(err)
	}
	rep := newReport(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := b.solo(ctx, rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Results) != 2 {
		t.Fatalf("results: %d", len(rep.Results))
	}
	q, c := rep.Results[0], rep.Results[1]
	if q.Class != app.ClassFast || q.FinalClass != app.ClassFast || q.Route.By != app.RouteStatic || !q.Pass {
		t.Errorf("quick.lines: class %s/%s route %+v pass %v (%s)", q.Class, q.FinalClass, q.Route, q.Pass, q.Detail)
	}
	if c.Class != app.ClassStrong || c.UsageByClass[app.ClassStrong].PromptTokens != 1000 || c.CallsByClass[app.ClassStrong] != 1 {
		t.Errorf("csv-stats: class %s usage %+v calls %v", c.Class, c.UsageByClass, c.CallsByClass)
	}
	cs := rep.Summary["classes"].(map[string]*ClassSummary)
	if f := cs[app.ClassFast]; f.Kind != "local" || f.Tasks != 1 || f.Passed != 1 || f.PromptTokens != q.Usage.PromptTokens || f.PromptTokens == 0 || f.ModelCalls != 2 {
		t.Errorf("fast: %+v", f)
	}
	if s := cs[app.ClassStrong]; s.Kind != "cloud" || s.Tasks != 1 || s.Passed != 0 || s.PromptTokens != 1000 || s.CachedTokens != 400 || s.CompletionTokens != 50 {
		t.Errorf("strong: %+v", s)
	}
	if rep.Summary["cloud_prompt_tokens"] != 1000 || rep.Summary["cloud_cached_tokens"] != 400 || rep.Summary["cloud_completion_tokens"] != 50 {
		t.Errorf("cloud tokens: %v %v %v", rep.Summary["cloud_prompt_tokens"], rep.Summary["cloud_cached_tokens"], rep.Summary["cloud_completion_tokens"])
	}
	if v := rep.Summary["cloud_tokens_per_passed_task"]; v != 1050.0 {
		t.Errorf("cloud tokens per passed task: %v", v)
	}
	var buf strings.Builder
	rep.Print(&buf)
	if !strings.Contains(buf.String(), "per class:") || !strings.Contains(buf.String(), "cloud-model") {
		t.Errorf("table:\n%s", buf.String())
	}
}

func TestStaticClassFollowsTheMeasurements(t *testing.T) {
	for _, tk := range Tasks() {
		want := app.ClassFast
		switch tk.Kind {
		case "log-triage", "csv-stats", "changelog":
			want = app.ClassStrong
		}
		if got := StaticClass(tk.Kind); got != want {
			t.Errorf("%s: %s, want %s", tk.ID, got, want)
		}
	}
}
