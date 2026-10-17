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
