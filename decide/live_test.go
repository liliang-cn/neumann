package decide_test

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/decide"
)

// TestLive asks a real System One server. It is skipped unless
// NEUMANN_DECIDE_LIVE=1; NEUMANN_DECIDE_URL (default the local Ollama),
// NEUMANN_DECIDE_MODEL (default tev1:4b) and NEUMANN_DECIDE_KEY (a relay's
// key) choose the server.
//
//	NEUMANN_DECIDE_LIVE=1 go test -v -run TestLive ./decide
func TestLive(t *testing.T) {
	if os.Getenv("NEUMANN_DECIDE_LIVE") != "1" {
		t.Skip("set NEUMANN_DECIDE_LIVE=1 to ask a real decision engine")
	}
	e := decide.NewSystemOne(decide.SystemOneConfig{
		URL:       os.Getenv("NEUMANN_DECIDE_URL"),
		Model:     os.Getenv("NEUMANN_DECIDE_MODEL"),
		APIKey:    os.Getenv("NEUMANN_DECIDE_KEY"),
		KeepAlive: 10 * time.Minute,
	})
	ctx := context.Background()
	if err := e.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	// The first request loads the model; time the ones after it.
	start := time.Now()
	if _, err := e.Decide(ctx, "warm up", map[string]decide.Question{"q": decide.Noul("Is this a greeting?")}); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s at %s: first request %s", e.Name(), e.URL(), time.Since(start).Round(time.Millisecond))

	t.Run("three types", func(t *testing.T) {
		qs := map[string]decide.Question{
			"risky":  decide.Noul("Is this request dangerous to carry out?"),
			"route":  decide.Choice("Which model should handle it?", "fast", "strong").Describe("fast", "short factual answers").Describe("strong", "multi-step reasoning and writing"),
			"effort": decide.Score("How many steps does it take?", "one step", "a few steps", "many steps"),
		}
		start := time.Now()
		got, usage, err := e.DecideUsage(ctx, "Draft a migration plan for moving our billing database to a new region with no downtime.", qs)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s, %d input tokens", time.Since(start).Round(time.Millisecond), usage.InputTokens)
		for _, name := range []string{"risky", "route", "effort"} {
			a := got[name]
			t.Logf("  %-6s %-12s confidence %.2f score %.2f p %v", name, a.Label, a.Confidence, a.Score, a.Probabilities)
		}
	})

	t.Run("policy", func(t *testing.T) {
		p := decide.Policy(e, decide.PolicyConfig{Logf: t.Logf})
		for _, c := range []struct {
			tool   string
			effect bus.Effect
			args   string
			want   neumann.Verdict
		}{
			{"notes_read", bus.ReadOnly, `{"id":"n-42"}`, neumann.VerdictAllow},
			{"notes_delete", bus.SideEffect, `{"id":"n-42"}`, neumann.VerdictRequireApproval},
			{"notes_save", bus.SideEffect, `{"title":"groceries","text":"milk, eggs"}`, neumann.VerdictAllow},
			{"bank_transfer", bus.SideEffect, `{"to":"XX99","amount":5000}`, neumann.VerdictRequireApproval},
		} {
			info := neumann.ToolCallInfo{TaskID: "live", CallID: "c-1", Tool: c.tool, Device: "notes", Effect: c.effect, Args: json.RawMessage(c.args), Class: "default", Attempt: 1}
			start := time.Now()
			d := p(ctx, info)
			t.Logf("  %-13s %-16s %6s  %s", c.tool, d.Verdict, time.Since(start).Round(time.Millisecond), d.Reason)
			if d.Verdict != c.want {
				t.Errorf("%s: %s, want %s", c.tool, d.Verdict, c.want)
			}
		}
	})

	t.Run("router", func(t *testing.T) {
		r, err := decide.NewRouter(e, decide.RouterConfig{
			Routes: []decide.Route{
				{Class: "fast", Description: "a small, fast model: short factual answers, simple lookups, one-step requests"},
				{Class: "strong", Description: "a large, careful model: multi-step reasoning, analysis, planning, writing"},
			},
			Default: "strong",
		})
		if err != nil {
			t.Fatal(err)
		}
		for input, want := range map[string]string{
			"What is 17 + 25?": "fast",
			"Read the three attached quarterly reports, reconcile the revenue figures between them, find where they disagree and draft a memo to the CFO explaining each discrepancy.": "strong",
		} {
			start := time.Now()
			class, ans, err := r.Route(ctx, input)
			t.Logf("  %-6s confidence %.2f %6s  %.50q", class, ans.Confidence, time.Since(start).Round(time.Millisecond), input)
			if err != nil || class != want {
				t.Errorf("routed to %q (%v), want %q", class, err, want)
			}
		}
	})

	t.Run("checker", func(t *testing.T) {
		c, err := decide.NewChecker(e, decide.CheckerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		request := "List the three largest files in the project directory with their sizes."
		for _, x := range []struct {
			name, answer string
			failed       []string
		}{
			{"good", "The three largest files are: data/dump.sql (412 MB), assets/video.mp4 (96 MB) and build/app.bin (31 MB).", nil},
			{"plan only", "I'll start by scanning the project directory recursively, then sort the files by size and report the top three back to you.", []string{decide.CheckPlanOnly}},
		} {
			start := time.Now()
			res, err := c.Check(ctx, request, x.answer, decide.WithEvidence("fs_list: data/dump.sql 412M, assets/video.mp4 96M, build/app.bin 31M, README.md 4K"))
			took := time.Since(start).Round(time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{decide.CheckOnTopic, decide.CheckPlanOnly, decide.CheckRefuses} {
				t.Logf("  %-9s %-9s p(yes)=%.2f", x.name, name, res.Answers[name].P("yes"))
			}
			t.Logf("  %-9s pass=%v failed=%v %s", x.name, res.Pass, res.Failed, took)
			if !slices.Equal(res.Failed, x.failed) {
				t.Errorf("%s: failed %v, want %v", x.name, res.Failed, x.failed)
			}
		}
	})
}
