package decide_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/decide"
	"github.com/liliang-cn/neumann/decide/decidetest"
)

// verdicts answers the default checks: responds, plan_only, refuses, each as
// the probability of yes.
func verdicts(responds, planOnly, refuses float64) *decidetest.Engine {
	return &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.CheckResponds: decidetest.Yes(responds),
		decide.CheckPlanOnly: decidetest.Yes(planOnly),
		decide.CheckRefuses:  decidetest.Yes(refuses),
	}}
}

func TestCheckerPasses(t *testing.T) {
	e := verdicts(0.95, 0.05, 0.02)
	c, err := decide.NewChecker(e, decide.CheckerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Check(bg, "What is 6 x 7?", "42.", decide.WithEvidence("calc_multiply: 42"))
	if err != nil || !res.Pass || res.Failed != nil || res.Feedback != "" || len(res.Answers) != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	a := e.Asked()[0]
	if len(a.Questions) != 3 || a.Questions[decide.CheckPlanOnly].Type != decide.TypeNoul {
		t.Fatalf("questions %+v", a.Questions)
	}
	if a.State != "REQUEST:\nWhat is 6 x 7?\n\nANSWER:\n42.\n\nEVIDENCE (tool results the answer rests on):\ncalc_multiply: 42" {
		t.Fatalf("state %q", a.State)
	}
}

func TestCheckerFailures(t *testing.T) {
	checks := decide.DefaultChecks()
	for _, c := range []struct {
		name   string
		engine *decidetest.Engine
		failed []string
	}{
		{"off topic", verdicts(0.1, 0.05, 0.02), []string{decide.CheckResponds}},
		{"plan only", verdicts(0.9, 0.9, 0.02), []string{decide.CheckPlanOnly}},
		{"refuses", verdicts(0.9, 0.05, 0.8), []string{decide.CheckRefuses}},
		{"all", verdicts(0.2, 0.7, 0.6), []string{decide.CheckPlanOnly, decide.CheckRefuses, decide.CheckResponds}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ch, err := decide.NewChecker(c.engine, decide.CheckerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			res, err := ch.Check(bg, "request", "answer")
			if err != nil || res.Pass || strings.Join(res.Failed, ",") != strings.Join(c.failed, ",") {
				t.Fatalf("%+v %v", res, err)
			}
			var want []string
			for _, name := range c.failed {
				want = append(want, checks[name].Feedback)
			}
			if res.Feedback != strings.Join(want, " ") {
				t.Fatalf("feedback %q", res.Feedback)
			}
		})
	}
}

func TestCheckerFloor(t *testing.T) {
	e := verdicts(0.9, 0.6, 0.02) // plan_only yes with confidence 0.2
	c, _ := decide.NewChecker(e, decide.CheckerConfig{Floor: 0.5})
	if res, err := c.Check(bg, "r", "a"); err != nil || !res.Pass {
		t.Fatalf("a doubtful failure under the floor: %+v %v", res, err)
	}
	c, _ = decide.NewChecker(e, decide.CheckerConfig{})
	if res, _ := c.Check(bg, "r", "a"); res.Pass {
		t.Fatalf("floor 0 fails on any failing answer: %+v", res)
	}
}

func TestCheckerFailsOpen(t *testing.T) {
	boom := errors.New("boom")
	failing := &decidetest.Engine{Fn: func(string, map[string]decide.Question) (map[string]decide.Answer, error) { return nil, boom }}
	c, _ := decide.NewChecker(failing, decide.CheckerConfig{})
	res, err := c.Check(bg, "r", "a")
	if !errors.Is(err, boom) || !res.Pass || res.Answers != nil || res.Failed != nil {
		t.Fatalf("%+v %v", res, err)
	}

	slow := verdicts(0.1, 0.9, 0.9)
	slow.Delay = time.Minute
	c, _ = decide.NewChecker(slow, decide.CheckerConfig{Timeout: 50 * time.Millisecond})
	start := time.Now()
	res, err = c.Check(bg, "r", "a")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "within 50ms") || !res.Pass {
		t.Fatalf("%+v %v", res, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s", took)
	}

	partial := &decidetest.Engine{Answers: map[string]decide.Answer{decide.CheckResponds: decidetest.Yes(0.1)}}
	c, _ = decide.NewChecker(partial, decide.CheckerConfig{})
	if res, err := c.Check(bg, "r", "a"); err == nil || !res.Pass {
		t.Fatalf("a missing answer: %+v %v", res, err)
	}
}

func TestCheckerCustomChecks(t *testing.T) {
	checks := decide.DefaultChecks()
	delete(checks, decide.CheckRefuses)
	checks["cites"] = decide.Check{Question: decide.Noul("Does the answer cite its sources?"), FailOn: false, Feedback: "Cite your sources."}
	e := &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.CheckResponds: decidetest.Yes(0.9),
		decide.CheckPlanOnly: decidetest.Yes(0.1),
		"cites":              decidetest.Yes(0.2),
	}}
	c, err := decide.NewChecker(e, decide.CheckerConfig{Checks: checks})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Check(bg, "r", "a")
	if err != nil || res.Pass || strings.Join(res.Failed, ",") != "cites" || res.Feedback != "Cite your sources." {
		t.Fatalf("%+v %v", res, err)
	}
	if _, ok := e.Asked()[0].Questions[decide.CheckRefuses]; ok {
		t.Fatal("a removed check was asked")
	}
}

func TestCheckerClipsTheState(t *testing.T) {
	e := verdicts(0.9, 0.1, 0.1)
	c, _ := decide.NewChecker(e, decide.CheckerConfig{})
	big := strings.Repeat("x", decide.MaxState)
	if _, err := c.Check(bg, big, big, decide.WithEvidence(big, big)); err != nil {
		t.Fatal(err)
	}
	if s := e.Asked()[0].State; len(s) > decide.MaxState {
		t.Fatalf("state of %d bytes", len(s))
	}
}

func TestNewCheckerChecks(t *testing.T) {
	e := &decidetest.Engine{}
	for name, cfg := range map[string]decide.CheckerConfig{
		"not a noul":  {Checks: map[string]decide.Check{"x": {Question: decide.Choice("?", "a", "b"), Feedback: "f"}}},
		"no feedback": {Checks: map[string]decide.Check{"x": {Question: decide.Noul("?")}}},
		"no checks":   {Checks: map[string]decide.Check{}},
		"bad noul":    {Checks: map[string]decide.Check{"x": {Question: decide.Noul(""), Feedback: "f"}}},
		"floor":       {Floor: -0.1},
	} {
		if _, err := decide.NewChecker(e, cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := decide.NewChecker(nil, decide.CheckerConfig{}); err == nil {
		t.Error("a checker without an engine")
	}
}
