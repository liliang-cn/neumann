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

// verdicts answers the default checks: on_topic, plan_only, refuses, each as
// the probability of yes.
func verdicts(onTopic, planOnly, refuses float64) *decidetest.Engine {
	return &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.CheckOnTopic:  decidetest.Yes(onTopic),
		decide.CheckPlanOnly: decidetest.Yes(planOnly),
		decide.CheckRefuses:  decidetest.Yes(refuses),
	}}
}

func names(asks []decide.Ask) string {
	var out []string
	for _, a := range asks {
		out = append(out, a.Name)
	}
	return strings.Join(out, ",")
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
	if got := names(a.Asks); got != "on_topic,plan_only,refuses" {
		t.Fatalf("asked %s", got)
	}
	if a.State != "A user asked an AI assistant:\n\"What is 6 x 7?\"\n\nThe assistant replied:\n\"42.\"\n\nTool results the assistant had:\n\"calc_multiply: 42\"" {
		t.Fatalf("state %q", a.State)
	}
}

func TestCheckerFailures(t *testing.T) {
	feedback := map[string]string{}
	for _, c := range decide.DefaultChecks() {
		feedback[c.Name] = c.Feedback
	}
	for _, c := range []struct {
		name   string
		engine *decidetest.Engine
		failed []string
	}{
		{"off topic", verdicts(0.1, 0.05, 0.02), []string{decide.CheckOnTopic}},
		{"plan only", verdicts(0.9, 0.9, 0.02), []string{decide.CheckPlanOnly}},
		{"refuses", verdicts(0.9, 0.05, 0.8), []string{decide.CheckRefuses}},
		{"all", verdicts(0.1, 0.9, 0.9), []string{decide.CheckOnTopic, decide.CheckPlanOnly, decide.CheckRefuses}},
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
				want = append(want, feedback[name])
			}
			if res.Feedback != strings.Join(want, " ") {
				t.Fatalf("feedback %q", res.Feedback)
			}
		})
	}
}

func TestCheckerFloor(t *testing.T) {
	e := verdicts(0.9, 0.6, 0.02) // plan_only yes with confidence 0.2
	c, _ := decide.NewChecker(e, decide.CheckerConfig{})
	if res, err := c.Check(bg, "r", "a"); err != nil || !res.Pass {
		t.Fatalf("a doubtful failure under the default floor: %+v %v", res, err)
	}
	c, _ = decide.NewChecker(e, decide.CheckerConfig{Floor: 0.1})
	if res, _ := c.Check(bg, "r", "a"); res.Pass {
		t.Fatalf("a failure over the floor: %+v", res)
	}
	c, _ = decide.NewChecker(verdicts(0.9, 0.5, 0.02), decide.CheckerConfig{Floor: -1})
	if res, _ := c.Check(bg, "r", "a"); res.Pass || res.Failed[0] != decide.CheckPlanOnly {
		t.Fatalf("a negative floor fails on any failing answer: %+v", res)
	}
}

func TestCheckerFailsOpen(t *testing.T) {
	boom := errors.New("boom")
	failing := &decidetest.Engine{Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) { return nil, boom }}
	c, _ := decide.NewChecker(failing, decide.CheckerConfig{})
	res, err := c.Check(bg, "r", "a")
	if !errors.Is(err, boom) || !res.Pass || res.Answers != nil || res.Failed != nil {
		t.Fatalf("%+v %v", res, err)
	}

	for _, ignore := range []bool{false, true} {
		slow := verdicts(0.1, 0.9, 0.9)
		slow.Delay, slow.IgnoreContext = 500*time.Millisecond, ignore
		c, _ = decide.NewChecker(slow, decide.CheckerConfig{Timeout: 50 * time.Millisecond})
		start := time.Now()
		res, err = c.Check(bg, "r", "a")
		if !errors.Is(err, context.DeadlineExceeded) || err.Error() != "decide: decidetest: no answer within 50ms" || !res.Pass {
			t.Fatalf("ignore context %v: %+v %v", ignore, res, err)
		}
		if took := time.Since(start); took > 400*time.Millisecond {
			t.Fatalf("ignore context %v: took %s", ignore, took)
		}
	}

	partial := &decidetest.Engine{Answers: map[string]decide.Answer{decide.CheckOnTopic: decidetest.Yes(0.1)}}
	c, _ = decide.NewChecker(partial, decide.CheckerConfig{})
	if res, err := c.Check(bg, "r", "a"); err == nil || !res.Pass {
		t.Fatalf("a missing answer: %+v %v", res, err)
	}
}

func TestCheckerCustomChecks(t *testing.T) {
	checks := decide.DefaultChecks()[:2]
	checks = append([]decide.Check{{Name: "cites", Question: decide.Noul("Does the answer cite its sources?"), FailOn: false, Feedback: "Cite your sources."}}, checks...)
	e := &decidetest.Engine{Answers: map[string]decide.Answer{
		decide.CheckOnTopic:  decidetest.Yes(0.9),
		decide.CheckPlanOnly: decidetest.Yes(0.9),
		"cites":              decidetest.Yes(0.2),
	}}
	c, err := decide.NewChecker(e, decide.CheckerConfig{Checks: checks})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Check(bg, "r", "a")
	if err != nil || res.Pass || strings.Join(res.Failed, ",") != "cites,plan_only" || !strings.HasPrefix(res.Feedback, "Cite your sources. Your answer described a plan") {
		t.Fatalf("%+v %v", res, err)
	}
	if got := names(e.Asked()[0].Asks); got != "cites,on_topic,plan_only" {
		t.Fatalf("asked %s", got)
	}
}

func TestCheckerClipsTheState(t *testing.T) {
	e := verdicts(0.9, 0.1, 0.1)
	c, _ := decide.NewChecker(e, decide.CheckerConfig{})
	answer := "START " + strings.Repeat("x", 20000) + " END"
	if _, err := c.Check(bg, strings.Repeat("r", 20000), answer, decide.WithEvidence(strings.Repeat("e", 20000))); err != nil {
		t.Fatal(err)
	}
	s := e.Asked()[0].State
	if len(s) > decide.DefaultMaxCheckState+300 {
		t.Fatalf("state of %d bytes", len(s))
	}
	if !strings.Contains(s, "\"START ") || !strings.Contains(s, " END\"") || strings.Count(s, "bytes cut …]") != 2 || strings.Count(s, "more bytes]") != 1 {
		t.Fatalf("state %q", s)
	}

	c, _ = decide.NewChecker(e, decide.CheckerConfig{MaxState: -1})
	if _, err := c.Check(bg, "r", answer); err != nil {
		t.Fatal(err)
	}
	if s := e.Asked()[1].State; !strings.Contains(s, answer) {
		t.Fatal("a negative MaxState cut the answer")
	}
}

func TestCheckerSurvivesAPanickingEngine(t *testing.T) {
	c, _ := decide.NewChecker(panicking(), decide.CheckerConfig{})
	res, err := c.Check(bg, "r", "a")
	if !res.Pass || err == nil || err.Error() != "decide: decidetest: panicked: assignment to entry in nil map" {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestNewCheckerChecks(t *testing.T) {
	e := &decidetest.Engine{}
	for name, cfg := range map[string]decide.CheckerConfig{
		"not a noul":  {Checks: []decide.Check{{Name: "x", Question: decide.Choice("?", "a", "b"), Feedback: "f"}}},
		"no feedback": {Checks: []decide.Check{{Name: "x", Question: decide.Noul("?")}}},
		"no checks":   {Checks: []decide.Check{}},
		"no name":     {Checks: []decide.Check{{Question: decide.Noul("?"), Feedback: "f"}}},
		"same name":   {Checks: append(decide.DefaultChecks(), decide.DefaultChecks()[0])},
		"bad noul":    {Checks: []decide.Check{{Name: "x", Question: decide.Noul(""), Feedback: "f"}}},
		"floor":       {Floor: 1},
	} {
		if _, err := decide.NewChecker(e, cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := decide.NewChecker(nil, decide.CheckerConfig{}); err == nil {
		t.Error("a checker without an engine")
	}
}
