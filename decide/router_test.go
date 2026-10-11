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

var routes = []decide.Route{
	{Class: "strong", Description: "multi-step reasoning, analysis, writing"},
	{Class: "fast", Description: "short factual answers"},
}

// byLength routes short inputs to fast, long ones to strong.
func byLength(state string, _ []decide.Ask) (map[string]decide.Answer, error) {
	if len(state) < 40 {
		return map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick("fast", 0.8)}, nil
	}
	return map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick("strong", 0.3)}, nil
}

func TestRouterRoutes(t *testing.T) {
	e := &decidetest.Engine{Fn: byLength}
	r, err := decide.NewRouter(e, decide.RouterConfig{Routes: routes, Default: "strong"})
	if err != nil {
		t.Fatal(err)
	}
	class, ans, err := r.Route(bg, "What is 17 + 25?")
	if err != nil || class != "fast" || ans.Label != "fast" || ans.Confidence != 0.8 {
		t.Fatalf("%q %+v %v", class, ans, err)
	}
	class, _, err = r.Route(bg, "Reconcile the revenue figures of three quarterly reports and write a memo.")
	if err != nil || class != "strong" {
		t.Fatalf("%q %v", class, err)
	}
	q, _ := e.Asked()[0].Question(decide.RouteQuestion)
	if q.Type != decide.TypeChoice || q.Instructions != decide.DefaultRouteInstructions ||
		strings.Join(q.Options, ",") != "strong,fast" || q.Descriptions["fast"] != "short factual answers" {
		t.Fatalf("question %+v", q)
	}
	if e.Asked()[0].State != "What is 17 + 25?" {
		t.Fatalf("state %q", e.Asked()[0].State)
	}
}

func TestRouterFloor(t *testing.T) {
	e := &decidetest.Engine{Fn: byLength}
	r, err := decide.NewRouter(e, decide.RouterConfig{Routes: routes, Default: "default", Floor: 0.5, Instructions: "Who?"})
	if err != nil {
		t.Fatal(err)
	}
	if class, _, err := r.Route(bg, "short"); err != nil || class != "fast" {
		t.Fatalf("%q %v", class, err)
	}
	class, ans, err := r.Route(bg, strings.Repeat("long ", 20))
	if err != nil || class != "default" || ans.Label != "strong" {
		t.Fatalf("under the floor: %q %+v %v", class, ans, err)
	}
	if q := r.Question(); q.Instructions != "Who?" {
		t.Fatalf("%+v", q)
	}

	unsure := &decidetest.Engine{Answers: map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick("fast", 0.1)}}
	r, _ = decide.NewRouter(unsure, decide.RouterConfig{Routes: routes, Default: "strong"})
	if class, _, _ := r.Route(bg, "x"); class != "strong" {
		t.Fatalf("a pick under DefaultRouteFloor went to %q", class)
	}
	r, _ = decide.NewRouter(unsure, decide.RouterConfig{Routes: routes, Default: "strong", Floor: -1})
	if class, _, _ := r.Route(bg, "x"); class != "fast" {
		t.Fatalf("a negative floor takes every pick, not %q", class)
	}
}

func TestRouterDefaultsOnFailure(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]*decidetest.Engine{
		"error": {Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) { return nil, boom }},
		"missing": {Fn: func(string, []decide.Ask) (map[string]decide.Answer, error) {
			return map[string]decide.Answer{}, nil
		}},
		"wrong type":  {Answers: map[string]decide.Answer{decide.RouteQuestion: decidetest.Yes(0.9)}},
		"not a route": {Answers: map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick("medium", 0.9)}},
		"slow":        {Fn: byLength, Delay: time.Minute},
		"stuck":       {Fn: byLength, Delay: 500 * time.Millisecond, IgnoreContext: true},
	}
	for name, e := range cases {
		r, err := decide.NewRouter(e, decide.RouterConfig{Routes: routes, Default: "strong", Timeout: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		class, ans, err := r.Route(bg, "short")
		if class != "strong" || err == nil || ans.Label != "" {
			t.Errorf("%s: %q %+v %v", name, class, ans, err)
		}
		if took := time.Since(start); took > 400*time.Millisecond {
			t.Errorf("%s: took %s", name, took)
		}
		if (name == "slow" || name == "stuck") && (!errors.Is(err, context.DeadlineExceeded) || err.Error() != "decide: decidetest: no answer within 50ms") {
			t.Errorf("%s: %v", name, err)
		}
	}
	r, _ := decide.NewRouter(&decidetest.Engine{Fn: byLength}, decide.RouterConfig{Routes: routes, Default: "strong"})
	if class, _, err := r.Route(bg, "  "); class != "strong" || err == nil {
		t.Errorf("empty input: %q %v", class, err)
	}
}

func TestRouterClipsLongInput(t *testing.T) {
	e := &decidetest.Engine{Fn: byLength}
	r, _ := decide.NewRouter(e, decide.RouterConfig{Routes: routes, Default: "strong"})
	input := "FIRST " + strings.Repeat("é", 20000) + " LAST"
	if _, _, err := r.Route(bg, input); err != nil {
		t.Fatal(err)
	}
	s := e.Asked()[0].State
	if len(s) > decide.DefaultMaxInput+64 || !strings.HasPrefix(s, "FIRST ") || !strings.HasSuffix(s, " LAST") || !strings.Contains(s, "bytes cut …]") {
		t.Fatalf("routed on %d bytes: %.30q", len(s), s)
	}
	r, _ = decide.NewRouter(e, decide.RouterConfig{Routes: routes, Default: "strong", MaxInput: -1})
	if _, _, err := r.Route(bg, input); err != nil {
		t.Fatal(err)
	}
	if s := e.Asked()[1].State; s != input {
		t.Fatalf("a negative MaxInput cut the input to %d bytes", len(s))
	}
}

func TestNewRouterChecks(t *testing.T) {
	e := &decidetest.Engine{}
	for name, cfg := range map[string]decide.RouterConfig{
		"one route":   {Routes: routes[:1], Default: "strong"},
		"no default":  {Routes: routes},
		"same class":  {Routes: []decide.Route{{Class: "a"}, {Class: "a"}}, Default: "a"},
		"floor":       {Routes: routes, Default: "strong", Floor: 1},
		"empty class": {Routes: []decide.Route{{Class: "a"}, {Class: ""}}, Default: "a"},
	} {
		if _, err := decide.NewRouter(e, cfg); err == nil || !strings.HasPrefix(err.Error(), "decide: ") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := decide.NewRouter(nil, decide.RouterConfig{Routes: routes, Default: "strong"}); err == nil {
		t.Error("a router without an engine")
	}
}
