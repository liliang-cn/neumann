package decide

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// The names of the default checks.
const (
	CheckOnTopic  = "on_topic"  // fails when the answer is not about the request
	CheckPlanOnly = "plan_only" // fails when the answer only plans or promises the work
	CheckRefuses  = "refuses"   // fails when the answer refuses or evades
)

// Checker defaults.
const (
	DefaultCheckTimeout = 5 * time.Second
	// DefaultCheckFloor is the confidence a failing answer needs to fail its
	// check: a probability of at least 0.65 for the failing side. A small
	// engine is often unsure about a good answer, and its doubt should not
	// fail it.
	DefaultCheckFloor = 0.3
	// DefaultMaxCheckState is how many bytes of request, answer and
	// evidence together the engine is shown, which fits tev1:4b's
	// 2050-token context with room for the default checks, for English.
	DefaultMaxCheckState = 4096
)

// Check is one thing a Checker asks about an answer: a noul, the answer to
// it that fails the check, and the sentence that says so in
// CheckResult.Feedback.
type Check struct {
	Name     string
	Question Question // a noul
	FailOn   bool     // true: a yes fails the check; false: a no does
	Feedback string   // one sentence, addressed to the model that wrote the answer
}

// DefaultChecks are the checks a Checker runs unless CheckerConfig.Checks
// replaces them, in the order they are asked: is the answer about the
// request, is it only a plan or a promise, does it refuse or evade. The
// slice is new on every call; change it freely.
//
// The wording and the order were tuned against tev1:4b, which answers the
// questions of a request one after another: asked alone, the plan question
// scored a plain promise ("I'll start by scanning the directory...") 0.19;
// asked after the topic question, 0.93.
func DefaultChecks() []Check {
	return []Check{
		{
			Name: CheckOnTopic,
			Question: Noul("Is the assistant's reply about what the user asked for (the same task or question), whether or not it finishes it?").
				Describe("yes", "it deals with the user's request").
				Describe("no", "it talks about something unrelated to the request"),
			FailOn:   false,
			Feedback: "Your answer did not respond to the request: answer what was asked.",
		},
		{
			Name:     CheckPlanOnly,
			Question: Noul("Does the reply talk about work in the future tense (I will, I'll, first I'm going to) instead of reporting a result?"),
			FailOn:   true,
			Feedback: "Your answer described a plan or promised the work without doing it: do the work now and give the result.",
		},
		{
			Name: CheckRefuses,
			Question: Noul("Does the assistant decline or refuse to help with the request?").
				Describe("yes", "it says it cannot or will not help").
				Describe("no", "it tries to help"),
			FailOn:   true,
			Feedback: "Your answer refused or evaded the request: if it can be done, do it; if not, say exactly what stands in the way.",
		},
	}
}

// CheckerConfig configures a Checker. As in PolicyConfig and RouterConfig, a
// zero number takes the package default and a negative one turns the bound
// off.
type CheckerConfig struct {
	// Checks are asked in one request, in order (nil: DefaultChecks()).
	Checks []Check
	// Floor is the confidence a failing answer needs to fail its check
	// (zero: DefaultCheckFloor; negative: every failing answer fails it).
	// It must be below 1.
	Floor float64
	// Timeout bounds each engine request (zero: DefaultCheckTimeout; it
	// cannot be turned off).
	Timeout time.Duration
	// MaxState is how many bytes of request, answer and evidence the engine
	// is shown (zero: DefaultMaxCheckState; negative: all of them). The
	// request and the evidence get up to a quarter each, cut at the end;
	// the answer gets the rest, and a longer one keeps its start and its
	// end, with a marker where the middle was cut.
	MaxState int
}

// Checker checks a finished task's answer against its request. It runs in
// host code, after the task's answer or done event: it changes nothing in
// the machine, and what to do about a failed check (submit again with the
// feedback, steer, tell a person) is the host's choice. It is safe for
// concurrent use.
type Checker struct {
	engine   Engine
	checks   []Check
	asks     []Ask
	floor    float64
	timeout  time.Duration
	maxState int
}

// NewChecker checks cfg and builds a Checker on engine.
func NewChecker(engine Engine, cfg CheckerConfig) (*Checker, error) {
	if engine == nil {
		return nil, fmt.Errorf("decide: a checker needs an engine")
	}
	checks := append([]Check(nil), cfg.Checks...)
	if cfg.Checks == nil {
		checks = DefaultChecks()
	}
	asks := make([]Ask, 0, len(checks))
	for _, c := range checks {
		if c.Question.Type != TypeNoul {
			return nil, fmt.Errorf("decide: check %q asks a %q, not a noul", c.Name, c.Question.Type)
		}
		if strings.TrimSpace(c.Feedback) == "" {
			return nil, fmt.Errorf("decide: check %q has no feedback", c.Name)
		}
		asks = append(asks, Ask{Name: c.Name, Question: c.Question})
	}
	if err := Validate(asks); err != nil {
		return nil, err
	}
	floor, err := bound("checker floor", cfg.Floor, DefaultCheckFloor)
	if err != nil {
		return nil, err
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultCheckTimeout
	}
	if cfg.MaxState == 0 {
		cfg.MaxState = DefaultMaxCheckState
	}
	return &Checker{engine: engine, checks: checks, asks: asks, floor: floor, timeout: cfg.Timeout, maxState: cfg.MaxState}, nil
}

// CheckOption adds to one Check call.
type CheckOption func(*checkInput)

type checkInput struct{ evidence []string }

// WithEvidence shows the engine more of what the answer rests on, a few tool
// results say, after the request and the answer.
func WithEvidence(text ...string) CheckOption {
	return func(in *checkInput) { in.evidence = append(in.evidence, text...) }
}

// CheckResult is what a Checker found.
type CheckResult struct {
	Pass bool
	// Failed names the checks that failed, in the order they were asked.
	Failed []string
	// Feedback is the failed checks' sentences, in Failed's order, for the
	// host to send back to the model (as a new task, or a Steer); empty
	// when it passed. It is built from fixed sentences, never generated.
	Feedback string
	// Answers holds the engine's answer to every check; nil when the engine
	// gave none.
	Answers map[string]Answer
}

// Check asks every check about answer, given request, in one request to the
// engine. A check fails when the engine gives its failing answer with at
// least the floor's confidence.
//
// It fails open: when the engine gives no answer (an error, or none within
// the timeout) the result passes, and the error is returned to say why. A
// small classifier's doubt is not a reason to hold back a large model's
// answer, and a missing doubt even less so.
func (c *Checker) Check(ctx context.Context, request, answer string, opts ...CheckOption) (CheckResult, error) {
	var in checkInput
	for _, o := range opts {
		o(&in)
	}
	answers, err := ask(ctx, c.engine, c.timeout, checkState(request, answer, in.evidence, c.maxState), c.asks)
	if err != nil {
		return CheckResult{Pass: true}, err
	}
	res := CheckResult{Pass: true, Answers: answers}
	var feedback []string
	for _, chk := range c.checks {
		a := answers[chk.Name]
		if a.Yes == chk.FailOn && a.Confidence >= c.floor {
			res.Pass = false
			res.Failed = append(res.Failed, chk.Name)
			feedback = append(feedback, strings.TrimSpace(chk.Feedback))
		}
	}
	res.Feedback = strings.Join(feedback, " ")
	return res, nil
}

// checkState lays out what a Checker shows the engine, within max bytes of
// text (no limit if max is negative).
func checkState(request, answer string, evidence []string, max int) string {
	request, answer = strings.TrimSpace(request), strings.TrimSpace(answer)
	ev := strings.TrimSpace(strings.Join(evidence, "\n---\n"))
	if max >= 0 {
		request = clipHead(request, max/4)
		ev = clipHead(ev, max/4)
		answer = clipMiddle(answer, max-len(request)-len(ev))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "A user asked an AI assistant:\n\"%s\"\n\nThe assistant replied:\n\"%s\"", request, answer)
	if ev != "" {
		fmt.Fprintf(&b, "\n\nTool results the assistant had:\n\"%s\"", ev)
	}
	return b.String()
}
