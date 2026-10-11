package decide

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// The names of the default checks.
const (
	CheckResponds = "responds"  // fails when the answer does not respond to the request
	CheckPlanOnly = "plan_only" // fails when the answer only plans or promises the work
	CheckRefuses  = "refuses"   // fails when the answer refuses or evades
)

// DefaultCheckTimeout bounds each engine request a Checker makes.
const DefaultCheckTimeout = 3 * time.Second

// Check is one thing a Checker asks about an answer: a noul, the answer to
// it that fails the check, and the sentence that says so in
// CheckResult.Feedback.
type Check struct {
	Question Question // a noul
	FailOn   bool     // true: a yes fails the check; false: a no does
	Feedback string   // one sentence, addressed to the model that wrote the answer
}

// DefaultChecks are the checks a Checker runs unless CheckerConfig.Checks
// replaces them: does the answer respond to the request, is it only a plan
// or a promise, does it refuse or evade. The map is new on every call; add
// to it or change it freely.
func DefaultChecks() map[string]Check {
	return map[string]Check{
		CheckResponds: {
			Question: Noul("Does the answer actually respond to the request: does it address what was asked, rather than something else?").
				Describe("yes", "it addresses what the request asked for").
				Describe("no", "it answers a different question, or talks around the request"),
			FailOn:   false,
			Feedback: "Your answer did not respond to the request: answer what was asked.",
		},
		CheckPlanOnly: {
			Question: Noul("Is the answer only a plan, an outline or a promise to do the work (\"I will...\", \"here is how I would...\") instead of the finished work or its result?").
				Describe("yes", "it describes or promises the work, but does not deliver it").
				Describe("no", "it delivers the work or its result"),
			FailOn:   true,
			Feedback: "Your answer described a plan or promised the work without doing it: do the work now and give the result.",
		},
		CheckRefuses: {
			Question: Noul("Does the answer refuse, deflect or evade the request instead of fulfilling it?").
				Describe("yes", "it declines, changes the subject, or answers with excuses").
				Describe("no", "it takes the request on"),
			FailOn:   true,
			Feedback: "Your answer refused or evaded the request: if it can be done, do it; if not, say exactly what stands in the way.",
		},
	}
}

// CheckerConfig configures a Checker.
type CheckerConfig struct {
	// Checks are asked by name in one request (default DefaultChecks()).
	Checks map[string]Check
	// Floor is the confidence a failing answer needs to fail its check, in
	// [0, 1). Zero fails a check on any failing answer.
	Floor float64
	// Timeout bounds each engine request (default DefaultCheckTimeout).
	Timeout time.Duration
}

// Checker checks a finished task's answer against its request. It runs in
// host code, after the task's answer or done event: it changes nothing in
// the machine, and what to do about a failed check (submit again with the
// feedback, steer, tell a person) is the host's choice. It is safe for
// concurrent use.
type Checker struct {
	engine  Engine
	checks  map[string]Check
	names   []string // sorted: the order of Failed and of Feedback
	qs      map[string]Question
	floor   float64
	timeout time.Duration
}

// NewChecker checks cfg and builds a Checker on engine.
func NewChecker(engine Engine, cfg CheckerConfig) (*Checker, error) {
	if engine == nil {
		return nil, fmt.Errorf("decide: a checker needs an engine")
	}
	checks := maps.Clone(cfg.Checks)
	if checks == nil {
		checks = DefaultChecks()
	}
	qs := make(map[string]Question, len(checks))
	for name, c := range checks {
		if c.Question.Type != TypeNoul {
			return nil, fmt.Errorf("decide: check %q asks a %q, not a noul", name, c.Question.Type)
		}
		if strings.TrimSpace(c.Feedback) == "" {
			return nil, fmt.Errorf("decide: check %q has no feedback", name)
		}
		qs[name] = c.Question
	}
	if err := Validate(qs); err != nil {
		return nil, err
	}
	if cfg.Floor < 0 || cfg.Floor >= 1 {
		return nil, fmt.Errorf("decide: checker floor %v is not in [0, 1)", cfg.Floor)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultCheckTimeout
	}
	return &Checker{
		engine: engine, checks: checks, names: slices.Sorted(maps.Keys(checks)), qs: qs,
		floor: cfg.Floor, timeout: cfg.Timeout,
	}, nil
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
	// Failed names the checks that failed, sorted.
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
//
// The engine is shown the request, the answer and any evidence, each cut
// short enough that together they fit MaxState.
func (c *Checker) Check(ctx context.Context, request, answer string, opts ...CheckOption) (CheckResult, error) {
	var in checkInput
	for _, o := range opts {
		o(&in)
	}
	state := checkState(request, answer, in.evidence)
	actx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	answers, err := c.engine.Decide(actx, state, c.qs)
	if err == nil {
		for _, name := range c.names {
			if _, ok := answers[name]; !ok {
				err = fmt.Errorf("decide: %s did not answer %q", c.engine.Name(), name)
				break
			}
		}
	}
	if err != nil {
		if ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("decide: %s gave no answer within %s: %w", c.engine.Name(), c.timeout, err)
		}
		return CheckResult{Pass: true}, err
	}
	res := CheckResult{Pass: true, Answers: answers}
	var feedback []string
	for _, name := range c.names {
		a, chk := answers[name], c.checks[name]
		if a.Yes == chk.FailOn && a.Confidence >= c.floor {
			res.Pass = false
			res.Failed = append(res.Failed, name)
			feedback = append(feedback, strings.TrimSpace(chk.Feedback))
		}
	}
	res.Feedback = strings.Join(feedback, " ")
	return res, nil
}

// checkState lays out what a Checker shows the engine. The request and the
// evidence get at most 8 KiB each; the answer gets what is left.
func checkState(request, answer string, evidence []string) string {
	const part = 8 << 10
	var b strings.Builder
	fmt.Fprintf(&b, "REQUEST:\n%s\n\n", clip(strings.TrimSpace(request), part))
	ev := ""
	if len(evidence) > 0 {
		ev = "\n\nEVIDENCE (tool results the answer rests on):\n" + clip(strings.TrimSpace(strings.Join(evidence, "\n---\n")), part)
	}
	room := MaxState - b.Len() - len("ANSWER:\n") - len(ev) - 64 // 64: clip's note
	fmt.Fprintf(&b, "ANSWER:\n%s%s", clip(strings.TrimSpace(answer), room), ev)
	return b.String()
}
