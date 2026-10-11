// Package decidetest has a scripted decide.Engine for tests and offline
// examples.
//
//	engine := &decidetest.Engine{Fn: func(state string, asks []decide.Ask) (map[string]decide.Answer, error) {
//		if strings.Contains(state, "tool: notes_delete") {
//			return map[string]decide.Answer{decide.NeedsApproval: decidetest.Yes(0.97)}, nil
//		}
//		return map[string]decide.Answer{decide.NeedsApproval: decidetest.Yes(0.02)}, nil
//	}}
//	cfg.ToolPolicy = decide.Policy(engine, decide.PolicyConfig{})
package decidetest

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/decide"
)

// Engine is a scripted decide.Engine. It is safe for concurrent use.
type Engine struct {
	// Fn answers each request. Nil: every question gets Answers[name], and
	// a question without one is an error.
	Fn func(state string, asks []decide.Ask) (map[string]decide.Answer, error)
	// Answers are the fixed answers used when Fn is nil.
	Answers map[string]decide.Answer
	// Delay makes each request wait this long before it is answered, or
	// until its context is done, which then is the error: a slow engine.
	Delay time.Duration
	// IgnoreContext makes Delay ignore the context: an engine that breaks
	// the Engine contract, to test that callers do not wait for it.
	IgnoreContext bool
	// EngineName is what Name returns (default "decidetest").
	EngineName string

	mu    sync.Mutex
	asked []Asked
}

// Asked is one request the engine got.
type Asked struct {
	State string
	Asks  []decide.Ask
}

// Question is the question asked under name, and whether there was one.
func (a Asked) Question(name string) (decide.Question, bool) {
	for _, x := range a.Asks {
		if x.Name == name {
			return x.Question, true
		}
	}
	return decide.Question{}, false
}

// Name is EngineName, or "decidetest".
func (e *Engine) Name() string {
	if e.EngineName != "" {
		return e.EngineName
	}
	return "decidetest"
}

// Decide notes the request, waits Delay, and answers by Fn or Answers.
func (e *Engine) Decide(ctx context.Context, state string, asks []decide.Ask) (map[string]decide.Answer, error) {
	e.mu.Lock()
	e.asked = append(e.asked, Asked{State: state, Asks: slices.Clone(asks)})
	e.mu.Unlock()
	if err := decide.Validate(asks); err != nil {
		return nil, err
	}
	if e.Delay > 0 && e.IgnoreContext {
		time.Sleep(e.Delay)
	} else if e.Delay > 0 {
		t := time.NewTimer(e.Delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil && !e.IgnoreContext {
		return nil, err
	}
	if e.Fn != nil {
		return e.Fn(state, asks)
	}
	out := make(map[string]decide.Answer, len(asks))
	for _, a := range asks {
		ans, ok := e.Answers[a.Name]
		if !ok {
			return nil, fmt.Errorf("decidetest: no answer for %q", a.Name)
		}
		out[a.Name] = ans
	}
	return out, nil
}

// Asked is every request so far, in order.
func (e *Engine) Asked() []Asked {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Asked(nil), e.asked...)
}

// Yes is a noul's answer whose probability of yes is p, as decide.SystemOne
// reads one: Yes when p >= 0.5, Confidence |2p-1|.
func Yes(p float64) decide.Answer {
	a := decide.Answer{
		Type: decide.TypeNoul, Label: "no", Yes: p >= 0.5, Confidence: math.Abs(2*p - 1),
		Probabilities: map[string]float64{"yes": p, "no": 1 - p},
	}
	if a.Yes {
		a.Label = "yes"
	}
	return a
}

// Pick is a choice's answer: label chosen with confidence, and probability
// 1 for it.
func Pick(label string, confidence float64) decide.Answer {
	return decide.Answer{
		Type: decide.TypeChoice, Label: label, Confidence: confidence,
		Probabilities: map[string]float64{label: 1},
	}
}
