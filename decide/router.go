package decide

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// RouteQuestion is the name Router asks its question under.
const RouteQuestion = "route"

// DefaultRouterTimeout bounds each engine request a Router makes.
const DefaultRouterTimeout = 2 * time.Second

// DefaultRouteInstructions is the question Router asks unless
// RouterConfig.Instructions replaces it.
const DefaultRouteInstructions = "Which model should handle this request?"

// Route is one class a Router may pick: an execution group's Class, and one
// line saying which requests it is for.
type Route struct {
	Class       string
	Description string
}

// RouterConfig configures a Router.
type RouterConfig struct {
	// Routes are the candidate classes, at least two, in order of
	// preference: an engine breaks ties by it.
	Routes []Route
	// Default is the class used when the engine fails or is not sure
	// enough. It need not be one of Routes, but must be a class the machine
	// runs.
	Default string
	// Floor is the confidence the engine's pick needs, in [0, 1); below it
	// Route returns Default. Zero takes every pick.
	Floor float64
	// Instructions replaces DefaultRouteInstructions.
	Instructions string
	// Timeout bounds each engine request (default DefaultRouterTimeout).
	Timeout time.Duration
}

// Router picks the class a submission runs in: the host asks it before
// Submit and sets proto.Instruction.Class to its answer. This is how
// several models share the work: a cheap one for easy requests, a strong one
// for hard ones, with a small engine deciding which. It is safe for
// concurrent use.
//
// The engine's request is made by the host, before the task exists: it is
// not an instruction of any task and is recorded nowhere by the machine.
// The class it picks is, in the task's accepted event.
type Router struct {
	engine  Engine
	q       Question
	def     string
	floor   float64
	timeout time.Duration
}

// NewRouter checks cfg and builds a Router on engine.
func NewRouter(engine Engine, cfg RouterConfig) (*Router, error) {
	if engine == nil {
		return nil, fmt.Errorf("decide: a router needs an engine")
	}
	if strings.TrimSpace(cfg.Default) == "" {
		return nil, fmt.Errorf("decide: a router needs a default class")
	}
	if cfg.Floor < 0 || cfg.Floor >= 1 {
		return nil, fmt.Errorf("decide: router floor %v is not in [0, 1)", cfg.Floor)
	}
	in := cfg.Instructions
	if strings.TrimSpace(in) == "" {
		in = DefaultRouteInstructions
	}
	q := Question{Type: TypeChoice, Instructions: in}
	for _, r := range cfg.Routes {
		q.Options = append(q.Options, r.Class)
		if d := strings.TrimSpace(r.Description); d != "" {
			q = q.Describe(r.Class, d)
		}
	}
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("%w (router routes)", err)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultRouterTimeout
	}
	return &Router{engine: engine, q: q, def: cfg.Default, floor: cfg.Floor, timeout: cfg.Timeout}, nil
}

// Question is the choice the router asks, one option per route.
func (r *Router) Question() Question { return r.q }

// Route picks a class for input. It is the engine's pick when the engine
// answers with at least the floor's confidence, and the default otherwise:
// below the floor, err is nil and ans is the answer that was not trusted; on
// an engine error, err says why and ans is zero. Either way class is usable.
// An input longer than MaxState is routed on its first MaxState bytes.
func (r *Router) Route(ctx context.Context, input string) (class string, ans Answer, err error) {
	if strings.TrimSpace(input) == "" {
		return r.def, Answer{}, fmt.Errorf("decide: nothing to route")
	}
	if len(input) > MaxState {
		cut := MaxState
		for cut > 0 && !utf8.RuneStart(input[cut]) {
			cut--
		}
		input = input[:cut]
	}
	actx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	answers, err := r.engine.Decide(actx, input, map[string]Question{RouteQuestion: r.q})
	if err != nil {
		if ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("decide: %s gave no route within %s: %w", r.engine.Name(), r.timeout, err)
		}
		return r.def, Answer{}, err
	}
	ans, ok := answers[RouteQuestion]
	if !ok {
		return r.def, Answer{}, fmt.Errorf("decide: %s did not answer %q", r.engine.Name(), RouteQuestion)
	}
	if !slices.Contains(r.q.Options, ans.Label) {
		return r.def, Answer{}, fmt.Errorf("decide: %s routed to %q, which is not a route", r.engine.Name(), ans.Label)
	}
	if ans.Confidence < r.floor {
		return r.def, ans, nil
	}
	return ans.Label, ans, nil
}
