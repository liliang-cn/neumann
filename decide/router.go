package decide

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// RouteQuestion is the name Router asks its question under.
const RouteQuestion = "route"

// Router defaults.
const (
	DefaultRouterTimeout = 3 * time.Second
	DefaultRouteFloor    = 0.2  // a pick less sure than this goes to the default class
	DefaultMaxInput      = 4096 // bytes of the input shown to the engine
)

// DefaultRouteInstructions is the question Router asks unless
// RouterConfig.Instructions replaces it.
const DefaultRouteInstructions = "Which model should handle this request?"

// Route is one class a Router may pick: an execution group's Class, and one
// line saying which requests it is for.
type Route struct {
	Class       string
	Description string
}

// RouterConfig configures a Router. As in PolicyConfig and CheckerConfig, a
// zero number takes the package default and a negative one turns the bound
// off.
type RouterConfig struct {
	// Routes are the candidate classes, at least two, in order of
	// preference: an engine breaks ties by it.
	Routes []Route
	// Default is the class used when the engine fails or is not sure
	// enough. It need not be one of Routes, but must be a class the machine
	// runs.
	Default string
	// Floor is the confidence the engine's pick needs (zero:
	// DefaultRouteFloor; negative: every pick is taken); below it Route
	// returns Default. It must be below 1.
	Floor float64
	// Instructions replaces DefaultRouteInstructions.
	Instructions string
	// Timeout bounds each engine request (zero: DefaultRouterTimeout; it
	// cannot be turned off).
	Timeout time.Duration
	// MaxInput is how many bytes of an input the engine is shown: a longer
	// one keeps its start and its end, with a marker where the middle was
	// cut (zero: DefaultMaxInput, which fits tev1:4b's 2050-token context
	// with room for the question, for English, and takes it a second or two
	// to read; negative: all of it).
	MaxInput int
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
	engine   Engine
	q        Question
	def      string
	floor    float64
	timeout  time.Duration
	maxInput int
}

// NewRouter checks cfg and builds a Router on engine.
func NewRouter(engine Engine, cfg RouterConfig) (*Router, error) {
	if engine == nil {
		return nil, fmt.Errorf("decide: a router needs an engine")
	}
	if strings.TrimSpace(cfg.Default) == "" {
		return nil, fmt.Errorf("decide: a router needs a default class")
	}
	floor, err := bound("router floor", cfg.Floor, DefaultRouteFloor)
	if err != nil {
		return nil, err
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
	if cfg.MaxInput == 0 {
		cfg.MaxInput = DefaultMaxInput
	}
	return &Router{engine: engine, q: q, def: cfg.Default, floor: floor, timeout: cfg.Timeout, maxInput: cfg.MaxInput}, nil
}

// Question is the choice the router asks, one option per route.
func (r *Router) Question() Question { return r.q }

// Route picks a class for input. It is the engine's pick when the engine
// answers with at least the floor's confidence, and the default otherwise:
// below the floor, err is nil and ans is the answer that was not trusted; on
// an engine error, err says why and ans is zero. Either way class is usable.
// An input longer than MaxInput is shown to the engine with its middle cut.
func (r *Router) Route(ctx context.Context, input string) (class string, ans Answer, err error) {
	if strings.TrimSpace(input) == "" {
		return r.def, Answer{}, fmt.Errorf("decide: nothing to route")
	}
	if r.maxInput > 0 {
		input = clipMiddle(input, r.maxInput)
	}
	answers, err := ask(ctx, r.engine, r.timeout, input, []Ask{{Name: RouteQuestion, Question: r.q}})
	if err != nil {
		return r.def, Answer{}, err
	}
	ans = answers[RouteQuestion]
	if !slices.Contains(r.q.Options, ans.Label) {
		return r.def, Answer{}, fmt.Errorf("decide: %s routed to %q, which is not a route", r.engine.Name(), ans.Label)
	}
	if ans.Confidence < r.floor {
		return r.def, ans, nil
	}
	return ans.Label, ans, nil
}
