package app

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/decide"
)

// The routes -route decide describes to the decision model: what each
// group is for, in the words of the requests that go there. The fast
// group's model answers short lookups well (the bench's quick probes and
// repo questions) and fails long analyses (a 4k-line log, a 3k-row CSV, a
// changelog over many commits); the strong group's does both, billed.
// They describe kinds of work, not the suite's tasks: tuning them to the
// suite would measure the tuning.
const (
	FastRoute   = "short lookups, one or two files, quick questions"
	StrongRoute = "reading large data, counting, multi-step analysis, writing summaries of many items"
)

// Pick is where a task goes and why.
type Pick struct {
	Class string        `json:"class,omitempty"` // empty: the machine's default (one-group backends)
	By    string        `json:"by,omitempty"`    // static, decide, default (the engine failed or was unsure), forced, route=<class>
	Label string        `json:"label,omitempty"` // decide: what the engine said
	Conf  float64       `json:"confidence,omitempty"`
	Took  time.Duration `json:"took_ns,omitempty"` // decide: the engine's time
	Err   string        `json:"error,omitempty"`
}

func (p Pick) String() string {
	if p.Class == "" {
		return ""
	}
	switch p.By {
	case RouteDecide:
		return fmt.Sprintf("%s (decide p=%.2f, %s)", p.Class, p.Conf, p.Took.Round(time.Millisecond))
	case "default":
		why := "the engine was unsure: " + p.Label + fmt.Sprintf(" p=%.2f", p.Conf)
		if p.Err != "" {
			why = p.Err
		}
		return fmt.Sprintf("%s (default, %s; %s)", p.Class, p.Took.Round(time.Millisecond), why)
	}
	return fmt.Sprintf("%s (%s)", p.Class, p.By)
}

// router is hybrid's routing: a mode and, for decide, the decision model.
type router struct {
	mode string
	dec  *decide.Router
}

func newRouter(ctx context.Context, cfg Config, engine decide.Engine) (*router, error) {
	r := &router{mode: cfg.Route}
	if r.mode != RouteDecide {
		return r, nil
	}
	if engine == nil {
		so := decide.NewSystemOne(decide.SystemOneConfig{URL: cfg.RouterURL, Model: cfg.RouterModel, KeepAlive: 30 * time.Minute})
		// A cold load takes seconds, longer than a routing decision may
		// wait: load it now. A server without the model is not fatal:
		// every decision then fails and its task goes to strong.
		wctx, cancel := context.WithTimeout(ctx, time.Minute)
		err := so.Ready(wctx)
		if err == nil {
			err = so.Warm(wctx)
		}
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "nm: router %s: %v; tasks go to %s until it answers\n", so.Name(), err, ClassStrong)
		}
		engine = so
	}
	dec, err := decide.NewRouter(engine, decide.RouterConfig{
		Routes: []decide.Route{
			{Class: ClassFast, Description: FastRoute},
			{Class: ClassStrong, Description: StrongRoute},
		},
		Default: ClassStrong,
		Timeout: cfg.RouteTimeout,
		Floor:   cfg.RouteFloor,
	})
	if err != nil {
		return nil, err
	}
	r.dec = dec
	return r, nil
}

// Route picks the group for a task's input. static is what the caller's
// static rule says (the bench: by task kind); empty means fast, the
// default for anything typed at the REPL. With a one-group backend the
// pick is empty: the task goes to the machine's only group.
func (a *App) Route(ctx context.Context, input, static string) Pick {
	if a.router == nil {
		return Pick{}
	}
	switch a.router.mode {
	case ClassFast, ClassStrong:
		return Pick{Class: a.router.mode, By: "route=" + a.router.mode}
	case RouteStatic:
		if static == "" {
			static = ClassFast
		}
		return Pick{Class: static, By: RouteStatic}
	}
	start := time.Now()
	class, ans, err := a.router.dec.Route(ctx, input)
	p := Pick{Class: class, By: RouteDecide, Label: ans.Label, Conf: ans.Confidence, Took: time.Since(start)}
	if err != nil {
		p.By, p.Err = "default", err.Error()
	} else if ans.Label != class {
		p.By = "default"
	}
	return p
}

// Force is a pick the user made (the REPL's /fast and /strong).
func (a *App) Force(class string) (Pick, error) {
	if a.router == nil {
		return Pick{}, fmt.Errorf("/%s needs -backend hybrid", class)
	}
	return Pick{Class: class, By: "forced"}, nil
}

// Hybrid reports whether the machine has the fast and strong groups.
func (a *App) Hybrid() bool { return a.router != nil }

// RouteMode is -route, or "" for a one-group backend.
func (a *App) RouteMode() string {
	if a.router == nil {
		return ""
	}
	return a.router.mode
}

// Routes describes the two routes, for /help.
func Routes() string {
	return strings.Join([]string{ClassFast + ": " + FastRoute, ClassStrong + ": " + StrongRoute}, "\n")
}
