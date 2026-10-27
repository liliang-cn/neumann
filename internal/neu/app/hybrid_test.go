package app

import (
	"context"
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/decide"
	"github.com/liliang-cn/neumann/decide/decidetest"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

func hybridFlags(t *testing.T, args ...string) Config {
	t.Helper()
	t.Setenv("CPA_BASE_URL", "https://cloud.example/v1")
	t.Setenv("CPA_MODEL", "cloud-model")
	t.Setenv("CPA_API_KEY", "k")
	var c Config
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c.Flags(fs)
	if err := fs.Parse(append([]string{"-backend", "hybrid"}, args...)); err != nil {
		t.Fatal(err)
	}
	c.MarkSet(fs)
	dir := t.TempDir()
	c.Root, c.Writable, c.Store, c.Quiet = dir, "out", filepath.Join(dir, ".neu", "state.db"), true
	return c
}

func reply(text string) *sim.Model {
	return &sim.Model{Name: "sim:" + text, Fn: func(model.Request) (model.Response, error) { return sim.Reply(text), nil }}
}

// -backend hybrid: a fast group configured like -backend local and a
// strong group like -backend cloud, each pinned at its vCores, fast
// falling back to strong.
func TestHybridConfig(t *testing.T) {
	c := hybridFlags(t, "-strong-cores", "3", "-fast-model", "small:4b")
	if err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
	sides := c.Sides()
	if len(sides) != 2 {
		t.Fatalf("sides %+v", sides)
	}
	fast, strong := sides[0], sides[1]
	if fast != (Side{Class: ClassFast, Kind: "local", Model: "small:4b", BaseURL: LocalBaseURL, Cores: 1, Parallel: 1, KeyEnv: LocalKeyEnv}) {
		t.Errorf("fast %+v", fast)
	}
	if strong != (Side{Class: ClassStrong, Kind: "cloud", API: "openai", Model: "cloud-model", BaseURL: "https://cloud.example/v1", Cores: 3, KeyEnv: "CPA_API_KEY"}) {
		t.Errorf("strong %+v", strong)
	}
	if c.Route != RouteStatic || !c.Fallback || c.Cores != 4 {
		t.Errorf("route %q fallback %v cores %d", c.Route, c.Fallback, c.Cores)
	}
	fa, sa := fast.Adapter().Adapter.(*openai.Adapter), strong.Adapter().Adapter.(*openai.Adapter)
	if fa.Parallel != 1 || !fa.Prefill || fa.ThinkOff != "none" || fa.ThinkTags != [2]string{"<think>", "</think>"} ||
		fa.Timeout != 0 || fa.FirstByteTimeout != localFirstByte || fa.IdleTimeout != localIdle || fa.MaxTokens != localMaxTokens {
		t.Errorf("fast adapter %+v", fa)
	}
	if sa.Parallel != 0 || sa.Prefill || sa.ThinkOff != "" || sa.Model != "cloud-model" || sa.APIKey != "k" {
		t.Errorf("strong adapter %+v", sa)
	}

	ctx := context.Background()
	a, err := Build(ctx, c, Options{Adapters: map[string]model.Adapter{ClassFast: reply("f"), ClassStrong: reply("s")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	st := a.M.Status()
	if len(st.Groups) != 2 {
		t.Fatalf("groups %+v", st.Groups)
	}
	for _, g := range st.Groups {
		switch g.Class {
		case ClassFast:
			if g.Target != 1 || g.Auto || g.Fallback != ClassStrong {
				t.Errorf("fast group %+v", g)
			}
		case ClassStrong:
			if g.Target != 3 || g.Auto || g.Fallback != "" {
				t.Errorf("strong group %+v", g)
			}
		default:
			t.Errorf("group %q", g.Class)
		}
	}

	// -fallback=false: fast's tasks wait for fast.
	c = hybridFlags(t, "-fallback=false")
	b, err := Build(ctx, c, Options{Adapter: reply("x")})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(5 * time.Second)
	for _, g := range b.M.Status().Groups {
		if g.Fallback != "" {
			t.Errorf("group %s falls back to %s", g.Class, g.Fallback)
		}
	}
}

func TestHybridRejectsOneGroupFlags(t *testing.T) {
	for _, f := range []string{"-model", "-base-url", "-cores"} {
		v := "x"
		if f == "-cores" {
			v = "2"
		}
		c := hybridFlags(t, f, v)
		if err := c.Resolve(); err == nil || !strings.Contains(err.Error(), "hybrid") {
			t.Errorf("%s: %v", f, err)
		}
	}
	c := hybridFlags(t, "-route", "random")
	if err := c.Resolve(); err == nil {
		t.Error("-route random was accepted")
	}
	t.Setenv("CPA_API_KEY", "")
	c = hybridFlags(t)
	t.Setenv("CPA_API_KEY", "")
	if err := c.Resolve(); err == nil {
		t.Error("hybrid without a cloud key was accepted")
	}
}

func buildHybrid(t *testing.T, c Config, opt Options) *App {
	t.Helper()
	if opt.Adapters == nil && opt.Adapter == nil {
		opt.Adapter = reply("ok")
	}
	a, err := Build(context.Background(), c, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(5 * time.Second) })
	return a
}

// -route static: the caller's class, fast when it has none; -route fast
// and strong send everything to one group; a one-group backend routes
// nothing.
func TestStaticRouting(t *testing.T) {
	ctx := context.Background()
	a := buildHybrid(t, hybridFlags(t), Options{})
	if p := a.Route(ctx, "anything", ""); p.Class != ClassFast || p.By != RouteStatic {
		t.Errorf("no hint: %+v", p)
	}
	if p := a.Route(ctx, "anything", ClassStrong); p.Class != ClassStrong || p.By != RouteStatic {
		t.Errorf("strong hint: %+v", p)
	}
	if p, err := a.Force(ClassStrong); err != nil || p.Class != ClassStrong || p.By != "forced" {
		t.Errorf("forced: %+v %v", p, err)
	}
	for _, mode := range []string{ClassFast, ClassStrong} {
		b := buildHybrid(t, hybridFlags(t, "-route", mode), Options{})
		if p := b.Route(ctx, "anything", ClassFast); p.Class != mode {
			t.Errorf("-route %s: %+v", mode, p)
		}
		if p := b.Route(ctx, "anything", ClassStrong); p.Class != mode {
			t.Errorf("-route %s: %+v", mode, p)
		}
	}
	one := hybridFlags(t)
	one.Backend = "cloud"
	o := buildHybrid(t, one, Options{})
	if p := o.Route(ctx, "anything", ClassStrong); p.Class != "" {
		t.Errorf("one group: %+v", p)
	}
	if _, err := o.Force(ClassFast); err == nil {
		t.Error("/fast on a one-group backend was accepted")
	}
}

// -route decide asks the engine with the two routes; an engine that fails
// or is unsure sends the task to strong.
func TestDecideRouting(t *testing.T) {
	ctx := context.Background()
	engine := &decidetest.Engine{Fn: func(state string, asks []decide.Ask) (map[string]decide.Answer, error) {
		switch {
		case strings.Contains(state, "How many lines"):
			return map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick(ClassFast, 0.9)}, nil
		case strings.Contains(state, "maybe"):
			return map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick(ClassFast, 0.05)}, nil
		case strings.Contains(state, "broken"):
			return nil, errors.New("engine down")
		}
		return map[string]decide.Answer{decide.RouteQuestion: decidetest.Pick(ClassStrong, 0.7)}, nil
	}}
	a := buildHybrid(t, hybridFlags(t, "-route", "decide"), Options{RouteEngine: engine})
	if p := a.Route(ctx, "How many lines does data/sales.csv have?", ClassStrong); p.Class != ClassFast || p.By != RouteDecide || p.Conf != 0.9 {
		t.Errorf("lines: %+v", p)
	}
	if p := a.Route(ctx, "Summarise the incident in logs/service.log", ClassFast); p.Class != ClassStrong || p.By != RouteDecide {
		t.Errorf("log: %+v", p)
	}
	if p := a.Route(ctx, "maybe", ""); p.Class != ClassStrong || p.By != "default" || p.Label != ClassFast {
		t.Errorf("unsure: %+v", p)
	}
	if p := a.Route(ctx, "broken", ""); p.Class != ClassStrong || p.By != "default" || !strings.Contains(p.Err, "engine down") {
		t.Errorf("failed: %+v", p)
	}
	asked := engine.Asked()
	if len(asked) != 4 {
		t.Fatalf("engine asked %d times", len(asked))
	}
	q, ok := asked[0].Question(decide.RouteQuestion)
	if !ok || strings.Join(q.Options, ",") != "fast,strong" || q.Descriptions[ClassFast] != FastRoute || q.Descriptions[ClassStrong] != StrongRoute {
		t.Errorf("question %+v", q)
	}
}

// While fast is down (its breaker open after consecutive transient
// failures), a fast task moves to strong and finishes there.
func TestFallbackMovesFastTasksToStrong(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	down := &sim.Model{Name: "sim:down", Fn: func(model.Request) (model.Response, error) {
		return model.Response{}, &model.Error{Class: model.Transient, Err: errors.New("connection refused")}
	}}
	a := buildHybrid(t, hybridFlags(t), Options{
		Adapters: map[string]model.Adapter{ClassFast: down, ClassStrong: reply("answered by strong")},
		Tune: func(c *neumann.Config) {
			c.BackoffBase, c.BackoffMax = time.Millisecond, 5*time.Millisecond
			c.Groups[0].BreakerThreshold = 2
		},
	})
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	tr := a.Track(ctx, 0)
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: "t1", Input: "quick question", Class: a.Route(ctx, "quick question", "").Class}); err != nil {
		t.Fatal(err)
	}
	ev, err := tr.Wait(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	tc, err := a.M.Load(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != proto.EvDone || tc.Class != ClassStrong || tc.LastAnswer() != "answered by strong" {
		t.Fatalf("ended %s on %s: %q", ev.Kind, tc.Class, tc.LastAnswer())
	}
	var moved bool
	for _, e := range tr.Events("t1") {
		if e.Kind == proto.EvMigrated {
			from, _ := Field[string](e, "from")
			to, _ := Field[string](e, "to")
			reason, _ := Field[string](e, "reason")
			moved = from == ClassFast && to == ClassStrong && reason == "group_down"
		}
	}
	if !moved {
		t.Error("no migrated event fast → strong (group_down)")
	}
	var downEv bool
	for _, e := range tr.Machine() {
		if c, _ := Field[string](e, "class"); e.Kind == proto.EvGroupDown && c == ClassFast {
			downEv = true
		}
	}
	if !downEv {
		t.Error("no group_down for fast")
	}
}
