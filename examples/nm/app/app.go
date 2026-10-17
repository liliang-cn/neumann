// Package app builds nm's machine: the backend (a local Ollama, a cloud
// OpenAI-compatible endpoint, or both as two execution groups with a
// router between them), the SQLite store, the workspace tools and the
// optional observe endpoints. The CLI and the bench harness share it.
package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/decide"
	"github.com/liliang-cn/neumann/examples/nm/tools"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
)

// Classes of -backend hybrid's two execution groups, and -route's modes
// besides "everything to one of them".
const (
	ClassFast   = "fast"
	ClassStrong = "strong"
	RouteStatic = "static"
	RouteDecide = "decide"

	// HybridFastModel is the fast group's default model: measured on the
	// bench, it answers short lookups well and quickly and fails long
	// analyses.
	HybridFastModel = "qwen3.5:4b"
)

// What -backend local sets: the same as cmd/neumann -local (design §6.4,
// §11): one request at a time, preemption, continuation of a cut-off
// answer or thought, thinking turned off with reasoning_effort "none", a
// generous first-byte limit, a short idle limit and no total limit.
const (
	LocalBaseURL   = "http://127.0.0.1:11434/v1"
	LocalModel     = "qwen3.5:9b-mlx"
	localThinkOff  = "none"
	localFirstByte = 10 * time.Minute
	localIdle      = 2 * time.Minute
	localMaxTokens = 8192
)

// System is nm's system prompt. It is generic on purpose: the bench
// measures the framework and the model on ordinary requests, not prompts
// tuned per task.
const System = `You are nm, a careful assistant that works on the files of a local workspace through tools.
- Every path is relative to the workspace root. Look at files with the tools instead of guessing; quote what you found.
- Prefer fs_grep to find things and fs_read to read the parts you need; read big files in pages.
- Use calc_eval for any arithmetic over more than two numbers; never add up long columns in your head.
- You can only write files where fs_write allows it. Never edit tests to make them pass.
- Finish with a short, direct answer that gives the exact values, names and file:line locations asked for.`

// Config is what the flags set.
type Config struct {
	Backend  string // local, cloud or hybrid
	Model    string
	BaseURL  string
	Cores    int
	Parallel int // requests a local backend serves at once
	Store    string
	Root     string
	Writable string // comma-separated, relative to Root
	MaxSteps int

	// -backend hybrid: two execution groups, fast on a local model and
	// strong on the cloud endpoint, and how tasks are routed between them.
	FastModel     string
	FastBaseURL   string
	FastCores     int
	StrongModel   string
	StrongBaseURL string
	StrongCores   int
	Fallback      bool   // fast's tasks move to strong while fast is down
	Route         string // static, decide, fast or strong
	RouterURL     string // decide: the System One server
	RouterModel   string // decide: the decision model
	RouteTimeout  time.Duration

	ObserveAddr string
	OTLP        bool
	Quiet       bool // no machine log lines on stderr

	set map[string]bool
}

// Flags registers the shared flags on fs.
func (c *Config) Flags(fs *flag.FlagSet) {
	fs.StringVar(&c.Backend, "backend", envOr("NM_BACKEND", "cloud"), "local (Ollama "+LocalModel+"), cloud (CPA_BASE_URL, CPA_API_KEY, CPA_MODEL) or hybrid (a fast local group and a strong cloud group) [NM_BACKEND]")
	fs.StringVar(&c.Model, "model", "", "model name; default: "+LocalModel+" (local) or $CPA_MODEL (cloud)")
	fs.StringVar(&c.BaseURL, "base-url", "", "endpoint up to and including /v1; default: "+LocalBaseURL+" (local) or $CPA_BASE_URL (cloud)")
	fs.IntVar(&c.Cores, "cores", 0, "vCores; default -parallel (local) or 8 (cloud)")
	fs.IntVar(&c.Parallel, "parallel", 1, "local, and hybrid's fast group: requests the backend serves at once (Ollama's MLX runner serves one; a GGUF model, OLLAMA_NUM_PARALLEL)")
	fs.StringVar(&c.Store, "store", filepath.Join(".nm", "state.db"), "SQLite file holding the machine's state")
	fs.StringVar(&c.Root, "root", ".", "workspace root; every tool is confined to it")
	fs.StringVar(&c.Writable, "writable", "out", "comma-separated directories under the root where fs_write may write")
	fs.IntVar(&c.MaxSteps, "max-steps", 40, "model calls a task may make")
	fs.StringVar(&c.FastModel, "fast-model", "", "hybrid: the fast group's local model; default "+HybridFastModel)
	fs.StringVar(&c.FastBaseURL, "fast-base-url", "", "hybrid: the fast group's endpoint; default "+LocalBaseURL)
	fs.IntVar(&c.FastCores, "fast-cores", 0, "hybrid: the fast group's vCores; default -parallel")
	fs.StringVar(&c.StrongModel, "strong-model", "", "hybrid: the strong group's cloud model; default $CPA_MODEL")
	fs.StringVar(&c.StrongBaseURL, "strong-base-url", "", "hybrid: the strong group's endpoint; default $CPA_BASE_URL")
	fs.IntVar(&c.StrongCores, "strong-cores", 0, "hybrid: the strong group's vCores; default 8")
	fs.BoolVar(&c.Fallback, "fallback", true, "hybrid: while the fast group is down (its breaker open), its tasks move to the strong group")
	fs.StringVar(&c.Route, "route", RouteStatic, "hybrid: how a task picks its group: static (by kind; the REPL's default is fast, /strong overrides), decide (a decision model reads the request), fast or strong (everything to one group)")
	fs.StringVar(&c.RouterURL, "router-url", decide.DefaultURL, "-route decide: the Ollama server with the System One API")
	fs.StringVar(&c.RouterModel, "router-model", decide.DefaultModel, "-route decide: the decision model")
	fs.DurationVar(&c.RouteTimeout, "route-timeout", 0, "-route decide: how long one routing decision may take before the task goes to strong (0: the router's default, 3s)")
	fs.StringVar(&c.ObserveAddr, "observe-addr", "", "serve the observe HTTP view (/status, /queue, /tasks/{id}, /events, /metrics) here, e.g. 127.0.0.1:43562; empty: off")
	fs.BoolVar(&c.OTLP, "otlp", false, "export traces and metrics over OTLP/HTTP as the standard OTEL_EXPORTER_OTLP_* variables say")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// MarkSet records which flags were given on the command line.
func (c *Config) MarkSet(fs *flag.FlagSet) {
	c.set = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { c.set[f.Name] = true })
}

// Resolve fills the backend defaults and checks that the keys it needs are
// in the environment. Keys are read from the environment when the adapters
// are built, never stored in Config (which reports print).
func (c *Config) Resolve() error { return c.resolve(true) }

// resolve is Resolve; without needKeys (scripted adapters) a missing cloud
// endpoint, model or key is not an error.
func (c *Config) resolve(needKeys bool) error {
	switch c.Backend {
	case "local":
		if c.Model == "" {
			c.Model = LocalModel
		}
		if c.BaseURL == "" {
			c.BaseURL = LocalBaseURL
		}
		if c.Parallel < 1 {
			return errors.New("-parallel must be at least 1")
		}
		if c.Cores == 0 {
			c.Cores = c.Parallel
		}
	case "cloud":
		if c.Model == "" {
			c.Model = os.Getenv("CPA_MODEL")
		}
		if c.BaseURL == "" {
			c.BaseURL = os.Getenv("CPA_BASE_URL")
		}
		if c.Cores == 0 {
			c.Cores = 8
		}
		if needKeys && (c.Model == "" || c.BaseURL == "" || cloudKey() == "") {
			return errors.New("cloud backend needs CPA_BASE_URL, CPA_API_KEY and CPA_MODEL (or -base-url and -model)")
		}
	case "hybrid":
		for _, f := range []string{"model", "base-url", "cores"} {
			if c.set[f] {
				return fmt.Errorf("-%s does not apply to -backend hybrid: use -fast-%s and -strong-%s", f, f, f)
			}
		}
		if c.Parallel < 1 {
			return errors.New("-parallel must be at least 1")
		}
		if c.FastModel == "" {
			c.FastModel = HybridFastModel
		}
		if c.FastBaseURL == "" {
			c.FastBaseURL = LocalBaseURL
		}
		if c.FastCores == 0 {
			c.FastCores = c.Parallel
		}
		if c.StrongModel == "" {
			c.StrongModel = os.Getenv("CPA_MODEL")
		}
		if c.StrongBaseURL == "" {
			c.StrongBaseURL = os.Getenv("CPA_BASE_URL")
		}
		if c.StrongCores == 0 {
			c.StrongCores = 8
		}
		if needKeys && (c.StrongModel == "" || c.StrongBaseURL == "" || cloudKey() == "") {
			return errors.New("hybrid backend's strong group needs CPA_BASE_URL, CPA_API_KEY and CPA_MODEL (or -strong-base-url and -strong-model)")
		}
		if c.FastCores < 1 || c.StrongCores < 1 {
			return errors.New("-fast-cores and -strong-cores must be at least 1")
		}
		switch c.Route {
		case "":
			c.Route = RouteStatic
		case RouteStatic, RouteDecide, ClassFast, ClassStrong:
		default:
			return fmt.Errorf("unknown -route %q: want static, decide, fast or strong", c.Route)
		}
		c.Model, c.BaseURL, c.Cores = "", "", c.FastCores+c.StrongCores
	default:
		return fmt.Errorf("unknown -backend %q: want local, cloud or hybrid", c.Backend)
	}
	if c.Cores < 1 {
		return errors.New("-cores must be at least 1")
	}
	return nil
}

func cloudKey() string { return os.Getenv("CPA_API_KEY") }

// Side is one model backend of the machine: an execution group.
type Side struct {
	Class    string // the group's class: neumann.DefaultClass for local and cloud, fast or strong for hybrid
	Kind     string // local or cloud
	Model    string
	BaseURL  string
	Cores    int
	Parallel int // local: requests the backend serves at once
}

// Sides are the groups the resolved config describes: one for local and
// cloud, fast and strong for hybrid.
func (c *Config) Sides() []Side {
	switch c.Backend {
	case "hybrid":
		return []Side{
			{Class: ClassFast, Kind: "local", Model: c.FastModel, BaseURL: c.FastBaseURL, Cores: c.FastCores, Parallel: c.Parallel},
			{Class: ClassStrong, Kind: "cloud", Model: c.StrongModel, BaseURL: c.StrongBaseURL, Cores: c.StrongCores},
		}
	case "local":
		return []Side{{Class: neumann.DefaultClass, Kind: "local", Model: c.Model, BaseURL: c.BaseURL, Cores: c.Cores, Parallel: c.Parallel}}
	}
	return []Side{{Class: neumann.DefaultClass, Kind: "cloud", Model: c.Model, BaseURL: c.BaseURL, Cores: c.Cores}}
}

// Describe is the backend in one line, for logs and reports.
func (c *Config) Describe() string {
	if c.Backend != "hybrid" {
		return fmt.Sprintf("%s (%s at %s), %d vCore(s)", c.Backend, c.Model, c.BaseURL, c.Cores)
	}
	fb := "no fallback"
	if c.Fallback {
		fb = "fast falls back to strong"
	}
	route := c.Route
	if route == RouteDecide {
		route += " (" + c.RouterModel + ")"
	}
	return fmt.Sprintf("hybrid: fast %s at %s, %d vCore(s); strong %s at %s, %d vCore(s); route %s; %s",
		c.FastModel, c.FastBaseURL, c.FastCores, c.StrongModel, c.StrongBaseURL, c.StrongCores, route, fb)
}

// BackendArgs are the flags that rebuild this backend in another process
// (the crash scenario's child).
func (c *Config) BackendArgs() []string {
	if c.Backend != "hybrid" {
		return []string{"-backend", c.Backend, "-model", c.Model, "-base-url", c.BaseURL, "-cores", strconv.Itoa(c.Cores), "-parallel", strconv.Itoa(c.Parallel)}
	}
	return []string{"-backend", "hybrid", "-parallel", strconv.Itoa(c.Parallel),
		"-fast-model", c.FastModel, "-fast-base-url", c.FastBaseURL, "-fast-cores", strconv.Itoa(c.FastCores),
		"-strong-model", c.StrongModel, "-strong-base-url", c.StrongBaseURL, "-strong-cores", strconv.Itoa(c.StrongCores),
		"-fallback=" + strconv.FormatBool(c.Fallback), "-route", c.Route, "-router-url", c.RouterURL, "-router-model", c.RouterModel,
		"-route-timeout", c.RouteTimeout.String()}
}

// Adapter builds the side's model adapter, wrapped to record call times.
// A local side is configured as cmd/neumann -local configures one; a cloud
// side keeps the adapter's defaults.
func (s Side) Adapter() *Timed {
	key := cloudKey()
	if s.Kind == "local" {
		key = envOr("NM_API_KEY", "ollama")
	}
	a := openai.New(s.BaseURL, key, s.Model)
	if s.Kind == "local" {
		a.Parallel = s.Parallel
		a.Prefill, a.ThinkOff = true, localThinkOff
		a.ThinkTags = [2]string{"<think>", "</think>"}
		a.Timeout, a.FirstByteTimeout, a.IdleTimeout = 0, localFirstByte, localIdle
		a.MaxTokens = localMaxTokens
	}
	return &Timed{Adapter: a, Class: s.Class}
}

// App is a built machine and what hangs off it.
type App struct {
	Cfg   Config
	M     *neumann.Machine
	Tools *tools.Tools
	Model model.Adapter // the default group's adapter
	// Models are every group's adapter, by class.
	Models map[string]model.Adapter
	timed  []*Timed // the adapters that are the timed OpenAI-compatible one
	Store  *sqlite.Store
	router *router // hybrid only

	mu       sync.Mutex
	toolRecs []tools.CallStat
	trackers []*Tracker
	obs      *observing
	logw     *log.Logger
}

// Options for Build beyond the flags.
type Options struct {
	// Logf receives the machine's log lines; nil: stderr unless Quiet.
	Logf func(string, ...any)
	// OnTool is told about every finished tool call, after it is recorded.
	OnTool func(tools.CallStat)
	// Adapter replaces the configured backend (tests use a scripted
	// model); the API key is then not needed and calls are not timed.
	// With -backend hybrid it replaces every group's adapter that Adapters
	// does not name.
	Adapter model.Adapter
	// Adapters replace the configured backend of the groups they name, by
	// class (hybrid: fast, strong).
	Adapters map[string]model.Adapter
	// RouteEngine replaces the System One engine of -route decide.
	RouteEngine decide.Engine
	// Tune, if set, may change the machine's config just before it is
	// built (tests shorten backoffs and breakers).
	Tune func(*neumann.Config)
}

func (o Options) adapterFor(class string) model.Adapter {
	if a := o.Adapters[class]; a != nil {
		return a
	}
	return o.Adapter
}

// Build opens the store, attaches the tools and builds the machine. It
// does not Start it.
func Build(ctx context.Context, cfg Config, opt Options) (*App, error) {
	scripted := opt.Adapter != nil || len(opt.Adapters) > 0
	if err := cfg.resolve(!scripted); err != nil && !scripted {
		return nil, err
	}
	box, err := tools.NewSandbox(cfg.Root, strings.Split(cfg.Writable, ","))
	if err != nil {
		return nil, err
	}
	tl, err := tools.New(box)
	if err != nil {
		return nil, err
	}
	a := &App{Cfg: cfg, Tools: tl, Models: map[string]model.Adapter{}}
	sides := cfg.Sides()
	for _, s := range sides {
		m := opt.adapterFor(s.Class)
		if m == nil {
			t := s.Adapter()
			a.timed = append(a.timed, t)
			m = t
		}
		a.Models[s.Class] = m
	}
	a.Model = a.Models[sides[0].Class]
	tl.OnCall = func(s tools.CallStat) {
		a.mu.Lock()
		a.toolRecs = append(a.toolRecs, s)
		a.mu.Unlock()
		if opt.OnTool != nil {
			opt.OnTool(s)
		}
	}
	if cfg.Backend == "hybrid" {
		if a.router, err = newRouter(ctx, cfg, opt.RouteEngine); err != nil {
			return nil, err
		}
	}
	if dir := filepath.Dir(cfg.Store); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	st, err := sqlite.Open(cfg.Store)
	if err != nil {
		return nil, err
	}
	a.Store = st
	logf := opt.Logf
	if logf == nil {
		if cfg.Quiet {
			logf = func(string, ...any) {}
		} else {
			a.logw = log.New(os.Stderr, "nm: ", log.Ltime)
			logf = a.logw.Printf
		}
	}
	mc := neumann.Config{
		Devices: tl.Devices(),
		Store:   st,
		System:  System,
		Logf:    logf,
	}
	if cfg.Backend == "hybrid" {
		mc.Groups, mc.Default = a.groups(sides), ClassFast
	} else {
		mc.Model, mc.Cores, mc.Preempt = a.Model, cfg.Cores, cfg.Backend == "local"
	}
	if opt.Tune != nil {
		opt.Tune(&mc)
	}
	m, err := neumann.New(mc)
	if err != nil {
		st.Close()
		return nil, err
	}
	a.M = m
	return a, nil
}

// groups are hybrid's execution groups. Each runs at most its side's
// vCores (pinned once the machine starts, as the one-group backends pin
// theirs). The local group preempts, as -backend local does; the cloud
// group does not (a cut-off call is billed and not counted). With
// Fallback, fast's tasks move to strong while fast's breaker is open
// (design §6.3): the cloud adapter takes tools and reports no window, so
// the move is never refused; a continuation prefix the local model left
// is dropped there (the cloud adapter does not continue), and the next
// call starts that step afresh. Strong has no fallback: a cloud task
// waits for the cloud to come back rather than land on a small model.
func (a *App) groups(sides []Side) []neumann.GroupConfig {
	var gs []neumann.GroupConfig
	for _, s := range sides {
		g := neumann.GroupConfig{Class: s.Class, Model: a.Models[s.Class], Max: s.Cores, Initial: float64(s.Cores)}
		if s.Kind == "local" {
			g.Preempt = true
		}
		if s.Class == ClassFast && a.Cfg.Fallback {
			g.Fallback = ClassStrong
		}
		gs = append(gs, g)
	}
	return gs
}

// Start starts the machine (recovering unfinished tasks) and the observe
// endpoints.
func (a *App) Start(ctx context.Context) error {
	if err := a.M.Start(ctx); err != nil {
		return err
	}
	if a.Cfg.Backend == "hybrid" {
		for _, s := range a.Cfg.Sides() {
			if err := a.M.SetCores(s.Class, s.Cores); err != nil {
				return err
			}
		}
	}
	obs, err := startObserving(ctx, a.Cfg, a.M, func(f string, args ...any) { fmt.Fprintf(os.Stderr, "nm: "+f+"\n", args...) })
	if err != nil {
		return err
	}
	a.obs = obs
	return nil
}

// Close stops the machine within timeout, then the observers, then the
// store.
func (a *App) Close(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := a.M.Stop(ctx)
	if a.obs != nil {
		a.obs.stop()
	}
	// The machine's event subscribers read the store until they have
	// everything committed; the store closes after them.
	a.mu.Lock()
	trs := a.trackers
	a.mu.Unlock()
	for _, t := range trs {
		select {
		case <-t.closed:
		case <-ctx.Done():
		}
	}
	return errors.Join(err, a.Store.Close())
}

// CallRecords returns every model call the adapter made so far; none when
// the adapter is not timed.
func (a *App) CallRecords() []CallRecord {
	var out []CallRecord
	for _, t := range a.timed {
		out = append(out, t.Records()...)
	}
	return out
}

// Side is the configured side of a class, and whether there is one.
func (a *App) Side(class string) (Side, bool) {
	for _, s := range a.Cfg.Sides() {
		if s.Class == class {
			return s, true
		}
	}
	return Side{}, false
}

// ToolRecords returns every tool call the devices finished so far.
func (a *App) ToolRecords() []tools.CallStat {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]tools.CallStat(nil), a.toolRecs...)
}
