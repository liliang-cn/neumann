// Package app builds neu's machine: the backend (a local Ollama, a cloud
// OpenAI-compatible or Anthropic endpoint, or a local and a cloud one as
// two execution groups with a router between them), the SQLite store, the
// workspace tools and the optional observe endpoints. The neu CLI
// (cmd/neu) and the bench harness (acceptance/bench) share it.
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
	"github.com/liliang-cn/neumann/internal/neu/tools"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/anthropic"
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
	LocalModel     = "qwen3.5:9b-mlx" // the bench's local model
	LocalKeyEnv    = "NEU_API_KEY"    // a local backend's key; unset: "ollama"
	localThinkOff  = "none"
	localFirstByte = 10 * time.Minute
	localIdle      = 2 * time.Minute
	localMaxTokens = 8192
)

// System is neu's system prompt. It is generic on purpose: the bench
// measures the framework and the model on ordinary requests, not prompts
// tuned per task.
const System = `You are neu, a careful assistant that works on the files of a local workspace through tools.
- Every path is relative to the workspace root. Look at files with the tools instead of guessing; quote what you found.
- Prefer fs_grep to find things and fs_read to read the parts you need; read big files in pages.
- Use calc_eval for any arithmetic over more than two numbers; never add up long columns in your head.
- You can only write files where fs_write allows it. Never edit tests to make them pass.
- Finish with a short, direct answer that gives the exact values, names and file:line locations asked for.`

// The openai backend's endpoint and key variable for neu (UserDefaults),
// overridden by OPENAI_BASE_URL, then by -base-url. neu has no default
// model for any backend: it must be given (-model, the config file, or
// OPENAI_MODEL / ANTHROPIC_MODEL).
const (
	OpenAIBaseURL = "https://api.openai.com/v1"
	OpenAIKeyEnv  = "OPENAI_API_KEY"
)

// Defaults are what a program fills in when the flags leave a backend's
// model, endpoint or key variable empty. The bench and neu differ: the
// bench measures against a fixed gateway (CPA_*) and the 9B local model;
// neu has no default model at all: an empty model is an error.
type Defaults struct {
	Backend        string // used when -backend is empty; "": none
	LocalModel     string
	FastModel      string // hybrid's fast group
	OpenAIBaseURL  string
	OpenAIModel    string
	OpenAIKeyEnv   string
	AnthropicModel string
}

// BenchDefaults are the bench's defaults, and those of a Config whose Def
// is zero: -backend openai on CPA_BASE_URL, CPA_MODEL and CPA_API_KEY, and
// LocalModel for -backend local.
func BenchDefaults() Defaults {
	return Defaults{
		Backend:        "openai",
		LocalModel:     LocalModel,
		FastModel:      HybridFastModel,
		OpenAIBaseURL:  os.Getenv("CPA_BASE_URL"),
		OpenAIModel:    os.Getenv("CPA_MODEL"),
		OpenAIKeyEnv:   "CPA_API_KEY",
		AnthropicModel: AnthropicModel,
	}
}

// UserDefaults are neu's: no backend (the CLI picks one), no model (it
// must be given explicitly; OPENAI_MODEL and ANTHROPIC_MODEL count as
// given), and OpenAI's endpoint (OPENAI_BASE_URL) and key variable for
// -backend openai and hybrid's strong group.
func UserDefaults() Defaults {
	return Defaults{
		OpenAIBaseURL:  envOr("OPENAI_BASE_URL", OpenAIBaseURL),
		OpenAIModel:    os.Getenv("OPENAI_MODEL"),
		OpenAIKeyEnv:   OpenAIKeyEnv,
		AnthropicModel: os.Getenv("ANTHROPIC_MODEL"),
	}
}

// Config is what the flags set.
type Config struct {
	// Def are the program's defaults; zero: BenchDefaults. Set it before
	// Flags, whose help shows them.
	Def Defaults

	Backend  string // local, openai (alias cloud), anthropic or hybrid
	Model    string
	BaseURL  string
	KeyEnv   string // the environment variable holding the API key
	Cores    int
	Parallel int // requests a local backend serves at once
	Store    string
	Root     string
	Writable string // comma-separated, relative to Root
	MaxSteps int
	// ContextWindow is the model's context window in tokens (input and
	// output together), which compaction sizes by (design §5.3); 0: the
	// adapter's own (anthropic 200000, openai unknown), or for local what
	// DetectWindows reads from Ollama.
	ContextWindow int

	// -backend hybrid: two execution groups, fast on a local model and
	// strong on the cloud endpoint, and how tasks are routed between them.
	FastModel     string
	FastBaseURL   string
	FastCores     int
	StrongModel   string
	StrongBaseURL string
	StrongCores   int
	StrongAPI     string // openai or anthropic
	StrongKeyEnv  string
	// The groups' context windows, as ContextWindow is the one-group
	// backends'.
	FastContextWindow   int
	StrongContextWindow int
	Fallback            bool   // fast's tasks move to strong while fast is down
	Route               string // static, decide, fast or strong
	RouterURL           string // decide: the System One server
	RouterModel         string // decide: the decision model
	RouteTimeout        time.Duration
	RouteFloor          float64 // decide: the confidence a pick needs (0: the router's default; negative: any)

	ObserveAddr string
	OTLP        bool
	Quiet       bool // no machine log lines on stderr

	set map[string]bool
}

// defaults are c.Def, or BenchDefaults when it is zero.
func (c *Config) defaults() Defaults {
	if c.Def == (Defaults{}) {
		return BenchDefaults()
	}
	return c.Def
}

// Flags registers the shared flags on fs.
func (c *Config) Flags(fs *flag.FlagSet) {
	d := c.defaults()
	backendDef := "default " + d.Backend
	if d.Backend == "" {
		backendDef = "default: see above"
	}
	openaiURL := or(d.OpenAIBaseURL, "(none)")
	modelDef := func(m string) string { return or(m, "none, required") }
	fs.StringVar(&c.Backend, "backend", "", "local (Ollama), openai (any OpenAI-compatible endpoint; also called cloud), anthropic (the Messages API) or hybrid (a fast local group and a strong group); "+backendDef+". HTTPS_PROXY is honoured")
	fs.StringVar(&c.Model, "model", "", "model name; default: "+modelDef(d.LocalModel)+" (local), "+modelDef(d.OpenAIModel)+" (openai), "+modelDef(d.AnthropicModel)+" (anthropic)")
	fs.StringVar(&c.BaseURL, "base-url", "", "endpoint up to and including /v1; default: "+LocalBaseURL+" (local), "+openaiURL+" (openai) or "+AnthropicBaseURL+" (anthropic)")
	fs.StringVar(&c.KeyEnv, "key-env", "", "name of the environment variable holding the API key (the key itself is never a flag); default: "+LocalKeyEnv+" (local, unset: \"ollama\"), "+d.OpenAIKeyEnv+" (openai) or ANTHROPIC_API_KEY (anthropic)")
	fs.IntVar(&c.Cores, "cores", 0, "vCores; default -parallel (local) or 8 (openai, anthropic)")
	fs.IntVar(&c.Parallel, "parallel", 1, "local, and hybrid's fast group: requests the backend serves at once (Ollama's MLX runner serves one; a GGUF model, OLLAMA_NUM_PARALLEL)")
	fs.StringVar(&c.Store, "store", "", "SQLite file holding the machine's state; default <root>/.neu/state.db")
	fs.StringVar(&c.Root, "root", ".", "workspace root; every tool is confined to it")
	fs.StringVar(&c.Writable, "writable", "out", "comma-separated directories under the root where fs_write may write")
	fs.IntVar(&c.MaxSteps, "max-steps", 40, "model calls a task may make")
	fs.IntVar(&c.ContextWindow, "context-window", 0, "the model's context window in tokens; a task is compacted once its request and output reserve pass 80% of it. 0: "+windowDefaults)
	fs.StringVar(&c.FastModel, "fast-model", "", "hybrid: the fast group's local model; default "+modelDef(d.FastModel))
	fs.StringVar(&c.FastBaseURL, "fast-base-url", "", "hybrid: the fast group's endpoint; default "+LocalBaseURL)
	fs.IntVar(&c.FastCores, "fast-cores", 0, "hybrid: the fast group's vCores; default -parallel")
	fs.IntVar(&c.FastContextWindow, "fast-context-window", 0, "hybrid: the fast group's context window in tokens; 0: read from Ollama")
	fs.StringVar(&c.StrongAPI, "strong-api", "openai", "hybrid: the strong group's protocol: openai or anthropic")
	fs.StringVar(&c.StrongModel, "strong-model", "", "hybrid: the strong group's model; default "+modelDef(d.OpenAIModel)+" (openai), "+modelDef(d.AnthropicModel)+" (anthropic)")
	fs.StringVar(&c.StrongBaseURL, "strong-base-url", "", "hybrid: the strong group's endpoint; default "+openaiURL+" (openai) or "+AnthropicBaseURL+" (anthropic)")
	fs.StringVar(&c.StrongKeyEnv, "strong-key-env", "", "hybrid: the environment variable holding the strong group's key; default "+d.OpenAIKeyEnv+" (openai) or ANTHROPIC_API_KEY (anthropic)")
	fs.IntVar(&c.StrongCores, "strong-cores", 0, "hybrid: the strong group's vCores; default 8")
	fs.IntVar(&c.StrongContextWindow, "strong-context-window", 0, "hybrid: the strong group's context window in tokens; 0: 200000 (anthropic) or unknown (openai)")
	fs.BoolVar(&c.Fallback, "fallback", true, "hybrid: while the fast group is down (its breaker open), its tasks move to the strong group")
	fs.StringVar(&c.Route, "route", RouteStatic, "hybrid: how a task picks its group: static (by kind; the REPL's default is fast, /strong overrides), decide (a decision model reads the request), fast or strong (everything to one group)")
	fs.StringVar(&c.RouterURL, "router-url", decide.DefaultURL, "-route decide: the Ollama server with the System One API")
	fs.StringVar(&c.RouterModel, "router-model", decide.DefaultModel, "-route decide: the decision model")
	fs.DurationVar(&c.RouteTimeout, "route-timeout", 0, "-route decide: how long one routing decision may take before the task goes to strong (0: the router's default, 3s)")
	fs.Float64Var(&c.RouteFloor, "route-floor", 0, "-route decide: the confidence a pick needs, below which the task goes to strong (0: the router's default, 0.2; negative: take every pick)")
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
	d := c.defaults()
	if c.Backend == "" {
		c.Backend = d.Backend
	}
	if c.Store == "" {
		c.Store = filepath.Join(c.Root, ".neu", "state.db")
	}
	if c.Backend == "openai" {
		c.Backend = "cloud"
	}
	switch c.Backend {
	case "local":
		c.Model = or(c.Model, d.LocalModel)
		if c.Model == "" {
			return errors.New("local backend needs a model: -model (e.g. an Ollama model name) or model in the config file")
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
		if c.KeyEnv == "" {
			c.KeyEnv = LocalKeyEnv
		}
	case "cloud":
		c.Model = or(c.Model, d.OpenAIModel)
		c.BaseURL = or(c.BaseURL, d.OpenAIBaseURL)
		if c.Cores == 0 {
			c.Cores = 8
		}
		c.KeyEnv = or(c.KeyEnv, d.OpenAIKeyEnv)
		if c.Model == "" {
			return errors.New("openai backend needs a model: -model, OPENAI_MODEL, or model in the config file")
		}
		if needKeys && (c.BaseURL == "" || os.Getenv(c.KeyEnv) == "") {
			return fmt.Errorf("openai backend needs -base-url and the key in $%s", c.KeyEnv)
		}
	case "anthropic":
		c.Model = or(c.Model, d.AnthropicModel)
		if c.Model == "" {
			return errors.New("anthropic backend needs a model: -model, ANTHROPIC_MODEL, or model in the config file")
		}
		if c.BaseURL == "" {
			c.BaseURL = AnthropicBaseURL
		}
		if c.Cores == 0 {
			c.Cores = 8
		}
		if c.KeyEnv == "" {
			c.KeyEnv = "ANTHROPIC_API_KEY"
		}
		if needKeys && os.Getenv(c.KeyEnv) == "" {
			return fmt.Errorf("anthropic backend needs the key in $%s", c.KeyEnv)
		}
	case "hybrid":
		for _, f := range []string{"model", "base-url", "cores", "key-env", "context-window"} {
			if c.set[f] {
				return fmt.Errorf("-%s does not apply to -backend hybrid: use -fast-%s and -strong-%s", f, f, f)
			}
		}
		if c.Parallel < 1 {
			return errors.New("-parallel must be at least 1")
		}
		c.FastModel = or(c.FastModel, d.FastModel)
		if c.FastModel == "" {
			return errors.New("hybrid backend needs the fast group's model: -fast-model or fast_model in the config file")
		}
		if c.FastBaseURL == "" {
			c.FastBaseURL = LocalBaseURL
		}
		if c.FastCores == 0 {
			c.FastCores = c.Parallel
		}
		switch c.StrongAPI {
		case "", "openai", "cloud":
			c.StrongAPI = "openai"
			c.StrongModel = or(c.StrongModel, d.OpenAIModel)
			c.StrongBaseURL = or(c.StrongBaseURL, d.OpenAIBaseURL)
			c.StrongKeyEnv = or(c.StrongKeyEnv, d.OpenAIKeyEnv)
		case "anthropic":
			c.StrongModel = or(c.StrongModel, d.AnthropicModel)
			c.StrongBaseURL = or(c.StrongBaseURL, AnthropicBaseURL)
			c.StrongKeyEnv = or(c.StrongKeyEnv, "ANTHROPIC_API_KEY")
		default:
			return fmt.Errorf("unknown -strong-api %q: want openai or anthropic", c.StrongAPI)
		}
		if c.StrongCores == 0 {
			c.StrongCores = 8
		}
		if c.StrongModel == "" {
			return errors.New("hybrid backend needs the strong group's model: -strong-model, OPENAI_MODEL / ANTHROPIC_MODEL, or [strong] model in the config file")
		}
		if needKeys && (c.StrongBaseURL == "" || os.Getenv(c.StrongKeyEnv) == "") {
			return fmt.Errorf("hybrid backend's strong group needs -strong-base-url, -strong-model and the key in $%s", c.StrongKeyEnv)
		}
		if c.FastCores < 1 || c.StrongCores < 1 {
			return errors.New("-fast-cores and -strong-cores must be at least 1")
		}
		if c.FastContextWindow < 0 || c.StrongContextWindow < 0 {
			return errors.New("-fast-context-window and -strong-context-window must not be negative")
		}
		switch c.Route {
		case "":
			c.Route = RouteStatic
		case RouteStatic, RouteDecide, ClassFast, ClassStrong:
		default:
			return fmt.Errorf("unknown -route %q: want static, decide, fast or strong", c.Route)
		}
		c.Model, c.BaseURL, c.Cores = "", "", c.FastCores+c.StrongCores
	case "":
		return errors.New("no backend: pass -backend local, openai, anthropic or hybrid")
	default:
		return fmt.Errorf("unknown -backend %q: want local, openai, anthropic or hybrid", c.Backend)
	}
	if c.Cores < 1 {
		return errors.New("-cores must be at least 1")
	}
	if c.ContextWindow < 0 {
		return errors.New("-context-window must not be negative")
	}
	return nil
}

func or(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// The anthropic backend's endpoint, and the bench's Anthropic model (neu
// has no default model).
const (
	AnthropicModel   = "claude-sonnet-5-5"
	AnthropicBaseURL = "https://api.anthropic.com/v1"
)

// anthropicThinkOff is the thinking type that turns thinking off, for the
// models that can (anthropic.Adapter.ThinkOff); a model not listed keeps
// its thinking on a NoThink redo.
var anthropicThinkOff = map[string]string{
	"claude-sonnet-5-5": "between_tools",
	"claude-sonnet-5":   "disabled",
	"claude-haiku-5-5":  "disabled",
}

// Side is one model backend of the machine: an execution group.
type Side struct {
	Class    string // the group's class: neumann.DefaultClass for local and cloud, fast or strong for hybrid
	Kind     string // local or cloud
	API      string // the cloud side's protocol: openai (default) or anthropic
	Model    string
	BaseURL  string
	Cores    int
	Parallel int    // local: requests the backend serves at once
	KeyEnv   string // the environment variable holding the key
	Window   int    // context window in tokens; 0: the adapter's default
}

// Sides are the groups the resolved config describes: one for local and
// cloud, fast and strong for hybrid.
func (c *Config) Sides() []Side {
	switch c.Backend {
	case "hybrid":
		return []Side{
			{Class: ClassFast, Kind: "local", Model: c.FastModel, BaseURL: c.FastBaseURL, Cores: c.FastCores, Parallel: c.Parallel, KeyEnv: LocalKeyEnv, Window: c.FastContextWindow},
			{Class: ClassStrong, Kind: "cloud", API: c.StrongAPI, Model: c.StrongModel, BaseURL: c.StrongBaseURL, Cores: c.StrongCores, KeyEnv: c.StrongKeyEnv, Window: c.StrongContextWindow},
		}
	case "local":
		return []Side{{Class: neumann.DefaultClass, Kind: "local", Model: c.Model, BaseURL: c.BaseURL, Cores: c.Cores, Parallel: c.Parallel, KeyEnv: c.KeyEnv, Window: c.ContextWindow}}
	case "anthropic":
		return []Side{{Class: neumann.DefaultClass, Kind: "cloud", API: "anthropic", Model: c.Model, BaseURL: c.BaseURL, Cores: c.Cores, KeyEnv: c.KeyEnv, Window: c.ContextWindow}}
	}
	return []Side{{Class: neumann.DefaultClass, Kind: "cloud", API: "openai", Model: c.Model, BaseURL: c.BaseURL, Cores: c.Cores, KeyEnv: c.KeyEnv, Window: c.ContextWindow}}
}

// Describe is the backend in one line, for logs and reports.
func (c *Config) Describe() string {
	if c.Backend != "hybrid" {
		b := c.Backend
		if b == "cloud" {
			b = "openai"
		}
		return fmt.Sprintf("%s (%s at %s), %d vCore(s), %s", b, c.Model, c.BaseURL, c.Cores, c.Sides()[0].WindowDesc())
	}
	fb := "no fallback"
	if c.Fallback {
		fb = "fast falls back to strong"
	}
	route := c.Route
	if route == RouteDecide {
		route += " (" + c.RouterModel + ")"
	}
	sides := c.Sides()
	return fmt.Sprintf("hybrid: fast %s at %s, %d vCore(s), %s; strong %s at %s, %d vCore(s), %s; route %s; %s",
		c.FastModel, c.FastBaseURL, c.FastCores, sides[0].WindowDesc(), c.StrongModel, c.StrongBaseURL, c.StrongCores, sides[1].WindowDesc(), route, fb)
}

// BackendArgs are the flags that rebuild this backend in another process
// (the crash scenario's child).
func (c *Config) BackendArgs() []string {
	if c.Backend != "hybrid" {
		return []string{"-backend", c.Backend, "-model", c.Model, "-base-url", c.BaseURL, "-cores", strconv.Itoa(c.Cores), "-parallel", strconv.Itoa(c.Parallel), "-key-env", c.KeyEnv,
			"-context-window", strconv.Itoa(c.ContextWindow)}
	}
	return []string{"-backend", "hybrid", "-parallel", strconv.Itoa(c.Parallel),
		"-fast-model", c.FastModel, "-fast-base-url", c.FastBaseURL, "-fast-cores", strconv.Itoa(c.FastCores),
		"-strong-api", c.StrongAPI, "-strong-key-env", c.StrongKeyEnv, "-strong-model", c.StrongModel, "-strong-base-url", c.StrongBaseURL, "-strong-cores", strconv.Itoa(c.StrongCores),
		"-fast-context-window", strconv.Itoa(c.FastContextWindow), "-strong-context-window", strconv.Itoa(c.StrongContextWindow),
		"-fallback=" + strconv.FormatBool(c.Fallback), "-route", c.Route, "-router-url", c.RouterURL, "-router-model", c.RouterModel,
		"-route-timeout", c.RouteTimeout.String(), "-route-floor", strconv.FormatFloat(c.RouteFloor, 'g', -1, 64)}
}

// Adapter builds the side's model adapter, wrapped to record call times.
// A local side is configured as cmd/neumann -local configures one; a cloud
// side keeps the adapter's defaults. Window, when set, is the adapter's
// ContextWindow. A local side with a known window reserves at most a
// quarter of it for the answer: Ollama's context holds the prompt and the
// answer together, and compaction keeps the request below 80% of the
// window minus that reserve.
func (s Side) Adapter() *Timed {
	key := os.Getenv(s.KeyEnv)
	if s.API == "anthropic" {
		a := anthropic.New(s.BaseURL, key, s.Model)
		a.ThinkOff = anthropicThinkOff[s.Model]
		a.ContextWindow = s.Window
		return &Timed{Adapter: a, Class: s.Class}
	}
	if s.Kind == "local" && key == "" {
		key = "ollama"
	}
	a := openai.New(s.BaseURL, key, s.Model)
	a.ContextWindow = s.Window
	if s.Kind == "local" {
		a.Parallel = s.Parallel
		a.Prefill, a.ThinkOff = true, localThinkOff
		a.ThinkTags = [2]string{"<think>", "</think>"}
		a.Timeout, a.FirstByteTimeout, a.IdleTimeout = 0, localFirstByte, localIdle
		a.MaxTokens = localMaxTokens
		if s.Window > 0 {
			a.MaxTokens = min(localMaxTokens, s.Window/4)
		}
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
	ext      extras // MCP servers, skills, messages (extras.go)
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
	// Extras are the tool sources besides the workspace tools.
	Extras Extras
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
			a.logw = log.New(os.Stderr, "neu: ", log.Ltime)
			logf = a.logw.Printf
		}
	}
	devs, sys, err := a.attach(ctx, tl.Devices(), opt.Extras)
	if err != nil {
		st.Close()
		return nil, err
	}
	mc := neumann.Config{
		Devices: devs,
		Store:   st,
		System:  System + sys,
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
		a.closeExtras()
		st.Close()
		return nil, err
	}
	a.bind(m)
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
	obs, err := startObserving(ctx, a.Cfg, a.M, func(f string, args ...any) { fmt.Fprintf(os.Stderr, "neu: "+f+"\n", args...) })
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
