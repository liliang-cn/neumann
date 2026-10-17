// Package app builds nm's machine: the backend (a local Ollama or a cloud
// OpenAI-compatible endpoint), the SQLite store, the workspace tools and
// the optional observe endpoints. The CLI and the bench harness share it.
package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/examples/nm/tools"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
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
	Backend  string // local or cloud
	Model    string
	BaseURL  string
	Cores    int
	Parallel int // requests a local backend serves at once
	Store    string
	Root     string
	Writable string // comma-separated, relative to Root
	MaxSteps int

	ObserveAddr string
	OTLP        bool
	Quiet       bool // no machine log lines on stderr

	set map[string]bool
}

// Flags registers the shared flags on fs.
func (c *Config) Flags(fs *flag.FlagSet) {
	fs.StringVar(&c.Backend, "backend", envOr("NM_BACKEND", "cloud"), "local (Ollama "+LocalModel+") or cloud (CPA_BASE_URL, CPA_API_KEY, CPA_MODEL) [NM_BACKEND]")
	fs.StringVar(&c.Model, "model", "", "model name; default: "+LocalModel+" (local) or $CPA_MODEL (cloud)")
	fs.StringVar(&c.BaseURL, "base-url", "", "endpoint up to and including /v1; default: "+LocalBaseURL+" (local) or $CPA_BASE_URL (cloud)")
	fs.IntVar(&c.Cores, "cores", 0, "vCores; default -parallel (local) or 8 (cloud)")
	fs.IntVar(&c.Parallel, "parallel", 1, "local: requests the backend serves at once (Ollama's MLX runner serves one; a GGUF model, OLLAMA_NUM_PARALLEL)")
	fs.StringVar(&c.Store, "store", filepath.Join(".nm", "state.db"), "SQLite file holding the machine's state")
	fs.StringVar(&c.Root, "root", ".", "workspace root; every tool is confined to it")
	fs.StringVar(&c.Writable, "writable", "out", "comma-separated directories under the root where fs_write may write")
	fs.IntVar(&c.MaxSteps, "max-steps", 40, "model calls a task may make")
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

// Resolve fills the backend defaults. The API key is read from the
// environment and returned, never stored in Config (which reports print).
func (c *Config) Resolve() (key string, err error) {
	switch c.Backend {
	case "local":
		if c.Model == "" {
			c.Model = LocalModel
		}
		if c.BaseURL == "" {
			c.BaseURL = LocalBaseURL
		}
		if c.Parallel < 1 {
			return "", errors.New("-parallel must be at least 1")
		}
		if c.Cores == 0 {
			c.Cores = c.Parallel
		}
		key = envOr("NM_API_KEY", "ollama")
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
		key = os.Getenv("CPA_API_KEY")
		if c.Model == "" || c.BaseURL == "" || key == "" {
			return "", errors.New("cloud backend needs CPA_BASE_URL, CPA_API_KEY and CPA_MODEL (or -base-url and -model)")
		}
	default:
		return "", fmt.Errorf("unknown -backend %q: want local or cloud", c.Backend)
	}
	if c.Cores < 1 {
		return "", errors.New("-cores must be at least 1")
	}
	return key, nil
}

// Adapter builds the model adapter, wrapped to record call times.
func (c *Config) Adapter(key string) *Timed {
	a := openai.New(c.BaseURL, key, c.Model)
	if c.Backend == "local" {
		a.Parallel = c.Parallel
		a.Prefill, a.ThinkOff = true, localThinkOff
		a.ThinkTags = [2]string{"<think>", "</think>"}
		a.Timeout, a.FirstByteTimeout, a.IdleTimeout = 0, localFirstByte, localIdle
		a.MaxTokens = localMaxTokens
	}
	return &Timed{Adapter: a}
}

// App is a built machine and what hangs off it.
type App struct {
	Cfg   Config
	M     *neumann.Machine
	Tools *tools.Tools
	Model model.Adapter
	timed *Timed // Model, when it is the timed OpenAI-compatible adapter
	Store *sqlite.Store

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
	Adapter model.Adapter
}

// Build opens the store, attaches the tools and builds the machine. It
// does not Start it.
func Build(ctx context.Context, cfg Config, opt Options) (*App, error) {
	var key string
	if opt.Adapter == nil {
		var err error
		if key, err = cfg.Resolve(); err != nil {
			return nil, err
		}
	}
	box, err := tools.NewSandbox(cfg.Root, strings.Split(cfg.Writable, ","))
	if err != nil {
		return nil, err
	}
	tl, err := tools.New(box)
	if err != nil {
		return nil, err
	}
	a := &App{Cfg: cfg, Tools: tl, Model: opt.Adapter}
	if a.Model == nil {
		a.timed = cfg.Adapter(key)
		a.Model = a.timed
	}
	tl.OnCall = func(s tools.CallStat) {
		a.mu.Lock()
		a.toolRecs = append(a.toolRecs, s)
		a.mu.Unlock()
		if opt.OnTool != nil {
			opt.OnTool(s)
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
	m, err := neumann.New(neumann.Config{
		Model:   a.Model,
		Cores:   cfg.Cores,
		Preempt: cfg.Backend == "local",
		Devices: tl.Devices(),
		Store:   st,
		System:  System,
		Logf:    logf,
	})
	if err != nil {
		st.Close()
		return nil, err
	}
	a.M = m
	return a, nil
}

// Start starts the machine (recovering unfinished tasks) and the observe
// endpoints.
func (a *App) Start(ctx context.Context) error {
	if err := a.M.Start(ctx); err != nil {
		return err
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
	if a.timed == nil {
		return nil
	}
	return a.timed.Records()
}

// ToolRecords returns every tool call the devices finished so far.
func (a *App) ToolRecords() []tools.CallStat {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]tools.CallStat(nil), a.toolRecs...)
}
