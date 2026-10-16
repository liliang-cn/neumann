// Command neumann runs a machine behind the stdio frontend: one instruction
// per line of JSON on stdin, replies and events as JSON lines on stdout, logs
// on stderr. The wire format is documented in package proto/jsonl.
//
//	NEUMANN_API_KEY=... neumann -provider openai -base-url https://host/v1 -model m -store state.db
//
// Every flag can also be set from the environment (shown in -help); a flag
// wins over its variable. The API key is read from the environment only —
// NEUMANN_API_KEY, else OPENAI_API_KEY or ANTHROPIC_API_KEY for the chosen
// provider — and is never printed.
//
// -local configures a local OpenAI-compatible backend (Ollama, llama.cpp,
// LM Studio) in one switch; -help lists what it sets:
//
//	neumann -local -model qwen3.5:9b-mlx -store state.db
//
// End of stdin, SIGINT or SIGTERM stop the machine gracefully (within
// -stop-timeout) and exit 0. A configuration or startup failure exits 1, a
// usage error 2.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/anthropic"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto/jsonl"
)

func main() {
	// A reader that goes away must not kill the process with SIGPIPE: the
	// write fails instead, and the machine still stops cleanly.
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr))
}

var defaultBaseURL = map[string]string{
	"openai":    "https://api.openai.com/v1",
	"anthropic": "https://api.anthropic.com/v1",
}

// What -local sets (design §11, §14). A local backend reads a long prompt
// slowly and then streams steadily: the first byte gets a generous limit, a
// stalled stream a short one, and the whole call none. The output cap stops
// a model that loops in its thinking.
const (
	localBaseURL   = "http://127.0.0.1:11434/v1" // Ollama
	localThinkOff  = "none"
	localFirstByte = 10 * time.Minute
	localIdle      = 2 * time.Minute
	localMaxTokens = 8192
)

type config struct {
	provider, baseURL, model, key string
	store, systemFile             string
	cores, maxQueued, maxTokens   int
	echo                          bool
	stopTimeout                   time.Duration

	local    bool
	parallel int
	set      map[string]bool // names of the flags and variables that were given

	observeAddr, observeToken string
	metrics, pprof            bool
}

// parse reads flags over environment defaults. The key comes only from env.
func parse(args []string, getenv func(string) string, stderr io.Writer) (config, error) {
	c := config{set: map[string]bool{}}
	fs := flag.NewFlagSet("neumann", flag.ContinueOnError)
	fs.SetOutput(stderr)
	envInt := func(name string, def int) int {
		if v := getenv(name); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				c.set[name] = true
				return n
			}
		}
		return def
	}
	envDur := func(name string, def time.Duration) time.Duration {
		if d, err := time.ParseDuration(getenv(name)); err == nil {
			return d
		}
		return def
	}
	envOr := func(name, def string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return def
	}
	fs.StringVar(&c.provider, "provider", envOr("NEUMANN_PROVIDER", "openai"), "model provider: openai (any OpenAI-compatible endpoint) or anthropic [NEUMANN_PROVIDER]")
	fs.StringVar(&c.baseURL, "base-url", getenv("NEUMANN_BASE_URL"), "endpoint up to and including the version, e.g. https://api.openai.com/v1 (default: the provider's) [NEUMANN_BASE_URL]")
	fs.StringVar(&c.model, "model", getenv("NEUMANN_MODEL"), "model name (required) [NEUMANN_MODEL]")
	fs.StringVar(&c.store, "store", getenv("NEUMANN_STORE"), "SQLite file holding the machine's state; empty: in memory, nothing durable [NEUMANN_STORE]")
	fs.StringVar(&c.systemFile, "system-file", getenv("NEUMANN_SYSTEM_FILE"), "file whose contents are the system prompt [NEUMANN_SYSTEM_FILE]")
	fs.IntVar(&c.cores, "cores", envInt("NEUMANN_CORES", neumann.DefaultCores), "vCores [NEUMANN_CORES]")
	fs.IntVar(&c.maxQueued, "max-queued", envInt("NEUMANN_MAX_QUEUED", neumann.DefaultMaxQueued), "unfinished tasks admitted at once [NEUMANN_MAX_QUEUED]")
	fs.IntVar(&c.maxTokens, "max-tokens", envInt("NEUMANN_MAX_TOKENS", 0), "output token limit per model call; 0: the adapter's default [NEUMANN_MAX_TOKENS]")
	fs.BoolVar(&c.local, "local", getenv("NEUMANN_LOCAL") == "1", fmt.Sprintf("a local OpenAI-compatible backend (Ollama, llama.cpp, LM Studio): -parallel 1 and -cores equal to it unless given, answers cut off are continued, reasoning_effort %q turns thinking off, %s to the first byte and %s between stream chunks with no limit on the whole call, -max-tokens %d unless given, -base-url %s unless given [NEUMANN_LOCAL=1]", localThinkOff, localFirstByte, localIdle, localMaxTokens, localBaseURL))
	fs.IntVar(&c.parallel, "parallel", envInt("NEUMANN_PARALLEL", 0), "requests the backend serves at once; it caps the vCores working on it. 0: unknown, or 1 with -local. OpenAI-compatible provider only [NEUMANN_PARALLEL]")
	fs.BoolVar(&c.echo, "echo", getenv("NEUMANN_ECHO") == "1", "attach a builtin device with one tool, util_echo, for testing [NEUMANN_ECHO=1]")
	fs.DurationVar(&c.stopTimeout, "stop-timeout", envDur("NEUMANN_STOP_TIMEOUT", jsonl.DefaultStopTimeout), "how long a graceful stop may take [NEUMANN_STOP_TIMEOUT]")
	fs.StringVar(&c.observeAddr, "observe-addr", getenv("NEUMANN_OBSERVE_ADDR"), "serve the read-only observe HTTP view (/status, /queue, /tasks/{id}, /events) here, e.g. 127.0.0.1:43517; empty: off. It shows task content: keep it private or set NEUMANN_OBSERVE_TOKEN [NEUMANN_OBSERVE_ADDR]")
	fs.BoolVar(&c.metrics, "metrics", getenv("NEUMANN_METRICS") == "1", "also serve Prometheus metrics at /metrics on -observe-addr [NEUMANN_METRICS=1]")
	fs.BoolVar(&c.pprof, "pprof", getenv("NEUMANN_PPROF") == "1", "also serve runtime profiles at /debug/pprof/ on -observe-addr [NEUMANN_PPROF=1]")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: neumann [flags]\n\nInstructions on stdin, replies and events on stdout, one JSON object per line.\n"+
			"The API key is read from NEUMANN_API_KEY, else OPENAI_API_KEY / ANTHROPIC_API_KEY.\n"+
			"NEUMANN_OBSERVE_TOKEN, if set, is required as a bearer token by the observe HTTP view.\n"+
			"OTEL_EXPORTER_OTLP_ENDPOINT (and the other standard OTEL_ variables), if set, exports traces and metrics over OTLP/HTTP.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() > 0 {
		return c, fmt.Errorf("unexpected arguments: %q", fs.Args())
	}
	fs.Visit(func(f *flag.Flag) { c.set[f.Name] = true })
	return c, nil
}

func (c *config) resolve(getenv func(string) string) error {
	def, ok := defaultBaseURL[c.provider]
	if !ok {
		return fmt.Errorf("unknown provider %q: want openai or anthropic", c.provider)
	}
	if (c.local || c.parallel != 0) && c.provider != "openai" {
		return errors.New("-local and -parallel are for an OpenAI-compatible backend: -provider openai")
	}
	if c.parallel < 0 {
		return errors.New("-parallel must not be negative")
	}
	if c.local {
		def = localBaseURL
		c.parallel = max(c.parallel, 1)
		if !c.set["cores"] && !c.set["NEUMANN_CORES"] {
			c.cores = c.parallel
		}
		if !c.set["max-tokens"] && !c.set["NEUMANN_MAX_TOKENS"] {
			c.maxTokens = localMaxTokens
		}
	}
	if c.baseURL == "" {
		c.baseURL = def
	}
	if c.model == "" {
		return errors.New("no model: set -model or NEUMANN_MODEL")
	}
	c.key = getenv("NEUMANN_API_KEY")
	if c.key == "" {
		c.key = getenv(map[string]string{"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}[c.provider])
	}
	if c.cores <= 0 || c.maxQueued <= 0 || c.maxTokens < 0 || c.stopTimeout <= 0 {
		return errors.New("-cores, -max-queued and -stop-timeout must be positive, -max-tokens not negative")
	}
	if (c.metrics || c.pprof) && c.observeAddr == "" {
		return errors.New("-metrics and -pprof are served on -observe-addr: set it too")
	}
	c.observeToken = getenv("NEUMANN_OBSERVE_TOKEN")
	return nil
}

func (c *config) adapter() model.Adapter {
	if c.provider == "anthropic" {
		a := anthropic.New(c.baseURL, c.key, c.model)
		if c.maxTokens > 0 {
			a.MaxTokens = c.maxTokens
		}
		return a
	}
	a := openai.New(c.baseURL, c.key, c.model)
	a.MaxTokens = c.maxTokens
	a.Parallel = c.parallel
	if c.local {
		a.Prefill, a.ThinkOff = true, localThinkOff
		a.Timeout, a.FirstByteTimeout, a.IdleTimeout = 0, localFirstByte, localIdle
	}
	return a
}

// echoDevice is util_echo: it returns its "text" argument.
func echoDevice() bus.Device {
	schema := json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)
	return builtin.New("util").Add(bus.ToolDef{Name: "echo", Description: "Return the given text unchanged.", Schema: schema, Effect: bus.ReadOnly},
		func(_ context.Context, call bus.DeviceCall) (string, error) {
			var v struct{ Text string }
			if err := json.Unmarshal(call.Args, &v); err != nil {
				return "", fmt.Errorf("bad arguments: %v", err)
			}
			return v.Text, nil
		})
}

// run is the whole program; it returns the exit code.
func run(ctx context.Context, args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)
	c, err := parse(args, getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "neumann: %v\n", err)
		return 2
	}
	if err := c.resolve(getenv); err != nil {
		logger.Printf("neumann: %v", err)
		return 1
	}
	if c.key == "" {
		logger.Printf("neumann: no API key in NEUMANN_API_KEY or the provider's variable; requests go out without one")
	}
	var system string
	if c.systemFile != "" {
		b, err := os.ReadFile(c.systemFile)
		if err != nil {
			logger.Printf("neumann: system prompt: %v", err)
			return 1
		}
		system = string(b)
	}
	var store mem.Store
	where := "memory"
	if c.store != "" {
		db, err := sqlite.Open(c.store)
		if err != nil {
			logger.Printf("neumann: %v", err)
			return 1
		}
		defer db.Close()
		store, where = db, c.store
	} else {
		logger.Printf("neumann: WARNING: no -store: state is kept in memory and nothing is durable; an accepted task is lost when the process exits")
	}
	var devices []bus.Device
	if c.echo {
		devices = append(devices, echoDevice())
	}
	// TODO(preempt): -local turns on preemption for the group (GroupConfig
	// Preempt, design §6.4, §14) once the core has it.
	m, err := neumann.New(neumann.Config{
		Model:     c.adapter(),
		Devices:   devices,
		Store:     store,
		Cores:     c.cores,
		System:    system,
		MaxQueued: c.maxQueued,
		Logf:      logger.Printf,
	})
	if err != nil {
		logger.Printf("neumann: %v", err)
		return 1
	}
	if err := m.Start(ctx); err != nil {
		logger.Printf("neumann: start: %v", err)
		return 1
	}
	obs, err := startObserving(ctx, c, getenv, m, logger.Printf)
	if err != nil {
		logger.Printf("neumann: observe: %v", err)
		sctx, cancel := context.WithTimeout(context.Background(), c.stopTimeout)
		defer cancel()
		m.Stop(sctx)
		return 1
	}
	backend := c.provider
	if c.local {
		backend = fmt.Sprintf("local (%d at once)", c.parallel)
	}
	logger.Printf("neumann: ready: provider %s, model %s, %d vCores, store %s", backend, c.model, c.cores, where)
	srv := &jsonl.Server{Machine: m, StopTimeout: c.stopTimeout, Logf: logger.Printf}
	err = srv.Serve(ctx, stdin, stdout)
	obs.stop() // the machine has stopped: observers read to the end and flush
	if err != nil {
		logger.Printf("neumann: %v", err)
		return 1
	}
	logger.Printf("neumann: stopped")
	return 0
}
