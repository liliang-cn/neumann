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

type config struct {
	provider, baseURL, model, key string
	store, systemFile             string
	cores, maxQueued, maxTokens   int
	echo                          bool
	stopTimeout                   time.Duration
}

// parse reads flags over environment defaults. The key comes only from env.
func parse(args []string, getenv func(string) string, stderr io.Writer) (config, error) {
	var c config
	fs := flag.NewFlagSet("neumann", flag.ContinueOnError)
	fs.SetOutput(stderr)
	envInt := func(name string, def int) int {
		if v := getenv(name); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
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
	fs.BoolVar(&c.echo, "echo", getenv("NEUMANN_ECHO") == "1", "attach a builtin device with one tool, util_echo, for testing [NEUMANN_ECHO=1]")
	fs.DurationVar(&c.stopTimeout, "stop-timeout", envDur("NEUMANN_STOP_TIMEOUT", jsonl.DefaultStopTimeout), "how long a graceful stop may take [NEUMANN_STOP_TIMEOUT]")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: neumann [flags]\n\nInstructions on stdin, replies and events on stdout, one JSON object per line.\n"+
			"The API key is read from NEUMANN_API_KEY, else OPENAI_API_KEY / ANTHROPIC_API_KEY.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() > 0 {
		return c, fmt.Errorf("unexpected arguments: %q", fs.Args())
	}
	return c, nil
}

func (c *config) resolve(getenv func(string) string) error {
	def, ok := defaultBaseURL[c.provider]
	if !ok {
		return fmt.Errorf("unknown provider %q: want openai or anthropic", c.provider)
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
	logger.Printf("neumann: ready: provider %s, model %s, %d vCores, store %s", c.provider, c.model, c.cores, where)
	srv := &jsonl.Server{Machine: m, StopTimeout: c.stopTimeout, Logf: logger.Printf}
	if err := srv.Serve(ctx, stdin, stdout); err != nil {
		logger.Printf("neumann: %v", err)
		return 1
	}
	logger.Printf("neumann: stopped")
	return 0
}
