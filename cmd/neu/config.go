package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/liliang-cn/neumann/internal/neu/app"
)

// OllamaURL is where neu looks for a local Ollama when nothing else
// configures a backend.
const OllamaURL = "http://127.0.0.1:11434"

// probeTimeout bounds the Ollama probe: a local server answers in
// milliseconds, and nobody should wait for one that is not there.
const probeTimeout = 300 * time.Millisecond

// File is the config file. Every key is optional; a flag given on the
// command line wins over the file. model, base_url and key_env apply only
// when the backend in use is the file's backend (or the file names none),
// so `-backend local` does not inherit a cloud model.
type File struct {
	Backend  string   `toml:"backend"`
	BaseURL  string   `toml:"base_url"`
	Model    string   `toml:"model"`
	KeyEnv   string   `toml:"key_env"`
	Cores    int      `toml:"cores"`
	Parallel int      `toml:"parallel"`
	Writable []string `toml:"writable"`
	MaxSteps int      `toml:"max_steps"`
	// ContextWindow is the model's context window in tokens; it goes with
	// model, so it applies only to the file's backend.
	ContextWindow int `toml:"context_window"`

	// The coding tools and their approvals: -approve, -sh-timeout,
	// -sh-shell, -sh-deny-env.
	Approve   string   `toml:"approve"`
	ShTimeout string   `toml:"sh_timeout"`
	ShShell   *bool    `toml:"sh_shell"`
	ShDenyEnv []string `toml:"sh_deny_env"`

	Hybrid struct {
		FastModel    string   `toml:"fast_model"`
		FastBaseURL  string   `toml:"fast_base_url"`
		FastCores    int      `toml:"fast_cores"`
		FastWindow   int      `toml:"fast_context_window"`
		Route        string   `toml:"route"`
		RouterURL    string   `toml:"router_url"`
		RouterModel  string   `toml:"router_model"`
		RouteTimeout string   `toml:"route_timeout"`
		RouteFloor   *float64 `toml:"route_floor"`
		Fallback     *bool    `toml:"fallback"`
	} `toml:"hybrid"`

	Strong struct {
		API     string `toml:"api"`
		Model   string `toml:"model"`
		BaseURL string `toml:"base_url"`
		KeyEnv  string `toml:"key_env"`
		Cores   int    `toml:"cores"`
		Window  int    `toml:"context_window"`
	} `toml:"strong"`

	BusFile // [[mcp]] and skills_dir (bus.go)
}

// DefaultConfigPath is $XDG_CONFIG_HOME/neu/config.toml, or
// ~/.config/neu/config.toml when XDG_CONFIG_HOME is unset.
func DefaultConfigPath() string {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "neu", "config.toml")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".config", "neu", "config.toml")
	}
	return filepath.Join(home, ".config", "neu", "config.toml")
}

// LoadFile reads the config file at path; a missing file is (nil, nil).
// Unknown keys are an error, so a typo does not pass unnoticed.
func LoadFile(path string) (*File, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var f File
	md, err := toml.Decode(string(b), &f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if und := md.Undecoded(); len(und) > 0 {
		var keys []string
		for _, k := range und {
			keys = append(keys, k.String())
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("%s: unknown key(s) %s", path, strings.Join(keys, ", "))
	}
	if err := f.BusFile.check(path); err != nil {
		return nil, err
	}
	if f.ShTimeout != "" {
		if _, err := time.ParseDuration(f.ShTimeout); err != nil {
			return nil, fmt.Errorf("%s: sh_timeout: %w", path, err)
		}
	}
	if f.Hybrid.RouteTimeout != "" {
		if _, err := time.ParseDuration(f.Hybrid.RouteTimeout); err != nil {
			return nil, fmt.Errorf("%s: hybrid.route_timeout: %w", path, err)
		}
	}
	return &f, nil
}

// Env is what Setup looks at besides the flags and the process
// environment.
type Env struct {
	ConfigPath string // the config file; "": none
	OllamaURL  string // the Ollama probed last; "": no probe
}

// Setup fills cfg from the config file and picks the backend when no
// -backend flag was given: the file's, else anthropic when
// ANTHROPIC_API_KEY is set, else openai when OPENAI_API_KEY is set, else
// local when Ollama answers at env.OllamaURL. set holds the flags given on
// the command line, which the file does not override. It returns where the
// backend came from, for the startup line.
func Setup(ctx context.Context, cfg *app.Config, set map[string]bool, env Env) (string, error) {
	var f *File
	if env.ConfigPath != "" {
		var err error
		if f, err = LoadFile(env.ConfigPath); err != nil {
			return "", err
		}
	}
	var src string
	switch {
	case set["backend"]:
		src = "-backend"
	case f != nil && f.Backend != "":
		cfg.Backend, src = f.Backend, env.ConfigPath
	case os.Getenv("ANTHROPIC_API_KEY") != "":
		cfg.Backend, src = "anthropic", "ANTHROPIC_API_KEY is set"
	case os.Getenv(app.OpenAIKeyEnv) != "":
		cfg.Backend, src = "openai", app.OpenAIKeyEnv+" is set"
	case env.OllamaURL != "" && ollamaUp(ctx, env.OllamaURL):
		cfg.Backend, src = "local", "Ollama at "+env.OllamaURL
	default:
		return "", noBackend(env.ConfigPath)
	}
	if f != nil {
		f.apply(cfg, set)
	}
	return src, nil
}

func norm(backend string) string {
	if backend == "cloud" {
		return "openai"
	}
	return backend
}

// apply copies the file's values into cfg where no flag set them.
func (f *File) apply(cfg *app.Config, set map[string]bool) {
	str := func(flag string, dst *string, v string) {
		if v != "" && !set[flag] {
			*dst = v
		}
	}
	num := func(flag string, dst *int, v int) {
		if v != 0 && !set[flag] {
			*dst = v
		}
	}
	if f.Backend == "" || norm(f.Backend) == norm(cfg.Backend) {
		str("model", &cfg.Model, f.Model)
		str("base-url", &cfg.BaseURL, f.BaseURL)
		str("key-env", &cfg.KeyEnv, f.KeyEnv)
		if cfg.Backend != "hybrid" {
			num("cores", &cfg.Cores, f.Cores)
			num("context-window", &cfg.ContextWindow, f.ContextWindow)
		}
	}
	num("parallel", &cfg.Parallel, f.Parallel)
	num("max-steps", &cfg.MaxSteps, f.MaxSteps)
	if len(f.Writable) > 0 {
		str("writable", &cfg.Writable, strings.Join(f.Writable, ","))
	}
	f.applyCoding(&cfg.Coding, set)
	h := f.Hybrid
	str("fast-model", &cfg.FastModel, h.FastModel)
	str("fast-base-url", &cfg.FastBaseURL, h.FastBaseURL)
	num("fast-cores", &cfg.FastCores, h.FastCores)
	num("fast-context-window", &cfg.FastContextWindow, h.FastWindow)
	str("route", &cfg.Route, h.Route)
	str("router-url", &cfg.RouterURL, h.RouterURL)
	str("router-model", &cfg.RouterModel, h.RouterModel)
	if d, err := time.ParseDuration(h.RouteTimeout); err == nil && !set["route-timeout"] {
		cfg.RouteTimeout = d
	}
	if h.RouteFloor != nil && !set["route-floor"] {
		cfg.RouteFloor = *h.RouteFloor
	}
	if h.Fallback != nil && !set["fallback"] {
		cfg.Fallback = *h.Fallback
	}
	s := f.Strong
	str("strong-api", &cfg.StrongAPI, s.API)
	str("strong-model", &cfg.StrongModel, s.Model)
	str("strong-base-url", &cfg.StrongBaseURL, s.BaseURL)
	str("strong-key-env", &cfg.StrongKeyEnv, s.KeyEnv)
	num("strong-cores", &cfg.StrongCores, s.Cores)
	num("strong-context-window", &cfg.StrongContextWindow, s.Window)
}

// ollamaUp reports whether an Ollama server answers GET /api/tags at base
// within probeTimeout.
func ollamaUp(ctx context.Context, base string) bool {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/api/tags", nil)
	if err != nil {
		return false
	}
	// A loopback probe must not go through HTTPS_PROXY / HTTP_PROXY.
	tr := &http.Transport{Proxy: nil}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func noBackend(configPath string) error {
	return fmt.Errorf(`no backend configured; any one of these works (a model must always be given):
  - a config file at %s, e.g.
        backend = "anthropic"
        model = "<model name>"
  - ANTHROPIC_API_KEY (and ANTHROPIC_MODEL or -model) in the environment: anthropic
  - %s (and OPENAI_MODEL or -model) in the environment: openai; OPENAI_BASE_URL for another endpoint
  - Ollama running at %s, with -model: local
  - flags: -backend local|openai|anthropic|hybrid, with -base-url, -model and -key-env as needed`,
		configPath, app.OpenAIKeyEnv, OllamaURL)
}

// applyCoding copies the coding keys into k where no flag set them.
func (f *File) applyCoding(k *app.Coding, set map[string]bool) {
	if f.Approve != "" && !set["approve"] {
		k.Approve = f.Approve
	}
	if d, err := time.ParseDuration(f.ShTimeout); err == nil && !set["sh-timeout"] {
		k.ShTimeout = d
	}
	if f.ShShell != nil && !set["sh-shell"] {
		k.ShScript = *f.ShShell
	}
	if len(f.ShDenyEnv) > 0 && !set["sh-deny-env"] {
		k.ShDenyEnv = f.ShDenyEnv
	}
}
