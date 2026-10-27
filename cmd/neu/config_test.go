package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
)

// clean empties every variable the resolution reads.
func clean(t *testing.T) {
	t.Helper()
	for _, k := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"} {
		t.Setenv(k, "")
	}
}

func ollama(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"models":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// down is an address nothing listens on.
func down(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// setup parses args as neu does and runs Setup and Resolve.
func setup(t *testing.T, env Env, args ...string) (app.Config, string, error) {
	t.Helper()
	cfg := app.Config{Def: app.UserDefaults()}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	cfg.Flags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	cfg.MarkSet(fs)
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	src, err := Setup(context.Background(), &cfg, set, env)
	if err != nil {
		return cfg, src, err
	}
	cfg.Root = t.TempDir()
	return cfg, src, cfg.Resolve()
}

func TestResolutionOrder(t *testing.T) {
	file := writeFile(t, "backend = \"local\"\nmodel = \"from-file\"\n")
	up, dn := ollama(t), down(t)
	for _, tc := range []struct {
		name               string
		anthropic, openai  string
		file, ollama       string
		backend, model, by string
	}{
		{"file wins over keys", "a", "o", file, up, "local", "from-file", file},
		{"anthropic key", "a", "o", "", up, "anthropic", app.AnthropicModel, "ANTHROPIC_API_KEY"},
		{"openai key", "", "o", "", up, "cloud", app.OpenAIModel, "OPENAI_API_KEY"},
		{"ollama", "", "", "", up, "local", app.HybridFastModel, "Ollama"},
		{"missing file falls through", "", "", filepath.Join(t.TempDir(), "none.toml"), up, "local", app.HybridFastModel, "Ollama"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clean(t)
			t.Setenv("ANTHROPIC_API_KEY", tc.anthropic)
			t.Setenv("OPENAI_API_KEY", tc.openai)
			cfg, src, err := setup(t, Env{ConfigPath: tc.file, OllamaURL: tc.ollama})
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Backend != tc.backend || cfg.Model != tc.model || !strings.Contains(src, tc.by) {
				t.Fatalf("backend %q model %q from %q; want %q %q from %q", cfg.Backend, cfg.Model, src, tc.backend, tc.model, tc.by)
			}
		})
	}
	t.Run("nothing", func(t *testing.T) {
		clean(t)
		start := time.Now()
		_, _, err := setup(t, Env{ConfigPath: filepath.Join(t.TempDir(), "none.toml"), OllamaURL: dn})
		if err == nil {
			t.Fatal("no backend, no error")
		}
		for _, want := range []string{"config file", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "Ollama", "-backend"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error does not mention %s: %v", want, err)
			}
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("probe took %s", d)
		}
	})
	t.Run("flag wins over everything", func(t *testing.T) {
		clean(t)
		t.Setenv("ANTHROPIC_API_KEY", "a")
		cfg, src, err := setup(t, Env{ConfigPath: file, OllamaURL: up}, "-backend", "local")
		if err != nil {
			t.Fatal(err)
		}
		// The file's backend is local too, so its model applies.
		if cfg.Backend != "local" || cfg.Model != "from-file" || src != "-backend" {
			t.Fatalf("backend %q model %q from %q", cfg.Backend, cfg.Model, src)
		}
	})
}

// OpenAI's defaults come from OPENAI_BASE_URL and OPENAI_MODEL.
func TestOpenAIEnvDefaults(t *testing.T) {
	clean(t)
	t.Setenv("OPENAI_API_KEY", "o")
	t.Setenv("OPENAI_BASE_URL", "https://gw.example/v1")
	t.Setenv("OPENAI_MODEL", "m2")
	cfg, _, err := setup(t, Env{})
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.Sides()[0]
	if s.API != "openai" || s.BaseURL != "https://gw.example/v1" || s.Model != "m2" || s.KeyEnv != "OPENAI_API_KEY" {
		t.Fatalf("side %+v", s)
	}
	t.Setenv("OPENAI_BASE_URL", "")
	t.Setenv("OPENAI_MODEL", "")
	cfg, _, err = setup(t, Env{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BaseURL != app.OpenAIBaseURL || cfg.Model != app.OpenAIModel {
		t.Fatalf("defaults %q %q", cfg.BaseURL, cfg.Model)
	}
}

func TestConfigFile(t *testing.T) {
	clean(t)
	t.Setenv("MY_KEY", "k")
	t.Setenv("STRONG_KEY", "s")
	file := writeFile(t, `
backend = "openai"
base_url = "https://gw.example/v1"
model = "file-model"
key_env = "MY_KEY"
cores = 3
parallel = 2
writable = ["out", "docs"]
max_steps = 12

[hybrid]
fast_model = "small:4b"
fast_cores = 1
route = "decide"
route_timeout = "5s"
route_floor = 0.4
fallback = false

[strong]
api = "anthropic"
model = "claude-haiku-5-5"
key_env = "STRONG_KEY"
cores = 4
`)
	cfg, src, err := setup(t, Env{ConfigPath: file})
	if err != nil {
		t.Fatal(err)
	}
	if src != file || cfg.Backend != "cloud" || cfg.BaseURL != "https://gw.example/v1" || cfg.Model != "file-model" || cfg.KeyEnv != "MY_KEY" ||
		cfg.Cores != 3 || cfg.Parallel != 2 || cfg.Writable != "out,docs" || cfg.MaxSteps != 12 {
		t.Fatalf("from the file: %+v (src %q)", cfg, src)
	}
	if !strings.HasSuffix(cfg.Store, filepath.Join(".neu", "state.db")) || !strings.HasPrefix(cfg.Store, cfg.Root) {
		t.Errorf("store %q, root %q", cfg.Store, cfg.Root)
	}

	// Flags override the file.
	cfg, _, err = setup(t, Env{ConfigPath: file}, "-model", "flag-model", "-cores", "5", "-writable", "out")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Model != "flag-model" || cfg.Cores != 5 || cfg.Writable != "out" || cfg.BaseURL != "https://gw.example/v1" {
		t.Fatalf("flags over the file: %+v", cfg)
	}

	// Another backend on the command line does not inherit the file's
	// model, endpoint or key variable; the rest still applies.
	cfg, _, err = setup(t, Env{ConfigPath: file}, "-backend", "local")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Model != app.HybridFastModel || cfg.BaseURL != app.LocalBaseURL || cfg.KeyEnv != app.LocalKeyEnv || cfg.Parallel != 2 || cfg.Cores != 2 || cfg.Writable != "out,docs" {
		t.Fatalf("-backend local with an openai file: %+v", cfg)
	}

	// The [hybrid] and [strong] sections.
	cfg, _, err = setup(t, Env{ConfigPath: file}, "-backend", "hybrid", "-route", "static")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FastModel != "small:4b" || cfg.FastCores != 1 || cfg.Route != "static" || cfg.RouteTimeout != 5*time.Second || cfg.RouteFloor != 0.4 || cfg.Fallback ||
		cfg.StrongAPI != "anthropic" || cfg.StrongModel != "claude-haiku-5-5" || cfg.StrongKeyEnv != "STRONG_KEY" || cfg.StrongCores != 4 || cfg.StrongBaseURL != app.AnthropicBaseURL {
		t.Fatalf("hybrid from the file: %+v", cfg)
	}
}

func TestConfigFileErrors(t *testing.T) {
	for _, body := range []string{
		"backend = \"local\"\nmodle = \"x\"\n",
		"[hybrid]\nroute_timeout = \"soon\"\n",
		"backend = = 1",
	} {
		if _, err := LoadFile(writeFile(t, body)); err == nil {
			t.Errorf("%q: no error", body)
		}
	}
	if f, err := LoadFile(filepath.Join(t.TempDir(), "none.toml")); f != nil || err != nil {
		t.Errorf("missing file: %v %v", f, err)
	}
}

func TestDefaultConfigPath(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/x/cfg")
	if p := DefaultConfigPath(); p != filepath.Join("/x/cfg", "neu", "config.toml") {
		t.Fatal(p)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "/home/u")
	if p := DefaultConfigPath(); p != filepath.Join("/home/u", ".config", "neu", "config.toml") {
		t.Fatal(p)
	}
}

func TestVersionFlag(t *testing.T) {
	var out, errw strings.Builder
	if code := run(context.Background(), []string{"-version"}, strings.NewReader(""), &out, &errw); code != 0 || !strings.HasPrefix(out.String(), "neu ") {
		t.Fatalf("code %d, out %q, err %q", code, out.String(), errw.String())
	}
	version = "v9.9.9"
	defer func() { version = "" }()
	if Version() != "v9.9.9" {
		t.Fatal(Version())
	}
}
