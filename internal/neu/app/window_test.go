package app

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/liliang-cn/neumann/model/anthropic"
	"github.com/liliang-cn/neumann/model/openai"
)

// fakeOllama answers /api/generate (loading the model, unless refuse) and
// /api/ps (the loaded models with their context_length).
type fakeOllama struct {
	mu     sync.Mutex
	loaded map[string]int // model → context_length once loaded
	ctxLen int            // what a load gives
	refuse bool           // /api/generate fails (the model is not there)
	loads  int
}

func (f *fakeOllama) serve(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/api/generate":
			if f.refuse {
				http.Error(w, `{"error":"model not found"}`, http.StatusNotFound)
				return
			}
			var req struct{ Model string }
			json.NewDecoder(r.Body).Decode(&req)
			if !strings.Contains(req.Model, ":") {
				req.Model += ":latest" // as Ollama names it
			}
			f.loads++
			f.loaded = map[string]int{req.Model: f.ctxLen} // one model at a time, as Ollama on a small machine
			w.Write([]byte(`{"done":true,"done_reason":"load"}`))
		case "/api/ps":
			type entry struct {
				Name          string `json:"name"`
				Model         string `json:"model"`
				ContextLength int    `json:"context_length"`
			}
			var ps struct {
				Models []entry `json:"models"`
			}
			for name, n := range f.loaded {
				ps.Models = append(ps.Models, entry{name, name, n})
			}
			json.NewEncoder(w).Encode(ps)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// OllamaWindow loads the model with the server's defaults and reads the
// runtime context length from /api/ps, whatever the base URL's /v1.
func TestOllamaWindowLoaded(t *testing.T) {
	f := &fakeOllama{ctxLen: 32768, loaded: map[string]int{"qwen3.5:4b": 8192}}
	base := f.serve(t)
	for _, b := range []string{base + "/v1", base + "/v1/", base} {
		w, err := OllamaWindow(context.Background(), b, "qwen3.5:4b")
		if err != nil || w != 32768 {
			t.Fatalf("%s: window %d, %v; want 32768 (loaded again with the default)", b, w, err)
		}
	}
	if f.loads != 3 {
		t.Fatalf("%d loads, want 3", f.loads)
	}
	// "m" is "m:latest".
	f.ctxLen = 4096
	if w, err := OllamaWindow(context.Background(), base, "gemma"); err != nil || w != 4096 {
		t.Fatalf("untagged: %d, %v", w, err)
	}
}

// A model that cannot be loaded and is not loaded is an error; one that is
// loaded although the load failed still counts.
func TestOllamaWindowNotLoaded(t *testing.T) {
	f := &fakeOllama{refuse: true, loaded: map[string]int{"other:1b": 2048}}
	base := f.serve(t)
	if w, err := OllamaWindow(context.Background(), base, "qwen3.5:4b"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("not loaded: %d, %v", w, err)
	}
	f.loaded["qwen3.5:4b"] = 16384
	if w, err := OllamaWindow(context.Background(), base, "qwen3.5:4b"); err != nil || w != 16384 {
		t.Fatalf("loaded, load refused: %d, %v", w, err)
	}
	f.loaded["qwen3.5:4b"] = 0
	if _, err := OllamaWindow(context.Background(), base, "qwen3.5:4b"); err == nil {
		t.Fatal("no context_length, no error")
	}
}

func TestOllamaWindowUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	if _, err := OllamaWindow(context.Background(), u+"/v1", "m"); err == nil {
		t.Fatal("unreachable, no error")
	}
	// Not an Ollama: 404 on both.
	srv = httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	if _, err := OllamaWindow(context.Background(), srv.URL, "m"); err == nil {
		t.Fatal("no /api/ps, no error")
	}
}

func parsed(t *testing.T, args ...string) Config {
	t.Helper()
	t.Setenv("CPA_BASE_URL", "https://cloud.example/v1")
	t.Setenv("CPA_MODEL", "cloud-model")
	t.Setenv("CPA_API_KEY", "k")
	t.Setenv("ANTHROPIC_API_KEY", "k")
	var c Config
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c.Flags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	c.MarkSet(fs)
	if err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
	return c
}

func detect(c *Config) []string {
	var warns []string
	c.DetectWindows(context.Background(), func(s string) { warns = append(warns, s) })
	return warns
}

// DetectWindows reads a local side's window from Ollama, falls back to
// LocalFallbackWindow with a warning, leaves a given one alone, and hints
// at -context-window for an openai side.
func TestDetectWindows(t *testing.T) {
	f := &fakeOllama{ctxLen: 32768}
	base := f.serve(t) + "/v1"

	c := parsed(t, "-backend", "local", "-model", "qwen3.5:4b", "-base-url", base)
	if w := detect(&c); len(w) != 0 || c.ContextWindow != 32768 {
		t.Fatalf("detected %d, warnings %q", c.ContextWindow, w)
	}
	if d := c.Describe(); !strings.Contains(d, "window 32768") {
		t.Fatalf("describe %q", d)
	}
	loads := f.loads

	c = parsed(t, "-backend", "local", "-model", "qwen3.5:4b", "-base-url", base, "-context-window", "12000")
	if w := detect(&c); len(w) != 0 || c.ContextWindow != 12000 || f.loads != loads {
		t.Fatalf("given: %d, warnings %q, loads %d", c.ContextWindow, w, f.loads-loads)
	}

	f.refuse = true
	c = parsed(t, "-backend", "local", "-model", "absent:1b", "-base-url", base)
	if w := detect(&c); len(w) != 1 || c.ContextWindow != LocalFallbackWindow || !strings.Contains(w[0], "-context-window") {
		t.Fatalf("fallback: %d, warnings %q", c.ContextWindow, w)
	}
	f.refuse = false

	c = parsed(t, "-backend", "openai")
	if w := detect(&c); len(w) != 1 || c.ContextWindow != 0 || !strings.Contains(w[0], "-context-window") {
		t.Fatalf("openai: %d, warnings %q", c.ContextWindow, w)
	}
	if d := c.Describe(); !strings.Contains(d, "window unknown") {
		t.Fatalf("describe %q", d)
	}
	c = parsed(t, "-backend", "openai", "-context-window", "128000")
	if w := detect(&c); len(w) != 0 || !strings.Contains(c.Describe(), "window 128000") {
		t.Fatalf("openai given: warnings %q, %q", w, c.Describe())
	}

	c = parsed(t, "-backend", "anthropic", "-model", "claude-x")
	if w := detect(&c); len(w) != 0 || !strings.Contains(c.Describe(), "window 200000") {
		t.Fatalf("anthropic: warnings %q, %q", w, c.Describe())
	}

	c = parsed(t, "-backend", "hybrid", "-fast-model", "qwen3.5:4b", "-fast-base-url", base)
	w := detect(&c)
	if c.FastContextWindow != 32768 || c.StrongContextWindow != 0 || len(w) != 1 || !strings.Contains(w[0], "-strong-context-window") {
		t.Fatalf("hybrid: fast %d strong %d, warnings %q", c.FastContextWindow, c.StrongContextWindow, w)
	}
	if d := c.Describe(); !strings.Contains(d, "window 32768") || !strings.Contains(d, "window unknown") {
		t.Fatalf("describe %q", d)
	}
}

// The windows reach the adapters' Caps; a local side's answer reserve is
// at most a quarter of its window.
func TestWindowReachesAdapters(t *testing.T) {
	c := parsed(t, "-backend", "anthropic", "-model", "claude-x", "-context-window", "1000000")
	if w := c.Sides()[0].Adapter().Caps().Window; w != 1000000 {
		t.Fatalf("anthropic window %d", w)
	}
	c = parsed(t, "-backend", "anthropic", "-model", "claude-x")
	if w := c.Sides()[0].Adapter().Caps().Window; w != anthropic.DefaultContextWindow {
		t.Fatalf("anthropic default window %d", w)
	}
	c = parsed(t, "-backend", "openai", "-context-window", "64000")
	if w := c.Sides()[0].Adapter().Caps().Window; w != 64000 {
		t.Fatalf("openai window %d", w)
	}
	for _, tc := range []struct{ window, maxOut int }{{0, 8192}, {32768, 8192}, {65536, 8192}, {16384, 4096}, {4096, 1024}} {
		c = parsed(t, "-backend", "local", "-model", "m", "-context-window", strconv.Itoa(tc.window))
		a := c.Sides()[0].Adapter()
		if a.Caps().Window != tc.window || a.Adapter.(*openai.Adapter).MaxTokens != tc.maxOut {
			t.Fatalf("local window %d: caps %+v", tc.window, a.Caps())
		}
	}
	c = parsed(t, "-backend", "hybrid", "-fast-model", "m", "-fast-context-window", "16384", "-strong-context-window", "100000")
	s := c.Sides()
	if s[0].Adapter().Caps().Window != 16384 || s[1].Adapter().Caps().Window != 100000 {
		t.Fatalf("hybrid sides %+v", s)
	}
	var h Config
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	h.Flags(fs)
	fs.Parse([]string{"-backend", "hybrid", "-fast-model", "m", "-context-window", "1"})
	h.MarkSet(fs)
	if err := h.Resolve(); err == nil || !strings.Contains(err.Error(), "-fast-context-window") {
		t.Fatalf("-context-window with hybrid: %v", err)
	}
}
