package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model/anthropic"
)

// The context window when nothing gives one (design §5.3: the machine
// compacts a task once its request and output reserve pass CompactAt of
// the adapter's Caps.Window; 0 there is unknown and compaction waits for
// the provider to reject a request as too long).
//
// A local Ollama never rejects one: past the context length its runner
// was loaded with (num_ctx) it drops the start of the prompt and answers
// anyway. So for a local side neu asks Ollama what that length is
// (DetectWindows), and when it cannot tell assumes LocalFallbackWindow,
// Ollama's own smallest default. The OpenAI-compatible endpoint cannot set
// num_ctx per request: the server's OLLAMA_CONTEXT_LENGTH, or a Modelfile's
// PARAMETER num_ctx, decides it.
const (
	LocalFallbackWindow = 4096
	windowDefaults      = "200000 (anthropic), read from Ollama (local; 4096 when it cannot be read), unknown (openai: compaction then waits until the provider rejects a request as too long)"

	// ollamaLoadTimeout bounds loading the model before reading its
	// context length; ollamaPSTimeout bounds reading it.
	ollamaLoadTimeout = 60 * time.Second
	ollamaPSTimeout   = 2 * time.Second
)

// WindowDesc is the side's context window for the startup line.
func (s Side) WindowDesc() string {
	switch {
	case s.Window > 0:
		return fmt.Sprintf("window %d", s.Window)
	case s.API == "anthropic":
		return fmt.Sprintf("window %d", anthropic.DefaultContextWindow)
	}
	return "window unknown"
}

// DetectWindows fills the context windows the flags and the config file
// left at 0: a local side's from the Ollama it talks to (OllamaWindow), or
// LocalFallbackWindow with a warning when that cannot be read. An openai
// side's stays unknown, with a hint. warn gets one line per side.
func (c *Config) DetectWindows(ctx context.Context, warn func(string)) {
	local := func(dst *int, base, model, flag string) {
		if *dst > 0 {
			return
		}
		w, err := OllamaWindow(ctx, base, model)
		if err != nil {
			*dst = LocalFallbackWindow
			warn(fmt.Sprintf("could not read the context length Ollama runs %s with (%v); assuming %d tokens, pass %s N to set it", model, err, LocalFallbackWindow, flag))
			return
		}
		*dst = w
	}
	hint := func(flag string) {
		warn(fmt.Sprintf("the openai endpoint's context window is unknown: a task is compacted only after the provider rejects a request as too long; pass %s N to compact before that", flag))
	}
	switch c.Backend {
	case "local":
		local(&c.ContextWindow, c.BaseURL, c.Model, "-context-window")
	case "cloud", "openai":
		if c.ContextWindow == 0 {
			hint("-context-window")
		}
	case "hybrid":
		local(&c.FastContextWindow, c.FastBaseURL, c.FastModel, "-fast-context-window")
		if c.StrongAPI != "anthropic" && c.StrongContextWindow == 0 {
			hint("-strong-context-window")
		}
	}
}

// OllamaWindow is the context length the Ollama behind base (its
// OpenAI-compatible URL, ending in /v1, or its root) runs model with: the
// runtime num_ctx, which /api/ps reports as context_length, not the
// model's training maximum in /api/show (qwen3.5:4b: 262144 there, 32768
// loaded on a machine without OLLAMA_CONTEXT_LENGTH). It first loads the
// model with the server's default options (POST /api/generate with no
// prompt, at once when it is loaded already), which is what a request on
// the OpenAI-compatible endpoint gets: a model another client loaded with
// its own num_ctx is loaded again with the default, as the first neu
// request would. The requests never go through a proxy.
func OllamaWindow(ctx context.Context, base, model string) (int, error) {
	root := strings.TrimSuffix(strings.TrimRight(base, "/"), "/v1")
	tr := &http.Transport{Proxy: nil}
	defer tr.CloseIdleConnections()
	hc := &http.Client{Transport: tr}

	body, _ := json.Marshal(map[string]string{"model": model})
	lctx, cancel := context.WithTimeout(ctx, ollamaLoadTimeout)
	loadErr := ollamaDo(lctx, hc, http.MethodPost, root+"/api/generate", body, nil)
	cancel()
	var unreachable *netError
	if errors.As(loadErr, &unreachable) {
		return 0, loadErr
	}

	var ps struct {
		Models []struct {
			Name          string `json:"name"`
			Model         string `json:"model"`
			ContextLength int    `json:"context_length"`
		} `json:"models"`
	}
	pctx, cancel := context.WithTimeout(ctx, ollamaPSTimeout)
	defer cancel()
	if err := ollamaDo(pctx, hc, http.MethodGet, root+"/api/ps", nil, &ps); err != nil {
		return 0, err
	}
	for _, m := range ps.Models {
		if sameModel(m.Name, model) || sameModel(m.Model, model) {
			if m.ContextLength > 0 {
				return m.ContextLength, nil
			}
			return 0, fmt.Errorf("/api/ps reports no context_length for %s", model)
		}
	}
	if loadErr != nil {
		return 0, fmt.Errorf("loading %s: %w", model, loadErr)
	}
	return 0, fmt.Errorf("%s is not loaded", model)
}

// sameModel compares Ollama model names, where "m" is "m:latest".
func sameModel(a, b string) bool {
	tag := func(s string) string {
		if !strings.Contains(s, ":") {
			return s + ":latest"
		}
		return s
	}
	return a != "" && tag(a) == tag(b)
}

// netError is a request that got no HTTP answer at all.
type netError struct{ err error }

func (e *netError) Error() string { return e.err.Error() }
func (e *netError) Unwrap() error { return e.err }

// ollamaDo sends one request and decodes a 200 answer into out (nil:
// discard it).
func ollamaDo(ctx context.Context, hc *http.Client, method, url string, body []byte, out any) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := hc.Do(req)
	if err != nil {
		return &netError{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return fmt.Errorf("%s %s: %s %s", method, url[strings.LastIndex(url, "/api/"):], resp.Status, strings.TrimSpace(string(msg)))
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
