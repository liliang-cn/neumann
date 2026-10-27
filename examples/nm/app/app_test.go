package app

import (
	"flag"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/anthropic"
	"github.com/liliang-cn/neumann/model/openai"
)

// Timed must show the machine exactly the optional interfaces the OpenAI
// adapter has: one it hides changes the machine's behaviour silently.
func TestTimedForwardsEveryOptionalInterface(t *testing.T) {
	for _, mk := range []func() Backend{
		func() Backend { return openai.New("http://x/v1", "k", "m") },
		func() Backend { return anthropic.New("http://x/v1", "k", "m") },
	} {
		forwards(t, mk(), &Timed{Adapter: mk()})
	}
}

func forwards(t *testing.T, inner, outer model.Adapter) {
	t.Helper()
	check := func(name string, has func(model.Adapter) bool) {
		if has(inner) != has(outer) {
			t.Errorf("%s %s: adapter %v, Timed %v", inner.Provider(), name, has(inner), has(outer))
		}
	}
	check("Streamer", func(a model.Adapter) bool { _, ok := a.(model.Streamer); return ok })
	check("CapsReporter", func(a model.Adapter) bool { _, ok := a.(model.CapsReporter); return ok })
	check("OutputCap", func(a model.Adapter) bool { _, ok := a.(model.OutputCap); return ok })
	check("Classifier", func(a model.Adapter) bool { _, ok := a.(model.Classifier); return ok })
	check("Metered", func(a model.Adapter) bool { _, ok := a.(model.Metered); return ok })
	check("RateReporter", func(a model.Adapter) bool { _, ok := a.(model.RateReporter); return ok })
	check("Warmer", func(a model.Adapter) bool { _, ok := a.(model.Warmer); return ok })
}

// -backend local configures the adapter as cmd/neumann -local does.
func TestLocalBackendSettings(t *testing.T) {
	var c Config
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c.Flags(fs)
	if err := fs.Parse([]string{"-backend", "local"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
	a := c.Sides()[0].Adapter().Adapter.(*openai.Adapter)
	caps := a.Caps()
	if c.Cores != 1 || c.Model != LocalModel || c.BaseURL != LocalBaseURL || a.Parallel != 1 || !a.Prefill || a.ThinkOff != "none" ||
		a.ThinkTags != [2]string{"<think>", "</think>"} || a.Timeout != 0 || a.FirstByteTimeout != localFirstByte || a.IdleTimeout != localIdle ||
		a.MaxTokens != 8192 || !caps.ThinkPrefill || !caps.Prefill || caps.Parallel != 1 {
		t.Fatalf("local adapter: cores %d, %+v, caps %+v", c.Cores, a, caps)
	}
}

// -backend anthropic builds the Anthropic adapter with the model's ThinkOff.
func TestAnthropicBackendSettings(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "k")
	var c Config
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c.Flags(fs)
	if err := fs.Parse([]string{"-backend", "anthropic"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
	s := c.Sides()[0]
	a, ok := s.Adapter().Adapter.(*anthropic.Adapter)
	if !ok || s.Kind != "cloud" || c.Cores != 8 || a.Model != AnthropicModel || a.BaseURL != AnthropicBaseURL || a.ThinkOff != "between_tools" || !a.Caps().ThinkOff {
		t.Fatalf("anthropic side %+v, adapter %+v", s, a)
	}
	t.Setenv("ANTHROPIC_API_KEY", "")
	if err := c.Resolve(); err == nil {
		t.Fatal("no key, no error")
	}
}
