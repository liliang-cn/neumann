package app

import (
	"flag"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
)

// Timed must show the machine exactly the optional interfaces the OpenAI
// adapter has: one it hides changes the machine's behaviour silently.
func TestTimedForwardsEveryOptionalInterface(t *testing.T) {
	var inner model.Adapter = openai.New("http://x/v1", "k", "m")
	var outer model.Adapter = &Timed{Adapter: openai.New("http://x/v1", "k", "m")}
	check := func(name string, has func(model.Adapter) bool) {
		if has(inner) != has(outer) {
			t.Errorf("%s: adapter %v, Timed %v", name, has(inner), has(outer))
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
	a := c.Sides()[0].Adapter().Adapter
	caps := a.Caps()
	if c.Cores != 1 || c.Model != LocalModel || c.BaseURL != LocalBaseURL || a.Parallel != 1 || !a.Prefill || a.ThinkOff != "none" ||
		a.ThinkTags != [2]string{"<think>", "</think>"} || a.Timeout != 0 || a.FirstByteTimeout != localFirstByte || a.IdleTimeout != localIdle ||
		a.MaxTokens != 8192 || !caps.ThinkPrefill || !caps.Prefill || caps.Parallel != 1 {
		t.Fatalf("local adapter: cores %d, %+v, caps %+v", c.Cores, a, caps)
	}
}
