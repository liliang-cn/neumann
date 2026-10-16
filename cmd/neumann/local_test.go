package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
)

func configure(t *testing.T, env map[string]string, args ...string) (config, error) {
	t.Helper()
	getenv := func(k string) string { return env[k] }
	c, err := parse(args, getenv, io.Discard)
	if err != nil {
		t.Fatalf("parse %q: %v", args, err)
	}
	return c, c.resolve(getenv)
}

func TestLocal(t *testing.T) {
	c, err := configure(t, nil, "-local", "-model", "qwen")
	if err != nil {
		t.Fatal(err)
	}
	if c.baseURL != localBaseURL || c.parallel != 1 || c.cores != 1 || c.maxTokens != localMaxTokens {
		t.Fatalf("-local: base %s, parallel %d, cores %d, max tokens %d", c.baseURL, c.parallel, c.cores, c.maxTokens)
	}
	a := c.adapter().(*openai.Adapter)
	if !a.Prefill || a.ThinkOff != "none" || a.Timeout != 0 || a.FirstByteTimeout != 10*time.Minute || a.IdleTimeout != 2*time.Minute {
		t.Fatalf("adapter: prefill %v, think off %q, timeouts %v %v %v", a.Prefill, a.ThinkOff, a.Timeout, a.FirstByteTimeout, a.IdleTimeout)
	}
	if got := model.CapsOf(a); got.Parallel != 1 || !got.Prefill || !got.ThinkOff || got.MaxOut != localMaxTokens {
		t.Fatalf("caps %+v", got)
	}
}

func TestLocalOverrides(t *testing.T) {
	for name, tc := range map[string]struct {
		env                        map[string]string
		args                       []string
		parallel, cores, maxTokens int
		base                       string
	}{
		"parallel":          {args: []string{"-local", "-parallel", "2"}, parallel: 2, cores: 2, maxTokens: localMaxTokens, base: localBaseURL},
		"env":               {env: map[string]string{"NEUMANN_LOCAL": "1", "NEUMANN_PARALLEL": "3"}, parallel: 3, cores: 3, maxTokens: localMaxTokens, base: localBaseURL},
		"max tokens":        {args: []string{"-local", "-max-tokens", "1024"}, parallel: 1, cores: 1, maxTokens: 1024, base: localBaseURL},
		"max tokens in env": {env: map[string]string{"NEUMANN_LOCAL": "1", "NEUMANN_MAX_TOKENS": "0"}, parallel: 1, cores: 1, maxTokens: 0, base: localBaseURL},
		"cores":             {args: []string{"-local", "-cores", "4"}, parallel: 1, cores: 4, maxTokens: localMaxTokens, base: localBaseURL},
		"cores in env":      {env: map[string]string{"NEUMANN_CORES": "3"}, args: []string{"-local"}, parallel: 1, cores: 3, maxTokens: localMaxTokens, base: localBaseURL},
		"base url":          {args: []string{"-local", "-base-url", "http://box:1234/v1"}, parallel: 1, cores: 1, maxTokens: localMaxTokens, base: "http://box:1234/v1"},
		"flag beats env":    {env: map[string]string{"NEUMANN_LOCAL": "1"}, args: []string{"-local=false"}, cores: neumann.DefaultCores, base: defaultBaseURL["openai"]},
		"parallel alone":    {args: []string{"-parallel", "2"}, parallel: 2, cores: neumann.DefaultCores, base: defaultBaseURL["openai"]},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := configure(t, tc.env, append(tc.args, "-model", "m")...)
			if err != nil {
				t.Fatal(err)
			}
			if c.parallel != tc.parallel || c.cores != tc.cores || c.maxTokens != tc.maxTokens || c.baseURL != tc.base {
				t.Fatalf("parallel %d, cores %d, max tokens %d, base %s", c.parallel, c.cores, c.maxTokens, c.baseURL)
			}
			a := c.adapter().(*openai.Adapter)
			if a.Parallel != tc.parallel || a.Prefill != c.local {
				t.Fatalf("adapter parallel %d, prefill %v", a.Parallel, a.Prefill)
			}
			if !c.local && (a.Timeout != openai.DefaultTimeout || a.ThinkOff != "") {
				t.Fatalf("without -local the adapter keeps its defaults: %v %q", a.Timeout, a.ThinkOff)
			}
		})
	}
}

func TestLocalRejects(t *testing.T) {
	for name, args := range map[string][]string{
		"anthropic":          {"-local", "-provider", "anthropic"},
		"anthropic parallel": {"-parallel", "2", "-provider", "anthropic"},
		"negative parallel":  {"-parallel", "-1"},
	} {
		if _, err := configure(t, nil, append(args, "-model", "m")...); err == nil || !strings.Contains(err.Error(), "parallel") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// -local end to end: one vCore, and requests carry the local output cap.
func TestLocalEndToEnd(t *testing.T) {
	f := newFake(t)
	p := launch(t, env(f), "-echo", "-local")
	p.send(`{"kind":"subscribe","from_seq":0}`)
	p.reply(nil)
	var evs []map[string]any
	p.send(`{"kind":"submit","task_id":"l1","input":"ping"}`)
	if r := p.reply(&evs); r["ok"] != true {
		t.Fatalf("submit: %v", r)
	}
	p.until(evs, "l1", "done")
	p.send(`{"kind":"status"}`)
	if r := p.reply(nil); r["status"].(map[string]any)["groups"].([]any)[0].(map[string]any)["target"] != float64(1) {
		t.Fatalf("status: %v", r)
	}
	if !strings.Contains(p.stderr.String(), "local (1 at once)") {
		t.Fatalf("stderr:\n%s", p.stderr.String())
	}
	p.stdin.Close()
	if err := p.wait(); err != nil {
		t.Fatalf("exit: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.maxTokens) != 2 || f.maxTokens[0] != localMaxTokens || f.maxTokens[1] != localMaxTokens {
		t.Fatalf("max_tokens %v", f.maxTokens)
	}
}
