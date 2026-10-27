package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/mcp"
)

// MCPServer is one [[mcp]] entry of neu's config file: a server over stdio
// (command, args, env) or streamable HTTP (url, headers), and the effects
// of its tools.
//
// A tool's effect is, in order: the list among read_only, idempotent and
// side_effect that names it; else effect, the server's default:
// "side_effect" (the default), "idempotent", "read_only", or "annotations"
// (what the server's readOnlyHint / idempotentHint say, trusted). The
// effect decides whether a call is repeated after a crash and how an
// approval policy treats it; the server's own hints are not trusted unless
// the entry says so.
type MCPServer struct {
	Name    string            `toml:"name"`
	Command string            `toml:"command"`
	Args    []string          `toml:"args"`
	Env     map[string]string `toml:"env"` // values: $VAR / ${VAR} of neu's environment are expanded
	URL     string            `toml:"url"`
	Headers map[string]string `toml:"headers"` // expanded as env is

	Effect     string   `toml:"effect"`
	ReadOnly   []string `toml:"read_only"`
	Idempotent []string `toml:"idempotent"`
	SideEffect []string `toml:"side_effect"`

	Enabled *bool `toml:"enabled"` // nil: true
	// StartupTimeout bounds starting the server and listing its tools
	// (a Go duration); "": DefaultMCPStartup.
	StartupTimeout string `toml:"startup_timeout"`
}

// DefaultMCPStartup bounds starting an MCP server and listing its tools.
const DefaultMCPStartup = 30 * time.Second

// BackendKeyEnvs are environment variables an MCP server does not get
// unless its entry's env names them, besides the key variables of the
// configured backend.
var BackendKeyEnvs = []string{"ANTHROPIC_API_KEY", OpenAIKeyEnv, LocalKeyEnv}

var serverName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*$`)

// On reports whether the entry is enabled.
func (s MCPServer) On() bool { return s.Enabled == nil || *s.Enabled }

// Check reports what is wrong with the entry.
func (s MCPServer) Check() error {
	if !serverName.MatchString(s.Name) {
		return fmt.Errorf("mcp server name %q: want letters, digits and '-' (its tools are <name>_<tool>)", s.Name)
	}
	switch {
	case s.Command == "" && s.URL == "":
		return fmt.Errorf("mcp %s: needs command (stdio) or url (HTTP)", s.Name)
	case s.Command != "" && s.URL != "":
		return fmt.Errorf("mcp %s: command and url are exclusive", s.Name)
	case s.URL != "" && (len(s.Args) > 0 || len(s.Env) > 0):
		return fmt.Errorf("mcp %s: args and env apply to a command, not a url", s.Name)
	case s.Command != "" && len(s.Headers) > 0:
		return fmt.Errorf("mcp %s: headers apply to a url, not a command", s.Name)
	}
	if _, err := s.defaultEffect(); err != nil {
		return err
	}
	seen := map[string]string{}
	for list, tools := range map[string][]string{"read_only": s.ReadOnly, "idempotent": s.Idempotent, "side_effect": s.SideEffect} {
		for _, t := range tools {
			if prev, dup := seen[t]; dup && prev != list {
				return fmt.Errorf("mcp %s: tool %q is in both %s and %s", s.Name, t, prev, list)
			}
			seen[t] = list
		}
	}
	if s.StartupTimeout != "" {
		if d, err := time.ParseDuration(s.StartupTimeout); err != nil || d <= 0 {
			return fmt.Errorf("mcp %s: startup_timeout %q: want a positive duration such as 30s", s.Name, s.StartupTimeout)
		}
	}
	return nil
}

// defaultEffect is the effect of a tool no list names; ok false: the
// server's annotations decide.
func (s MCPServer) defaultEffect() (e *bus.Effect, err error) {
	switch s.Effect {
	case "", "side_effect":
		return ptr(bus.SideEffect), nil
	case "idempotent":
		return ptr(bus.Idempotent), nil
	case "read_only":
		return ptr(bus.ReadOnly), nil
	case "annotations":
		return nil, nil
	}
	return nil, fmt.Errorf("mcp %s: effect %q: want side_effect, idempotent, read_only or annotations", s.Name, s.Effect)
}

func ptr[T any](v T) *T { return &v }

// effects maps each tool the lists name to its effect.
func (s MCPServer) effects() map[string]bus.Effect {
	m := map[string]bus.Effect{}
	for _, t := range s.ReadOnly {
		m[t] = bus.ReadOnly
	}
	for _, t := range s.Idempotent {
		m[t] = bus.Idempotent
	}
	for _, t := range s.SideEffect {
		m[t] = bus.SideEffect
	}
	return m
}

// env is the KEY=VALUE pairs added to the server's environment: every
// key variable in scrub that the entry does not name, set empty (bus/mcp
// starts a server with neu's environment and can only add to it), then
// the entry's own pairs, expanded.
func (s MCPServer) env(scrub []string) []string {
	var kv []string
	for _, k := range scrub {
		if _, named := s.Env[k]; !named {
			kv = append(kv, k+"=")
		}
	}
	keys := make([]string, 0, len(s.Env))
	for k := range s.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kv = append(kv, k+"="+os.ExpandEnv(s.Env[k]))
	}
	return kv
}

// mcpDevice is an MCP server as neu attaches it: its tools as listed at
// start-up, with the effects the entry gives them.
type mcpDevice struct {
	*mcp.Device
	defs []bus.ToolDef
	log  io.Closer // the server's stderr; nil: none
}

// Tools is the snapshot taken at start-up; the bus takes one anyway.
func (d *mcpDevice) Tools(context.Context) ([]bus.ToolDef, error) {
	return append([]bus.ToolDef(nil), d.defs...), nil
}

func (d *mcpDevice) Close() error {
	err := d.Device.Close()
	if d.log != nil {
		d.log.Close()
	}
	return err
}

// scrubEnvs are the key variables an MCP server does not inherit.
func (a *App) scrubEnvs() []string {
	set := map[string]bool{}
	for _, k := range BackendKeyEnvs {
		set[k] = true
	}
	for _, s := range a.Cfg.Sides() {
		if s.KeyEnv != "" {
			set[s.KeyEnv] = true
		}
	}
	if a.Cfg.KeyEnv != "" {
		set[a.Cfg.KeyEnv] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// startMCP starts the enabled servers at once and returns those that
// started, in config order. One that fails is a warning.
func (a *App) startMCP(ctx context.Context, servers []MCPServer, warn func(string, ...any)) []*mcpDevice {
	out := make([]*mcpDevice, len(servers))
	var wg sync.WaitGroup
	scrub := a.scrubEnvs()
	for i, s := range servers {
		if !s.On() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, warns, err := a.startOne(ctx, s, scrub)
			for _, w := range warns {
				warn("mcp %s: %s", s.Name, w)
			}
			if err != nil {
				warn("mcp %s: %v; continuing without it", s.Name, err)
				return
			}
			out[i] = d
		}()
	}
	wg.Wait()
	var started []*mcpDevice
	for _, d := range out {
		if d != nil {
			started = append(started, d)
		}
	}
	return started
}

// startOne starts one server, lists its tools and sets their effects.
// warns are names in the effect lists that the server has no tool for.
func (a *App) startOne(ctx context.Context, s MCPServer, scrub []string) (*mcpDevice, []string, error) {
	if err := s.Check(); err != nil {
		return nil, nil, err
	}
	timeout := DefaultMCPStartup
	if s.StartupTimeout != "" {
		timeout, _ = time.ParseDuration(s.StartupTimeout)
	}
	opts := []mcp.Option{mcp.WithConnectTimeout(timeout)}
	var logf *os.File
	var dev *mcp.Device
	if s.URL != "" {
		for k, v := range s.Headers {
			opts = append(opts, mcp.WithHeader(k, os.ExpandEnv(v)))
		}
		dev = mcp.HTTP(s.Name, s.URL, opts...)
	} else {
		// The server's stderr goes to a log next to the store, which a
		// failure points to.
		if dir := filepath.Dir(a.Cfg.Store); dir != "" {
			if f, err := os.Create(filepath.Join(dir, "mcp-"+s.Name+".log")); err == nil {
				logf = f
				opts = append(opts, mcp.WithStderr(f))
			}
		}
		opts = append(opts, mcp.WithEnv(s.env(scrub)...))
		dev = mcp.Stdio(s.Name, s.Command, s.Args, opts...)
	}
	fail := func(err error) (*mcpDevice, []string, error) {
		dev.Close()
		if logf != nil {
			logf.Close()
			if st, serr := os.Stat(logf.Name()); serr == nil && st.Size() > 0 {
				err = fmt.Errorf("%w (its stderr: %s)", err, logf.Name())
			}
		}
		return nil, nil, err
	}
	sctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := dev.Connect(sctx); err != nil {
		return fail(trimMCP(err, s.Name))
	}
	defs, err := dev.Tools(sctx)
	if err != nil {
		return fail(trimMCP(err, s.Name))
	}
	if len(defs) == 0 {
		return fail(errors.New("the server has no tools"))
	}
	def, _ := s.defaultEffect()
	over := s.effects()
	have := map[string]bool{}
	for i := range defs {
		have[defs[i].Name] = true
		switch e, ok := over[defs[i].Name]; {
		case ok:
			defs[i].Effect = e
		case def != nil:
			defs[i].Effect = *def
		}
	}
	var warns []string
	var unknown []string
	for t := range over {
		if !have[t] {
			unknown = append(unknown, t)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		warns = append(warns, fmt.Sprintf("no tool named %s (in its effect lists); ignored", strings.Join(unknown, ", ")))
	}
	md := &mcpDevice{Device: dev, defs: defs}
	if logf != nil {
		md.log = logf
	}
	return md, warns, nil
}

// trimMCP drops the "mcp <name>: " the device puts before its errors, as
// the warning names the server already.
func trimMCP(err error, name string) error {
	msg := strings.TrimPrefix(err.Error(), "mcp "+name+": ")
	return errors.New(msg)
}
