// Package mcp is a bus device backed by a Model Context Protocol server,
// spoken through the official SDK (github.com/modelcontextprotocol/go-sdk):
// a child process over stdio ([Stdio]), a streamable HTTP endpoint ([HTTP]),
// or any SDK transport ([New]).
//
// # Connecting
//
// A device connects lazily, on the first Tools or Call; [Device.Connect]
// connects ahead of time for a caller that wants a bad command or URL to fail
// at start-up. Connecting and listing tools are bounded by ctx and, when ctx
// has no earlier deadline, by [WithConnectTimeout] (default 30s), so attaching
// a device whose server never answers fails instead of hanging.
//
// # Effects
//
// Each MCP tool's annotations map to a [bus.Effect]: readOnlyHint → ReadOnly;
// otherwise idempotentHint → Idempotent; otherwise SideEffect.
// destructiveHint never loosens anything: a tool that says it is not
// destructive is still SideEffect. Annotations are hints from the server, not
// guarantees, and recovery after a crash relies on them (design §4.2): a
// ReadOnly or Idempotent call may be sent again. [WithEffect] overrides the
// mapping per tool. Overriding toward ReadOnly or Idempotent is the operator's
// statement that a repeat is harmless, and the operator's responsibility.
//
// # Calls
//
// A call's idempotency key travels in the request's _meta under [KeyMeta],
// so a server can drop a repeat. Text content is concatenated; other content
// is summarised ("[image image/png 1234 bytes]"); structuredContent is
// appended as JSON when the result carries no text. A tool that reports an
// error (isError) is an ordinary result with IsError set. A protocol or
// transport failure is a Go error, which the bus turns into an error result.
//
// # Reconnecting
//
// When the session dies (the server process exits, the HTTP session is lost),
// the next call reconnects, once; if that fails, the call fails. A call in
// flight when the session died returns an error and is not retried here: its
// outcome is unknown, and the bus and the call log decide what happens next.
// The one call that is sent again is one an HTTP server refused because it no
// longer knew the session (404): the server did not run it.
//
// # Tool list changes
//
// The bus snapshots a device's tools when it is attached, and in this version
// does not re-attach. A notifications/tools/list_changed from the server is
// only recorded: [Device.ToolsChanged] reports it until Tools is called again.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

// KeyMeta is the _meta key that carries a call's idempotency key.
const KeyMeta = "neumann/idempotency_key"

const (
	DefaultConnectTimeout = 30 * time.Second
	DefaultStopTimeout    = 2 * time.Second
)

// ErrClosed is returned by a device used after Close.
var ErrClosed = errors.New("mcp: device closed")

type config struct {
	effects        map[string]bus.Effect
	reportUnknown  map[string]bool
	env            []string
	stderr         io.Writer
	header         http.Header
	httpClient     *http.Client
	connectTimeout time.Duration
	stopTimeout    time.Duration
}

// Option configures a device. Options that do not apply to its transport
// (WithEnv on an HTTP device, say) are ignored.
type Option func(*config)

// WithEffect sets a tool's effect, overriding what its annotations say. tool
// is the server's name for it, without the device prefix. Naming a tool the
// server does not have makes Tools fail.
func WithEffect(tool string, e bus.Effect) Option {
	return func(c *config) { c.effects[tool] = e }
}

// WithReportUnknown marks tools whose unknown outcome after a restart may be
// reported to the model instead of held for an operator (ToolDef.ReportUnknown).
// Naming a tool the server does not have makes Tools fail.
func WithReportUnknown(tools ...string) Option {
	return func(c *config) {
		for _, t := range tools {
			c.reportUnknown[t] = true
		}
	}
}

// WithEnv adds KEY=VALUE pairs to a stdio server's environment, which is
// otherwise this process's own; a later pair overrides an earlier one.
func WithEnv(kv ...string) Option {
	return func(c *config) { c.env = append(c.env, kv...) }
}

// WithStderr sends a stdio server's stderr to w; by default it is discarded.
func WithStderr(w io.Writer) Option {
	return func(c *config) { c.stderr = w }
}

// WithHeader adds a header to every HTTP request, e.g. Authorization.
func WithHeader(key, value string) Option {
	return func(c *config) { c.header.Add(key, value) }
}

// WithHTTPClient sets the HTTP client; headers from WithHeader are added on
// top of its transport.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) { c.httpClient = hc }
}

// WithConnectTimeout bounds connecting (also a reconnect inside Call) and
// listing tools when the caller's ctx has no earlier deadline; 0 leaves only
// ctx. Calls themselves are bounded by the bus, not by this.
func WithConnectTimeout(d time.Duration) Option {
	return func(c *config) { c.connectTimeout = d }
}

// WithStopTimeout is how long closing a stdio server waits after closing its
// stdin, and again after SIGTERM, before killing it.
func WithStopTimeout(d time.Duration) Option {
	return func(c *config) { c.stopTimeout = d }
}

// dialer opens a fresh transport for one connection. after, if not nil, runs
// once the connection has closed.
type dialer func(ctx context.Context, cfg *config) (t sdk.Transport, after func(), err error)

// Device is an MCP server attached to the bus. It is safe for concurrent use.
type Device struct {
	name   string
	dial   dialer
	cfg    config
	client *sdk.Client

	// lock guards cur and closed, and is held while connecting; it is a
	// channel so that a caller waiting for it can give up when ctx ends.
	lock    chan struct{}
	cur     *conn
	closed  bool
	changed atomic.Bool
	pending sync.WaitGroup // abandoned handshakes still being cleaned up
}

// watched is a transport whose connection can be closed from outside while
// the SDK is still in the handshake.
type watched struct {
	sdk.Transport
	mu      sync.Mutex
	conn    sdk.Connection
	aborted bool
}

func (w *watched) Connect(ctx context.Context) (sdk.Connection, error) {
	c, err := w.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.aborted {
		_ = c.Close()
		return nil, errors.New("connect abandoned")
	}
	w.conn = c
	return c, nil
}

// abort closes the connection, if there is one yet, without waiting: closing
// a child's stdin waits for the child to exit.
func (w *watched) abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.aborted = true
	if c := w.conn; c != nil {
		go func() { _ = c.Close() }()
	}
}

// conn is one session. dead is closed once the session has ended and its
// cleanup (reaping a child process) has run.
type conn struct {
	cs   *sdk.ClientSession
	dead chan struct{}
}

// New is a device over any SDK transport. dial must return a fresh transport
// on every call: each reconnection uses a new one.
func New(name string, dial func(ctx context.Context) (sdk.Transport, error), opts ...Option) *Device {
	return newDevice(name, func(ctx context.Context, _ *config) (sdk.Transport, func(), error) {
		t, err := dial(ctx)
		return t, nil, err
	}, opts)
}

// Stdio is a device whose server is a child process speaking MCP on its stdin
// and stdout. The process is started on connect and stopped by Close: stdin
// is closed, then SIGTERM, then SIGKILL, each after WithStopTimeout. On unix
// it runs in its own process group, and whatever is left of the group when
// it exits is killed too, so a server started through a wrapper (npx, uvx)
// leaves nothing behind.
func Stdio(name, command string, args []string, opts ...Option) *Device {
	args = append([]string(nil), args...)
	return newDevice(name, func(_ context.Context, cfg *config) (sdk.Transport, func(), error) {
		cmd := exec.Command(command, args...)
		cmd.Env = append(os.Environ(), cfg.env...)
		cmd.Stderr = cfg.stderr
		ownGroup(cmd)
		after := func() {
			if cmd.Process != nil {
				killGroup(cmd.Process.Pid)
			}
		}
		return &sdk.CommandTransport{Command: cmd, TerminateDuration: cfg.stopTimeout}, after, nil
	}, opts)
}

// HTTP is a device whose server is a streamable HTTP endpoint.
func HTTP(name, url string, opts ...Option) *Device {
	return newDevice(name, func(_ context.Context, cfg *config) (sdk.Transport, func(), error) {
		hc := cfg.httpClient
		if hc == nil {
			hc = &http.Client{}
		}
		if len(cfg.header) > 0 {
			c := *hc
			base := c.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			c.Transport = headerTransport{base: base, header: cfg.header}
			hc = &c
		}
		return &sdk.StreamableClientTransport{Endpoint: url, HTTPClient: hc}, nil, nil
	}, opts)
}

type headerTransport struct {
	base   http.RoundTripper
	header http.Header
}

func (h headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range h.header {
		r.Header[k] = append([]string(nil), v...)
	}
	return h.base.RoundTrip(r)
}

func newDevice(name string, dial dialer, opts []Option) *Device {
	d := &Device{
		name: name,
		dial: dial,
		cfg: config{
			effects:        map[string]bus.Effect{},
			reportUnknown:  map[string]bool{},
			header:         http.Header{},
			connectTimeout: DefaultConnectTimeout,
			stopTimeout:    DefaultStopTimeout,
		},
		lock: make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(&d.cfg)
	}
	d.client = sdk.NewClient(&sdk.Implementation{Name: "neumann", Version: "v1"}, &sdk.ClientOptions{
		ToolListChangedHandler: func(context.Context, *sdk.ToolListChangedRequest) { d.changed.Store(true) },
	})
	return d
}

func (d *Device) Name() string { return d.name }

// ToolsChanged reports whether the server announced a change to its tool list
// since Tools was last called. The bus does not act on it in this version.
func (d *Device) ToolsChanged() bool { return d.changed.Load() }

// Connect connects now instead of on first use. It is a no-op on a device
// that is already connected.
func (d *Device) Connect(ctx context.Context) error {
	ctx, cancel := d.setupContext(ctx)
	defer cancel()
	_, err := d.session(ctx)
	return err
}

func (d *Device) setupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if d.cfg.connectTimeout > 0 {
		return context.WithTimeout(ctx, d.cfg.connectTimeout)
	}
	return context.WithCancel(ctx)
}

// session returns the live session, connecting (once) if there is none or
// the last one died.
func (d *Device) session(ctx context.Context) (*conn, error) {
	select {
	case d.lock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-d.lock }()
	if d.closed {
		return nil, ErrClosed
	}
	if c := d.cur; c != nil {
		select {
		case <-c.dead:
			d.cur = nil
		default:
			return c, nil
		}
	}
	// Connecting is bounded on every path, including a reconnect inside Call.
	ctx, cancel := d.setupContext(ctx)
	defer cancel()
	t, after, err := d.dial(ctx, &d.cfg)
	if err != nil {
		return nil, fmt.Errorf("mcp %s: connect: %w", d.name, err)
	}
	if after == nil {
		after = func() {}
	}
	// The SDK honours ctx while it waits for an answer but not while it
	// writes, and a peer that never reads blocks the write for good. So the
	// handshake runs aside, and a caller whose ctx ends closes the connection
	// under it and leaves.
	wt := &watched{Transport: t}
	type connected struct {
		cs  *sdk.ClientSession
		err error
	}
	done := make(chan connected, 1)
	go func() {
		cs, err := d.client.Connect(ctx, wt, nil)
		done <- connected{cs, err}
	}()
	var r connected
	select {
	case r = <-done:
	case <-ctx.Done():
		wt.abort()
		d.pending.Add(1)
		go func() {
			defer d.pending.Done()
			if r := <-done; r.cs != nil {
				_ = r.cs.Close()
			}
			after()
		}()
		return nil, fmt.Errorf("mcp %s: connect: %w", d.name, ctx.Err())
	}
	if r.err != nil {
		// Connect closes what it opened, which reaps a child it started.
		after()
		return nil, fmt.Errorf("mcp %s: connect: %w", d.name, r.err)
	}
	cs := r.cs
	c := &conn{cs: cs, dead: make(chan struct{})}
	go func() {
		_ = cs.Wait()
		_ = cs.Close() // the server went away: release (and reap) at once
		after()
		close(c.dead)
	}()
	d.cur = c
	return c, nil
}

// drop retires a session that failed, so the next call reconnects.
func (d *Device) drop(c *conn) {
	d.lock <- struct{}{}
	if d.cur == c {
		d.cur = nil
	}
	<-d.lock
	d.shut(c)
}

// shut closes a session and waits, bounded, for its cleanup.
func (d *Device) shut(c *conn) error {
	_ = c.cs.Close()
	// Closing a stdio session already waits stdin-close, SIGTERM and SIGKILL
	// out; the rest is margin for the waiter goroutine.
	select {
	case <-c.dead:
		return nil
	case <-time.After(3*d.cfg.stopTimeout + 5*time.Second):
		return fmt.Errorf("mcp %s: session did not shut down", d.name)
	}
}

// transportFailed reports whether err means the session is unusable: not the
// caller's cancellation, and not an error response the server sent.
func transportFailed(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var we *jsonrpc.Error
	return !errors.As(err, &we)
}

// Tools lists the server's tools, following pagination, and maps each to a
// ToolDef. It clears ToolsChanged.
func (d *Device) Tools(ctx context.Context) ([]bus.ToolDef, error) {
	ctx, cancel := d.setupContext(ctx)
	defer cancel()
	c, err := d.session(ctx)
	if err != nil {
		return nil, err
	}
	d.changed.Store(false)
	var defs []bus.ToolDef
	seen := map[string]bool{}
	for t, err := range c.cs.Tools(ctx, nil) {
		if err != nil {
			if transportFailed(ctx, err) {
				d.drop(c)
			}
			return nil, fmt.Errorf("mcp %s: list tools: %w", d.name, err)
		}
		def, err := d.toolDef(t)
		if err != nil {
			return nil, err
		}
		seen[t.Name] = true
		defs = append(defs, def)
	}
	for tool := range d.cfg.effects {
		if !seen[tool] {
			return nil, fmt.Errorf("mcp %s: WithEffect names %q, which the server does not have", d.name, tool)
		}
	}
	for tool := range d.cfg.reportUnknown {
		if !seen[tool] {
			return nil, fmt.Errorf("mcp %s: WithReportUnknown names %q, which the server does not have", d.name, tool)
		}
	}
	return defs, nil
}

func (d *Device) toolDef(t *sdk.Tool) (bus.ToolDef, error) {
	schema := json.RawMessage(`{"type":"object"}`)
	if t.InputSchema != nil {
		b, err := json.Marshal(t.InputSchema)
		if err != nil {
			return bus.ToolDef{}, fmt.Errorf("mcp %s: tool %q: schema: %w", d.name, t.Name, err)
		}
		if s := bytes.TrimSpace(b); len(s) > 0 && string(s) != "null" {
			schema = s
		}
	}
	effect := effectOf(t.Annotations)
	if e, ok := d.cfg.effects[t.Name]; ok {
		if !e.Valid() {
			return bus.ToolDef{}, fmt.Errorf("mcp %s: WithEffect(%q): unknown effect %d", d.name, t.Name, int(e))
		}
		effect = e
	}
	desc := t.Description
	if desc == "" && t.Annotations != nil {
		desc = t.Annotations.Title
	}
	return bus.ToolDef{
		Name:          t.Name,
		Description:   desc,
		Schema:        schema,
		Effect:        effect,
		ReportUnknown: d.cfg.reportUnknown[t.Name],
	}, nil
}

// effectOf maps annotations to an effect. destructiveHint is not consulted:
// it can only say a tool is less safe than SideEffect already assumes.
func effectOf(a *sdk.ToolAnnotations) bus.Effect {
	switch {
	case a == nil:
		return bus.SideEffect
	case a.ReadOnlyHint:
		return bus.ReadOnly
	case a.IdempotentHint:
		return bus.Idempotent
	}
	return bus.SideEffect
}

// Call calls one tool. See the package documentation for how results, errors
// and a lost session are handled.
func (d *Device) Call(ctx context.Context, call bus.DeviceCall) (task.ToolResult, error) {
	args := bytes.TrimSpace(call.Args)
	if len(args) == 0 || string(args) == "null" {
		args = []byte("{}")
	}
	if !json.Valid(args) {
		return task.ToolResult{}, fmt.Errorf("mcp %s: %s: arguments are not valid JSON", d.name, call.Tool)
	}
	for attempt := 1; ; attempt++ {
		c, err := d.session(ctx)
		if err != nil {
			return task.ToolResult{}, err
		}
		// Fresh params each attempt: the SDK adds its own _meta entries.
		params := &sdk.CallToolParams{Name: call.Tool, Arguments: json.RawMessage(args)}
		if call.Key != "" {
			params.Meta = sdk.Meta{KeyMeta: call.Key}
		}
		res, err := c.cs.CallTool(ctx, params)
		if err == nil {
			return result(res), nil
		}
		if !transportFailed(ctx, err) {
			return task.ToolResult{}, fmt.Errorf("mcp %s: %s: %w", d.name, call.Tool, err)
		}
		d.drop(c)
		// The server answered 404 for a session it no longer has: the call
		// was refused, not run, so sending it on a new session is safe.
		if attempt == 1 && errors.Is(err, sdk.ErrSessionMissing) {
			continue
		}
		return task.ToolResult{}, fmt.Errorf("mcp %s: %s: session lost, outcome unknown: %w", d.name, call.Tool, err)
	}
}

func result(r *sdk.CallToolResult) task.ToolResult {
	var parts []string
	text := false
	for _, c := range r.Content {
		switch c := c.(type) {
		case *sdk.TextContent:
			parts, text = append(parts, c.Text), true
		case *sdk.ImageContent:
			parts = append(parts, fmt.Sprintf("[image %s %d bytes]", mimeOr(c.MIMEType), len(c.Data)))
		case *sdk.AudioContent:
			parts = append(parts, fmt.Sprintf("[audio %s %d bytes]", mimeOr(c.MIMEType), len(c.Data)))
		case *sdk.ResourceLink:
			parts = append(parts, "[resource link "+c.URI+"]")
		case *sdk.EmbeddedResource:
			switch r := c.Resource; {
			case r == nil:
				parts = append(parts, "[resource]")
			case r.Text != "" || r.Blob == nil:
				parts, text = append(parts, r.Text), true
			default:
				parts = append(parts, fmt.Sprintf("[resource %s %s %d bytes]", r.URI, mimeOr(r.MIMEType), len(r.Blob)))
			}
		default:
			parts = append(parts, fmt.Sprintf("[%s content]", contentType(c)))
		}
	}
	if !text && r.StructuredContent != nil {
		if b, err := json.Marshal(r.StructuredContent); err == nil {
			parts = append(parts, string(b))
		}
	}
	return task.ToolResult{Content: strings.Join(parts, "\n"), IsError: r.IsError}
}

func mimeOr(m string) string {
	if m == "" {
		return "application/octet-stream"
	}
	return m
}

func contentType(c sdk.Content) string {
	b, err := c.MarshalJSON()
	if err != nil {
		return "unknown"
	}
	var v struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(b, &v) != nil || v.Type == "" {
		return "unknown"
	}
	return v.Type
}

// Close closes the session and, for stdio, stops the server process. A device
// cannot be used after Close; calling Close again does nothing.
func (d *Device) Close() error {
	d.lock <- struct{}{}
	if d.closed {
		<-d.lock
		return nil
	}
	d.closed = true
	c := d.cur
	d.cur = nil
	<-d.lock
	var err error
	if c != nil {
		err = d.shut(c)
	}
	d.pending.Wait()
	return err
}
