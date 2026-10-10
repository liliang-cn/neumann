package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/mcp"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

func memDevice(t *testing.T, rec *seen, pageSize int, opts ...mcp.Option) (*mcp.Device, *memDial) {
	t.Helper()
	md := &memDial{srv: newServer(rec, pageSize)}
	d := mcp.New("srv", md.dial, opts...)
	t.Cleanup(func() { _ = d.Close() })
	return d, md
}

func toolMap(t *testing.T, d *mcp.Device) map[string]bus.ToolDef {
	t.Helper()
	defs, err := d.Tools(bg)
	if err != nil {
		t.Fatalf("Tools: %v", err)
	}
	m := map[string]bus.ToolDef{}
	for _, def := range defs {
		if _, dup := m[def.Name]; dup {
			t.Fatalf("tool %q listed twice", def.Name)
		}
		m[def.Name] = def
	}
	return m
}

func call(t *testing.T, d *mcp.Device, tool, args, key string) task.ToolResult {
	t.Helper()
	res, err := d.Call(bg, bus.DeviceCall{Tool: tool, Args: json.RawMessage(args), Key: key})
	if err != nil {
		t.Fatalf("Call %s: %v", tool, err)
	}
	return res
}

func TestToolsMapAnnotationsToEffectsAcrossPages(t *testing.T) {
	d, _ := memDevice(t, nil, 2) // two tools a page: listing must follow the cursor
	tools := toolMap(t, d)
	for _, a := range annotated {
		def, ok := tools[a.name]
		if !ok {
			t.Fatalf("tool %q missing (pagination not followed?): have %d tools", a.name, len(tools))
		}
		if def.Effect != a.effect {
			t.Errorf("%s: effect %v, want %v", a.name, def.Effect, a.effect)
		}
		if def.Description != "tool "+a.name {
			t.Errorf("%s: description %q", a.name, def.Description)
		}
		if def.ReportUnknown {
			t.Errorf("%s: ReportUnknown set without being asked", a.name)
		}
	}
	echo := tools["echo"]
	var schema map[string]any
	if err := json.Unmarshal(echo.Schema, &schema); err != nil || schema["type"] != "object" || schema["properties"] == nil {
		t.Fatalf("echo schema %s (%v)", echo.Schema, err)
	}
}

func TestOperatorOverridesEffectAndReportUnknown(t *testing.T) {
	d, _ := memDevice(t, nil, 0,
		mcp.WithEffect("plain", bus.ReadOnly),
		mcp.WithEffect("read", bus.SideEffect),
		mcp.WithReportUnknown("destructive"),
	)
	tools := toolMap(t, d)
	if e := tools["plain"].Effect; e != bus.ReadOnly {
		t.Errorf("plain overridden to %v", e)
	}
	if e := tools["read"].Effect; e != bus.SideEffect {
		t.Errorf("read overridden to %v", e)
	}
	if !tools["destructive"].ReportUnknown || tools["plain"].ReportUnknown {
		t.Errorf("ReportUnknown: destructive=%v plain=%v", tools["destructive"].ReportUnknown, tools["plain"].ReportUnknown)
	}
}

func TestOverrideNamingAnAbsentToolFails(t *testing.T) {
	d, _ := memDevice(t, nil, 0, mcp.WithEffect("no_such_tool", bus.ReadOnly))
	if _, err := d.Tools(bg); err == nil || !contains(err.Error(), "no_such_tool") {
		t.Fatalf("got %v", err)
	}
	d2, _ := memDevice(t, nil, 0, mcp.WithEffect("plain", bus.Effect(42)))
	if _, err := d2.Tools(bg); err == nil {
		t.Fatal("an invalid effect was accepted")
	}
}

func TestCallPassesArgsAndKeyInMeta(t *testing.T) {
	rec := &seen{}
	d, _ := memDevice(t, rec, 0)
	res := call(t, d, "echo", `{"msg":"hi"}`, "task-1/call-2/1")
	if res.IsError || !contains(res.Content, `"msg":"hi"`) {
		t.Fatalf("result %+v", res)
	}
	key, args := rec.last()
	if key != "task-1/call-2/1" {
		t.Fatalf("server saw key %q", key)
	}
	if !contains(args, `"msg":"hi"`) {
		t.Fatalf("server saw args %s", args)
	}
}

func TestCallWithEmptyArgsSendsAnObject(t *testing.T) {
	rec := &seen{}
	d, _ := memDevice(t, rec, 0)
	for _, a := range []string{"", "null", "  "} {
		call(t, d, "echo", a, "")
		key, args := rec.last()
		if args != "{}" {
			t.Errorf("args %q arrived as %q", a, args)
		}
		if key != "" {
			t.Errorf("an empty key arrived as %q", key)
		}
	}
	if _, err := d.Call(bg, bus.DeviceCall{Tool: "echo", Args: json.RawMessage(`{bad`)}); err == nil {
		t.Fatal("invalid JSON args were sent")
	}
}

func TestToolErrorIsAResultNotAGoError(t *testing.T) {
	d, _ := memDevice(t, nil, 0)
	res := call(t, d, "fail", `{}`, "")
	if !res.IsError || res.Content != "boom" {
		t.Fatalf("got %+v", res)
	}
}

func TestNonTextContentIsSummarised(t *testing.T) {
	d, _ := memDevice(t, nil, 0)
	if res := call(t, d, "image", `{}`, ""); res.Content != "[image image/png 1234 bytes]" {
		t.Fatalf("image: %q", res.Content)
	}
	want := "here it is\n[image image/jpeg 10 bytes]\n[audio audio/wav 7 bytes]\n[resource link file:///a.txt]\n" +
		"[resource file:///b.bin application/octet-stream 3 bytes]\ninline text"
	if res := call(t, d, "mixed", `{}`, ""); res.Content != want {
		t.Fatalf("mixed:\n%s\nwant:\n%s", res.Content, want)
	}
}

func TestStructuredContentOnlyWhenThereIsNoText(t *testing.T) {
	d, _ := memDevice(t, nil, 0)
	if res := call(t, d, "structured", `{}`, ""); res.Content != `{"a":1}` {
		t.Fatalf("structured: %q", res.Content)
	}
	if res := call(t, d, "structured_text", `{}`, ""); res.Content != "summary" {
		t.Fatalf("structured_text: %q", res.Content)
	}
}

// A protocol error (unknown tool) is a Go error, and the session survives it.
func TestProtocolErrorKeepsTheSession(t *testing.T) {
	d, md := memDevice(t, nil, 0)
	if _, err := d.Call(bg, bus.DeviceCall{Tool: "no_such_tool"}); err == nil {
		t.Fatal("unknown tool returned no error")
	}
	call(t, d, "plain", `{}`, "")
	if n := md.n(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

func TestConnectIsLazyOrExplicit(t *testing.T) {
	d, md := memDevice(t, nil, 0)
	if md.n() != 0 {
		t.Fatal("connected before first use")
	}
	if err := d.Connect(bg); err != nil {
		t.Fatal(err)
	}
	if err := d.Connect(bg); err != nil {
		t.Fatal(err)
	}
	toolMap(t, d)
	call(t, d, "plain", `{}`, "")
	if n := md.n(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

func TestCancelDuringCallReturnsPromptly(t *testing.T) {
	rec := &seen{entered: make(chan struct{}, 1)}
	d, md := memDevice(t, rec, 0)
	ctx, cancel := context.WithCancel(bg)
	go func() { <-rec.entered; cancel() }()
	start := time.Now()
	_, err := d.Call(ctx, bus.DeviceCall{Tool: "hang"})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %v", el)
	}
	// The server is told; the session stays usable.
	deadline := time.Now().Add(2 * time.Second)
	for {
		rec.mu.Lock()
		n := rec.cancelled
		rec.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never saw the cancellation")
		}
		time.Sleep(5 * time.Millisecond)
	}
	call(t, d, "plain", `{}`, "")
	if n := md.n(); n != 1 {
		t.Fatalf("%d connections after a cancel, want 1", n)
	}
}

func TestToolsRespectsContext(t *testing.T) {
	// A transport whose peer never answers: initialize cannot complete.
	d := mcp.New("deaf", func(context.Context) (sdk.Transport, error) {
		ct, _ := sdk.NewInMemoryTransports()
		return ct, nil
	})
	defer d.Close()
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := d.Tools(ctx); err == nil {
		t.Fatal("Tools succeeded against a server that never answers")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %v", el)
	}
	// Without a deadline of its own, the connect timeout bounds it.
	d2 := mcp.New("deaf", func(context.Context) (sdk.Transport, error) {
		ct, _ := sdk.NewInMemoryTransports()
		return ct, nil
	}, mcp.WithConnectTimeout(100*time.Millisecond))
	defer d2.Close()
	start = time.Now()
	if _, err := d2.Tools(bg); err == nil {
		t.Fatal("Tools succeeded against a server that never answers")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %v", el)
	}
}

func TestClosedDeviceRefuses(t *testing.T) {
	d, _ := memDevice(t, nil, 0)
	call(t, d, "plain", `{}`, "")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal("second Close:", err)
	}
	if _, err := d.Call(bg, bus.DeviceCall{Tool: "plain"}); !errors.Is(err, mcp.ErrClosed) {
		t.Fatalf("Call after Close: %v", err)
	}
	if _, err := d.Tools(bg); !errors.Is(err, mcp.ErrClosed) {
		t.Fatalf("Tools after Close: %v", err)
	}
}

func TestToolListChangeIsRecorded(t *testing.T) {
	md := &memDial{srv: newServer(nil, 0)}
	d := mcp.New("srv", md.dial)
	defer d.Close()
	toolMap(t, d)
	if d.ToolsChanged() {
		t.Fatal("changed before any change")
	}
	md.srv.AddTool(&sdk.Tool{Name: "late", InputSchema: objectSchema}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return text("late"), nil
	})
	deadline := time.Now().Add(3 * time.Second)
	for !d.ToolsChanged() {
		if time.Now().After(deadline) {
			t.Fatal("tools/list_changed never recorded")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := toolMap(t, d)["late"]; !ok {
		t.Fatal("relisting did not show the new tool")
	}
	if d.ToolsChanged() {
		t.Fatal("still marked changed after relisting")
	}
}

// --- stdio ---

func helperDevice(t *testing.T, mode string, opts ...mcp.Option) *mcp.Device {
	t.Helper()
	opts = append([]mcp.Option{mcp.WithEnv(helperEnv + "=" + mode), mcp.WithStopTimeout(200 * time.Millisecond)}, opts...)
	d := mcp.Stdio("helper", os.Args[0], []string{"-test.run=^$"}, opts...)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func pidOf(t *testing.T, d *mcp.Device) int {
	t.Helper()
	pid, err := strconv.Atoi(call(t, d, "pid", `{}`, "").Content)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// gone reports whether pid no longer exists at all: not running and reaped
// (a zombie still answers signal 0).
func gone(pid int) bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }

func waitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !gone(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("process %d still exists (running or a zombie)", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStdioRoundTrip(t *testing.T) {
	d := helperDevice(t, "serve")
	tools := toolMap(t, d)
	if tools["read"].Effect != bus.ReadOnly || tools["idem"].Effect != bus.Idempotent || tools["plain"].Effect != bus.SideEffect {
		t.Fatalf("effects over stdio: %v %v %v", tools["read"].Effect, tools["idem"].Effect, tools["plain"].Effect)
	}
	if res := call(t, d, "echo", `{"msg":"x"}`, "k/1/1"); !contains(res.Content, "key=k/1/1") || !contains(res.Content, `"msg":"x"`) {
		t.Fatalf("echo: %q", res.Content)
	}
}

func TestStdioKilledMidCallErrorsThenReconnects(t *testing.T) {
	d := helperDevice(t, "serve")
	pid1 := pidOf(t, d)
	errc := make(chan error, 1)
	go func() {
		_, err := d.Call(bg, bus.DeviceCall{Tool: "hang", Key: "t/c/1"})
		errc <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the call reach the server
	if err := syscall.Kill(pid1, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("a call whose server died returned no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight call never returned")
	}
	waitGone(t, pid1) // the dead child is reaped, not left a zombie
	pid2 := pidOf(t, d)
	if pid2 == pid1 {
		t.Fatal("no new server process")
	}
}

// The server exiting between calls is noticed; the next call reconnects.
func TestStdioServerExitBetweenCallsReconnects(t *testing.T) {
	d := helperDevice(t, "serve")
	pid1 := pidOf(t, d)
	if err := syscall.Kill(pid1, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitGone(t, pid1)
	if pid2 := pidOf(t, d); pid2 == pid1 {
		t.Fatal("no new server process")
	}
}

func TestStdioCloseLeavesNoChild(t *testing.T) {
	d := helperDevice(t, "serve")
	pid := pidOf(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if !gone(pid) {
		t.Fatalf("process %d still exists after Close", pid)
	}
}

func TestStdioCloseKillsAStubbornChild(t *testing.T) {
	d := helperDevice(t, "stubborn")
	pid := pidOf(t, d)
	start := time.Now()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("Close took %v", el)
	}
	if !gone(pid) {
		t.Fatalf("process %d still exists after Close", pid)
	}
}

// A server started through a wrapper leaves nothing behind: what is left of
// its process group is killed once it exits.
func TestStdioCloseStopsGrandchildren(t *testing.T) {
	d := helperDevice(t, "spawner")
	child, err := strconv.Atoi(call(t, d, "child", `{}`, "").Content)
	if err != nil {
		t.Fatal(err)
	}
	if gone(child) {
		t.Fatal("grandchild not running")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	waitGone(t, child)
}

func TestStdioMuteServerDoesNotHangTools(t *testing.T) {
	d := helperDevice(t, "mute", mcp.WithConnectTimeout(200*time.Millisecond))
	start := time.Now()
	if _, err := d.Tools(bg); err == nil {
		t.Fatal("Tools succeeded against a mute server")
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("took %v", el)
	}
}

func TestStdioBadCommand(t *testing.T) {
	d := mcp.Stdio("nope", "/no/such/binary", nil)
	defer d.Close()
	if _, err := d.Tools(bg); err == nil {
		t.Fatal("started a missing binary")
	}
}

// --- streamable HTTP ---

// swapHandler serves through whichever handler is current, so a test can
// replace the server and lose its sessions.
type swapHandler struct{ cur atomic.Pointer[http.Handler] }

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	(*s.cur.Load()).ServeHTTP(w, r)
}

func (s *swapHandler) fresh(rec *seen, header *atomic.Value) {
	srv := newServer(rec, 0)
	h := sdk.NewStreamableHTTPHandler(func(r *http.Request) *sdk.Server {
		header.Store(r.Header.Get("X-Test-Token"))
		return srv
	}, nil)
	var hh http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header.Store(r.Header.Get("X-Test-Token"))
		h.ServeHTTP(w, r)
	})
	s.cur.Store(&hh)
}

func TestHTTPRoundTripHeadersAndServerRestart(t *testing.T) {
	rec := &seen{}
	var header atomic.Value
	sw := &swapHandler{}
	sw.fresh(rec, &header)
	ts := httptest.NewServer(sw)
	defer ts.Close()

	d := mcp.HTTP("web", ts.URL, mcp.WithHeader("X-Test-Token", "secret"))
	defer d.Close()
	tools := toolMap(t, d)
	if tools["read"].Effect != bus.ReadOnly || tools["idem"].Effect != bus.Idempotent {
		t.Fatal("effects over HTTP")
	}
	if res := call(t, d, "echo", `{"msg":"web"}`, "h/1/1"); !contains(res.Content, "key=h/1/1") {
		t.Fatalf("echo: %q", res.Content)
	}
	if v, _ := header.Load().(string); v != "secret" {
		t.Fatalf("header arrived as %q", v)
	}
	// The server restarts and forgets every session: the next call still works.
	sw.fresh(rec, &header)
	if res := call(t, d, "echo", `{"msg":"again"}`, "h/2/1"); !contains(res.Content, "key=h/2/1") {
		t.Fatalf("echo after restart: %q", res.Content)
	}
}

// --- on a real bus ---

func TestEndToEndThroughTheBus(t *testing.T) {
	rec := &seen{entered: make(chan struct{}, 1)}
	md := &memDial{srv: newServer(rec, 3)}
	dev := mcp.New("srv", md.dial, mcp.WithEffect("plain", bus.Idempotent))
	b, err := bus.New(bg, 2, dev)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	names := map[string]bool{}
	for _, s := range b.Specs(nil, nil) {
		names[s.Name] = true
	}
	for _, want := range []string{"srv_echo", "srv_read", "srv_plain", "srv_hang"} {
		if !names[want] {
			t.Fatalf("bus is missing %s: %v", want, names)
		}
	}
	if e := b.Effect("srv_read"); e != bus.ReadOnly {
		t.Fatalf("srv_read effect %v", e)
	}
	if e := b.Effect("srv_plain"); e != bus.Idempotent {
		t.Fatalf("srv_plain effect %v", e)
	}
	if e := b.Effect("srv_destructive"); e != bus.SideEffect {
		t.Fatalf("srv_destructive effect %v", e)
	}

	res, how := b.Run(bg, "srv_echo", json.RawMessage(`{"msg":"bus"}`), "task/call/1", nil, nil, nil)
	if how != bus.Returned || res.IsError || !contains(res.Content, `"msg":"bus"`) {
		t.Fatalf("got %+v, %v", res, how)
	}
	if key, _ := rec.last(); key != "task/call/1" {
		t.Fatalf("server saw key %q", key)
	}
	if res := b.Call(bg, "srv_fail", nil, "", nil, nil); !res.IsError || res.Content != "boom" {
		t.Fatalf("fail through the bus: %+v", res)
	}
	// A protocol error becomes an error result.
	if res := b.Call(bg, "srv_no_such", nil, "", nil, nil); !res.IsError {
		t.Fatalf("unknown tool: %+v", res)
	}

	// The bus's cancellation reaches the server through the device.
	ctx, cancel := context.WithCancel(bg)
	go func() { <-rec.entered; cancel() }()
	if _, how := b.Run(ctx, "srv_hang", nil, "task/call/2", nil, nil, nil); how != bus.Abandoned {
		t.Fatalf("cancelled call: %v", how)
	}
}
