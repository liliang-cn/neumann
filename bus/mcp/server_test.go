package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/mcp"
)

// helperEnv makes the test binary act as a stdio MCP server instead of
// running tests: the os.Args[0] helper pattern.
const helperEnv = "NEUMANN_MCP_TEST_HELPER"

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperEnv); mode != "" {
		runHelper(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runHelper(mode string) {
	switch mode {
	case "serve":
		_ = newServer(nil, 0).Run(context.Background(), &sdk.StdioTransport{})
	case "stubborn":
		// Ignores SIGTERM and stays alive after stdin closes: only a kill ends it.
		signal.Ignore(syscall.SIGTERM)
		_ = newServer(nil, 0).Run(context.Background(), &sdk.StdioTransport{})
		time.Sleep(time.Hour) // not select{}: the runtime would call that a deadlock and exit
	case "sleeper":
		time.Sleep(time.Hour)
	case "spawner":
		// Starts a grandchild in its own process group and reports its pid.
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), helperEnv+"=sleeper")
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		s := newServer(nil, 0)
		s.AddTool(&sdk.Tool{Name: "child", InputSchema: objectSchema}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
			return text(fmt.Sprint(cmd.Process.Pid)), nil
		})
		_ = s.Run(context.Background(), &sdk.StdioTransport{})
	case "mute":
		// Reads its input and never answers, so initialize never completes.
		buf := make([]byte, 4096)
		for {
			if _, err := os.Stdin.Read(buf); err != nil {
				time.Sleep(time.Hour) // not select{}: the runtime would call that a deadlock and exit
			}
		}
	}
}

// seen records what the server's tools observed.
type seen struct {
	mu        sync.Mutex
	keys      []string
	args      []string
	cancelled int
	entered   chan struct{}
}

func (s *seen) record(key, args string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, key)
	s.args = append(s.args, args)
}

func (s *seen) last() (key, args string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.keys) == 0 {
		return "", ""
	}
	return s.keys[len(s.keys)-1], s.args[len(s.args)-1]
}

func ptr[T any](v T) *T { return &v }

var objectSchema = json.RawMessage(`{"type":"object"}`)

// annotated is every annotation combination and the effect it must map to.
var annotated = []struct {
	name   string
	ann    *sdk.ToolAnnotations
	effect bus.Effect
}{
	{"plain", nil, bus.SideEffect},
	{"empty_annotations", &sdk.ToolAnnotations{}, bus.SideEffect},
	{"read", &sdk.ToolAnnotations{ReadOnlyHint: true}, bus.ReadOnly},
	{"read_idem", &sdk.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true}, bus.ReadOnly},
	{"read_destructive", &sdk.ToolAnnotations{ReadOnlyHint: true, DestructiveHint: ptr(true)}, bus.ReadOnly},
	{"idem", &sdk.ToolAnnotations{IdempotentHint: true}, bus.Idempotent},
	{"idem_destructive", &sdk.ToolAnnotations{IdempotentHint: true, DestructiveHint: ptr(true)}, bus.Idempotent},
	{"not_destructive", &sdk.ToolAnnotations{DestructiveHint: ptr(false)}, bus.SideEffect},
	{"destructive", &sdk.ToolAnnotations{DestructiveHint: ptr(true)}, bus.SideEffect},
}

func text(s string) *sdk.CallToolResult {
	return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: s}}}
}

// newServer builds the test server. pageSize > 0 forces paginated listing.
func newServer(rec *seen, pageSize int) *sdk.Server {
	s := sdk.NewServer(&sdk.Implementation{Name: "test", Version: "v0"}, &sdk.ServerOptions{PageSize: pageSize})
	add := func(name, desc string, ann *sdk.ToolAnnotations, h sdk.ToolHandler) {
		s.AddTool(&sdk.Tool{Name: name, Description: desc, Annotations: ann, InputSchema: objectSchema}, h)
	}
	for _, a := range annotated {
		add(a.name, "tool "+a.name, a.ann, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
			return text("ok"), nil
		})
	}
	s.AddTool(&sdk.Tool{
		Name:        "echo",
		Description: "Echo the arguments and the idempotency key.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`),
	}, func(_ context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		key, _ := req.Params.Meta[mcp.KeyMeta].(string)
		args := string(req.Params.Arguments)
		if rec != nil {
			rec.record(key, args)
		}
		return text(fmt.Sprintf("args=%s key=%s", args, key)), nil
	})
	add("fail", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "boom"}}, IsError: true}, nil
	})
	add("image", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.ImageContent{MIMEType: "image/png", Data: make([]byte, 1234)}}}, nil
	})
	add("mixed", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{
			&sdk.TextContent{Text: "here it is"},
			&sdk.ImageContent{MIMEType: "image/jpeg", Data: make([]byte, 10)},
			&sdk.AudioContent{MIMEType: "audio/wav", Data: make([]byte, 7)},
			&sdk.ResourceLink{URI: "file:///a.txt", Name: "a"},
			&sdk.EmbeddedResource{Resource: &sdk.ResourceContents{URI: "file:///b.bin", MIMEType: "application/octet-stream", Blob: make([]byte, 3)}},
			&sdk.EmbeddedResource{Resource: &sdk.ResourceContents{URI: "file:///c.txt", Text: "inline text"}},
		}}, nil
	})
	add("structured", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{StructuredContent: map[string]any{"a": 1}}, nil
	})
	add("structured_text", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "summary"}}, StructuredContent: map[string]any{"a": 1}}, nil
	})
	add("pid", "", nil, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return text(fmt.Sprint(os.Getpid())), nil
	})
	add("hang", "", nil, func(ctx context.Context, _ *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		if rec != nil && rec.entered != nil {
			rec.entered <- struct{}{}
		}
		<-ctx.Done()
		if rec != nil {
			rec.mu.Lock()
			rec.cancelled++
			rec.mu.Unlock()
		}
		return nil, ctx.Err()
	})
	return s
}

// memDial connects each new session to srv over an in-memory pipe and counts
// the connections made.
type memDial struct {
	srv   *sdk.Server
	mu    sync.Mutex
	count int
}

func (m *memDial) dial(ctx context.Context) (sdk.Transport, error) {
	ct, st := sdk.NewInMemoryTransports()
	if _, err := m.srv.Connect(ctx, st, nil); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
	return ct, nil
}

func (m *memDial) n() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
