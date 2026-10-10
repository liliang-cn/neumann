// Command mcp attaches an MCP server to a machine as a bus device and runs a
// task that uses its tools. The server is this same binary started with
// "serve": a tiny notes server speaking MCP over stdio, with one tool that
// changes state (add) and one annotated read-only (list).
//
//	go run ./examples/mcp
//
// Without configuration the model is scripted (sim), so it runs offline. With
// OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL set it asks a real
// OpenAI-compatible model instead.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/mcp"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		serve()
		return
	}
	self, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	notes := mcp.Stdio("notes", self, []string{"serve"}, mcp.WithStderr(os.Stderr))

	ctx := context.Background()
	// Connect now, so a server that cannot start fails here and not on the
	// first tool call.
	if err := notes.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defs, err := notes.Tools(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range defs {
		fmt.Printf("tool %-18s effect=%-10s %s\n", bus.FullName(notes.Name(), d.Name), d.Effect, d.Description)
	}

	m, err := neumann.New(neumann.Config{
		Model:   adapter(),
		Devices: []bus.Device{notes},
		System:  "You keep the user's notes with the notes tools. Answer in one sentence.",
		Logf:    log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer m.Stop(ctx) // closes the device, which stops the server process

	id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: "Note that I need to buy milk, then tell me what my notes say."})
	if err != nil {
		log.Fatal(err)
	}
	ectx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for ev := range m.Events(ectx, 0) {
		if ev.TaskID != id {
			continue
		}
		fmt.Printf("%-12s %s\n", ev.Kind, ev.Data)
		if ev.Terminal() {
			break
		}
	}
	t, err := m.Load(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%s: %s\n", t.State, t.LastAnswer())
}

// adapter is a real model when one is configured, otherwise a script that
// adds a note, lists the notes and answers with what the list returned.
func adapter() model.Adapter {
	base, key, name := os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL")
	if base != "" && key != "" && name != "" {
		return openai.New(base, key, name)
	}
	call := func(tool, args string) model.Response {
		return model.Response{FinishReason: "tool_calls", Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
			{Kind: task.PartToolCall, ProviderCallID: "p-" + tool, Name: tool, Args: json.RawMessage(args)},
		}}}
	}
	return &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		switch sim.ToolRounds(req) {
		case 0:
			return call("notes_add", `{"text":"buy milk"}`), nil
		case 1:
			return call("notes_list", `{}`), nil
		}
		last := req.Messages[len(req.Messages)-1]
		for _, p := range last.Parts {
			if p.Kind == task.PartToolResult {
				return sim.Reply("Your notes: " + p.Text), nil
			}
		}
		return sim.Reply("I could not read your notes."), nil
	}}
}

// serve is the notes server, on stdin and stdout.
func serve() {
	var (
		mu    sync.Mutex
		notes []string
		seen  = map[string]bool{} // idempotency keys already applied
	)
	s := sdk.NewServer(&sdk.Implementation{Name: "notes", Version: "v0"}, nil)
	s.AddTool(&sdk.Tool{
		Name:        "add",
		Description: "Add a note.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
	}, func(_ context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		var in struct{ Text string }
		if err := json.Unmarshal(req.Params.Arguments, &in); err != nil || in.Text == "" {
			return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: "text is required"}}}, nil
		}
		mu.Lock()
		defer mu.Unlock()
		// A repeat of a call already applied is answered, not applied again.
		if key, _ := req.Params.Meta[mcp.KeyMeta].(string); key != "" {
			if seen[key] {
				return text("already added"), nil
			}
			seen[key] = true
		}
		notes = append(notes, in.Text)
		return text(fmt.Sprintf("added note %d", len(notes))), nil
	})
	s.AddTool(&sdk.Tool{
		Name:        "list",
		Description: "List the notes.",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true},
	}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(notes) == 0 {
			return text("no notes"), nil
		}
		return text(strings.Join(notes, "; ")), nil
	})
	if err := s.Run(context.Background(), &sdk.StdioTransport{}); err != nil {
		log.Printf("notes server: %v", err)
	}
}

func text(s string) *sdk.CallToolResult {
	return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: s}}}
}
