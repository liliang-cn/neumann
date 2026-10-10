// Package sim holds deterministic stand-ins for the world outside the machine,
// for tests.
package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// Model is a scripted adapter. Fn decides each reply from the request, so
// concurrent tasks can share one script and stay deterministic per task.
type Model struct {
	Fn func(req model.Request) (model.Response, error)

	mu    sync.Mutex
	calls int
}

func (m *Model) Provider() string { return "sim" }

func (m *Model) Call(ctx context.Context, req model.Request) (model.Response, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.Response{}, err
	}
	return m.Fn(req)
}

// Stream is Call that also reports the reply's text in three pieces, so tests
// exercise the streaming path the machine takes by default.
func (m *Model) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	resp, err := m.Call(ctx, req)
	if err != nil {
		return resp, err
	}
	r := []rune(resp.Message.TextContent())
	for i, n := 0, len(r); i < 3 && n > 0; i++ {
		if piece := string(r[i*n/3 : (i+1)*n/3]); piece != "" {
			emit(model.Delta{Text: piece})
		}
	}
	return resp, nil
}

// Calls is how many times Call or Stream was invoked.
func (m *Model) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

var simUsage = task.Usage{PromptTokens: 10, CompletionTokens: 5}

// Reply is a final text answer.
func Reply(text string) model.Response {
	return model.Response{Message: task.Text(task.RoleAssistant, text), FinishReason: "stop", Usage: simUsage}
}

// CallTools is a turn calling each named tool once; call i gets args {"n":i}
// and provider id "p-i".
func CallTools(names ...string) model.Response {
	msg := task.Message{Role: task.RoleAssistant}
	for i, n := range names {
		msg.Parts = append(msg.Parts, task.Part{
			Kind:           task.PartToolCall,
			ProviderCallID: fmt.Sprintf("p-%d", i),
			Name:           n,
			Args:           json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		})
	}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: simUsage}
}

// FirstUser is the first user message's text; tests use it as a task's name.
func FirstUser(req model.Request) string {
	for _, m := range req.Messages {
		if m.Role == task.RoleUser {
			return m.TextContent()
		}
	}
	return ""
}

// UserTexts is every user message's text, in order.
func UserTexts(req model.Request) []string {
	var out []string
	for _, m := range req.Messages {
		if m.Role == task.RoleUser {
			out = append(out, m.TextContent())
		}
	}
	return out
}

// ToolRounds counts tool-result messages: the tool rounds completed so far.
func ToolRounds(req model.Request) int {
	n := 0
	for _, m := range req.Messages {
		if m.Role == task.RoleTool {
			n++
		}
	}
	return n
}
