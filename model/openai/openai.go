// Package openai adapts OpenAI-compatible /chat/completions endpoints
// (OpenAI, DeepSeek, Ollama, and gateways that front other models) to the
// machine's provider-neutral messages.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/internal/httperr"
	"github.com/liliang-cn/neumann/task"
)

type Adapter struct {
	BaseURL   string // up to and including the version, e.g. https://api.openai.com/v1
	APIKey    string
	Model     string
	MaxTokens int // 0: let the provider decide
	// What the model behind the endpoint can take (model.Caps), read before
	// a task is moved onto this adapter's group. ContextWindow 0 is unknown:
	// a gateway fronts many models, so there is no default. NoTools marks a
	// model that cannot call tools. MaxTools 0 is DefaultMaxTools, the
	// OpenAI API's limit; a negative value is no limit.
	ContextWindow int
	NoTools       bool
	MaxTools      int
	HTTP          *http.Client
	// Now is the clock rate-limit resets are read against; nil: time.Now.
	Now func() time.Time
}

var _ model.Adapter = (*Adapter)(nil)

func New(baseURL, apiKey, modelName string) *Adapter {
	return &Adapter{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Model:   modelName,
		HTTP:    &http.Client{Transport: NewTransport(HTTP2), Timeout: 5 * time.Minute},
	}
}

// Provider names this endpoint and model. Tool calls carry it, and their
// provider-private fields are replayed only to the same Provider.
func (a *Adapter) Provider() string { return "openai:" + a.Model }

// MaxOut is the output cap sent when a request carries none: MaxTokens, or 0
// when the provider decides (model.OutputCap).
func (a *Adapter) MaxOut() int { return max(a.MaxTokens, 0) }

// DefaultMaxTools is the most tools one chat-completions request may carry.
const DefaultMaxTools = 128

// Caps reports ContextWindow, the output cap, NoTools and MaxTools
// (model.CapsReporter).
func (a *Adapter) Caps() model.Caps {
	n := a.MaxTools
	switch {
	case n == 0:
		n = DefaultMaxTools
	case n < 0:
		n = 0
	}
	return model.Caps{Window: max(a.ContextWindow, 0), MaxOut: a.MaxOut(), Tools: !a.NoTools, MaxTools: n}
}

// maxTokens is the request's own cap when it has one, else MaxTokens; 0 sends
// no max_tokens at all.
func (a *Adapter) maxTokens(req model.Request) int {
	if req.MaxOut > 0 {
		return req.MaxOut
	}
	return a.MaxTokens
}

type wireMsg struct {
	Role       string            `json:"role"`
	Content    *string           `json:"content"`
	ToolCalls  []json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
}

type wireCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type wireResp struct {
	Choices []struct {
		Message struct {
			Content   *string           `json:"content"`
			ToolCalls []json.RawMessage `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage wireUsage `json:"usage"`
}

type wireUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	CacheHitTokens   int `json:"prompt_cache_hit_tokens"` // DeepSeek's spelling of cached_tokens
	PromptDetails    struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

func (u wireUsage) usage() task.Usage {
	return task.Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		ReasoningTokens:  u.CompletionDetails.ReasoningTokens,
		CachedTokens:     max(u.PromptDetails.CachedTokens, u.CacheHitTokens),
	}
}

func (a *Adapter) Call(ctx context.Context, req model.Request) (model.Response, error) {
	resp, err := a.post(ctx, req, false)
	if err != nil {
		return model.Response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.Response{}, a.ioError(ctx, "read response", err, true, resp.Header)
	}
	out, err := a.parse(data)
	if err != nil {
		return model.Response{}, err
	}
	out.Rate = httperr.RateInfo(resp.Header, a.now())
	return out, nil
}

// post sends a chat-completions request and returns the response once its
// status is known to be 2xx. Every failure is a classified *model.Error.
func (a *Adapter) post(ctx context.Context, req model.Request, stream bool) (*http.Response, error) {
	msgs, err := a.messages(req)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"model": a.Model, "messages": msgs}
	if len(req.Tools) > 0 {
		body["tools"] = tools(req.Tools)
	}
	if n := a.maxTokens(req); n > 0 {
		body["max_tokens"] = n
	}
	if stream {
		body["stream"] = true
		body["stream_options"] = map[string]bool{"include_usage": true}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if stream {
		hreq.Header.Set("Accept", "text/event-stream")
	}
	if a.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+a.APIKey)
	}
	resp, err := a.HTTP.Do(hreq)
	if err != nil {
		return nil, a.ioError(ctx, "send request", err, false, nil)
	}
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, a.statusError(resp.StatusCode, resp.Header, data)
	}
	return resp, nil
}

func (a *Adapter) parse(data []byte) (model.Response, error) {
	var w wireResp
	if err := json.Unmarshal(data, &w); err != nil {
		return model.Response{}, a.lostError("decode response: "+snippet(data), err)
	}
	if len(w.Choices) == 0 {
		// Some gateways answer 200 with no choices when their upstream
		// failed; the provider produced nothing to retry against.
		return model.Response{}, &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: "response has no choices: " + a.redact(snippet(data))}
	}
	ch := w.Choices[0]
	text := ""
	if ch.Message.Content != nil {
		text = *ch.Message.Content
	}
	return a.response(text, ch.Message.ToolCalls, ch.FinishReason, w.Usage.usage())
}

// response assembles the neutral turn from the provider's text, its complete
// tool-call objects (kept whole as Opaque for replay) and the usage.
func (a *Adapter) response(text string, calls []json.RawMessage, finish string, u task.Usage) (model.Response, error) {
	msg := task.Message{Role: task.RoleAssistant}
	if text != "" {
		msg.Parts = append(msg.Parts, task.Part{Kind: task.PartText, Text: text})
	}
	for _, rc := range calls {
		var c wireCall
		if err := json.Unmarshal(rc, &c); err != nil {
			return model.Response{}, a.lostError("decode tool call", err)
		}
		msg.Parts = append(msg.Parts, task.Part{
			Kind:           task.PartToolCall,
			ProviderCallID: c.ID,
			Name:           c.Function.Name,
			Args:           json.RawMessage(c.Function.Arguments),
			Provider:       a.Provider(),
			Opaque:         bytes.Clone(rc),
		})
	}
	if len(msg.Parts) == 0 && finish != "length" {
		return model.Response{}, &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: "response has neither text nor tool calls"}
	}
	return model.Response{Message: msg, FinishReason: finish, Usage: u}, nil
}

// messages renders neutral messages in the chat-completions shape. A tool
// call this same provider produced is replayed byte-for-byte from Opaque,
// because gateways put fields there (Gemini's thought signatures, for one)
// that must come back; any other call is rendered under the machine's id.
func (a *Adapter) messages(req model.Request) ([]wireMsg, error) {
	var out []wireMsg
	if req.System != "" {
		out = append(out, wireMsg{Role: "system", Content: ptr(req.System)})
	}
	ids := map[string]string{} // machine CallID -> id sent on the wire
	for _, m := range req.Messages {
		switch m.Role {
		case task.RoleSystem, task.RoleUser:
			out = append(out, wireMsg{Role: string(m.Role), Content: ptr(m.TextContent())})
		case task.RoleAssistant:
			w := wireMsg{Role: "assistant"}
			if t := m.TextContent(); t != "" {
				w.Content = ptr(t)
			}
			for _, p := range m.ToolCalls() {
				raw, id, err := a.toolCall(p)
				if err != nil {
					return nil, err
				}
				ids[p.CallID] = id
				w.ToolCalls = append(w.ToolCalls, raw)
			}
			if w.Content == nil && len(w.ToolCalls) == 0 {
				w.Content = ptr("")
			}
			out = append(out, w)
		case task.RoleTool:
			for _, p := range m.Parts {
				if p.Kind != task.PartToolResult {
					continue
				}
				id := ids[p.CallID]
				if id == "" {
					id = p.CallID
				}
				out = append(out, wireMsg{Role: "tool", ToolCallID: id, Content: ptr(p.Text)})
			}
		}
	}
	return out, nil
}

func (a *Adapter) toolCall(p task.Part) (json.RawMessage, string, error) {
	if p.Provider == a.Provider() && len(p.Opaque) > 0 && p.ProviderCallID != "" {
		return p.Opaque, p.ProviderCallID, nil
	}
	args := string(p.Args)
	if strings.TrimSpace(args) == "" {
		args = "{}"
	}
	var c wireCall
	c.ID, c.Type = p.CallID, "function"
	c.Function.Name, c.Function.Arguments = p.Name, args
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, "", fmt.Errorf("openai: encode tool call %s: %w", p.CallID, err)
	}
	return raw, c.ID, nil
}

func tools(specs []task.ToolSpec) []map[string]any {
	out := make([]map[string]any, 0, len(specs))
	for _, s := range specs {
		params := json.RawMessage(s.Schema)
		if len(params) == 0 {
			params = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out = append(out, map[string]any{
			"type":     "function",
			"function": map[string]any{"name": s.Name, "description": s.Description, "parameters": params},
		})
	}
	return out
}

func ptr(s string) *string { return &s }

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}
