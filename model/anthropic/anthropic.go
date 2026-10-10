// Package anthropic adapts Anthropic's native Messages API (POST /messages)
// to the machine's provider-neutral messages.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/task"
)

// Version is the anthropic-version header every request carries.
const Version = "2023-06-01"

// DefaultMaxTokens is the output budget New sets. The API requires one.
const DefaultMaxTokens = 8192

type Adapter struct {
	BaseURL   string // up to and including the version, e.g. https://api.anthropic.com/v1
	APIKey    string
	Model     string
	MaxTokens int // required by the API; 0 sends DefaultMaxTokens
	// ThinkingBudget turns on extended thinking with this many tokens when
	// positive. It must be below MaxTokens.
	ThinkingBudget int
	HTTP           *http.Client
}

var (
	_ model.Adapter = (*Adapter)(nil)
	_ model.Warmer  = (*Adapter)(nil)
)

func New(baseURL, apiKey, modelName string) *Adapter {
	return &Adapter{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		APIKey:    apiKey,
		Model:     modelName,
		MaxTokens: DefaultMaxTokens,
		HTTP:      &http.Client{Transport: openai.NewTransport(openai.HTTP2), Timeout: 5 * time.Minute},
	}
}

// Provider names this endpoint and model. Tool calls and thinking blocks
// carry it, and their provider-private bytes are replayed only to the same
// Provider: a thinking signature is bound to the model that produced it.
func (a *Adapter) Provider() string { return "anthropic:" + a.Model }

// WithProtocol switches the adapter's wire protocol and returns it.
func (a *Adapter) WithProtocol(p openai.Protocol) *Adapter {
	a.HTTP.Transport = openai.NewTransport(p)
	return a
}

type wireResp struct {
	Content    []json.RawMessage `json:"content"`
	StopReason string            `json:"stop_reason"`
	Usage      wireUsage         `json:"usage"`
}

type wireUsage struct {
	InputTokens              int `json:"input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	OutputTokens             int `json:"output_tokens"`
}

// usage reports the whole prompt: Anthropic's input_tokens counts only the
// part after the last cache breakpoint, so cache writes and reads are added
// back in.
func (u wireUsage) usage() task.Usage {
	return task.Usage{
		PromptTokens:     u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens,
		CompletionTokens: u.OutputTokens,
		CachedTokens:     u.CacheReadInputTokens,
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
		return model.Response{}, fmt.Errorf("anthropic: read response: %w", err)
	}
	var w wireResp
	if err := json.Unmarshal(data, &w); err != nil {
		return model.Response{}, fmt.Errorf("anthropic: decode response: %w: %s", err, a.snippet(data))
	}
	return a.response(w.Content, w.StopReason, w.Usage.usage())
}

func (a *Adapter) setHeaders(h http.Header) {
	if a.APIKey != "" {
		h.Set("x-api-key", a.APIKey)
	}
	h.Set("anthropic-version", Version)
}

// post sends a Messages request and returns the response once its status is
// known to be 2xx.
func (a *Adapter) post(ctx context.Context, req model.Request, stream bool) (*http.Response, error) {
	raw, err := a.body(req, stream)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/messages", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("anthropic: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if stream {
		hreq.Header.Set("Accept", "text/event-stream")
	}
	a.setHeaders(hreq.Header)
	resp, err := a.HTTP.Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("anthropic: %s returned %d%s: %s", a.Model, resp.StatusCode, statusNote(resp.StatusCode), a.snippet(data))
	}
	return resp, nil
}

// statusNote names the two statuses later phases must tell apart: 429 is a
// rate limit on this key, 529 is Anthropic as a whole being overloaded.
func statusNote(code int) string {
	switch code {
	case http.StatusTooManyRequests:
		return " (rate limited)"
	case 529:
		return " (overloaded)"
	}
	return ""
}

// response assembles the neutral turn from the provider's content blocks.
// Every block that is not plain text is kept whole as Opaque for replay.
func (a *Adapter) response(blocks []json.RawMessage, stop string, u task.Usage) (model.Response, error) {
	msg := task.Message{Role: task.RoleAssistant}
	answered := false
	for _, rb := range blocks {
		var b struct {
			Type     string          `json:"type"`
			Text     string          `json:"text"`
			Thinking string          `json:"thinking"`
			ID       string          `json:"id"`
			Name     string          `json:"name"`
			Input    json.RawMessage `json:"input"`
		}
		if err := json.Unmarshal(rb, &b); err != nil {
			return model.Response{}, fmt.Errorf("anthropic: decode content block: %w", err)
		}
		opaque, err := compact(rb)
		if err != nil {
			return model.Response{}, fmt.Errorf("anthropic: decode content block: %w", err)
		}
		switch b.Type {
		case "text":
			if b.Text != "" {
				msg.Parts = append(msg.Parts, task.Part{Kind: task.PartText, Text: b.Text})
				answered = true
			}
		case "tool_use":
			args := json.RawMessage(`{}`)
			if len(b.Input) > 0 {
				if args, err = compact(b.Input); err != nil {
					return model.Response{}, fmt.Errorf("anthropic: decode tool input: %w", err)
				}
			}
			msg.Parts = append(msg.Parts, task.Part{
				Kind:           task.PartToolCall,
				ProviderCallID: b.ID,
				Name:           b.Name,
				Args:           args,
				Provider:       a.Provider(),
				Opaque:         opaque,
			})
			answered = true
		default:
			// thinking, redacted_thinking, and any block type this adapter
			// does not know: private to this provider, replayed verbatim.
			msg.Parts = append(msg.Parts, task.Part{
				Kind:     task.PartReasoning,
				Text:     b.Thinking,
				Provider: a.Provider(),
				Opaque:   opaque,
			})
		}
	}
	finish := finishReason(stop)
	if !answered && finish != "length" {
		return model.Response{}, errors.New("anthropic: response has neither text nor tool calls")
	}
	return model.Response{Message: msg, FinishReason: finish, Usage: u}, nil
}

// finishReason maps stop_reason onto the vocabulary the OpenAI adapter
// reports, so the machine reads one set of words.
func finishReason(s string) string {
	switch s {
	case "end_turn", "stop_sequence":
		return "stop"
	case "tool_use":
		return "tool_calls"
	case "max_tokens", "model_context_window_exceeded":
		return "length"
	case "refusal":
		return "content_filter"
	}
	return s
}

// Warm opens the connection before the first model call needs it. Any HTTP
// status counts as warm; only a network failure is an error.
func (a *Adapter) Warm(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.BaseURL+"/models", nil)
	if err != nil {
		return err
	}
	a.setHeaders(req.Header)
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// snippet is a short piece of a response body for an error message, with
// the key cut out in case the server echoed it.
func (a *Adapter) snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if a.APIKey != "" {
		s = strings.ReplaceAll(s, a.APIKey, "[redacted]")
	}
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

func compact(raw []byte) (json.RawMessage, error) {
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
