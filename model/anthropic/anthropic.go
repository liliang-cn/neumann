package anthropic

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
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/task"
)

// Version is the anthropic-version header every request carries.
const Version = "2023-06-01"

// DefaultMaxTokens is the output budget New sets. The API requires one.
const DefaultMaxTokens = 8192

// Adapter talks to one model through Anthropic's Messages API. Build it with
// New; set the exported fields before the machine starts.
type Adapter struct {
	BaseURL   string // up to and including the version, e.g. https://api.anthropic.com/v1
	APIKey    string
	Model     string
	MaxTokens int // required by the API; 0 sends DefaultMaxTokens
	// ThinkingBudget turns on extended thinking with this many tokens when
	// positive. It must be below MaxTokens. A request with NoThink is sent
	// without thinking whatever the budget: on the models that take a budget,
	// a request with no thinking field does not think.
	ThinkingBudget int
	// ThinkOff is the thinking type a request with NoThink sends to turn
	// thinking off: "disabled" (Opus 4.7, 4.8, Opus 5 and Haiku 5.5 at effort
	// high or below, Sonnet 5), "between_tools" (Sonnet 5.5: no up-front
	// thinking). Current models think when the field is left out, and some
	// cannot turn thinking off at all (Opus 5.5, Fable): empty, NoThink is
	// sent with no thinking field, which turns it off only on the models that
	// do not think by default. Caps.ThinkOff is true when ThinkOff or
	// ThinkingBudget is set.
	ThinkOff string
	// What the model can take (model.Caps), read before a task is moved
	// onto this adapter's group. ContextWindow 0 is DefaultContextWindow; a
	// negative value is unknown. NoTools marks a model that cannot call
	// tools. MaxTools 0 is no limit: the API documents none.
	ContextWindow int
	NoTools       bool
	MaxTools      int
	HTTP          *http.Client
	// Now is the clock rate-limit resets are read against; nil: time.Now.
	Now func() time.Time
}

var (
	_ model.Adapter = (*Adapter)(nil)
	_ model.Warmer  = (*Adapter)(nil)
)

// New returns an adapter for modelName at baseURL (up to and including the
// version, e.g. https://api.anthropic.com/v1), sending apiKey as x-api-key.
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

// MaxOut is the max_tokens sent when a request carries no MaxOut: MaxTokens,
// or DefaultMaxTokens when that is unset (model.OutputCap).
func (a *Adapter) MaxOut() int {
	if a.MaxTokens > 0 {
		return a.MaxTokens
	}
	return DefaultMaxTokens
}

// DefaultContextWindow is the context window of current Claude models.
const DefaultContextWindow = 200000

// Caps reports ContextWindow, the output cap, NoTools and MaxTools
// (model.CapsReporter). ThinkOff is true when the adapter knows how to turn
// thinking off: ThinkOff is set, or ThinkingBudget is (a model that takes a
// budget does not think without one). Prefill is false: Anthropic rejects an
// assistant prefix that ends in whitespace and forbids one while thinking is
// on, and some models take no prefix at all, so the machine never asks this
// adapter to continue a cut-off answer. Parallel is 0, unknown.
func (a *Adapter) Caps() model.Caps {
	w := a.ContextWindow
	switch {
	case w == 0:
		w = DefaultContextWindow
	case w < 0:
		w = 0
	}
	return model.Caps{Window: w, MaxOut: a.MaxOut(), Tools: !a.NoTools, MaxTools: max(a.MaxTools, 0),
		ThinkOff: a.ThinkOff != "" || a.ThinkingBudget > 0}
}

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

// Call sends one non-streaming POST /messages request. A failure it can
// explain is a *model.Error classified as design §11 describes; a call whose
// ctx ended reports that.
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
	var w wireResp
	if err := json.Unmarshal(data, &w); err != nil {
		return model.Response{}, a.lostError("decode response: "+a.snippet(data), err)
	}
	out, err := a.response(w.Content, w.StopReason, w.Usage.usage())
	if err != nil {
		return model.Response{}, err
	}
	out.Rate = httperr.RateInfo(resp.Header, a.now())
	return out, nil
}

func (a *Adapter) setHeaders(h http.Header) {
	if a.APIKey != "" {
		h.Set("x-api-key", a.APIKey)
	}
	h.Set("anthropic-version", Version)
}

// post sends a Messages request and returns the response once its status is
// known to be 2xx. Every failure past building the request is a classified
// *model.Error.
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
		return nil, a.ioError(ctx, "send request", err, false, nil)
	}
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, a.statusError(resp.StatusCode, resp.Header, data)
	}
	return resp, nil
}

// response assembles the neutral turn from the provider's content blocks.
// Every block that is not plain text is kept whole as Opaque for replay. A
// turn with no text and no tool call is a Transient error unless it was cut
// off, or it thought: a turn that ends with thinking alone is a thought that
// stopped without an answer, returned as it is for the machine to judge
// (design §6.4).
func (a *Adapter) response(blocks []json.RawMessage, stop string, u task.Usage) (model.Response, error) {
	msg := task.Message{Role: task.RoleAssistant}
	answered, thought := false, false
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
			return model.Response{}, a.lostError("decode content block", err)
		}
		opaque, err := compact(rb)
		if err != nil {
			return model.Response{}, a.lostError("decode content block", err)
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
					return model.Response{}, a.lostError("decode tool input", err)
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
			thought = thought || b.Type == "thinking" || b.Type == "redacted_thinking"
			msg.Parts = append(msg.Parts, task.Part{
				Kind:     task.PartReasoning,
				Text:     b.Thinking,
				Provider: a.Provider(),
				Opaque:   opaque,
			})
		}
	}
	finish := finishReason(stop)
	if !answered && !thought && finish != "length" {
		return model.Response{}, &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: "response has neither text nor tool calls", Err: model.ErrEmptyResponse}
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
