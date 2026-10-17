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

// Adapter talks to one model behind an OpenAI-compatible endpoint. Build it
// with New; set the exported fields before the machine starts.
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
	// Local backends (design §6.4). Parallel is how many requests the
	// backend serves at once, 0 unknown; Ollama serves one. Prefill says a
	// request ending with an assistant message is continued and only the
	// continuation comes back. ThinkOff is the reasoning_effort value that
	// turns thinking off ("none" on Ollama), sent when a request asks for
	// NoThink; empty means the model cannot turn it off and NoThink is
	// ignored. All three are reported through Caps as they are.
	Parallel int
	Prefill  bool
	ThinkOff string
	// ThinkTags are the tags the model writes its thinking between when
	// thinking is part of its content ("<think>", "</think>" on Ollama's
	// qwen models). Set (the open tag not empty), a call cut off while the
	// model was still thinking can be continued (Caps.ThinkPrefill, design
	// §6.4): a request ending with an assistant message that holds only
	// reasoning is sent with that message's content set to the open tag, a
	// newline and the reasoning, and the model goes on thinking from there.
	// Its answer then comes back all in content: the rest of the thinking,
	// the close tag, the answer. The adapter splits it at the close tag, in
	// a stream too (a tag split across chunks included): what comes before
	// is reasoning (Delta.Reasoning, a reasoning part), what comes after is
	// text, leading newlines dropped. A backend that parses the tags out
	// itself (llama.cpp with reasoning parsing) answers in the reasoning
	// field and untagged content instead: once reasoning arrives in its own
	// field, the content from there on is text. Sending the reasoning in
	// the reasoning field does not work on Ollama: the model thinks again
	// from the start. The answer to any other call is split the same way
	// when its content opens with the open tag (white space before it
	// aside): a backend that leaves the thinking in content.
	ThinkTags [2]string
	// Per-request timeouts (design §11). Timeout bounds the whole call,
	// from sending the request to the end of the response body; New sets
	// DefaultTimeout, 0 is no limit. FirstByteTimeout bounds sending to the
	// first byte of the response body; 0 leaves it to Timeout. A
	// non-streaming response body arrives only when the answer is complete,
	// so for Call it bounds the whole generation. IdleTimeout bounds the gap
	// between two reads of the body that yield data once the first byte has
	// arrived (the gap between stream chunks); 0 is no limit. A local
	// backend takes long to read a long prompt and then sends steadily, so
	// it wants no Timeout, a generous FirstByteTimeout and a short
	// IdleTimeout. While the caller's context is alive, any of them running
	// out is a Transient *model.Error wrapping a *TimeoutError, and also
	// model.ErrResponseLost once the response had started.
	Timeout          time.Duration
	FirstByteTimeout time.Duration
	IdleTimeout      time.Duration
	// HTTP sends the requests. Its own Timeout, if set, applies on top of
	// the ones above.
	HTTP *http.Client
	// Now is the clock rate-limit resets are read against; nil: time.Now.
	Now func() time.Time
}

var _ model.Adapter = (*Adapter)(nil)

// New returns an adapter for modelName at baseURL (up to and including the
// version, e.g. https://api.openai.com/v1), sending apiKey as a bearer token,
// over HTTP/2, with a Timeout of DefaultTimeout.
func New(baseURL, apiKey, modelName string) *Adapter {
	return &Adapter{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Model:   modelName,
		Timeout: DefaultTimeout,
		HTTP:    &http.Client{Transport: NewTransport(HTTP2)},
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

// Caps reports ContextWindow, the output cap, NoTools, MaxTools, Parallel,
// Prefill, whether ThinkOff is set, and whether ThinkTags are
// (ThinkPrefill) (model.CapsReporter).
func (a *Adapter) Caps() model.Caps {
	n := a.MaxTools
	switch {
	case n == 0:
		n = DefaultMaxTools
	case n < 0:
		n = 0
	}
	return model.Caps{
		Window: max(a.ContextWindow, 0), MaxOut: a.MaxOut(), Tools: !a.NoTools, MaxTools: n,
		Parallel: max(a.Parallel, 0), Prefill: a.Prefill, ThinkOff: a.ThinkOff != "",
		ThinkPrefill: a.ThinkTags[0] != "" && a.ThinkTags[1] != "",
	}
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
			Content          *string           `json:"content"`
			ReasoningContent *string           `json:"reasoning_content"`
			Reasoning        *string           `json:"reasoning"` // Ollama's spelling
			ToolCalls        []json.RawMessage `json:"tool_calls"`
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

// Call sends one non-streaming /chat/completions request. A failure it can
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
	out, err := a.parse(data, a.thinkPrefill(req))
	if err != nil {
		return model.Response{}, err
	}
	out.Rate = httperr.RateInfo(resp.Header, a.now())
	return out, nil
}

// post sends a chat-completions request and returns the response once its
// status is known to be 2xx. Every failure is a classified *model.Error. The
// returned body is under the adapter's timeouts until it is closed.
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
	if req.NoThink && a.ThinkOff != "" {
		body["reasoning_effort"] = a.ThinkOff
	}
	if stream {
		body["stream"] = true
		body["stream_options"] = map[string]bool{"include_usage": true}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}
	w := a.watch(ctx)
	hreq, err := http.NewRequestWithContext(w.ctx, http.MethodPost, a.BaseURL+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		w.stop()
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
		err = w.cause(err)
		w.stop()
		return nil, a.ioError(ctx, "send request", err, false, nil)
	}
	resp.Body = &watchedBody{ReadCloser: resp.Body, w: w}
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, a.statusError(resp.StatusCode, resp.Header, data)
	}
	return resp, nil
}

func (a *Adapter) parse(data []byte, thinking bool) (model.Response, error) {
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
	text, reasoning := "", ""
	if ch.Message.Content != nil {
		text = *ch.Message.Content
	}
	native := ""
	for _, r := range []*string{ch.Message.ReasoningContent, ch.Message.Reasoning} {
		if r != nil {
			native += *r
		}
	}
	fresh := "" // the thinking of a call that continued none
	switch {
	case thinking && native != "":
		// The backend parsed the thinking out itself: the content is the
		// answer, untagged.
		reasoning = native
	case thinking:
		sp := a.splitter()
		reasoning, text = sp.feed(text)
		r, t := sp.flush()
		reasoning, text = reasoning+r, text+t
	default:
		fresh = native
		if rest, ok := a.opensThought(text); ok {
			sp := a.splitter()
			r, t := sp.feed(rest)
			rr, tt := sp.flush()
			fresh, text = fresh+r+rr, t+tt
		}
	}
	return a.response(text, reasoning, fresh, ch.Message.ToolCalls, ch.FinishReason, w.Usage.usage())
}

// opensThought reports whether content, leading white space aside, begins
// with the open tag of ThinkTags, and returns what follows the tag: an answer
// to a call that continued no thought, written by a backend that leaves the
// thinking in content.
func (a *Adapter) opensThought(content string) (string, bool) {
	if a.ThinkTags[0] == "" || a.ThinkTags[1] == "" {
		return "", false
	}
	return strings.CutPrefix(strings.TrimLeft(content, " \t\r\n"), a.ThinkTags[0])
}

// response assembles the neutral turn from the provider's text, the
// reasoning of a continued thought (ThinkTags), the thinking of any other
// call (fresh), its complete tool-call objects (kept whole as Opaque for
// replay) and the usage. The thinking of a call that continued no thought
// is not part of an answer; it is kept only when the turn has nothing else,
// so that the machine can tell a thought that stopped without an answer from
// an empty turn (design §6.4). White space alone is no text: dropped, so
// that "</think>\n\n" after a thought is not taken for an answer. A turn
// with nothing at all, no thinking either (no reasoning text, no reasoning
// tokens), is a Transient error unless it was cut off.
func (a *Adapter) response(text, reasoning, fresh string, calls []json.RawMessage, finish string, u task.Usage) (model.Response, error) {
	msg := task.Message{Role: task.RoleAssistant}
	if strings.TrimSpace(text) == "" && len(calls) == 0 {
		text = ""
		if reasoning == "" {
			reasoning = fresh
		}
	}
	if text == "" && reasoning == "" && len(calls) == 0 && finish != "length" && u.ReasoningTokens == 0 {
		return model.Response{}, &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: "response has neither text nor tool calls", Err: model.ErrEmptyResponse}
	}
	if reasoning != "" {
		msg.Parts = append(msg.Parts, task.Part{Kind: task.PartReasoning, Text: reasoning, Provider: a.Provider()})
	}
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
	return model.Response{Message: msg, FinishReason: finish, Usage: u}, nil
}

// messages renders neutral messages in the chat-completions shape. A tool
// call this same provider produced is replayed byte-for-byte from Opaque,
// because gateways put fields there (Gemini's thought signatures, for one)
// that must come back; any other call is rendered under the machine's id.
// A request that ends with an assistant text message ends with it as is: a
// backend with Prefill continues it. One that ends with an assistant message
// holding only reasoning, with ThinkTags set, ends with that reasoning
// inside the open tag (thinkPrefill).
func (a *Adapter) messages(req model.Request) ([]wireMsg, error) {
	var out []wireMsg
	if req.System != "" {
		out = append(out, wireMsg{Role: "system", Content: ptr(req.System)})
	}
	ids := map[string]string{} // machine CallID -> id sent on the wire
	for i, m := range req.Messages {
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
			if i == len(req.Messages)-1 && a.thinkPrefill(req) {
				w.Content = ptr(a.ThinkTags[0] + "\n" + reasoningOf(m))
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

// thinkPrefill reports whether req asks the model to go on thinking: it ends
// with an assistant message holding reasoning and no text or tool call, and
// ThinkTags are set.
func (a *Adapter) thinkPrefill(req model.Request) bool {
	if a.ThinkTags[0] == "" || a.ThinkTags[1] == "" || len(req.Messages) == 0 {
		return false
	}
	m := req.Messages[len(req.Messages)-1]
	return m.Role == task.RoleAssistant && m.TextContent() == "" && len(m.ToolCalls()) == 0 && reasoningOf(m) != ""
}

// reasoningOf is the text of m's reasoning parts, joined.
func reasoningOf(m task.Message) string {
	var b strings.Builder
	for _, p := range m.Parts {
		if p.Kind == task.PartReasoning {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// thinkSplit splits the content of an answer that continues a thought
// (ThinkTags): reasoning up to the close tag, text after it, its leading
// newlines dropped. Fed piece by piece, it holds back the end of a piece
// that may be the start of the close tag until the next piece says.
type thinkSplit struct {
	close    string
	thinking bool
	pend     string
	begun    bool // text other than newlines has come
}

func (a *Adapter) splitter() *thinkSplit { return &thinkSplit{close: a.ThinkTags[1], thinking: true} }

// feed takes the next piece of content and returns what of it is now known
// to be reasoning and text.
func (s *thinkSplit) feed(piece string) (reasoning, text string) {
	if !s.thinking {
		return "", s.text(piece)
	}
	buf := s.pend + piece
	if i := strings.Index(buf, s.close); i >= 0 {
		s.thinking, s.pend = false, ""
		return buf[:i], s.text(buf[i+len(s.close):])
	}
	k := 0 // the longest end of buf that starts the close tag
	for n := min(len(s.close)-1, len(buf)); n > 0; n-- {
		if strings.HasSuffix(buf, s.close[:n]) {
			k = n
			break
		}
	}
	s.pend = buf[len(buf)-k:]
	return buf[:len(buf)-k], ""
}

// parsed says the backend parses the thinking out itself: reasoning came in
// its own field (reasoning, reasoning_content). From here on content is
// text, untagged; what was held back is returned as reasoning.
func (s *thinkSplit) parsed() (held string) {
	if !s.thinking {
		return ""
	}
	s.thinking = false
	held, s.pend = s.pend, ""
	return held
}

// flush ends the content: what was held back was reasoning after all.
func (s *thinkSplit) flush() (reasoning, text string) {
	r := s.pend
	s.pend = ""
	return r, ""
}

func (s *thinkSplit) text(piece string) string {
	if !s.begun {
		piece = strings.TrimLeft(piece, "\r\n")
		s.begun = piece != ""
	}
	return piece
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
