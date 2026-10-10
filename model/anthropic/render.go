package anthropic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

type wireReq struct {
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	System    string        `json:"system,omitempty"`
	Messages  []wireMsg     `json:"messages"`
	Tools     []wireTool    `json:"tools,omitempty"`
	Thinking  *wireThinking `json:"thinking,omitempty"`
	Stream    bool          `json:"stream,omitempty"`
}

type wireMsg struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
}

type wireTool struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"input_schema"`
	CacheControl *cacheControl   `json:"cache_control,omitempty"`
}

type wireThinking struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens"`
}

type cacheControl struct {
	Type string `json:"type"`
}

var ephemeral = &cacheControl{Type: "ephemeral"}

type textBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolUseBlock struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type toolResultBlock struct {
	Type      string `json:"type"`
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

// body renders the request. The order is fixed (model, budget, system,
// messages, tools) and every block's bytes depend only on its own part, so
// the prefix a prompt cache matches stays byte-stable across a task.
func (a *Adapter) body(req model.Request, stream bool) ([]byte, error) {
	msgs, err := a.messages(req)
	if err != nil {
		return nil, err
	}
	w := wireReq{
		Model:     a.Model,
		MaxTokens: a.MaxTokens,
		System:    req.System,
		Messages:  msgs,
		Tools:     tools(req.Tools),
		Stream:    stream,
	}
	if w.MaxTokens <= 0 {
		w.MaxTokens = DefaultMaxTokens
	}
	if a.ThinkingBudget > 0 {
		w.Thinking = &wireThinking{Type: "enabled", BudgetTokens: a.ThinkingBudget}
	}
	raw, err := marshal(w)
	if err != nil {
		return nil, fmt.Errorf("anthropic: encode request: %w", err)
	}
	return raw, nil
}

// turn is one wire message being assembled. Tool results are kept apart so
// they can lead the turn whatever order they were committed in: Anthropic
// requires a tool_use turn to be answered by its results first.
type turn struct {
	role    string
	results []json.RawMessage
	other   []json.RawMessage
}

// messages renders neutral messages as alternating user and assistant
// turns. Consecutive messages that land on the same role are merged, and a
// message that renders to nothing is left out.
//
// A tool call this same provider produced goes back byte-for-byte from
// Opaque under its own id; any other call is rendered under the machine's id
// made safe for Anthropic. Thinking blocks go back only to the provider that
// signed them, and are dropped for anyone else.
func (a *Adapter) messages(req model.Request) ([]wireMsg, error) {
	var turns []*turn
	add := func(role string, results, other []json.RawMessage) {
		if len(results) == 0 && len(other) == 0 {
			return
		}
		if n := len(turns); n > 0 && turns[n-1].role == role {
			turns[n-1].results = append(turns[n-1].results, results...)
			turns[n-1].other = append(turns[n-1].other, other...)
			return
		}
		turns = append(turns, &turn{role: role, results: results, other: other})
	}

	ids := map[string]string{} // machine CallID -> id sent on the wire
	for _, m := range req.Messages {
		var blocks []json.RawMessage
		switch m.Role {
		case task.RoleSystem, task.RoleUser:
			for _, p := range m.Parts {
				if p.Kind != task.PartText || p.Text == "" {
					continue
				}
				b, err := marshal(textBlock{Type: "text", Text: p.Text})
				if err != nil {
					return nil, fmt.Errorf("anthropic: encode text: %w", err)
				}
				blocks = append(blocks, b)
			}
			add("user", nil, blocks)
		case task.RoleAssistant:
			for _, p := range m.Parts {
				switch p.Kind {
				case task.PartText:
					if p.Text == "" {
						continue
					}
					b, err := marshal(textBlock{Type: "text", Text: p.Text})
					if err != nil {
						return nil, fmt.Errorf("anthropic: encode text: %w", err)
					}
					blocks = append(blocks, b)
				case task.PartReasoning:
					if p.Provider == a.Provider() && isObject(p.Opaque) {
						blocks = append(blocks, p.Opaque)
					}
				case task.PartToolCall:
					b, id, err := a.toolUse(p)
					if err != nil {
						return nil, err
					}
					ids[p.CallID] = id
					blocks = append(blocks, b)
				}
			}
			add("assistant", nil, blocks)
		case task.RoleTool:
			for _, p := range m.Parts {
				if p.Kind != task.PartToolResult {
					continue
				}
				id, ok := ids[p.CallID]
				if !ok {
					id = sanitizeID(p.CallID)
				}
				b, err := marshal(toolResultBlock{Type: "tool_result", ToolUseID: id, Content: p.Text, IsError: p.IsError})
				if err != nil {
					return nil, fmt.Errorf("anthropic: encode tool result %s: %w", p.CallID, err)
				}
				blocks = append(blocks, b)
			}
			add("user", blocks, nil)
		}
	}

	out := make([]wireMsg, 0, len(turns))
	for _, t := range turns {
		out = append(out, wireMsg{Role: t.role, Content: append(t.results, t.other...)})
	}
	if len(out) >= 2 {
		markLast(out[len(out)-2].Content)
	}
	return out, nil
}

// toolUse renders one tool call and returns the id it went out under.
func (a *Adapter) toolUse(p task.Part) (json.RawMessage, string, error) {
	if p.Provider == a.Provider() && p.ProviderCallID != "" {
		if isObject(p.Opaque) {
			return p.Opaque, p.ProviderCallID, nil
		}
		return a.renderToolUse(p, p.ProviderCallID)
	}
	return a.renderToolUse(p, sanitizeID(p.CallID))
}

func (a *Adapter) renderToolUse(p task.Part, id string) (json.RawMessage, string, error) {
	in, err := input(p.Args)
	if err != nil {
		return nil, "", fmt.Errorf("anthropic: encode tool call %s: %w", p.CallID, err)
	}
	b, err := marshal(toolUseBlock{Type: "tool_use", ID: id, Name: p.Name, Input: in})
	if err != nil {
		return nil, "", fmt.Errorf("anthropic: encode tool call %s: %w", p.CallID, err)
	}
	return b, id, nil
}

// input makes tool arguments a JSON object, as Anthropic requires. Empty
// arguments are {}; anything that is not an object is kept, as text, under
// "_raw" so the next turn can still see what was sent.
func input(args json.RawMessage) (json.RawMessage, error) {
	s := bytes.TrimSpace(args)
	if len(s) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if isObject(s) {
		return compact(s)
	}
	return marshal(map[string]string{"_raw": string(args)})
}

// isObject reports whether raw is one valid JSON object.
func isObject(raw json.RawMessage) bool {
	s := bytes.TrimSpace(raw)
	return len(s) > 0 && s[0] == '{' && json.Valid(s)
}

// sanitizeID makes a machine id acceptable as a tool_use id, which must
// match ^[a-zA-Z0-9_-]+$. Machine ids ("c-17") already do.
func sanitizeID(id string) string {
	if id == "" {
		return "call"
	}
	return strings.Map(func(r rune) rune {
		if r == '_' || r == '-' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') {
			return r
		}
		return '_'
	}, id)
}

func tools(specs []task.ToolSpec) []wireTool {
	if len(specs) == 0 {
		return nil
	}
	out := make([]wireTool, 0, len(specs))
	for _, s := range specs {
		schema := json.RawMessage(s.Schema)
		if len(bytes.TrimSpace(schema)) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out = append(out, wireTool{Name: s.Name, Description: s.Description, InputSchema: schema})
	}
	out[len(out)-1].CacheControl = ephemeral
	return out
}

// markLast puts a cache breakpoint on the last block of a turn that can
// carry one (thinking blocks cannot). The marker is spliced in before the
// block's closing brace into a new slice: every other byte of the block is
// unchanged, and so is the Opaque it may have come from.
func markLast(blocks []json.RawMessage) {
	for i := len(blocks) - 1; i >= 0; i-- {
		var b struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(blocks[i], &b) != nil || b.Type == "thinking" || b.Type == "redacted_thinking" {
			continue
		}
		s := bytes.TrimSpace(blocks[i])
		body := bytes.TrimSpace(s[1 : len(s)-1])
		marked := make([]byte, 0, len(s)+40)
		marked = append(marked, s[:len(s)-1]...)
		if len(body) > 0 {
			marked = append(marked, ',')
		}
		marked = append(marked, `"cache_control":{"type":"ephemeral"}}`...)
		blocks[i] = marked
		return
	}
}

// marshal encodes without HTML escaping, so replayed text and signatures
// keep the bytes the provider sent.
func marshal(v any) (json.RawMessage, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}
