// Package task holds the state a task carries between instructions: its
// control block and the provider-neutral messages that make up its registers.
package task

import (
	"encoding/json"
	"strings"
)

// Role says who a message is from.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// PartKind says what a Part holds.
type PartKind string

const (
	PartText       PartKind = "text"
	PartToolCall   PartKind = "tool_call"
	PartToolResult PartKind = "tool_result"
	PartReasoning  PartKind = "reasoning"
)

// Part is one piece of a message. Which fields are meaningful depends on Kind.
type Part struct {
	Kind PartKind `json:"kind"`
	Text string   `json:"text,omitempty"`

	// CallID is the machine's own id for a tool call ("c-<n>"), shared by the
	// tool_call and its tool_result. ProviderCallID is the id the provider
	// returned, replayed only to that same provider.
	CallID         string          `json:"call_id,omitempty"`
	ProviderCallID string          `json:"provider_call_id,omitempty"`
	Name           string          `json:"name,omitempty"`
	Args           json.RawMessage `json:"args,omitempty"`
	IsError        bool            `json:"is_error,omitempty"`

	// Provider and Opaque carry a provider-private reasoning block, sent back
	// only to the provider that produced it.
	Provider string          `json:"provider,omitempty"`
	Opaque   json.RawMessage `json:"opaque,omitempty"`
}

// Message is one provider-neutral turn: a role and its ordered parts.
// Summary marks the message a compaction put in place of the messages it
// swapped out (design §5.3); Feedback marks the user message an answer check
// gave back with an answer it rejected (design §9.8). Adapters send both as
// the plain messages they are.
type Message struct {
	Role     Role   `json:"role"`
	Parts    []Part `json:"parts"`
	Summary  bool   `json:"summary,omitempty"`
	Feedback bool   `json:"feedback,omitempty"`
}

// Text builds a message holding a single text part.
func Text(role Role, s string) Message {
	return Message{Role: role, Parts: []Part{{Kind: PartText, Text: s}}}
}

// FeedbackMessage is the message an answer check's rejection puts after the
// answer it rejected: a user message, so the model answers again, marked
// Feedback. Compaction treats it as the user message it is.
func FeedbackMessage(text string) Message {
	m := Text(RoleUser, text)
	m.Feedback = true
	return m
}

// ToolCalls returns the message's tool_call parts in order.
func (m Message) ToolCalls() []Part {
	var out []Part
	for _, p := range m.Parts {
		if p.Kind == PartToolCall {
			out = append(out, p)
		}
	}
	return out
}

// TextContent joins the message's text parts.
func (m Message) TextContent() string {
	var b strings.Builder
	for _, p := range m.Parts {
		if p.Kind == PartText {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// ToolSpec is a tool as the model sees it.
type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// ToolResult is what one tool call returned.
type ToolResult struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error,omitempty"`
}
