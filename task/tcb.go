package task

import (
	"encoding/json"
	"time"
)

// State is where a task is in its life.
type State int

const (
	Ready State = iota
	Running
	Waiting
	Suspended
	Done
	Failed
	Cancelled
)

var stateNames = [...]string{"ready", "running", "waiting", "suspended", "done", "failed", "cancelled"}

func (s State) String() string {
	if s < 0 || int(s) >= len(stateNames) {
		return "unknown"
	}
	return stateNames[s]
}

// Terminal reports whether the task has finished for good.
func (s State) Terminal() bool { return s == Done || s == Failed || s == Cancelled }

// PC is the kind of the task's next instruction.
type PC int

const (
	StepModel PC = iota
	StepTools
	StepCompact
)

var pcNames = [...]string{"model", "tools", "compact"}

func (p PC) String() string {
	if p < 0 || int(p) >= len(pcNames) {
		return "unknown"
	}
	return pcNames[p]
}

// WaitReason says what a Waiting task is waiting for.
type WaitReason int

const (
	WaitNone WaitReason = iota
	WaitIO
	WaitBackoff
	WaitReconcile
	WaitGroup
)

// Budget bounds a task. Phase 1 enforces MaxSteps only.
type Budget struct {
	MaxSteps int `json:"max_steps,omitempty"` // model instructions; 0 = unlimited
}

// Usage is confirmed usage: only what a provider's response reported.
type Usage struct {
	Steps            int `json:"steps"`
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	ReasoningTokens  int `json:"reasoning_tokens"`
	CachedTokens     int `json:"cached_tokens"`
}

// Add sums o's token counts into u. Steps are counted by the caller.
func (u *Usage) Add(o Usage) {
	u.PromptTokens += o.PromptTokens
	u.CompletionTokens += o.CompletionTokens
	u.ReasoningTokens += o.ReasoningTokens
	u.CachedTokens += o.CachedTokens
}

// TCB is a task control block: everything a task carries between
// instructions. Exactly one owner may modify it at a time (design §8).
type TCB struct {
	ID         string     `json:"id"`
	State      State      `json:"state"`
	PC         PC         `json:"pc"`
	Wait       WaitReason `json:"wait"`
	Class      string     `json:"class"`
	Priority   int        `json:"priority"`
	ReadySince time.Time  `json:"ready_since"`
	System     string     `json:"system,omitempty"`
	Tools      []ToolSpec `json:"tools,omitempty"`
	Regs       []Message  `json:"regs"`
	Allow      []string   `json:"allow,omitempty"`
	Deny       []string   `json:"deny,omitempty"`
	Budget     Budget     `json:"budget"`
	Used       Usage      `json:"used"`
	StopReason string     `json:"stop_reason,omitempty"`
	Version    int64      `json:"version"`
}

// Clone returns a deep copy. The store keeps clones so that the owner can go
// on mutating its TCB without changing what was committed.
func (t *TCB) Clone() *TCB {
	b, err := json.Marshal(t)
	if err != nil {
		panic("task: marshal TCB: " + err.Error())
	}
	var c TCB
	if err := json.Unmarshal(b, &c); err != nil {
		panic("task: unmarshal TCB: " + err.Error())
	}
	return &c
}

// LastAnswer returns the text of the last assistant message that has text.
func (t *TCB) LastAnswer() string {
	for i := len(t.Regs) - 1; i >= 0; i-- {
		if t.Regs[i].Role != RoleAssistant {
			continue
		}
		if s := t.Regs[i].TextContent(); s != "" {
			return s
		}
	}
	return ""
}

// CallCount is how many tool calls the task has made; the next call's id is
// "c-<CallCount()+1>".
func (t *TCB) CallCount() int {
	n := 0
	for _, m := range t.Regs {
		n += len(m.ToolCalls())
	}
	return n
}
