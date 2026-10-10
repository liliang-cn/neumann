package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// nameOf returns names[v], or an error naming the type when v is out of range.
func nameOf(typ string, v int, names []string) ([]byte, error) {
	if v < 0 || v >= len(names) {
		return nil, fmt.Errorf("task: invalid %s %d", typ, v)
	}
	return []byte(names[v]), nil
}

// indexOf returns the index of name in names, or an error naming the type.
func indexOf(typ string, name []byte, names []string) (int, error) {
	for i, n := range names {
		if n == string(name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("task: unknown %s %q", typ, name)
}

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

func (s State) MarshalText() ([]byte, error) { return nameOf("State", int(s), stateNames[:]) }

func (s *State) UnmarshalText(b []byte) error {
	i, err := indexOf("State", b, stateNames[:])
	if err == nil {
		*s = State(i)
	}
	return err
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

func (p PC) MarshalText() ([]byte, error) { return nameOf("PC", int(p), pcNames[:]) }

func (p *PC) UnmarshalText(b []byte) error {
	i, err := indexOf("PC", b, pcNames[:])
	if err == nil {
		*p = PC(i)
	}
	return err
}

// WaitReason says what a Waiting task is waiting for.
type WaitReason int

const (
	WaitNone WaitReason = iota
	WaitIO
	WaitBackoff
	WaitReconcile
	WaitGroup
	WaitApproval // a tool round held until an operator approves its calls
)

var waitNames = [...]string{"none", "io", "backoff", "reconcile", "group", "approval"}

func (w WaitReason) String() string {
	if w < 0 || int(w) >= len(waitNames) {
		return "unknown"
	}
	return waitNames[w]
}

func (w WaitReason) MarshalText() ([]byte, error) { return nameOf("WaitReason", int(w), waitNames[:]) }

func (w *WaitReason) UnmarshalText(b []byte) error {
	i, err := indexOf("WaitReason", b, waitNames[:])
	if err == nil {
		*w = WaitReason(i)
	}
	return err
}

// Budget bounds a task.
type Budget struct {
	// MaxSteps bounds answered model calls plus unconfirmed ones
	// (Used.Steps + Unconfirmed); 0 = unlimited. Failed calls are bounded by
	// the machine's retry limit instead.
	MaxSteps int `json:"max_steps,omitempty"`
	// Deadline is an absolute time: once it passes, the task is cancelled
	// whatever state it is in and ends Failed with StopReason "deadline"
	// (design §9.4). Zero means none. It survives a restart.
	Deadline time.Time `json:"deadline,omitzero"`
}

// Usage is confirmed usage: only what a provider's response reported.
type Usage struct {
	// Steps counts answered model calls: a response came back (a final
	// answer, a tool-call turn, a truncated turn that is redone). A call that
	// failed with an error is not a step, and a lost response is counted in
	// TCB.Unconfirmed instead. A provider response leaves it 0.
	Steps            int `json:"steps,omitempty"`
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
// instructions (fields: design §3.2). Exactly one owner may modify it at a
// time (design §8).
type TCB struct {
	ID         string     `json:"id"`
	State      State      `json:"state"`
	PC         PC         `json:"pc"`
	Wait       WaitReason `json:"wait"`
	Class      string     `json:"class"`
	PinClass   bool       `json:"pin_class,omitempty"` // never moved to a fallback group when its own is down (design §6.3)
	Priority   int        `json:"priority"`
	ReadySince time.Time  `json:"ready_since"`
	System     string     `json:"system,omitempty"`
	Tools      []ToolSpec `json:"tools,omitempty"`
	Regs       []Message  `json:"regs"`
	Allow      []string   `json:"allow,omitempty"`
	Deny       []string   `json:"deny,omitempty"`
	Budget     Budget     `json:"budget"`
	Used       Usage      `json:"used"`
	// CallSeq is how many tool call ids the task has handed out; the next one
	// is c-<CallSeq+1>. It never decreases, so ids stay unique after compaction
	// removes old messages.
	CallSeq int `json:"call_seq"`
	// Unconfirmed counts model requests that were sent but whose response was
	// never received (a crash or halt during the call). The provider may have
	// processed and billed them; their tokens are unknown, so not in Used.
	Unconfirmed int `json:"unconfirmed,omitempty"`
	// WakeAt is when a task in Waiting(Backoff) is due to run again. Only a
	// running machine honours it: after a restart the task is Ready at once.
	WakeAt time.Time `json:"wake_at,omitzero"`
	// Retries counts consecutive model calls that failed with an error worth
	// retrying (overload, transient); a success resets it. Rate limits do not
	// count. At the machine's limit the task fails with "model_error".
	Retries int `json:"retries,omitempty"`
	// MaxOut is the task's own output cap; 0 is the adapter's default. A turn
	// cut off before it produced anything is redone with that cap raised
	// fourfold per redo in a row (Truncations, at most 2, design §13); the
	// first call answered without being cut off resets Truncations, and the
	// raise with it.
	MaxOut      int `json:"max_out,omitempty"`
	Truncations int `json:"truncations,omitempty"`
	// Window is the context window the task's provider turned out to have:
	// set when it rejected a request as too long for its window (smaller
	// than what that request was estimated at), 0 when none did. Compaction
	// sizes by it when it is below the adapter's Caps.Window, or when that is
	// unknown. A move to another group clears it.
	Window int `json:"window,omitempty"`
	// CompactChunk is how many chunks the compaction pass in progress (PC
	// StepCompact) has swapped out so far; 0 when none is in progress.
	CompactChunk int `json:"compact_chunk,omitempty"`
	// Archived is how many messages compaction has moved from the registers
	// to the task's archive (mem.Store.Archive); a reader takes that many,
	// so a compaction committed between its two reads changes nothing.
	Archived   int    `json:"archived,omitempty"`
	StopReason string `json:"stop_reason,omitempty"`
	Version    int64  `json:"version"`
}

// Clone returns a deep copy, or nil for a nil receiver. The store keeps clones
// so that the owner can go on mutating its TCB without changing what was
// committed. It copies field by field rather than through JSON: raw bytes
// (a truncated Args, a provider's Opaque block) must survive byte-for-byte.
func (t *TCB) Clone() *TCB {
	if t == nil {
		return nil
	}
	c := *t
	c.Allow = slices.Clone(t.Allow)
	c.Deny = slices.Clone(t.Deny)
	if t.Tools != nil {
		c.Tools = make([]ToolSpec, len(t.Tools))
		for i, ts := range t.Tools {
			ts.Schema = json.RawMessage(bytes.Clone(ts.Schema))
			c.Tools[i] = ts
		}
	}
	c.Regs = CloneMessages(t.Regs)
	return &c
}

// CloneMessages returns a deep copy of msgs (nil for nil), raw bytes
// included.
func CloneMessages(msgs []Message) []Message {
	if msgs == nil {
		return nil
	}
	out := make([]Message, len(msgs))
	for i, m := range msgs {
		if m.Parts != nil {
			parts := make([]Part, len(m.Parts))
			for j, p := range m.Parts {
				p.Args = json.RawMessage(bytes.Clone(p.Args))
				p.Opaque = json.RawMessage(bytes.Clone(p.Opaque))
				parts[j] = p
			}
			m.Parts = parts
		}
		out[i] = m
	}
	return out
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
