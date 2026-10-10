# neumann 第一阶段实现计划：固定并发 + 状态机

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现设计文档第 17 节的第一阶段：一个执行组、固定数量的 vCore、内存版 Store；TCB 状态机、就绪队列和等待集合、工具异步派发、中断表和插话，并让第 18 节的状态机不变量（1–6）和准入不变量（20）在 `-race` 下通过。

**Architecture:** 所有状态变化走唯一的写路径 `Commit(CommitSet)`，先提交、后动作（第一阶段的 Store 是内存实现，但接口和语义与以后的 SQLite 版一致）。vCore 从调度器取任务、执行一条指令、提交、再交还调度器；工具调用在提交 `dispatched` 之后才派发，在独立的 goroutine 里执行，全部完成后由批次 goroutine 把任务从等待集合取出、提交结果、放回就绪队列。外部输入（插话）写进中断表，由任务的持有者在下一条模型指令开头取用。

**Tech Stack:** Go 1.25+（本机 1.27），标准库，`github.com/google/uuid`。

**范围外（后续阶段）：** SQLite、重启恢复、待核对（阶段 2）；取消、Deadline、故障注入（阶段 3）；多执行组、拥塞窗口、限流器、真实模型适配器、压缩、截断升级、L2（阶段 4）。

**提交规则：** commit message 只写一行标题；不加任何 Co-Authored-By 或生成声明。

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `go.mod`, `Makefile` | 模块与检查命令 |
| `task/message.go` | 中立消息格式、`ToolSpec`、`ToolResult` |
| `task/tcb.go` | TCB、状态、PC、预算、用量、`Clone` |
| `proto/proto.go` | `Instruction`、`Event`、事件种类 |
| `mem/mem.go` | `CommitSet`、`CallRecord`、`Interrupt`、`Store` 接口、错误 |
| `mem/memory.go` | 内存版 Store |
| `model/model.go` | `Adapter` 接口、`Request`、`Response` |
| `sim/sim.go` | 脚本模型和请求检查函数（测试用） |
| `bus/bus.go` | 总线：命名、页表、设备并发、调用 |
| `bus/builtin/builtin.go` | 用 Go 函数实现的设备 |
| `sched/sched.go` | 就绪队列、等待集合、老化、所有权交接 |
| `core/core.go` | `Env`、vCore 循环、`step`、`finish`、`fail` |
| `core/model.go` | `execModel`、插话合并 |
| `core/tools.go` | `execTools`、批次派发、完成后唤醒 |
| `neumann.go` | `Machine`、`Config`、公开 API |
| `hub.go` | 事件通知 |
| `invariants_test.go` | 第 18 节第一阶段不变量的端到端测试 |

---

### Task 1: 模块骨架与 task 包

**Files:**
- Create: `go.mod`, `Makefile`, `task/message.go`, `task/tcb.go`
- Test: `task/task_test.go`

- [ ] **Step 1: 初始化模块**

```bash
cd ~/Things/AI/base/neumann
go mod init github.com/liliang-cn/neumann
go mod edit -go=1.25
```

写 `Makefile`：

```makefile
.PHONY: check test

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...

test:
	go test ./...
```

- [ ] **Step 2: 写失败的测试** `task/task_test.go`

```go
package task

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCloneIsDeep(t *testing.T) {
	orig := &TCB{
		ID:         "t1",
		ReadySince: time.Unix(100, 0).UTC(),
		Regs:       []Message{Text(RoleUser, "hi")},
		Allow:      []string{"a"},
		Version:    3,
	}
	c := orig.Clone()
	c.Regs[0].Parts[0].Text = "changed"
	c.Allow[0] = "b"
	if orig.Regs[0].Parts[0].Text != "hi" || orig.Allow[0] != "a" {
		t.Fatalf("clone shares memory with the original")
	}
	if c.ID != "t1" || c.Version != 3 || !c.ReadySince.Equal(orig.ReadySince) {
		t.Fatalf("clone lost fields: %+v", c)
	}
}

func TestTerminal(t *testing.T) {
	want := map[State]bool{
		Ready: false, Running: false, Waiting: false, Suspended: false,
		Done: true, Failed: true, Cancelled: true,
	}
	for s, w := range want {
		if s.Terminal() != w {
			t.Errorf("%s.Terminal() = %v, want %v", s, !w, w)
		}
	}
}

func TestLastAnswerAndCallCount(t *testing.T) {
	tc := &TCB{Regs: []Message{
		Text(RoleUser, "q"),
		{Role: RoleAssistant, Parts: []Part{
			{Kind: PartToolCall, Name: "x", Args: json.RawMessage(`{}`)},
			{Kind: PartToolCall, Name: "y"},
		}},
		{Role: RoleTool, Parts: []Part{{Kind: PartToolResult, CallID: "c-1"}}},
		Text(RoleAssistant, "answer"),
	}}
	if got := tc.LastAnswer(); got != "answer" {
		t.Fatalf("LastAnswer = %q", got)
	}
	if got := tc.CallCount(); got != 2 {
		t.Fatalf("CallCount = %d", got)
	}
}
```

- [ ] **Step 3: 运行，确认失败**

Run: `go test ./task/`
Expected: 编译失败，`undefined: TCB`、`undefined: Text` 等。

- [ ] **Step 4: 写 `task/message.go`**

```go
// Package task holds the state a task carries between instructions: its
// control block and the provider-neutral messages that make up its registers.
package task

import (
	"encoding/json"
	"strings"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

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

type Message struct {
	Role  Role   `json:"role"`
	Parts []Part `json:"parts"`
}

// Text builds a message holding a single text part.
func Text(role Role, s string) Message {
	return Message{Role: role, Parts: []Part{{Kind: PartText, Text: s}}}
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
```

- [ ] **Step 5: 写 `task/tcb.go`**

```go
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
```

- [ ] **Step 6: 运行，确认通过**

Run: `go test ./task/`
Expected: `ok  github.com/liliang-cn/neumann/task`

- [ ] **Step 7: 提交**

```bash
git add go.mod Makefile task
git commit -m "feat(task): TCB, neutral messages and state machine types"
```

---

### Task 2: proto 包

**Files:**
- Create: `proto/proto.go`
- Test: `proto/proto_test.go`

- [ ] **Step 1: 写失败的测试** `proto/proto_test.go`

```go
package proto

import "testing"

func TestNewEventMarshalsData(t *testing.T) {
	ev := NewEvent(EvAnswer, "t1", map[string]string{"text": "hi"})
	if ev.Kind != EvAnswer || ev.TaskID != "t1" || string(ev.Data) != `{"text":"hi"}` {
		t.Fatalf("event = %+v data=%s", ev, ev.Data)
	}
	if ev.Seq != 0 || ev.TaskSeq != 0 {
		t.Fatalf("sequence numbers are the store's to assign, got %d/%d", ev.Seq, ev.TaskSeq)
	}
	if NewEvent(EvStep, "t1", nil).Data != nil {
		t.Fatalf("nil data should stay nil")
	}
}

func TestTerminalEvents(t *testing.T) {
	for kind, want := range map[string]bool{
		EvDone: true, EvFailed: true, EvCancelled: true,
		EvAccepted: false, EvAnswer: false, EvSteerDropped: false,
	} {
		if (Event{Kind: kind}).Terminal() != want {
			t.Errorf("%s: Terminal() != %v", kind, want)
		}
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./proto/`
Expected: 编译失败，`undefined: NewEvent`。

- [ ] **Step 3: 写 `proto/proto.go`**

```go
// Package proto is the machine's wire format: instructions in, events out.
package proto

import (
	"encoding/json"

	"github.com/liliang-cn/neumann/task"
)

// Instruction kinds. Phase 1 implements submit and steer.
const (
	KindSubmit = "submit"
	KindSteer  = "steer"
)

// Instruction is everything the machine accepts from outside.
type Instruction struct {
	Kind     string       `json:"kind"`
	TaskID   string       `json:"task_id,omitempty"`
	Input    string       `json:"input,omitempty"`
	Class    string       `json:"class,omitempty"`
	Priority int          `json:"priority,omitempty"`
	Budget   *task.Budget `json:"budget,omitempty"`
	Allow    []string     `json:"allow,omitempty"`
	Deny     []string     `json:"deny,omitempty"`
}

// Event kinds.
const (
	EvAccepted     = "accepted"
	EvStep         = "step"
	EvToolCall     = "tool_call"
	EvToolResult   = "tool_result"
	EvUsage        = "usage"
	EvAnswer       = "answer"
	EvDone         = "done"
	EvFailed       = "failed"
	EvCancelled    = "cancelled"
	EvSteerDropped = "steer_dropped"
)

// Event is something that happened, published only after it was committed.
// Seq is global and TaskSeq is per task; the store assigns both.
type Event struct {
	Kind    string          `json:"kind"`
	TaskID  string          `json:"task_id,omitempty"`
	Seq     int64           `json:"seq"`
	TaskSeq int64           `json:"task_seq"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// NewEvent builds an uncommitted event; data is marshalled to JSON.
func NewEvent(kind, taskID string, data any) Event {
	ev := Event{Kind: kind, TaskID: taskID}
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			panic("proto: marshal event data: " + err.Error())
		}
		ev.Data = b
	}
	return ev
}

// Terminal reports whether the event ends its task.
func (e Event) Terminal() bool {
	return e.Kind == EvDone || e.Kind == EvFailed || e.Kind == EvCancelled
}
```

- [ ] **Step 4: 运行，确认通过**

Run: `go test ./proto/`
Expected: `ok`

- [ ] **Step 5: 提交**

```bash
git add proto
git commit -m "feat(proto): instructions and events"
```

---

### Task 3: mem 包——提交与内存版 Store

**Files:**
- Create: `mem/mem.go`, `mem/memory.go`
- Test: `mem/memory_test.go`

- [ ] **Step 1: 写失败的测试** `mem/memory_test.go`

```go
package mem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func newTask(id string) *task.TCB {
	return &task.TCB{ID: id, State: task.Ready, Class: "default", Regs: []task.Message{task.Text(task.RoleUser, "q")}, Version: 1}
}

func mustCommit(t *testing.T, s *mem.Memory, c mem.CommitSet) []proto.Event {
	t.Helper()
	evs, err := s.Commit(ctx, c)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	return evs
}

func steer(id, text string) mem.CommitSet {
	return mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: id, Kind: mem.InterruptSteer, Text: text}}}
}

func TestNewTaskThenLoad(t *testing.T) {
	s := mem.NewMemory()
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Regs[0].TextContent() != "q" {
		t.Fatalf("loaded %+v", got)
	}
	if _, err := s.Commit(ctx, mem.CommitSet{Task: newTask("a"), NewTask: true}); !errors.Is(err, mem.ErrExists) {
		t.Fatalf("second create: %v, want ErrExists", err)
	}
	if _, err := s.Load(ctx, "nope"); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("load missing: %v, want ErrNotFound", err)
	}
}

func TestVersionMustAdvanceByOne(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.Version = 3
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc}); !errors.Is(err, mem.ErrVersion) {
		t.Fatalf("got %v, want ErrVersion", err)
	}
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc})
}

func TestStoreKeepsItsOwnCopy(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.Regs[0].Parts[0].Text = "mutated after commit"
	got, _ := s.Load(ctx, "a")
	if got.Regs[0].TextContent() != "q" {
		t.Fatalf("store saw a mutation made after commit")
	}
	got.Regs[0].Parts[0].Text = "mutated after load"
	again, _ := s.Load(ctx, "a")
	if again.Regs[0].TextContent() != "q" {
		t.Fatalf("store saw a mutation made after load")
	}
}

func TestEventsGetSequenceNumbers(t *testing.T) {
	s := mem.NewMemory()
	evs := mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true, Events: []proto.Event{
		proto.NewEvent(proto.EvAccepted, "a", nil),
		proto.NewEvent(proto.EvStep, "a", nil),
	}})
	if evs[0].Seq != 1 || evs[0].TaskSeq != 1 || evs[1].Seq != 2 || evs[1].TaskSeq != 2 {
		t.Fatalf("got %+v", evs)
	}
	evs = mustCommit(t, s, mem.CommitSet{Task: newTask("b"), NewTask: true, Events: []proto.Event{
		proto.NewEvent(proto.EvAccepted, "b", nil),
	}})
	if evs[0].Seq != 3 || evs[0].TaskSeq != 1 {
		t.Fatalf("got %+v", evs)
	}
	check := func(after int64, limit, want int, firstSeq int64) {
		t.Helper()
		got, err := s.Events(ctx, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != want || (want > 0 && got[0].Seq != firstSeq) {
			t.Fatalf("Events(%d,%d) = %+v", after, limit, got)
		}
	}
	check(0, 0, 3, 1)
	check(2, 0, 1, 3)
	check(0, 2, 2, 1)
	check(3, 0, 0, 0)
}

func TestInterruptsAreAppliedWithTheTask(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "one"))
	pending, _ := s.Interrupts(ctx, "a", true)
	if len(pending) != 1 || pending[0].Seq != 1 || pending[0].Text != "one" {
		t.Fatalf("pending %+v", pending)
	}
	tc.Version = 2
	mustCommit(t, s, mem.CommitSet{Task: tc, Applied: []int64{pending[0].Seq}})
	if pending, _ = s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
	if all, _ := s.Interrupts(ctx, "a", false); len(all) != 1 || !all[0].Applied {
		t.Fatalf("all %+v", all)
	}
}

func TestAppliedNeedsATask(t *testing.T) {
	s := mem.NewMemory()
	if _, err := s.Commit(ctx, mem.CommitSet{Applied: []int64{1}}); err == nil {
		t.Fatal("Applied without Task was accepted")
	}
}

func TestInterruptForUnknownOrFinishedTask(t *testing.T) {
	s := mem.NewMemory()
	if _, err := s.Commit(ctx, steer("nope", "x")); !errors.Is(err, mem.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	tc.State, tc.Version = task.Done, 2
	mustCommit(t, s, mem.CommitSet{Task: tc})
	if _, err := s.Commit(ctx, steer("a", "x")); !errors.Is(err, mem.ErrFinished) {
		t.Fatalf("got %v, want ErrFinished", err)
	}
}

func TestTerminalCommitDropsUnappliedInterrupts(t *testing.T) {
	s := mem.NewMemory()
	tc := newTask("a")
	mustCommit(t, s, mem.CommitSet{Task: tc, NewTask: true})
	mustCommit(t, s, steer("a", "late"))
	tc.State, tc.Version = task.Done, 2
	evs := mustCommit(t, s, mem.CommitSet{Task: tc, Events: []proto.Event{proto.NewEvent(proto.EvDone, "a", nil)}})
	if len(evs) != 2 || evs[0].Kind != proto.EvDone || evs[1].Kind != proto.EvSteerDropped {
		t.Fatalf("events %+v", evs)
	}
	if pending, _ := s.Interrupts(ctx, "a", true); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
}

func TestCallsUpsertInIndexOrder(t *testing.T) {
	s := mem.NewMemory()
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{
		{TaskID: "a", CallID: "c-2", Index: 2, Status: mem.CallPlanned},
		{TaskID: "a", CallID: "c-1", Index: 1, Status: mem.CallPlanned},
	}})
	mustCommit(t, s, mem.CommitSet{Calls: []mem.CallRecord{
		{TaskID: "a", CallID: "c-1", Index: 1, Status: mem.CallDone, Result: &task.ToolResult{Content: "x"}},
	}})
	calls, err := s.Calls(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].CallID != "c-1" || calls[0].Status != mem.CallDone ||
		calls[0].Result.Content != "x" || calls[1].Status != mem.CallPlanned {
		t.Fatalf("calls %+v", calls)
	}
}

func TestUnfinished(t *testing.T) {
	s := mem.NewMemory()
	mustCommit(t, s, mem.CommitSet{Task: newTask("a"), NewTask: true})
	done := newTask("b")
	mustCommit(t, s, mem.CommitSet{Task: done, NewTask: true})
	done.State, done.Version = task.Done, 2
	mustCommit(t, s, mem.CommitSet{Task: done})
	got, _ := s.Unfinished(ctx)
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("unfinished %+v", got)
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./mem/`
Expected: 编译失败，`undefined: mem.NewMemory`。

- [ ] **Step 3: 写 `mem/mem.go`**

```go
// Package mem is main memory: the one place task state is committed. Every
// state change in the machine is a CommitSet applied by Store.Commit, and
// nothing a commit describes may happen before Commit returns (design §4).
package mem

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

type CallStatus string

const (
	CallPlanned    CallStatus = "planned"
	CallDispatched CallStatus = "dispatched"
	CallDone       CallStatus = "done"
	CallUnknown    CallStatus = "unknown"
	CallCancelled  CallStatus = "cancelled"
	CallLate       CallStatus = "late"
)

// CallRecord is the write-ahead log entry of one tool call.
type CallRecord struct {
	TaskID  string           `json:"task_id"`
	CallID  string           `json:"call_id"`
	Index   int              `json:"index"` // order of the call within its task
	Tool    string           `json:"tool"`  // full name as the model sees it
	Args    json.RawMessage  `json:"args,omitempty"`
	Status  CallStatus       `json:"status"`
	Attempt int              `json:"attempt"`
	Key     string           `json:"key,omitempty"` // idempotency key: task/call/attempt
	Result  *task.ToolResult `json:"result,omitempty"`
}

const InterruptSteer = "steer"

// Interrupt is an outside input to a task, durable before it is applied.
type Interrupt struct {
	TaskID  string `json:"task_id"`
	Seq     int64  `json:"seq"` // assigned by the store
	Kind    string `json:"kind"`
	Text    string `json:"text,omitempty"`
	Applied bool   `json:"applied"`
}

// CommitSet is one atomic state change.
//
//   - Task, when set, is the new TCB. NewTask requires Version 1 and an
//     unused ID; otherwise Version must be the stored Version + 1.
//   - Calls are upserted by (TaskID, CallID).
//   - NewInterrupts are rejected for unknown (ErrNotFound) or finished
//     (ErrFinished) tasks.
//   - Applied marks interrupts of Task as applied; it requires Task.
//   - When Task is terminal, every interrupt still unapplied is marked applied
//     and a steer_dropped event is added for it, in the same commit.
//   - Events get their Seq and TaskSeq; Commit returns them.
type CommitSet struct {
	Task          *task.TCB
	NewTask       bool
	Calls         []CallRecord
	NewInterrupts []Interrupt
	Applied       []int64
	Events        []proto.Event
}

var (
	ErrVersion  = errors.New("mem: version conflict")
	ErrNotFound = errors.New("mem: task not found")
	ErrFinished = errors.New("mem: task already finished")
	ErrExists   = errors.New("mem: task already exists")
)

type Store interface {
	Commit(ctx context.Context, c CommitSet) ([]proto.Event, error)
	Load(ctx context.Context, id string) (*task.TCB, error)
	Unfinished(ctx context.Context) ([]*task.TCB, error)
	Calls(ctx context.Context, taskID string) ([]CallRecord, error)
	Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error)
	Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error)
}
```

- [ ] **Step 4: 写 `mem/memory.go`**

```go
package mem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Memory is an in-process Store. It is not durable; it exists for phase 1 and
// for tests, and it implements exactly the semantics the SQLite store will.
type Memory struct {
	mu         sync.Mutex
	tasks      map[string]*task.TCB
	calls      map[string][]CallRecord
	interrupts map[string][]Interrupt
	events     []proto.Event
	taskSeq    map[string]int64
	intSeq     int64
}

var _ Store = (*Memory)(nil)

func NewMemory() *Memory {
	return &Memory{
		tasks:      map[string]*task.TCB{},
		calls:      map[string][]CallRecord{},
		interrupts: map[string][]Interrupt{},
		taskSeq:    map[string]int64{},
	}
}

func (m *Memory) Commit(ctx context.Context, c CommitSet) ([]proto.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.validate(c); err != nil {
		return nil, err
	}

	events := append([]proto.Event(nil), c.Events...)
	if c.Task != nil {
		m.tasks[c.Task.ID] = c.Task.Clone()
	}
	for _, r := range c.Calls {
		m.upsertCall(r)
	}
	for _, in := range c.NewInterrupts {
		m.intSeq++
		in.Seq, in.Applied = m.intSeq, false
		m.interrupts[in.TaskID] = append(m.interrupts[in.TaskID], in)
	}
	if c.Task != nil {
		applied := make(map[int64]bool, len(c.Applied))
		for _, s := range c.Applied {
			applied[s] = true
		}
		list := m.interrupts[c.Task.ID]
		for i := range list {
			switch {
			case list[i].Applied:
			case applied[list[i].Seq]:
				list[i].Applied = true
			case c.Task.State.Terminal():
				list[i].Applied = true
				events = append(events, proto.NewEvent(proto.EvSteerDropped, c.Task.ID,
					map[string]any{"seq": list[i].Seq, "text": list[i].Text}))
			}
		}
	}
	for i := range events {
		m.taskSeq[events[i].TaskID]++
		events[i].Seq = int64(len(m.events)) + 1
		events[i].TaskSeq = m.taskSeq[events[i].TaskID]
		m.events = append(m.events, events[i])
	}
	return events, nil
}

// validate checks a commit completely before anything is applied, so that a
// commit applies whole or not at all.
func (m *Memory) validate(c CommitSet) error {
	if len(c.Applied) > 0 && c.Task == nil {
		return errors.New("mem: Applied requires Task")
	}
	if t := c.Task; t != nil {
		old, exists := m.tasks[t.ID]
		switch {
		case c.NewTask && exists:
			return fmt.Errorf("%w: %s", ErrExists, t.ID)
		case c.NewTask && t.Version != 1:
			return fmt.Errorf("%w: new task %s must have version 1, got %d", ErrVersion, t.ID, t.Version)
		case !c.NewTask && !exists:
			return fmt.Errorf("%w: %s", ErrNotFound, t.ID)
		case !c.NewTask && t.Version != old.Version+1:
			return fmt.Errorf("%w: task %s is at %d, commit carries %d", ErrVersion, t.ID, old.Version, t.Version)
		}
	}
	for _, in := range c.NewInterrupts {
		t, ok := m.tasks[in.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, in.TaskID)
		}
		if t.State.Terminal() {
			return fmt.Errorf("%w: %s", ErrFinished, in.TaskID)
		}
	}
	return nil
}

func (m *Memory) upsertCall(r CallRecord) {
	r = cloneCall(r)
	list := m.calls[r.TaskID]
	for i := range list {
		if list[i].CallID == r.CallID {
			list[i] = r
			return
		}
	}
	m.calls[r.TaskID] = append(list, r)
}

func cloneCall(r CallRecord) CallRecord {
	r.Args = append(json.RawMessage(nil), r.Args...)
	if r.Result != nil {
		res := *r.Result
		r.Result = &res
	}
	return r
}

func (m *Memory) Load(ctx context.Context, id string) (*task.TCB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return t.Clone(), nil
}

func (m *Memory) Unfinished(ctx context.Context) ([]*task.TCB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*task.TCB
	for _, t := range m.tasks {
		if !t.State.Terminal() {
			out = append(out, t.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) Calls(ctx context.Context, taskID string) ([]CallRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CallRecord, 0, len(m.calls[taskID]))
	for _, r := range m.calls[taskID] {
		out = append(out, cloneCall(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (m *Memory) Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Interrupt
	for _, in := range m.interrupts[taskID] {
		if unappliedOnly && in.Applied {
			continue
		}
		out = append(out, in)
	}
	return out, nil
}

func (m *Memory) Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if afterSeq < 0 {
		afterSeq = 0
	}
	n := int64(len(m.events))
	if afterSeq >= n {
		return nil, nil
	}
	end := n
	if limit > 0 && afterSeq+int64(limit) < end {
		end = afterSeq + int64(limit)
	}
	return append([]proto.Event(nil), m.events[afterSeq:end]...), nil
}
```

- [ ] **Step 5: 运行，确认通过**

Run: `go test -race ./mem/`
Expected: `ok`

- [ ] **Step 6: 提交**

```bash
git add mem
git commit -m "feat(mem): atomic commit set and in-memory store"
```

---

### Task 4: model 接口与 sim 脚本模型

**Files:**
- Create: `model/model.go`, `sim/sim.go`
- Test: `sim/sim_test.go`

- [ ] **Step 1: 写失败的测试** `sim/sim_test.go`

```go
package sim

import (
	"context"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestHelpers(t *testing.T) {
	req := model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "first"),
		CallTools("a", "b").Message,
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1"}}},
		task.Text(task.RoleUser, "second"),
	}}
	if FirstUser(req) != "first" {
		t.Fatalf("FirstUser = %q", FirstUser(req))
	}
	if got := UserTexts(req); len(got) != 2 || got[1] != "second" {
		t.Fatalf("UserTexts = %v", got)
	}
	if ToolRounds(req) != 1 {
		t.Fatalf("ToolRounds = %d", ToolRounds(req))
	}
	calls := CallTools("a", "b").Message.ToolCalls()
	if len(calls) != 2 || calls[1].Name != "b" || string(calls[1].Args) != `{"n":1}` || calls[1].ProviderCallID != "p-1" {
		t.Fatalf("CallTools = %+v", calls)
	}
}

func TestModelCountsCalls(t *testing.T) {
	m := &Model{Fn: func(model.Request) (model.Response, error) { return Reply("ok"), nil }}
	for range 3 {
		if _, err := m.Call(context.Background(), model.Request{}); err != nil {
			t.Fatal(err)
		}
	}
	if m.Calls() != 3 {
		t.Fatalf("Calls = %d", m.Calls())
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./sim/`
Expected: 编译失败，缺 `model` 包和 `sim` 函数。

- [ ] **Step 3: 写 `model/model.go`**

```go
// Package model is the seam between the machine and a model provider. Every
// call through an Adapter is one instruction on a vCore.
package model

import (
	"context"

	"github.com/liliang-cn/neumann/task"
)

type Request struct {
	System   string
	Tools    []task.ToolSpec
	Messages []task.Message
}

// Response carries the assistant turn in neutral form. Tool calls come back
// with ProviderCallID set and CallID empty; the machine assigns CallID.
type Response struct {
	Message      task.Message
	FinishReason string
	Usage        task.Usage
}

type Adapter interface {
	Provider() string
	Call(ctx context.Context, req Request) (Response, error)
}
```

- [ ] **Step 4: 写 `sim/sim.go`**

```go
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

// Calls is how many times Call was invoked.
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
```

- [ ] **Step 5: 运行，确认通过**

Run: `go test ./sim/ ./model/`
Expected: `ok`（model 包无测试文件）

- [ ] **Step 6: 提交**

```bash
git add model sim
git commit -m "feat(model): adapter seam and scripted sim model"
```

---

### Task 5: 总线与 builtin 设备

**Files:**
- Create: `bus/bus.go`, `bus/builtin/builtin.go`
- Test: `bus/bus_test.go`

- [ ] **Step 1: 写失败的测试** `bus/bus_test.go`

```go
package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func echo(_ context.Context, c bus.DeviceCall) (string, error) { return string(c.Args), nil }

func mustBus(t *testing.T, perDevice int, devs ...bus.Device) *bus.Bus {
	t.Helper()
	b, err := bus.New(ctx, perDevice, devs...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func names(specs []task.ToolSpec) []string {
	var out []string
	for _, s := range specs {
		out = append(out, s.Name)
	}
	return out
}

func TestFullName(t *testing.T) {
	cases := map[[2]string]string{
		{"builtin", "echo"}:           "builtin_echo",
		{"mcp.github", "list issues"}: "mcp_github_list_issues",
		{"a-b", "c"}:                  "a-b_c",
	}
	for in, want := range cases {
		if got := bus.FullName(in[0], in[1]); got != want {
			t.Errorf("FullName(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestDuplicateNamesAreRejected(t *testing.T) {
	d1 := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, echo)
	d2 := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, echo)
	if _, err := bus.New(ctx, 4, d1, d2); err == nil {
		t.Fatal("duplicate tool name was accepted")
	}
}

func TestSpecsAreSortedAndFiltered(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "zeta"}, echo).
		Add(bus.ToolDef{Name: "alpha"}, echo).
		Add(bus.ToolDef{Name: "beta"}, echo)
	b := mustBus(t, 4, d)
	check := func(allow, deny []string, want ...string) {
		t.Helper()
		if got := names(b.Specs(allow, deny)); !slices.Equal(got, want) {
			t.Errorf("Specs(%v,%v) = %v, want %v", allow, deny, got, want)
		}
	}
	check(nil, nil, "dev_alpha", "dev_beta", "dev_zeta")
	check(nil, []string{"dev_b"}, "dev_alpha", "dev_zeta")
	check([]string{"dev_a", "dev_z"}, nil, "dev_alpha", "dev_zeta")
}

func TestPermitted(t *testing.T) {
	cases := []struct {
		allow, deny []string
		want        bool
	}{
		{nil, nil, true},
		{[]string{"dev_"}, nil, true},
		{[]string{"other_"}, nil, false},
		{nil, []string{"dev_echo"}, false},
		{[]string{"dev_"}, []string{"dev_echo"}, false},
	}
	for _, c := range cases {
		if got := bus.Permitted("dev_echo", c.allow, c.deny); got != c.want {
			t.Errorf("Permitted(dev_echo, %v, %v) = %v", c.allow, c.deny, got)
		}
	}
}

func TestCallFailuresBecomeErrorResults(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "boom"}, func(context.Context, bus.DeviceCall) (string, error) {
			return "", errors.New("it broke")
		}).
		Add(bus.ToolDef{Name: "hang", Timeout: 20 * time.Millisecond}, func(c context.Context, _ bus.DeviceCall) (string, error) {
			<-c.Done()
			return "", c.Err()
		})
	b := mustBus(t, 4, d)
	cases := []struct {
		name string
		deny []string
		want string
	}{
		{"dev_nope", nil, "unknown tool"},
		{"dev_boom", []string{"dev_boom"}, "not permitted"},
		{"dev_boom", nil, "it broke"},
		{"dev_hang", nil, "deadline exceeded"},
	}
	for _, c := range cases {
		res := b.Call(ctx, c.name, nil, "", nil, c.deny)
		if !res.IsError || !strings.Contains(res.Content, c.want) {
			t.Errorf("%s: got %+v, want an error containing %q", c.name, res, c.want)
		}
	}
}

func TestCallPassesArgsAndKey(t *testing.T) {
	var got bus.DeviceCall
	d := builtin.New("dev").Add(bus.ToolDef{Name: "rec"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		got = c
		return "ok", nil
	})
	b := mustBus(t, 4, d)
	res := b.Call(ctx, "dev_rec", json.RawMessage(`{"a":1}`), "k-1", nil, nil)
	if res.IsError || res.Content != "ok" {
		t.Fatalf("result %+v", res)
	}
	if got.Tool != "rec" || string(got.Args) != `{"a":1}` || got.Key != "k-1" {
		t.Fatalf("device saw %+v", got)
	}
}

func TestDeviceConcurrencyIsBounded(t *testing.T) {
	var inflight, peak atomic.Int32
	d := builtin.New("dev").Add(bus.ToolDef{Name: "slow"}, func(context.Context, bus.DeviceCall) (string, error) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		return "ok", nil
	})
	b := mustBus(t, 2, d)
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Call(ctx, "dev_slow", nil, "", nil, nil)
		}()
	}
	wg.Wait()
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency %d, want 2", peak.Load())
	}
}

func TestEffectDefaultsToSideEffect(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "w"}, echo).
		Add(bus.ToolDef{Name: "r", Effect: bus.ReadOnly}, echo)
	b := mustBus(t, 4, d)
	if b.Effect("dev_w") != bus.SideEffect || b.Effect("dev_r") != bus.ReadOnly || b.Effect("dev_nope") != bus.SideEffect {
		t.Fatal("wrong effects")
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./bus/...`
Expected: 编译失败，`undefined: bus.New` 等。

- [ ] **Step 3: 写 `bus/bus.go`**

```go
// Package bus is the system bus: every tool the machine can call is a device
// attached here, addressed by a full name, behind a per-device queue.
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/task"
)

// Effect is what calling a tool does to the world; recovery depends on it.
// The zero value is SideEffect: an undeclared tool is assumed to have one.
type Effect int

const (
	SideEffect Effect = iota
	ReadOnly
	Idempotent
)

type ToolDef struct {
	Name        string
	Description string
	Schema      json.RawMessage
	Effect      Effect
	Timeout     time.Duration // 0 = DefaultTimeout
}

// DeviceCall is one call as a device receives it. Key is the idempotency key
// (task/call/attempt); a device may use it to drop a repeat.
type DeviceCall struct {
	Tool string
	Args json.RawMessage
	Key  string
}

type Device interface {
	Name() string
	Tools(ctx context.Context) ([]ToolDef, error)
	Call(ctx context.Context, call DeviceCall) (task.ToolResult, error)
	Close() error
}

const DefaultTimeout = 120 * time.Second

type route struct {
	dev  Device
	tool string
	def  ToolDef
	slot chan struct{} // shared by all tools of one device
}

type Bus struct {
	routes map[string]route
	specs  []task.ToolSpec
	devs   []Device
}

// New attaches devices. perDevice bounds concurrent calls per device; calls
// beyond it wait in that device's queue.
func New(ctx context.Context, perDevice int, devs ...Device) (*Bus, error) {
	if perDevice <= 0 {
		perDevice = 4
	}
	b := &Bus{routes: map[string]route{}, devs: devs}
	for _, d := range devs {
		defs, err := d.Tools(ctx)
		if err != nil {
			return nil, fmt.Errorf("bus: list tools of %s: %w", d.Name(), err)
		}
		slot := make(chan struct{}, perDevice)
		for _, def := range defs {
			name := FullName(d.Name(), def.Name)
			if _, dup := b.routes[name]; dup {
				return nil, fmt.Errorf("bus: duplicate tool name %q", name)
			}
			b.routes[name] = route{dev: d, tool: def.Name, def: def, slot: slot}
			b.specs = append(b.specs, task.ToolSpec{Name: name, Description: def.Description, Schema: def.Schema})
		}
	}
	sort.Slice(b.specs, func(i, j int) bool { return b.specs[i].Name < b.specs[j].Name })
	return b, nil
}

// FullName is the name the model sees: <device>_<tool>, every character
// outside [A-Za-z0-9_-] replaced by '_'.
func FullName(device, tool string) string {
	return sanitize(device) + "_" + sanitize(tool)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '_' || r == '-', 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
			return r
		}
		return '_'
	}, s)
}

// Permitted applies a page table to a full tool name. Allow and Deny are
// prefix lists; an empty Allow permits whatever Deny does not name.
func Permitted(name string, allow, deny []string) bool {
	for _, p := range deny {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	if len(allow) == 0 {
		return true
	}
	for _, p := range allow {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// Specs is the tool list a task with this page table sees, sorted by name so
// that it is byte-identical on every request (it sits in the cached prefix).
func (b *Bus) Specs(allow, deny []string) []task.ToolSpec {
	var out []task.ToolSpec
	for _, s := range b.specs {
		if Permitted(s.Name, allow, deny) {
			out = append(out, s)
		}
	}
	return out
}

// Effect is a tool's declared effect; unknown names are SideEffect.
func (b *Bus) Effect(name string) Effect {
	if r, ok := b.routes[name]; ok {
		return r.def.Effect
	}
	return SideEffect
}

// Call runs one tool call, waiting in the device's queue for a free slot. It
// never returns a Go error: every failure becomes an error result, because
// each call the model made must get exactly one result.
func (b *Bus) Call(ctx context.Context, name string, args json.RawMessage, key string, allow, deny []string) task.ToolResult {
	if !Permitted(name, allow, deny) {
		return task.ToolResult{Content: "tool " + name + " is not permitted for this task", IsError: true}
	}
	r, ok := b.routes[name]
	if !ok {
		return task.ToolResult{Content: "unknown tool " + name, IsError: true}
	}
	select {
	case r.slot <- struct{}{}:
	case <-ctx.Done():
		return task.ToolResult{Content: "cancelled before the tool ran: " + ctx.Err().Error(), IsError: true}
	}
	defer func() { <-r.slot }()

	timeout := r.def.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := r.dev.Call(cctx, DeviceCall{Tool: r.tool, Args: args, Key: key})
	if err != nil {
		return task.ToolResult{Content: err.Error(), IsError: true}
	}
	return res
}

func (b *Bus) Close() error {
	var errs []error
	for _, d := range b.devs {
		errs = append(errs, d.Close())
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 4: 写 `bus/builtin/builtin.go`**

```go
// Package builtin is a device made of Go functions.
package builtin

import (
	"context"
	"fmt"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

// Func implements one tool. A returned error becomes an error result for the
// model, not a failure of the device.
type Func func(ctx context.Context, call bus.DeviceCall) (string, error)

type Device struct {
	name string
	defs []bus.ToolDef
	fns  map[string]Func
}

func New(name string) *Device {
	return &Device{name: name, fns: map[string]Func{}}
}

// Add registers a tool and returns the device, for chaining.
func (d *Device) Add(def bus.ToolDef, fn Func) *Device {
	d.defs = append(d.defs, def)
	d.fns[def.Name] = fn
	return d
}

func (d *Device) Name() string { return d.name }

func (d *Device) Tools(context.Context) ([]bus.ToolDef, error) {
	return append([]bus.ToolDef(nil), d.defs...), nil
}

func (d *Device) Call(ctx context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	fn, ok := d.fns[c.Tool]
	if !ok {
		return task.ToolResult{}, fmt.Errorf("builtin %s: no tool %q", d.name, c.Tool)
	}
	out, err := fn(ctx, c)
	if err != nil {
		return task.ToolResult{Content: err.Error(), IsError: true}, nil
	}
	return task.ToolResult{Content: out}, nil
}

func (d *Device) Close() error { return nil }
```

- [ ] **Step 5: 运行，确认通过**

Run: `go test -race ./bus/...`
Expected: `ok`

- [ ] **Step 6: 提交**

```bash
git add bus
git commit -m "feat(bus): system bus with page table, per-device queue and builtin device"
```

---

### Task 6: 调度器

**Files:**
- Create: `sched/sched.go`
- Test: `sched/sched_test.go`

- [ ] **Step 1: 写失败的测试** `sched/sched_test.go`

```go
package sched_test

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/sched"
	"github.com/liliang-cn/neumann/task"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

var t0 = time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)

func tcb(id string, prio int, at time.Duration) *task.TCB {
	return &task.TCB{ID: id, State: task.Ready, Class: "default", Priority: prio, ReadySince: t0.Add(at)}
}

func next(t *testing.T, s *sched.Sched) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, ok := s.Next(ctx, "default")
	if !ok {
		t.Fatal("Next returned nothing")
	}
	return got.ID
}

func order(t *testing.T, s *sched.Sched, want ...string) {
	t.Helper()
	for _, w := range want {
		if got := next(t, s); got != w {
			t.Fatalf("got %s, want %s", got, w)
		}
	}
}

func TestFIFOWithinPriority(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(5 * time.Second)}, 30*time.Second)
	s.Add(tcb("c", 0, 3*time.Second))
	s.Add(tcb("a", 0, 1*time.Second))
	s.Add(tcb("b", 0, 2*time.Second))
	order(t, s, "a", "b", "c")
}

func TestInteractiveBeforeBackground(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(10 * time.Second)}, 30*time.Second)
	s.Add(tcb("bg", 1, 0))
	s.Add(tcb("fg", 0, 5*time.Second))
	order(t, s, "fg", "bg")
}

func TestAgingPromotesBackground(t *testing.T) {
	s := sched.New(&clock{now: t0.Add(31 * time.Second)}, 30*time.Second)
	s.Add(tcb("bg", 1, 0))
	s.Add(tcb("fg", 0, 30*time.Second))
	order(t, s, "bg", "fg")
}

func TestNextWaitsForWork(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	got := make(chan string, 1)
	go func() {
		if tc, ok := s.Next(context.Background(), "default"); ok {
			got <- tc.ID
		}
	}()
	select {
	case id := <-got:
		t.Fatalf("Next returned %s before any task was added", id)
	case <-time.After(20 * time.Millisecond):
	}
	s.Add(&task.TCB{ID: "x", State: task.Ready, Class: "default", ReadySince: time.Now()})
	select {
	case id := <-got:
		if id != "x" {
			t.Fatalf("got %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not wake up")
	}
}

func TestNextStopsOnCancelAndClose(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	done := make(chan bool, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, ok := s.Next(ctx, "default"); done <- ok }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Next returned a task after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("Next ignored cancel")
	}
	go func() { _, ok := s.Next(context.Background(), "default"); done <- ok }()
	time.Sleep(10 * time.Millisecond)
	s.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Next returned a task after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Next ignored Close")
	}
}

func TestCheckoutOnlyFindsWaitingTasks(t *testing.T) {
	s := sched.New(sched.RealClock{}, 30*time.Second)
	w := &task.TCB{ID: "w", State: task.Waiting, Wait: task.WaitIO, Class: "default"}
	s.Requeue(w)
	if got, ok := s.Checkout("w"); !ok || got != w {
		t.Fatal("waiting task not checked out")
	}
	if _, ok := s.Checkout("w"); ok {
		t.Fatal("checked out twice")
	}
	s.Add(tcb("r", 0, 0))
	if _, ok := s.Checkout("r"); ok {
		t.Fatal("checked out a ready task")
	}
}

func TestClassesAreSeparate(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	o := tcb("o", 0, 0)
	o.Class = "other"
	s.Add(o)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, ok := s.Next(ctx, "default"); ok {
		t.Fatal("default class got a task of another class")
	}
	if got, ok := s.Next(context.Background(), "other"); !ok || got.ID != "o" {
		t.Fatal("other class did not get its task")
	}
}

func TestCountsAndTerminalTasksLeave(t *testing.T) {
	s := sched.New(&clock{now: t0}, 30*time.Second)
	s.Add(tcb("a", 0, 0))
	s.Add(tcb("b", 0, time.Second))
	s.Requeue(&task.TCB{ID: "w", State: task.Waiting, Class: "default"})
	got, _ := s.Next(context.Background(), "default")
	if r, w, o := s.Counts(); r != 1 || w != 1 || o != 1 {
		t.Fatalf("counts %d/%d/%d", r, w, o)
	}
	got.State = task.Done
	s.Requeue(got)
	if r, w, o := s.Counts(); r != 1 || w != 1 || o != 0 {
		t.Fatalf("after done: %d/%d/%d", r, w, o)
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./sched/`
Expected: 编译失败，`undefined: sched.New`。

- [ ] **Step 3: 写 `sched/sched.go`**

```go
// Package sched owns every task that is not on a vCore: the ready queues (one
// per class, interactive before background, FIFO by ReadySince, with aging)
// and the wait set. Ownership of a TCB passes only through Next, Checkout and
// Requeue (design §8).
package sched

import (
	"context"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/task"
)

type Clock interface{ Now() time.Time }

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

const DefaultAging = 30 * time.Second

type Sched struct {
	mu      sync.Mutex
	cond    *sync.Cond
	clock   Clock
	aging   time.Duration
	ready   map[string][]*task.TCB
	waiting map[string]*task.TCB
	out     map[string]bool // held by a vCore or a tool batch
	closed  bool
}

func New(clock Clock, aging time.Duration) *Sched {
	if aging <= 0 {
		aging = DefaultAging
	}
	s := &Sched{
		clock:   clock,
		aging:   aging,
		ready:   map[string][]*task.TCB{},
		waiting: map[string]*task.TCB{},
		out:     map[string]bool{},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Add hands a task the caller owns to the scheduler.
func (s *Sched) Add(t *task.TCB) { s.Requeue(t) }

// Requeue returns a task to the scheduler after its holder committed the
// task's new state. Ready tasks join their class's queue, Waiting and
// Suspended tasks the wait set, terminal tasks are forgotten.
func (s *Sched) Requeue(t *task.TCB) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.out, t.ID)
	switch t.State {
	case task.Ready:
		s.ready[t.Class] = append(s.ready[t.Class], t)
	case task.Waiting, task.Suspended:
		s.waiting[t.ID] = t
	}
	s.cond.Broadcast()
}

// Next blocks until a task of class is ready and hands it to the caller. It
// returns false once the scheduler is closed or ctx ends.
func (s *Sched) Next(ctx context.Context, class string) (*task.TCB, bool) {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed || ctx.Err() != nil {
			return nil, false
		}
		if t := s.pickLocked(class); t != nil {
			return t, true
		}
		s.cond.Wait()
	}
}

func (s *Sched) pickLocked(class string) *task.TCB {
	q := s.ready[class]
	if len(q) == 0 {
		return nil
	}
	now := s.clock.Now()
	best := 0
	for i := 1; i < len(q); i++ {
		if s.before(q[i], q[best], now) {
			best = i
		}
	}
	t := q[best]
	s.ready[class] = append(q[:best], q[best+1:]...)
	s.out[t.ID] = true
	return t
}

// effective is the priority used for ordering: a background task that has
// been ready for longer than the aging threshold competes as interactive.
func (s *Sched) effective(t *task.TCB, now time.Time) int {
	if t.Priority > 0 && now.Sub(t.ReadySince) >= s.aging {
		return 0
	}
	return t.Priority
}

func (s *Sched) before(a, b *task.TCB, now time.Time) bool {
	ea, eb := s.effective(a, now), s.effective(b, now)
	if ea != eb {
		return ea < eb
	}
	return a.ReadySince.Before(b.ReadySince)
}

// Checkout takes a waiting task out of the wait set so that its caller can
// commit a transition for it (for example, its tools finished).
func (s *Sched) Checkout(id string) (*task.TCB, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.waiting[id]
	if !ok {
		return nil, false
	}
	delete(s.waiting, id)
	s.out[id] = true
	return t, true
}

// Forget drops a held task whose commit failed: it is no longer scheduled.
func (s *Sched) Forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.out, id)
}

// Close makes every Next return false. Requeue keeps working, so in-flight
// work can still hand its tasks back.
func (s *Sched) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Broadcast()
}

// Counts reports tasks in ready queues, in the wait set, and held elsewhere.
func (s *Sched) Counts() (ready, waiting, held int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.ready {
		ready += len(q)
	}
	return ready, len(s.waiting), len(s.out)
}
```

- [ ] **Step 4: 运行，确认通过**

Run: `go test -race ./sched/`
Expected: `ok`

- [ ] **Step 5: 提交**

```bash
git add sched
git commit -m "feat(sched): ready queues with aging, wait set and ownership handover"
```

---

### Task 7: vCore 循环与指令执行

**Files:**
- Create: `core/core.go`, `core/model.go`, `core/tools.go`
- Test: `core/core_test.go`

- [ ] **Step 1: 写失败的测试** `core/core_test.go`

```go
package core

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sched"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

type modelFn = func(model.Request) (model.Response, error)

func newTestEnv(t *testing.T, fn modelFn, devs ...bus.Device) (*Env, *mem.Memory, *sim.Model) {
	t.Helper()
	store := mem.NewMemory()
	b, err := bus.New(bg, 4, devs...)
	if err != nil {
		t.Fatal(err)
	}
	sm := &sim.Model{Fn: fn}
	e := &Env{
		Store: store,
		Sched: sched.New(sched.RealClock{}, sched.DefaultAging),
		Bus:   b,
		Model: sm,
		Clock: sched.RealClock{},
		Logf:  t.Logf,
	}
	e.Commit = func(ctx context.Context, c mem.CommitSet) error {
		_, err := store.Commit(ctx, c)
		return err
	}
	return e, store, sm
}

func newTask(t *testing.T, e *Env, store *mem.Memory, input string) *task.TCB {
	t.Helper()
	tc := &task.TCB{
		ID: "t1", State: task.Ready, PC: task.StepModel, Class: "default",
		ReadySince: time.Now(), Tools: e.Bus.Specs(nil, nil),
		Regs: []task.Message{task.Text(task.RoleUser, input)}, Version: 1,
	}
	if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
		t.Fatal(err)
	}
	return tc
}

// take fetches the task back from the scheduler, as a vCore would.
func take(t *testing.T, e *Env) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	tc, ok := e.Sched.Next(ctx, "default")
	if !ok {
		t.Fatal("no ready task")
	}
	return tc
}

func load(t *testing.T, store *mem.Memory) *task.TCB {
	t.Helper()
	tc, err := store.Load(bg, "t1")
	if err != nil {
		t.Fatal(err)
	}
	return tc
}

func kinds(store *mem.Memory) string {
	evs, _ := store.Events(bg, 0, 0)
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Kind)
	}
	return strings.Join(out, ",")
}

func echoDev() bus.Device {
	return builtin.New("dev").Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		return string(c.Args), nil
	})
}

func TestModelStepAnswers(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) { return sim.Reply("hello"), nil })
	e.step(bg, newTask(t, e, store, "hi"), 0)
	got := load(t, store)
	if got.State != task.Done || got.StopReason != "end_turn" || got.LastAnswer() != "hello" || got.Version != 2 {
		t.Fatalf("task %+v", got)
	}
	if k := kinds(store); k != "step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
}

func TestModelStepPlansToolCalls(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_echo", "dev_echo"), nil
	}, echoDev())
	e.step(bg, newTask(t, e, store, "go"), 0)
	got := load(t, store)
	if got.State != task.Ready || got.PC != task.StepTools {
		t.Fatalf("task %s/%s", got.State, got.PC)
	}
	calls := got.Regs[1].ToolCalls()
	if len(calls) != 2 || calls[0].CallID != "c-1" || calls[0].ProviderCallID != "p-0" || calls[1].CallID != "c-2" {
		t.Fatalf("calls %+v", calls)
	}
	recs, _ := store.Calls(bg, "t1")
	if len(recs) != 2 || recs[0].Status != mem.CallPlanned || recs[1].Index != 2 {
		t.Fatalf("records %+v", recs)
	}
}

func TestToolsAreCommittedBeforeDispatch(t *testing.T) {
	var store *mem.Memory
	var mu sync.Mutex
	var seen []mem.CallStatus
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "check"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
		parts := strings.Split(c.Key, "/") // task/call/attempt
		recs, err := store.Calls(ctx, parts[0])
		if err != nil {
			return "", err
		}
		for _, r := range recs {
			if r.CallID == parts[1] {
				mu.Lock()
				seen = append(seen, r.Status)
				mu.Unlock()
			}
		}
		return "checked", nil
	})
	e, s, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_check", "dev_check"), nil
		}
		return sim.Reply("done"), nil
	}, dev)
	store = s
	e.step(bg, newTask(t, e, s, "go"), 0) // model: plan
	e.step(bg, take(t, e), 0)             // tools: commit dispatched, then dispatch
	e.WaitBatches()

	got := load(t, s)
	if got.State != task.Ready || got.PC != task.StepModel {
		t.Fatalf("after tools: %s/%s", got.State, got.PC)
	}
	last := got.Regs[len(got.Regs)-1]
	if last.Role != task.RoleTool || len(last.Parts) != 2 || last.Parts[0].CallID != "c-1" || last.Parts[0].Text != "checked" {
		t.Fatalf("tool message %+v", last)
	}
	if !slices.Equal(seen, []mem.CallStatus{mem.CallDispatched, mem.CallDispatched}) {
		t.Fatalf("device saw statuses %v, want dispatched twice", seen)
	}
	recs, _ := s.Calls(bg, "t1")
	if recs[0].Status != mem.CallDone || recs[0].Attempt != 1 || recs[0].Key != "t1/c-1/1" {
		t.Fatalf("record %+v", recs[0])
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, s); got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("final %+v", got)
	}
}

func TestSteerIsAppliedBeforeTheModelCall(t *testing.T) {
	var saw []string
	e, store, _ := newTestEnv(t, func(req model.Request) (model.Response, error) {
		saw = sim.UserTexts(req)
		return sim.Reply("ok"), nil
	})
	tc := newTask(t, e, store, "q")
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: "also this"}}}); err != nil {
		t.Fatal(err)
	}
	e.step(bg, tc, 0)
	if !slices.Equal(saw, []string{"q", "also this"}) {
		t.Fatalf("model saw %v", saw)
	}
	if pending, _ := store.Interrupts(bg, "t1", true); len(pending) != 0 {
		t.Fatalf("still pending %+v", pending)
	}
}

func TestSteerDuringFinalCallContinuesTheTask(t *testing.T) {
	var store *mem.Memory
	calls := 0
	e, s, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		calls++
		if calls == 1 {
			if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptSteer, Text: "one more"}}}); err != nil {
				return model.Response{}, err
			}
			return sim.Reply("first"), nil
		}
		return sim.Reply("second"), nil
	})
	store = s
	e.step(bg, newTask(t, e, s, "q"), 0)
	got := load(t, s)
	if got.State != task.Ready || got.PC != task.StepModel || len(got.Regs) != 3 ||
		got.Regs[1].TextContent() != "first" || got.Regs[2].TextContent() != "one more" {
		t.Fatalf("after first call: %+v", got)
	}
	e.step(bg, take(t, e), 0)
	if got := load(t, s); got.State != task.Done || got.LastAnswer() != "second" {
		t.Fatalf("final %+v", got)
	}
}

func TestBudgetEndsTheTask(t *testing.T) {
	e, store, sm := newTestEnv(t, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_echo"), nil
	}, echoDev())
	tc := newTask(t, e, store, "loop")
	tc.Budget.MaxSteps = 2
	for range 10 {
		e.step(bg, tc, 0)
		if load(t, store).State.Terminal() {
			break
		}
		e.WaitBatches()
		tc = take(t, e)
	}
	got := load(t, store)
	if got.State != task.Done || got.StopReason != "budget" || sm.Calls() != 2 {
		t.Fatalf("state %s reason %q model calls %d", got.State, got.StopReason, sm.Calls())
	}
}

func TestModelErrorFailsTheTask(t *testing.T) {
	e, store, _ := newTestEnv(t, func(model.Request) (model.Response, error) {
		return model.Response{}, errors.New("provider down")
	})
	e.step(bg, newTask(t, e, store, "q"), 0)
	got := load(t, store)
	if got.State != task.Failed || got.StopReason != "model: provider down" {
		t.Fatalf("task %+v", got)
	}
	if k := kinds(store); k != "step,failed" {
		t.Fatalf("events %s", k)
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./core/`
Expected: 编译失败，`undefined: Env`。

- [ ] **Step 3: 写 `core/core.go`**

```go
// Package core runs vCores: the loop that fetches a ready task, executes one
// instruction, commits, and only then acts on what it committed (design §5).
package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sched"
	"github.com/liliang-cn/neumann/task"
)

// Env is what every vCore shares. The machine builds one.
type Env struct {
	Store mem.Store
	Sched *sched.Sched
	Bus   *bus.Bus
	Model model.Adapter
	Clock sched.Clock

	// Commit is the only write path: commit to the store and, on success, let
	// subscribers see the events. Nothing a commit describes may happen before
	// Commit returns nil.
	Commit func(ctx context.Context, c mem.CommitSet) error
	Logf   func(format string, args ...any)

	batches sync.WaitGroup
}

type stepData struct {
	PC   string `json:"pc"`
	Core int    `json:"core"`
}

// RunVCore is one vCore. It returns when the scheduler closes or ctx ends,
// and only ever between two instructions.
func (e *Env) RunVCore(ctx context.Context, class string, core int) {
	for {
		t, ok := e.Sched.Next(ctx, class)
		if !ok {
			return
		}
		e.step(ctx, t, core)
	}
}

// WaitBatches waits for every dispatched tool batch to hand its task back.
// Call it only after every vCore has returned.
func (e *Env) WaitBatches() { e.batches.Wait() }

func (e *Env) step(ctx context.Context, t *task.TCB, core int) {
	var (
		c     mem.CommitSet
		batch []mem.CallRecord
		err   error
	)
	switch t.PC {
	case task.StepModel:
		c, err = e.execModel(ctx, t, core)
	case task.StepTools:
		c, batch, err = e.execTools(ctx, t, core)
	default:
		err = fmt.Errorf("instruction %s is not implemented", t.PC)
	}
	if err != nil {
		c, batch = e.fail(t, mem.CommitSet{}, err.Error()), nil
	}
	if err := e.Commit(ctx, c); err != nil {
		e.Logf("neumann: commit for task %s failed; it is no longer scheduled: %v", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	id := t.ID
	allow := append([]string(nil), t.Allow...)
	deny := append([]string(nil), t.Deny...)
	e.Sched.Requeue(t) // t now belongs to the scheduler
	if len(batch) > 0 {
		e.dispatch(id, allow, deny, batch)
	}
}

// ready makes t runnable at its next instruction.
func (e *Env) ready(t *task.TCB, pc task.PC) {
	t.State = task.Ready
	t.Wait = task.WaitNone
	t.PC = pc
	t.ReadySince = e.Clock.Now()
	t.Version++
}

func (e *Env) finish(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Done
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events,
		proto.NewEvent(proto.EvAnswer, t.ID, map[string]string{"text": t.LastAnswer()}),
		proto.NewEvent(proto.EvDone, t.ID, map[string]string{"stop_reason": reason}))
	return c
}

func (e *Env) fail(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Failed
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID, map[string]string{"error": reason}))
	return c
}
```

- [ ] **Step 4: 写 `core/model.go`**

```go
package core

import (
	"context"
	"fmt"
	"slices"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execModel is one model instruction: apply pending steers, call the model
// once, and describe the result as a commit set.
func (e *Env) execModel(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, fmt.Errorf("read interrupts: %w", err)
	}
	c := mem.CommitSet{Events: []proto.Event{
		proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepModel.String(), Core: core}),
	}}
	if t.Budget.MaxSteps > 0 && t.Used.Steps >= t.Budget.MaxSteps {
		return e.finish(t, c, "budget"), nil
	}
	c.Applied = applySteers(t, pending)

	resp, err := e.Model.Call(ctx, model.Request{System: t.System, Tools: t.Tools, Messages: t.Regs})
	t.Used.Steps++
	if err != nil {
		return e.fail(t, c, "model: "+err.Error()), nil
	}
	t.Used.Add(resp.Usage)
	c.Events = append(c.Events, proto.NewEvent(proto.EvUsage, t.ID, resp.Usage))

	msg := resp.Message
	msg.Role = task.RoleAssistant
	if len(msg.ToolCalls()) > 0 {
		n := t.CallCount()
		for i := range msg.Parts {
			p := &msg.Parts[i]
			if p.Kind != task.PartToolCall {
				continue
			}
			n++
			p.CallID = fmt.Sprintf("c-%d", n)
			c.Calls = append(c.Calls, mem.CallRecord{
				TaskID: t.ID, CallID: p.CallID, Index: n, Tool: p.Name, Args: p.Args, Status: mem.CallPlanned,
			})
			c.Events = append(c.Events, proto.NewEvent(proto.EvToolCall, t.ID,
				map[string]any{"call_id": p.CallID, "tool": p.Name, "args": p.Args}))
		}
		t.Regs = append(t.Regs, msg)
		e.ready(t, task.StepTools)
		c.Task = t
		return c, nil
	}

	t.Regs = append(t.Regs, msg)
	// A steer committed while the model was answering turns this answer into
	// a turn: the task goes on instead of finishing over it.
	late, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, fmt.Errorf("read interrupts: %w", err)
	}
	if late = withoutApplied(late, c.Applied); len(late) > 0 {
		c.Applied = append(c.Applied, applySteers(t, late)...)
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil
	}
	return e.finish(t, c, "end_turn"), nil
}

// applySteers appends each pending steer as a user message and returns the
// interrupt sequence numbers it consumed.
func applySteers(t *task.TCB, pending []mem.Interrupt) []int64 {
	var seqs []int64
	for _, in := range pending {
		if in.Kind == mem.InterruptSteer {
			t.Regs = append(t.Regs, task.Text(task.RoleUser, in.Text))
		}
		seqs = append(seqs, in.Seq)
	}
	return seqs
}

func withoutApplied(list []mem.Interrupt, applied []int64) []mem.Interrupt {
	var out []mem.Interrupt
	for _, in := range list {
		if !slices.Contains(applied, in.Seq) {
			out = append(out, in)
		}
	}
	return out
}
```

- [ ] **Step 5: 写 `core/tools.go`**

```go
package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execTools commits the planned calls as dispatched and the task as waiting.
// The caller dispatches the returned batch only after that commit succeeds.
func (e *Env) execTools(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []mem.CallRecord, error) {
	recs, err := e.Store.Calls(ctx, t.ID)
	if err != nil {
		return mem.CommitSet{}, nil, fmt.Errorf("read calls: %w", err)
	}
	c := mem.CommitSet{Events: []proto.Event{
		proto.NewEvent(proto.EvStep, t.ID, stepData{PC: task.StepTools.String(), Core: core}),
	}}
	var batch []mem.CallRecord
	for _, r := range recs {
		if r.Status != mem.CallPlanned {
			continue
		}
		r.Status = mem.CallDispatched
		r.Attempt++
		r.Key = fmt.Sprintf("%s/%s/%d", r.TaskID, r.CallID, r.Attempt)
		batch = append(batch, r)
	}
	if len(batch) == 0 {
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil, nil
	}
	c.Calls = batch
	t.State = task.Waiting
	t.Wait = task.WaitIO
	t.Version++
	c.Task = t
	return c, batch, nil
}

// dispatch runs a committed batch off the vCore. Each result is committed as
// it arrives; when all are in, the task is checked out of the wait set, given
// its tool message, committed as ready, and requeued.
func (e *Env) dispatch(taskID string, allow, deny []string, batch []mem.CallRecord) {
	e.batches.Add(1)
	go func() {
		defer e.batches.Done()
		ctx := context.Background() // phase 3 replaces this with the task's cancel context
		results := make([]task.ToolResult, len(batch))
		var wg sync.WaitGroup
		for i, r := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := e.Bus.Call(ctx, r.Tool, r.Args, r.Key, allow, deny)
				results[i] = res
				r.Status = mem.CallDone
				r.Result = &res
				ev := proto.NewEvent(proto.EvToolResult, taskID, map[string]any{"call_id": r.CallID, "is_error": res.IsError})
				if err := e.Commit(ctx, mem.CommitSet{Calls: []mem.CallRecord{r}, Events: []proto.Event{ev}}); err != nil {
					e.Logf("neumann: commit of result %s/%s failed: %v", taskID, r.CallID, err)
				}
			}()
		}
		wg.Wait()
		e.complete(ctx, taskID, batch, results)
	}()
}

func (e *Env) complete(ctx context.Context, taskID string, batch []mem.CallRecord, results []task.ToolResult) {
	t, ok := e.Sched.Checkout(taskID)
	if !ok {
		e.Logf("neumann: task %s was not waiting when its tools finished", taskID)
		return
	}
	msg := task.Message{Role: task.RoleTool}
	for i, r := range batch {
		msg.Parts = append(msg.Parts, task.Part{
			Kind: task.PartToolResult, CallID: r.CallID, Text: results[i].Content, IsError: results[i].IsError,
		})
	}
	t.Regs = append(t.Regs, msg)
	e.ready(t, task.StepModel)
	if err := e.Commit(ctx, mem.CommitSet{Task: t}); err != nil {
		e.Logf("neumann: commit after tools for task %s failed; it is no longer scheduled: %v", taskID, err)
		e.Sched.Forget(taskID)
		return
	}
	e.Sched.Requeue(t)
}
```

- [ ] **Step 6: 运行，确认通过**

Run: `go test -race ./core/`
Expected: `ok`

- [ ] **Step 7: 提交**

```bash
git add core
git commit -m "feat(core): vCore loop, model and tool instructions, commit before dispatch"
```

---

### Task 8: Machine 与公开 API

**Files:**
- Create: `neumann.go`, `hub.go`
- Test: `neumann_test.go`

- [ ] **Step 1: 写失败的测试** `neumann_test.go`

```go
package neumann_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var bg = context.Background()

type modelFn = func(model.Request) (model.Response, error)

func start(t *testing.T, cfg neumann.Config, fn modelFn) (*neumann.Machine, *sim.Model) {
	t.Helper()
	sm := &sim.Model{Fn: fn}
	cfg.Model = sm
	if cfg.Logf == nil {
		cfg.Logf = t.Logf
	}
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		if err := m.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
	return m, sm
}

func submit(t *testing.T, m *neumann.Machine, in proto.Instruction) string {
	t.Helper()
	in.Kind = proto.KindSubmit
	id, err := m.Submit(bg, in)
	if err != nil {
		t.Fatalf("submit %q: %v", in.Input, err)
	}
	return id
}

func waitDone(t *testing.T, m *neumann.Machine, id string) *task.TCB {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	for ev := range m.Events(ctx, 0) {
		if ev.TaskID == id && ev.Terminal() {
			tc, err := m.Load(bg, id)
			if err != nil {
				t.Fatalf("load %s: %v", id, err)
			}
			return tc
		}
	}
	t.Fatalf("task %s did not finish within 5s", id)
	return nil
}

func eventsOf(t *testing.T, m *neumann.Machine, id string) []proto.Event {
	t.Helper()
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []proto.Event
	for _, ev := range all {
		if ev.TaskID == id {
			out = append(out, ev)
		}
	}
	return out
}

func echoDev() bus.Device {
	return builtin.New("dev").Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		return string(c.Args), nil
	})
}

// gateDev is a device whose "slow" tool reports on entered when a call
// starts and blocks until gate is closed.
func gateDev(entered chan<- string, gate <-chan struct{}) bus.Device {
	return builtin.New("dev").
		Add(bus.ToolDef{Name: "echo"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			return string(c.Args), nil
		}).
		Add(bus.ToolDef{Name: "slow"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
			entered <- c.Key
			select {
			case <-gate:
				return "slow done", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
}

func newGate() (chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch) }) }
}

func TestSubmitAnswers(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("hello"), nil })
	id := submit(t, m, proto.Instruction{Input: "hi"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "hello" {
		t.Fatalf("task %+v", got)
	}
	var kinds []string
	for _, ev := range eventsOf(t, m, id) {
		kinds = append(kinds, ev.Kind)
	}
	if k := strings.Join(kinds, ","); k != "accepted,step,usage,answer,done" {
		t.Fatalf("events %s", k)
	}
}

func TestSubmitValidation(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	cases := []proto.Instruction{
		{Kind: proto.KindSubmit},
		{Kind: proto.KindSteer, Input: "x"},
		{Kind: proto.KindSubmit, Input: "x", Class: "nope"},
	}
	for _, in := range cases {
		if _, err := m.Submit(bg, in); err == nil {
			t.Errorf("Submit(%+v) succeeded", in)
		}
	}
}

func TestAdmissionIsExact(t *testing.T) { // design §18 invariant 20
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Cores: 1, MaxQueued: 4}, func(model.Request) (model.Response, error) {
		<-gate
		return sim.Reply("ok"), nil
	})
	var accepted, busy atomic.Int32
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: fmt.Sprintf("t-%d", i)})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, neumann.ErrBusy):
				var ce *neumann.CapacityError
				if !errors.As(err, &ce) || ce.Limit != 4 {
					t.Errorf("busy error without capacity details: %v", err)
				}
				busy.Add(1)
			default:
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 4 || busy.Load() != 28 {
		t.Fatalf("accepted %d busy %d, want 4 and 28", accepted.Load(), busy.Load())
	}
}

func TestSteerOnFinishedOrUnknownTask(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	id := submit(t, m, proto.Instruction{Input: "q"})
	waitDone(t, m, id)
	if err := m.Steer(bg, id, "too late"); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("steer after done: %v", err)
	}
	if err := m.Steer(bg, "no-such-task", "x"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("steer unknown: %v", err)
	}
}

func TestEventsReplayFromAnySeq(t *testing.T) {
	m, _ := start(t, neumann.Config{}, func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil })
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "q"}))
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for from := range int64(len(all)) {
		ctx, cancel := context.WithTimeout(bg, time.Second)
		var got []proto.Event
		for ev := range m.Events(ctx, from) {
			got = append(got, ev)
			if len(got) == len(all)-int(from) {
				break
			}
		}
		cancel()
		if len(got) != len(all)-int(from) || got[0].Seq != from+1 {
			t.Fatalf("from %d: got %d events starting at %d", from, len(got), got[0].Seq)
		}
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./ -run 'TestSubmit|TestAdmission|TestSteerOn|TestEventsReplay'`
Expected: 编译失败，`undefined: neumann.New`。

- [ ] **Step 3: 获取依赖**

```bash
go get github.com/google/uuid@v1.6.0
```

- [ ] **Step 4: 写 `hub.go`**

```go
package neumann

import "sync"

// hub wakes event subscribers after a commit. A subscriber takes the current
// channel before reading the log, so a commit that lands in between still
// wakes it: the producer never blocks and no wake-up is lost.
type hub struct {
	mu     sync.Mutex
	ch     chan struct{}
	closed bool
}

func newHub() *hub { return &hub{ch: make(chan struct{})} }

func (h *hub) notify() {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.ch)
	h.ch = make(chan struct{})
}

func (h *hub) wait() (<-chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ch, h.closed
}

func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	close(h.ch)
	h.ch = make(chan struct{})
}
```

- [ ] **Step 5: 写 `neumann.go`**

```go
// Package neumann is a resident agent runtime organised like a von Neumann
// machine: vCores fetch, execute and commit one instruction at a time; tools
// sit on a bus and run as asynchronous I/O; every state change is committed
// before anything acts on it. See docs/design.md.
package neumann

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/core"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sched"
	"github.com/liliang-cn/neumann/task"
)

const (
	DefaultClass     = "default"
	DefaultCores     = 4
	DefaultMaxQueued = 1000
	DefaultPerDevice = 4
)

// Config of a phase-1 machine: one execution group with a fixed vCore count.
type Config struct {
	Model        model.Adapter
	Devices      []bus.Device
	Store        mem.Store // nil: in-memory, not durable
	Cores        int
	Class        string
	System       string
	MaxQueued    int // unfinished tasks admitted at once
	MaxPerDevice int // concurrent calls per device
	AgingAfter   time.Duration
	Clock        sched.Clock
	Logf         func(format string, args ...any)
}

var (
	ErrBusy     = errors.New("neumann: at capacity")
	ErrFinished = mem.ErrFinished
	ErrNotFound = mem.ErrNotFound
)

// CapacityError is returned by Submit when admission would exceed MaxQueued.
// errors.Is(err, ErrBusy) is true for it.
type CapacityError struct{ Count, Limit int }

func (e *CapacityError) Error() string {
	return fmt.Sprintf("neumann: at capacity: %d unfinished tasks, limit %d", e.Count, e.Limit)
}

func (e *CapacityError) Is(target error) bool { return target == ErrBusy }

type Machine struct {
	cfg   Config
	store mem.Store
	sched *sched.Sched
	bus   *bus.Bus
	env   *core.Env
	hub   *hub

	admit  sync.Mutex
	active int // unfinished tasks

	life    sync.Mutex
	started bool
	cancel  context.CancelFunc
	cores   sync.WaitGroup
}

func New(cfg Config) (*Machine, error) {
	if cfg.Model == nil {
		return nil, errors.New("neumann: Config.Model is required")
	}
	if cfg.Store == nil {
		cfg.Store = mem.NewMemory()
	}
	if cfg.Cores <= 0 {
		cfg.Cores = DefaultCores
	}
	if cfg.Class == "" {
		cfg.Class = DefaultClass
	}
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = DefaultMaxQueued
	}
	if cfg.MaxPerDevice <= 0 {
		cfg.MaxPerDevice = DefaultPerDevice
	}
	if cfg.AgingAfter <= 0 {
		cfg.AgingAfter = sched.DefaultAging
	}
	if cfg.Clock == nil {
		cfg.Clock = sched.RealClock{}
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	b, err := bus.New(context.Background(), cfg.MaxPerDevice, cfg.Devices...)
	if err != nil {
		return nil, err
	}
	m := &Machine{
		cfg:   cfg,
		store: cfg.Store,
		sched: sched.New(cfg.Clock, cfg.AgingAfter),
		bus:   b,
		hub:   newHub(),
	}
	m.env = &core.Env{
		Store: m.store, Sched: m.sched, Bus: b, Model: cfg.Model, Clock: cfg.Clock,
		Commit: m.commit, Logf: cfg.Logf,
	}
	return m, nil
}

// commit is the machine's single write path for vCores and tool batches.
func (m *Machine) commit(ctx context.Context, c mem.CommitSet) error {
	evs, err := m.store.Commit(ctx, c)
	if err != nil {
		return err
	}
	ended := 0
	for _, ev := range evs {
		if ev.Terminal() {
			ended++
		}
	}
	if ended > 0 {
		m.admit.Lock()
		m.active -= ended
		m.admit.Unlock()
	}
	m.hub.notify()
	return nil
}

// Start launches the vCores. The machine runs until Stop or until ctx ends.
// Phase 1 keeps nothing across processes, so there is nothing to recover.
func (m *Machine) Start(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	if m.started {
		return errors.New("neumann: already started")
	}
	m.started = true
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	for i := range m.cfg.Cores {
		m.cores.Add(1)
		go func() {
			defer m.cores.Done()
			m.env.RunVCore(runCtx, m.cfg.Class, i)
		}()
	}
	return nil
}

// Stop stops fetching, waits for in-flight instructions and tool batches to
// commit, and closes the devices. If ctx ends first it cancels what is left.
func (m *Machine) Stop(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	if !m.started {
		return m.bus.Close()
	}
	m.sched.Close()
	done := make(chan struct{})
	go func() {
		m.cores.Wait()
		m.env.WaitBatches()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	m.cancel()
	m.hub.close()
	return errors.Join(err, m.bus.Close())
}

// Submit admits a task. It returns only after the task is committed, so a
// returned id is a task that exists (design §4.3).
func (m *Machine) Submit(ctx context.Context, in proto.Instruction) (string, error) {
	if in.Kind != "" && in.Kind != proto.KindSubmit {
		return "", fmt.Errorf("neumann: Submit got an instruction of kind %q", in.Kind)
	}
	if strings.TrimSpace(in.Input) == "" {
		return "", errors.New("neumann: submit needs input")
	}
	class := in.Class
	if class == "" {
		class = m.cfg.Class
	}
	if class != m.cfg.Class {
		return "", fmt.Errorf("neumann: unknown class %q", class)
	}
	id := in.TaskID
	if id == "" {
		id = uuid.NewString()
	}
	t := &task.TCB{
		ID:         id,
		State:      task.Ready,
		PC:         task.StepModel,
		Class:      class,
		Priority:   in.Priority,
		ReadySince: m.cfg.Clock.Now(),
		System:     m.cfg.System,
		Tools:      m.bus.Specs(in.Allow, in.Deny),
		Regs:       []task.Message{task.Text(task.RoleUser, in.Input)},
		Allow:      in.Allow,
		Deny:       in.Deny,
		Version:    1,
	}
	if in.Budget != nil {
		t.Budget = *in.Budget
	}

	// Admission is decided, committed and recorded under one lock, so
	// concurrent submits cannot all slip past the limit together.
	m.admit.Lock()
	defer m.admit.Unlock()
	if m.active >= m.cfg.MaxQueued {
		return "", &CapacityError{Count: m.active, Limit: m.cfg.MaxQueued}
	}
	accepted := proto.NewEvent(proto.EvAccepted, id, map[string]any{"class": class, "priority": in.Priority})
	if _, err := m.store.Commit(ctx, mem.CommitSet{Task: t, NewTask: true, Events: []proto.Event{accepted}}); err != nil {
		return "", err
	}
	m.active++
	m.sched.Add(t)
	m.hub.notify()
	return id, nil
}

// Steer queues a message for a task. It returns nil only after the steer is
// committed; it is then either delivered before the task's next model call or
// reported by a steer_dropped event.
func (m *Machine) Steer(ctx context.Context, taskID, text string) error {
	_, err := m.store.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{
		{TaskID: taskID, Kind: mem.InterruptSteer, Text: text},
	}})
	if err == nil {
		m.hub.notify()
	}
	return err
}

// Events streams committed events with Seq > fromSeq, in order. The stream
// ends when ctx ends, or after the machine stops and the subscriber has read
// everything committed. A slow subscriber never blocks the machine and never
// misses an event: it reads the log at its own pace.
func (m *Machine) Events(ctx context.Context, fromSeq int64) <-chan proto.Event {
	out := make(chan proto.Event, 64)
	go func() {
		defer close(out)
		after := fromSeq
		for {
			wake, closed := m.hub.wait()
			evs, err := m.store.Events(ctx, after, 256)
			if err != nil {
				if ctx.Err() == nil {
					m.cfg.Logf("neumann: read events: %v", err)
				}
				return
			}
			for _, ev := range evs {
				select {
				case out <- ev:
					after = ev.Seq
				case <-ctx.Done():
					return
				}
			}
			if len(evs) > 0 {
				continue
			}
			if closed {
				return
			}
			select {
			case <-wake:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// EventLog reads committed events with Seq > afterSeq; limit 0 means all.
func (m *Machine) EventLog(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	return m.store.Events(ctx, afterSeq, limit)
}

// Load returns the committed state of a task.
func (m *Machine) Load(ctx context.Context, id string) (*task.TCB, error) {
	return m.store.Load(ctx, id)
}

// Calls returns a task's tool call records.
func (m *Machine) Calls(ctx context.Context, id string) ([]mem.CallRecord, error) {
	return m.store.Calls(ctx, id)
}

type Status struct {
	Cores      int `json:"cores"`
	Ready      int `json:"ready"`      // in ready queues
	Waiting    int `json:"waiting"`    // in the wait set
	Held       int `json:"held"`       // on a vCore or being completed by a tool batch
	Unfinished int `json:"unfinished"` // admitted and not yet terminal
}

// Status is a cheap snapshot: no model call, no store read.
func (m *Machine) Status() Status {
	r, w, h := m.sched.Counts()
	m.admit.Lock()
	u := m.active
	m.admit.Unlock()
	return Status{Cores: m.cfg.Cores, Ready: r, Waiting: w, Held: h, Unfinished: u}
}
```

- [ ] **Step 6: 运行，确认通过**

Run: `go test -race ./ -run 'TestSubmit|TestAdmission|TestSteerOn|TestEventsReplay'`
Expected: `ok`

- [ ] **Step 7: 提交**

```bash
git add go.mod go.sum neumann.go hub.go neumann_test.go
git commit -m "feat: machine API with committed submit and steer, admission and event replay"
```

---

### Task 9: 第一阶段不变量

**Files:**
- Test: `invariants_test.go`

- [ ] **Step 1: 写测试** `invariants_test.go`

```go
package neumann_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// Phase-1 invariants from docs/design.md §18. Run with -race.

func TestEveryCallGetsExactlyOneResultInOrder(t *testing.T) { // invariant 1
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo", "dev_echo", "dev_echo"), nil
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "q"})
	got := waitDone(t, m, id)
	if len(got.Regs) != 4 {
		t.Fatalf("regs %+v", got.Regs)
	}
	calls := got.Regs[1].ToolCalls()
	results := got.Regs[2].Parts
	if len(calls) != 3 || len(results) != 3 {
		t.Fatalf("%d calls, %d results", len(calls), len(results))
	}
	for i := range calls {
		if results[i].Kind != task.PartToolResult || results[i].CallID != calls[i].CallID ||
			results[i].Text != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("result %d = %+v for call %+v", i, results[i], calls[i])
		}
	}
	recs, _ := m.Calls(bg, id)
	for _, r := range recs {
		if r.Status != mem.CallDone || r.Attempt != 1 {
			t.Fatalf("record %+v", r)
		}
	}
}

func TestOneInstructionPerTaskAtATime(t *testing.T) { // invariant 2
	var mu sync.Mutex
	inflight := map[string]bool{}
	m, _ := start(t, neumann.Config{Cores: 4, Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		name := sim.FirstUser(req)
		mu.Lock()
		if inflight[name] {
			t.Errorf("two instructions of %s at once", name)
		}
		inflight[name] = true
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		inflight[name] = false
		mu.Unlock()
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_echo"), nil
		}
		return sim.Reply("done " + name), nil
	})
	var ids []string
	for i := range 20 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)}))
	}
	for _, id := range ids {
		if got := waitDone(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s: %s", id, got.State, got.StopReason)
		}
	}
}

func TestSlowToolDoesNotHoldTheCore(t *testing.T) { // invariant 3
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Cores: 1, Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "A" && sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		return sim.Reply(sim.FirstUser(req) + " done"), nil
	})
	a := submit(t, m, proto.Instruction{Input: "A"})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("A's slow tool never started")
	}
	b := submit(t, m, proto.Instruction{Input: "B"})
	if got := waitDone(t, m, b); got.LastAnswer() != "B done" {
		t.Fatalf("B: %+v", got)
	}
	if st := m.Status(); st.Waiting != 1 {
		t.Fatalf("A should be waiting on I/O; status %+v", st)
	}
	release()
	if got := waitDone(t, m, a); got.LastAnswer() != "A done" {
		t.Fatalf("A: %+v", got)
	}
}

func TestSteerWhileToolInFlight(t *testing.T) { // invariant 5, delivered after the tool round
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	var mu sync.Mutex
	var saw []string
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		mu.Lock()
		saw = sim.UserTexts(req)
		mu.Unlock()
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A"})
	<-entered
	if err := m.Steer(bg, id, "also check B"); err != nil {
		t.Fatal(err)
	}
	release()
	got := waitDone(t, m, id)
	roles := []task.Role{task.RoleUser, task.RoleAssistant, task.RoleTool, task.RoleUser, task.RoleAssistant}
	if len(got.Regs) != len(roles) {
		t.Fatalf("regs %+v", got.Regs)
	}
	for i, r := range roles {
		if got.Regs[i].Role != r {
			t.Fatalf("reg %d is %s, want %s", i, got.Regs[i].Role, r)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(saw, "|") != "A|also check B" {
		t.Fatalf("model saw %v", saw)
	}
}

func TestSteerIsDeliveredOrReportedDropped(t *testing.T) { // invariant 5, racing the final answer
	m, _ := start(t, neumann.Config{Cores: 2}, func(model.Request) (model.Response, error) {
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		return sim.Reply("ok"), nil
	})
	for i := range 40 {
		steer := fmt.Sprintf("steer-%d", i)
		id := submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)})
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		err := m.Steer(bg, id, steer)
		got := waitDone(t, m, id)
		if err != nil {
			if !errors.Is(err, neumann.ErrFinished) {
				t.Fatalf("steer: %v", err)
			}
			continue
		}
		delivered := false
		for _, msg := range got.Regs {
			if msg.Role == task.RoleUser && msg.TextContent() == steer {
				delivered = true
			}
		}
		dropped := false
		for _, ev := range eventsOf(t, m, id) {
			if ev.Kind == proto.EvSteerDropped {
				dropped = true
			}
		}
		if delivered == dropped {
			t.Errorf("%s: delivered=%v dropped=%v, want exactly one", steer, delivered, dropped)
		}
	}
}

func TestModelCallsEqualModelInstructions(t *testing.T) { // invariant 6
	m, sm := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_echo", "dev_echo"), nil
		}
		return sim.Reply("ok"), nil
	})
	var ids []string
	for i := range 10 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("task-%d", i)}))
	}
	for _, id := range ids {
		waitDone(t, m, id)
	}
	all, _ := m.EventLog(bg, 0, 0)
	steps := 0
	for _, ev := range all {
		if ev.Kind != proto.EvStep {
			continue
		}
		var d struct {
			PC string `json:"pc"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.PC == task.StepModel.String() {
			steps++
		}
	}
	if steps != sm.Calls() || steps != 30 {
		t.Fatalf("model steps %d, model calls %d, want 30 and 30", steps, sm.Calls())
	}
}

func TestDeniedToolIsHiddenAndRefused(t *testing.T) {
	m, _ := start(t, neumann.Config{Devices: []bus.Device{echoDev()}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_echo"), nil // the model names it anyway
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "q", Deny: []string{"dev_echo"}})
	got := waitDone(t, m, id)
	for _, s := range got.Tools {
		if s.Name == "dev_echo" {
			t.Fatal("denied tool was offered")
		}
	}
	res := got.Regs[2].Parts[0]
	if !res.IsError || !strings.Contains(res.Text, "not permitted") {
		t.Fatalf("result %+v", res)
	}
}
```

- [ ] **Step 2: 运行全部测试**

Run: `go test -race ./...`
Expected: 所有包 `ok`。任何一条不变量失败都说明实现与设计不符：回到对应的 Task 修实现，不要改测试的期望。

- [ ] **Step 3: 运行 make check**

Run: `make check`
Expected: 退出码 0（gofmt 无输出、vet 无告警、`-race` 测试全绿）。

- [ ] **Step 4: 稳定性检查**

Run: `go test -race -count=20 ./...`
Expected: 20 轮全部 `ok`。并发相关的不变量（2、3、5）偶发失败就是真 bug，按 superpowers:systematic-debugging 排查。

- [ ] **Step 5: 提交**

```bash
git add invariants_test.go
git commit -m "test: phase-1 invariants for ownership, tool pairing, slow tools, steer and model accounting"
```

---

## 完成标准

- `make check` 退出码 0。
- `go test -race -count=20 ./...` 全绿。
- 第 18 节第一阶段不变量（1、2、3、5、6）和第 20 条都有对应测试：1 → `TestEveryCallGetsExactlyOneResultInOrder`；2 → `TestOneInstructionPerTaskAtATime`；3 → `TestSlowToolDoesNotHoldTheCore`；4 → `sched.TestAgingPromotesBackground`；5 → `TestSteerWhileToolInFlight`、`TestSteerIsDeliveredOrReportedDropped`、`core.TestSteerDuringFinalCallContinuesTheTask`；6 → `TestModelCallsEqualModelInstructions`；20 → `TestAdmissionIsExact`。
- 先提交后派发有直接测试：`core.TestToolsAreCommittedBeforeDispatch`。
