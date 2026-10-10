# neumann 设计

neumann 是一个 Go 写的常驻 agent 运行时，按冯·诺依曼计算机的方式组织：
核心不停地取指、执行、写回；工具挂在总线上；状态存在分层的存储里；外部只通过约定的协议和它交换数据。

模块路径：`github.com/liliang-cn/neumann`。

## 1. 目标与非目标

**目标**

- 一个常驻进程，N 个核心并行推进 M 个任务（M 可以远大于 N）。
- 一条指令 = 一次模型调用，或一批工具调用的派发。每次模型调用都是核心上的一条指令，调度器看得见、计费算得到，没有藏起来的调用。
- 慢工具不占核心：工具调用是异步 I/O，完成后用中断唤醒任务。
- 每步写回持久存储：上下文切换、崩溃恢复、断点续跑都是同一件事（从主存加载任务）。
- 对外只有两种结构：输入 `Instruction`，输出 `Event`。

**非目标（第一版不做）**

- 团队、角色、编排层。
- 多租户。
- HTTP 服务（只做进程内 Go API 和 stdio JSON Lines）。
- 插件系统。
- RAG 实现（只定义"磁盘"接口）。
- 子 agent / 协处理器设备。
- 输出 lint。确定性检查以后作为可选协处理器提供，默认关闭。

## 2. 概念对应

| 计算机 | neumann | 包 |
|---|---|---|
| 核心 | 一个 goroutine，循环 取指 → 执行 → 写回 | `core` |
| 指令 | 一步：`StepModel`、`StepTools` 或 `StepCompact` | `task` |
| 进程 / PCB | 任务 / TCB | `task` |
| 程序计数器 | `TCB.PC`：下一步的类型 | `task` |
| 寄存器 | 这一步发给模型的消息序列 | `task` |
| 调度器 | 就绪队列 + 等待集合 | `sched` |
| 系统总线 | `Bus`：地址 → 设备，带仲裁 | `bus` |
| 设备 | 内置 Go 函数、MCP server、skills | `bus/builtin`、`bus/mcp`、`bus/skills` |
| DMA + 中断 | 异步派发工具调用，完成时发中断 | `bus`、`sched` |
| 中断控制器 | 插话、取消、定时器、工具完成 | `sched` |
| L1 缓存 | 服务商 prompt cache，靠前缀字节稳定命中 | `task`（寄存器布局） |
| L2 缓存 | 只读工具的结果缓存 | `cache` |
| 主存 | 任务状态库（SQLite） | `mem` |
| 磁盘 / 换页 | 长期记忆接口；上下文过长时换出成摘要 | `mem`、`task` |
| MMU / 页表 | 每个任务的工具权限表，派发时检查 | `bus` |
| I/O 端口 | `Instruction` / `Event` 协议 | `proto` |

## 3. 任务（TCB）

```go
type State int // Ready, Running, Waiting, Done, Failed, Cancelled

type PC int // StepModel, StepTools, StepCompact

type WaitReason int // WaitIO, WaitBackoff

type TCB struct {
    ID        string
    State     State
    PC        PC
    Wait      WaitReason
    WakeAt    time.Time      // WaitBackoff 时有效
    Priority  int            // 0 = 交互，1 = 后台
    Regs      []Message      // 对话：user / assistant / tool
    Tools     []ToolDef      // 创建时按页表快照，按名字排序，之后不变
    Pending   []ToolCall     // PC == StepTools 时待派发或在途的调用
    Results   map[string]ToolResult // 按调用 id
    Inbox     []string       // 尚未送达的插话
    Budget    Budget         // MaxSteps、Deadline、MaxTokens
    Used      Usage          // 累计：步数、prompt/completion/reasoning/cached token
    Retries   int            // 当前指令的重试次数
    MaxOut    int            // 当前输出 token 上限（截断时升级）
    StopReason string
    Version   int64          // 每次写回 +1，用于乐观并发
}
```

一个任务同一时刻最多在一个核心上。

## 4. 核心循环

```go
for {
    t := sched.Next(ctx)            // 取指；无就绪任务时阻塞，直到中断或 ctx 结束
    t.ApplyInterrupts()             // 送达 Inbox 里的插话
    switch t.PC {
    case StepModel:   execModel(t)   // 一次模型调用
    case StepTools:   execTools(t)   // 派发后立刻返回，任务转 Waiting(IO)
    case StepCompact: execCompact(t) // 一次模型调用，把旧消息换出成摘要
    }
    mem.Save(t)                     // 写回主存
    sched.Requeue(t)                // 按状态放回就绪队列 / 等待集合 / 结束
}
```

### 4.1 execModel

1. 按第 6 节的布局组装寄存器，检查预算；超预算则 `Done`，`StopReason = "budget"`，返回最后一条 assistant 文本（不额外做"强行总结"调用）。
2. 调一次模型，累计 `Usage`，发 `usage` 事件。
3. 结果分三种：
   - **有工具调用**：assistant 消息原样追加（模型发出的每个调用都保留），`Pending` = 这些调用，`PC = StepTools`。
   - **最终文本**：若 `Inbox` 非空，把这段文本作为 assistant 轮次，送达插话，`PC = StepModel` 继续；否则 `Done`。
   - **截断且没有内容**（`finish_reason=length`）：`MaxOut` 乘 4 后重做本指令，最多升级两次，之后 `Failed`。
4. 出错：见第 10 节。

### 4.2 execTools

把 `Pending` 交给总线异步派发，任务转 `Waiting(IO)`，核心立刻去取下一条指令。
每个调用完成都写入 `Results`；全部完成后，按调用顺序把结果追加成 tool 消息（每个调用恰好一个结果），发中断，任务转 `Ready`、`PC = StepModel`。

### 4.3 execCompact

当寄存器的 token 估算超过阈值（模型窗口的 70%，窗口未知时用 `Config.CompactAt`，默认 60000）时，`execModel` 不调模型，改设 `PC = StepCompact` 后重新入队。
execCompact 用一次模型调用把最早的若干个完整轮次（assistant 加上它的全部 tool 结果，从不拆开一对）换出成一条摘要消息，然后 `PC = StepModel`。
这次调用是普通指令，计费照常。

## 5. 调度

- 两个优先级：交互（0）和后台（1）。同级 FIFO。
- 防饿死：后台任务在就绪队列里等待超过 `Config.AgingAfter`（默认 30s）后，按交互级处理。
- `Waiting(Backoff)` 的任务到 `WakeAt` 由定时器发中断回到就绪队列。
- 中断进入时：任务在 `Ready` 或 `Waiting(Backoff)` 时，插话直接进 `Inbox`；在 `Waiting(IO)` 时也进 `Inbox`，等工具结果全部回来后再送达（保证调用和结果的配对不被打断）；在 `Running` 时进 `Inbox`，下一步开头送达。
- 取消：任务在 `Waiting(IO)` 时取消在途调用的 context；任何状态下最终都转 `Cancelled`，写回，发 `cancelled` 事件。未送达的插话各发一个 `steer_dropped` 事件。

## 6. 存储层次

### 6.1 寄存器布局与 L1

每次模型调用的请求按固定顺序组装：

1. 系统提示词（创建任务时确定，不含随时间变化的内容）；
2. 工具定义（`TCB.Tools`，创建时快照、按名字排序）；
3. 消息序列。

前两部分在任务的整个生命周期里逐字节不变，任务在哪个核心上执行都一样，所以服务商的 prompt cache 能持续命中。
需要显式缓存断点的服务商（Anthropic 风格），在第 2 部分末尾和消息序列的倒数第二条打标记。

### 6.2 L2：工具结果缓存

- 只缓存设备声明为 `ReadOnly && Cacheable` 的工具。
- key = 设备地址 + 工具名 + 参数的规范化 JSON；作用域默认是单个任务；TTL 由设备声明。
- 同一任务里任何非只读调用完成后，清空该任务的 L2。
- 命中时返回真实的缓存结果，作为这个调用的结果交给模型。不允许返回"已经调用过"之类的空结果：模型对空结果唯一能做的就是再调一次。

### 6.3 主存

```go
type Store interface {
    Save(ctx context.Context, t *TCB) error            // Version 不匹配时报错
    Load(ctx context.Context, id string) (*TCB, error)
    Unfinished(ctx context.Context) ([]*TCB, error)    // 启动时恢复
    AppendEvent(ctx context.Context, ev Event) error
}
```

第一版实现：SQLite（纯 Go 驱动 `modernc.org/sqlite`），一个任务一行，`Regs` 等存 JSON。每步写回一次。

### 6.4 磁盘

```go
type Disk interface {
    Recall(ctx context.Context, query string, k int) ([]string, error)
    Remember(ctx context.Context, text string) error
}
```

磁盘不会被自动注入上下文（那会是一次藏起来的调用）。挂了磁盘时，它以 `memory_recall` / `memory_remember` 两个工具出现在总线上，由模型决定何时用。第一版只有接口和一个空实现。

## 7. 总线与设备

```go
type Device interface {
    Name() string                          // 设备地址，如 "mcp.github"、"builtin"
    Tools(ctx context.Context) ([]ToolDef, error)
    Call(ctx context.Context, tool string, args json.RawMessage) (ToolResult, error)
    Close() error
}

type ToolDef struct {
    Name        string          // 设备内名字
    Description string
    Schema      json.RawMessage
    ReadOnly    bool
    Cacheable   bool
    CacheTTL    time.Duration
}
```

- 模型看到的工具名是 `<设备名>_<工具名>`（非法字符替换成 `_`）。挂载时检测重名，重名直接报错，不静默覆盖。
- **仲裁**：每个设备有 `MaxConcurrent`（默认 4），超出的调用在该设备上 FIFO 排队。
- **页表**：任务创建时带 `Allow` / `Deny`（设备或工具地址的前缀列表）。快照工具列表时就过滤；派发时再检查一次，越权调用返回错误结果给模型，不执行。
- **超时**：每个调用有超时（设备可声明，默认 120s），超时作为错误结果交给模型。
- 第一版设备：
  - `builtin`：注册 Go 函数。
  - `mcp`：用 `github.com/modelcontextprotocol/go-sdk` 连接 stdio 或 streamable HTTP 的 MCP server，一个 server 一个设备。
  - `skills`：读取目录下的 `SKILL.md`。每个 skill 是一个工具 `skills_<name>`，调用时返回 skill 正文；skill 的名字和一句话描述就写在工具描述里，不另外塞进系统提示词。

## 8. 模型

```go
type Model interface {
    Call(ctx context.Context, req Request) (Response, error)
}

type Response struct {
    Text         string
    ToolCalls    []ToolCall
    FinishReason string          // stop / tool_calls / length
    Usage        Usage           // prompt、completion、reasoning、cached
}
```

第一版只实现 OpenAI 兼容的 `/chat/completions` 客户端。错误要分类为 `Transient`（429、5xx、网络错误）和 `Permanent`（其余 4xx）；永久错误的分类优先于报错文字里出现的"timeout"之类字样；`context.Canceled` 永远不是临时错误。

## 9. 协议

```go
type Instruction struct {
    Kind     string          `json:"kind"`     // submit / steer / cancel
    TaskID   string          `json:"task_id,omitempty"`
    Input    string          `json:"input,omitempty"`    // submit、steer
    Priority int             `json:"priority,omitempty"` // submit
    Budget   *Budget         `json:"budget,omitempty"`   // submit
    Allow    []string        `json:"allow,omitempty"`    // submit
    Deny     []string        `json:"deny,omitempty"`     // submit
}

type Event struct {
    Kind   string          `json:"kind"`
    TaskID string          `json:"task_id"`
    Seq    int64           `json:"seq"`   // 每个任务内单调递增
    Data   json.RawMessage `json:"data,omitempty"`
}
```

事件种类：`accepted`、`step`（开始一条指令：类型、核心号）、`tool_call`、`tool_result`、`usage`、`answer`、`done`、`failed`、`cancelled`、`steer_dropped`。

`submit` 不带 `task_id` 时由机器分配 UUID。

传输：

- 进程内：`m.Submit(ins) (taskID, error)`、`m.Events() <-chan Event`。
- stdio：`cmd/neumann` 读 stdin 的 JSON Lines 指令，往 stdout 写 JSON Lines 事件，日志只写 stderr。

事件通道满时不阻塞核心：每个订阅者有一个有界缓冲，溢出时丢最旧的非终态事件，并发一个 `events_dropped` 计数事件；终态事件（`done`、`failed`、`cancelled`）从不丢弃，因为它们同时写进了主存。

## 10. 出错处理

| 情况 | 处理 |
|---|---|
| 模型临时错误 | 任务转 `Waiting(Backoff)`，退避 1s、2s、4s…上限 60s，最多 `Config.MaxRetries`（默认 5）次，之后 `Failed` |
| 模型永久错误 | `Failed`，事件里带服务商的原始错误 |
| 截断且无内容 | 提高输出上限重做，最多两次（4.1） |
| 工具出错 / 超时 / 越权 | 错误文本作为该调用的结果交给模型 |
| 写回失败 | 记 ERROR，发 `failed` 事件，任务不再调度（不能在主存不一致的状态上继续） |
| 进程崩溃后重启 | `Unfinished()` 恢复所有任务：`Running` → `Ready`（重做这条指令，可能重复一次模型调用的花费）；`Waiting(IO)` 中没有结果的调用写入结果"进程重启，执行状态未知"，然后 `Ready`；`Waiting(Backoff)` 保持 |
| 超预算 | `Done`，`StopReason = "budget"`，答案是最后一条 assistant 文本 |

## 11. 观测

`m.Status() Status`：每个核心在做什么（空闲 / 任务 id + 指令类型）、就绪队列长度、等待集合大小、每个未结束任务的状态、步数和累计 token。加锁拷贝，不调模型、不读主存，可以高频轮询。

因为每次模型调用都是一条指令，`Status` 和 `usage` 事件里的 token 总数就是这个进程真实发出去的总数。

## 12. 公开 API

```go
m, err := neumann.New(neumann.Config{
    Cores:   4,
    Model:   model,          // neumann.Model
    Store:   store,          // nil = 内存实现，不持久
    Devices: []bus.Device{builtin, mcpGitHub, skillsDir},
    System:  "You are ...",
})
err = m.Start(ctx)           // 启动核心；从 Store 恢复未完成任务
id, err := m.Submit(neumann.Instruction{Kind: "submit", Input: "..."})
for ev := range m.Events() { ... }
err = m.Stop(ctx)            // 停止取指，等在途指令写回后返回
```

## 13. 包结构

```
neumann/            Machine、Config、公开 API
  core/             核心循环、execModel / execTools / execCompact
  sched/            就绪队列、等待集合、定时器、中断
  task/             TCB、寄存器组装、token 估算
  bus/              Bus、仲裁、页表
    builtin/  mcp/  skills/
  cache/            L2
  mem/              Store 接口、memory 实现、sqlite 实现、Disk 接口
  model/            Model 接口、OpenAI 兼容客户端、错误分类
  proto/            Instruction、Event、JSON Lines 编解码
  sim/              确定性模拟器：假时钟、脚本模型、脚本设备
  cmd/neumann/      stdio 前端
```

## 14. 测试

**模拟器（`sim`）**：假时钟、按脚本回复的模型、可控延迟的设备，全部在 `-race` 下跑。必须成立的不变量：

1. 每个工具调用恰好一个结果，顺序与调用一致。
2. 一个任务不会同时在两个核心上。
3. 工具在途时，核心数为 1 也能推进其他任务（慢工具不占核心）。
4. 后台任务在持续的交互负载下仍能在 `AgingAfter` 后被执行。
5. 在任意指令之后杀掉机器、用同一个 Store 重启，所有任务都能结束，且不丢消息。
6. 插话在任何状态下送达或以 `steer_dropped` 报告，不会无声丢失。
7. 模型调用次数 = `StepModel` + `StepCompact` 指令数；`usage` 事件之和 = 脚本模型返回的用量之和。
8. 同一任务连续两步请求的前缀（系统提示词 + 工具定义）字节相同。

**真实对比**：在 eval-go agentbench 里加一个 neumann 适配器，和 agent-go、harness-rs 在 core-30 + hard-15 上比，经过同一个计费代理统计成本。

## 15. 第一版的验收标准

1. 第 14 节的模拟器测试全部通过，`go test -race ./...` 为绿。
2. 一个真实模型（OpenAI 兼容网关）上，跑一个多轮工具任务的过程中 `kill -9`，重启后任务完成。
3. agentbench core-30 + hard-15（rubric 判定、2 轮）：通过数不低于 harness-rs 减 2；每题 total token 不高于 harness-rs 的 1.1 倍；计费代理统计到的模型调用次数等于 neumann 自己报告的模型指令数。
