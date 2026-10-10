# neumann 设计

neumann 是一个 Go 写的常驻 agent 运行时，按冯·诺依曼计算机的方式组织：
虚拟核心不停地取指、执行、写回；工具挂在总线上；状态存在分层的存储里；外部只通过约定的协议和它交换数据。

模块路径：`github.com/liliang-cn/neumann`。

## 1. 目标与非目标

**目标**

- 一个常驻进程，任意多个任务在弹性伸缩的虚拟核心（vCore）上轮转推进。
- 一条指令 = 一次模型调用，或一批工具调用的派发。每次模型调用都是 vCore 上的一条指令，调度器看得见、计费算得到，没有藏起来的调用。
- 支持多个模型服务商：每个服务商是一组 vCore（核心组），任务按亲和性调度，服务商不可用时可以迁移到另一组。
- vCore 数量不人为固定：每组按服务商的真实承受能力自动探测（加性增长、乘性减少），也可以手动固定。
- 慢工具不占核心：工具调用是异步 I/O，完成后用中断唤醒任务。
- 每步写回持久存储：增减 vCore、上下文切换、迁移、崩溃恢复都是同一件事（从主存加载任务）。
- 对外只有两种结构：输入 `Instruction`，输出 `Event`。

**非目标（第一版不做）**

- 团队、角色、编排层。
- 多租户。
- HTTP 服务（只做进程内 Go API 和 stdio JSON Lines）。
- 插件系统。
- RAG 实现（只定义"磁盘"接口）。
- 子 agent / 协处理器设备。
- 输出 lint。确定性检查以后作为可选协处理器提供，默认关闭。
- 缓存感知调度（见 6.3 末尾）。

## 2. 概念对应

| 计算机 | neumann | 包 |
|---|---|---|
| 核心 | vCore：一个 goroutine，循环 取指 → 执行 → 写回；不带状态，可随时增减 | `core` |
| 大小核 / 异构核心 | 核心组：一个模型服务商（或一个模型）一组 vCore | `core` |
| 拥塞窗口 | 每组的 vCore 上限 `W`，按服务商反馈自动调整 | `core` |
| 指令 | 一步：`StepModel`、`StepTools` 或 `StepCompact` | `task` |
| 进程 / PCB | 任务 / TCB | `task` |
| 程序计数器 | `TCB.PC`：下一步的类型 | `task` |
| 寄存器 | 这一步发给模型的消息序列（服务商无关的中立格式） | `task` |
| 调度器 | 每组的就绪队列 + 等待集合 + 中断队列 | `sched` |
| 系统总线 | `Bus`：地址 → 设备，带仲裁和设备队列 | `bus` |
| 设备 | 内置 Go 函数、MCP server、skills | `bus/builtin`、`bus/mcp`、`bus/skills` |
| DMA + 中断 | 异步派发工具调用，完成时发中断 | `bus`、`sched` |
| L1 缓存 | 服务商 prompt cache，靠前缀字节稳定命中 | `model`（适配器打缓存标记） |
| L2 缓存 | 只读工具的结果缓存 | `cache` |
| 主存 | 任务状态库（SQLite，组提交） | `mem` |
| 磁盘 / 换页 | 长期记忆接口；上下文过长时换出成摘要 | `mem`、`task` |
| MMU / 页表 | 每个任务的工具权限表，派发时检查 | `bus` |
| I/O 端口 | `Instruction` / `Event` 协议 | `proto` |

## 3. 消息与任务

### 3.1 中立消息格式

TCB 里存的消息与服务商无关，由模型适配器在发请求时翻译（第 10 节）。

```go
type Role string // system / user / assistant / tool

type Message struct {
    Role  Role
    Parts []Part
}

type Part struct {
    Kind string // text / tool_call / tool_result / reasoning

    Text string // text、reasoning 的可读部分

    CallID string          // tool_call、tool_result：机器内部 id，形如 "c-17"
    Name   string          // tool_call
    Args   json.RawMessage // tool_call
    IsError bool           // tool_result

    // reasoning：服务商私有的推理块（例如带签名的 thinking）。
    // 只在发回同一个服务商时原样带上；发往其他服务商时丢弃。
    Provider string
    Opaque   json.RawMessage
}
```

工具调用 id 一律由机器生成（`c-<序号>`，任务内唯一）。适配器负责把服务商自己的 id 映射到内部 id，并把映射记在 `Part` 里，回放时还原。

### 3.2 TCB

```go
type State int      // Ready, Running, Waiting, Suspended, Done, Failed, Cancelled
type PC int         // StepModel, StepTools, StepCompact
type WaitReason int // WaitIO, WaitBackoff

type TCB struct {
    ID         string
    State      State
    PC         PC
    Wait       WaitReason
    WakeAt     time.Time       // WaitBackoff 时有效
    Class      string          // 核心组；迁移时改变
    Priority   int             // 0 = 交互，1 = 后台
    ReadySince time.Time       // 进入就绪队列的时刻，决定队内顺序
    System     string          // 创建时确定
    Tools      []ToolDef       // 创建时按页表快照，按名字排序，之后不变
    Regs       []Message
    Pending    []ToolCall      // PC == StepTools 时待派发或在途的调用
    Results    map[string]ToolResult
    Inbox      []string        // 尚未送达的插话
    Allow      []string        // 页表
    Deny       []string
    Budget     Budget          // MaxSteps、Deadline、MaxTokens
    Used       Usage           // 累计：步数、prompt / completion / reasoning / cached token
    Retries    int             // 当前指令的重试次数
    MaxOut     int             // 当前输出 token 上限（截断时升级）
    StopReason string
    Version    int64           // 每次写回 +1，乐观并发
}
```

`Suspended` 是挂起状态：不进就绪队列，但中断（工具结果、插话）照常记录。

## 4. vCore 循环

```go
for {
    t, ok := sched.Next(ctx, group) // 取指：从本组就绪队列取一个任务；无就绪任务时阻塞
    if !ok { return }                // 被要求退役，或机器停止
    t.ApplyInterrupts()
    switch t.PC {
    case StepModel:   execModel(t)
    case StepTools:   execTools(t)   // 派发后立刻返回
    case StepCompact: execCompact(t)
    }
    mem.Save(t)                      // 写回主存（组提交，返回时已落盘）
    sched.Requeue(t)                 // 交还调度器：就绪队列 / 等待集合 / 结束
    group.AfterStep(result)          // 报告这一步的结果给拥塞控制（第 5 节）
}
```

vCore 只在两条指令之间退役，从不在指令中途被杀。

### 4.1 execModel

1. 检查预算；超预算则 `Done`，`StopReason = "budget"`，答案是最后一条 assistant 文本（不额外做"强行总结"调用）。
2. 寄存器 token 估算超过阈值（模型窗口的 70%，窗口未知时用 `Config.CompactAt`，默认 60000）时，改设 `PC = StepCompact`，本步结束。
3. 通过本组的适配器调一次模型，累计 `Usage`，发 `usage` 事件。
4. 结果分三种：
   - **有工具调用**：assistant 消息原样追加（模型发出的每个调用都保留），`Pending` = 这些调用，`PC = StepTools`。
   - **最终文本**：若 `Inbox` 非空，把这段文本作为 assistant 轮次，送达插话，`PC = StepModel` 继续；否则 `Done`。
   - **截断且没有内容**（`finish_reason=length`）：`MaxOut` 乘 4 后重做本指令，最多升级两次，之后 `Failed`。
5. 出错：见第 12 节。

### 4.2 execTools

把 `Pending` 交给总线异步派发，任务转 `Waiting(IO)`，vCore 立刻去取下一条指令。
每个调用完成后，结果作为中断进入中断队列（不直接写 TCB，见第 7 节）。全部完成时，调度器按调用顺序把结果追加成 tool 消息（每个调用恰好一个结果），任务转 `Ready`、`PC = StepModel`。

### 4.3 execCompact

用一次模型调用把最早的若干个完整轮次（assistant 加上它的全部 tool 结果，从不拆开一对）换出成一条摘要消息，然后 `PC = StepModel`。这是普通指令，计费照常。

## 5. 核心组与弹性 vCore

```go
type CoreGroup struct {
    Class      string
    Model      model.Adapter
    Min        int           // 至少保留的 vCore，默认 1
    Max        int           // 0 = 不设上限，靠探测
    Initial    float64       // 初始窗口，默认 2
    IdleRetire time.Duration // 空闲多久退役，默认 30s
    TPM        int           // 每分钟 token 上限，0 = 不限
    Fallback   string        // 服务商不可用时迁往的组，空 = 不迁移
    MaxQueued  int           // 本组未结束任务上限，0 = 只受全局限制
}
```

### 5.1 拥塞窗口

每组维护一个窗口 `W`（浮点数）。本组存活的 vCore 数 `n` 满足 `Min ≤ n ≤ max(Min, floor(W))`，`Max > 0` 时再不超过 `Max`。

- **加性增长**：每次模型调用成功，`W += 1/W`（每成功一个窗口的调用，`W` 增加 1）。处于冷却期时不增长。
- **乘性减少**：收到过载信号（429、529、带 overload 字样的 503），`W = max(Min, W/2)`，进入 10 秒冷却期。触发这次过载的指令按第 12 节退避。
- **扩容**：本组就绪队列非空、所有 vCore 都在忙、`n < floor(W)` 时，立刻启动一个新 vCore。新 vCore 直接取指，不需要预热。
- **缩容**：`n > floor(W)` 时，最先执行完当前指令的 vCore 退役。
- **空闲回收**：vCore 空闲超过 `IdleRetire` 且 `n > Min` 时退役。空闲回收不改变 `W`：窗口记住的是服务商能承受多少，不是现在需要多少。
- **手动**：`m.SetCores(class, n)` 把 `W` 固定为 `n` 并关闭自动调整；`m.AutoScale(class, true)` 恢复。

每次 vCore 增减都发 `core_up` / `core_down` 事件，带原因：`demand`、`overload`、`idle`、`manual`、`shutdown`。

### 5.2 token 速率上限

`TPM > 0` 时，组内维护一个 60 秒滑动窗口的 token 计数。发模型请求前，用寄存器估算的 prompt token 预占额度；超出时本组暂停发新请求（任务留在就绪队列），发 `throttled` 事件，直到窗口释放。调用完成后按实际用量修正。只算 token，不算钱。

### 5.3 熔断与迁移

- 同一组连续 `5` 次指令以临时错误失败（跨任务计数，成功一次清零），本组熔断：停止发请求，发 `group_down` 事件。
- 熔断 30 秒后半开：放一个 vCore 试一条指令，成功则恢复（`group_up`），失败则再等 30 秒。
- 熔断期间，若配置了 `Fallback`，本组就绪队列里的任务迁往 `Fallback` 组：`Class` 改变，`ReadySince` 保留（迁移不让任务重新排队尾），发 `migrated` 事件。迁移后第一次请求会丢掉原服务商私有的 `reasoning` 块，且这一步 L1 不命中。
- 未配置 `Fallback` 时，任务留在原组就绪队列里等待恢复，直到超出各自的 `Deadline`。
- 任务提交时可以用 `PinClass` 禁止迁移。

## 6. 队列与调度

### 6.1 五个队列

| 队列 | 内容 | 入队 | 出队 |
|---|---|---|---|
| 就绪队列 | 可以执行下一步的任务；每组一条，每条分交互、后台两级 | Submit、中断唤醒、执行完一步、迁移 | 本组 vCore 取指 |
| 等待集合 | `Waiting(IO)`、`Waiting(Backoff)`、`Suspended` 的任务 | 派发工具、模型出错退避、挂起 | 中断唤醒、定时器到点、恢复 |
| 中断队列 | 每个任务一条：插话、取消、工具结果 | 外部调用、总线 | 调度器在自己持有任务时合并 |
| 设备队列 | 等待设备执行的工具调用，每个设备一条 FIFO | 总线派发 | 设备空出并发名额 |
| 事件出队列 | 每个订阅者一个有界环形缓冲 | vCore、调度器 | 订阅者 |

### 6.2 准入

- 全局 `Config.MaxQueued`（默认 1000），每组 `CoreGroup.MaxQueued`。计数的是所有未结束的任务（包括等待和挂起的），因为它们都占内存。
- 超限时 `Submit` 立刻返回 `*CapacityError{Scope, Count, Limit}`（`errors.Is(err, ErrBusy)` 为真），不阻塞调用方。
- 准入判断和登记在同一把锁里完成，并发提交不会一起越过上限。
- 插话、取消、挂起、恢复、改优先级不受准入限制。

### 6.3 出队顺序

1. vCore 只取本组的就绪队列。
2. 组内交互级先于后台级。
3. 同级按 `ReadySince` 先进先出。任务每执行完一步重新入队，`ReadySince` 更新为当时，所以同组任务轮转推进，时间片是一步。
4. 防饿死：后台任务的 `ReadySince` 早于 `now - AgingAfter`（默认 30s）时，按交互级参与排序。
5. 退避中的任务不在就绪队列里，定时器到点才放回，不会出现取出来不能执行的空转。

不在第一版：缓存感知调度（距上次调用接近服务商缓存 TTL 的任务略微优先）。需要先在真实负载下测出收益。

### 6.4 操作员控制

```go
m.Pause()                       // 全部 vCore 执行完当前指令后停止取指；中断照常收
m.Resume()
m.Drain(ctx)                    // 拒绝新 Submit，等所有任务结束
m.Suspend(id) / m.ResumeTask(id)
m.SetPriority(id, p)
m.SetClass(id, class)           // 手动迁移，规则同 5.3
m.Cancel(id)
m.SetCores(class, n) / m.AutoScale(class, on)
m.Queue() QueueStatus           // 每组每级的长度、队首任务、最长等待时长、等待集合大小
```

这些操作也可以通过协议发出（第 11 节）。

### 6.5 重启时重建

队列不单独持久化。重启时从主存加载所有未结束的 TCB，按 `Class`、`Priority`、`ReadySince`、`State` 重建就绪队列和等待集合，顺序与崩溃前一致。每组按 `Min` 启动 vCore，`W` 回到 `Initial`。

### 6.6 背压

| 哪里慢 | 结果 |
|---|---|
| 订阅者读得慢 | 丢最旧的非终态事件，发一个 `events_dropped` 计数；终态事件从不丢（它们也在主存里） |
| 某个设备慢 | 它的设备队列变长，用它的任务留在等待集合；不占 vCore，不影响其他设备 |
| 服务商过载 | 该组窗口减半，任务在就绪队列多等；其他组不受影响 |
| 服务商 token 速率 | 该组暂停发请求（5.2） |
| 整体太忙 | 准入拒绝，`ErrBusy` |

## 7. 并发规则

1. **TCB 单一所有者。** 任务在就绪队列、等待集合里时归调度器；被 `Next` 取走后归那个 vCore，直到 `Requeue` 交还。不允许两个 goroutine 同时修改一个 TCB。
2. **中断不直接写 TCB。** 插话、取消、工具结果先进该任务的中断队列；调度器在持有任务时（`Requeue` 时、任务在等待集合里时）在自己的锁内合并。
3. **一个任务同一时刻最多一条指令。** 任务内的并行只有一批工具调用的并发派发。
4. **工具调用在设备上并发**，受设备 `MaxConcurrent`（默认 4）和全局 I/O 上限 `Config.MaxIO`（默认 256）约束，超出的在设备队列里排队。
5. **主存单写入者 + 组提交。** 一个写入 goroutine 把 5 毫秒内（或满 64 条）到达的写回合并成一个事务；`Save` 在所属批次提交后才返回，所以"每步写回"的保证不变。`Version` 不匹配时报错（说明违反了规则 1）。
6. **拥塞控制状态**（`W`、冷却、熔断）由每组一把锁保护，vCore 在 `AfterStep` 和退役判断时读取。
7. **事件发送不阻塞** vCore 和调度器（6.6）。

## 8. 存储层次

### 8.1 L1：服务商 prompt cache

每次请求按固定顺序组装：系统提示词（创建时确定，不含随时间变化的内容）→ 工具定义（创建时快照、按名字排序）→ 消息序列。前两部分在任务生命周期内逐字节不变，任务在哪个 vCore 上执行都一样。

缓存标记由适配器负责：OpenAI 兼容服务自动缓存，不打标记；Anthropic 在工具定义末尾和消息序列倒数第二条打 `cache_control`。

迁移到另一组时 L1 必然不命中，这是迁移的代价。

### 8.2 L2：工具结果缓存

- 只缓存设备声明为 `ReadOnly && Cacheable` 的工具。
- key = 设备地址 + 工具名 + 参数的规范化 JSON；作用域是单个任务；TTL 由设备声明。
- 同一任务里任何非只读调用完成后，清空该任务的 L2。
- 命中时返回真实的缓存结果。不允许返回"已经调用过"之类的空结果。

### 8.3 主存

```go
type Store interface {
    Save(ctx context.Context, t *TCB) error         // 组提交；Version 不匹配时报错
    Load(ctx context.Context, id string) (*TCB, error)
    Unfinished(ctx context.Context) ([]*TCB, error) // 重启时重建队列
    AppendEvent(ctx context.Context, ev Event) error
}
```

第一版实现：内存版（不持久，测试用）和 SQLite 版（纯 Go 驱动 `modernc.org/sqlite`，WAL 模式），一个任务一行，`Regs` 等存 JSON。

### 8.4 磁盘

```go
type Disk interface {
    Recall(ctx context.Context, query string, k int) ([]string, error)
    Remember(ctx context.Context, text string) error
}
```

磁盘不自动注入上下文。挂了磁盘时，它以 `memory_recall` / `memory_remember` 两个工具出现在总线上，由模型决定何时用。第一版只有接口和空实现。

## 9. 总线与设备

```go
type Device interface {
    Name() string
    Tools(ctx context.Context) ([]ToolDef, error)
    Call(ctx context.Context, tool string, args json.RawMessage) (ToolResult, error)
    Close() error
}

type ToolDef struct {
    Name        string
    Description string
    Schema      json.RawMessage
    ReadOnly    bool
    Cacheable   bool
    CacheTTL    time.Duration
    Timeout     time.Duration // 0 = 120s
}
```

- 模型看到的工具名是 `<设备名>_<工具名>`（非法字符替换成 `_`）。挂载时检测重名，重名直接报错。
- **仲裁**：每个设备 `MaxConcurrent`（默认 4），超出的在设备队列 FIFO 排队。
- **页表**：任务的 `Allow` / `Deny` 是设备或工具地址的前缀列表。快照工具列表时过滤；派发时再检查一次，越权调用返回错误结果给模型，不执行。
- **超时**：超时作为错误结果交给模型。
- 第一版设备：`builtin`（注册 Go 函数）、`mcp`（`github.com/modelcontextprotocol/go-sdk`，stdio 或 streamable HTTP，一个 server 一个设备）、`skills`（目录下的 `SKILL.md`，每个 skill 一个工具 `skills_<name>`，调用时返回正文，名字和一句话描述写在工具描述里）。

## 10. 模型适配器

```go
type Adapter interface {
    Provider() string // 用于判断 reasoning 块能否原样回传
    Window() int      // 上下文窗口，未知为 0
    Call(ctx context.Context, req Request) (Response, error)
}

type Request struct {
    System   string
    Tools    []ToolDef
    Messages []Message // 中立格式
    MaxOut   int
}

type Response struct {
    Message      Message // 中立格式：text、tool_call、reasoning
    FinishReason string  // stop / tool_calls / length
    Usage        Usage   // prompt、completion、reasoning、cached
}
```

第一版两个适配器：

- **OpenAI 兼容**（`/chat/completions`）：覆盖 OpenAI、DeepSeek、各类网关（包括转发 Gemini 的网关）、Ollama。`completion_tokens` 不含推理 token 的网关，从 `completion_tokens_details.reasoning_tokens` 单独读。
- **Anthropic 原生**（`/v1/messages`）：tool_use / tool_result 块、带签名的 thinking 块、`cache_control` 标记。

两种格式差别最大，同时实现能证明中立格式真的中立。

错误分类（适配器负责）：

- `Overload`：429、529、带 overload 的 503。触发窗口减半（5.1）并退避。
- `Transient`：其余 5xx、网络错误、超时。退避，计入熔断计数。
- `Permanent`：其余 4xx。任务失败。永久错误的判断优先于报错文字里出现的"timeout"之类字样。
- `context.Canceled` 永远不是可重试错误。

## 11. 协议

```go
type Instruction struct {
    Kind     string   `json:"kind"`     // submit / steer / cancel / suspend / resume / priority / class
    TaskID   string   `json:"task_id,omitempty"`
    Input    string   `json:"input,omitempty"`    // submit、steer
    Class    string   `json:"class,omitempty"`    // submit、class
    PinClass bool     `json:"pin_class,omitempty"`
    Priority int      `json:"priority,omitempty"` // submit、priority
    Budget   *Budget  `json:"budget,omitempty"`
    Allow    []string `json:"allow,omitempty"`
    Deny     []string `json:"deny,omitempty"`
}

type Event struct {
    Kind   string          `json:"kind"`
    TaskID string          `json:"task_id,omitempty"` // 机器级事件为空
    Seq    int64           `json:"seq"`               // 任务事件按任务递增；机器事件单独递增
    Data   json.RawMessage `json:"data,omitempty"`
}
```

任务事件：`accepted`、`rejected`（准入失败）、`step`（指令类型、组、vCore 号）、`tool_call`、`tool_result`、`usage`、`answer`、`migrated`、`done`、`failed`、`cancelled`、`steer_dropped`。

机器事件：`core_up`、`core_down`、`throttled`、`group_down`、`group_up`、`events_dropped`。

`submit` 不带 `task_id` 时由机器分配 UUID。

传输：进程内（`m.Submit`、`m.Events()`）；stdio（`cmd/neumann` 读 stdin 的 JSON Lines 指令，往 stdout 写 JSON Lines 事件，日志只写 stderr）。

## 12. 出错处理

| 情况 | 处理 |
|---|---|
| 模型过载 | 组窗口减半；任务转 `Waiting(Backoff)`，退避 1s、2s、4s…上限 60s |
| 模型临时错误 | 任务退避同上，计入熔断计数；单个任务重试超过 `Config.MaxRetries`（默认 5）次则 `Failed` |
| 模型永久错误 | `Failed`，事件里带服务商原始错误 |
| 截断且无内容 | 提高输出上限重做，最多两次（4.1） |
| 组熔断 | 迁往 `Fallback` 或留在队列等待恢复（5.3） |
| 工具出错 / 超时 / 越权 | 错误文本作为该调用的结果交给模型 |
| 写回失败 | 记 ERROR，发 `failed`，任务不再调度 |
| 进程崩溃后重启 | 按 6.5 重建。`Running` → `Ready`（重做这条指令，可能重复一次模型调用的花费）；`Waiting(IO)` 中没有结果的调用写入结果"进程重启，执行状态未知"，然后 `Ready`；`Waiting(Backoff)` 和 `Suspended` 保持 |
| 超预算 | `Done`，`StopReason = "budget"` |
| vCore goroutine panic | 恢复 panic，任务 `Failed` 并带上 panic 信息，这个 vCore 退役，必要时按需求重新扩容 |

## 13. 观测

`m.Status() Status`：每组的 `W`、存活 vCore 数、冷却/熔断状态、最近一次过载时间、TPM 占用；每个 vCore 在做什么；每条队列长度和最长等待；每个未结束任务的状态、组、步数和累计 token。加锁拷贝，不调模型、不读主存，可以高频轮询。

因为每次模型调用都是一条指令，`Status` 和 `usage` 事件里的 token 总数就是这个进程真实发出去的总数。

## 14. 公开 API

```go
m, err := neumann.New(neumann.Config{
    Groups: []neumann.CoreGroup{
        {Class: "big",    Model: anthropic, Min: 1, Fallback: "little"},
        {Class: "little", Model: flash,     Min: 2, Max: 64, TPM: 2_000_000},
    },
    Default:   "little",
    Store:     store,        // nil = 内存实现
    Devices:   []bus.Device{builtin, mcpGitHub, skillsDir},
    System:    "You are ...",
    MaxQueued: 1000,
})
err = m.Start(ctx)           // 启动各组 Min 个 vCore；从 Store 重建队列
id, err := m.Submit(neumann.Instruction{Kind: "submit", Input: "...", Class: "big"})
for ev := range m.Events() { ... }
err = m.Stop(ctx)            // 停止取指，等在途指令写回后返回
```

## 15. 包结构

```
neumann/            Machine、Config、公开 API、操作员控制
  core/             vCore 循环、execModel / execTools / execCompact、核心组、拥塞窗口、熔断
  sched/            就绪队列、等待集合、中断队列、定时器、准入
  task/             TCB、中立消息、寄存器组装、token 估算
  bus/              Bus、仲裁、设备队列、页表
    builtin/  mcp/  skills/
  cache/            L2
  mem/              Store 接口、memory 实现、sqlite 实现（组提交）、Disk 接口
  model/            Adapter 接口、错误分类
    openai/  anthropic/
  proto/            Instruction、Event、JSON Lines 编解码
  sim/              确定性模拟器：假时钟、脚本模型（可注入 429 / 5xx / 截断）、脚本设备
  cmd/neumann/      stdio 前端
```

## 16. 测试

**模拟器（`sim`）**：假时钟、按脚本回复的模型（可注入过载、临时错误、截断）、可控延迟的设备，全部在 `-race` 下跑。必须成立的不变量：

1. 每个工具调用恰好一个结果，顺序与调用一致。
2. 一个 TCB 任何时刻只有一个所有者（调度器或一个 vCore）。
3. 工具在途时，即使每组只有 1 个 vCore，也能推进其他任务。
4. 后台任务在持续的交互负载下，`AgingAfter` 之后仍会被执行。
5. 在任意指令之后杀掉机器、用同一个 Store 重启，所有任务都能结束，不丢消息，就绪队列顺序与崩溃前一致。
6. 插话在任何状态下送达，或以 `steer_dropped` 报告。
7. 模型调用次数 = `StepModel` + `StepCompact` 指令数；`usage` 事件之和 = 脚本模型返回的用量之和。
8. 同一任务连续两步请求的前缀（系统提示词 + 工具定义）字节相同（迁移那一步除外）。
9. 拥塞窗口：脚本服务商在并发超过 K 时返回 429，稳定后存活 vCore 数在 K 附近振荡，不超过 2K。
10. vCore 只在指令之间退役：缩容和空闲回收不会产生半截指令（没有写回就消失的步）。
11. 32 个并发 `Submit` 对 `MaxQueued = 4`：恰好 4 个被接受。
12. 熔断后迁移：迁移的任务在新组完成，发往新服务商的请求里不含原服务商的 reasoning 块。
13. 中立格式往返：同一段对话经 OpenAI 和 Anthropic 适配器各翻译一次再解析回来，与原文一致（reasoning 块按规则保留或丢弃）。

**真实对比**：在 eval-go agentbench 里加一个 neumann 适配器，和 agent-go、harness-rs 在 core-30 + hard-15 上比，经过同一个计费代理统计成本。

## 17. 第一版验收标准

1. 第 16 节的模拟器测试全部通过，`go test -race ./...` 为绿。
2. 真实模型上，跑一个多轮工具任务的过程中 `kill -9`，重启后任务完成。
3. 真实网关上，同时提交 200 个短任务（单组、自动伸缩、`Max = 0`）：全部完成；`Status` 里的 `W` 在出现 429 后回落，之后重新增长；计费代理统计到的模型调用次数等于 neumann 报告的模型指令数。
4. agentbench core-30 + hard-15（rubric 判定、2 轮）：通过数不低于 harness-rs 减 2；每题 total token 不高于 harness-rs 的 1.1 倍。
