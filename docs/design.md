# neumann 设计

neumann 是一个 Go 写的常驻 agent 运行时，按冯·诺依曼计算机的方式组织：
虚拟核心不停地取指、执行、写回；工具挂在总线上；状态存在分层的存储里；外部只通过约定的协议和它交换数据。

冯·诺依曼类比用来解释结构。工程上的保证由第 4 节的提交与恢复语义定义，并由第 18 节的故障注入测试验证。

模块路径：`github.com/liliang-cn/neumann`。

## 1. 目标与非目标

**目标**

- 一个常驻进程，任意多个任务在弹性伸缩的虚拟核心（vCore）上轮转推进。
- 一条指令 = 一次模型调用，或一批工具调用的派发。每次模型调用（包括压缩）都是 vCore 上的一条指令，调度器看得见、计量算得到，没有藏起来的调用。
- 支持多个模型服务商：每个服务商（或模型）是一个执行组；限流额度单独建模，可以跨执行组共享。
- 慢工具不占核心：工具调用是异步 I/O，完成后用中断唤醒任务。
- **先提交，后动作**：任何对外可见的效果（API 返回成功、发出事件、派发工具）都发生在使它可恢复的那次提交之后。
- 对外只有两种结构：输入 `Instruction`，输出 `Event`。事件可以按序号补读。

**非目标（第一版不做）**

- 团队、角色、编排层。
- 多租户。
- HTTP 服务（只做进程内 Go API 和 stdio JSON Lines）。
- 插件系统。
- RAG 实现（只定义"磁盘"接口）。
- 子 agent / 协处理器设备。
- 输出 lint。确定性检查以后作为可选协处理器提供，默认关闭。
- 缓存感知调度。
- 多模态输入（第一版只有文本和工具调用）。

## 2. 概念对应

| 计算机 | neumann | 包 |
|---|---|---|
| 核心 | vCore：一个 goroutine，循环 取指 → 执行 → 提交；不带状态，可随时增减 | `core` |
| 大小核 | 执行组：一个适配器 + 一组 vCore | `core` |
| 拥塞窗口 | 执行组的并发上限 `W`，只对过载信号做加性增长、乘性减少 | `core` |
| 内存带宽配额 | 限流器：RPM / TPM 等额度，按服务商的额度范围建模，可被多个执行组共享 | `limit` |
| 指令 | 一步：`StepModel`、`StepTools` 或 `StepCompact` | `task` |
| 进程 / PCB | 任务 / TCB | `task` |
| 寄存器 | 发给模型的消息序列（服务商无关的中立格式） | `task` |
| 调度器 | 就绪队列 + 等待集合 + Deadline 定时器 | `sched` |
| 系统总线 / 设备 | Bus（仲裁、页表）/ builtin、MCP、skills | `bus` |
| DMA + 中断 | 异步派发工具调用，完成时发中断 | `bus`、`sched` |
| 预写日志 | 调用记录：派发前提交意图，完成后提交结果 | `mem` |
| L1 / L2 | 服务商 prompt cache / 只读工具结果缓存 | `model`、`cache` |
| 主存 / 磁盘 | SQLite（状态、调用记录、中断、事件，同一事务提交）/ 长期记忆接口 | `mem` |
| MMU | 每个任务的工具权限表 | `bus` |
| I/O 端口 | `Instruction` / `Event` 协议 | `proto` |

## 3. 消息与任务

### 3.1 中立消息格式

```go
type Role string // system / user / assistant / tool

type Message struct {
    Role    Role
    Parts   []Part
    Summary bool // 压缩放进来的摘要消息（5.3）；适配器照普通消息发
}

type Part struct {
    Kind string // text / tool_call / tool_result / reasoning

    Text string // text、reasoning 的可读部分

    CallID         string          // tool_call、tool_result：机器内部 id，形如 "c-17"
    ProviderCallID string          // tool_call：服务商返回的原始 id，回放给同一服务商时使用
    Name           string          // tool_call
    Args           json.RawMessage // tool_call
    IsError        bool            // tool_result

    // reasoning：服务商私有的推理块（例如带签名的 thinking）。
    // 只在发回同一个 Provider 时原样带上；发往其他服务商时丢弃。
    Provider string
    Opaque   json.RawMessage
}
```

工具调用的内部 id 由机器生成，任务内唯一。适配器发请求时：目标服务商与 `Provider` 相同则使用 `ProviderCallID`，否则用内部 id 生成该服务商要求格式的 id。

### 3.2 TCB

```go
type State int      // Ready, Running, Waiting, Suspended, Done, Failed, Cancelled
type PC int         // StepModel, StepTools, StepCompact
type WaitReason int // WaitIO, WaitBackoff, WaitReconcile, WaitGroup, WaitApproval

type TCB struct {
    ID         string
    State      State
    PC         PC
    Wait       WaitReason
    WakeAt     time.Time       // WaitBackoff 时有效；只在本次运行内有意义，重启后任务直接就绪
    Class      string          // 执行组；迁移时改变
    PinClass   bool            // true = 禁止迁移
    Priority   int             // 0 = 交互，1 = 后台
    ReadySince time.Time       // 进入就绪队列的时刻
    System     string          // 创建时确定
    Tools      []ToolDef       // 创建时按页表快照，按名字排序，之后不变
    Regs       []Message
    Allow      []string        // 页表
    Deny       []string
    Budget     Budget          // MaxSteps、Deadline（绝对时间）、MaxTokens；MaxSteps 约束 Used.Steps + Unconfirmed
    Used       Usage           // 已确认的用量（见 4.6）；Used.Steps = 有回答的模型调用数
    Unconfirmed int            // 已发出、但没有拿到响应的模型请求数
    CallSeq    int             // 已分配的工具调用 id 数；下一个是 c-<CallSeq+1>，只增不减，压缩后 id 也不重复
    Retries    int             // 连续的可重试模型失败（过载、临时错误）；成功清零；限流不计
    MaxOut     int             // 本任务模型调用的输出上限，0 = 适配器默认
    Truncations int            // 因"截断且无内容"放大过 MaxOut 的次数，最多 2
    Window     int             // 服务商以"超出上下文"拒绝请求后学到的窗口，0 = 用 Caps.Window；迁移时清零（5.3）
    CompactChunk int           // 进行中的压缩轮已换出的块数，不在压缩轮中为 0
    WindowAt   int             // 学到 Window 时的 Used.Steps；32 次有回答的调用后忘掉（5.3）
    CompactRaise int           // 进行中的压缩被截断后放大输出上限的次数，最多 2
    CompactLimit int           // 最大上限仍被截断后，下一块最多带几个单元，0 = 不限
    Archived   int             // 压缩移进归档的消息数（5.3）
    StopReason string
    Version    int64           // 每次提交 +1
}
```

工具调用的状态不在 TCB 里，在调用记录里（4.2）。插话等外部输入不在 TCB 里，在中断表里（4.3）。

## 4. 提交与恢复语义

这一节定义 neumann 的全部持久化保证，其余各节都以它为准。

### 4.1 提交

```go
type CommitSet struct {
    Task          *TCB         // 新状态；NewTask 时 Version 必须为 1，否则必须等于库里的 Version + 1
    NewTask       bool
    Calls         []CallRecord // 新增或更新的调用记录；所属任务必须存在
    NewInterrupts []Interrupt  // 外部输入；任务不存在或已结束时整个提交被拒绝
    Applied       []int64      // 把 Task 的这些未送达中断标记为已送达；序号不属于 Task 或已送达则拒绝
    Archive       []Message    // 追加到 Task 的归档：压缩换出的原始消息；必须带 Task，与提交同进同退
    Events        []Event      // 本次提交产生的事件；外部指令本身由 accepted 等事件记录
}

type Store interface {
    Commit(ctx context.Context, c CommitSet) ([]Event, error) // 一个事务；返回带好 Seq / TaskSeq 的事件
    Load(ctx context.Context, id string) (*TCB, error)
    Unfinished(ctx context.Context) ([]*TCB, error)
    Calls(ctx context.Context, taskID string) ([]CallRecord, error)
    Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error)
    Events(ctx context.Context, afterSeq int64, limit int) ([]Event, error)
    Archive(ctx context.Context, taskID string) ([]Message, error) // 按提交顺序
}
```

规则：

1. **任何状态转换都通过 `Commit` 完成**，无论是 vCore 执行完一步，还是调度器处理工具完成、退避到点、Deadline、挂起、改优先级、迁移。
2. **先提交，后动作。** 顺序永远是：算出新状态 → `Commit` 成功 → 才去改内存里的队列、通知订阅者、派发工具、给调用方返回成功。`Commit` 失败则这次转换不存在。
3. **状态、调用记录、中断、事件在同一个事务里。** 客户端看到的任何事件，重启后都能在状态里找到对应的结果。
4. **取消以提交为准。** 任务有未送达的取消中断时，`Commit` 只接受它的最终提交（终态，`StopReason` 等于该取消的原因），也不接受把它的调用写成 `dispatched`，都返回 `ErrCancelPending`；结果、晚到结果、新中断照常记录。所以先提交的取消赢，持有任务的一方即使还没收到取消信号，提交被拒时也就地做最终提交。
5. 写入者单线程，组提交：写入者正忙时到达的 `CommitSet`（最多 128 个）合成下一个事务，不设等待窗口；每个调用方在自己所属的批次提交后才返回。

两个实现：`mem.Memory`（不持久，测试用）和 `mem/sqlite`（纯 Go 驱动 `modernc.org/sqlite`，WAL，`synchronous=FULL`，macOS 上加 `fullfsync`，事务开始即取写锁）。两者都必须通过 `mem/storetest` 一致性套件，它把语义钉死在同一组用例上，包括每个字段的完整往返。SQLite 版：TCB 和调用记录用 gob 编码（模型可能给出不是合法 JSON 的工具参数，要逐字节保存）；单写入者（Open 对 `<path>.lock` 取独占 flock，第二个 Open 得到 `ErrLocked`；非 unix 平台不强制）；正在写一个事务时到达的提交排队进下一个事务（组提交，不加等待窗口）；每个提交集一个 savepoint，校验失败只回滚它自己。

### 4.2 调用记录：工具的预写日志

```go
type CallStatus string // planned, dispatched, done, unknown, cancelled, late

type CallRecord struct {
    TaskID   string
    CallID   string
    Device   string
    Tool     string
    Args     json.RawMessage
    Effect   Effect          // 来自设备声明，见第 10 节
    Index    int             // 在任务内的调用顺序；结果按它排列
    Status   CallStatus
    Attempt  int
    Key      string          // 幂等键：taskID/callID/attempt，作为参数旁的元数据传给设备
    Result   *ToolResult

    // 工具策略与人工批准（9.7），在第一次派发之前决定，与调用同一次提交
    Policy   string          // allow / deny / rewrite / approval；空 = 规划时没有配置策略
    Approval string          // pending / allowed / denied / edited：操作员的回答
    Reason   string          // 被拒或待批准的原因
    OrigArgs json.RawMessage // 策略或操作员换掉参数时，模型原来的参数；Args 是发给设备的
}
```

生命周期：

1. 模型返回工具调用时，先由工具策略逐个决定（9.7），`execModel` 的同一次提交里写入这些调用，状态直接是 `dispatched`（`Attempt = 1`），任务转 `Waiting(IO)`（被拒的调用直接 `done`；有调用要批准时整轮留在 `planned`，任务转 `Waiting(Approval)`）；提交成功、任务交还调度器之后，同一个 vCore 立刻把调用交给总线。一轮工具不需要再排一次队等 vCore：实测在 cpa 上，派发曾因此等了 8 秒。
2. `planned` 状态留给"已规划、从未派发"的调用（重启恢复时出现）；`execTools` 处理这种任务：**先提交** `dispatched`、`Attempt + 1`，任务转 `Waiting(IO)`，**然后**派发。
3. 每个调用完成后，调度器提交 `done` 和结果，同一事务里带上 `tool_result` 事件。
4. 全部 `done` 时，调度器提交：按调用顺序把结果追加成 tool 消息，任务转 `Ready`、`PC = StepModel`。这次提交把所有 `done` 记录和任务一起再写一遍（幂等），保证模型看到的每个结果都在调用记录里，即使第 3 步的某次单条提交失败过。

崩溃后重启，每条记录按状态处理：

| 状态 | 处理 |
|---|---|
| `planned` | 从未离开进程，正常派发；从没决定过的先问工具策略（9.7）。`Approval = pending` 的这一轮不在 `Waiting(IO)` 里，见 9.7 |
| `dispatched`，`Effect` 为 `ReadOnly` 或 `Idempotent` | 用同一个 `Key` 重新派发（`Attempt` 不变） |
| `dispatched`，`Effect` 为 `SideEffect` | 状态改为 `unknown`。工具声明了 `ReportUnknown` 的，把"结果未知"作为错误结果交给模型；否则任务转 `Waiting(Reconcile)`，发 `reconcile_needed` 事件（带调用 id、工具、参数、幂等键）。不自动重试，也不假装失败。同一轮里只要有一个调用在等核对，这一轮什么都不发 |
| `done` | 已有结果，不再执行 |

`Waiting(Reconcile)` 的任务由操作员用 `m.Resolve(ctx, taskID, callID, Outcome)` 处理：给出实际结果（`Result`，失败时 `IsError`），或要求重试（`Retry`：新的一次尝试、新的幂等键）。所有等核对的调用都处理完，这一轮就按上表重新结算。`ReportUnknown` 按工具声明（`ToolDef.ReportUnknown`），默认是等待人工核对。

调用记录在规划时就记下 `Device`、`Effect` 和 `ReportUnknown`：恢复按当时的声明判断，不去问重启后的总线。`Effect` 按名字存储，不依赖常量顺序。重启时被打断的一轮，tool 消息从调用记录组装（已完成的结果加上重发得到的结果）。

保证范围：**每个调用恰好一个结果**；`ReadOnly` / `Idempotent` 的调用可能执行多于一次；`SideEffect` 的调用在没有人工核对的情况下**最多自动派发一次**。外部副作用恰好一次需要设备自己利用 `Key` 去重，neumann 不承诺。

### 4.3 外部输入

`Submit`、`Steer`、`Cancel`、`Suspend` 等外部指令，在 **提交之后** 才返回成功：

- `Submit`：提交新 TCB 和 `accepted` 事件后返回任务 id。返回成功 = 已持久化。
- `Steer` 等：作为一条中断写入中断表并提交后返回。返回成功 = 已持久化，之后一定会被送达，或以 `steer_dropped` 报告。
- 任务已结束时返回 `ErrFinished`；不存在时返回 `ErrNotFound`。

插话送达时以一条用户消息出现，内容经 `Config.SteerFormat` 渲染。默认格式会注明"这是你工作期间用户发来的，请和原请求一并考虑"：实测裸发"Also tell me the weather there."时，模型只答了天气，丢掉了刚查到的人口。

中断表与 TCB 分开，正是为了让外部输入可以在任务被某个 vCore 持有时立刻持久化，而不违反单一所有者（第 8 节）。持有任务的一方在下一次提交里把它取用的中断标记为 `applied`，与它造成的状态变化在同一事务里。

任务进入终态的那次提交，把所有未送达的插话标记为 `applied` 并为每条写一个 `steer_dropped` 事件。

### 4.4 事件

- 每个事件有全局序号 `Seq`（单调递增）和任务内序号 `TaskSeq`。
- 事件只在提交成功后发布给订阅者。
- 订阅：`m.Events(ctx, fromSeq)`；`m.EventLog(ctx, afterSeq, limit)` 直接读。第一阶段没有内存环形缓冲，订阅者每次被唤醒都从 Store 按序号读，语义相同。订阅者先从内存环形缓冲读；落后太多、超出缓冲时，自动改为从 `Store.Events` 按序号补读，追上后再回到内存。**生产者从不阻塞，订阅者也不丢事件。**
- 第一版事件表不自动清理；清理策略（按时间或按已确认的序号）属于运维配置，留到有真实数据量再定。

### 4.4.1 流式增量

模型调用默认流式：适配器实现 `model.Streamer` 就走流式，正文和推理的增量实时推给 `m.Deltas(ctx)` 的订阅者（`proto.Delta{TaskID, Step, Text, Reasoning}`）。增量是预览，不提交、没有序号：订阅者落后超过 1024 条就丢（计入 `Status.DeltasDropped`），推送从不阻塞 vCore；失败或被打断的一步可能已经流出过文字。记录以提交的 `answer` 事件为准。模型不支持流式时，`Start` 记一条日志——包一层只转发 `Call` 的适配器会悄悄关掉流式，这是实际踩到过的。

实测 cpa 上的 gemini-3.8-flash-high：它先在后台推理、推理不流出，首段正文只比整段回答早 9–75ms。流式的收益取决于模型：推理可见或回答较长时才明显。

### 4.5 重启

1. 加载所有未结束的 TCB、它们的调用记录和未送达的中断。
2. `Running` → `Ready`（这一步重做，可能重复一次模型调用的花费，`Unconfirmed + 1`；`PC` 保留：压缩调用中途崩溃，重启后重做这次压缩）。`Running` 在模型调用之前提交，含义是"请求可能已经发出"：提交之后、调用之前被停机打断时，会多算一次未确认。这一步要送达的插话在 `Running` 提交之后才合并，崩溃不会留下"上下文里有、中断表里却还未送达"的插话。预算按 `Used.Steps + Unconfirmed` 计，反复崩溃的任务不会越过预算重发。
3. 调用记录按 4.2 的表处理。
4. 按 `Class`、`Priority`、`ReadySince` 重建就绪队列；所有未结束任务重设 Deadline 定时器。`Waiting(Reconcile)` 和 `Waiting(Approval)` 保持原状，各自再宣布一次等待的调用（9.7）。`Waiting(Backoff)` 和 `Waiting(Group)` 提交为 `Ready`（`ReadySince` 不变，`Retries` 保留）：退避定时器和熔断状态都不持久，重启后各组从闭合开始，退避从下一次失败重新起算。
5. `Class` 已不在配置里的任务（换了配置重启），在第 2、3 步之前移到 `Config.Default` 的组：先做 6.3 的能力校验，通过则提交 `Class` 改变（状态不变）和 `migrated`（`reason = "class_removed"`）；不通过则提交一个原因为 `class_removed` 的取消中断和 `migrate_refused`（带不满足的那一条），随后按取消做最终提交（调用照 9.5 处理），任务 `Failed`，`StopReason = "class_removed"`。不留没有 vCore 会取的任务。
6. 各执行组的 `W` 回到 `Initial`，vCore 按恢复出的就绪队列启动（6.1：有积压才启动，`Min` 只是空闲回收的下限）；限流器从空状态开始，由服务商响应头重新校准（7.3）。

### 4.6 计量

- `Used` 只累计**已确认**的用量，也就是从服务商响应里读到的。`Used.Steps` 是**有回答的**模型调用数：最终答案、要工具的一轮、被截断后重做的那一次、压缩调用（5.3）都算；以任何错误类别失败的调用（限流、过载、临时、永久、超出上下文、额度耗尽、`Unknown`、panic、被取消打断）都不算。失败的调用由 `MaxModelRetries` 约束，所以限流风暴不会让任务以 `budget` 结束。
- 请求已发出但没拿到响应（响应丢失、调用被取消打断、进程崩溃），服务商可能已经处理并计费，本地无法知道。这种情况只计入 `Unconfirmed`，不计入 `Steps`（不重复计数），不估算 token。
- 预算按 `Used.Steps + Unconfirmed` 计。`step` 事件是每次调用尝试一个（失败的也有），所以 `step` 事件数 = 适配器调用次数 = `Used.Steps` + 失败的调用数（每个失败的调用对应一个非截断的 `model_retry`，或一个 `model_error` 的 `failed`，或一个取消）。
- `Status` 和 `usage` 事件给出的是已确认用量和未确认请求数，两者分开报告。

## 5. vCore 循环

```go
for {
    t, ok := sched.Next(ctx, group) // 取指；无就绪任务时阻塞；被要求退役时返回 false
    if !ok { return }
    var c CommitSet
    switch t.PC {
    case StepModel:   c = execModel(t)   // 一次模型调用
    case StepTools:   c = execTools(t)   // 只算出 dispatched 的提交，派发在提交之后
    case StepCompact: c = execCompact(t) // 一次模型调用
    }
    if halted() { sched.Forget(t.ID); continue }     // 强制停机打断的指令什么都不提交
    if err := mem.Commit(ctx, c); err != nil { sched.Forget(t.ID); continue }
    sched.Requeue(t)                                  // 先交还任务……
    if t.PC == StepTools { bus.Dispatch(c.Calls) }    // ……再派发，批次完成时才能从等待集合取到它
    group.AfterStep(result)
}
```

每条指令开头先把未送达的中断（来自中断表）合并进寄存器，合并结果和这一步的状态变化一起提交。

vCore 只在两条指令之间退役，从不在指令中途被杀。

### 5.1 execModel

1. 检查 `MaxSteps`、`MaxTokens`；超出则提交 `Done`，`StopReason = "budget"`，答案是最后一条 assistant 文本，**不算一条模型指令**（不发 step 事件）。未送达的插话由终态提交报告为 `steer_dropped`。Deadline 由调度器负责（第 9 节）。
2. 按 5.3 量窗口：`估算(System + Tools + Regs) + 输出预留 > CompactAt × 窗口`（或任务正处在压缩轮中）时，这条指令改为压缩（`execCompact`，`PC = StepCompact` 随 `Running` 提交）；怎么压都放不下时 `Failed`，`StopReason = "context_overflow"`，不调用、不算指令。窗口未知不压缩。
3. 本组熔断中（不闭合、也没到放探测请求的时候）：不预占、不调用，任务提交 `Waiting(Group)`。否则向本组的限流器预占额度（7.2），等额度时 vCore 持着任务、什么都不提交；拿到额度后 `Admit`，不放行则退还额度、任务转 `Waiting(Group)`；放行则提交 `Running`，通过适配器调一次模型。等额度和调用都以任务的 context 进行，取消和 Deadline 会立刻打断。调用结束先与限流器结算（7.2），再把结果报给执行组（第 6 节），然后才组装任务自己的提交。
4. 结果：
   - **有工具调用**：assistant 消息原样追加，调用记录写为 `planned`，`PC = StepTools`。
   - **最终文本**：调用期间到达了插话，则文本作为 assistant 轮次，`PC = StepModel`，**插话留在中断表里不合并**：下一条模型指令开头再送达，若下一步因预算或模型出错而结束，它由终态提交报告为 `steer_dropped`。没有插话则 `Done`。
   - 模型调用 panic 等同于永久错误：任务 `Failed`，`StopReason = "model_error"`，事件里的错误是 `model panicked: …`；不报给执行组（说明不了服务商的状况）。适配器的钩子（`Classify`、`RateInfo`、`Metered.Reserve`、`Metered.Settle`）在同样的保护下运行，panic 一律按调用 panic 处理（`model panicked in <钩子>: …`），预占全额退回，调用即使有回答也不采用。
   - **截断且没有内容**（`finish_reason = "length"`，没有文本也没有工具调用）：按任务自己的上限乘 4 重做（`MaxOut` 为 0 时从适配器的 `model.OutputCap` 起算，没有则 4096），连续最多两次（`Truncations`），第三次 `Failed`，`StopReason = "truncated"`。放大后的上限不超过本组限流器 `output_tokens` / `tokens` 维度的最小 `Limit`（再大就永远预占不下）；放大后不比刚发的大时，不再重做，直接 `truncated`。重做的那次调用有回答，计入 `Used.Steps`，发 `model_retry`（`class = "truncated"`，带新的 `max_out`），这一步合并的插话退回未送达，重做时再送达。放大只管这一串截断：下一次有回答且没被截断的调用把 `Truncations` 清零，之后的调用回到任务自己的 `MaxOut`，不会每次都按 4–16 倍预占输出。
5. 提交里带 `usage` 事件（已确认用量）。成功（包括截断）清零 `Retries`。出错见第 11、13 节。

### 5.2 execTools

只用于调用处于 `planned`、从未派发的任务（重启恢复）。从没决定过的调用先问工具策略（9.7）；提交调用记录 `dispatched` 和任务 `Waiting(IO)`；提交成功后派发。vCore 不等结果。正常路径上，模型那一步已经派发了自己的调用（4.2 第 1 步）。

### 5.3 execCompact

用一次模型调用把最早的若干个完整轮次（assistant 加上它的全部 tool 结果，从不拆开）换出成一条摘要消息。被换出的部分按"压缩请求本身必须放得进本组窗口"来切块：一块放不下就分几次压缩，每次一条指令。单条消息本身就超过窗口时，任务 `Failed`，`StopReason = "context_overflow"`。

实现中的决定（`internal/core/compact.go`，寄存器的切分和渲染在 `task/compact.go`）：

- **窗口**：`model.CapsOf(适配器).Window`；任务的 `Window`（服务商拒绝过时学到的）更小或 Caps 未知时用它。两者都没有：不主动压缩，只在服务商以"超出上下文"拒绝时压缩。
- **触发**：每条模型指令开头（预算、熔断检查之后），`估算(System + Tools + Regs，按目标服务商过滤 reasoning) + 输出预留 > CompactAt × 窗口` 时压缩。`CompactAt` 按组配置（`GroupConfig.CompactAt`），默认 0.8。输出预留：这次请求带的 `MaxOut`（截断放大后的也算），否则 `Caps.MaxOut`，否则 4096。估算就是 `task.EstimateTokens`，与限流预占、迁移校验同一个。 送达前的插话也算在内：下一条模型指令会把它们加进寄存器，一条很长的插话因此先触发压缩，而不是让请求越过阈值或被服务商拒绝。
- **单元**：寄存器从头切：头部到第一条 user 消息为止（任务的请求）永远原样保留；之后每个 assistant 消息连同紧跟着回答它的 tool 消息是一个单元（"轮次"），每条 user / system 消息（插话、之前的摘要）各自是一个单元。单元整体保留或整体换出，所以换出后不会有没有调用的结果、没有结果的调用（配对不变式，`task.CheckPairing`）；还在等结果的轮次和它之后的都不换出；等批准（`Waiting(Approval)`，9.7）的一轮在批准前没有模型指令运行，批准后照常派发、补齐结果，之后才可能被压缩。已经送达的插话是普通 user 消息，可以被压缩。
- **保留**：最新的若干单元，合计估算不超过 `CompactKeep × 窗口`（默认 0.3），至少最新一个。中间的就是要换出的部分。
- **切块**：从最早的单元起，取请求放得进窗口的最长一段：`估算(压缩提示词 + 这段的文字稿) + 摘要输出上限 ≤ 窗口`。上一块的摘要排在下一块开头，跟着一起压，一轮压完只剩一条摘要。摘要带不上下一个单元时，那个单元单独压（它的摘要排在旧摘要后面，下一块再合并）。只剩一条摘要可压时这一轮结束；两条以上摘要合并。每块一条指令，`PC` 保持 `StepCompact`，`CompactChunk` 计数，换完回到 `StepModel`。块的估算按各单元转义后文字稿字节数相加算，与整段 `EstimateTokens` 逐字节一致（有测试），不用每个候选块都序列化一遍。
- **放不下**：必须压（请求加预留超过窗口，或服务商拒绝了）时先判断能不能压到放得下：头部 + 保留的单元 + 一条按上限算的摘要 + 输出预留 ≤ 窗口（保留 30% 不行就只留最新一个单元再试），且每个要换出的单元单独放得进压缩请求。不行就直接 `context_overflow`，不做无用的压缩、也不调用：单条消息（或一轮）本身就超过窗口、请求本身就超过窗口，都在这里。只是超过阈值、还放得进窗口时，有单元放不进压缩请求就先不压，照常调用。
- **压缩请求**：系统提示词是固定常量 `task.CompactSystem`（要求忠实摘要，保留事实、数字、标识、决定、后面要用的工具结果、失败的尝试、用户后来的指示、未决问题、当前计划和下一步），没有工具，一条 user 消息装这段的文字稿（`task.Transcript`：每条消息 `### <角色>`，工具调用和结果带 id，旧摘要单独标出，reasoning 不带），`MaxOut = min(2048, 窗口/8, Caps.MaxOut)`。同一段永远渲染出同样的字节。发文字稿而不是原样的轮次：带 `tool_use` 的请求在有的服务商那里必须同时带工具定义，而压缩请求不给工具。
- **摘要消息**：role 为 user、`Summary = true`，正文以固定的 `task.SummaryHeader`（"这是之前工作的摘要……"）开头。选 user：紧跟在任务请求之后，Anthropic 适配器把相邻的同角色消息并成一个 user 回合，OpenAI 兼容接受连续的 user 消息；下一个保留单元是 assistant 时角色照常交替。两个适配器都有渲染测试。
- **一条模型指令**：压缩调用走与模型步完全相同的路径（`modelCall`：熔断、预占、`Admit`、`Running` 提交、调用、结算、上报、分类），失败也一样处理：退避、等组、迁移、`model_error`；退避和等组时 `PC` 保持 `StepCompact`，回来接着压。有回答就计入 `Used.Steps` 和用量，清零 `Retries`，发 `step`（`pc = "compact"`）和 `usage`；不流式推送增量。回答没有文字（也没被截断）按临时错误重试。这一步不送达插话：插话留在中断表里，下一条模型指令送达。
- **截断**：压缩的回答 `finish_reason = "length"`（有没有文字都一样）永远不当摘要用：被截一半的摘要会悄悄丢掉其余部分。按模型步截断的办法重做（`CompactRaise`）：输出上限乘 4，最多两次，不超过窗口的一半和本组限流器 `output_tokens` / `tokens` 的最小 `Limit`，放大后这一块的第一个单元还得放得进请求；块随上限变大自动变小。推理模型常把小上限全花在推理上，没有这一步它会按临时错误以同一上限重试到 `model_error`。最大上限仍被截断：拆块（`CompactLimit` 减半，下一块最多带那么多单元，直到 1），`model_retry` 的 `wait` 为 `split`。一个单元的摘要在最大上限仍被截断：`Failed`，`StopReason = "truncated"`（与模型步的截断同一个停止原因，错误文字写明是压缩），寄存器和归档不动。每次被截断的调用都有回答，计入 `Used.Steps`；压缩成功一次，两个计数清零。估"压完放不放得下"时摘要仍按放大前的上限算。
- **先提交后动作**：结果一次提交：新 `Regs`（摘要替换这一块）、`Archive`（这一块里除旧摘要以外的原始消息）、`Archived` 加上它们的条数、`compacted` 事件 `{rounds, messages_before, messages_after, tokens_before_est, tokens_after_est, chunk, chunks}`（`chunks` 是切这一块时按每条摘要满上限估出的总块数），任务 `Ready`。崩溃在 `Running` 之后：重启按 `PC` 重做这次压缩（`Unconfirmed + 1`）；提交落盘之后：新寄存器已经生效。不存在只做了一半的压缩。有未送达的取消时这次提交被拒（4.1 规则 4），按取消最终提交：寄存器、归档不变，有回答的调用仍计入用量。
- **归档**：原始消息不丢，存在 Store 的归档里（`CommitSet.Archive`、`Store.Archive`，SQLite 一张 `archive` 表，消息 gob 编码），与任务同一个事务。选 Store 而不是事件载荷：事件表按序号读，要拿一个任务的全部历史得扫全表；归档按任务读，且与寄存器的变化原子提交。`m.History(ctx, id)` 返回任务的完整历史：头部 + 归档（取 TCB 里 `Archived` 那么多条，两次读之间又压缩了也不错乱）+ 寄存器里摘要之后的消息，不含摘要。每块总是取最早的未压缩单元，所以归档顺序就是时间顺序。
- **服务商拒绝**：新的错误类别 `model.ContextLength`（第 11 节）。不按永久错误失败：任务学到窗口 `(估算(被拒的请求) + 它的输出上限) × 3/4`（比已学到的小才更新），按它必须压缩，提交 `Ready`、`PC = StepCompact` 和 `model_retry`（`class = "context_length"`，`wait = "compact"`）；压不下就 `context_overflow`。被拒的调用不算 `Used.Steps`、不计 `Retries`、预占全额退回，对执行组算一次回答（同 `Permanent`）。压缩请求本身被拒同样处理，块切得更小。每次拒绝都让窗口变小，每块压缩都减少剩余的原始单元或摘要数，所以不会无限循环。 学到的窗口不是永久的：之后有回答的请求比它大，说明窗口至少有那么大，`Window` 随之放大；学到后再有 32 次有回答的调用（`WindowAt`），忘掉它，下一次拒绝再学（网关偶尔把请求路由到小模型时，任务不会一辈子按小窗口压缩）；代价是至多每 32 步一次拒绝。
- **预算结束时的答案**：`budget` 结束的 `answer` 取最后一条有文字的 assistant 消息；寄存器里没有、而压缩已经把它换进归档时，从归档取（寄存器里剩下的总比归档里的新）。
- **代价**：任务的头部（系统提示词、工具、请求）加输出预留本身接近阈值减去保留部分时，几乎每步都会压缩一块；这是配置问题（`CompactKeep` 必须小于 `CompactAt`，`New` 校验）。

## 6. 执行组与弹性 vCore

```go
type GroupConfig struct {
    Class      string
    Model      model.Adapter
    Limits     []string      // 引用的限流器名字，可以多个（例如组织级 + 模型级）
    Min        int           // 默认 1
    Max        int           // 0 = 不设上限
    Initial    float64       // 初始窗口，默认 2
    IdleRetire time.Duration // 默认 30s
    Cooldown   time.Duration // 减半后不增长的时间，默认 10s
    BreakerThreshold int           // 默认 5
    BreakerOpenFor   time.Duration // 默认 30s
    Fallback   string        // 熔断时迁往的组
    OnMigrateFail string     // "wait"（默认）或 "fail"
    MaxQueued  int           // 本组未结束任务上限，0 = 只受全局限制；迁入的任务不受它限制
    CompactAt   float64      // 压缩阈值，占窗口的比例，默认 0.8（5.3）
    CompactKeep float64      // 原样保留的最新轮次占窗口的比例，默认 0.3；必须小于 CompactAt
}
```

`New` 时校验：类名非空、不重复、不含 `/`；每组有 `Model`；`0 ≤ CompactKeep < CompactAt ≤ 1`（0 取默认）；`Limits` 里的名字都在 `Config.Limiters` 里；`Fallback` 指向存在的组，沿 `Fallback` 链走不会回到起点（无环）；`OnMigrateFail` 只能是 `wait` / `fail`；`Max > 0` 时不小于 `Min`。`Config.Default` 是不带 `Class` 提交的任务的类，空则取第一个组。

单组简写：`Config.Groups` 为空时，`Model`、`Class`（默认 `default`）、`Cores`（默认 4）、`Limits` 构成一个组，窗口用 `SetCores(Cores)` 固定：至多 `Cores` 个 vCore、有活才启动，和以前的固定 vCore 数一致；`AutoScale(class, true)` 可让它自适应。简写字段与 `Groups` 同时出现是配置错误。

### 6.1 拥塞窗口只管并发

窗口 `W` 只回答"同时向这个服务商发多少个请求"，不负责额度（额度归限流器，第 7 节）。

- **加性增长**：模型调用成功，`W += 1/W`。冷却期内不增长。
- **乘性减少**：只对**过载**信号（第 11 节分类）：`W = max(Min, W/2)`，冷却 10 秒。
- **每个拥塞周期只减一次**：执行组有一个周期号 `epoch`，每个请求发出时记下当时的 `epoch`。收到过载信号时，只有 `请求的 epoch == 当前 epoch` 才减半并让 `epoch + 1`。同一批在途请求各自返回的过载，只触发一次减半。
- **扩容**：本组就绪队列非空、所有 vCore 都忙、`n < floor(W)` 时，启动新 vCore。所以初始 `W = 2` 时，一有积压就能直接扩到 2 个，之后靠成功调用继续增长。
- **缩容**：`n > floor(W)` 时，最先执行完当前指令的 vCore 退役。
- **空闲回收**：空闲超过 `IdleRetire` 且 `n > Min` 时退役，不改变 `W`。
- **手动**：`SetCores(class, n)` 固定 `W = n` 并关闭自动调整；`AutoScale(class, true)` 恢复。

vCore 增减发 `core_up` / `core_down` 事件，原因：`demand`、`overload`、`idle`、`manual`、`shutdown`。

实现（`internal/core/cores.go`，计数和决策全在 `internal/group` 的 `Scaler`）：

- `Grow(class)`：`Scaler.Grow(就绪队列长度)` 给出要启动的个数 k（积压、全忙、`n < floor(W)`），立即计入存活，启动 k 个 goroutine。三处调用：任务进入本类就绪队列（调度器 `OnReady`，在调度器锁外）；vCore 取到指令 `Acquire` 之后（取指和 `Acquire` 之间别的 `Grow` 看到"有空闲 vCore"而不扩，这里补上）；vCore 空闲退役之后（退役那一刻到达的任务不会没人取）。`SetCores` / `AutoScale` 之后也调一次。
- vCore 循环：`NextIdle(class, IdleRetire)` 取指；取到 → `Acquire` → 执行一条指令 → `Release`，`Release` 说退役就退出（`overload`，窗口被操作员固定时 `manual`）；等满 `IdleRetire` 没活 → `IdleTimeout(空闲时长)`，`n > Min` 才退役（`idle`），否则接着等；调度器关闭或取指 context 结束 → `Stop`（`shutdown`）。所以 vCore 只在两条指令之间退役。
- 不预先启动 `Min` 个 vCore：没活的组不占 goroutine，`Min` 只是空闲回收的下限。
- 窗口约束并发的方式就是 vCore 数：一个 vCore 同一时刻至多一个模型请求，`n` 只在 `n < floor(W)` 时增长、`n > floor(W)` 时最先执行完的退役。窗口刚缩小时、或 `SetCores` 调低时空闲的 vCore，可能再执行一条指令才退役，超出是暂时的。
- `core_up` / `core_down` 是机器事件 `{class, core, reason, live}`，由该 vCore 自己的 goroutine 单独提交（`core_up` 在第一次取指之前），不阻塞触发扩容的一方；提交失败只记日志。`core` 是全机唯一的编号，和 `step` 事件里的 `core` 对得上。
- `Stop`：先禁止启动新 vCore，再关调度器，等全部 vCore 退出（`core_down` `shutdown`）。

### 6.2 熔断

- 同组连续 5 次指令以临时错误失败（成功一次清零），或适配器报告额度耗尽（第 11 节），本组熔断：停止发请求，发 `group_down`。
- 临时错误熔断 30 秒后半开：放一个请求试探，成功则 `group_up`。额度耗尽熔断不自动半开，等操作员 `group_reset`。
- 熔断期间，本组就绪的任务转 `Waiting(Group)`：惰性的，vCore 取到它时才提交，不调用、不预占。没有 `Fallback`、或任务 `PinClass` 时，就这样等到恢复或 Deadline。
- 等在 `Waiting(Group)` 的任务没人取，所以有任务停进来时挂一个半开定时器（机器时钟）：到放探测请求的时刻，唤醒最早的一个去发探测，并再挂一次（探测的任务被取消、探测丢失时，`OpenFor` 之后换一个）。探测成功（或得到任何非临时错误的回答）即 `group_up`，唤醒本组全部 `Waiting(Group)` 任务。额度耗尽熔断不挂定时器，只有 `m.GroupReset(ctx, class)`：熔断复位、`group_up`（带 `reset`）、唤醒。唤醒 = 提交 `Ready`，`ReadySince` 不变。任务停进来与 `group_up` 交错时，停进来的一方交还后再看一次熔断状态，已闭合就自己唤醒，不会漏。
- `group_down` / `group_up` 是机器事件（没有任务），单独提交：熔断状态不持久，没有可以一起提交的状态。

### 6.3 迁移

有 `Fallback` 且任务未 `PinClass` 时，尝试迁往 `Fallback` 组。迁移前做能力校验：

1. **工具能力**：目标适配器必须支持工具调用；任务快照的工具数不超过目标适配器的上限。
2. **上下文**：`估算(System + Tools + Regs) + MaxOut 预留 ≤ 目标窗口`。
3. 第 2 条不满足时，检查能否在目标组上压缩到放得下：按 5.3 切块，每块压缩请求都必须放得进目标窗口，压完（头部 + 保留的轮次 + 一条摘要 + 输出预留）放得进目标窗口。能，则迁移后第一条指令就是 `StepCompact`。
4. 都不满足：按 `OnMigrateFail`，`wait` 则留在 `Waiting(Group)`，`fail` 则 `Failed`，`StopReason = "migrate_incompatible"`，事件里带不满足的那一条。

校验通过：提交 `Class` 改变、`ReadySince` 保留（不重新排队尾）、`migrated` 事件。迁移后第一次请求丢掉原服务商的 `reasoning` 块，L1 必然不命中。

实现中的决定（`internal/core/migrate.go`）：

- 触发点：`execModel` 发现本组熔断中（取到任务时、或 `Admit` 不放行时），以及调用以 `QuotaExhausted` 失败、熔断刚打开时。三处都走同一个 `groupGone`。
- 目标沿 `Fallback` 链找：跳过熔断中的组，取第一个通过校验的。链上的组全在熔断中：留在 `Waiting(Group)` 等本组，不算迁移失败；之后任何一个组恢复（`group_up`、`GroupReset`），除了唤醒它自己的 `Waiting(Group)` 任务，也唤醒 `Fallback` 链经过它的那些组里未 `PinClass` 的等待任务，让它们重新走一遍迁移判断（本组若是额度熔断，永远不会半开，不这样就永远等下去）；停进来与恢复交错时，交还后再看一次链上有没有组已恢复。校验不过而停下的任务（发过 `migrate_refused`）不在交还后重试，后备组再恢复也不唤醒它（后备组能接什么没变，唤醒只会再拒一次）：它等本组的 `group_up` / `GroupReset`，或操作员 `SetClass`（这个标记只在内存里，重启后任务本来就直接就绪、重新判断）；有没熔断但校验不过的：按**本组**的 `OnMigrateFail`，`wait` 时停进 `Waiting(Group)` 并发 `migrate_refused`（`{from, to, check, detail}`），`fail` 时 `failed` 事件带 `check`、`detail`。
- 能力取自 `model.CapsOf(适配器)`（在 panic 保护下读，适配器 panic 即校验不过，`check = "caps"`）：适配器实现 `model.CapsReporter`（`Caps() Caps{Window, MaxOut, Tools, MaxTools}`）就用它；没实现的按"支持工具、数量不限、窗口未知"，窗口未知（0）不拒绝。OpenAI 兼容适配器：`ContextWindow` 默认 0（网关背后模型不定），`MaxTools` 默认 128，`NoTools`；Anthropic：`ContextWindow` 默认 200000，`MaxTools` 默认不限，`NoTools`。
- 估算用 `task.EstimateTokens`：JSON 字节数 / 4 + 1，与 7.2 默认预占同一个估算；按目标服务商实际会收到的消息算（去掉外来 reasoning）。输出预留：任务的 `MaxOut`，否则目标的 `Caps.MaxOut` / `OutputCap`，否则 4096。
- 第 3 条用 5.3 的同一套判断（`canCompact`，按目标的 Caps 和组的 `CompactKeep` 算），通过则迁移提交里 `PC = StepCompact`；不通过才是 `check = "context"`，`detail` 写明哪一条放不下（某一轮放不进压缩请求，或请求和最新一轮本身就放不下）。迁移清掉任务学到的 `Window` 和 `CompactChunk`（换了服务商）；`SetClass`、类被删除时的 `Rehome` 同样，任务在等工具时保留 `StepTools`，下一条模型指令按新窗口量。
- reasoning 由 vCore 在组请求时过滤（`forProvider`）：`Provider` 非空且不等于目标适配器 `Provider()` 的 reasoning 块不发，块全被去掉的消息不发，它两边同角色的消息并成一条（角色仍然交替，严格的服务商不会收到非法序列）；寄存器原样保留，迁回原服务商时还在。两个适配器本来也按 `Provider` 丢弃外来 reasoning（Anthropic 丢 thinking，OpenAI 兼容不渲染 reasoning），过滤在 vCore 做是为了对任何适配器成立。
- 迁移提交：`State = Ready`、`PC = StepModel`、`ReadySince` 不变、`Retries` 不变、`migrated`（`{from, to, reason: "group_down"}`）；交还后进新组的就绪队列，触发新组扩容。准入计数随之从旧类移到新类。
- 手动迁移 `m.SetClass(ctx, id, class)`：同样的校验，不通过返回 `*MigrateError`（`Check`、`Detail`），什么都不提交；`PinClass` 只禁止自动迁移，不拦操作员。只能在指令之间移动：任务在就绪队列、`Waiting(Group / Backoff / Reconcile)`、`Suspended` 时取出、提交、交还；在 vCore 上或等工具时返回 `ErrTaskBusy`，调用方重试。等旧组的 `Waiting(Group)` 改为 `Ready`，其他状态不变（退避保留 `WakeAt`）；`migrated` 的 `reason` 为 `manual`。

## 7. 限流器

### 7.1 为什么和执行组分开

服务商的额度范围和模型不一定一一对应：一个组织下多个模型可能共享 TPM，一个模型可能同时受 RPM、输入 TPM、输出 TPM 限制，额度还可能按天耗尽。所以限流器是独立的对象，执行组按名字引用；一个请求要同时拿到它引用的所有限流器的额度才能发出。

```go
type Rate struct {
    Per   time.Duration // 例如 1 分钟
    Limit int64
}

type Config struct {
    Name   string
    Limits map[string]Rate // 维度名 → 速率，例如 "requests"、"tokens"、"input_tokens"、"output_tokens"
    Clock  Clock           // nil = 真实时钟；测试注入假时钟（Now + After）
}

func New(cfg Config) (*Limiter, error)

func (l *Limiter) Reserve(ctx context.Context, amounts map[string]int64) (*Reservation, error)
func ReserveAll(ctx context.Context, ls []*Limiter, amounts map[string]int64) (*Reservation, error)
func (r *Reservation) Settle(actual map[string]int64)
func (r *Reservation) Cancel()
func (r *Reservation) Time() time.Time // 获批时刻

func (l *Limiter) Observe(info Info)       // 7.3
func (l *Limiter) PauseUntil(t time.Time)  // 429 + retry-after
func (l *Limiter) Status() Status          // 各维度 Limit、Used、Available、NextReset，服务商校准值
```

**计数方式：滑动窗口日志。** 每次批准记一条（时刻, 数量），在 `Per` 之内计入该维度。由此得到的不变式是：任意长度为 `Per` 的区间里，一个维度批出的总量不超过 `Limit`。没有用令牌桶：满桶可以一次用光、再在下一个 `Per` 里补满，一个窗口里最多能发出两倍额度，对按固定或滑动窗口计数的服务商（大多数 OpenAI 兼容网关）就是 429；滑动窗口对令牌桶式的服务商（Anthropic）同样不会超。代价是突发之后额度按旧记录过期逐条回来，而不是连续回补；稳态吞吐一样是每 `Per` 一个 `Limit`。

**全有或全无。** 一次预占要么在所有限流器的所有维度上同时扣下，要么什么都不扣。任何一项超过该维度 `Limit` 的预占永远放不下，立刻返回 `ErrTooLarge`。等待中 ctx 结束返回 `ctx.Err()`，不留任何预占。

**公平：严格先来先到。** 有人在排队时，后来的预占即使放得下也排在后面，所以大请求不会被源源不断的小请求饿死；代价是队头阻塞，这是有意的。`ReserveAll` 按全局固定顺序给限流器上锁，并在同一步里加入所有队列，所以不同调用方以不同顺序引用同一组限流器不会死锁，各队列对谁先来的看法一致：最早的等待者在它所在的每条队列里都排第一，总能前进。

### 7.2 预占与结算由适配器定义

```go
type Metered interface { // 可选
    Reserve(req Request) map[string]int64               // 发请求前各维度预占多少
    Settle(req Request, resp Response) map[string]int64 // 拿到响应后各维度实际用了多少
}
type RateReporter interface { // 可选
    RateInfo(resp *Response, err error) *RateInfo // 从响应头读到的剩余额度和重置时间
}
```

适配器没实现 `Metered` 时用默认：预占 `requests` 1，`tokens` 和 `input_tokens` 按请求 JSON 字节数 / 4 粗估，`output_tokens` 取 `MaxOut`（没有就是适配器的 `OutputCap`，再没有 4096）；结算按响应的 `Usage`（`tokens` = 输入 + 输出），响应没报用量时只结算 `requests`，token 维度保持预占量。

不同服务商算法不同，所以不统一估算：例如有的服务商把请求里的输出上限也计入 TPM，有的把输入、输出分开限制，缓存命中的 token 可能不计入。预占在请求发出前从各限流器扣除，响应后按 `Settle` 多退少补；请求失败且服务商未处理时 `Cancel` 全额退回。`Settle`、`Cancel` 只有第一次生效。

调用结束后的处理：

| 结果 | 预占 |
|---|---|
| 成功（含截断） | `Settle(实际用量)`，再 `Observe(Response 的 RateInfo)` |
| `RateLimited`、`Overload`；`Transient` 且响应丢失（`ErrResponseLost`）或带状态码（5xx）；被取消打断的调用 | `Settle(预占量)`：服务商可能已经计数；然后 `Observe(Error 的 RateInfo)` |
| `Transient` 不带状态码也没丢响应（连接被拒绝、拨号失败：请求没出去）；`Permanent`；`QuotaExhausted`；适配器 panic；调用前就取消 | `Cancel()` |

`Unknown` 按 `Transient` 处理（见第 11 节），状态码为 0，所以全额退回。预占量超过某维度 `Limit`（`ErrTooLarge`）：任务 `Failed`，`StopReason = "too_large"`。

多退：缩小这次预占自己的那条记录，它仍按原时刻过期（已过期的没有可退的）。少补：差额按**结算时刻**新记一条，可能让窗口超出 `Limit`，从而推迟之后的预占——一个流式调用跑了九十秒才报出超额的 token，这些 token 是刚用掉的，按发出时刻记会让它们还没算进去就过期了。`actual` 里没有的维度保持预占量（例如 `requests` 总是 1）。

预占发生在 vCore 取到任务、组装好请求之后（预占量取决于请求），在 `Running` 提交之前：等额度的任务状态不变。额度不够时，vCore 持着这个任务等待额度，等待可以被取消和 Deadline 打断（任务的 context），打断后按取消做最终提交；本组其他 vCore 也各自在预占处等待，所以本组实际上暂停了。其他执行组不受影响，除非它们引用了同一个限流器。等待超过 50ms 时发机器事件 `throttled`（`{task, class, waited_ms}`），随下一次提交落盘。`Stop` 停止取指时，等额度的 vCore 放弃等待、把原样的任务交还调度器：什么都没提交，下次启动照常执行。

### 7.3 用服务商的反馈校准

适配器从响应头（剩余请求数、剩余 token、重置时间、`retry-after`）读出 `RateInfo`，转成 `limit.Info{Dims: map[string]limit.Observed{维度: {Remaining, ResetAt}}}` 交给 `Observe`，限流器据此修正自己的计数：本地以为还有额度但服务商说没了，以服务商为准——该维度的可用量封顶为服务商报的剩余量，直到它给的 `ResetAt`；期间批出的预占从这个数里扣，这些预占的退款也退回这个数。服务商报的比本地多的忽略：响应乱序到达，晚到的响应头描述的是更早的时刻。没有未来 `ResetAt` 的报告也忽略：不知道该信多久。同一个响应先 `Settle` 再 `Observe`，否则这次请求的用量会被算两遍。

`RateLimited`（第 11 节）用 `PauseUntil(重试时刻)`：在那之前不批任何预占，暂停只会延长不会缩短，本组其他任务因此也等，而不是各自再挨一次 429。重试时刻依次取：`Error.RetryAfter`（按机器时钟换算）、`RateInfo.RetryAt`、已耗尽维度里最晚的 `ResetAt`；都没有时不暂停限流器，任务按第 11 节退避并计入重试次数。没有配置限流器时只有这个任务自己等。`Overload` 不归限流器管，是执行组拥塞窗口的事（6.1）。

限流器没有给 `Clock` 时用机器的时钟（假时钟同样驱动限流等待）。

重启后限流器从空开始，第一个响应就会把它校准。

## 8. 并发规则

1. **TCB 单一所有者。** 在就绪队列、等待集合时归调度器；被 `Next` 取走后归那个 vCore，被 `Checkout` 取走后归那个工具批次，直到 `Requeue`（或提交失败、强制停机时的 `Forget`）。只有所有者能提交这个任务的状态变化。重复入队、未知状态入队、批次找不到自己的任务都是所有权错误，直接 panic。
2. **外部输入不直接改 TCB。** 写入中断表并提交即可返回（4.3），由所有者在下一次提交时取用。
3. **取消信号不受所有权限制。** 每个任务有一个由调度器持有的 `context.CancelFunc`。取消时先提交取消中断，再立刻调用它：在途的模型请求和工具调用马上收到取消（第 9 节）。信号可以随时送达，状态修改仍由所有者做。
4. **一个任务同一时刻最多一条指令。**
5. **工具调用在设备上并发**，受设备 `MaxConcurrent`（默认 4）和全局 `MaxIO`（默认 256）约束。
6. **执行组状态**（`W`、`epoch`、冷却、熔断）和**限流器状态**各自一把锁。
7. **提交是唯一的写路径**（4.1），`Version` 不匹配即报错，说明违反了规则 1。

## 9. 调度、取消与 Deadline

### 9.1 队列

| 队列 | 内容 | 持久化在哪 |
|---|---|---|
| 就绪队列 | 每组一条，分交互、后台两级 | 由 TCB 的 `State/Class/Priority/ReadySince` 重建 |
| 等待集合 | `Waiting(IO / Backoff / Reconcile / Group / Approval)`、`Suspended` | 由 TCB 重建 |
| 中断表 | 每个任务未送达的外部输入 | 中断表本身 |
| 设备队列 | 等待设备的工具调用，每个设备一条 FIFO | 调用记录（`dispatched`） |
| 事件流 | 全局有序的事件 | 事件表；内存环形缓冲只是加速 |

### 9.2 准入

全局 `MaxQueued`（默认 1000）和每组 `MaxQueued`，计未结束的任务数。超限时 `Submit` 返回 `*CapacityError`（`errors.Is(err, ErrBusy)`），不阻塞。准入判断、登记和提交在同一把锁的保护下完成。

### 9.3 出队顺序

组内交互级先于后台级；同级按 `ReadySince` 先进先出，每步之后 `ReadySince` 更新，所以同组任务轮转推进；后台任务 `ReadySince` 早于 `now - AgingAfter`（默认 30s）时按交互级排序。退避中的任务不在就绪队列里。

### 9.4 Deadline

调度器为每个有 Deadline 的任务维护一个定时器，**与任务处于什么状态无关**：就绪、等待工具、退避、待核对、待批准、熔断等待、挂起、正在某个 vCore 上执行（包括在限流器上等额度），都会触发。触发时按"取消"处理（9.5），`StopReason = "deadline"`，终态为 `Failed`。

退避定时器也归调度器：任务以 `Waiting(Backoff)` 交还时按 `WakeAt` 挂上，到点由唤醒方取出、提交 `Ready`（`ReadySince` 不变）、交还；被取消、取出或调度器关闭时停掉。唤醒提交被取消拒绝（`ErrCancelPending`）时就地做最终提交。机器停止后不再唤醒，任务留在 `Waiting(Backoff)`，下次启动按 4.5 就绪。

### 9.5 取消

1. 提交取消中断（4.3），调用方随即得到成功返回。
2. 调用任务的 `CancelFunc`：在途的模型请求立即中断；在途的工具调用的 context 被取消。
3. 任务的所有者做最终提交：
   - `planned` 的调用 → `cancelled`，从未派发；
   - `dispatched` 且这次发送没有到达设备、之前也没发过 → `cancelled`；之前发过的（重启后同一 `Key` 重发、或结果未知后的重试）按结果未知处理；有更早尝试的 `planned`（操作员要求重试）同样；
   - `dispatched` 且结果未知 → `unknown`，发 `reconcile_needed`（带 `cancelled: true`；任务已取消，不再等待核对，但记录留给操作员）；
   - `dispatched` 且结果已经回来 → `done`，结果不进寄存器；
   - 任务转 `Cancelled`，未送达的插话各一个 `steer_dropped`。
4. 取消之后才到达的工具结果，调用记录标为 `late` 并保存结果，不进入寄存器，发 `tool_late` 事件。

挂起、退避、熔断等待和待批准中的任务取消时没有在途调用，由取消方从等待集合取出、直接做最终提交（退避定时器一并停掉；待批准这一轮的 `planned` 调用全部 `cancelled`）。在限流器上等额度的任务归 vCore 持有：取消信号打断等待，vCore 做最终提交。

与任务自己的结束同时到达的取消：任务按它自己的提交结束，取消中断随终态提交被标为已送达，不产生效果。

### 9.6 操作员控制

```go
m.Pause() / m.Resume()          // 全部 vCore 执行完当前指令后停止取指
m.Drain(ctx)                    // 拒绝新 Submit，等所有任务结束
m.Suspend(id) / m.ResumeTask(id)
m.SetPriority(id, p)
m.SetClass(id, class)           // 手动迁移，同样做 6.3 的校验
m.Cancel(id)
m.Resolve(id, callID, outcome)  // 处理 Waiting(Reconcile)
m.Approve(id, callID, decision) // 处理 Waiting(Approval)（9.7）
m.SetCores(class, n) / m.AutoScale(class, on)
m.GroupReset(class)             // 额度耗尽熔断后手动恢复
m.Queue() QueueStatus
```

每个控制都是一条指令，提交后返回，也可以通过协议发出。

### 9.7 工具策略与人工批准

```go
type ToolPolicy func(ctx context.Context, info ToolCallInfo) Decision // Config.ToolPolicy

type ToolCallInfo struct {
    TaskID, CallID string
    Tool    string          // 完整工具名
    Device  string
    Effect  Effect
    Args    json.RawMessage // 副本；模型原样给出，可能不是合法 JSON
    Class   string
    Attempt int             // 将以第几次尝试派发：新调用为 1
}

type Decision struct { // 零值 = Allow
    Verdict Verdict         // Allow / Deny / Rewrite / RequireApproval
    Reason  string
    Args    json.RawMessage // Rewrite
}
// 构造：Allow()、Deny(reason)、RewriteArgs(args)、RequireApproval(reason)

err = m.Approve(ctx, taskID, callID, ApprovalDecision{Verdict: ApproveAllow}) // 或 ApproveDeny + Reason、ApproveEdit + Args
```

**第一层：同步策略钩子。**

- 每个调用第一次派发之前问一次，由持有任务的一方调用（`execModel` 所在的 vCore；`execTools`；恢复时只问从没决定过的 `planned` 调用），在把调用写成 `dispatched` 的那次提交之前。决定写进调用记录（`Policy`、`Approval`、`Reason`、`OrigArgs`），与调用同一次提交：先提交，后动作。提交失败则决定作废，重做那一步时再问；已提交决定的调用（`Policy` 非空）永远不再问，重发（`Attempt > 0`：重启后同 `Key` 重发、`Resolve` 的重试）也不问。页表拒绝的调用不问，由总线派发时拒绝。没有配置策略时什么都不记。
- 钩子必须快、没有副作用：它运行时 vCore 持着任务，它的结论可能随提交失败作废。**钩子没有超时**：阻塞的钩子卡住它所在的 vCore（本组排在后面的任务跟着等）；`Start` 恢复时也会为从没决定过的 `planned` 调用（配置策略之前写下的库）调用它，阻塞的钩子会卡住 `Start`。需要人或慢服务判断的，返回 `RequireApproval`，交给 `m.Approve`。panic 按 `Deny` 处理，原因是 panic 信息，进程不受影响。未知的 `Verdict` 也按 `Deny`。
- `Allow`：照常派发。`Deny`：不执行；调用记录直接 `done`、`IsError`，结果文字带原因，`Attempt` 仍是 0；发 `tool_denied`（`{call_id, tool, reason, by: "policy"}`），它代替这个调用的 `tool_result`。`RewriteArgs`：参数换掉，模型的参数存进 `OrigArgs`，发 `tool_rewritten`（`{call_id, tool, original, args}`）；新参数必须是 JSON 对象，非法 JSON、`null`、数组等按 `Deny`。`tool_call` 事件里始终是模型的原始参数。

**第二层：人工批准。**

- 一轮里只要有一个调用 `RequireApproval`，整轮都不派发：任务转 `Waiting(Approval)`（`PC = StepTools`），待批准的调用 `planned` + `Approval = pending`，被放行的留在 `planned`（`Policy = allow`），被拒的已是 `done`；每个待批准的调用一个 `approval_needed`（`{call_id, tool, device, effect, args, reason}`）。
- **整轮作为一个单元在批准后派发**，不是先派发放行的、只扣住待批准的。理由：`Waiting(Approval)` 里没有在途调用，所以它对取消、Deadline、`Stop`、恢复、`SetClass` 的行为与 `Waiting(Reconcile)` 完全相同，不需要"一半等 I/O、一半等人"的第三种状态，所有权规则不变；操作员做决定时这一轮还没有任何东西生效；代价只是放行的调用多等一会儿，而人工批准本来就以秒到小时计。tool 消息仍按调用顺序组装，每个调用恰好一个结果（4.2）。
- `m.Approve(ctx, taskID, callID, d)`：经 `enter()` 准入（`Start` 前 `ErrNotStarted`，停机后 `ErrStopped`），只从等待集合取出 `Waiting(Approval)` 的任务，提交这个决定后返回。`approval_needed` 先于 vCore 交还任务发布，另一个操作员也可能正持着它（同一轮的另一个调用）：主存里已是 `Waiting(Approval)` 而任务不在等待集合时，等它被交还（`Sched.CheckoutWait`，持有方交还或放弃时唤醒），不报错。醒来时任务又被别人持有：调度器记着最后一次交还时的状态，那时仍在等批准（几个操作员同时批同一轮）就接着等；那时已不在等批准（这一轮已被别人批完、vCore 又取走了它），立即返回 `ErrNotAwaitingApproval`，不陪着跑完下一条指令。交还次数在读主存之前记下（`Sched.Backs`），读主存之后、开始等之前发生的交还也算。`ApproveAllow` → `Approval = allowed`，`approved`（`{call_id}`）；`ApproveEdit` → 参数换成 `d.Args`（必须是 JSON 对象），原参数存进 `OrigArgs`，`Approval = edited`，`approved`（`{call_id, edited: true, original, args}`）；`ApproveDeny` → `done`、`IsError`、`Approval = denied`，`tool_denied`（`by: "operator"`）。决定这一轮最后一个待批准调用的那次提交，同时把全部 `planned` 调用写成 `dispatched`、任务转 `Waiting(IO)`（全被拒则直接组装 tool 消息、`Ready`）；交还调度器后才派发。任务不在 `Waiting(Approval)`、或调用不是待批准的：`ErrNotAwaitingApproval`（像 `ErrNotReconciling`）；未知任务 `ErrNotFound`；决定本身不合法（编辑参数不是 JSON 对象、非编辑带参数）返回错误，什么都不变。
- 取消、Deadline：取消方从等待集合取出任务、直接做最终提交：这一轮的 `planned` 调用（待批准的和随轮等待的）全部 `cancelled`，从未派发；被拒的保持 `done`。与 `Approve` 竞争时先提交的赢（4.1 规则 4）：`Approve` 的提交被 `ErrCancelPending` 拒绝，就地最终提交并返回 `ErrNotAwaitingApproval`；`Approve` 先提交、交还时发现已取消，按交还被拒处理（刚写成 `dispatched` 的调用 `cancelled`）。模型那一步的提交被取消拒绝时，它规划的调用（含待批准、被拒的）同样记 `cancelled`。
- `Stop`：没有在途的东西，任务留在 `Waiting(Approval)`。
- 恢复：仍是 `Waiting(Approval)`，不问钩子；发 `recovered`（`{from: "waiting_approval", pending}`），并为每个待批准调用再发一次 `approval_needed`（带 `recovered: true`）。
- 插话：留在中断表，这一轮结束后下一条模型指令开头送达，排在 tool 消息之后。
- `Status.Approval`、`GroupStatus.Approval`、`ClassQueue.Approval`（JSON `approval`）：等批准的任务数。

## 10. 总线与设备

```go
type Device interface {
    Name() string
    Tools(ctx context.Context) ([]ToolDef, error)
    Call(ctx context.Context, call DeviceCall) (ToolResult, error)
    Close() error
}

type DeviceCall struct {
    Tool string
    Args json.RawMessage
    Key  string // 幂等键，设备可以用它去重
}

type Effect int // ReadOnly, Idempotent, SideEffect

type ToolDef struct {
    Name        string
    Description string
    Schema      json.RawMessage
    Effect      Effect        // 没有声明时按 SideEffect 处理
    Cacheable   bool          // 只对 ReadOnly 有效
    CacheTTL    time.Duration
    Timeout     time.Duration // 0 = 120s
    OnUnknown   string        // "hold"（默认）或 "report"，见 4.2
}
```

- 模型看到的工具名是 `<设备名>_<工具名>`，`[A-Za-z0-9_-]` 以外的字符替换成 `_`。设备名或工具名为空、或替换后重名，挂载时报错并关闭已传入的设备。页表的前缀按完整工具名匹配，所以 `dev_` 也匹配名为 `dev_extra` 的设备。
- 超时、取消由总线强制执行，不依赖设备配合：设备调用在单独的 goroutine 里，超时或取消时总线立刻返回错误结果；设备 panic 被恢复成错误结果。不理会 context 的设备会一直占着自己的并发名额，直到它真正返回。设备恰好在超时同一时刻完成时，以它的结果为准。
- 设备没有声明 `Effect` 时一律当作 `SideEffect`：宁可在恢复时停下来核对，也不重复一个有副作用的操作。MCP 工具的 `readOnlyHint` / `idempotentHint` 映射到 `Effect`。
- 仲裁：每个设备 `MaxConcurrent`，超出的 FIFO 排队。
- 页表：快照工具列表时按 `Allow` / `Deny` 过滤，派发时再检查一次，越权调用返回错误结果给模型，不执行。
- L2（`cache`）：只缓存 `ReadOnly && Cacheable`（其他 `Effect` 上的 `Cacheable` 忽略），作用域是单个任务，取自幂等键 `task/call/attempt` 最后两段之前的部分；键里没有任务的调用不查、不填、不清。键是完整工具名 + 规范化参数：JSON 排序键、去空白、字符串统一转义，数字按原文（`1` 与 `1.0` 不同，宁可不命中也不错命中）；非 JSON 或对象里有重复键的参数按原始字节。
  - 命中返回同一任务里先前调用的真实结果，不进设备队列、不跑设备，`Outcome` 照常是 `Returned`：调用照常记录、恰好一个结果；`RunCached` 另外报告是否命中，`Run` 签名不变。页表先于缓存检查，被拒的调用碰不到缓存；已取消的 ctx 先于缓存返回 `NotRun`。
  - 未命中才跑设备，只存非 `IsError` 的结果；超时、晚到结果从不入缓存。同一任务同时发出的相同调用不合并（不做 single-flight），各跑各的，后完成的覆盖：`ReadOnly` 本来就允许多跑，合并会把一个调用的取消牵连到另一个。
  - 清空看完成，不看派发：非 `ReadOnly` 调用在设备返回时（结果交出之前）清空本任务缓存；总线因超时或取消不再等它时也清一次，因为它可能仍会生效。查询时记下任务的代数，期间有写完成的，那次读的结果不存（它可能读到写之前的状态）。
  - `CacheTTL` 到期后不再命中（0 = 直到被清空），查到时惰性删除；时钟可注入（`cache.Config.Now`）。
  - 内存有界：每任务最多 256 条（`DefaultMaxEntriesPerTask`），全局 64 MiB（`DefaultMaxBytes`，按键 + 内容 + 每条 160 字节估算），各按 LRU 淘汰；单条超过全局上限的不存。
  - 任务结束时由机器调用 `bus.ForgetTask(id)`（即 `L2.Forget`）丢掉它的缓存，在途的未命中也不会再填回：机器在每个带终态事件的提交成功后调用。工具批次用 `RunCached` 派发，命中的那次 `tool_result` 事件带 `cached: true`。总线默认带一个默认上限的 L2，`SetCache(nil)` 关闭；`CacheStats()` 给出命中、未命中、填入、淘汰、过期、清空次数和当前条目、字节、任务数，供 `m.Status()` 使用。
- 第一版设备：`builtin`、`mcp`（`github.com/modelcontextprotocol/go-sdk`）、`skills`。

## 11. 模型适配器

```go
type Adapter interface {
    Provider() string
    Caps() Caps // 窗口、最大输出、是否支持工具调用、单次工具数上限
    Call(ctx context.Context, req Request) (Response, error)
    Reserve(req Request) map[string]int64
    Settle(req Request, resp Response) map[string]int64
}

// 可选：适配器比 model.Classify 更懂自己服务商的错误时实现
type Classifier interface{ Classify(err error) ErrClass }
// 可选：从响应头读剩余额度和重置时间（7.3）
type RateReporter interface{ RateInfo(resp *Response, err error) *RateInfo }
```

错误分类：

| 类别 | 例子 | 执行组 | 限流器 | 任务 |
|---|---|---|---|---|
| `RateLimited` | 429 且带限流响应头或 `retry-after` | 不变 | 按 `RateInfo` 暂停到重置时间 | `Waiting(Backoff)` 到重试时刻（7.3），不计重试次数；没有任何提示的按临时错误退避、计次数 |
| `QuotaExhausted` | 额度或账单耗尽 | 熔断，不自动半开 | 退还预占 | `Waiting(Group)` 或迁移 |
| `Overload` | 529、503 overloaded；无法区分的 429 | 窗口减半（每周期一次） | 保留预占 | 退避后重试，计重试次数 |
| `Transient` | 其余 5xx、网络错误、超时 | 计入熔断 | 见 7.2 | 退避后重试，计重试次数；响应丢失时 `Unconfirmed + 1` |
| `Permanent` | 其余 4xx | — | 退还预占 | `Failed`，`StopReason = "model_error"` |
| `ContextLength` | 请求超出上下文窗口（OpenAI `context_length_exceeded`、Anthropic "prompt is too long"） | 算一次回答（同 `Permanent`） | 退还预占 | 学到更小的窗口，压缩后重发（5.3）；压不下 `Failed`，`StopReason = "context_overflow"` |

退避：第 n 次连续失败等 `min(60s, 1s·2^(n-1))` 的全抖动（在 0 到该值之间均匀取），不短于服务商的 `RetryAfter`；第 8 次（`Config.MaxModelRetries`）连续失败时任务 `Failed`，`StopReason = "model_error"`，错误文字在 `failed` 事件里。`Retries` 记在 TCB 上随提交持久化，成功（包括截断）清零。每个重试决定都是一次提交：`Waiting(Backoff)` + `WakeAt` + `model_retry` 事件（`{class, attempt, delay_ms, wait, error}`，`attempt` 是计入的重试次数，限流时不变），提交之后调度器才挂定时器；`QuotaExhausted` 同样发 `model_retry`（`wait = "group"`）。失败的调用仍是一条指令（发 `step` 事件），但不计入 `Used.Steps`（4.6），`MaxSteps` 不约束它们，约束它们的是重试上限；带提示的限流（`RetryAfter`、`RateInfo.RetryAt` 或已耗尽维度的重置时间）不计重试次数，由服务商的重试时刻和限流器暂停节制；什么提示都没有的 429 和被误判的额度错误分不开，按临时错误退避并计入重试次数，也不暂停限流器，所以服务商一直这么回时任务以 `model_error` 结束，不会无限循环。退避步长到上限前不会溢出（`BackoffMax` 接近最大时长时也一样）。`Unknown`（谁也解释不了的错误）按 `Transient` 处理：网络抖动比请求本身有错更常见，重试上限兜住判断错的代价。适配器报了 `Canceled` 而任务并没有被取消（适配器自己超时、报成裸的 `DeadlineExceeded`）也按 `Transient`。

`Unconfirmed` 只在 `errors.Is(err, ErrResponseLost)` 时加一（以及原有的：调用被取消打断、重启时发现 `Running`）；连接被拒绝不加（18.12）。

`context.Canceled` 永远不是可重试错误。永久错误的判断优先于报错文字里的"timeout"之类字样。

实现（`model`）：`ErrClass` 取值 `Unknown`（零值）/ `RateLimited` / `QuotaExhausted` / `Overload` / `Transient` / `Permanent` / `Canceled` / `ContextLength`，`Retryable()` 只对 `RateLimited`、`Overload`、`Transient` 为真。适配器能解释的失败一律返回 `*model.Error{Class, Status, RetryAfter, Provider, Msg, Err, Rate}`，`Msg` 是状态码之外的响应片段，服务端回显的 key 会被抹掉。`model.Classify(err)` 按顺序判断：链上有 `context.Canceled` → `Canceled`（适配器说什么都不改变这一条）；链上有 `*model.Error` 且类别不是 `Unknown` → 用它的类别（所以永久错误不会被文字里的"timeout"翻案——这里根本不读文字）；`context.DeadlineExceeded` → `Canceled`；`net.Error`、`io.ErrUnexpectedEOF`、连接被重置 / 拒绝、`EPIPE`、`model.ErrResponseLost` → `Transient`；其余 `Unknown`。`model.ClassifyWith(adapter, err)` 先排除取消，再问适配器的 `Classifier`，答 `Unknown` 时回落到 `Classify`。

**调用方的 Deadline 和单次请求超时是两回事。** 到达 `Classify` 的裸 `DeadlineExceeded` 是调用方的 ctx 到期，也就是任务的 Deadline（9.4），重试只会越过它，所以算 `Canceled`。单次请求超时（`http.Client.Timeout`、流读到一半卡住）发生时调用方还有时间，是服务商慢，算 `Transient`。两者在错误链上都表现为 `DeadlineExceeded`，只有适配器分得清：出错时看自己调用方的 `ctx.Err()`，非空就是 `Canceled`（`Err` 设为 `ctx.Err()`），为空就包成 `*model.Error{Class: Transient}`。

**响应丢失**：请求已经到达服务商、但响应没有完整回来（body 中途断开、流在 finish_reason / `[DONE]` 之前结束、2xx 的 body 解析不了），错误包裹 `model.ErrResponseLost`，类别 `Transient`。服务商可能已经做完并计费，这一点和连接被拒绝（请求根本没出去，不包 `ErrResponseLost`）不同，`Unconfirmed` 只应在前者增加（18.12）。

OpenAI 兼容适配器的分类，状态码先定，正文只在 429/403/402 里区分额度和限流、在 5xx 里区分过载：

- 402 → `QuotaExhausted`。
- 429 / 403 / 流内错误：服务商给的等待时间（`retry-after-ms`、`Retry-After` 秒数或 HTTP 日期、正文里 Google 的 `retryDelay`、"Resets in 120h41m12s"、"try again in 6m0s"，取最长）超过 1 小时 → `QuotaExhausted`，`RetryAfter` 带上这个时间；正文有 `insufficient_quota` / billing / credit balance / quota reached → `QuotaExhausted`；正文只说 quota 或 RESOURCE_EXHAUSTED、却没有短窗口的迹象（retry 头、正文里的等待时间、per minute / per second）→ `QuotaExhausted`。实测 CLIProxyAPI 在凭证全部冷却时回 429 + `rate_limit_error`，内层是上游的 "Individual quota reached … Resets in 120h41m12s"：外层说限流，实际是五天后才重置的额度耗尽，重试没有意义。Gemini 的每分钟配额同样说 "Quota exceeded"，但带 `retryDelay` 和 PerMinute，归 `RateLimited`。
- 429 其余：有 `retry-after` 类头或正文等待时间 → `RateLimited`；只有 `x-ratelimit-*` / `anthropic-ratelimit-*` 头 → `RateLimited`，`RetryAfter` 取已耗尽维度里最晚的重置时间；什么都没有 → `Overload`。
- 429 以外的 4xx（以及流内错误）正文说请求超出上下文窗口 → `ContextLength`：`context_length_exceeded`、"maximum context length"、"prompt is too long"、"exceed context limit"（`httperr.ContextTooLong`，两个适配器共用）。429 和 5xx 里出现这些字样不算。
- 529 → `Overload`；5xx 正文含 overload → `Overload`；其余 5xx、408 → `Transient`；其余 4xx → `Permanent`。
- 流内错误 `{"error":{...}}` 的数字 `code` 当作状态码；没有数字 code 时含 overload / rate limit → `Overload`，否则 `Transient`。
- 网络错误、单次请求超时 → `Transient`；响应丢失同上。

`RateInfo` 在成功（`Response.Rate`）和失败（`Error.Rate`）时都解析：`Dims` 以 `requests` / `tokens` / `input_tokens` / `output_tokens` 为键，每维 `Limit`、`Remaining`、`ResetAt`（只有报了 `remaining` 的维度才出现）；`RetryAt` 是 retry-after 换算成的时刻，`At` 是读取时刻。重置值接受 OpenAI 的时长（`6m0s`、`1s`、`120ms`，以及 `1d2h`）、秒数、Unix 时间戳、RFC 3339、HTTP 日期。适配器的 `Now` 字段可以换成假时钟。

第一版两个适配器：OpenAI 兼容（`/chat/completions`）和 Anthropic 原生（`/v1/messages`）。

传输层：默认 HTTP/2，所有 vCore 的请求共用一条 TLS 连接多路复用，TLS 会话可恢复；HTTP/3（QUIC）可选，QUIC 拨号失败时永久回退到 HTTP/2——只有拨号阶段的失败才回退，请求发出后的超时不重发。`Start` 时后台预热连接。实测 cpa（家用宽带）：预热后的单次往返约 75ms，16 个并发请求 HTTP/2 比 HTTP/1.1 快约 40%，HTTP/3 与 HTTP/2 持平；一次模型调用约 3.5s，协议层的空间约占 2%。

流式下工具调用按 `index` 汇总：`arguments` 逐片拼接，其他字段（id、type、服务商私有的扩展）以最后一次为准，组装成完整对象后同样存进 `Opaque`。流在收到 finish_reason 或 `[DONE]` 之前断开，是错误，不是短回答。

已知网关差异：cpa 的非流式响应里 `completion_tokens` 包含推理 token（OpenAI 的约定），流式响应里不包含。`Usage` 原样记录服务商报的数，不做猜测性修正。

OpenAI 兼容适配器把服务商返回的 tool_call 原样存进 `Part.Opaque`，回传给同一服务商时逐字节重放（Gemini 的 thought signature 就在这里）；发给其他服务商时用机器自己的 id 重新渲染。缓存标记由适配器负责：OpenAI 兼容服务自动缓存；Anthropic 在工具定义末尾和消息序列倒数第二条打 `cache_control`。请求按固定顺序组装（系统提示词 → 排好序的工具 → 消息），前两部分在任务生命周期内逐字节不变。

Anthropic 原生适配器（`model/anthropic`，标准库实现，传输层复用 `openai.NewTransport`）：`POST {BaseURL}/messages`，头 `x-api-key` 与 `anthropic-version: 2023-06-01`；`max_tokens` 必填，默认 8192；`Provider()` 为 `anthropic:<模型>`。系统提示词放顶层 `system`；工具调用渲染为 `tool_use` 块，工具结果渲染为 user 消息里的 `tool_result` 块。Anthropic 要求角色交替、`tool_result` 紧跟它的 `tool_use` 回合，所以同角色的相邻消息合并成一条，合并后的 user 回合里 `tool_result` 块排在最前（插话、中途的 system 消息作为文本块排在后面），渲染为空的消息直接省略。id：同一 Provider 且有 `ProviderCallID` 时用服务商的 id，否则用机器 id，`[a-zA-Z0-9_-]` 以外的字符替换成 `_`；结果引用同一个 id。`input` 必须是 JSON 对象：空参数发 `{}`，不是对象（包括非法 JSON）的参数原文放在 `{"_raw": "..."}` 里，不丢信息。响应里除纯文本外的每个块（`tool_use`、`thinking`、`redacted_thinking`、未知类型）原样存进 `Opaque`；回放给同一 Provider 时逐字节发回（thinking 的签名绑定在产生它的模型上），发给其他 Provider 时 thinking 丢弃、tool_use 按中立字段重新渲染。流式下按 `index` 重组每个块：`text_delta`、`thinking_delta`、`signature_delta` 逐片拼接，`input_json_delta` 拼成完整的 `input`，键顺序沿用 `content_block_start`，组装结果与非流式响应的块一致；流在 `message_stop` 之前断开是错误。用量：`PromptTokens = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`（`input_tokens` 只计最后一个缓存断点之后的部分），`CachedTokens = cache_read_input_tokens`。`stop_reason` 映射到与 OpenAI 兼容适配器相同的词：`end_turn`/`stop_sequence` → `stop`，`tool_use` → `tool_calls`，`max_tokens` → `length`。缓存标记 `cache_control: {"type":"ephemeral"}` 打在最后一个工具定义上，以及倒数第二条消息的最后一个可标记块上（thinking 块不能带标记，向前找）；标记插在块的右花括号之前、写进新的切片，块的其他字节和它来自的 `Opaque` 都不变。错误分类和 `RateInfo` 与 OpenAI 兼容适配器共用 `model/internal/httperr`（状态码、retry 提示、额度规则、限流头），在其上读正文的 `error.type`：共用的额度规则先定（CLIProxyAPI 的 `rate_limit_error` + "Resets in 120h41m12s" 仍是 `QuotaExhausted`），529 一律 `Overload`；然后 `rate_limit_error` → `RateLimited`（没有 retry 提示时取已耗尽维度的重置时间），`overloaded_error` → `Overload`，`api_error` / `timeout_error` → `Transient`，`billing_error` 和 400 的 "credit balance is too low" → `QuotaExhausted`，`invalid_request_error` / `authentication_error` / `permission_error` / `not_found_error` / `request_too_large` → `Permanent`（共用规则先判出的 `ContextLength` 优先：Anthropic 以 400 `invalid_request_error` 发 "prompt is too long: … tokens > … maximum" 和 "input length and `max_tokens` exceed context limit"）；没有或不认识的 `error.type` 沿用共用结论。流内 `error` 事件按同样规则分类（状态码记 0）；流在 `message_stop` 之前结束、body 被截断、2xx 解析不了 → `Transient` 包 `ErrResponseLost`；取消与单次请求超时的区分同 OpenAI 兼容适配器。不带 Anthropic 专有信息的同一组状态码、头和正文，两个适配器的结论一致（有交叉测试）。

## 12. 协议

```go
type Instruction struct {
    Kind     string          `json:"kind"`
    // submit / steer / cancel / suspend / resume / priority / class / resolve / approve
    TaskID   string          `json:"task_id,omitempty"`
    Input    string          `json:"input,omitempty"`
    Class    string          `json:"class,omitempty"`
    PinClass bool            `json:"pin_class,omitempty"`
    Priority int             `json:"priority,omitempty"`
    Budget   *Budget         `json:"budget,omitempty"`
    Allow    []string        `json:"allow,omitempty"`
    Deny     []string        `json:"deny,omitempty"`
    CallID   string          `json:"call_id,omitempty"` // resolve
    Outcome  *ResolveOutcome `json:"outcome,omitempty"` // resolve：done(result) / failed / retry
    Decision *Decision       `json:"decision,omitempty"` // approve：{verdict: allow / deny / edit, reason, args}
}

type Event struct {
    Kind    string          `json:"kind"`
    TaskID  string          `json:"task_id,omitempty"`
    Seq     int64           `json:"seq"`      // 全局序号
    TaskSeq int64           `json:"task_seq"` // 任务内序号
    Time    time.Time       `json:"time,omitzero"` // 提交时刻，UTC
    Data    json.RawMessage `json:"data,omitempty"`
}
```

`Time` 由 Store 在提交时盖上，和 `Seq` 一样：同一个提交集里的事件是同一时刻；按 `Seq` 单调不减（墙上时钟回拨时停在上一次的时刻，等时钟追上）；持久化，补读得到的与在线时逐纳秒相同。`mem.Memory` 的 `Now` 字段、`sqlite.WithClock` 可注入时钟。SQLite 存为 Unix 纳秒，`Open` 给旧库的事件表补列，旧事件读出零值；消费方把零值当作未知（JSON 里省略）。

任务事件：`accepted`、`model_call`（随 `Running` 提交、请求即将发出，`{pc, core}`；到带结果的 `step` 为服务商时延）、`step`、`tool_call`、`tool_result`、`tool_late`、`usage`、`answer`、`model_retry`、`compacted`、`migrated`、`migrate_refused`、`recovered`（重启时恢复了这个任务，`{from, ...}`）、`reconcile_needed`、`resolved`（操作员处理了一个待核对的调用）、`tool_denied`、`tool_rewritten`、`approval_needed`、`approved`（9.7）、`done`、`failed`、`cancelled`、`steer_dropped`。

`compacted`：`{rounds, messages_before, messages_after, tokens_before_est, tokens_after_est, chunk, chunks}`（5.3）。`step` 的 `pc` 为 `model`、`tools` 或 `compact`。`model_retry` 的 `class` 可以是 `context_length`（`wait = "compact"`）。

`migrated`：`{from, to, reason}`，`reason` 为 `group_down` 或 `manual`。`migrate_refused`：`{from, to, check, detail}`，本组熔断、`Fallback` 校验不过、任务留下等本组。`tool_result` 来自 L2 时带 `cached: true`。

机器事件（`task_id` 为空）：`core_up` / `core_down`（`{class, core, reason, live}`）、`throttled`（`{task, class, waited_ms}`）、`group_down`（`{class, cause}`）、`group_up`（`{class}`，操作员复位时带 `reset`）。

stdio 前端：每条指令一行 JSON，处理完回一行 `{"ok":true,"task_id":...}` 或错误（返回即已持久化）；事件另起一个流，支持 `{"kind":"subscribe","from_seq":N}` 补读。

操作员控制也走同一协议：`class`（`task_id`、`class`）、`cores`（`class`、`cores`）、`autoscale`（`class`、`on`）、`group_reset`（`class`）、`queue`；`submit` 可带 `pin_class`。`class` 被校验拒绝时错误码 `incompatible`，带 `check`；任务正在执行时 `task_busy`。`suspend`、`resume`、`priority` 仍回 `unsupported`。`approve`（`task_id`、`call_id`、`decision`：`{"verdict":"allow"}`、`{"verdict":"deny","reason":…}`、`{"verdict":"edit","args":{…}}`，`args` 必须是对象）对应 `m.Approve`，任务或调用不在等批准时错误码 `not_awaiting_approval`。工具策略只能在 Go 里配置（`Config.ToolPolicy`）。

实现在 `proto/jsonl`（线格式的完整说明在它的包文档里），`cmd/neumann` 把它接到 stdin/stdout。回复和事件共用 stdout，每行第一个字段 `"type"` 是 `"reply"` 或 `"event"`：宿主进程（Node、Python）天然只有 stdout，多开一个 fd 两边都别扭；共用一个流还给出了顺序，`subscribe` 的回复恰好落在旧流最后一个事件和新流第一个事件之间。指令可带 `"id"`，回复原样带回；指令按到达顺序逐条执行。读得慢的一方不阻塞机器：事件按输出写出的速度从主存读，回复优先于积压的事件写出。

## 13. 出错处理

| 情况 | 处理 |
|---|---|
| 模型请求失败 | 按第 11 节分类：限流、过载、临时错误退避重试，额度耗尽等执行组，永久错误 `Failed`（`model_error`） |
| 截断且无内容 | `MaxOut` 乘 4 重做，最多两次，之后 `Failed`（`truncated`） |
| 上下文超过阈值 / 服务商拒绝为超出上下文 | 压缩（5.3）；怎么压都放不下 `Failed`（`context_overflow`） |
| 预占量永远放不下 | `Failed`（`too_large`） |
| 工具出错 / 超时 / 越权 | 错误文本作为该调用的结果交给模型 |
| 提交失败 | 记 ERROR；这次状态转换不存在，任务不再调度，发不出事件（因为事件也没提交）；`Status` 里标出该任务和错误 |
| 进程崩溃 | 按 4.5 恢复 |
| 超出 MaxSteps / MaxTokens | `Done`，`StopReason = "budget"`（`Used.Steps + Unconfirmed`：只算有回答的调用和未确认的请求，失败的调用不算） |
| Deadline | `Failed`，`StopReason = "deadline"`（9.4） |
| vCore panic | 恢复 panic，该任务 `Failed` 并带 panic 信息，这个 vCore 退役 |

## 14. 观测

`m.Status()`：每个执行组的 `W`、`epoch`、存活 vCore 数、冷却/熔断状态（当前实现：`Status.Groups` 每组给出模型、`Fallback`、`W` / `Target` / `Min` / `Max` / `Auto` / `epoch` / 冷却、存活与忙碌的 vCore 数、就绪数、等待数（其中等本组的）、最老就绪任务已等多久、熔断状态与下一次放探测的时刻；`Status.Cores` 是全部存活 vCore 数；`Status.Limiters` 给出各限流器的 `limit.Status`；`Status.Cache` 是总线 L2 的 `CacheStats`；`Status.Approval` 等批准的任务数；`m.Queue()` 按类给出就绪数、其中后台数、等待数（其中等批准的）、最长等待）；每个限流器各维度的剩余额度和下次重置时间；每个 vCore 在做什么；每条队列长度和最长等待；待核对的调用列表；每个未结束任务的状态、组、步数、已确认用量和未确认请求数。加锁拷贝，不调模型、不读主存。

`observe` 包只用公开 API（`Events`、`EventLog`、`Status`、`Queue`、`Load`、`Calls`），任何宿主都能挂；不挂就没有任何开销。事件从主存按序号读，消费方慢只会落后，不会挡住机器；`Follow` 返回最后读到的 `Seq`，宿主存下来重启后接着读；`Head` 用 O(log n) 次单条读找到日志末尾，只想统计"从现在起"的就从那里开始。

- `Metrics`：OpenTelemetry 指标，名字都在 `neumann.` 下，带单位，完整的表在包文档里。事件来的计数和直方图：任务接受 / 结束（按类、`stop_reason`）、模型调用与失败（按类、错误类别）、模型指令耗时、工具调用（按工具、结果：ok / error / cached / denied / unknown / late / cancelled）与耗时（`tool_call` → 结果）、token（prompt / completion / reasoning / cached）、重试、限流等待、`core_up` / `core_down`、熔断、迁移、`reconcile_needed`；工具策略与批准（9.7）：`tool_denied`（按工具、`by`，同时是该调用的 `denied` 结果）、`tool_rewritten`、待批准的调用数（重启再宣布不重复计）、决定数与等待时长（首次 `approval_needed` → 决定，按 `verdict`：allow / edit / deny，任务先结束为 cancelled）。采集时读 `Status` / `Queue` 的仪表：各组 vCore、`W`、`Target`、`epoch`、熔断状态，各类队列与最长等待，各限流器各维度用量与可用量，L2，未结束任务数、等批准的任务数（总数与各类），丢掉的增量，进程的 goroutine、堆（Linux 上还有 RSS）。基数有界：不按任务 id 打标签；工具名、类名和其他标签各有上限，超出的记为 `_other`（模型会编造工具名）。
- 模型指令耗时的起点：`step` 事件和它的结果在同一个提交里，调用前的 `Running` 提交不带事件，所以日志里没有"请求发出"的时刻。起点取任务能运行的时刻：上一次提交、退避结束、或放行这次调用的 `throttled`；终点是结果的提交。包括就绪队列和限流器上的等待，空闲时就是调用本身。
- `Tracer`：从事件重建 OpenTelemetry trace，一个任务一个 trace。根 span `task`（accepted → 终态，`failed` 为错误状态），子 span：每条模型 / 压缩指令一个（失败带错误类别），每轮工具一个 `tools`，其下每个调用一个 `tool <名字>`；每个待批准的调用一个 `approval <工具>`（首次宣布 → 决定，带 `verdict`）；被拒的调用 span 在 `tool_denied` 结束、错误状态；重试、限流、迁移、恢复、核对、策略与批准事件（不带参数和原因）是根 span 上的事件；终态之后提交的（`steer_dropped`、`tool_late`）是根下零长度的 span，这样 trace 不取决于何时 flush。时间全用 `Event.Time`，id 由任务 id 和打开该 span 的事件的 `Seq` 经 SHA-256 得到（trace id = `SHA-256("neumann/trace/"+任务)` 前 16 字节），所以从 0 补读重建的 trace 与在线导出的逐字段相同；从中途接着读时，根 span 从第一个看到的事件开始并标 `neumann.partial`，id 不变。直接把 span 交给 `SpanExporter`（OTLP、内存），不经过 TracerProvider：后者自己生成 id 和时间。
- `Log`：slog，每个值得一提的事件一行，消息是事件种类，`seq`、`task`、`class` 等是可以 grep 的键值，`at` 是提交时刻。生命周期 info；重试、限流、熔断、迁移被拒、插话丢弃、晚到结果、`tool_denied`、`approval_needed`（操作员要处理）warn；`approved` 带 `verdict` 与等待时长；`failed`、`reconcile_needed` error；`step`、`usage`、工具调用和结果 debug。`LogResources` 定期写一行资源。
- 压缩（5.3）：`compacted` 是 `Metrics` 的 `neumann.compactions`（每块一次）、`neumann.compaction.rounds`（换出的轮次）、`neumann.compaction.tokens`（`tokens_before_est - tokens_after_est`），都按类；`compact` span 延到同一提交里的 `compacted` 事件结束，带它的数字（`neumann.compact.*`），任务根 span 上也记一个 `compacted` 事件；`Log` 以 info 写一行，带轮次、消息数、估算 token 和块号。没落地的压缩（被取消抢先）仍是一个 `compact` span，不计数。
- `NewHandler`：只读的 HTTP 视图，`GET /status`、`/queue`、`/tasks/{id}`（TCB + 调用记录）、`/events?from_seq=N`（Server-Sent Events，先补读再在线，`Last-Event-ID` 续读，`task=` 过滤），可选 `/metrics`（OTel 的 Prometheus 导出器）和 `/debug/pprof/`（默认关；自己实现，不引入 `net/http/pprof`，它会注册到 `DefaultServeMux`）。没有任何会改状态的端点，其他方法 405。`/tasks`、`/events` 带任务内容（提示词、工具参数和结果），只能给操作员看；可以要求 bearer token。指标、trace、活动日志都不带任务内容。

`cmd/neumann`：`-observe-addr`（空 = 关）在那里挂 `NewHandler`，`-metrics` 加 `/metrics`，`-pprof` 加 pprof，`NEUMANN_OBSERVE_TOKEN` 设了就要求 token；设了标准的 `OTEL_EXPORTER_OTLP_ENDPOINT` 时 trace 和指标经 OTLP/HTTP 导出，`service.name` 为 `neumann`。观测从启动时的日志末尾开始；退出时先等机器停、观测读到日志末尾，再 flush 导出器，每步有时限。

## 15. 公开 API

```go
m, err := neumann.New(neumann.Config{
    Groups: []neumann.GroupConfig{
        {Class: "big",    Model: anthropic, Limits: []string{"anthropic-org"}, Fallback: "little"},
        {Class: "little", Model: flash,     Limits: []string{"gw"}, Min: 2, Max: 64},
    },
    Limiters: []limit.Config{
        {Name: "anthropic-org", Limits: map[string]limit.Rate{"requests": {time.Minute, 50}, "input_tokens": {time.Minute, 40000}}},
        {Name: "gw", Limits: map[string]limit.Rate{"requests": {time.Minute, 600}}},
    },
    Default:   "little",
    Store:     store,
    Devices:   []bus.Device{builtin, mcpGitHub, skillsDir},
    ToolPolicy: policy,                    // 可选：每个调用第一次派发前的决定（9.7）
    System:    "You are ...",
    MaxQueued: 1000,
})
err = m.Start(ctx)                         // 按 4.5 恢复，然后启动 vCore
id, err := m.Submit(ctx, neumann.Instruction{Kind: "submit", Input: "...", Class: "big"}) // 返回即已持久化
sub := m.Events(ctx, fromSeq)              // 可补读；m.EventLog / m.Load / m.Calls 直接读主存
h, err := m.History(ctx, id)               // 完整历史：压缩换出的原始消息从归档取回，不含摘要
err = m.Stop(ctx)                          // 停止取指，等在途指令和工具批次提交；ctx 到期则取消它们（被打断的不提交），再等 2 秒后关闭

err = m.SetCores("little", 8)              // 固定窗口；多出的 vCore 执行完当前指令后退役
err = m.AutoScale("little", true)          // 恢复自适应
err = m.SetClass(ctx, id, "little")        // 手动迁移：*MigrateError / ErrTaskBusy / ErrFinished / ErrNotFound
err = m.GroupReset(ctx, "big")             // 额度耗尽熔断后复位
err = m.Approve(ctx, id, "c-3", neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}) // 回答待批准的调用：ErrNotAwaitingApproval / ErrNotFound
q := m.Queue()                             // 各类队列
```

`Submit` 超过本组 `MaxQueued` 时返回 `*CapacityError{Class: ...}`（`errors.Is(err, ErrBusy)`）。

## 16. 包结构

```
neumann/            Machine、Config、公开 API、操作员控制（Resolve、SetClass、SetCores、AutoScale、GroupReset、Queue）
  internal/core/    vCore 循环与弹性 vCore、execModel / execTools、恢复与结算、执行组、熔断、迁移与能力校验
  internal/group/   拥塞窗口、Scaler、熔断器（只做计数和决策）
  internal/sched/   就绪队列、等待集合、Deadline 与退避定时器（internal：外部代码不能绕过所有权规则）
  limit/            限流器
  task/             TCB、中立消息、寄存器组装、token 估算
  bus/              Bus、仲裁、设备队列、页表
    builtin/  mcp/  skills/
  cache/            L2
  mem/              Store 接口、memory 实现、Disk 接口
    sqlite/         SQLite 实现（组提交）
    storetest/      每个 Store 都要通过的一致性套件
  model/            Adapter 接口、错误分类
    openai/  anthropic/
  proto/            Instruction、Event
    jsonl/          JSON Lines 编解码与 stdio 服务（Server）
  observe/          只用公开 API 的观测：OTel 指标与 trace、slog 活动日志、只读 HTTP 视图
  sim/              确定性模拟器：假时钟、脚本模型、脚本设备、故障注入点
  cmd/neumann/      stdio 前端
```

## 17. 实现顺序

每一阶段结束时第 18 节对应的测试必须为绿，再进入下一阶段。

1. **固定并发 + 状态机**：一个执行组、固定 vCore 数、内存版 Store；TCB 状态机、就绪队列、等待集合、工具异步派发、中断表、插话。
2. **原子持久化与工具恢复**：SQLite Store、`Commit` 组提交、调用记录、事件表与补读、4.5 的重启流程、`Waiting(Reconcile)` 与 `resolve`。
   第一阶段留下的 8 处接缝已全部处理：`Running` 在调用前提交；调用记录在规划时记下 `Device`、`Effect`、`ReportUnknown`；tool 消息从调用记录组装；`dispatched` 但从未发出的调用会进入核对（预写日志固有）；准入计数在 `Start` 时从未完成任务数初始化；`Submit` / `Steer` 锁内预留、锁外提交，`Stop` 带截止时间地等它们；单条结果提交失败时 `tool_result` 随完成提交补发；`core`、`sched` 已移到 `internal/`。
   验证：`TestCrashAtEveryCommitPoint`（每个提交点注入一次崩溃，SQLite，压测 30 轮）、`TestKillNine`（真进程 SIGKILL，副作用只执行一次）。
3. **取消、Deadline 与故障注入**：9.4、9.5；在每一个提交点前后注入崩溃。
   已完成：`Commit` 在有未送达取消时只接受取消的最终提交、拒绝 `dispatched`（`ErrCancelPending`，两个 Store 同一规则，一致性套件覆盖），取消信号到达前提交被拒的一方据此最终提交，先提交的取消一定赢；`m.Cancel(ctx, id)` 提交取消中断（`Text` 是原因）后返回，结束的任务返回 `ErrFinished`、不存在的返回 `ErrNotFound`；调度器持有每个任务的 context 和 Deadline 定时器（`Budget.Deadline`，绝对时间，`Submit` 时给出），不论状态；没人在干活的任务（就绪、待核对、挂起，以后的退避、熔断等待）由取消方 `Sched.Cancel` 取出并做最终提交，vCore 或工具批次持有的任务只收到信号，交还时 `Requeue` 在同一把锁下拒收，由持有方最终提交——取消不会落在提交和交还之间；总线 `Run` 区分"没碰到设备"（之前没发过的才是 `cancelled`）、"设备跑着时放弃"（`unknown` + 带 `cancelled` 标记的 `reconcile_needed`，任务不等）和结果，放弃之后设备的真实结果在最终提交之后记为 `late` 并发 `tool_late`，不进寄存器；模型调用期间被取消算一次 `Unconfirmed`，已回来的工具调用记为 `cancelled`、从不派发；Deadline 已过的任务不再开始任何指令、不再把任何调用交给总线（包括恢复和 `resolve` 的批次），即使定时器还没跑；晚到结果只在最终提交落盘后记录，`Stop` 等在途的晚到提交；恢复时有未完成的取消中断就直接做最终提交，Deadline 重新挂上、已过的立即触发。
   验证：`TestCrashAtEveryCommitPoint*`（支付、执行中取消、执行中超时、模型调用期间取消已提交但未通知（要工具 / 给答案）、只读调用里超时后重启六种任务，每个提交点之前和之后各崩一次，SQLite；"之后"= 提交已落盘、调用方没收到）、`TestAcceptedInputSurvivesACrash`（第 18 节 9）、`TestDeadlineFiresInEveryState`（第 18 节 10：就绪、等工具、待核对、执行中、已过期）、`TestCancelWhileToolsInFlight` / `TestCancelledPlannedCallsNeverRun` / `TestLateResultIsRecordedLate`（第 18 节 11）。
4. **自适应伸缩、限流器与迁移**：拥塞窗口、限流器、错误分类、熔断、迁移校验、第二个适配器。
   已完成（错误处理接进 vCore 循环，vCore 数仍固定、仍只有一个组）：`execModel` 用 `model.ClassifyWith` 分类，按第 11 节处理每一类；`Waiting(Backoff)` 带 `WakeAt` 提交后由调度器挂定时器，到点提交 `Ready`、`ReadySince` 不变；`Retries` 持久化、成功清零、默认 8 次；截断无内容按 `MaxOut` × 4 重做两次（`model.Request.MaxOut`，两个适配器都发 `max_tokens`）；`Config.Limiters` / `Config.Limits`，调用前 `ReserveAll`（等额度在 `Running` 提交之前、可被取消/Deadline 打断、`Stop` 时原样交还），调用后先结算再校准，按 7.2 的表退还，`RateLimited` 暂停限流器；每个类一个 `core.Group`（`internal/group` 的窗口与熔断），调用前 `Admit`、调用后 `Report`，熔断时任务惰性转 `Waiting(Group)`，半开定时器唤醒最早的任务去探测，`group_up` 唤醒全部，`m.GroupReset` 复位额度熔断；`Status` 带每组窗口/熔断和每个限流器的快照；重启时 `Waiting(Backoff)` / `Waiting(Group)` 直接就绪。新提交路径（退避、等组、截断重做、唤醒）都遵守 4.1 规则 4。
   验证：`TestRateLimitedWaitsWithoutSpendingRetries`、`TestOverloadAndTransientBackOff`、`TestModelRetriesRunOut`、`TestPermanentFailsAtOnce`、`TestOnlyALostResponseIsUnconfirmed`（第 18 节 12）、`TestTruncatedTurnIsRedoneWithALargerCap`、`TestQuotaExhaustedHoldsTheGroupUntilReset`（第 18 节 16）、`TestTransientFailuresOpenTheBreaker`、`TestSharedLimiterSerialisesCalls`、`TestCancelAndDeadlineWhileWaiting`（第 18 节 10 的退避、熔断等待，外加等额度）、`TestCrashAtEveryCommitPointWithModelFailures`（同一任务里临时错误 + 限流，每个提交点前后各崩一次）、`TestSettleBeforeObserve`、`TestRetryCommitsYieldToACommittedCancel`。
   `Used.Steps` 只计有回答的调用，失败的调用不计、响应丢失只计 `Unconfirmed`（`TestRateLimitStormDoesNotSpendTheBudget`：20 次限流、`MaxSteps = 3` 仍完成；`TestFailedCallsAreNotSteps`）。
   评审后修正：`WaitingFor` 在锁内复制排序键（数据竞争）；截断重做的上限受限流器约束、放不大就直接 `truncated`、有回答后清零；无提示的 429 计入重试；适配器钩子的 panic 不再拖垮进程；退避步长防溢出。`TestCrashAtEveryCommitPointWithTheGroupDown`：额度耗尽后崩溃，重启从 `Waiting(Group)` 恢复。
   弹性 vCore 与多执行组（阶段 4 完成）：`Config.Groups`（单组字段保留为简写，窗口固定在 `Cores`）；每组的 `Scaler` 接进 vCore 循环（6.1 的实现说明），窗口通过 vCore 数真正约束并发；`IdleRetire` 空闲回收到 `Min`；`core_up` / `core_down` 带原因；`m.SetCores` / `m.AutoScale`；熔断时按 `Fallback` 链迁移，能力校验（`model.Caps`、`task.EstimateTokens`），失败按 `OnMigrateFail`；`m.SetClass` 手动迁移；`PinClass`；发往新服务商的请求不带外来 reasoning；多个组共享限流器；每组 `MaxQueued`；`m.Queue()`、`Status` 的每组 vCore 与队列；L2 接进机器（终态提交后 `ForgetTask`、派发用 `RunCached`、命中的 `tool_result` 带 `cached`、`Status.Cache`）；stdio 前端接上 `class` / `cores` / `autoscale` / `group_reset` / `queue`。
   验证：`TestCoresTrackProviderCapacity`（第 18 节 13，机器层，K = 4）、`TestOneEpochOfOverloadsHalvesTheWindowOnce`（14）、`TestTwoGroupsShareALimiter`（15）、`TestMigrationToTheFallbackGroup` / `TestMigrationRefusedByEachCheck`（17：成功、三种校验 × `wait` / `fail`、`PinClass`）、`TestCoresRetireOnlyBetweenInstructions`（19）、`TestSetClassMovesATask`、`TestSetCoresAndAutoScale`、`TestIdleCoresRetireDownToMin`、`TestStopShutsEveryCoreDown`（`shutdown`，goroutine 数回落）、`TestGroupConfigIsValidated`、`TestGroupMaxQueued`、`TestCachedToolResultThroughTheMachine`、`TestCrashAtEveryCommitPointWithAMigration`（两组，任务中途迁移，每个提交点前后各崩一次）。
   评审后修正：后备组恢复时唤醒链上停着的任务（`TestParkedTaskMigratesWhenItsFallbackReturns`：本组额度熔断；`TestParkedBacklogMovesWhenTheFallbackComesBack`：本组临时错误熔断，积压在后备组恢复后立刻迁走，不再每个半开周期一个）；类已删除的恢复任务移到默认组或以 `class_removed` 失败（`TestRecoveredTaskOfARemovedClass`）；`forProvider` 去掉空消息后合并同角色邻居。
   没做：`suspend` / `resume` / `priority`。
5. **上下文压缩**（5.3）：`StepCompact` 是一条模型指令，与模型步共用 `modelCall`；按组的 `CompactAt` / `CompactKeep` 触发和保留，按窗口切块，一块一条指令；摘要以 user 消息替换换出的轮次，原始消息进 Store 的归档（`CommitSet.Archive`、`Store.Archive`，两个 Store 和一致性套件），`m.History` 取回完整历史；`model.ContextLength` 从两个适配器分出来，触发压缩而不是失败；迁移的第 3 条校验接上，放不下但能压的任务迁移后先压缩；`context_overflow`。
   验证：`TestCompactionTriggersAtTheThreshold`（阈值、组的 `CompactAt`、压缩请求都放得进窗口、计步）、`TestLongTaskCompactsAndCompletes`（60 轮对 4k 窗口：完成、每个请求配对、请求原样在头、最新轮次原样、历史完整）、`TestMultiChunkCompactionAfterAMigration`（64k → 4k 迁移，第一条指令是压缩，分块 i/n）、`TestOversizedMessageOverflows`（请求、最新一轮、放不进压缩请求的旧轮次）、`TestContextLengthRejectionCompactsThenSucceeds`（窗口未知、服务商拒绝 → 压缩 → 成功；压不下 → `context_overflow`）、`TestCompactionFailureBacksOff`、`TestCancelAndDeadlineDuringCompaction`、`TestCompactionCountsAgainstTheBudget`、`TestSteerDuringCompactionReachesTheNextStep`、`TestCompactionYieldsToACommittedCancel`、`TestNextChunk`、`TestCompactRequestEstimateIsExact`、两个适配器的 `TestCompactedRegistersRender` 和 `ContextLength` 分类用例、一致性套件的 `ArchiveAppendsWithItsTask` / `ArchiveIsAllOrNothing`、`TestCrashAtEveryCommitPointWithACompaction`（640 token 窗口，付款后中途压缩两次，每个提交点前后各崩一次，含压缩调用中途，重启后寄存器和历史都配对、完整）。

5. **工具策略与人工批准**（9.7）：`Config.ToolPolicy` 在每个调用第一次派发之前决定（放行、拒绝、改参数、要批准），决定与调用同一次提交、写在调用记录上，恢复不再问；`Waiting(Approval)` 整轮等待，`m.Approve` 允许、拒绝或编辑，最后一个决定的提交派发这一轮；取消、Deadline、`Stop`、恢复、插话、`Status` / `Queue`；stdio 的 `approve`、`not_awaiting_approval`；`examples/approval`。
   验证：`TestToolPolicyDecidesEachCall`（放行、拒绝、改参数、改成非法 JSON：调用记录、事件、设备只见放行的、tool 消息顺序）、`TestToolPolicyPanicDenies`、`TestApprovalAllowDenyEdit`、`TestMixedRoundKeepsOrderAndPairing`（放行、待批准、拒绝混在一轮：批准前什么都不跑，之后顺序与配对不变）、`TestApproveErrors`、`TestApproveRightAfterTheAnnouncement`（收到事件立刻批准、两个操作员同时批准同一轮）、`TestCheckoutWaitGivesUpOnceTheTaskIsHandedBackElsewhere`、`TestConcurrentApprovalsAllGetTheTask`、`TestFiveApprovalsThenReadyAllReturn`、`TestEntryAfterAReadyHandBackGivesUp`、`TestCancelInsteadKeepsADenialWithItsEvent`、`TestRewriteArgsMustBeAnObject`、`TestCancelInsteadSettlesALoggedPlannedCall`（`execTools` 的提交被取消拒绝时，库里已有的待批准 `planned` 调用由 `cancelSet` 记 `cancelled`）、`TestCancelWhileAwaitingApproval`、`TestDeadlineWhileAwaitingApproval`、`TestSteerWaitsForTheApprovedRound`、`TestApprovalSurvivesARestart`（`Stop` 立即返回，重启后再宣布、不再问钩子）、`TestCrashAtEveryCommitPointWithAnApproval`（支付要批准，每个提交点前后各崩一次，批准在重启前或重启后给出，钩子不重问已决定的调用，支付至多一次）、`TestApproveOverTheWire`。

## 18. 测试

**模拟器（`sim`）**：假时钟、脚本模型（可注入 `RateLimited` / `QuotaExhausted` / `Overload` / `Transient` / 截断 / 响应丢失）、可控延迟和可注入"执行了但结果丢失"的设备，全部在 `-race` 下跑。

状态机（阶段 1）：

1. 每个工具调用恰好一个结果，顺序与调用一致。
2. 一个 TCB 任何时刻只有一个所有者。
3. 工具在途时，即使只有 1 个 vCore，也能推进其他任务。
4. 后台任务在持续的交互负载下，`AgingAfter` 之后仍会被执行。
5. 插话在任何状态下送达，或以 `steer_dropped` 报告。
6. 模型调用次数 = `StepModel` + `StepCompact` 指令数（= `step` 事件数，每次调用尝试一个；`Used.Steps` 只是其中有回答的那些，见 4.6）。

持久化与恢复（阶段 2、3）：

7. **在每一个提交点之前和之后各注入一次崩溃**，重启后：所有任务都能结束；`SideEffect` 调用的自动派发次数 ≤ 1；`ReadOnly` / `Idempotent` 调用重派时携带相同的 `Key`；不存在"事件已发布、状态里却没有"的情况。
8. 事件补读：从任意 `Seq` 订阅，得到的事件序列与一直在线的订阅者完全相同；内存缓冲溢出时订阅者无缝切到补读。
9. `Submit` / `Steer` 返回成功后立刻崩溃，重启后该任务 / 插话都在。
10. Deadline 在就绪、等待工具、退避、待核对、熔断等待、挂起、执行中七种状态下都能触发。
11. 取消时：`planned` 调用从未派发；在途调用收到 context 取消；晚到结果记为 `late` 且不进寄存器。
12. `Unconfirmed` 只在响应丢失时增加；`Used` 只包含响应里读到的用量。

伸缩、限流与迁移（阶段 4）：

13. 脚本服务商在并发超过 K 时返回 `Overload`：稳定后存活 vCore 在 K 附近振荡，不超过 2K。（`internal/group` 里有离散事件版本，K = 8；机器层版本 K = 4、300 个短任务、假时钟推进。）
14. 同一周期内 10 个在途请求同时返回 `Overload`，窗口只减半一次。
15. 两个执行组共享一个限流器：两组合计的请求速率不超过限流器的额度。
16. `RateLimited` 不改变窗口；`QuotaExhausted` 熔断且不自动半开。
17. 迁移校验：目标窗口放不下且无法切块压缩时，按 `OnMigrateFail` 处理；能切块压缩时迁移，第一条指令是压缩；迁移成功时发往新服务商的请求不含原服务商的 reasoning 块。（断言在目标模拟模型实际收到的请求上做。）
18. 中立格式经 OpenAI 和 Anthropic 适配器各往返一次，与原文一致（reasoning 块按规则保留或丢弃，`ProviderCallID` 按规则使用）。
19. vCore 只在指令之间退役。
20. 32 个并发 `Submit` 对 `MaxQueued = 4`：恰好 4 个被接受。

**真实对比**：eval-go agentbench 里加 neumann 适配器，和 agent-go、harness-rs 在 core-30 + hard-15 上比，经过同一个计费代理统计成本。

## 19. 第一版验收标准

1. 第 18 节全部通过，`go test -race ./...` 为绿。
2. 真实模型上，一个带 `SideEffect` 工具的多轮任务在工具执行中途 `kill -9`：重启后任务进入 `Waiting(Reconcile)`，`resolve` 之后完成；工具实际只执行了一次。
3. 真实网关上，同时提交 200 个短任务（单组、自动伸缩、`Max = 0`）：全部完成；出现过载后窗口回落再增长；计费代理统计到的模型请求数 = 各任务 `Used.Steps` 之和 + `Unconfirmed` + 带回答失败的调用数（4.6；连接被拒绝的请求代理看不到）。
4. agentbench core-30 + hard-15（rubric 判定、2 轮）：通过数不低于 harness-rs 减 2；每题 total token 不高于 harness-rs 的 1.1 倍。
