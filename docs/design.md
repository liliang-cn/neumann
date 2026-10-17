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
- 输出 lint。确定性检查以后作为可选协处理器提供，默认关闭。第一个这样的钩子是答案检查（9.8）：宿主给一个函数，neumann 只负责在什么时候问、问的结论怎么提交。
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
    Role     Role
    Parts    []Part
    Summary  bool // 压缩放进来的摘要消息（5.3）；适配器照普通消息发
    Feedback bool // 答案检查驳回答案时放进来的反馈（9.8）；适配器、压缩都照普通 user 消息处理
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
    Rejections int             // 被答案检查驳回的最终答案数（9.8）；检查看到的 Attempt
    Partial    string          // 被抢占或流中断的模型调用已收到的正文，下一次调用续写（6.4）；有回答时、续写被拒时清空
    PartialThink string        // 同上，但调用还在思考、没有正文：已收到的思考，下一次调用接着想（6.4）；与 Partial 至多一个非空
    Preempts   int             // 本步被抢占的次数，有回答时清零（6.4）
    NoThink    bool            // 下一次调用关掉思考：只想不答的截断之后（13）；有回答时清除
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

插话可以带发送方和幂等键（`m.SteerWith`，见 10.1）：带发送方的经 `Config.MessageFormat` 送达，默认说明它来自谁、不是用户、是信息而不是指令，送达有 `steer_delivered` 事件；不带的与以前逐字节相同。

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
4. 按 `Class`、`Priority`、`ReadySince` 重建就绪队列；所有未结束任务重设 Deadline 定时器。`Waiting(Reconcile)` 和 `Waiting(Approval)` 保持原状，各自再宣布一次等待的调用（9.7）。`Suspended` 保持原状（`recovered`，`from = "suspended"`）；未落地的挂起、恢复中断按 9.9 在这里落地。`Waiting(Backoff)` 和 `Waiting(Group)` 提交为 `Ready`（`ReadySince` 不变，`Retries` 保留）：退避定时器和熔断状态都不持久，重启后各组从闭合开始，退避从下一次失败重新起算。
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
   - **最终文本**：调用期间到达了插话，则文本作为 assistant 轮次，`PC = StepModel`，**插话留在中断表里不合并**：下一条模型指令开头再送达，若下一步因预算或模型出错而结束，它由终态提交报告为 `steer_dropped`。没有插话则 `Done`；配置了答案检查时先问它（9.8），驳回则带着反馈再来一步。
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
    Preempt     bool          // 允许抢占本组在途的模型调用（6.4），默认关
    PreemptAfter time.Duration // 调用至少跑这么久才能被抢，默认 2s
    MaxPreempts int          // 一步最多被抢几次，默认 2；之后不再被抢
}
```

窗口上限还受适配器的 `Caps.Parallel`（后端同时处理的请求数，0 = 未知）约束：有效上限是 `Max` 和 `Parallel` 中不为 0 的较小者，单组简写的 `Cores` 同样取较小者，`SetCores` 超过它时按它截断。`Min` 大于 `Parallel` 是配置错误（`New` 报错）。`Parallel` 在 `New` 时读一次（适配器 panic 按未知）。

`New` 时校验：类名非空、不重复、不含 `/`；每组有 `Model`；`0 ≤ CompactKeep < CompactAt ≤ 1`（0 取默认）；`Limits` 里的名字都在 `Config.Limiters` 里；`Fallback` 指向存在的组，沿 `Fallback` 链走不会回到起点（无环）；`OnMigrateFail` 只能是 `wait` / `fail`；`Max > 0` 时不小于 `Min`。`Config.Default` 是不带 `Class` 提交的任务的类，空则取第一个组。

单组简写：`Config.Groups` 为空时，`Model`、`Class`（默认 `default`）、`Cores`（默认 4）、`Limits` 构成一个组，窗口用 `SetCores(Cores)` 固定：至多 `Cores` 个 vCore、有活才启动，和以前的固定 vCore 数一致；`AutoScale(class, true)` 可让它自适应。`Preempt`、`PreemptAfter`、`MaxPreempts` 也有简写（`Config` 上同名字段）。简写字段与 `Groups` 同时出现是配置错误。

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

### 6.4 抢占与续写

本地后端（Ollama、llama.cpp、LM Studio）一次只处理很少几个请求：实测 Ollama 的 MLX runner 严格串行，多发的请求在后端内部排队。那个队列机器看不见、不能重排，排队时间还算进请求超时（2026-10-16 实测：第二个请求在 Ollama 里排了 4 分 48 秒）。原则是**队列留在机器里**：并发对齐到后端能力（`Caps.Parallel`，6 节），其余任务在就绪队列里等，重排和抢占才有意义。

一颗被占满的"CPU"上来了更急的任务，就该像操作系统一样抢占。和 CPU 不同的是，模型调用的"寄存器现场"是已经生成的文字：丢掉就要重新生成。所以被抢的调用**保留已收到的正文，下次让模型接着写**（续写，prefill）。实测 Ollama 收到以 assistant 消息结尾的请求时只返回接下来的部分，而且跳过思考；把半截思考作为 assistant 的 reasoning 回传则不行，模型会从头重想，还把正文混进思考里。但把半截思考放在 assistant 消息的**正文**里、前面加上开标签（`<think>\n` + 思考）可以：Ollama 接着想下去，回答全在 `content` 里——剩下的思考、`</think>`、正文（2026-10-16 实测，qwen3.5:9b-mlx）。这个模型连简单任务也先想几千块，抢占几乎总落在思考里，只留正文就等于丢掉全部思考（实测丢了 40 秒，重想 4 分钟）。所以：正文已经开始时只保留正文（思考已经结束）；还在思考时，适配器能接着想（`Caps.ThinkPrefill`）就保留思考，下次接着想。半截的工具调用参数总是丢弃。

**抢占条件**（组的 `Preempt` 开着才会发生）：本组有就绪任务 `h`；存活 vCore 已经达到窗口、没有空闲的；在途的模型调用（`StepModel`，压缩不抢）里有任务 `v` 满足：

- `h.Priority < v.Priority`，比较 TCB 上的原始优先级，不看老化：老化只决定出队顺序，被老化提上来的后台任务不能抢别的后台任务；
- `v` 这次调用已经跑了至少 `PreemptAfter`，不满时挂一个定时器，到点再查；
- `v.Preempts < MaxPreempts`。

多个候选时抢优先级数最大（最不急）的，同级抢这次调用跑得最短的（损失最少）。一次只抢一个，被抢的任务交还之后再查下一次。检查的触发点：任务进入就绪队列、`SetPriority` 提交之后、上面的定时器到点。

**被抢的调用**：用 `errPreempted` 作为原因取消这次调用的 context（任务自己的 context 不受影响），适配器像对待取消一样返回。组开着抢占时，机器总是用流式调用（适配器实现了 `Streamer` 就用，不管有没有增量接收方），自己累加收到的正文增量（`Delta.Text`）。然后一次提交：`step` 和 `preempted`（`{by, kept, ran_ms, n}`：抢它的任务、保留的字符数、这次调用跑了多久、这一步第几次被抢）；任务回 `Ready`，PC 不变，`ReadySince = now`，`Preempts + 1`；`Partial` 追加这次收到的正文，适配器不支持续写（`Caps.Prefill` 为假）时清空、`kept` 为 0。

- 不计 `Used.Steps`（没有回答），不计 `Retries`，不计 `Unconfirmed`：请求是机器自己切断的，结果已知。本地后端不计费；远端服务商上开抢占，被切断的那部分用量就算不进 `Used`，这是配置者的选择。
- 限流器按"被取消打断的调用"一行结算（7.2）；不向执行组报告结果（同 `Canceled`），窗口和熔断都不受影响。
- 这次调用用掉的插话退回待送达，和失败的调用一样。
- `ReadySince = now`，不保留原来的：保留的话，一个等了很久的后台任务会被老化立刻提成交互级，按先进先出又排在 `h` 前面，抢占就成了空转。
- 抢占和取消同时到达时取消赢，按 9.5 的被取消打断处理，`Partial` 随任务一起丢弃。

**续写**：下一次模型步，`Partial` 不为空时：

- 没有待送达的插话，当前组的适配器 `Caps.Prefill` 为真：请求末尾加一条 assistant 文本消息，内容就是 `Partial`。回答的正文接在 `Partial` 后面合成一条消息：第一段文本前面接上 `Partial`，没有文本段时把 `Partial` 作为第一段；工具调用照常。用量只计这次的。
- 有待送达的插话：`Partial` 先作为一条完整的 assistant 消息进寄存器（被打断的那一轮），插话跟在后面，像人打断别人说话。这一步不再续写。
- 适配器不支持续写（例如任务迁到了别的组）：丢弃 `Partial`，从头生成。
- **接着想**：`Partial` 为空、`PartialThink` 不为空、没有待送达的插话、适配器 `Caps.ThinkPrefill` 为真：请求末尾加一条只有 reasoning 段（内容就是 `PartialThink`）、没有正文的 assistant 消息。回答的 reasoning 接在 `PartialThink` 后面（第一个 reasoning 段前面接上，没有就作为第一段），正文照常。有待送达的插话、或适配器不能接着想时丢掉 `PartialThink`：问题变了，或者没法接。
- 有回答时 `Partial`、`PartialThink` 清空、`Preempts` 清零。**截断与否只看这次调用自己产出了什么**：续写的调用回来 `length`、没有新正文也没有工具调用，就是"截断且无内容"，按 13 处理（只有思考时先关思考），`Partial` 留着给重做续写，`PartialThink` 清空（跑满上限的思考不再接着想）；不是只把前缀当作答案结束任务。有新内容的截断照常按"有内容的截断"处理。接着想的调用回来没有正文、没有工具调用、只有思考却以 `stop` 结束（模型想完了没作答），清空 `PartialThink`，按下面"想完了没作答"处理（`TestAThoughtThatStopsIsRedoneWithoutThinking`）。
- 窗口估算（5.3）把 `Partial` 算在请求里。

**流中断也续写**：调用以 `Transient` 且 `ErrResponseLost` 失败时（流读到一半断了、空闲超时），已收到的正文（或还在思考时的思考）同样保留，随退避的提交落盘，重试时续写。`Unconfirmed` 照旧加一。

**续写被拒就从头来**：续写的调用（请求以 `Partial` 或 `PartialThink` 结尾，`step` 带 `prefill` / `prefill_think`）失败了，不是被抢、挂起、取消或 Deadline 切断的，这次调用一点东西都没流出来，而且失败是服务商对这个请求的回答，就在记录这次失败的那个提交里丢掉前缀，`Partial`、`PartialThink` 都清空，重试是一次从头的调用。照原样再发多半得到同样的结果：实测 Ollama 对接着想的请求回空回答（适配器报"既无正文也无工具调用"的临时错误），同一个请求连发 7 次都一样，任务差点以 `model_error` 结束；连续失败还计入执行组，可能打开熔断，把同一后端上的其他任务一起卡住（2026-10-17，qwen3.5:9b-mlx）。

- "服务商对这个请求的回答"指三种：完整回来却没有可用内容的回答（适配器的错误包 `model.ErrEmptyResponse`，11）；请求到达之后回答没有完整回来、或回来了解析不了（`ErrResponseLost`，这次调用什么都没流出来）；拒绝（`Permanent`，例如服务商不收 assistant 前缀、不收以空白结尾的前缀）。限流（带不带提示都一样：服务商什么都没生成，带提示的连重试都不计）、额度耗尽（任务可能迁到别的组，到那里由新组适配器的 `Caps` 决定续不续写）、过载、其他 5xx、连接被拒（请求根本没到，`Unconfirmed` 不加，18.12）都和前缀无关，前缀留着。
- 流出过东西就不丢：后端显然接得下去。流中断时收到的照上一段接上；以分了类的流内错误（OpenRouter、vLLM 的 `{"error": ...}` 块）结束时，收到的不保留（只有流中断才保留），前缀原样留给重试；这时的拒绝照旧让任务 `Failed`。
- 拒绝本来让任务 `Failed`；续写被拒时改为丢掉前缀、按临时错误退避重试一次（计一次 `Retries`，`model_retry` 的 `class` 是 `permanent`）：从头的调用是另一个请求，多半能回答。从头的调用再被拒，照旧 `Failed`（`model_error`）。这一次重试是每个保留的前缀一次，仍受重试上限约束；和前缀无关的拒绝（key 不对、模型不存在）落在续写的调用上，就多花一次退避后的请求才失败。
- "流出了东西"按前缀的种类算：正文对两种前缀都算；思考只对接着想的调用算。续写正文的调用流出的思考接不到 `Partial` 上，先想了一会儿再回空回答、或流只带思考就断了，都按什么都没流出来处理，照丢（`TestTextContinuationThatOnlyThinksStartsOver`）。OpenAI 兼容适配器上，续写正文的调用先想再回空正文、正常结束，回来的是只有思考的回答，不是空回答：按下面"想完了没作答"关思考重做、`Partial` 留着（`TestTextPrefillThatOnlyThinks`）；走到这一条的是流只带思考就断了，或直接报空回答的适配器。
- 重试次数、退避、`model_retry` 照旧；`model_retry` 多带 `dropped`（丢掉的正文字符数）或 `dropped_think`（丢掉的思考字符数），下一次调用的 `step` 不再带 `prefill` / `prefill_think`。任务因此失败（重试用完）时没有 `model_retry`，前缀随任务结束。
- 从头的调用不关思考。"想完了没作答"关思考，是因为照原样重想还会想完不答；这里没有迹象说明思考本身有问题（流断之前那次调用正想得好好的），失败的是续写这个请求，等同于"适配器不能接着想"时的从头生成。`NoThink` 只由截断（13）和想完了没作答（下一段）设置，这里原样保留。
- 上下文超长的拒绝（`ContextLength`）不在此列：按 5.3 学到更小的窗口、压缩后重发，前缀算在窗口估算里，压缩就是对它的回答。
- 丢弃和失败在同一个提交里，重启后看到的是没有前缀的任务，不会永远重发过时的前缀；失败还没提交时崩溃，重启按 4.5 重做那次续写，再失败照样丢。
- 流中断时保留什么仍按 `keepPartial`：接着想的调用已经流出了正文、适配器能续写正文（`Caps.Prefill`），正文进 `Partial`，思考丢掉（正文开始了，思考已经结束）；适配器不能续写正文，这段正文丢掉，思考（加上这次新流出的）留着，重试再接着想。

**想完了没作答**：调用正常结束（不是 `length`，没被切断），回答没有正文、没有工具调用，却有思考（reasoning 段、用量报了推理 token，或这次调用流出过 `Delta.Reasoning`），是模型把整个回答想完了、没写出来就停了。不管这次是不是接着想的调用，都按同一条处理；接着想的调用回来什么都没有，也算（它本来就在想）。续写正文的调用回来什么都没有不在此列：前缀就是完整的回答。实测本地小模型常这样：Ollama 上的 qwen3.5:4b 读完文件，把列名在思考里数了一遍（"They are: 1. order_id 2. date … 7. revenue"）就以 `stop` 结束，`content` 为空；同一个请求重放三次，两次如此、一次正常作答。原来适配器把它报成"既无正文也无工具调用"的临时错误、思考丢掉，机器照原样重发：一个任务连续 8 次都是这样，以 `model_error` 结束，连续失败还打开了组的熔断，卡住同一后端上的其他任务；qwen3.5:9b-mlx 一次 9 个任务的运行里，20 次重试有 16 次是它（2026-10-17）。

- 它不是服务商的错误：适配器把它作为回答返回（11），机器把它当作一次有回答的调用——计 `Used.Steps`、计用量，限流器按回答结算，执行组记一次成功，熔断不受影响。但它不是答案：寄存器、插话、`Partial` 像"截断且无内容"一样撤回，`PartialThink` 清空，这一轮重做。
- 适配器 `Caps.ThinkOff` 为真、任务还没关思考：置 `NoThink`，`MaxOut` 不变，立即重做（不退避）。和只有思考的截断（13）同一个函数：清零 `Retries`，不计 `Truncations`，`model_retry` 是 `{class: "thought", no_think: true, max_out, attempt, delay_ms: 0, wait: "none"}`。关掉思考，模型就得把回答写在正文里（实测 Ollama 收到 `reasoning_effort: "none"` 直接作答）。`NoThink` 照旧留到下一次有回答且没被截断的调用。
- 关不掉思考（`Caps.ThinkOff` 为假），或已经关了模型还是只想不答（后端不理会关思考）：照原样立即重做，不放大 `MaxOut`（没有被截断，放大没用），不退避（服务商答了，等也没用）。这一次计入 `Retries`，接着之前的连续失败往上数（不清零），`model_retry` 是 `{class: "thought", attempt, delay_ms: 0, wait: "none"}`；到 `MaxModelRetries` 时任务 `Failed`（`model_error`，"the model stopped after thinking, with no answer (N consecutive failures)"）。重放实测三次里有一次正常作答，所以值得照原样再试，次数和临时错误一样有上限。
- 关思考重做回来又是空的（既无正文、无工具调用、也无思考）：仍是适配器的临时错误，按 11 退避重试、计 `Retries`，`NoThink` 一直开着，到上限 `model_error`。关思考的重做只发生在还没关思考时；关了之后 `Retries` 只被有回答的调用清零：有正文或工具调用的回答（任务往前走了），或截断（它自己最多两次放大，`Truncations` 也只被正常回答清零）。所以不会在"想完了没作答"和"空回答"之间无限循环。
- 真正空的回答（没有思考）照旧是临时错误：没有迹象说明模型想过，关思考没有依据；它计入执行组，重试有上限，和原来一样。
- 状态改动都在记录这次结果的那个提交里：`NoThink` 或 `Retries` 随 `step`、`usage`、`model_retry` 一起落盘，提交之前崩溃，重启按 4.5 重做原来的调用。
- 压缩的摘要只有思考：没有摘要可以替换，按原来"空摘要"的临时错误处理。原来先把 `Retries` 清零再计一次，连续的空摘要永远停在第 1 次、无限重试（适配器把只有思考的回答报成错误时到不了这里），现在空摘要不清零，接着往上数，到上限 `model_error`。压缩的请求不关思考（它不带 `NoThink`），没有为它另加关思考的重做。

**持久化**：`Partial` 只随提交持久化。在调用中途崩溃，重启后按 4.5 重做，用的是最近一次提交的 `Partial`。加字段之前写下的 TCB 解码为空。

**重排**：`m.SetPriority(ctx, id, p)`（`p ≥ 0`，0 交互、越大越不急）。任务在调度器手里（就绪、退避、熔断等待、待核对、待批准）时取出，提交新的 `Priority` 和 `priority_set`（`{priority, was}`）事件，再交还；在 vCore 上或等工具时返回 `ErrTaskBusy`，调用方重试。已结束的返回 `ErrFinished`。提交之后做一次抢占检查。

实现中的决定（`internal/core/preempt.go`，续写在 `internal/core/model.go` 的 `modelStep` / `modelCall`）：

- **登记**：开着抢占的组，每个模型步（压缩不登记）在 `Running` 提交、插话合并之后、调用之前登记一条在途记录（任务、原始优先级、`Preempts`、vCore、开始时刻、调用自己的 `context.WithCancelCause`），调用返回即注销。注销时取消原因是 `errPreempted`、调用出了错、任务自己的 context 还没结束，才算被抢；被选中的同时回答已经到了，回答照常采用。**登记之后立刻查一次**：调用从取到任务到真正发出之间（老化让它排在更急的任务前面被取走、在限流器上等额度、`Running` 提交）可能已经有更急的任务就绪，那时的检查看不到这次调用，之后也没有别的触发点，它就会一直跑完（评审发现，`TestPreemptACallTakenAheadByAging`、`TestPreemptACallThatWaitedForRoom`）。
- **熔断探测不被抢**：半开时放出去的探测请求不登记。被抢的调用不向执行组报告结果，探测被抢就永远没有结论，组要等熔断器放弃这个探测（`OpenFor`）才再放一个，期间本组什么都发不出去（`TestProbeIsNeverPreempted`）。其他被放行的请求没有这种特殊身份。
- **空出的 vCore 给谁**：被选中的 vCore 记下它是为哪个任务让出的；执行完这条指令后，它下一次取指先直接取那个任务（`Sched.TakeReady`），那个任务已经不在就绪队列（被别的 vCore 取走、被取消）才照常按队列取。否则老化会把它交给一个等得更久的后台任务，抢占就白做了，`h` 还得再抢一次（`TestFreedVCoreGoesToTheUrgentTask`）。vCore 因窗口缩小退役、没去取时，再查一次。被让出给的任务在这期间被 `SetPriority` 调得比被抢的任务还不急，空出的 vCore 仍然先取它：多抢了一次，无害，不为此另加判断。
- **"没有空闲 vCore"**：`存活 ≥ floor(W)` 且 `存活 − 忙碌 + 让出中 = 0`。"让出中"是被选为牺牲者、还没执行完这条指令的 vCore：它马上会空出来，算作空闲，所以被抢的任务交还时（`OnReady`）、那个 vCore `Release` 之后都不会再抢第二个。**"一次只抢一个"的下一次检查放在空出来的 vCore 取到下一条指令时**（`runCore` 里 `Acquire` 之后），而不是交还时：交还发生在 `Release` 之前，那时忙碌数里还算着它，若在那里查就会为同一个 `h` 多抢一个。`Release` 之后清掉让出标记再查一次（vCore 因窗口缩小退役时由这一查补上）。
- **候选与选择**：`h` 是就绪队列里原始优先级最小的（同级 `ReadySince` 最早，`Sched.Urgent`）；不看老化。多个候选：优先级数最大的，同级开始最晚的（跑得最短），再同取任务 id 小的，结果确定。未满 `PreemptAfter` 的候选只决定定时器：组里只挂一个，取最早到期的那个，更早的到期会换掉它；到点再查一次。停机开始后（`Sched.Stopping`）或强制停机后不抢。
- **取消一个调用**：只结束调用自己的 context（原因 `errPreempted`）；`errPreempted` 不会到达任务的 context，所以取消信号、Deadline 照旧各走各的。之后的判断顺序：先看强制停机，再看取消（`Sched.Cancelled`，取消赢，按被取消打断处理，`Unconfirmed + 1`），再看抢占。取消已提交但信号未到时，`preempted` 提交被 `ErrCancelPending` 拒绝，`cancelInstead` 照常最终提交，没有 `preempted` 事件（`TestCancelBeatsPreempt` 两种都测）。
- **提交的事件**：`step` 在前、`preempted` 在后，没有 `usage`（`observe` 把 `usage` 当作有回答的调用）；没有 `steer_delivered`（插话退回）。前面可能有 `throttled`。`preempted` 是 `{by, kept, kept_think, ran_ms, n}`：`kept` 是留下的正文字符数，`kept_think` 是留下的思考字符数（两者至多一个不为 0）。接着想的调用的 `step` 带 `prefill_think`（思考前缀的字符数），续写正文的带 `prefill`。都按字符（rune）计。
- **保留哪段文字**（`keepPartial`，被抢和流中断同一个函数）：只有这次调用没合并插话时才保留（续写的调用，或没有 `Partial` 的新调用）。任务已有 `Partial`（续写正文，或插话要打断的那一轮）：适配器能续写就把收到的正文接上，不能就什么都不加，但**已有的 `Partial` 不清**——它是插话要打断的那一轮，下一步照样落成 assistant 消息再送插话（评审指出原来被抢时清掉、流中断时保留，两条路不一致）。没有 `Partial`、正文已经开始、能续写：正文进 `Partial`，`PartialThink` 清空。还没有正文、能接着想：思考追加进 `PartialThink`。合并了插话的调用是对插话的新回答：插话退回待送达，收到的东西都丢掉（`kept`、`kept_think` 为 0），先前的 `Partial` 原样恢复——否则下一步会把"对插话的回答"当作被打断的一轮放到插话前面。
- **插话与 `Partial`**：`Partial` 落成 assistant 消息这件事在 `apply` 钩子里做（`Running` 提交之后，和合并插话同一处），调用没有回答（失败、退避、被抢、被取消、截断且无内容）时连同插话一起撤回、`Partial` 恢复。所以"截断且无内容"时 `Partial` 不清空——它随插话退回，重做时再落成一轮。`Preempts` 在任何有回答的调用后清零。
- **合并**：`Partial` 接在回答第一段文本的前面；回答没有文本时作为一段文本插在工具调用之前、开头的 reasoning 之后（服务商要求 reasoning 在前）。`PartialThink` 接在第一个 reasoning 段前面，没有就作为第一段。截断判断在合并**之前**、只看这次调用的回答做（评审发现：原来先合并，只想不答、跑满上限的续写因为合并后"有内容"，任务就以被截断的前缀作为答案结束了，`TestThinkingOnlyContinuationIsRedone`）。
- **始终流式**：组开着抢占、或机器有增量接收方（`Machine` 总是有）时，适配器实现了 `Streamer` 就流式调用；收到的 `Delta.Text` 累加在 vCore 上，有接收方才转发。所以"流中断也续写"不要求组开着抢占：只要这次调用是流式的、适配器 `Caps.Prefill` 为真。
- **`first_ms`**：机器时钟上从调用适配器前一刻到第一块增量（正文或推理都算）的毫秒数，`step` 事件在调用之后改写带上它；`prefill` 在合并插话的钩子里写上。`check_ms` 也改为在原事件上改写，三者可以同时出现。
- **`SetPriority`**：优先级没变时什么都不提交、返回 nil；`ReadySince` 不变（不重新排队尾）。负数报错。`Submit` 也放开到任何 `≥ 0` 的优先级（原来只接受 0、1）。调度器本来就按数值比较、老化把任何大于 0 的提成 0，`Queue` 的 `Background` 计所有大于 0 的。
- **续写被拒**（`modelCall`）：`modelStep` 把它是否在请求末尾放了前缀作为参数传给 `modelCall`（压缩传假），不从 TCB 推断。失败分支里被抢、挂起、取消先走各自的路；流中断先 `keepPartial`；`ContextLength` 去压缩；然后，续写的调用、任务的 context 没结束（取消、Deadline、强制停机）、这次调用的流里没收到能接上前缀的东西（正文都算，思考只在接着想时算）、`prefixRefused`（`Permanent`，或 `Transient` 且包 `ErrEmptyResponse` / `ErrResponseLost`）时清空两者：`Permanent` 走计次的退避（`retry`，`modelFailed` 的计次部分拆出来的），其余照常 `modelFailed`；再在 `model_retry` 上补 `dropped` / `dropped_think`（`amendRetry`，像 `amendStep` 一样按类型 `retryData` 改写原事件）。适配器 panic 不在此列，任务照旧失败。没有新增提交点：丢弃随记录失败的那次提交落盘（`TestFailedThoughtContinuationStartsOver`、`TestFailedContinuationStartsOver`（从这次提交重启）、`TestLostContinuationThatAddedNothingStartsOver`、`TestRejectedContinuationIsRetriedFresh`；保留前缀：`TestContinuationFailuresThatKeepThePrefix`、`TestContinuationErrorAfterStreamingKeepsItsPrefix`、`TestBrokenContinuationKeepsWhatItAdded`、`TestCutOffContinuationKeepsItsPrefix`（被抢、挂起，包括第一块之前就被挂起））。
- **只有思考的截断**（13）：判定为截断且无内容，并且回答里有 reasoning 段、或用量报了推理 token、或**这次调用流出过 `Delta.Reasoning`**（Ollama 两者都不报，截断的回答连段都没有，只能从流里看）。适配器 `Caps.ThinkOff` 为真且任务还没关思考时，置 `NoThink`、`MaxOut` 不变重做：一次有回答的调用，发 `model_retry`（`class = "truncated"`，`no_think: true`，`max_out` 是这次的上限，`attempt` 不变），不计入 `Truncations`；关掉思考仍被截断才走两次放大，`NoThink` 一直开着。下一次有回答且没被截断的调用清除 `NoThink`（和 `Truncations` 同时）。请求的 `NoThink` 取自 TCB，适配器 `ThinkOff` 为假时它自己忽略。
- **想完了没作答**（`modelStep`）：截断判断之后，`stopped` = 没截断、没有正文和工具调用、不是续写正文的调用，并且有思考（同上三种迹象）或这次是接着想的调用。和截断共用撤回寄存器、插话、`Partial` 的分支；能关思考走 `redoWithoutThinking`（带 `stopped`，事件 `class` 为 `"thought"`），否则 `redoThought`：`Retries` 恢复为这次调用之前的值再加一，到上限 `failWith(model_error)`，否则 `model_retry` 加 `ready`。执行组看到的是成功（`modelCall` 里 `err == nil`）。压缩（`execCompact`）：截断照旧清零 `Retries`，空摘要不清零、交给 `modelFailed`，有摘要才清零（`TestAFreshThoughtThatStopsIsRedoneWithoutThinking`、`TestARedoWithoutThinkingThatStaysEmptyIsBounded`、`TestAThoughtThatStopsWithoutThinkOff`、`TestATrulyEmptyTurnStaysTransient`、`TestAThoughtForASummaryIsBounded`）。

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

组内交互级先于后台级；同级按 `ReadySince` 先进先出，每步之后 `ReadySince` 更新，所以同组任务轮转推进；后台任务 `ReadySince` 早于 `now - AgingAfter`（默认 30s）时按交互级排序。退避中的任务不在就绪队列里。优先级数可以大于 1，数越小越先；老化把任何大于 0 的提成 0。组开着抢占时，更急的任务还能把正在调用模型的任务换下来（6.4）。

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
m.Suspend(id) / m.ResumeTask(id) // 9.9
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

### 9.8 答案检查

```go
type AnswerCheck func(ctx context.Context, info AnswerInfo) AnswerVerdict // Config.AnswerCheck

type AnswerInfo struct {
    TaskID, Class string
    Request string // 任务的请求：第一条 user 消息（Instruction.Input），压缩永远保留它
    Answer  string // 模型刚给出的最终文本
    Attempt int    // 0 = 第一个被检查的答案；被驳回 n 次之后为 n（TCB.Rejections）
}

type AnswerVerdict struct { // 零值 = 通过
    Reject   bool
    Reason   string // 写进事件，要短
    Feedback string // 驳回时作为 user 消息交给模型；空 = 一句带原因的"请修改后重答"
}
// Config.AnswerCheckTimeout（默认 3s），Config.MaxAnswerRetries（默认 1，负数 = 不重答）
```

可选，默认关闭（`nil`）：没有配置时行为与之前逐字节相同，事件里不多任何字段。

- **在哪问**：只在 `execModel` 里模型给出最终答案、本来要以 `end_turn` 提交 `Done` 的那条路上：没有工具调用、不是"截断且没有内容"（5.1 第 4 步；截断前已经给出文字的回答本来就当作最终答案，同样要问，检查正好是能发现它不完整的地方）、调用期间没有插话（有插话时照旧把答案当作一轮、再走一步，不问）。预算结束的答案（5.1 第 1 步）、压缩、取消、失败都不问。
- **谁、何时问**：持有任务的 vCore，在模型调用结算之后、组装最终提交之前。和工具策略（9.7）一样，它运行时 vCore 持着任务（钩子本身在单独的 goroutine 里，vCore 等它），所以有界：钩子拿到的 ctx 由任务的 context 派生（取消、Deadline、强制停机照样打断它），再加 `AnswerCheckTimeout` 的期限（墙上时间，不走机器时钟）。钩子在单独的 goroutine 里跑，vCore 最多等这么久，不管钩子理不理 ctx；放弃之后它的结论不再采用。**超时、ctx 结束、panic 一律按通过处理**（fail open），原因记进 `done` 的 `check_error`（`check timed out`、`check panicked: …`；任务被取消、Deadline 到了或强制停机时钩子的 ctx 也会结束，但那时取消已先提交、这一步按取消最终提交，强制停机什么都不提交，所以提交的 `done` 里不会出现这种原因）：轻量的检查不能卡住任务，也不能默认丢掉答案。
- **钩子运行期间到达的输入**：当作调用期间到达的处理：检查之后再读一次中断表，有取消则按取消最终提交、答案丢弃；有插话则答案作为一轮、`PC = StepModel`。两种情况结论都作废，什么都不记。
- **通过**：与没有检查时同样提交 `Done`；`answer`、`done` 带 `checked: true`，`done` 另带 `check_reason`（钩子给了原因时）或 `check_error`。
- **驳回且还能重答**（`Rejections < MaxAnswerRetries`）：一次提交：答案作为 assistant 消息留在寄存器里，后面追加一条 `Feedback = true` 的 user 消息（反馈），`Rejections + 1`，`answer_rejected`（`{reason, feedback, attempt}`），任务 `Ready`、`PC = StepModel`，交还调度器。这一步的模型调用照常计入 `Used.Steps`，下一次回答也计，所以 `MaxSteps` 同样约束这个循环。
- **驳回且不能再重答**：重答次数用完，或预算里已没有下一次模型调用的余地（`MaxSteps > 0` 且这一步之后 `Used.Steps + Unconfirmed >= MaxSteps`，与 5.1 第 1 步同一个判断）：任务照样以这个答案 `Done`，**答案从不丢**；`Rejections + 1`，`answer_rejected` 带 `final: true`（`{reason, attempt, final}`，没有 `feedback`；因为预算时另带 `budget: true`），`answer`、`done` 带 `check_failed: true`，`done` 带 `check_reason`。要更严格的宿主可以据此重新提交。停止原因是 `end_turn` 而不是 `budget`：这一步是模型自己的回答，与重答用完同一个形状；如果把它退回去，下一步一开头就会以 `budget` 结束，带着被驳回、却没有任何标记的答案和一条没人回答的反馈。
- **预算结束的答案**：驳回提交之后，下一次请求丢了响应（例如强制停机打断调用，重启 `Unconfirmed + 1`）而用完预算时，任务在下一条指令开头按 5.1 第 1 步以 `budget` 结束，答案是被驳回的那条；这时最后一个寄存器是反馈消息，`answer`、`done` 带 `checked: true`、`check_failed: true`（没有新的 `answer_rejected`，也不再问检查）。
- **提交失败、被取消抢先**：与原来的最终提交完全一样。提交被 `ErrCancelPending` 拒绝时按取消最终提交（`cancelInstead`：从库里的任务出发，只保留已确认用量和 `step` / `usage` 事件），结论丢弃：没有 `answer_rejected`、没有反馈消息、`Rejections` 不变。
- **崩溃**：结论只通过它的提交存在。提交前崩溃，任务停在 `Running`，重启按 4.5 重做这次模型调用（`Unconfirmed + 1`），新答案再问一次检查（`Attempt` 不变）；驳回提交落盘之后崩溃，重启从反馈之后接着答，检查拿到的 `Attempt` 是持久化的 `Rejections`。钩子没有副作用的要求与 9.7 相同。
- **反馈消息**：role 为 user、`Feedback = true`。两个适配器都照普通 user 消息发（Anthropic 与相邻的 user 消息合并成一个回合）；压缩把它当作普通 user 消息，自成一个单元，可以换出到归档，文字稿里是 `### user`；`m.History` 里保留它；估算照常计入。
- **耗时**：检查占用的时间（机器时钟）记在这一步的 `step` 事件上（`check_ms`，没有检查时不出现）：`step` 和结果同一次提交，检查让这次提交晚了这么久；`observe` 用它把模型指令的时延和 span 提前结束，`neumann.model.duration` 仍是服务商的时延。
- 观测（第 14 节）：`neumann.answers.checked`（按类、`outcome`：`passed` / `failed_open` / `rejected` / `failed`）；`answer_rejected` 是任务根 span 上的事件（`neumann.attempt`、`neumann.final`），活动日志 info 一行（`attempt`、`final`），`done` 行带 `checked`、`check_failed`、`check_error`；原因、反馈、答案都不进指标、trace 和日志。
- 答案检查只能在 Go 里配置（`Config.AnswerCheck`），stdio 前端不提供。

### 9.9 挂起与恢复

```go
m.Suspend(ctx, id) error     // 提交后返回；ErrNotFound / ErrFinished
m.ResumeTask(ctx, id) error  // 提交后返回；ErrNotFound / ErrFinished / ErrNotSuspended
```

挂起是操作员把一个任务"拿下 CPU、放在一边"：它不再被调度，直到恢复。和取消一样，两条指令都先作为中断提交（中断表的新种类 `suspend`、`resume`），提交之后才返回，崩溃后仍然有效。和取消不同，未送达的挂起、恢复不拒绝任何提交（4.1 规则 4 只管取消）：它们由持有任务的一方在下一个边界取用。

**落地**：挂起不一定立刻生效，它在任务的下一个指令边界落地，由当时持有任务的一方执行：

- 任务在调度器手里、没人在干活（就绪、退避、熔断等待）：挂起方取出任务，直接提交 `Suspended`（退避定时器停掉，`WakeAt` 不保留，恢复后直接就绪）。
- 在 vCore 上、模型调用在途：像抢占一样打断这次调用（6.4），用 `errSuspended` 作为原因取消这次调用的 context，按 `keepPartial` 保留已收到的正文或思考，一次提交 `step` 和 `suspended`（没有 `usage`，插话退回待送达，没有 `steer_delivered`）。不计 `Preempts`、`Used.Steps`、`Retries`、`Unconfirmed`；限流器按"被取消打断的调用"结算，不向执行组报告。还在限流器上等额度时同样打断，什么都没发出，提交只有 `suspended`（`was = "model"`）。压缩调用不打断，压缩结束后的那次提交落地；熔断探测也不打断（被打断的探测永远没有结论，同 6.4），它的结果提交落地。
- 在 vCore 上、其他时刻（调用有回答之后、答案检查、派发工具）：这条指令的提交落地。指令开头读中断表时已有挂起：什么都不跑，直接提交 `Suspended`（`was = "ready"`）。
- 等工具（`Waiting(IO)`）：工具不打断（可能有副作用），整轮结果回来后的那次提交改为 `Suspended`。
- 待批准、待核对：照常等操作员；决定到了，那次提交若是整轮已有结果（全被拒、全部核对完），改为 `Suspended`；若是派发这一轮，这一轮照常跑完，结果回来后的那次提交落地（`was = "tools"`）。
- 终态提交（最终答案、预算、失败、取消）不挂起：任务按它自己的提交结束，挂起中断由终态提交标为已送达，没有事件。

落地的提交把挂起中断标为已送达，带 `suspended` 事件（`{was, kept, kept_think}`：`was` 是落地前在做什么，`ready` / `backoff` / `group` / `model` / `tools` / `approval` / `reconcile` / `check`；后两项只在打断模型调用时非 0），事件排在这次提交自己的事件之后。

**恢复**：任务是 `Suspended` 时，恢复方从等待集合取出，提交 `Ready`（`ReadySince = now`）和 `resumed`（`{pending: false}`），交还调度器，触发一次抢占检查（6.4）。挂起还没落地（任务在 vCore 上或等工具）时，恢复抵消它：落地方按序号依次看待送达的挂起、恢复中断，最后一条是恢复就不挂起，全部标为已送达，事件是 `resumed`（`{pending: true}`），跟在它放行的那条指令的提交里、排在那条指令自己的 `step` / `usage` 之后，任务照常往下走。最后一条是挂起就落地，前面的恢复一并标为已送达，只有一个 `suspended`。前面没有挂起的恢复（并发的重复恢复）标为已送达，没有事件。既没挂起、也没有待落地的挂起：`ErrNotSuspended`，什么都不提交。重复挂起一个已挂起（或挂起已待落地）的任务：成功，什么都不提交。

**挂起期间**：Deadline 照常触发（9.4），取消照常生效（9.5：由取消方从等待集合取出做最终提交）。插话照收，留在中断表里，恢复后的第一步送达。`SetPriority`、`SetClass` 可以改挂起中的任务，任务仍是 `Suspended`。挂起的任务仍计入未结束任务数（9.2）。保留的 `Partial` / `PartialThink` 恢复后续写，规则同 6.4（换了组、适配器不能续写就丢掉）。

**持久化**：`Suspended` 重启后仍是 `Suspended`（4.5），再发一次 `recovered`（`{from: "suspended"}`）。待送达的挂起、恢复中断在恢复流程里按上面的规则落地：`Running` 中崩溃的任务先看挂起，最后一条是挂起就提交 `Suspended`（`recovered` + `suspended`，`was = "model"`，`Unconfirmed + 1`）而不是重做；退避、熔断等待的任务同理（`was` 为 `backoff` / `group`）；就绪的任务单独一次提交；挂起的任务有待送达的恢复就 `Ready`；等工具、待批准、待核对的照常等，等完那次提交落地。

**观测**：`Status.Suspended`（JSON `suspended`）、`GroupStatus.Suspended`、`ClassQueue.Suspended` 是挂起的任务数；`observe` 在根 span 下开一个 `suspended` span（`suspended` → `resumed` 或终态，带 `was`），根 span 上记 `suspended`、`resumed` 事件；仪表 `neumann.tasks.suspended`；`Log` 两行 info。被挂起打断的模型调用 `outcome = suspended`，不算失败；挂起的时间不算进任何模型、工具 span（细节见 14 节）。

stdio：`suspend`、`resume`（`task_id`），错误码 `not_suspended`。

实现中的决定（`internal/core/suspend.go`，调度器一侧在 `internal/sched`）：

- **通知与交还**：挂起、恢复提交中断之后调 `Sched.Nudge`，和取消的 `Sched.Cancel` 同一个模式。任务在就绪队列或等待集合里、且处在会立刻落地的状态（就绪、`Suspended`、退避、熔断等待，`sched.Landable`）：交给挂起方 / 恢复方，它读中断表、落地、交还。否则在调度器里给任务打一个标记（`nudged`；挂起另打 `cutting`）：持有方交还时（`Sched.Hand`），任务已被标记、交还的状态又会立刻落地，调度器就不收，持有方读中断表、落地、再交还，直到收下。判断在调度器那把锁下做，所以"持有方读完中断表、交还之前"提交的挂起或恢复不会漏（挂起与落地、恢复与落地交错时都靠它）。持有方在读中断表之前清标记（`Sched.Heard`）：vCore 在每条指令开头，工具批次、`Approve`、`Resolve` 在提交前看到标记时再读一次中断表。取消在先：已取消的任务不收标记，交还按取消处理。
- **切断在途的调用**：每个模型步的调用（探测除外）和每条模型指令的等额度都登记在一张按任务的表里（`cuttable`），不管组开不开抢占；挂起方在 `Nudge` 之后切断登记了的那个。登记之后检查 `cutting` 标记：挂起若在登记之前就到了，登记方自己切断。两边都在锁下，顺序是"先标记后切断"对"先登记后查标记"，不会两边都错过。
- **挂起与抢占打的是同一个调用**：先到的取消原因算数。先被抢的，照常提交 `preempted`，挂起在同一次提交里落地（`was = "model"`，`kept` 为 0，留下的正文已记在 `preempted` 上），或在交还时落地（`was = "ready"`）；先被挂起切断的，之后再被选作牺牲者也不算被抢，那个 vCore 照样先去取为之让出的任务。被让出给的任务在 vCore 取到它之前被挂起：`TakeReady` 取不到，vCore 照常按队列取。
- **挂起后紧跟恢复**：调用若已被切断，任务照常 `Ready`（保留的正文照 6.4 续写），带 `resumed`（`pending: true`），不算被抢；这是一次白切的调用，只在两条指令几乎同时到达时发生。
- **答案检查**：检查期间到达的挂起在检查之后落地：驳回的那次提交改为 `Suspended`（`was = "check"`）；通过则任务结束，挂起没有效果。
- **`execTools`**（恢复时留下的 `planned` 调用）：指令开头已有挂起时调用不派发，任务 `Suspended`，`PC = StepTools`，恢复后由 `execTools` 派发。其余路径上，挂起的任务一轮工具总是已经有结果（`PC` 为 `StepModel` / `StepCompact`）。
- **判断"挂起了没有"**（`ErrNotSuspended`、重复挂起不提交）：一次读出任务的全部中断（含已送达的），最后一条挂起 / 恢复说了算；一条都没有时才看状态。每次落地都按"最后一条"把当时待送达的全部用掉，所以最后一条不论落地与否都决定任务最终是否挂起；落地只改送达标记，不改这张表，一次读就不会被另一方的落地骗到。评审发现原来分两次读（先读状态、再读待送达的中断）：落地恰好在两次读之间时，读到的是旧状态和已被用掉的中断——挂起、恢复、再挂起，第二次挂起以为已经挂起、什么都不提交，任务跑完；挂起后立刻恢复，恢复读到 `Running`、又读不到待送达的挂起，返回 `ErrNotSuspended`，任务停在挂起（`TestSuspendAfterAResumeStillLanding`、`TestResumeOfASuspendStillLanding`）。这次读之后才提交的挂起或恢复排在调用方之后。
- **读中断表失败**：落地推迟，任务保留标记，下一次交还或下一条指令开头再落地，不因此失败任务。
- **重启**：标记不持久。恢复流程给仍有待落地挂起的任务（等工具、待批准、待核对）重新打标记，等完那次提交照样落地。
- stdio 的 `UnsupportedKinds` 保留为空表：设计里的操作员指令都已实现。

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
    Task string // 发起调用的任务 id：Key 的任务部分，由总线填写
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
- 第一版设备：`builtin`、`mcp`（`github.com/modelcontextprotocol/go-sdk`）、`skills`、`tasks`（10.1）。

### 10.1 插话的发送方与 tasks 设备

```go
type SteerOptions struct {
    From string // 发送方："operator"、"user"、"task:<id>"……；空 = 未指明
    Key  string // 幂等键：同一任务已有同键的插话就不再提交
}
err = m.SteerWith(ctx, id, text, neumann.SteerOptions{From: "operator"}) // Steer(ctx, id, text) 即 SteerOptions{}
had, err := m.HasSteer(ctx, id, key)                                     // 这个键的插话提交过没有
infos := m.Tasks()                                                       // 未结束任务的轻量快照：id、类、状态、等待原因、标题

dev := tasks.New("tasks", tasks.WithVisible(tasks.All)) // 模型看到 tasks_send、tasks_list
m, err := neumann.New(neumann.Config{Devices: []bus.Device{dev /* , ... */}})
dev.Bind(m) // New 之后、Start 之前
```

**发送方。**

- `From` 随中断一起提交（`mem.Interrupt.From`，SQLite 中断表的 `sender` 列；`Open` 给旧库补列，旧中断读出为空）。只能是 1–256 字节（`MaxSteerFrom`）的 `[A-Za-z0-9:._@/-]`（`ValidFrom`）：没有括号、空格、换行，也没有显示成换行或改变文字方向的字符，框架里写不出别的东西。否则 `SteerWith` 报错、什么都不提交。`Submit` 带 `From` 也报错（stdio 本来就只给 `steer` 这个字段）。
- 送达时模型看到的不是 `SteerFormat`，而是 `Config.MessageFormat(from, text)`；默认 `DefaultMessageFormat`：先一行说明这条消息来自谁、**不是用户**、在你工作期间到达，要当作需要权衡的信息，而不是用户的指令，不改变用户要的东西；然后是正文，每一行前面加 `> `（`QuoteMessage`；`\r\n`、`\r`、`\v`、`\f`、U+0085、U+2028、U+2029 都先当作换行，非法 UTF-8 替换掉），所以正文里任何一行都冒充不了框架或别的发送方的消息。理由：别的任务的消息带着那个任务读到的东西，读不可信内容的任务不能以"用户"的身份对另一个有写工具的任务说话。默认格式把"这是你工作期间用户发来的"那句换成了"这不是用户"；自定义 `MessageFormat` 要做到同样两点。
- 事件：带 `From` 的插话被送达时，送达它的那次模型指令的提交里有一个 `steer_delivered`（`{seq, from}`），排在这次提交的其他事件之前（插话在指令开头放进请求）。它由 vCore 写出，不是 Store：`execModel` 知道读到的待送达中断和这次提交的 `Applied`，无论哪条路径提交了 `Applied`（工具调用、最终答案、答案检查的通过或驳回、调用期间或检查期间到达的插话把答案变成一轮），都不漏、不重；调用失败被收回的插话没有提交 `Applied`，也就没有事件。`steer_dropped` 仍由 Store 在终态提交里写，带 `From` 的多一个 `from`。Store 只需原样保存 `From` 和 `Key`。
- 不带 `From` 的插话：仍走 `SteerFormat`，没有 `steer_delivered`，`steer_dropped` 的内容逐字节不变。不用发送方的机器，寄存器和事件流与以前完全相同。
- 不变量不变：每条插话要么送达、要么报 `steer_dropped`（4.3）；带 `From` 的，两种结局都有事件。
- 与答案检查（9.8）：检查期间到达的带 `From` 的插话和别的插话一样让答案变成一轮，结论作废；它留在中断表，下一条模型指令开头按 `MessageFormat` 送达，那次提交里有 `steer_delivered`。驳回留下的反馈是另一条用户消息：标 `Feedback`、不经任何格式；插话的消息经 `MessageFormat`、不标 `Feedback`，两者不会混。
- `Key`：任务已有同 `Key` 的插话（待送达、已送达或已丢弃）时不再提交，`SteerWith` 照样返回 nil，任务结束后也是；新键对已结束的任务照常 `ErrFinished`。查重读这个任务的中断表，与提交在**这个任务的**锁里（不同任务的带键插话互不等待；等锁随 ctx 结束）。键随中断持久化（`idem_key` 列），所以跨重启有效——前提是 Store 保存了 `Key`：两个自带 Store 都过一致性套件的 `SteerSenderAndKeyRoundTrip`；自己写的 Store 丢了 `Key`，带键插话（以及 `tasks_send`）重启后会送两次。没有在 `Start` 时做往返检查：要往一个存在的任务写中断才能查，代价和副作用都不值得，一致性套件已经覆盖。没给 `(task_id, idem_key)` 建索引：查重走 `Store.Interrupts(task)`，用的是已有的 `(task_id, applied)` 索引，按键查的接口不存在，多一个索引只增加写入。
- 停机：带键的插话在 `Stop` 等工具批次排空期间仍然放行（不带键的照旧在 `Stop` 开始时就拒绝），批次排空后才拒绝并等放行的提交完。理由：设备的调用在批次里跑，`Stop` 本来就等它；它发的插话若被拒，调用会以"什么都没发"的结果提交为 `done`，消息就丢了。Go 错误不是出路：总线把 Go 错误也变成错误结果、照样提交。

**tasks 设备**（`bus/tasks`）。

- `tasks_send {task_id, text}`：先按这次调用的幂等键查（`HasSteer`），提交过就直接回 `sent to task <id>`，不看此后变化了的任何东西（对方已结束、可见性、长度上限、配额）；否则校验，再以 `From = "task:<调用者>"`、`Key = 调用的幂等键` 调 `SteerWith`。调用者取自幂等键 `task/call/attempt`（`DeviceCall.Task`），由机器填写，模型改不了；id 里有 `ValidFrom` 不允许的字符的任务不能发。未知任务、看不见的任务、已结束的任务、发给自己、空文本、超长（`WithMaxText`，默认 8 KiB）、超出配额都是给模型的错误结果，不是设备故障。
- `tasks_send` 声明 `Idempotent`：派发后、结果提交前崩溃，恢复用同一个键重发（4.2），设备按键查到已提交的插话，回 `sent`，不再发。`Idempotent` 的调用重启后不会变成 unknown、不会等操作员核对，也就不经过 `Resolve`。
- 配额：每个任务一生最多发 `WithMaxSends` 条（默认 32，负数不限），超出回错误结果。两个有礼貌、有问必答的任务否则会一直聊到预算耗尽。计数取自调用者自己的调用记录：这次调用之前（按调用序号）的 `tasks_send` 中结果不是错误的、已派发的、未知的，所以重启不清零，重发的调用算得和第一次一样。
- `tasks_list {}`：调用者看得见的其他未结束任务，JSON 数组 `{id, class, state, wait, title}`；`wait` 只给等待中的任务，`title` 是任务请求开头至多 80 个字符、压成一行。读 `m.Tasks()`：机器在内存里维护的快照，每次带任务的提交和恢复时更新，不读主存、不解码寄存器；没为它扩 `Store` 接口。`ReadOnly`，不缓存。
- 可见性：默认只看见同一类（执行组）的未结束任务（`tasks.SameClass`），`tasks_list` 会露出它们请求的开头。`WithVisible(func(caller, other neumann.TaskInfo) bool)` 放宽（`tasks.All`）或收紧（同租户前缀……）；看不见的任务一律报"不存在"，不泄露它存在与否。谁能用这两个工具由页表决定（`Deny: ["tasks_"]`），`ToolPolicy` 可以像对任何工具一样拒绝或扣住 `tasks_send`（9.7）。
- 接线：设备要用机器，机器又由设备构造，所以 `New` 之后、`Start` 之前 `Bind`：恢复时重发的调用在 `Start` 里就会跑，没绑定的设备给错误结果。设备只通过一个小接口用机器（`SteerWith`、`HasSteer`、`Tasks`、`Load`、`Calls`），`*neumann.Machine` 满足它；`bus/tasks` 引用 `neumann`，`neumann` 不引用它，没有环。
- 限制：消息只能送给还没结束的任务。等回复的任务在回复到达时必须还在工作（比如还有工具在跑）；最终答案正在生成时到达的消息让它多走一步（4.3），但已经结束的任务收不到。没有"等消息"的原语，`examples/messaging` 用一个占位工具代替。

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

`Caps` 另有四项，给本地后端用（6.4）：`ThinkPrefill`（以只有 reasoning、没有正文的 assistant 消息结尾的请求会接着想下去，回答只带之后的思考和正文）、`Parallel`（后端同时处理的请求数，0 = 未知，约束组的窗口上限）、`Prefill`（以 assistant 消息结尾的请求会被续写，只返回接下来的部分）、`ThinkOff`（`Request.NoThink` 能关掉思考）。`Request.NoThink` 为真时适配器关掉思考：OpenAI 兼容适配器发配置的 `ThinkOff` 值（Ollama 上是 `reasoning_effort: "none"`，实测只生成 3 个 token 就作答），没配置时 `Caps.ThinkOff` 为假、这个字段被忽略；Anthropic 适配器不发 `thinking`（配了 `ThinkingBudget` 也不发），所以它的 `Caps.ThinkOff` 恒为真。Anthropic 适配器的 `Caps.Prefill` 为假：Anthropic 拒绝以空白结尾的 assistant 前缀、开着思考时不许前缀，有的模型根本不收前缀，被抢的调用在这个组上从头生成。

OpenAI 兼容适配器的本地选项：`Parallel`、`Prefill`、`ThinkOff` 原样进 `Caps`；`ThinkTags`（开、闭标签，`-local` 设为 `<think>`、`</think>`）设了就报 `ThinkPrefill`：末尾只有 reasoning 的 assistant 消息以 `content = 开标签 + "\n" + 思考` 发出，这时回答（Ollama 全在 `content` 里）在闭标签处切开，之前是 reasoning（流式时是 `Delta.Reasoning`，跨块的标签也认得出，可能是标签开头的尾巴先压着等下一块），之后是正文（去掉开头的换行）；回答里没有闭标签就全是思考。自己解析思考标签的后端（开了推理解析的 llama.cpp）接着想时把剩下的思考放在 `reasoning` / `reasoning_content` 字段、正文不带标签：这次调用一收到这两个字段，之前按标签切出的照旧，之后的 `content` 一律是正文，字段里的思考计入回答的 reasoning（流式和非流式都一样）。接着想的调用回答里的思考总是进回答。其他调用：`content` 去掉开头的空白后以开标签开头（不解析思考的后端把思考写在正文里），同样在闭标签处切开，之前是思考、之后是正文（流式时开头先压着，直到看得出是不是开标签）；不以开标签开头的照旧整段是正文（`x</think>y` 不切）。这类调用的思考（`reasoning` / `reasoning_content` 字段，或标签切出的）照常作为 `Delta.Reasoning` 流出，但**只在回答没有正文、没有工具调用时才放进回答**（一个 reasoning 段）：有答案时思考不进寄存器，和原来一样；什么都没有时带上思考，机器才分得清"想完了没作答"和空回答（6.4）。回答没有正文、没有工具调用、没有思考、也没报推理 token，并且不是 `length`，才是"既无正文也无工具调用"的临时错误；只有思考的（包括只报了推理 token、不给思考文字的 OpenAI 推理模型）原样返回给机器判断。截断（`length`）的回答也带上思考，非流式调用截断在思考里，机器也能看出来先关思考（13）。超时分三种，`Timeout`（整次调用，默认 5 分钟，0 = 不限）、`FirstByteTimeout`（发出到第一个字节，默认 0 = 只受 `Timeout` 约束）、`IdleTimeout`（流式时相邻两块之间，默认 0 = 不限）。本地后端处理长提示词时首字节可能很慢，生成时却一直有数据，所以本地配置是 `Timeout = 0`、首字节 10 分钟、空闲 2 分钟，失控的生成靠输出上限截住（13）。三种超时都按单次请求超时处理：调用方还有时间，就是 `Transient`；流中途超时包 `ErrResponseLost`，已收到的正文进 `Partial`（6.4）。

实现：三种超时由适配器自己用请求的 context 实现（以 `*openai.TimeoutError` 为原因取消，`Which` 为 `call` / `first byte` / `idle`），不再用 `http.Client.Timeout`，所以共用的传输层（HTTP/2 连接、HTTP/3 回退）不受影响；`HTTP.Timeout` 若另外设了，叠加生效。错误链里是 `*TimeoutError`，没有 `context.Canceled`，所以不会被误判为取消；调用方的 ctx 已结束时仍按取消处理。"第一个字节"指响应体的第一个字节：本地后端排队时可能先回响应头、正文迟迟不来，只看响应头量不到排队。非流式调用的响应体要等整个回答生成完才来，所以对 `Call` 来说 `FirstByteTimeout` 约束的是整次生成。`IdleTimeout` 从第一个字节之后开始，每次读到数据重新计时。响应头之前超时不包 `ErrResponseLost`（和原来的 `http.Client.Timeout` 一样），收到响应头之后超时包。流式增量的思考除了 `reasoning_content`（DeepSeek）也认 Ollama 的 `reasoning`，作为 `Delta.Reasoning` 报出；回答本身不带思考。

错误分类：

| 类别 | 例子 | 执行组 | 限流器 | 任务 |
|---|---|---|---|---|
| `RateLimited` | 429 且带限流响应头或 `retry-after` | 不变 | 按 `RateInfo` 暂停到重置时间 | `Waiting(Backoff)` 到重试时刻（7.3），不计重试次数；没有任何提示的按临时错误退避、计次数 |
| `QuotaExhausted` | 额度或账单耗尽 | 熔断，不自动半开 | 退还预占 | `Waiting(Group)` 或迁移 |
| `Overload` | 529、503 overloaded；无法区分的 429 | 窗口减半（每周期一次） | 保留预占 | 退避后重试，计重试次数 |
| `Transient` | 其余 5xx、网络错误、超时 | 计入熔断 | 见 7.2 | 退避后重试，计重试次数；响应丢失时 `Unconfirmed + 1` |
| `Permanent` | 其余 4xx | — | 退还预占 | `Failed`，`StopReason = "model_error"`；续写的请求被拒时先丢掉前缀从头重试一次（6.4） |
| `ContextLength` | 请求超出上下文窗口（OpenAI `context_length_exceeded`、Anthropic "prompt is too long"） | 算一次回答（同 `Permanent`） | 退还预占 | 学到更小的窗口，压缩后重发（5.3）；压不下 `Failed`，`StopReason = "context_overflow"` |

退避：第 n 次连续失败等 `min(60s, 1s·2^(n-1))` 的全抖动（在 0 到该值之间均匀取），不短于服务商的 `RetryAfter`；第 8 次（`Config.MaxModelRetries`）连续失败时任务 `Failed`，`StopReason = "model_error"`，错误文字在 `failed` 事件里。`Retries` 记在 TCB 上随提交持久化，成功（包括截断）清零；关不掉思考时的"想完了没作答"不清零，算一次失败（6.4）。每个重试决定都是一次提交：`Waiting(Backoff)` + `WakeAt` + `model_retry` 事件（`{class, attempt, delay_ms, wait, error}`，`attempt` 是计入的重试次数，限流时不变；失败的是续写时多带 `dropped` 或 `dropped_think`，6.4），提交之后调度器才挂定时器；`QuotaExhausted` 同样发 `model_retry`（`wait = "group"`）。失败的调用仍是一条指令（发 `step` 事件），但不计入 `Used.Steps`（4.6），`MaxSteps` 不约束它们，约束它们的是重试上限；带提示的限流（`RetryAfter`、`RateInfo.RetryAt` 或已耗尽维度的重置时间）不计重试次数，由服务商的重试时刻和限流器暂停节制；什么提示都没有的 429 和被误判的额度错误分不开，按临时错误退避并计入重试次数，也不暂停限流器，所以服务商一直这么回时任务以 `model_error` 结束，不会无限循环。退避步长到上限前不会溢出（`BackoffMax` 接近最大时长时也一样）。`Unknown`（谁也解释不了的错误）按 `Transient` 处理：网络抖动比请求本身有错更常见，重试上限兜住判断错的代价。适配器报了 `Canceled` 而任务并没有被取消（适配器自己超时、报成裸的 `DeadlineExceeded`）也按 `Transient`。

`Unconfirmed` 只在 `errors.Is(err, ErrResponseLost)` 时加一（以及原有的：调用被取消打断、重启时发现 `Running`）；连接被拒绝不加（18.12）。

`context.Canceled` 永远不是可重试错误。永久错误的判断优先于报错文字里的"timeout"之类字样。

实现（`model`）：`ErrClass` 取值 `Unknown`（零值）/ `RateLimited` / `QuotaExhausted` / `Overload` / `Transient` / `Permanent` / `Canceled` / `ContextLength`，`Retryable()` 只对 `RateLimited`、`Overload`、`Transient` 为真。适配器能解释的失败一律返回 `*model.Error{Class, Status, RetryAfter, Provider, Msg, Err, Rate}`，`Msg` 是状态码之外的响应片段，服务端回显的 key 会被抹掉。`model.Classify(err)` 按顺序判断：链上有 `context.Canceled` → `Canceled`（适配器说什么都不改变这一条）；链上有 `*model.Error` 且类别不是 `Unknown` → 用它的类别（所以永久错误不会被文字里的"timeout"翻案——这里根本不读文字）；`context.DeadlineExceeded` → `Canceled`；`net.Error`、`io.ErrUnexpectedEOF`、连接被重置 / 拒绝、`EPIPE`、`model.ErrResponseLost` → `Transient`；其余 `Unknown`。`model.ClassifyWith(adapter, err)` 先排除取消，再问适配器的 `Classifier`，答 `Unknown` 时回落到 `Classify`。

**调用方的 Deadline 和单次请求超时是两回事。** 到达 `Classify` 的裸 `DeadlineExceeded` 是调用方的 ctx 到期，也就是任务的 Deadline（9.4），重试只会越过它，所以算 `Canceled`。单次请求超时（`http.Client.Timeout`、流读到一半卡住）发生时调用方还有时间，是服务商慢，算 `Transient`。两者在错误链上都表现为 `DeadlineExceeded`，只有适配器分得清：出错时看自己调用方的 `ctx.Err()`，非空就是 `Canceled`（`Err` 设为 `ctx.Err()`），为空就包成 `*model.Error{Class: Transient}`。

**空回答**：响应完整回来了，却既没有正文也没有工具调用、也不是被截断，两个适配器都报 `Transient`，错误包裹 `model.ErrEmptyResponse`。服务商回答了，所以它和请求有关；续写的请求得到空回答时机器丢掉前缀（6.4）。

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

Anthropic 原生适配器（`model/anthropic`，标准库实现，传输层复用 `openai.NewTransport`）：`POST {BaseURL}/messages`，头 `x-api-key` 与 `anthropic-version: 2023-06-01`；`max_tokens` 必填，默认 8192；`Provider()` 为 `anthropic:<模型>`。系统提示词放顶层 `system`；工具调用渲染为 `tool_use` 块，工具结果渲染为 user 消息里的 `tool_result` 块。Anthropic 要求角色交替、`tool_result` 紧跟它的 `tool_use` 回合，所以同角色的相邻消息合并成一条，合并后的 user 回合里 `tool_result` 块排在最前（插话、中途的 system 消息作为文本块排在后面），渲染为空的消息直接省略。id：同一 Provider 且有 `ProviderCallID` 时用服务商的 id，否则用机器 id，`[a-zA-Z0-9_-]` 以外的字符替换成 `_`；结果引用同一个 id。`input` 必须是 JSON 对象：空参数发 `{}`，不是对象（包括非法 JSON）的参数原文放在 `{"_raw": "..."}` 里，不丢信息。响应里除纯文本外的每个块（`tool_use`、`thinking`、`redacted_thinking`、未知类型）原样存进 `Opaque`；回放给同一 Provider 时逐字节发回（thinking 的签名绑定在产生它的模型上），发给其他 Provider 时 thinking 丢弃、tool_use 按中立字段重新渲染。流式下按 `index` 重组每个块：`text_delta`、`thinking_delta`、`signature_delta` 逐片拼接，`input_json_delta` 拼成完整的 `input`，键顺序沿用 `content_block_start`，组装结果与非流式响应的块一致；流在 `message_stop` 之前断开是错误。用量：`PromptTokens = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`（`input_tokens` 只计最后一个缓存断点之后的部分），`CachedTokens = cache_read_input_tokens`。回答没有文本、没有 `tool_use`，有 `thinking` / `redacted_thinking` 块、以 `end_turn` 结束（想完了没作答），作为回答返回（只有这些块），交给机器判断（6.4）；它的 `Caps.ThinkOff` 恒为真，机器不带 `thinking` 重做。什么块都没有、或只有别的未知块，仍是"既无正文也无工具调用"的临时错误。`stop_reason` 映射到与 OpenAI 兼容适配器相同的词：`end_turn`/`stop_sequence` → `stop`，`tool_use` → `tool_calls`，`max_tokens` → `length`。缓存标记 `cache_control: {"type":"ephemeral"}` 打在最后一个工具定义上，以及倒数第二条消息的最后一个可标记块上（thinking 块不能带标记，向前找）；标记插在块的右花括号之前、写进新的切片，块的其他字节和它来自的 `Opaque` 都不变。错误分类和 `RateInfo` 与 OpenAI 兼容适配器共用 `model/internal/httperr`（状态码、retry 提示、额度规则、限流头），在其上读正文的 `error.type`：共用的额度规则先定（CLIProxyAPI 的 `rate_limit_error` + "Resets in 120h41m12s" 仍是 `QuotaExhausted`），529 一律 `Overload`；然后 `rate_limit_error` → `RateLimited`（没有 retry 提示时取已耗尽维度的重置时间），`overloaded_error` → `Overload`，`api_error` / `timeout_error` → `Transient`，`billing_error` 和 400 的 "credit balance is too low" → `QuotaExhausted`，`invalid_request_error` / `authentication_error` / `permission_error` / `not_found_error` / `request_too_large` → `Permanent`（共用规则先判出的 `ContextLength` 优先：Anthropic 以 400 `invalid_request_error` 发 "prompt is too long: … tokens > … maximum" 和 "input length and `max_tokens` exceed context limit"）；没有或不认识的 `error.type` 沿用共用结论。流内 `error` 事件按同样规则分类（状态码记 0）；流在 `message_stop` 之前结束、body 被截断、2xx 解析不了 → `Transient` 包 `ErrResponseLost`；取消与单次请求超时的区分同 OpenAI 兼容适配器。不带 Anthropic 专有信息的同一组状态码、头和正文，两个适配器的结论一致（有交叉测试）。

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
    From     string          `json:"from,omitempty"`    // steer：发送方（10.1）
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

任务事件：`accepted`、`model_call`（随 `Running` 提交、请求即将发出，`{pc, core}`；到带结果的 `step` 为服务商时延）、`step`、`tool_call`、`tool_result`、`tool_late`、`usage`、`answer`、`model_retry`、`compacted`、`migrated`、`migrate_refused`、`recovered`（重启时恢复了这个任务，`{from, ...}`）、`reconcile_needed`、`resolved`（操作员处理了一个待核对的调用）、`tool_denied`、`tool_rewritten`、`approval_needed`、`approved`（9.7）、`answer_rejected`（9.8）、`done`、`failed`、`cancelled`、`steer_delivered`（带发送方的插话送达，10.1）、`steer_dropped`（带发送方的多一个 `from`）、`preempted`（`{by, kept, kept_think, ran_ms, n}`，6.4）、`priority_set`（`{priority, was}`，6.4）、`suspended`（`{was, kept, kept_think}`，9.9）、`resumed`（`{pending}`，9.9）。`step` 在流式调用收到第一块数据时带 `first_ms`（发出到第一块的毫秒数），用来区分后端排队和生成；续写的调用带 `prefill`（续写前缀的字符数），接着想的调用带 `prefill_think`（思考前缀的字符数）；这样的调用失败、前缀被丢掉时，`model_retry` 带 `dropped` / `dropped_think`（丢掉的字符数，6.4）。配置了答案检查时，检查看过的答案的 `answer`、`done` 带 `checked`（9.8）。

`compacted`：`{rounds, messages_before, messages_after, tokens_before_est, tokens_after_est, chunk, chunks}`（5.3）。`step` 的 `pc` 为 `model`、`tools` 或 `compact`。`model_retry` 的 `class` 可以是 `context_length`（`wait = "compact"`）。

`steer_delivered`：`{seq, from}`，带发送方的插话送达（10.1）；`steer_dropped`：`{seq, text}`，带发送方的多一个 `from`。

`migrated`：`{from, to, reason}`，`reason` 为 `group_down` 或 `manual`。`migrate_refused`：`{from, to, check, detail}`，本组熔断、`Fallback` 校验不过、任务留下等本组。`tool_result` 来自 L2 时带 `cached: true`。

机器事件（`task_id` 为空）：`core_up` / `core_down`（`{class, core, reason, live}`）、`throttled`（`{task, class, waited_ms}`）、`group_down`（`{class, cause}`）、`group_up`（`{class}`，操作员复位时带 `reset`）。

stdio 前端：每条指令一行 JSON，处理完回一行 `{"ok":true,"task_id":...}` 或错误（返回即已持久化）；事件另起一个流，支持 `{"kind":"subscribe","from_seq":N}` 补读。`steer` 可带 `from`（发送方，10.1）；stdio 的对端就是操作员，它给的 `from` 不做核实。

操作员控制也走同一协议：`class`（`task_id`、`class`）、`cores`（`class`、`cores`）、`autoscale`（`class`、`on`）、`group_reset`（`class`）、`queue`；`submit` 可带 `pin_class`。`class` 被校验拒绝时错误码 `incompatible`，带 `check`；任务正在执行时 `task_busy`。`priority`（`task_id`、`priority`）对应 `m.SetPriority`，任务正在执行时 `task_busy`。`suspend`、`resume`（`task_id`）对应 `m.Suspend` / `m.ResumeTask`（9.9），不在挂起中的任务恢复时错误码 `not_suspended`。`approve`（`task_id`、`call_id`、`decision`：`{"verdict":"allow"}`、`{"verdict":"deny","reason":…}`、`{"verdict":"edit","args":{…}}`，`args` 必须是对象）对应 `m.Approve`，任务或调用不在等批准时错误码 `not_awaiting_approval`。工具策略只能在 Go 里配置（`Config.ToolPolicy`）。

实现在 `proto/jsonl`（线格式的完整说明在它的包文档里），`cmd/neumann` 把它接到 stdin/stdout。回复和事件共用 stdout，每行第一个字段 `"type"` 是 `"reply"` 或 `"event"`：宿主进程（Node、Python）天然只有 stdout，多开一个 fd 两边都别扭；共用一个流还给出了顺序，`subscribe` 的回复恰好落在旧流最后一个事件和新流第一个事件之间。指令可带 `"id"`，回复原样带回；指令按到达顺序逐条执行。读得慢的一方不阻塞机器：事件按输出写出的速度从主存读，回复优先于积压的事件写出。

## 13. 出错处理

| 情况 | 处理 |
|---|---|
| 模型请求失败 | 按第 11 节分类：限流、过载、临时错误退避重试，额度耗尽等执行组，永久错误 `Failed`（`model_error`） |
| 截断且无内容 | `MaxOut` 乘 4 重做，最多两次，之后 `Failed`（`truncated`）。截断的回答里只有思考（reasoning 段、推理 token，或这次调用流出过推理增量）、适配器 `Caps.ThinkOff` 为真时，第一次重做先关掉思考（`NoThink`），`MaxOut` 不变，不算两次放大之一；关掉思考仍被截断才按倍数放大 |
| 想完了没作答 | 正常结束、没有正文和工具调用、只有思考：不是临时错误，是一次有回答的调用（执行组记成功）。能关思考就关掉思考立即重做（`NoThink`，`model_retry` 的 `class = "thought"`）；关不掉或关了还这样，照原样立即重做、计 `Retries`，到上限 `Failed`（`model_error`）。关思考的重做回来是空回答，按临时错误退避重试，有上限（6.4） |
| 思考停不下来 | 本地小模型会在思考里打转（实测 qwen3.5 9B 写 150 字小故事，连续三次跑满 5 分钟超时）。本地配置给默认输出上限，打转到上限时变成"只有思考的截断"，按上一行关掉思考重做，不再原样重发同一个请求 |
| 上下文超过阈值 / 服务商拒绝为超出上下文 | 压缩（5.3）；怎么压都放不下 `Failed`（`context_overflow`） |
| 预占量永远放不下 | `Failed`（`too_large`） |
| 工具出错 / 超时 / 越权 | 错误文本作为该调用的结果交给模型 |
| 提交失败 | 记 ERROR；这次状态转换不存在，任务不再调度，发不出事件（因为事件也没提交）；`Status` 里标出该任务和错误 |
| 进程崩溃 | 按 4.5 恢复 |
| 超出 MaxSteps / MaxTokens | `Done`，`StopReason = "budget"`（`Used.Steps + Unconfirmed`：只算有回答的调用和未确认的请求，失败的调用不算） |
| Deadline | `Failed`，`StopReason = "deadline"`（9.4） |
| vCore panic | 恢复 panic，该任务 `Failed` 并带 panic 信息，这个 vCore 退役 |

## 14. 观测

`m.Status()`：每个执行组的 `W`、`epoch`、存活 vCore 数、冷却/熔断状态（当前实现：`Status.Groups` 每组给出模型、`Fallback`、`W` / `Target` / `Min` / `Max` / `Auto` / `epoch` / 冷却、存活与忙碌的 vCore 数、就绪数、等待数（其中等本组的）、最老就绪任务已等多久、熔断状态与下一次放探测的时刻；`Status.Cores` 是全部存活 vCore 数；`Status.Limiters` 给出各限流器的 `limit.Status`；`Status.Cache` 是总线 L2 的 `CacheStats`；`Status.Approval` 等批准的任务数；`Status.Suspended` 挂起的任务数（每组同名字段）；`m.Queue()` 按类给出就绪数、其中后台数、等待数（其中等批准的、挂起的）、最长等待）；每个限流器各维度的剩余额度和下次重置时间；每个 vCore 在做什么；每条队列长度和最长等待；待核对的调用列表；每个未结束任务的状态、组、步数、已确认用量和未确认请求数。加锁拷贝，不调模型、不读主存。

`observe` 包只用公开 API（`Events`、`EventLog`、`Status`、`Queue`、`Load`、`Calls`），任何宿主都能挂；不挂就没有任何开销。事件从主存按序号读，消费方慢只会落后，不会挡住机器；`Follow` 返回最后读到的 `Seq`，宿主存下来重启后接着读；`Head` 用 O(log n) 次单条读找到日志末尾，只想统计"从现在起"的就从那里开始。

- `Metrics`：OpenTelemetry 指标，名字都在 `neumann.` 下，带单位，完整的表在包文档里。事件来的计数和直方图：任务接受 / 结束（按类、`stop_reason`）、模型调用与失败（按类、错误类别）、模型指令耗时、工具调用（按工具、结果：ok / error / cached / denied / unknown / late / cancelled）与耗时（`tool_call` → 结果）、token（prompt / completion / reasoning / cached）、重试、限流等待、`core_up` / `core_down`、熔断、迁移、`reconcile_needed`；工具策略与批准（9.7）：`tool_denied`（按工具、`by`，同时是该调用的 `denied` 结果）、`tool_rewritten`、待批准的调用数（重启再宣布不重复计）、决定数与等待时长（首次 `approval_needed` → 决定，按 `verdict`：allow / edit / deny，任务先结束为 cancelled）；答案检查（9.8）：`neumann.answers.checked`（按类、`outcome`：passed / failed_open / rejected / failed）。采集时读 `Status` / `Queue` 的仪表：各组 vCore、`W`、`Target`、`epoch`、熔断状态，各类队列与最长等待，各限流器各维度用量与可用量，L2，未结束任务数、等批准的任务数（总数与各类），丢掉的增量，进程的 goroutine、堆（Linux 上还有 RSS）。基数有界：不按任务 id 打标签；工具名、类名和其他标签各有上限，超出的记为 `_other`（模型会编造工具名）。
- 模型指令耗时的起点：调用前的 `Running` 提交带 `model_call`（12 节），它的时刻就是请求发出的时刻；终点是结果的提交（减去答案检查的 `check_ms`），即服务商的时延。`model_call` 出现之前写下的日志没有这个时刻，起点退回任务能运行的时刻：上一次提交、退避结束、或放行这次调用的 `throttled`，包括就绪队列和限流器上的等待。`steer_delivered` 在它那一步的提交里、排在 `step` 前面，不改变这一步的起点。
- `Tracer`：从事件重建 OpenTelemetry trace，一个任务一个 trace。根 span `task`（accepted → 终态，`failed` 为错误状态），子 span：每条模型 / 压缩指令一个（失败带错误类别），每轮工具一个 `tools`，其下每个调用一个 `tool <名字>`；每个待批准的调用一个 `approval <工具>`（首次宣布 → 决定，带 `verdict`）；被拒的调用 span 在 `tool_denied` 结束、错误状态；重试、限流、迁移、恢复、核对、策略与批准事件（不带参数和原因）是根 span 上的事件；终态之后提交的（`steer_dropped`、`tool_late`）是根下零长度的 span，这样 trace 不取决于何时 flush。时间全用 `Event.Time`，id 由任务 id 和打开该 span 的事件的 `Seq` 经 SHA-256 得到（trace id = `SHA-256("neumann/trace/"+任务)` 前 16 字节），所以从 0 补读重建的 trace 与在线导出的逐字段相同；从中途接着读时，根 span 从第一个看到的事件开始并标 `neumann.partial`，id 不变。直接把 span 交给 `SpanExporter`（OTLP、内存），不经过 TracerProvider：后者自己生成 id 和时间。
- `Log`：slog，每个值得一提的事件一行，消息是事件种类，`seq`、`task`、`class` 等是可以 grep 的键值，`at` 是提交时刻。生命周期 info；重试、限流、熔断、迁移被拒、插话丢弃、晚到结果、`tool_denied`、`approval_needed`（操作员要处理）warn；`approved` 带 `verdict` 与等待时长；`failed`、`reconcile_needed` error；`step`、`usage`、工具调用和结果 debug。`LogResources` 定期写一行资源。
- 压缩（5.3）：`compacted` 是 `Metrics` 的 `neumann.compactions`（每块一次）、`neumann.compaction.rounds`（换出的轮次）、`neumann.compaction.tokens`（`tokens_before_est - tokens_after_est`），都按类；`compact` span 延到同一提交里的 `compacted` 事件结束，带它的数字（`neumann.compact.*`），任务根 span 上也记一个 `compacted` 事件；`Log` 以 info 写一行，带轮次、消息数、估算 token 和块号。没落地的压缩（被取消抢先）仍是一个 `compact` span，不计数。
- 抢占与续写（6.4）：`preempted` 结束它那条模型指令的 span，`outcome = preempted`、不是错误，带 `neumann.preempt.kept`、`.kept_think`、`.n`，并链接（span link）到抢它的任务的根 span；根 span 上记一个 `preempted` 事件（`by`、`kept`、`kept_think`、`ran_ms`、`n`）；`neumann.model.preemptions`（按类）；`Log` 以 info 写一行（`by`、`kept`、`kept_think`、`ran_ms`、`n`）。被抢的调用在 `neumann.model.calls` 和 `neumann.model.duration` 里记 `outcome = preempted`：不算失败（不进 `neumann.model.failures`），耗时是它占着后端、被切断之前跑的时间，和正常回答的时延分开统计；续写是另一次调用，从它自己的 `model_call` 计时。续写的模型 span 带 `neumann.prefill`，续写思考的带 `neumann.prefill_think`。续写失败丢掉前缀时，`model_retry` 的 warn 日志带 `dropped` / `dropped_think`。`priority_set` 是根 span 上的事件（`neumann.priority`、`neumann.priority_was`）和一行 info 日志；根 span 的 `neumann.priority` 是任务最后的优先级（从中途接着读、没看到 `accepted` 也没看到 `priority_set` 时不带）。`step` 的 `first_ms` 是模型 span 上的 `first_byte` 事件（span 起点即请求发出之后 `first_ms` 毫秒，不超出 span）和直方图 `neumann.model.first_byte`（秒，按类、`pc`，各种结果都记，包括被抢的）。`steer_delivered` 是根 span 上的事件（`neumann.steer.seq`、`neumann.from`）和 `neumann.steers.delivered`（按类）。这些都只来自事件的数据和提交时刻，从 0 补读与在线导出逐字段相同。
- 挂起与恢复（9.9）：每次挂起是根下一个 `suspended` span，从 `suspended` 事件到 `resumed`（`neumann.outcome = resumed`）或挂起中的终态（`cancelled`、`failed`，`outcome` 取任务的），带 `neumann.was`，保留了正文或思考时带 `neumann.suspend.kept`、`.kept_think`（为 0 不带）；id 由任务 id 和 `suspended` 的 `Seq` 得到。重启再落地一次（没有 `resumed` 又来一个 `suspended`）还是同一次挂起，从第一次计。根 span 上记 `suspended`（`neumann.was`、保留数）和 `resumed`（抵消未落地的挂起时带 `neumann.pending`）事件；`pending` 的 `resumed` 不开 span，也不结束、不改它所在提交的那条指令。打断在途调用的 `suspended` 结束那条模型 span：`outcome = suspended`，不是错误、不进 `neumann.model.failures`，带保留数；`neumann.model.calls` / `neumann.model.duration` 记 `outcome = suspended`，耗时是被切断前跑的时间。恢复后的下一次调用从它自己的 `model_call` 计时（没有 `model_call` 的旧日志从 `resumed` 的提交计时，不从挂起或被挂起丢掉的退避结束计时），挂起的时间不进任何模型或工具 span。仪表 `neumann.tasks.suspended` 读 `Status.Suspended`；没有 `Source` 时由 `observe` 从事件数（`suspended` 加一，非 pending 的 `resumed` 或终态减一）；从中途接着读时，开始之前就挂起的任务不计。`Log` 两行 info：`suspended`（`was`，保留了的话 `kept`、`kept_think`），`resumed`（`pending`，或 `was` 和挂起了多久 `suspended`）。
- `NewHandler`：只读的 HTTP 视图，`GET /status`、`/queue`、`/tasks/{id}`（TCB + 调用记录）、`/events?from_seq=N`（Server-Sent Events，先补读再在线，`Last-Event-ID` 续读，`task=` 过滤），可选 `/metrics`（OTel 的 Prometheus 导出器）和 `/debug/pprof/`（默认关；自己实现，不引入 `net/http/pprof`，它会注册到 `DefaultServeMux`）。没有任何会改状态的端点，其他方法 405。`/tasks`、`/events` 带任务内容（提示词、工具参数和结果），只能给操作员看；可以要求 bearer token。指标、trace、活动日志都不带任务内容。

`cmd/neumann`：`-observe-addr`（空 = 关）在那里挂 `NewHandler`，`-metrics` 加 `/metrics`，`-pprof` 加 pprof，`NEUMANN_OBSERVE_TOKEN` 设了就要求 token；设了标准的 `OTEL_EXPORTER_OTLP_ENDPOINT` 时 trace 和指标经 OTLP/HTTP 导出，`service.name` 为 `neumann`。`-local` 一次配好本地后端：`Parallel = 1`（`-parallel` 可改，`Cores` 取它）、`Prefill`、`Preempt`、`ThinkOff = "none"`、超时按 11 节的本地配置、默认输出上限 8192（`-max-tokens` 可改）。显式给了 `-cores`、`-max-tokens`（命令行或环境变量）就用给的；`-base-url` 默认 Ollama 的 `http://127.0.0.1:11434/v1`；只能配 `-provider openai`。环境变量 `NEUMANN_LOCAL=1`、`NEUMANN_PARALLEL`。不带 `-local` 的 `-parallel` 只设适配器的 `Parallel`。观测从启动时的日志末尾开始；退出时先等机器停、观测读到日志末尾，再 flush 导出器，每步有时限。

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
    AnswerCheck: check,                    // 可选：最终答案提交前的检查（9.8）
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
err = m.SetPriority(ctx, id, 0)            // 重排（6.4）：ErrTaskBusy / ErrFinished / ErrNotFound；之后可能抢占
err = m.GroupReset(ctx, "big")             // 额度耗尽熔断后复位
err = m.Approve(ctx, id, "c-3", neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}) // 回答待批准的调用：ErrNotAwaitingApproval / ErrNotFound
err = m.SteerWith(ctx, id, "…", neumann.SteerOptions{From: "operator"}) // 带发送方、幂等键的插话（10.1）
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
    builtin/  mcp/  skills/  tasks/
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

6. **答案检查**（9.8）：`Config.AnswerCheck` / `AnswerCheckTimeout` / `MaxAnswerRetries`；在 `execModel` 的最终答案路径上、最终提交之前问，超时、panic、取消按通过；驳回一次提交（答案、`Feedback` 消息、`Rejections`、`answer_rejected`、`Ready`），用完重答次数照样 `Done` 并记 `check_failed`；取消抢先、插话抢先时结论作废；观测接上。
   验证：`TestAnswerCheckRejectsThenPasses`、`TestNoAnswerCheckRecordsNothing`、`TestAnswerCheckRetriesRunOut`、`TestAnswerCheckLoopIsBoundedByTheBudget`、`TestAnswerCheckTimeoutPasses`（理不理 ctx 都一样，单 vCore 上三个超时的检查各占它一个超时）、`TestAnswerCheckPanicPasses`、`TestSteerDuringTheCallSkipsTheCheck`、`TestSteerDuringTheCheckWins`、`TestCancelBeatsTheVerdict`（驳回、最后一次驳回、通过三种提交被取消抢先）、`TestCancelDuringTheCheck`、`TestRejectionsSurviveARestart`（SQLite）、`TestCrashAtEveryCommitPointWithAnAnswerCheck`（付款后先驳回再通过、两次都驳回，每个提交点前后各崩一次）、`TestOldTaskBlobsDecode`（加字段之前写下的 TCB gob 照常读出）、两个适配器的反馈渲染测试、`observe` 的指标 / trace / 日志测试。

7. **插话的发送方与 tasks 设备**（10.1）：`m.SteerWith` 与 `SteerOptions{From, Key}`、`ValidFrom`、`Config.MessageFormat` / `DefaultMessageFormat` / `QuoteMessage`、`HasSteer`、`Tasks`；`From`、`Key` 随中断提交（两个 Store、一致性套件、SQLite 旧库补列），`steer_delivered` 由 vCore 写，`steer_dropped` 带 `from`，不带 `From` 的逐字节不变；`Key` 去重（按任务加锁）、停机排空期间放行带键插话；`Submit` 拒绝 `from`；stdio `steer` 的 `from`；`DeviceCall.Task`；`bus/tasks` 的 `tasks_send`（`Idempotent`，先按键查、再校验，配额）、`tasks_list`、`WithVisible`（默认同类）、`Bind`；`examples/messaging`。
   验证：`SteerSenderAndKeyRoundTrip`、`SteerWithASenderIsReportedDropped`（一致性套件）、`TestOpenAddsTheSenderColumns`（旧库的中断照常读出、丢弃）、`TestSteerWithASenderIsFramedAndReported`（默认与自定义格式、`steer_delivered` 紧挨在它的 `step` 之前）、`TestAMessageCannotForgeItsFrame`、`TestSendersAreRestricted`、`TestSteerWithASenderIsReportedDropped`、`TestSteerWithoutASenderIsDeliveredSilently`（事件序列与以前相同）、`TestSteerWithChecksItsSenderAndKeepsItsKey`、`TestSteerWithASenderDuringTheCheck`、`TestFeedbackAndASteerWithASender`、`TestTwoTasksExchangeMessages`、`TestSendAcrossACrashIsSentOnce`（插话落盘后立刻崩溃，恢复用同一个键重发，设备两个进程各跑一次、对方只收到一条，重启后送达）、`TestRepeatAfterTheReceiverFinished`（重发时对方已结束、可见性隐藏已结束的任务，仍回 `sent`）、`TestSendDuringStopIsCommitted`、`bus/tasks` 的发送、列表、错误结果、默认同类可见、可见性、配额（互相回复的两个任务停在配额上）、先查键再校验、页表与策略测试、`TestSteerWithASender`（stdio）、`examples/messaging` 的测试。

8. **本地模型：抢占、续写、重排**（6.4）：`Caps.Parallel` / `Prefill` / `ThinkOff`、`Request.NoThink`；组的 `Preempt` / `PreemptAfter` / `MaxPreempts`；抢占检查（就绪、`SetPriority`、定时器）、`preempted` 提交、`Partial` 续写与合并、有插话时先落成一轮、流中断续写；`m.SetPriority` 与 stdio `priority`；只有思考的截断先关思考；OpenAI 兼容适配器的本地选项与三种超时；`-local`；观测接上。
   核心已完成（`internal/core/preempt.go`、`model.go`；实现中的决定见 6.4 末尾）：TCB 的 `Partial` / `Preempts` / `NoThink`（gob、JSON 都向后兼容）；`GroupConfig` 和单组简写的 `Preempt` / `PreemptAfter`（默认 2s）/ `MaxPreempts`（默认 2），`Caps.Parallel` 约束 `Max`、简写 `Cores` 和 `SetCores`；每组在途模型调用登记、抢占检查（`OnReady`、`SetPriority` 之后、量子定时器、空出的 vCore 取到下一条指令时）、`preempted` 提交、取消优先；开着抢占时总是流式并在核心累加正文；续写（末尾 assistant 前缀、合并、有插话时先落成一轮并可撤回、不支持续写时丢弃、窗口估算计入）；流中断续写；`step` 的 `prefill`、`first_ms`；只有思考的截断先关思考（含只从流里看得出的思考）；`Sched.Urgent`，优先级可以大于 1，`Submit` 接受任何 `≥ 0` 的优先级；`m.SetPriority`；stdio `priority`（`task_busy`），不再是 `unsupported`；`sim.Model` 可以按假时钟分块流式、报告 `Caps`、记下收到的请求（`Requests`），`sim.Prefill` / `sim.Continue` 只回续写部分。
   验证：`TestPreemptedCallIsContinued`（模拟模型收到以 `Partial` 结尾的请求，最终答案 = 前缀 + 续写，被抢的提交只有 `step`、`preempted`，续写的 `step` 带 `prefill`，两次都带 `first_ms`）、`TestPreemptTakesTheLeastUrgentCall`（最不急的；同级跑得最短的；另一个不被抢）、`TestPreemptWaitsForTheQuantum`、`TestPreemptLimitPerStep`、`TestAgedTaskDoesNotPreempt`、`TestPreemptWithoutPrefillRestarts`、`TestPreemptedPartialThenSteer`、`TestCancelBeatsPreempt`（取消信号先到；只有取消的提交先到）、`TestLostStreamIsContinued`、`TestLostStreamWithoutPrefillStartsOver`、`TestSetPriority`（就绪、退避等待、在 vCore 上、已结束、不存在、负数、同值；提交后立刻触发抢占）、`TestThinkingOnlyTruncationTurnsThinkingOff`（关思考后回答；关思考仍截断再放大；没有 `ThinkOff` 直接放大；思考只从流里看得出）、`TestParallelCapsTheWindow`、`TestCrashAtEveryCommitPointWithAPreemption`（付款后回答被紧急任务抢占、紧急任务跑完、续写，每个提交点前后各崩一次，SQLite；重启后从 `Partial` 续写的崩溃点有 8 个）、`TestPrioritiesAboveOne`（sched）、`TestPacedStreamAndPrefill`（sim）、`TestPriorityOverTheWire`（stdio）、一致性套件的整 TCB 往返和 `TestOldTaskBlobsDecode` 覆盖三个新字段。
   评审后修正：登记之后立刻查一次（取到任务到发出之间就绪的更急任务）、熔断探测不被抢、空出的 vCore 先取为之让出的任务、截断只看续写这次自己的产出、被抢与流中断保留 `Partial` 的规则一致（`keepPartial`）。验证：`TestPreemptACallTakenAheadByAging`、`TestPreemptACallThatWaitedForRoom`、`TestFreedVCoreGoesToTheUrgentTask`、`TestProbeIsNeverPreempted`、`TestThinkingOnlyContinuationIsRedone`、`TestAgedTaskDoesNotPreempt` 断言空出的 vCore 先跑谁、`TestKeepPartial` / `TestContinuedJoins`（core）。
   接着想：TCB 的 `PartialThink`、`Caps.ThinkPrefill`，OpenAI 兼容适配器的 `ThinkTags`（渲染与切分，含流式跨块标签），`-local` 设 `ThinkTags` 并打开组的 `Preempt`；`preempted` 的 `kept_think`、`step` 的 `prefill_think`；`sim.Think` / `sim.PrefillThink` / `sim.ContinueThink`，`sim.Model` 先流思考后流正文。验证：`TestPreemptedThinkingIsContinued`（思考中被抢：留下思考，下次请求以只有思考的 assistant 消息结尾，答案的 reasoning 是两段接起来的；正文已开始：只留正文）、`TestThinkTagsContinueAThought`、`TestThinkTagsSplitTheStream`、`TestThinkSplitAtEveryOffset`、`TestThinkTagsLeaveOtherAnswersWhole`、`TestThinkTagsWithABackendThatParsesThinking`、`TestAThoughtThatStopsWithoutAnAnswer`（openai）、`TestAThoughtThatStopsIsRedoneWithoutThinking`、`TestLocal`（cmd）。
   想完了没作答（新调用也算）：OpenAI 兼容适配器只有思考的回答带上思考返回（字段、开标签开头的 `content`、只报推理 token），Anthropic 只有思考块的 `end_turn` 返回；`modelStep` 的 `stopped`、`redoThought`，空摘要不清零 `Retries`。验证：`TestAFreshThoughtThatStopsIsRedoneWithoutThinking`（真实失败任务的提示词、工具和文件，假的 Ollama 端点；`reasoning` 字段、思考标签、思考中截断，流式与非流式）、`TestARedoWithoutThinkingThatStaysEmptyIsBounded`、`TestAThoughtThatStopsWithoutThinkOff`（熔断不打开）、`TestATrulyEmptyTurnStaysTransient`、`TestAThoughtForASummaryIsBounded`、`TestAFreshThoughtThatStops`、`TestAFreshAnswerLeavesItsThinkingOut`、`TestAnEmptyTurn`（openai）、`TestAThoughtThatStops`（anthropic）。
   真实验收（`acceptance/local`，`NEUMANN_ACCEPT_LIVE=1`，本机 Ollama `qwen3.5:9b-mlx`，单 vCore、`Preempt`、`-local` 的适配器配置）：后台任务（17 × 23）思考 40 块后提交交互任务，后台调用立即被抢（已跑过 2 秒的量子），只留思考，交互任务答完，后台任务下一次调用带 `prefill_think`，接着想完、答 391。两次运行：被抢于 5.5 秒 / 2.1 秒，`kept_think` 168 / 159、`kept` 0，续写调用首块 393 / 430ms，交互任务 39 / 24 秒答完，后台任务 2 分 29 秒 / 1 分 27 秒完成；第二次断言答案的 reasoning 只开头一次（"Thinking Process:" 只出现一次，没有从头重想）。`observe` 的 span / 指标 / 日志在 `obs` 分支里做；`kept_think`、`prefill_think` 待它同步。

9. **挂起与恢复**（9.9）：中断表的 `suspend` / `resume`、各状态下的落地、打断在途模型调用（复用 6.4 的打断与 `keepPartial`）、恢复抵消未落地的挂起、`suspended` / `resumed` 事件、`ErrNotSuspended`、重启、stdio、观测。
   已完成（实现中的决定见 9.9 末尾）：`mem.InterruptSuspend` / `InterruptResume`（两个 Store 不用改表，种类本来就是字符串）；`Machine.Suspend` / `ResumeTask`、`ErrNotSuspended`；`Sched.Nudge` / `Hand` / `Heard` / `Mark` / `Landable`，交还被标记的任务时落地；所有模型步调用与等额度可被挂起切断（`cuttable`）；指令开头、指令提交、工具轮结束、`Approve`、`Resolve`、交还、恢复流程各处落地；`Status` / `GroupStatus` / `ClassQueue` 的 `Suspended`；stdio `suspend` / `resume`、`not_suspended`（不再是 `unsupported`）；README。
   验证：`TestSuspendReadyTask`（立即挂起、重复挂起不提交、`Status` / `Queue`、各种错误、恢复后运行）、`TestSuspendBackoffAndGroupWait`（`WakeAt` 丢弃、时钟走过和 `GroupReset` 都不唤醒）、`TestSuspendInterruptsTheModelCall`（组不开抢占也切断；`step` + `suspended`、保留正文、什么都不计；恢复后续写）、`TestSuspendWhileThinkingKeepsTheThought`、`TestSuspendWaitsForTools`、`TestSuspendDuringApproval`（拒绝：`was = approval`；放行：这一轮跑完，`was = tools`）、`TestSuspendDuringCompaction`、`TestSuspendWhileWaitingForRoom`、`TestResumeBeforeSuspendLands`（`resumed` `pending: true` 跟在完成工具轮的提交里、没有 `suspended`、第二次恢复 `ErrNotSuspended`）、`TestSuspendResumeSuspendInOrder`、`TestDeadlineAndCancelWhileSuspended`（含插话 `steer_dropped`）、`TestSteerWhileSuspended`、`TestSetPriorityAndClassWhileSuspended`、`TestResumeTriggersPreemption`、`TestSuspendAPreemptedCall`、`TestSuspendTheTaskAVCoreWasFreedFor`、`TestSuspendDuringTheCheck`（驳回：`was = check`；通过：任务结束）、`TestSuspendedSurvivesARestart`（挂起的任务、`Running` 中待落地的挂起、挂起任务待落地的恢复、就绪任务被抵消的挂起）、`TestCrashAtEveryCommitPointWithASuspend`（付款期间挂起、回答流到一半再挂起，操作员每次恢复，每个提交点前后各崩一次，SQLite：38 个崩溃点，重启时任务处于挂起的 8 个）、`TestCrashAtEveryCommitPointWithASuspendWhileWaiting`（等批准时挂起再放行 / 拒绝、退避中挂起、熔断等待中挂起，同样每个提交点前后各崩一次）、`TestLandSuspend` / `TestSuspendedNow`（core）、`TestNudgeTakesALandableTask` / `TestNudgeMarksAWaitThatLandsLater` / `TestHandRefusesANudgedTask` / `TestNudgeLeavesUnknownAndCancelledTasks`（sched）、一致性套件的 `SuspendAndResumeRoundTripInOrder` / `PendingSuspendRefusesNothing` / `SuspendAndResumeAreAppliedSilently`、`TestSuspendAndResumeOverTheWire`（stdio）。
   评审后修正：挂起、恢复判断任务是否挂起改为一次读中断表（见 9.9 末尾）。验证：`TestSuspendAfterAResumeStillLanding`、`TestResumeOfASuspendStillLanding`（用存储钩子把另一方的落地放在两次读之间，原实现每次都失败），以及评审的随机压测改成的常规测试 `TestSuspendResumeStress`（三个任务、一个开抢占的 vCore、时钟在走）、`TestSuspendResumeStressWithTools`（真时钟、三轮工具、临时错误，每个调用恰好执行一次、最后都不停在挂起）、`TestCancelRacesSuspendAndResume`（取消总是赢）。`observe` 的 span / 仪表 / 日志在 `obs2` 分支里做。

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
