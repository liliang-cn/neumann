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
    Role  Role
    Parts []Part
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
type WaitReason int // WaitIO, WaitBackoff, WaitReconcile, WaitGroup

type TCB struct {
    ID         string
    State      State
    PC         PC
    Wait       WaitReason
    WakeAt     time.Time       // WaitBackoff 时有效
    Class      string          // 执行组；迁移时改变
    PinClass   bool            // true = 禁止迁移
    Priority   int             // 0 = 交互，1 = 后台
    ReadySince time.Time       // 进入就绪队列的时刻
    System     string          // 创建时确定
    Tools      []ToolDef       // 创建时按页表快照，按名字排序，之后不变
    Regs       []Message
    Allow      []string        // 页表
    Deny       []string
    Budget     Budget          // MaxSteps、Deadline（绝对时间）、MaxTokens
    Used       Usage           // 已确认的用量（见 4.6）
    Unconfirmed int            // 已发出、但没有拿到响应的模型请求数
    CallSeq    int             // 已分配的工具调用 id 数；下一个是 c-<CallSeq+1>，只增不减，压缩后 id 也不重复
    Retries    int
    MaxOut     int
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
    Events        []Event      // 本次提交产生的事件；外部指令本身由 accepted 等事件记录
}

type Store interface {
    Commit(ctx context.Context, c CommitSet) ([]Event, error) // 一个事务；返回带好 Seq / TaskSeq 的事件
    Load(ctx context.Context, id string) (*TCB, error)
    Unfinished(ctx context.Context) ([]*TCB, error)
    Calls(ctx context.Context, taskID string) ([]CallRecord, error)
    Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]Interrupt, error)
    Events(ctx context.Context, afterSeq int64, limit int) ([]Event, error)
}
```

规则：

1. **任何状态转换都通过 `Commit` 完成**，无论是 vCore 执行完一步，还是调度器处理工具完成、退避到点、Deadline、挂起、改优先级、迁移。
2. **先提交，后动作。** 顺序永远是：算出新状态 → `Commit` 成功 → 才去改内存里的队列、通知订阅者、派发工具、给调用方返回成功。`Commit` 失败则这次转换不存在。
3. **状态、调用记录、中断、事件在同一个事务里。** 客户端看到的任何事件，重启后都能在状态里找到对应的结果。
4. 写入者单线程，组提交：5 毫秒内（或满 64 个）到达的 `CommitSet` 合成一个事务；每个调用方在自己所属的批次提交后才返回。

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
}
```

生命周期：

1. 模型返回工具调用时，`execModel` 的同一次提交里写入这些调用，状态直接是 `dispatched`（`Attempt = 1`），任务转 `Waiting(IO)`；提交成功、任务交还调度器之后，同一个 vCore 立刻把调用交给总线。一轮工具不需要再排一次队等 vCore：实测在 cpa 上，派发曾因此等了 8 秒。
2. `planned` 状态留给"已规划、从未派发"的调用（重启恢复时出现）；`execTools` 处理这种任务：**先提交** `dispatched`、`Attempt + 1`，任务转 `Waiting(IO)`，**然后**派发。
3. 每个调用完成后，调度器提交 `done` 和结果，同一事务里带上 `tool_result` 事件。
4. 全部 `done` 时，调度器提交：按调用顺序把结果追加成 tool 消息，任务转 `Ready`、`PC = StepModel`。这次提交把所有 `done` 记录和任务一起再写一遍（幂等），保证模型看到的每个结果都在调用记录里，即使第 3 步的某次单条提交失败过。

崩溃后重启，每条记录按状态处理：

| 状态 | 处理 |
|---|---|
| `planned` | 从未离开进程，正常派发 |
| `dispatched`，`Effect` 为 `ReadOnly` 或 `Idempotent` | 用同一个 `Key` 重新派发（`Attempt` 不变） |
| `dispatched`，`Effect` 为 `SideEffect` | 状态改为 `unknown`，任务转 `Waiting(Reconcile)`，发 `reconcile_needed` 事件。不自动重试，也不假装失败 |
| `done` | 已有结果，不再执行 |

`Waiting(Reconcile)` 的任务由操作员用 `resolve` 指令处理（第 12 节）：给出实际结果、判定失败，或要求重试。也可以对某个设备配置 `OnUnknown: ReportToModel`，把"调用已发出、进程在收到结果前重启，结果未知"作为结果交给模型，由模型用只读工具核实。默认是等待人工核对。

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

### 4.5 重启

1. 加载所有未结束的 TCB、它们的调用记录和未送达的中断。
2. `Running` → `Ready`（这一步重做，可能重复一次模型调用的花费，`Unconfirmed + 1`）。
3. 调用记录按 4.2 的表处理。
4. 按 `Class`、`Priority`、`ReadySince` 重建就绪队列；`Waiting(Backoff)` 按 `WakeAt` 重设定时器；所有未结束任务重设 Deadline 定时器。
5. 每个执行组按 `Min` 启动 vCore，`W` 回到 `Initial`；限流器从空状态开始，由服务商响应头重新校准（7.3）。

### 4.6 计量

- `Used` 只累计**已确认**的用量，也就是从服务商响应里读到的。
- 请求已发出但没拿到响应（超时、连接断开、进程崩溃），服务商可能已经处理并计费，本地无法知道。这种情况计入 `Unconfirmed`，不估算 token。
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
2. 寄存器 token 估算超过 `0.7 × 本组模型窗口`（窗口未知时用 `Config.CompactAt`，默认 60000）时，改设 `PC = StepCompact`，提交后结束本步。
3. 向本组的限流器预占额度（7.2），通过适配器调一次模型。请求以任务的 context 发出，取消会立刻中断它。
4. 结果：
   - **有工具调用**：assistant 消息原样追加，调用记录写为 `planned`，`PC = StepTools`。
   - **最终文本**：调用期间到达了插话，则文本作为 assistant 轮次，`PC = StepModel`，**插话留在中断表里不合并**：下一条模型指令开头再送达，若下一步因预算或模型出错而结束，它由终态提交报告为 `steer_dropped`。没有插话则 `Done`。
   - 模型调用 panic 等同于模型出错：任务 `Failed`，原因 `model panicked: …`。
   - **截断且没有内容**：`MaxOut` 乘 4 重做，最多两次，之后 `Failed`。
5. 提交里带 `usage` 事件（已确认用量）。出错见第 13 节。

### 5.2 execTools

只用于调用处于 `planned`、从未派发的任务（重启恢复）。提交调用记录 `dispatched` 和任务 `Waiting(IO)`；提交成功后派发。vCore 不等结果。正常路径上，模型那一步已经派发了自己的调用（4.2 第 1 步）。

### 5.3 execCompact

用一次模型调用把最早的若干个完整轮次（assistant 加上它的全部 tool 结果，从不拆开）换出成一条摘要消息。被换出的部分按"压缩请求本身必须放得进本组窗口"来切块：一块放不下就分几次压缩，每次一条指令。单条消息本身就超过窗口时，任务 `Failed`，`StopReason = "context_overflow"`。

## 6. 执行组与弹性 vCore

```go
type ExecGroup struct {
    Class      string
    Model      model.Adapter
    Limits     []string      // 引用的限流器名字，可以多个（例如组织级 + 模型级）
    Min        int           // 默认 1
    Max        int           // 0 = 不设上限
    Initial    float64       // 初始窗口，默认 2
    IdleRetire time.Duration // 默认 30s
    Fallback   string        // 熔断时迁往的组
    OnMigrateFail string     // "wait"（默认）或 "fail"
    MaxQueued  int
}
```

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

### 6.2 熔断

- 同组连续 5 次指令以临时错误失败（成功一次清零），或适配器报告额度耗尽（第 11 节），本组熔断：停止发请求，发 `group_down`。
- 临时错误熔断 30 秒后半开：放一个请求试探，成功则 `group_up`。额度耗尽熔断不自动半开，等操作员 `group_reset`。
- 熔断期间，本组就绪的任务转 `Waiting(Group)`。没有 `Fallback`、或任务 `PinClass` 时，就这样等到恢复或 Deadline。

### 6.3 迁移

有 `Fallback` 且任务未 `PinClass` 时，尝试迁往 `Fallback` 组。迁移前做能力校验：

1. **工具能力**：目标适配器必须支持工具调用；任务快照的工具数不超过目标适配器的上限。
2. **上下文**：`估算(System + Tools + Regs) + MaxOut 预留 ≤ 目标窗口`。
3. 第 2 条不满足时，检查能否在目标组上压缩到放得下：按 5.3 切块，每块压缩请求都必须放得进目标窗口。能，则迁移后第一条指令就是 `StepCompact`。
4. 都不满足：按 `OnMigrateFail`，`wait` 则留在 `Waiting(Group)`，`fail` 则 `Failed`，`StopReason = "migrate_incompatible"`，事件里带不满足的那一条。

校验通过：提交 `Class` 改变、`ReadySince` 保留（不重新排队尾）、`migrated` 事件。迁移后第一次请求丢掉原服务商的 `reasoning` 块，L1 必然不命中。

## 7. 限流器

### 7.1 为什么和执行组分开

服务商的额度范围和模型不一定一一对应：一个组织下多个模型可能共享 TPM，一个模型可能同时受 RPM、输入 TPM、输出 TPM 限制，额度还可能按天耗尽。所以限流器是独立的对象，执行组按名字引用；一个请求要同时拿到它引用的所有限流器的额度才能发出。

```go
type Limiter struct {
    Name   string
    Limits map[string]Rate // 维度名 → 速率，例如 "requests"、"tokens"、"input_tokens"、"output_tokens"
}

type Rate struct {
    Per   time.Duration // 例如 1 分钟
    Limit int64
}
```

### 7.2 预占与结算由适配器定义

```go
type Adapter interface {
    // ...
    Reserve(req Request) map[string]int64           // 发请求前各维度预占多少
    Settle(req Request, resp Response) map[string]int64 // 拿到响应后各维度实际用了多少
    RateInfo(resp Response, err error) *RateInfo     // 从响应头读到的剩余额度和重置时间
}
```

不同服务商算法不同，所以不统一估算：例如有的服务商把请求里的输出上限也计入 TPM，有的把输入、输出分开限制，缓存命中的 token 可能不计入。预占在请求发出前从各限流器扣除，响应后按 `Settle` 多退少补；请求失败且服务商未处理时全额退回。

预占发生在 vCore 取到任务、组装好请求之后（预占量取决于请求）。额度不够时，vCore 持着这个任务等待额度，等待可以被取消和 Deadline 打断；本组其他 vCore 也各自在预占处等待，所以本组实际上暂停了。其他执行组不受影响，除非它们引用了同一个限流器。发 `throttled` 事件。

### 7.3 用服务商的反馈校准

适配器从响应头（剩余请求数、剩余 token、重置时间、`retry-after`）读出 `RateInfo`，限流器据此修正自己的计数：本地以为还有额度但服务商说没了，以服务商为准。重启后限流器从空开始，第一个响应就会把它校准。

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
| 等待集合 | `Waiting(IO / Backoff / Reconcile / Group)`、`Suspended` | 由 TCB 重建 |
| 中断表 | 每个任务未送达的外部输入 | 中断表本身 |
| 设备队列 | 等待设备的工具调用，每个设备一条 FIFO | 调用记录（`dispatched`） |
| 事件流 | 全局有序的事件 | 事件表；内存环形缓冲只是加速 |

### 9.2 准入

全局 `MaxQueued`（默认 1000）和每组 `MaxQueued`，计未结束的任务数。超限时 `Submit` 返回 `*CapacityError`（`errors.Is(err, ErrBusy)`），不阻塞。准入判断、登记和提交在同一把锁的保护下完成。

### 9.3 出队顺序

组内交互级先于后台级；同级按 `ReadySince` 先进先出，每步之后 `ReadySince` 更新，所以同组任务轮转推进；后台任务 `ReadySince` 早于 `now - AgingAfter`（默认 30s）时按交互级排序。退避中的任务不在就绪队列里。

### 9.4 Deadline

调度器为每个有 Deadline 的任务维护一个定时器，**与任务处于什么状态无关**：就绪、等待工具、退避、待核对、熔断等待、挂起、正在某个 vCore 上执行，都会触发。触发时按"取消"处理（9.5），`StopReason = "deadline"`，终态为 `Failed`。

### 9.5 取消

1. 提交取消中断（4.3），调用方随即得到成功返回。
2. 调用任务的 `CancelFunc`：在途的模型请求立即中断；在途的工具调用的 context 被取消。
3. 任务的所有者做最终提交：
   - `planned` 的调用 → `cancelled`，从未派发；
   - `dispatched` 且设备在取消后确认没有执行 → `cancelled`；
   - `dispatched` 且结果未知 → `unknown`，发 `reconcile_needed`（任务已取消，不再等待核对，但记录留给操作员）；
   - 任务转 `Cancelled`，未送达的插话各一个 `steer_dropped`。
4. 取消之后才到达的工具结果，调用记录标为 `late` 并保存结果，不进入寄存器，发 `tool_late` 事件。

挂起和熔断等待中的任务取消时没有在途调用，直接由调度器做最终提交。

### 9.6 操作员控制

```go
m.Pause() / m.Resume()          // 全部 vCore 执行完当前指令后停止取指
m.Drain(ctx)                    // 拒绝新 Submit，等所有任务结束
m.Suspend(id) / m.ResumeTask(id)
m.SetPriority(id, p)
m.SetClass(id, class)           // 手动迁移，同样做 6.3 的校验
m.Cancel(id)
m.Resolve(id, callID, outcome)  // 处理 Waiting(Reconcile)
m.SetCores(class, n) / m.AutoScale(class, on)
m.GroupReset(class)             // 额度耗尽熔断后手动恢复
m.Queue() QueueStatus
```

每个控制都是一条指令，提交后返回，也可以通过协议发出。

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
- L2：只缓存 `ReadOnly && Cacheable`，作用域是单个任务，同一任务里任何非 `ReadOnly` 调用完成后清空，命中返回真实结果。
- 第一版设备：`builtin`、`mcp`（`github.com/modelcontextprotocol/go-sdk`）、`skills`。

## 11. 模型适配器

```go
type Adapter interface {
    Provider() string
    Caps() Caps // 窗口、最大输出、是否支持工具调用、单次工具数上限
    Call(ctx context.Context, req Request) (Response, error)
    Classify(err error, resp *Response) ErrClass
    Reserve(req Request) map[string]int64
    Settle(req Request, resp Response) map[string]int64
    RateInfo(resp *Response, err error) *RateInfo
}
```

错误分类：

| 类别 | 例子 | 执行组 | 限流器 | 任务 |
|---|---|---|---|---|
| `RateLimited` | 429 且带限流响应头或 `retry-after` | 不变 | 按 `RateInfo` 暂停到重置时间 | 回就绪队列，不计重试次数 |
| `QuotaExhausted` | 额度或账单耗尽 | 熔断，不自动半开 | — | `Waiting(Group)` 或迁移 |
| `Overload` | 529、503 overloaded；无法区分的 429 | 窗口减半（每周期一次） | 退避 | 退避后重试 |
| `Transient` | 其余 5xx、网络错误、超时 | 计入熔断 | — | 退避后重试，`Unconfirmed + 1` |
| `Permanent` | 其余 4xx | — | 退还预占 | `Failed` |

`context.Canceled` 永远不是可重试错误。永久错误的判断优先于报错文字里的"timeout"之类字样。

第一版两个适配器：OpenAI 兼容（`/chat/completions`）和 Anthropic 原生（`/v1/messages`）。

传输层：默认 HTTP/2，所有 vCore 的请求共用一条 TLS 连接多路复用，TLS 会话可恢复；HTTP/3（QUIC）可选，QUIC 拨号失败时永久回退到 HTTP/2——只有拨号阶段的失败才回退，请求发出后的超时不重发。`Start` 时后台预热连接。实测 cpa（家用宽带）：预热后的单次往返约 75ms，16 个并发请求 HTTP/2 比 HTTP/1.1 快约 40%，HTTP/3 与 HTTP/2 持平；一次模型调用约 3.5s，协议层的空间约占 2%。

OpenAI 兼容适配器把服务商返回的 tool_call 原样存进 `Part.Opaque`，回传给同一服务商时逐字节重放（Gemini 的 thought signature 就在这里）；发给其他服务商时用机器自己的 id 重新渲染。缓存标记由适配器负责：OpenAI 兼容服务自动缓存；Anthropic 在工具定义末尾和消息序列倒数第二条打 `cache_control`。请求按固定顺序组装（系统提示词 → 排好序的工具 → 消息），前两部分在任务生命周期内逐字节不变。

## 12. 协议

```go
type Instruction struct {
    Kind     string          `json:"kind"`
    // submit / steer / cancel / suspend / resume / priority / class / resolve
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
}

type Event struct {
    Kind    string          `json:"kind"`
    TaskID  string          `json:"task_id,omitempty"`
    Seq     int64           `json:"seq"`      // 全局序号
    TaskSeq int64           `json:"task_seq"` // 任务内序号
    Data    json.RawMessage `json:"data,omitempty"`
}
```

任务事件：`accepted`、`step`、`tool_call`、`tool_result`、`tool_late`、`usage`、`answer`、`migrated`、`reconcile_needed`、`done`、`failed`、`cancelled`、`steer_dropped`。

机器事件：`core_up`、`core_down`、`throttled`、`group_down`、`group_up`。

stdio 前端：每条指令一行 JSON，处理完回一行 `{"ok":true,"task_id":...}` 或错误（返回即已持久化）；事件另起一个流，支持 `{"kind":"subscribe","from_seq":N}` 补读。

## 13. 出错处理

| 情况 | 处理 |
|---|---|
| 模型请求失败 | 按第 11 节分类 |
| 截断且无内容 | 提高输出上限重做，最多两次 |
| 工具出错 / 超时 / 越权 | 错误文本作为该调用的结果交给模型 |
| 提交失败 | 记 ERROR；这次状态转换不存在，任务不再调度，发不出事件（因为事件也没提交）；`Status` 里标出该任务和错误 |
| 进程崩溃 | 按 4.5 恢复 |
| 超出 MaxSteps / MaxTokens | `Done`，`StopReason = "budget"` |
| Deadline | `Failed`，`StopReason = "deadline"`（9.4） |
| vCore panic | 恢复 panic，该任务 `Failed` 并带 panic 信息，这个 vCore 退役 |

## 14. 观测

`m.Status()`：每个执行组的 `W`、`epoch`、存活 vCore 数、冷却/熔断状态；每个限流器各维度的剩余额度和下次重置时间；每个 vCore 在做什么；每条队列长度和最长等待；待核对的调用列表；每个未结束任务的状态、组、步数、已确认用量和未确认请求数。加锁拷贝，不调模型、不读主存。

## 15. 公开 API

```go
m, err := neumann.New(neumann.Config{
    Groups: []neumann.ExecGroup{
        {Class: "big",    Model: anthropic, Limits: []string{"anthropic-org"}, Fallback: "little"},
        {Class: "little", Model: flash,     Limits: []string{"gw"}, Min: 2, Max: 64},
    },
    Limiters: []limit.Limiter{
        {Name: "anthropic-org", Limits: map[string]limit.Rate{"requests": {time.Minute, 50}, "input_tokens": {time.Minute, 40000}}},
        {Name: "gw", Limits: map[string]limit.Rate{"requests": {time.Minute, 600}}},
    },
    Default:   "little",
    Store:     store,
    Devices:   []bus.Device{builtin, mcpGitHub, skillsDir},
    System:    "You are ...",
    MaxQueued: 1000,
})
err = m.Start(ctx)                         // 按 4.5 恢复，然后启动 vCore
id, err := m.Submit(ctx, neumann.Instruction{Kind: "submit", Input: "...", Class: "big"}) // 返回即已持久化
sub := m.Events(ctx, fromSeq)              // 可补读；m.EventLog / m.Load / m.Calls 直接读主存
err = m.Stop(ctx)                          // 停止取指，等在途指令和工具批次提交；ctx 到期则取消它们（被打断的不提交），再等 2 秒后关闭
```

## 16. 包结构

```
neumann/            Machine、Config、公开 API、操作员控制
  core/             vCore 循环、execModel / execTools / execCompact、执行组、拥塞窗口、熔断、迁移校验
  limit/            限流器
  sched/            就绪队列、等待集合、Deadline 与退避定时器、准入、取消
  task/             TCB、中立消息、寄存器组装、token 估算
  bus/              Bus、仲裁、设备队列、页表
    builtin/  mcp/  skills/
  cache/            L2
  mem/              Store 接口、memory 实现、sqlite 实现（组提交）、Disk 接口
  model/            Adapter 接口、错误分类
    openai/  anthropic/
  proto/            Instruction、Event、JSON Lines 编解码
  sim/              确定性模拟器：假时钟、脚本模型、脚本设备、故障注入点
  cmd/neumann/      stdio 前端
```

## 17. 实现顺序

每一阶段结束时第 18 节对应的测试必须为绿，再进入下一阶段。

1. **固定并发 + 状态机**：一个执行组、固定 vCore 数、内存版 Store；TCB 状态机、就绪队列、等待集合、工具异步派发、中断表、插话。
2. **原子持久化与工具恢复**：SQLite Store、`Commit` 组提交、调用记录、事件表与补读、4.5 的重启流程、`Waiting(Reconcile)` 与 `resolve`。
   第一阶段留下、这一阶段必须处理的接缝：
   - `Running` 从未提交。强制停机打断模型调用后，库里的任务是 `Ready`；要么在调用前提交 `Running`，要么删掉 4.5 第 2 步的规则，并据此计 `Unconfirmed`。
   - `CallRecord` 还没有 `Device` 和 `Effect`。规划调用时就要从总线记下来，不能在恢复时去问当前的总线（工具声明可能已经变了）。
   - `Waiting(IO)` 的任务可能同时有 `done` 和 `dispatched` 记录，且没有完成提交。把 `complete` 里组装 tool 消息的部分抽成"从调用记录组装"的函数，恢复时复用。
   - `dispatched` 的调用可能从未发出（停机发生在提交之后、派发之前）。这是预写日志固有的，`SideEffect` 调用会因此进入核对；写进运维说明。
   - 准入计数从 0 开始。`Start` 要用 `Unfinished` 的数量初始化，否则重启后的终态提交会把它减成负数。
   - `Submit` / `Steer` 目前在准入锁里做提交。换成 SQLite 组提交后会串行化所有提交：改成在锁里预留名额、锁外提交、失败时回滚。
   - 单条结果提交失败时，对应的 `tool_result` 事件不会发出（完成提交会补记录，但不补事件）。
   - 把 `core`、`sched` 移到 `internal/`，外部代码不能绕过所有权规则调用 `Requeue` / `Checkout` / `Forget`；`Config.Clock` 的类型随之移到根包。
3. **取消、Deadline 与故障注入**：9.4、9.5；在每一个提交点前后注入崩溃。
4. **自适应伸缩、限流器与迁移**：拥塞窗口、限流器、错误分类、熔断、迁移校验、第二个适配器。

## 18. 测试

**模拟器（`sim`）**：假时钟、脚本模型（可注入 `RateLimited` / `QuotaExhausted` / `Overload` / `Transient` / 截断 / 响应丢失）、可控延迟和可注入"执行了但结果丢失"的设备，全部在 `-race` 下跑。

状态机（阶段 1）：

1. 每个工具调用恰好一个结果，顺序与调用一致。
2. 一个 TCB 任何时刻只有一个所有者。
3. 工具在途时，即使只有 1 个 vCore，也能推进其他任务。
4. 后台任务在持续的交互负载下，`AgingAfter` 之后仍会被执行。
5. 插话在任何状态下送达，或以 `steer_dropped` 报告。
6. 模型调用次数 = `StepModel` + `StepCompact` 指令数。

持久化与恢复（阶段 2、3）：

7. **在每一个提交点之前和之后各注入一次崩溃**，重启后：所有任务都能结束；`SideEffect` 调用的自动派发次数 ≤ 1；`ReadOnly` / `Idempotent` 调用重派时携带相同的 `Key`；不存在"事件已发布、状态里却没有"的情况。
8. 事件补读：从任意 `Seq` 订阅，得到的事件序列与一直在线的订阅者完全相同；内存缓冲溢出时订阅者无缝切到补读。
9. `Submit` / `Steer` 返回成功后立刻崩溃，重启后该任务 / 插话都在。
10. Deadline 在就绪、等待工具、退避、待核对、熔断等待、挂起、执行中七种状态下都能触发。
11. 取消时：`planned` 调用从未派发；在途调用收到 context 取消；晚到结果记为 `late` 且不进寄存器。
12. `Unconfirmed` 只在响应丢失时增加；`Used` 只包含响应里读到的用量。

伸缩、限流与迁移（阶段 4）：

13. 脚本服务商在并发超过 K 时返回 `Overload`：稳定后存活 vCore 在 K 附近振荡，不超过 2K。
14. 同一周期内 10 个在途请求同时返回 `Overload`，窗口只减半一次。
15. 两个执行组共享一个限流器：两组合计的请求速率不超过限流器的额度。
16. `RateLimited` 不改变窗口；`QuotaExhausted` 熔断且不自动半开。
17. 迁移校验：目标窗口放不下且无法切块压缩时，按 `OnMigrateFail` 处理；迁移成功时发往新服务商的请求不含原服务商的 reasoning 块。
18. 中立格式经 OpenAI 和 Anthropic 适配器各往返一次，与原文一致（reasoning 块按规则保留或丢弃，`ProviderCallID` 按规则使用）。
19. vCore 只在指令之间退役。
20. 32 个并发 `Submit` 对 `MaxQueued = 4`：恰好 4 个被接受。

**真实对比**：eval-go agentbench 里加 neumann 适配器，和 agent-go、harness-rs 在 core-30 + hard-15 上比，经过同一个计费代理统计成本。

## 19. 第一版验收标准

1. 第 18 节全部通过，`go test -race ./...` 为绿。
2. 真实模型上，一个带 `SideEffect` 工具的多轮任务在工具执行中途 `kill -9`：重启后任务进入 `Waiting(Reconcile)`，`resolve` 之后完成；工具实际只执行了一次。
3. 真实网关上，同时提交 200 个短任务（单组、自动伸缩、`Max = 0`）：全部完成；出现过载后窗口回落再增长；计费代理统计到的模型请求数 = 已确认的模型指令数 + `Unconfirmed`。
4. agentbench core-30 + hard-15（rubric 判定、2 轮）：通过数不低于 harness-rs 减 2；每题 total token 不高于 harness-rs 的 1.1 倍。
