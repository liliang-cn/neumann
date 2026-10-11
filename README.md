# neumann

neumann is a resident agent runtime for Go, organised like a von Neumann
machine: virtual cores (vCores) fetch a task, execute one instruction (one
model call, or one dispatch of tool calls) and commit the result; tools sit on
a bus and run as asynchronous I/O. Every state change is committed before
anything acts on it, so a crash at any point loses nothing that was
acknowledged and never silently repeats a side effect.

```
go get github.com/liliang-cn/neumann@latest
```

Requires Go 1.26 or newer (see `go.mod`). Status: **v0.1**; the API may change
before v1.

## Why you would use it

- **Side effects that survive crashes.** Every tool call is written to a call
  log before it is dispatched. After a crash, a call with side effects whose
  outcome is unknown is held for an operator instead of being sent again;
  read-only and idempotent calls are re-sent with the same idempotency key.
- **Elastic concurrency per provider.** Each model provider (or model) is an
  execution group with its own congestion window: it grows on success, halves
  on overload, and opens a circuit breaker when the provider keeps failing.
  Tasks can fall back to another group while one is down.
- **Rate limits as first-class objects.** Requests-, tokens-, input- and
  output-per-minute limiters that several groups can share, calibrated from the
  provider's rate-limit headers.
- **Cancellation and deadlines that always work.** A cancel or a deadline
  reaches a task in any state and interrupts its model call at once;
  `Cancel` returns once the cancel is committed, and it holds across a crash.
- **A replayable event log.** Everything that happens is an event with a
  global sequence number. A client that reconnects with the last number it
  saw gets exactly what it missed.
- **Slow tools do not hold a core.** A task waiting for tools gives its vCore
  to other tasks; the tool results wake it up.

## When you would not

neumann is a runtime, not a prompt framework. It does not write prompts, plan,
chain, or evaluate. Version 0.1 has no long-term memory or RAG, no sub-agents,
no teams or orchestration layer, no multimodal input and no HTTP API for
submitting work (there is an in-process Go API and a stdio JSON Lines
frontend). If you need one agent
answering one request in a script, a plain loop around a model SDK is simpler.

## Quick start (offline, no API key)

```
git clone https://github.com/liliang-cn/neumann
cd neumann
go run ./examples/hello
```

[`examples/hello`](examples/hello/main.go) runs a whole task on a scripted
model from package `sim`: the model calls two read-only calculator tools in
parallel, then a note-taking tool with a side effect, then answers. It prints
each committed event as it streams, then the task's call log:

```
#1   task    accepted     {"class":"default","pin_class":false,"priority":0}
#2   machine core_up      {"class":"default","core":1,"live":1,"reason":"demand"}
#3   task    step         {"pc":"model","core":1}
...
#5   task    tool_call    {"args":"{\"a\":19,\"b\":23}","call_id":"c-1","tool":"calc_add"}
#6   task    tool_call    {"args":"{\"a\":6,\"b\":7}","call_id":"c-2","tool":"calc_multiply"}
...
#15  task    answer       {"text":"19 + 23 = 42 and 6 × 7 = 42; both results are in your notes."}
#16  task    done         {"stop_reason":"end_turn"}

call log:
  c-1  calc_add       read_only   done  key=hello/c-1/1 result="42"
  c-2  calc_multiply  read_only   done  key=hello/c-2/1 result="42"
  c-3  notes_save     side_effect done  key=hello/c-3/1 result="saved"
```

The smallest program looks like this (it is also the package's godoc
example, `Example` in [example_test.go](example_test.go)):

```go
m, err := neumann.New(neumann.Config{
	Model:   adapter,                // a model.Adapter: sim, openai or anthropic
	Devices: []bus.Device{tools},    // tools the model may call
})
if err := m.Start(ctx); err != nil { ... } // recovers unfinished tasks, then runs
defer m.Stop(ctx)

id, err := m.Submit(ctx, proto.Instruction{Input: "What is the date?"}) // returns once committed
for ev := range m.Events(ctx, 0) {       // every committed event, in order
	if ev.TaskID == id && ev.Terminal() { // done, failed or cancelled
		break
	}
}
t, _ := m.Load(ctx, id)
fmt.Println(t.LastAnswer())
```

## With a real model

Two adapters ship with neumann: `model/openai` for any OpenAI-compatible
`/chat/completions` endpoint (OpenAI, DeepSeek, Ollama, gateways), and
`model/anthropic` for Anthropic's Messages API. Read keys from the
environment; never put them in source or in a config file you commit.

```go
gpt := openai.New(os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL"))
claude := anthropic.New("https://api.anthropic.com/v1", os.Getenv("ANTHROPIC_API_KEY"), os.Getenv("ANTHROPIC_MODEL"))

store, err := sqlite.Open("neumann.db") // durable state; the default is in memory
if err != nil { ... }
defer store.Close()

m, err := neumann.New(neumann.Config{
	Groups: []neumann.GroupConfig{
		{Class: "smart", Model: claude, Limits: []string{"anthropic"}, Fallback: "fast"},
		{Class: "fast", Model: gpt, Max: 32, CompactAt: 0.7}, // compact at 70% of its window
	},
	Limiters: []limit.Config{{Name: "anthropic", Limits: map[string]limit.Rate{
		"requests":     {Per: time.Minute, Limit: 50},
		"input_tokens": {Per: time.Minute, Limit: 40000},
	}}},
	Default: "fast",
	Store:   store,
	Devices: []bus.Device{outbox},
	System:  "You are a careful assistant.",
})
```

The base URL goes up to and including the API version
(`https://api.openai.com/v1`). Two runnable programs use a real endpoint:

```
OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/live
OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/mcp
```

`examples/live` takes `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL` when the
`OPENAI_` ones are unset; `examples/mcp` runs offline on a scripted model when
they are unset. The stdio frontend `cmd/neumann` (below) reads
`NEUMANN_API_KEY`, else `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for the chosen
`-provider`.

`openai.New` does not know the model's context window (a gateway may serve
any model): set `ContextWindow` on the adapter so tasks are compacted before
they outgrow it. Without it, a task is compacted only once the provider
rejects a request as too long. `anthropic.New` assumes 200000 tokens.

Model output streams by default when the adapter supports it. `m.Deltas(ctx)`
is a live preview of it; it is not committed and a slow reader may miss
pieces. The committed `answer` event is the record.

## Concepts

```
  Submit · Steer · Cancel · Resolve · Approve
                 │
┌────────────────┼─────────────────────── Machine ─────────────────────┐
│                ▼                                                     │
│  scheduler: a ready queue per class, the wait set, deadline timers   │
│       │ fetch a task                          ▲ hand it back         │
│       ▼                                       │                      │
│  execution groups, one per model provider (window W, breaker)        │
│    "smart": vCore vCore            "fast": vCore vCore vCore         │
│       │                                                              │
│       │ execute ONE instruction                                      │
│       ├──► a model call ──► limiters ──► model adapter               │
│       ├──► a tool dispatch ──► bus ──► devices                       │
│       │             (async: a finished call wakes the task)          │
│       │                                                              │
│       ▼ commit, then act                                             │
│  store: TCBs · tool call log · interrupts · events, one transaction  │
│       │                                                              │
│       └──► events published, in Seq order ──► Events(fromSeq)        │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

- **Machine** (`neumann.Machine`): the process-resident runtime. `New` builds
  it from a `Config`, `Start` recovers the store and starts work, `Stop` drains
  it.
- **Task and TCB** (`task.TCB`): a task is one conversation with one goal. Its
  task control block holds its state (`Ready`, `Running`, `Waiting`, `Done`,
  `Failed`, `Cancelled`), its program counter (the kind of its next
  instruction), its registers (the provider-neutral messages sent to the
  model), its budget and its confirmed usage.
- **Instruction and vCore**: an instruction is one step of one task: a model
  call (`StepModel`), a dispatch of the tool calls a model turn asked for
  (`StepTools`), or a compaction (`StepCompact`, below). A vCore is a goroutine that loops fetch → execute → commit. A
  task runs at most one instruction at a time; a vCore holds no state between
  instructions and retires only between them.
- **Execution group** (`GroupConfig`): one model adapter and its vCores. Its
  congestion **window** `W` bounds how many requests go to that provider at
  once: it grows by `1/W` per success and halves (once per congestion epoch) on
  overload. Its **breaker** opens after repeated transient failures or when the
  quota is exhausted; a `Fallback` group takes its tasks meanwhile. A task's
  `Class` names its group.
- **Compaction**: a task whose request (system prompt, tools, registers and
  the output reserve) passes `CompactAt` of its model's context window
  (default 0.8; the window comes from the adapter's `Caps`) compacts before
  its next model call. One model call, an instruction like any other,
  summarises the oldest whole rounds (an assistant turn with all its tool
  results, never split) into one user message marked as a summary; the
  task's request and its newest rounds (up to `CompactKeep` of the window,
  default 0.3) stay verbatim. A span too long for one compaction request is
  done in chunks, one instruction each. A provider rejecting a request as too
  long for its window (`model.ContextLength`) also triggers it. A task that
  no compaction can make fit fails with stop reason `context_overflow`. The
  original messages are kept in the store's archive: `m.History(ctx, id)`
  returns the whole conversation.
- **Limiter** (`limit.Config`): a provider's quota, separate from groups
  because quotas rarely line up with one model. A request needs room in every
  limiter its group names; groups can share limiters.
- **Bus and devices** (`bus.Device`): every tool belongs to a device; the model
  sees it as `<device>_<tool>`. Each tool declares its effect: `ReadOnly`,
  `Idempotent`, or `SideEffect` (the default). A task's `Allow` / `Deny`
  prefixes are its page table. Tool calls run on the device's queue while the
  task waits without a vCore.
- **Store and recovery** (`mem.Store`): main memory. Every state change is one
  `Commit` that writes the TCB, call records, interrupts and events together.
  `mem.NewMemory()` is the in-memory store (the default; nothing survives the
  process), `mem/sqlite` the durable one. On `Start`, the machine reads every
  unfinished task back and settles it.
- **Events** (`proto.Event`): published only after they are committed, with a
  global `Seq` and a per-task `TaskSeq`. Task events include `accepted`,
  `step`, `tool_call`, `tool_result`, `usage`, `answer`, `done`, `failed`,
  `cancelled`, `model_retry`, `compacted`, `reconcile_needed`, `tool_denied`,
  `approval_needed`, `approved`; machine events (no task)
  include `core_up`, `core_down`, `group_down`, `group_up`, `throttled`.

[docs/overview.md](docs/overview.md) walks through these mechanisms in more
depth for contributors.

## Extension points

### A custom device

Implement `bus.Device`. For plain Go functions, `bus/builtin` does it for
you; for an MCP server use `bus/mcp`; for skill directories, `bus/skills`.

```go
// outbox sends messages. Sending twice would be visible, so it is a
// SideEffect tool, and it uses the idempotency key to drop repeats.
type outbox struct {
	mu   sync.Mutex
	sent map[string]string // idempotency key -> message id
}

func (o *outbox) Name() string { return "outbox" }

func (o *outbox) Tools(ctx context.Context) ([]bus.ToolDef, error) {
	return []bus.ToolDef{{
		Name:        "send",
		Description: "Send a message to a user.",
		Schema:      json.RawMessage(`{"type":"object","properties":{"to":{"type":"string"},"text":{"type":"string"}},"required":["to","text"]}`),
		Effect:      bus.SideEffect, // the zero value; stated for the reader
		Timeout:     30 * time.Second,
	}}, nil
}

func (o *outbox) Call(ctx context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	var in struct{ To, Text string }
	if err := json.Unmarshal(c.Args, &in); err != nil {
		return task.ToolResult{Content: "bad arguments: " + err.Error(), IsError: true}, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if id, ok := o.sent[c.Key]; ok { // a repeat of a call that already went out
		return task.ToolResult{Content: "already sent as " + id}, nil
	}
	id := deliver(ctx, in.To, in.Text) // your real side effect
	if o.sent == nil {
		o.sent = map[string]string{}
	}
	o.sent[c.Key] = id
	return task.ToolResult{Content: "sent as " + id}, nil
}

func (o *outbox) Close() error { return nil }
```

Declare `ReadOnly` (optionally with `Cacheable`) or `Idempotent` only when a
repeat is harmless: recovery re-sends those after a crash. An error result
(`IsError`) goes to the model; a Go error from `Call` becomes an error result
too. Timeouts, cancellation and panics are handled by the bus, not the device.

### A custom model adapter

`model.Adapter` is two methods. Classify every failure you can explain; the
class decides whether the machine retries, backs off, halves the window or
opens the breaker.

```go
type myAdapter struct{ name string }

func (a *myAdapter) Provider() string { return "myprovider:" + a.name }

func (a *myAdapter) Call(ctx context.Context, req model.Request) (model.Response, error) {
	text, status, err := a.send(ctx, req) // your provider's client
	switch {
	case ctx.Err() != nil:
		return model.Response{}, ctx.Err() // the machine cancelled it: never retried
	case err != nil:
		return model.Response{}, &model.Error{Class: model.Transient, Provider: a.Provider(), Err: err}
	case status == 429:
		return model.Response{}, &model.Error{Class: model.RateLimited, Status: status, RetryAfter: 2 * time.Second, Provider: a.Provider()}
	case status >= 500:
		return model.Response{}, &model.Error{Class: model.Transient, Status: status, Provider: a.Provider()}
	case status >= 400:
		return model.Response{}, &model.Error{Class: model.Permanent, Status: status, Provider: a.Provider()}
	}
	return model.Response{
		Message:      task.Text(task.RoleAssistant, text),
		FinishReason: "stop",
		Usage:        task.Usage{PromptTokens: 100, CompletionTokens: 20},
	}, nil
}
```

A request the provider rejects as longer than the model's context window is
`model.ContextLength`: the machine compacts the task and sends it again
instead of failing it. Tool calls come back as `task.PartToolCall` parts with
`ProviderCallID`, `Name` and `Args` set. If the request reached the provider but the answer did not
arrive whole, wrap `model.ErrResponseLost` so it is counted as unconfirmed.

Optional interfaces, each discovered by type assertion:

```go
func (a *myAdapter) Caps() model.Caps { // model.CapsReporter: compaction sizes by Window; checked before a task migrates here
	return model.Caps{Window: 128000, MaxOut: 4096, Tools: true}
}

func (a *myAdapter) Classify(err error) model.ErrClass { // model.Classifier
	return model.Unknown // Unknown falls back to model.Classify
}

func (a *myAdapter) Reserve(req model.Request) map[string]int64 { // model.Metered: limiter accounting
	in := task.EstimateTokens(req.System, req.Tools, req.Messages)
	return map[string]int64{model.DimRequests: 1, model.DimInputTokens: in}
}

func (a *myAdapter) Settle(req model.Request, resp model.Response) map[string]int64 {
	return map[string]int64{model.DimInputTokens: int64(resp.Usage.PromptTokens)}
}

func (a *myAdapter) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) { // model.Streamer
	resp, err := a.Call(ctx, req) // a real adapter emits pieces as they arrive
	if err == nil {
		emit(model.Delta{Text: resp.Message.TextContent()})
	}
	return resp, err
}
```

Also available: `model.OutputCap` (the default output cap), `model.RateReporter`
(rate-limit headers, to calibrate limiters) and `model.Warmer` (open the
connection at `Start`). A wrapper around an adapter must forward `Stream`
as well as `Call`, or streaming silently turns off; `Start` logs a line when an
adapter is not a `model.Streamer`.

### A custom store

Implement `mem.Store` (seven methods: `Commit`, `Load`, `Unfinished`, `Calls`,
`Interrupts`, `Events`, `Archive`) and run the conformance suite against it. The
in-memory and SQLite stores pass the same suite:

```go
func TestMyStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) mem.Store {
		s, err := mystore.Open(filepath.Join(t.TempDir(), "state"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	})
}
```

The rules a store must keep are in the `mem.CommitSet` documentation and
[design §4.1](docs/design.md): one commit is one transaction; versions advance
by exactly one; events get dense sequence numbers; a pending cancel refuses
everything but the task's final commit. The machine never closes the store;
its owner does, after `Stop`.

## Operating it

**Status.** `m.Status()` is a cheap snapshot (no store read, no model call):
live vCores, ready / waiting / held tasks, and per group its window, vCores,
queue and breaker; per limiter its dimensions; the tool cache's counters.
`m.Queue()` gives the queues by class.

```go
for _, g := range m.Status().Groups {
	fmt.Printf("%s: W=%.1f live=%d ready=%d breaker=%s\n", g.Class, g.W, g.Live, g.Ready, g.Breaker)
}
```

**Events.** `m.Events(ctx, fromSeq)` streams every committed event after
`fromSeq` and then live ones; it never blocks the machine and never skips an
event. `m.EventLog`, `m.Load` and `m.Calls` read the store directly;
`m.History(ctx, id)` is a task's whole conversation, the messages compaction
swapped out included.

**Steering and cancelling.** `m.Steer(ctx, id, text)` adds a message to a
running task (delivered at its next model call, or reported as
`steer_dropped`); `m.Cancel(ctx, id)` ends it. Both return once committed.
A deadline is `Budget.Deadline` on submit; `Budget.MaxSteps` bounds the model
calls that got an answer plus those whose response was lost.

**Resolving after a crash.** When a restart finds a `SideEffect` call that was
dispatched but has no result, the task waits in `Waiting(Reconcile)` and a
`reconcile_needed` event names the call, its tool, arguments and idempotency
key. Check the far side, then answer:

```go
err := m.Resolve(ctx, taskID, callID, neumann.Outcome{Result: "sent as m-1"}) // it happened
err = m.Resolve(ctx, taskID, callID, neumann.Outcome{Result: "bounced", IsError: true}) // it failed
err = m.Resolve(ctx, taskID, callID, neumann.Outcome{Retry: true}) // send again, under a new key
```

A tool can opt out of being held with `ToolDef.ReportUnknown`: the model is
told the outcome is unknown instead.

**Tool policy and approval.** `Config.ToolPolicy` decides every tool call
before its first dispatch is committed: allow it, deny it (the model gets an
error result, a `tool_denied` event records it), rewrite its arguments, or
hold it for a person. The decision is committed with the call, so a restart
never asks again. The policy runs on the vCore and has no timeout: keep it
fast and side-effect free, and hand anything slow to a person:

```go
cfg.ToolPolicy = func(_ context.Context, c neumann.ToolCallInfo) neumann.Decision {
	if c.Effect == bus.SideEffect {
		return neumann.RequireApproval("moves money")
	}
	return neumann.Allow()
}
```

A held call parks its whole round in `Waiting(Approval)`, nothing sent, and an
`approval_needed` event names it. Answer each held call; the last answer
dispatches the round:

```go
err := m.Approve(ctx, taskID, callID, neumann.ApprovalDecision{Verdict: neumann.ApproveAllow})
err = m.Approve(ctx, taskID, callID, neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "not today"})
err = m.Approve(ctx, taskID, callID, neumann.ApprovalDecision{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`{"amount":59}`)})
```

A cancel, a deadline or `Stop` while the round waits sends nothing; a restart
keeps it waiting and announces the held calls again. `examples/approval` runs
it offline.

**Groups.** `m.SetCores(class, n)` pins a group's window at `n` vCores;
`m.AutoScale(class, true)` lets it adapt again. `m.SetClass(ctx, id, class)`
moves a task to another group between instructions (`ErrTaskBusy` means try
again). `m.GroupReset(ctx, class)` closes a breaker, which an exhausted quota
never does by itself.

**The stdio frontend.** `cmd/neumann` runs a machine behind JSON Lines on
stdin and stdout, for hosts in other languages; logs go to stderr.

```
NEUMANN_API_KEY=... go run ./cmd/neumann -provider openai -base-url https://host/v1 -model m -store state.db
```

```
{"id":1,"kind":"submit","task_id":"t1","input":"Say hello."}
{"id":2,"kind":"subscribe","from_seq":0}
{"id":3,"kind":"cancel","task_id":"t1"}
```

```
{"type":"reply","id":1,"kind":"submit","ok":true,"task_id":"t1"}
{"type":"reply","id":2,"kind":"subscribe","ok":true,"from_seq":0}
{"type":"reply","id":3,"kind":"cancel","ok":true,"task_id":"t1"}
{"type":"event","kind":"accepted","task_id":"t1","seq":1,"task_seq":1,"data":{"class":"default","pin_class":false,"priority":0}}
{"type":"event","kind":"cancelled","task_id":"t1","seq":2,"task_seq":2,"data":{"stop_reason":"cancelled"}}
```

An `"ok":true` reply to `submit`, `steer`, `cancel`, `resolve`, `approve` or `class`
means the instruction is committed. The other kinds (`status`, `queue`,
`cores`, `autoscale`, `group_reset`, `unsubscribe`) and the error codes are
documented in package `proto/jsonl`. Run `go run ./cmd/neumann -help` for
every flag and its environment variable. Without `-store` state is in memory
and nothing is durable.

## Observability

Package `observe` turns a machine's committed event log into the signals an
operator expects. It uses only the public `Machine` API, never blocks the
machine (events are read from the durable log at the consumer's pace), and a
consumer that restarts resumes from the last sequence number it read.

```go
reader, prom, _ := observe.PrometheusReader()            // or any sdkmetric.Reader (OTLP...)
metrics, _ := observe.NewMetrics(m, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), observe.MetricsOptions{})
tracer := observe.NewTracer(spanExporter, observe.TracerOptions{}) // any sdktrace.SpanExporter
activity := observe.NewLog(slog.Default())
go observe.Follow(ctx, m, 0, metrics, tracer, activity)  // replay from seq 0, then live

http.Handle("/", observe.NewHandler(m, observe.HandlerOptions{Metrics: prom, Token: os.Getenv("TOKEN")}))
```

- **Metrics** (OpenTelemetry, `neumann.` prefix): tasks by outcome, model call
  latency and failures by error class, tokens, retries, throttling, tool calls
  by outcome (incl. denied, cached, late), approvals, compactions, migrations,
  vCore changes; gauges for live/busy vCores, the window and breaker per
  group, queues, limiter headroom, the cache and the process. Labels are
  bounded; nothing is labelled by task id. The full table is in
  `go doc github.com/liliang-cn/neumann/observe`.
- **Traces**: one trace per task, rebuilt from the event log (`proto.Event`
  carries the commit `Time`): a task span, a span per instruction and per
  tool call, span events for retries, migrations and approvals. Span ids are
  derived from the task id and sequence numbers, so replaying the log after a
  crash rebuilds the same trace. Model spans start at the `model_call` event,
  committed as the request goes out, so they measure the provider's latency.
- **Activity log**: one `key=value` line per notable event through
  `log/slog` (no prompts or arguments).
- **HTTP** (`observe.NewHandler`, read-only): `/status`, `/queue`,
  `/tasks/{id}`, `/events?from_seq=N` (server-sent events, replay then live,
  `Last-Event-ID` resumes), `/metrics` when given, `/debug/pprof/` only when
  `Pprof` is set. `/tasks` and `/events` expose prompts and tool arguments:
  set `Token` or keep the handler private.

The stdio frontend serves the same view with `-observe-addr` (plus
`-metrics`, `-pprof`; `NEUMANN_OBSERVE_TOKEN` sets the bearer token) and
exports traces and metrics over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT`
is set:

```
NEUMANN_API_KEY=... go run ./cmd/neumann -model m -store state.db -observe-addr 127.0.0.1:43517 -metrics
```

`go run ./examples/observe` runs all of it offline and prints the URLs and a
span tree.

## Guarantees, precisely

What neumann promises:

- **Acknowledged means durable.** `Submit`, `Steer`, `Cancel`, `Resolve` and
  `SetClass` return success only after the change is committed. An event is
  published only after the commit that produced it.
- **Exactly one result per tool call.** Before the model is called again,
  every call of its last turn has one result, in call order: the device's
  result, an error result (refused, timed out, panicked), or, after a crash,
  the operator's `Resolve` outcome or a "result unknown" report for tools that
  declare `ReportUnknown`.
- **A `SideEffect` call is dispatched at most once automatically.** If the
  process dies after dispatching it and before its result is committed, the
  restarted machine marks it unknown and holds the task for an operator. It
  does not retry it and does not pretend it failed. Only `Resolve` with
  `Retry` sends it again, under a new idempotency key.
- **`ReadOnly` and `Idempotent` calls may run more than once**: after a
  crash they are re-sent with the same idempotency key.
- **The first committed cancel wins.** Once a cancel (or a passed deadline)
  is committed, the task ends `Cancelled` (or `Failed` with stop reason
  `deadline`) even across a restart; calls it had in flight are recorded as
  cancelled, unknown or late, and a late result never reaches the model.
- **Events can be replayed.** Sequence numbers are dense and global; reading
  from any `Seq` gives the same sequence a subscriber that never disconnected
  saw.
- **Usage counts only what a provider confirmed.** A request whose response
  was lost is counted in `Unconfirmed`, not in `Used`; budgets count both.

What it does not promise:

- **Exactly-once side effects in the outside world.** neumann dispatches a
  `SideEffect` call at most once by itself; making the effect itself happen at
  most once across an operator's retry needs the device (or the system behind
  it) to deduplicate by `DeviceCall.Key`.
- **That effect declarations are true.** Recovery trusts each tool's declared
  `Effect`, and MCP annotations are hints from the server.
- **Durability with the default store.** `mem.NewMemory()` keeps nothing
  across a restart; use `mem/sqlite`.
- **Exact model-call counts.** A model call interrupted by a crash or a
  cancel may have been processed and billed by the provider; it is counted as
  unconfirmed, and the step runs again after a restart.
- **Deltas.** Streamed output is a preview, not committed, and may be lost.
- **Rate limits and breaker state across restarts.** Limiters, windows and
  breakers start fresh; limiters recalibrate from the first responses.

## Project status

v0.1. The core described above is built and tested: commit-before-act state
machine, SQLite store with group commit, crash recovery and reconciliation,
cancellation and deadlines, elastic vCores, execution groups with breakers and
fallback migration, context compaction (including a migration that compacts
on a smaller target window), shared rate limiters, the OpenAI-compatible and Anthropic
adapters, MCP and skills devices, the tool result cache and the stdio
frontend. The API may change before v1.

Designed but not built yet:

- long-term memory and RAG, sub-agents, multimodal input;
- the operator controls `Pause` / `Resume` / `Drain`, per-task
  `Suspend` / `Resume` and `SetPriority` (the stdio frontend answers
  `suspend`, `resume` and `priority` with `unsupported`);
- a token budget per task (`Budget` has `MaxSteps` and `Deadline`);
- retention for the event log, which grows without bound for now.

## Testing

```
make check                    # gofmt, go vet, go test -race ./...
go test ./...                 # quicker, without the race detector
go test -race -count=20 -timeout 60m ./internal/... .   # what core changes are held to
```

Tests run offline. Package `sim` provides a scripted model with every failure
class (rate limited, overloaded, transient, permanent, quota exhausted, lost
response, truncated) and a fake clock that drives backoffs, deadlines,
breakers and limiter waits; crash tests restart a machine at every commit
point. The acceptance tests in `acceptance/` run against a real endpoint and
are skipped unless `NEUMANN_ACCEPT_LIVE=1`:

```
NEUMANN_ACCEPT_LIVE=1 OPENAI_BASE_URL=... OPENAI_API_KEY=... OPENAI_MODEL=... \
  go test -v -count=1 -timeout 30m ./acceptance/killnine   # SIGKILL mid side effect, then resolve
NEUMANN_ACCEPT_LIVE=1 OPENAI_BASE_URL=... OPENAI_API_KEY=... OPENAI_MODEL=... \
  go test -v -count=1 -timeout 40m ./acceptance/load       # 200 tasks on one autoscaling group
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for how changes are reviewed.

## Documentation

- [docs/overview.md](docs/overview.md): the architecture for contributors (English).
- [docs/design.md](docs/design.md): the full design, section by section (Chinese).
- [docs/architecture.html](docs/architecture.html): an illustrated architecture page (Chinese; open it in a browser).
- [examples/](examples/README.md): runnable programs.
- `go doc github.com/liliang-cn/neumann` and each package's documentation.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
