# neumann architecture overview

This is the map for contributors: how a task moves through the machine, who
may change it, and why a crash at any point is survivable. Each section names
the part of [design.md](design.md) (Chinese) that specifies it in full; when
this page and design.md disagree, design.md and the tests win.

## Where things live

| Package | What it owns |
|---|---|
| `neumann` (root) | `Machine`, `Config`, the public API, admission, start-up recovery, operator controls |
| `internal/core` | the vCore loop, `execModel` / `execTools`, tool batches, recovery and reconciliation, cancellation, migration |
| `internal/sched` | ready queues, the wait set, deadline and backoff timers; the only place a TCB changes hands |
| `internal/group` | congestion window, scaler, breaker: counts and decisions only, no goroutines |
| `limit` | rate limiters (sliding-window logs, all-or-nothing reservations) |
| `task` | the TCB, provider-neutral messages, the token estimate |
| `mem`, `mem/sqlite`, `mem/storetest` | the `Store` interface, the in-memory and SQLite stores, the conformance suite |
| `bus`, `bus/builtin`, `bus/mcp`, `bus/skills`, `cache` | tools as devices, the page table, per-device queues, the L2 result cache |
| `model`, `model/openai`, `model/anthropic` | the adapter interface, error classes, two adapters |
| `proto`, `proto/jsonl`, `cmd/neumann` | instructions and events, the JSON Lines frontend |
| `decide`, `decide/decidetest` | decision engines as a coprocessor (System One client, tool policy, router, answer checker), outside the machine: host code and the tool policy ask them, no instruction does |
| `sim` | scripted model, fake clock: the test harness |

`internal/` is deliberate: nothing outside the module can bypass the ownership
rules below.

## The rule everything rests on: commit, then act

(design §4.1)

Every state change is one `mem.CommitSet` passed to `Store.Commit`: the new
TCB, the call records it touches, new interrupts, interrupts it consumed, and
the events it produces, in one transaction. The order is always:

1. compute the new state;
2. `Commit` succeeds;
3. only then: requeue the task, wake subscribers, dispatch tools, return
   success to the caller.

If `Commit` fails, the transition did not happen. Because events are in the
same transaction as the state, a client never sees an event whose effect a
restart could lose. `Version` must advance by exactly one per commit; a
mismatch is a bug in ownership, not a retryable condition.

One more rule makes cancellation decisive (rule 4): while a task has a
committed, undelivered cancel, the store accepts only the task's final commit
and refuses to record any of its calls as dispatched (`ErrCancelPending`).
Whoever holds the task when that refusal arrives finalises it on the spot. So
the first committed cancel always wins, whatever the holder had seen.

The SQLite store has a single writer goroutine and group commit: commits that
arrive while a transaction is being written go into the next one together
(each under its own savepoint), so one fsync is shared under load without
delaying a lone commit.

## Ownership of a TCB

(design §8)

A TCB has exactly one owner at a time, and only the owner commits changes to
it:

- the **scheduler** while it is in a ready queue or the wait set;
- a **vCore** from `Sched.Next` until it hands the task back;
- a **tool batch** from `Checkout` until it hands the task back.

Hand-back is `Requeue` (or `Forget` after a failed commit or a halt).
Requeuing a task twice, or a batch not finding its own task, panics: those are
ownership bugs.

Two things reach a task without owning it:

- **Outside input** (`Submit` aside): `Steer`, `Cancel` and the like are
  written to the interrupt table and committed; the owner consumes them in its
  next commit, marking them applied in the same transaction (design §4.3).
- **The cancel signal**: the scheduler holds a context per task. `Cancel`
  commits the cancel interrupt, then cancels that context, so an in-flight
  model call or tool call stops at once; the owner still makes the final
  commit (design §9.5).

## The instruction cycle

(design §5)

A vCore is a goroutine running `RunVCore`: fetch a task from its group's ready
queue, execute one instruction, commit, hand back, act. A task's `PC` says
which instruction comes next.

**`StepModel`** (design §5.1), in order:

1. Merge undelivered steers into the registers.
2. Budget: past `MaxSteps` (answered plus unconfirmed calls), finish `Done`
   with stop reason `budget`.
3. Group: if the group's breaker is open, park the task in `Waiting(Group)`
   or migrate it to the fallback (below).
4. Limiters: reserve room in every limiter the group names. The vCore holds
   the task while it waits; nothing is committed yet, and a cancel or
   deadline interrupts the wait.
5. Commit `Running`. This means "the request may have gone out": a restart
   that finds a task `Running` counts one unconfirmed call and redoes the
   step.
6. Call the adapter (streaming when it is a `model.Streamer`), under the
   task's context.
7. Settle the reservation, report the outcome to the group (window, breaker),
   then build the task's own commit:
   - **tool calls**: append the assistant turn, write each call record as
     `dispatched`, set the task `Waiting(IO)`. After the commit and the
     hand-back, the same vCore gives the calls to the bus;
   - **final text**: `Done` with `answer` and `done` events (or one more
     model step if a steer arrived during the call). With
     `Config.AnswerCheck` set, the vCore first asks the check about the
     answer (design §9.8), bounded by a timeout and failing open; a
     rejection with a retry left commits the answer, the feedback as a
     user message and `answer_rejected`, and the task is ready for another
     model step; past the retries, or with no step left in its budget, it
     is `Done` anyway, marked `check_failed`. The verdict exists only through that commit, so a
     cancel that lands first wins and a crash before it asks again;
   - **failure**: by error class (design §11): back off in
     `Waiting(Backoff)`, park on the group, or fail.

**`StepTools`** (design §5.2) commits a task's `planned` calls `dispatched`,
then dispatches them. In the current code no path leaves a task ready at
`StepTools` (a model step dispatches its own calls, and recovery and
`Resolve` dispatch through their own settlement), but a store may still hold
one, and this instruction handles it.

**Tool batches** (design §4.2): each call runs on its device's queue in its
own goroutine. A finished call commits `done` with its result and a
`tool_result` event. When every call of the round is done, the batch checks
the task out, appends the results as one tool message in call order, commits
it `Ready` with `PC = StepModel`, and requeues it. A vCore is never held while
tools run.

## The call log

(design §4.2)

A call record is the write-ahead log entry of one tool call. It records, at
planning time, the device, the declared `Effect` and `ReportUnknown`, so
recovery decides by what was declared then, not by whatever the bus says
after a restart. The idempotency key is `task/call/attempt`.

```
              planned ──commit──► dispatched ──result──► done
                                      │
          restart, Effect = SideEffect│        cancel before it reached the device
                                      ▼                 ──► cancelled
                                   unknown ──Resolve──► done (or a new attempt)
                                                     result after a cancel ──► late
```

On restart, per record:

| Status | Recovery |
|---|---|
| `planned` | never left the process: dispatch normally |
| `dispatched`, `ReadOnly` / `Idempotent` | dispatch again with the same key |
| `dispatched`, `SideEffect`, `ReportUnknown` | mark `unknown`, give the model an error result saying the outcome is unknown |
| `dispatched`, `SideEffect` | mark `unknown`, task `Waiting(Reconcile)`, `reconcile_needed` event; nothing in that round is sent until an operator calls `Resolve` |
| `done` | keep the result |

That is the guarantee: every call gets exactly one result; `ReadOnly` and
`Idempotent` calls may run more than once; a `SideEffect` call is dispatched
at most once without an operator. Exactly once in the outside world needs the
device to deduplicate by key.

Cancellation sorts calls the same way (design §9.5): a call that never reached
its device is `cancelled`; one that did is `unknown` with a
`reconcile_needed` event marked `cancelled` (the task does not wait for it);
a result that arrives after the final commit is kept as `late` and never
reaches the model.

## Recovery

(design §4.5)

`Start` runs recovery before any vCore starts, and commits every decision
before anything is scheduled, so a failure part way leaves nothing scheduled
and `Start` can be called again:

1. Load every unfinished TCB with its call records and undelivered interrupts.
2. A task whose class is no longer configured moves to the default group (the
   migration checks apply) or fails with `class_removed`.
3. `Running` becomes `Ready`, with `Unconfirmed + 1`.
4. Call records are settled by the table above.
5. `Waiting(Backoff)` and `Waiting(Group)` become `Ready`, keeping their place
   (`ReadySince`): timers and breakers are not persisted. A committed but
   unfinished cancel is finalised; deadlines are armed again, and passed ones
   fire at once.
6. Ready queues are rebuilt from `Class`, `Priority` and `ReadySince`; windows
   restart at `Initial`; limiters start empty and recalibrate from the first
   responses.

Every commit point is covered by crash tests (`TestCrashAtEveryCommitPoint*`
in `recovery_test.go`): crash before and after each commit, restart on SQLite,
check the invariants. `acceptance/killnine` does the same with a real model
and `SIGKILL`.

## How groups scale

(design §6)

An execution group is one adapter plus elastic vCores. Its congestion window
`W` answers one question: how many requests to this provider at once.

- **Additive increase**: each successful call adds `1/W` (not during a
  cooldown).
- **Multiplicative decrease**: an overload (529, 503 overloaded, a bare 429)
  halves `W`, down to `Min`, and starts a 10s cooldown. Each request records
  the congestion epoch it was sent in; only an overload from the current epoch
  halves and advances it, so a burst of overloads from one batch of in-flight
  requests halves once.
- **vCores follow the window**: a vCore makes at most one request at a time,
  so the group runs at most `floor(W)` of them. A new vCore starts when the
  queue has work, every vCore is busy and there are fewer than `floor(W)`;
  above `floor(W)` the first vCore to finish its instruction retires; one idle
  for `IdleRetire` retires while more than `Min` remain. vCores retire only
  between instructions. `core_up` / `core_down` record each change.
- **Operator**: `SetCores` pins `W` and stops adaptation; `AutoScale`
  resumes it. The one-group shorthand (`Config.Model`) pins `W` at `Cores`.

The **breaker** (design §6.2) opens after `BreakerThreshold` consecutive
transient failures (half-open after `BreakerOpenFor`: one probe request, and
`group_up` on success) or at once on an exhausted quota (stays open until
`GroupReset`). Tasks of an open group park in `Waiting(Group)` lazily, when a
vCore fetches them.

**Migration** (design §6.3): an unpinned task of a down group moves along the
`Fallback` chain to the first group that is up and passes the capability
checks (tools supported, tool count, context fits the target window). The move
commits the new `Class` with the same `ReadySince` and a `migrated` event;
requests to the new provider drop the old provider's reasoning blocks. A
refusal parks the task (`migrate_refused`) or fails it, per `OnMigrateFail`.
`SetClass` is the operator's version of the same move.

**Limiters** (design §7) are separate from groups because quotas are scoped
differently from models. A request needs room in every limiter its group
names, all at once or not at all, first come first served. A reservation is
settled after the call (refund or extra charge), and the provider's rate-limit
headers cap what the limiter believes is left. A 429 with a retry time pauses
the limiter, not just the task.

## Cancellation and deadlines

(design §9.4, §9.5)

A deadline is a cancel whose reason is `deadline`, fired by a scheduler timer
whatever state the task is in. A task nobody is working on (ready, backing
off, parked, waiting for reconciliation) is taken out of the wait set by the
canceller and finalised directly. A task held by a vCore or a tool batch only
receives the signal; its holder finalises it, and `Requeue` refuses a
cancelled task under the scheduler's lock, so a cancel cannot fall between a
commit and a hand-back.

## Reading the code

Start with `neumann.go` (`New`, `Start`, `Submit`, `commit`), then
`internal/core/core.go` (`RunVCore`, `step`), `internal/core/model.go`
(`execModel`) and `internal/core/tools.go` (dispatch and batch completion).
`internal/core/recover.go` is recovery and `Resolve`; `internal/core/cancel.go`
the final commit of a cancelled task. `mem/mem.go` documents every rule a
`CommitSet` must obey. The tests in the root package (`invariants_test.go`,
`recovery_test.go`, `cancel_test.go`, `elastic_test.go`) state the design's
invariants as executable checks; design §18 lists them.
