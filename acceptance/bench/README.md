# nm

`nm` is a small assistant for a local workspace, built on neumann: one
process, one machine with a SQLite store, and tools confined to a workspace
root. It is also the test vehicle for measuring neumann on realistic work:
`nm bench` runs a suite of auto-checked tasks in several scenarios and
reports what the machine did, from its committed event stream.

```
go build -o nm ./examples/nm
```

## Backends

| flag | what it uses |
|---|---|
| `-backend openai` (default; also `cloud`) | any OpenAI-compatible endpoint; defaults `CPA_BASE_URL`, `CPA_MODEL`, key in `$CPA_API_KEY`; 8 vCores |
| `-backend anthropic` | Anthropic's Messages API; defaults `https://api.anthropic.com/v1`, `claude-sonnet-5-5`, key in `$ANTHROPIC_API_KEY`; 8 vCores |
| `-backend local` | Ollama at `http://127.0.0.1:11434/v1`, `qwen3.5:9b-mlx`, configured like `cmd/neumann -local`: one request at a time and as many vCores (`-parallel N` for a backend that serves more, e.g. a GGUF model with `OLLAMA_NUM_PARALLEL`), preemption, continuation of a cut-off answer or thought, `reasoning_effort: "none"` to turn thinking off, 10 min to the first byte, 2 min between chunks, no total limit, 8192 output tokens |
| `-backend hybrid` | two execution groups: `fast` is the local backend on `qwen3.5:4b` (configured exactly like `-backend local`, `-parallel` vCores, preemption on) and `strong` is an openai or anthropic backend (`-strong-api`, 8 vCores, no preemption); `-route` picks each task's group |

Every backend takes its endpoint, model and key from flags: `-base-url`, `-model`
and `-key-env`, the name of the environment variable holding the key (the key itself
is never a flag, so it stays out of `ps` and shell history; local's default is
`NM_API_KEY`, unset: `ollama`). `NM_BASE_URL`, `NM_MODEL` and `NM_KEY_ENV` set the
same. Hybrid's strong group has `-strong-api`, `-strong-base-url`, `-strong-model` and
`-strong-key-env`. `-parallel` and `-cores` override the rest. `HTTPS_PROXY` is honoured.

```
nm -backend openai -base-url https://gw.example/v1 -model some-model -key-env GW_KEY "..."
HTTPS_PROXY=http://127.0.0.1:7890 nm -backend anthropic -model claude-haiku-5-5 "..."
nm -backend hybrid -strong-api anthropic -route static "..."
```

### Hybrid: a fast local group and a strong cloud group

```
nm -backend hybrid                                 # REPL; a line goes to fast, /strong <text> to the cloud
nm -backend hybrid -route decide "How many lines does data/sales.csv have?"
nm bench -backend hybrid -route static -out hybrid-static-solo.json
```

| flag | default | |
|---|---|---|
| `-fast-model`, `-fast-base-url`, `-fast-cores` | `qwen3.5:4b`, Ollama, `-parallel` | the local group |
| `-strong-model`, `-strong-base-url`, `-strong-cores` | `$CPA_MODEL`, `$CPA_BASE_URL`, 8 | the cloud group (`CPA_API_KEY`) |
| `-route` | `static` | `static`, `decide`, `fast` or `strong` |
| `-router-url`, `-router-model` | Ollama, `tev1:4b` | `-route decide`'s System One engine |
| `-route-timeout`, `-route-floor` | 3s, 0.2 | a decision that takes longer, or is less sure, sends the task to strong |
| `-fallback` | `true` | fast's tasks move to strong while fast is down |

Each group's window is pinned at its vCores once the machine starts, as
the one-group backends pin theirs; `-model`, `-base-url` and `-cores` are
refused with `-backend hybrid`.

**Routing.** The router runs in nm, before `Submit`: the task's class is
set on the instruction, and the `accepted` event records it.

- `static`: the bench routes by task kind, from what the 4B model was
  measured to do (same suite, each model alone): it passed every quick
  probe and repo question, `fix-bug` (2 of 2) and `code-review` (1 of 1),
  so those run on `fast`; it failed `log-triage` (wrong root cause),
  `csv-stats` (never summed the column) and `changelog` (skipped a
  commit), so those run on `strong`. In the REPL and one-shot mode a
  request goes to `fast`; `/strong <text>` and `/fast <text>` override.
- `decide`: `decide.Router` asks `tev1:4b` on Ollama's System One API to
  choose between `fast` ("short lookups, one or two files, quick
  questions") and `strong` ("reading large data, counting, multi-step
  analysis, writing summaries of many items"). An engine that fails,
  times out, or is less sure than `-route-floor` sends the task to
  `strong`: a wrong pick toward the cloud costs tokens, one toward the
  small model costs the answer. The engine is loaded at start (a cold load
  takes seconds); it is another Ollama model, so it shares the GPU and
  memory with the fast group's model.
- `fast`, `strong`: everything to one group (baselines on the same
  machine).

The REPL and one-shot mode print each task's group and why
(`→ strong (decide p=0.24, 412ms)`); `/status` shows both groups: model,
vCores, window, queue, breaker and fallback. Answers say which group
finished the task.

**Fallback.** `fast` falls back to `strong` (`-fallback=false` turns it
off); `strong` has none, so a cloud task waits for the cloud to come back
instead of landing on a model that fails long analyses. While fast's
breaker is open (5 consecutive transient failures: Ollama not running, a
connection refused, a stream that breaks), its tasks that are not
`PinClass` move to `strong` with a `migrated` event (`reason:
group_down`), keep their place in the queue, and their next model step
runs on the cloud. The capability checks of design §6.3 pass: the cloud
adapter takes tools (nm has 7, under its limit of 128) and reports no
context window, so no context check can refuse. A continuation prefix the
local model left (after a preemption or a broken stream) is dropped: the
cloud adapter does not continue an assistant message, so that step starts
afresh there. Tasks stay on `strong` after fast comes back (migration is
one way; `SetClass` would move them back). A local backend that is slow
rather than down (a long first byte, a model loading) does not open the
breaker and moves nothing.

## Tools

All paths are relative to `-root` (default `.`); anything that leaves it,
through `..`, an absolute path or a symlink, is refused.

| tool | effect | |
|---|---|---|
| `fs_list {path, depth}` | read-only | a tree with sizes |
| `fs_read {path, offset_line, limit_lines}` | read-only | numbered lines, at most 400 per call, says how many the file has |
| `fs_grep {pattern, path, glob, ignore_case, count_only}` | read-only | `file:line: text` for up to 100 matches, always the total |
| `git_run {dir, args}` | read-only | `log show diff status blame rev-parse ls-files`, no shell, no `--output`/`--no-index`/`--ext-diff`, no repository above the root |
| `go_test {dir, run}` | idempotent | `go test -count=1 ./...` with a timeout |
| `calc_eval {expr}` | read-only | exact arithmetic, `sum avg min max abs round` |
| `fs_write {path, content}` | side effect | only under `-writable` (default `out`); idempotent by the call's key, every write recorded in `out/.ledger` |

## Modes

```
nm "Which files mention preemption?"     # one question; tool calls stream on stderr, the answer on stdout
nm                                       # REPL: each line is a task at priority 0
```

REPL commands: `/bg <text>` (priority 2), `/pri <id> <n>`, `/suspend <id>`,
`/resume <id>`, `/steer <id> <text>`, `/cancel <id>`, `/status`, `/tasks`,
`/wait <id>`, `/quit`. Answers print as tasks finish, prefixed by their id.
The store is `./.nm/state.db` (`-store`): an interrupted task continues on
the next start.

`-observe-addr 127.0.0.1:43562` serves the observe view (`/status`,
`/queue`, `/tasks/{id}`, `/events`, `/metrics`); `-otlp` exports traces and
metrics over OTLP/HTTP as the standard `OTEL_EXPORTER_OTLP_*` variables say.

## Bench

```
nm bench -scenario solo  -backend cloud -out solo-cloud.json
nm bench -scenario solo  -backend local -tasks repo-qa,quick,fix-bug -out solo-local.json
nm bench -scenario mixed -backend local -bg repo-qa.cores,code-review -probes 3 -every 60s -baseline solo-local.json -out mixed-local.json
nm bench -scenario ops   -backend cloud -out ops.json
nm bench -scenario crash -backend cloud -seed 3 -out crash.json
nm bench -scenario crash -backend cloud -kill-on fs_write -out crash-write.json
nm bench -scenario burst -backend cloud -k 30 -out burst.json
nm bench -rejudge solo-local.json            # run the checkers again on a report's answers and files
```

Run it from the repository root (or pass `-repo`): the workspace gets a
`git archive` of tag `v0.3.0`. Everything else is generated or copied from
`testdata/` into `-work` (default: a new directory under `$TMPDIR`), with
the store in `<work>/.nm/<scenario>.db`. `nm bench -help` lists the tasks.

### Tasks

| id | what the user asks | checked by |
|---|---|---|
| `repo-qa.preempt` | where neumann decides to preempt a model call (file:line) | `internal/core/preempt.go`, `preemptCheck`, a line in 138–207 |
| `repo-qa.cores` | the default number of vCores and where it is defined | 4, `neumann.go:36–44` |
| `repo-qa.warn` | which events `observe/log.go` logs at warn | all eight kinds |
| `log-triage` | root cause, first error time, affected endpoints and failed requests in a 4k-line log | pool exhaustion, `14:07:31`, the three endpoints, the exact count |
| `csv-stats` | top region by Q3 revenue and its total | exact region and amount |
| `code-review` | review a diff with a lock left held on one return path | file, function and the not-found branch |
| `fix-bug` | fix an off-by-one so `go test` passes | the test file untouched, a write in the ledger, `go test` passes |
| `changelog` | changelog since a tag, grouped by type, written to a file | every commit covered, features and fixes under the right headings |
| `quick.*` | short questions (line count, columns, where is X, a percentage, the root's entries) | exact values |

### Scenarios

- `solo`: each task alone, one after another.
- `mixed`: the background tasks (`-bg`, default every non-quick task) at
  priority 2, then a quick probe at priority 0 every `-every`; probe latency
  against its solo latency (`-baseline` report, or the probes run solo
  first), preemptions, background completion.
- `ops`: three tasks at once. `log-triage` is suspended after its first
  streamed piece and resumed after `-suspend-for`; `csv-stats` is steered
  ("also tell me how many Q3 orders") after its first tool result;
  `changelog` is cancelled at its first tool call. Each must end as asked:
  passed with no model call while suspended, the extra question answered,
  cancelled with no tool call or write after.
- `crash`: the batch runs in a child process that is killed with SIGKILL
  after `-kill-after` tool results (default 3–10 from `-seed`), or as soon as
  a call of the `-kill-on` tool is dispatched, then started
  again on the same store. Calls of `fs_write` whose outcome is unknown are
  resolved from the write ledger (applied: its result; not applied: retry).
  Every task must finish and no key may be applied twice.
- `burst`: `-k` tasks of the mix at once; throughput, latency, retries,
  machine events and the window and breaker sampled each second.

With `-backend hybrid` every scenario routes each task as `-route` says
(the bench's static rule is by task kind, above). In `mixed`, a probe's
routing decision is part of what the user waits for: `probe_e2e_*` adds
it to the probe's wall time.

### What is measured

Per task, from the committed events: wall time, queue time (ready but not
running: from the commit that made the task ready to its next `model_call`),
model time (`model_call` → the step's commit), tool time (the dispatching
commit → the round's last result), suspended time, model calls, answered
steps, tool calls and rounds, tokens (prompt, cached with the cache hit
rate, completion, reasoning), each step's `first_ms` (only steps that
streamed text or reasoning have one), retries (their backoff counts as
queue time), preemptions. The adapter is wrapped to time its
own calls (`adapter s`) and the devices time theirs (`device s`, per round
the longest call): `overhead ms` = wall − adapter − device − queue −
suspended, the time the framework added around the calls (commits,
dispatch, wake-ups). It is shown only when every model call of the task
could be matched to its adapter call.

Per execution group (`per class:` in the output, `summary.classes` in the
report): tasks accepted in the class, how many passed, their wall time,
and the model calls and tokens that ran in it (a migrated task's calls
after the move count in the class it moved to). A cloud class's tokens are
the run's cost: `cloud_prompt_tokens`, `cloud_cached_tokens`,
`cloud_completion_tokens`, and `cloud_tokens_per_passed_task` (cloud
prompt + completion tokens over every passed task, local or cloud). Each
result records its route (`route`: class, how it was picked, the engine's
label and confidence) and `route_ms`; the summary has the picks and the
router's latency (`route_ms_p50`, `_p95`, `_max`), and `migrations`.
