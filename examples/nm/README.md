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
| `-backend cloud` (default) | `CPA_BASE_URL`, `CPA_API_KEY`, `CPA_MODEL`; 8 vCores |
| `-backend local` | Ollama at `http://127.0.0.1:11434/v1`, `qwen3.5:9b-mlx`, configured like `cmd/neumann -local`: one request at a time, 1 vCore, preemption, continuation of a cut-off answer or thought, `reasoning_effort: "none"` to turn thinking off, 10 min to the first byte, 2 min between chunks, no total limit, 8192 output tokens |

`-model`, `-base-url` and `-cores` override. The key is read from the
environment only (`NM_API_KEY` for local, default `ollama`) and never printed.

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
