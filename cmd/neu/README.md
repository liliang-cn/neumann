# neu

`neu` is an assistant for the files of a local workspace, built on
[neumann](../../README.md). Ask it one question, or open a REPL and run
several tasks at once: each line is a task, with priorities, suspend,
resume, steer and cancel. Its tools are confined to the workspace, and
every task is kept in a SQLite store, so an interrupted task continues the
next time you start `neu` there.

## Install

```
go install github.com/liliang-cn/neumann/cmd/neu@latest
```

or download an archive for macOS, Linux or Windows (amd64, arm64) from the
[releases](https://github.com/liliang-cn/neumann/releases) page.
`neu -version` prints the version.

## Configure

Without `-backend`, neu takes the first of these that applies and prints
its choice on stderr (never the key):

1. the config file `~/.config/neu/config.toml` (`$XDG_CONFIG_HOME/neu/config.toml`
   when that is set; `-config` names another), if it sets `backend`;
2. `ANTHROPIC_API_KEY` set: Anthropic at `https://api.anthropic.com/v1`;
3. `OPENAI_API_KEY` set: OpenAI at `https://api.openai.com/v1`, or at
   `OPENAI_BASE_URL` (any OpenAI-compatible server works);
4. Ollama answering at `127.0.0.1:11434`: the local backend.

Otherwise it exits and lists these options.

**There is no default model.** Name it every time, one of: `-model`, `model`
in the config file, or `ANTHROPIC_MODEL` / `OPENAI_MODEL` in the environment.
Without one neu exits and says how to give it.

```
$ ANTHROPIC_MODEL=claude-sonnet-5-5 neu "How many lines does go.mod have?"
neu: anthropic (claude-sonnet-5-5 at https://api.anthropic.com/v1), 8 vCore(s), window 200000 [ANTHROPIC_API_KEY is set]
...
```

### Config file

Every key is optional, and a flag on the command line wins over the file.
`model`, `base_url` and `key_env` apply only when the backend in use is the
file's `backend` (or the file names none), so `neu -backend local` does not
pick up a cloud model from the file. Unknown keys are an error.

```toml
backend  = "openai"                   # local, openai, anthropic or hybrid
base_url = "https://gateway.example/v1"
model    = "some-model"
key_env  = "GATEWAY_KEY"              # the variable holding the key, never the key
cores    = 8                          # tasks running model calls at once
parallel = 1                          # local: requests the server serves at once
writable = ["out", "docs"]            # where fs_write may write
max_steps = 40                        # model calls per task
context_window = 32768                # tokens; see "Context window" below

[hybrid]                              # -backend hybrid
fast_model    = "qwen3.5:4b"
fast_base_url = "http://127.0.0.1:11434/v1"
fast_cores    = 1
fast_context_window = 32768
route         = "static"              # static, decide, fast or strong
router_model  = "tev1:4b"             # route = "decide"
route_timeout = "3s"
route_floor   = 0.2
fallback      = true                  # fast's tasks move to strong while fast is down

[strong]                              # hybrid's cloud group
api      = "anthropic"                # openai or anthropic
model    = "claude-sonnet-5-5"
key_env  = "ANTHROPIC_API_KEY"
cores    = 8
context_window = 200000
```

### Flags and proxies

`-backend`, `-model`, `-base-url` and `-key-env` choose the backend
directly; the key itself is never a flag, so it stays out of `ps` and shell
history. `neu -h` lists every flag.

```
neu -backend local -model qwen3.5:9b-mlx "..."
neu -backend openai -base-url https://gw.example/v1 -model m -key-env GW_KEY "..."
HTTPS_PROXY=http://127.0.0.1:7890 neu -backend anthropic -model claude-haiku-5-5 "..."
```

Cloud calls honour `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY`; the local
Ollama on the loopback address is never proxied.

### Context window, compaction and caching

neu compacts a task when its next request plus the answer's reserve would
pass 80% of the model's context window: the oldest tool rounds are
replaced by a summary (the originals stay in the store). For that it has to
know the window. `-context-window N` (tokens; `context_window` in the file,
`-fast-context-window` / `fast_context_window` and `-strong-context-window`
/ `[strong] context_window` for hybrid's groups) sets it; otherwise:

| backend | window |
|---|---|
| `anthropic` | 200000 |
| `local` | read from Ollama: neu loads the model with the server's defaults (as the first request would) and reads the `context_length` it runs with from `/api/ps`; that is the runtime `num_ctx`, not the model's training maximum (`qwen3.5:4b`: 262144 in `/api/show`, 32768 loaded on a Mac without `OLLAMA_CONTEXT_LENGTH`). When it cannot be read, 4096 (Ollama's smallest default) and a warning |
| `openai` | unknown: compaction waits until the provider rejects a request as too long, and neu says so on start |

The startup line shows the window in use. This matters most for Ollama: it
never rejects a long prompt, it silently drops its start once the prompt
passes `num_ctx`, so a task neu does not compact loses context without any
error. The OpenAI-compatible endpoint cannot set `num_ctx` per request; set
it on the server (`OLLAMA_CONTEXT_LENGTH`) or in a Modelfile (`PARAMETER
num_ctx`), and neu reads whatever results. With a known local window the
answer's reserve is at most a quarter of it (8192 tokens at 32768).

Prompt caching needs nothing from you: the Anthropic adapter places
`cache_control` breakpoints itself, OpenAI-compatible providers and Ollama
cache by prefix, and a task's requests share their prefix from one call
to the next (until a compaction rewrites it). After each task neu prints
one line with what it used, with the cache hit rate when the provider
reports one:

```
[t1a2b3] 7 calls, 41.2k in (83% cached), 1.1k out, 12.3s, compacted 1×
```

`in` counts every prompt token (cached ones included), `out` every output
token (reasoning, shown in brackets when reported, included); `compacted
N×` counts compaction calls.

## One question

```
neu "Which files mention preemption?"
```

Tool calls stream on stderr, the answer is printed on stdout, the usage
line follows on stderr, and the exit status is 0 when the task finished. Ctrl-C leaves the task in the store; it
continues the next time neu starts in that workspace.

## REPL

`neu` with no question reads one task per line and prints each answer as
its task finishes, prefixed by the task's id and followed by its usage
line.

| command | |
|---|---|
| `<text>` | a task at priority 0 (interactive) |
| `/bg <text>` | a task at priority 2 (background) |
| `/pri <id> <n>` | change a task's priority (0 is the most urgent) |
| `/suspend <id>`, `/resume <id>` | take a task off the machine, put it back |
| `/steer <id> <text>` | add a message to a running task |
| `/cancel <id>` | cancel a task |
| `/status` | vCores, queues, groups and breakers, and the session's usage |
| `/usage` | the session's model calls and tokens, running tasks included |
| `/tasks` | unfinished tasks |
| `/wait <id>` | block until a task ends and print its answer |
| `/fast <text>`, `/strong <text>` | hybrid: send a task to one group |
| `/quit` | stop; unfinished tasks continue on the next start |

## Backends

| `-backend` | |
|---|---|
| `anthropic` | the Messages API; 8 vCores |
| `openai` | any OpenAI-compatible endpoint; 8 vCores |
| `local` | Ollama at `http://127.0.0.1:11434/v1` (`-model` names the Ollama model): one request at a time (`-parallel N` for a server that serves more), preemption of background work by interactive tasks, continuation of a cut-off answer, thinking off |
| `hybrid` | two execution groups: `fast` on the local model and `strong` on a cloud model (`-strong-api openai` or `anthropic`); `-route` picks each task's group |

**Hybrid.** `-route static` sends REPL and one-shot requests to `fast`
(`/strong <text>` overrides); `-route decide` asks a small decision model on
Ollama (`tev1:4b`) to choose, and a slow or unsure decision goes to
`strong`; `-route fast` and `-route strong` send everything to one group.
While the local group is down, its tasks move to `strong` (`-fallback=false`
turns that off). The flags are `-fast-model`, `-fast-base-url`,
`-fast-cores`, `-strong-api`, `-strong-model`, `-strong-base-url`,
`-strong-key-env`, `-strong-cores`, `-route`, `-router-url`,
`-router-model`, `-route-timeout` and `-route-floor`.

```
neu -backend hybrid -strong-api anthropic                  # REPL; /strong <text> for the cloud
neu -backend hybrid -route decide "How many lines does data/sales.csv have?"
```

## Tools and the sandbox

Every path is relative to the workspace root (`-root`, default `.`);
anything that leaves it, through `..`, an absolute path or a symlink, is
refused.

| tool | | |
|---|---|---|
| `fs_list` | read-only | a tree with sizes |
| `fs_read` | read-only | numbered lines, up to 400 per call |
| `fs_grep` | read-only | `file:line: text` for up to 100 matches, and the total |
| `git_run` | read-only | `log show diff status blame rev-parse ls-files`, no shell |
| `go_test` | idempotent | `go test -count=1 ./...` with a timeout |
| `calc_eval` | read-only | exact arithmetic, `sum avg min max abs round` |
| `fs_write` | side effect | only under the writable directories; each write is recorded in a ledger and applied once |

By default neu may write only under `out/` (created on start). Allow more
with `-writable out,docs` or `writable = [...]` in the config file;
`-writable .` allows the whole workspace.

## State

The store is `<root>/.neu/state.db` (`-store` puts it elsewhere). It holds
every task's history, so add `.neu/` to your `.gitignore`. A task that was
running when neu stopped continues on the next start with the same store.

## Observability

`-observe-addr 127.0.0.1:43562` serves `/status`, `/queue`, `/tasks/{id}`,
`/events` and `/metrics`; `-otlp` exports traces and metrics over OTLP/HTTP
as the standard `OTEL_EXPORTER_OTLP_*` variables say; `-v` logs the
machine's own lines on stderr.

## Examples

```
neu "Summarise the last ten commits"
neu "Run the tests in ./parser and tell me which fail and why"
neu -writable out "Write a CHANGELOG since v0.3.0 to out/CHANGELOG.md"
neu                                    # then: /bg review the diff of HEAD~1 for locking bugs
```
