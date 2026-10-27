# neu

`neu` is an assistant for the files of a local workspace, and a coding
assistant: it reads, edits, deletes, runs commands and fetches web pages,
built on [neumann](../../README.md). Ask it one question, or open a REPL
and run several tasks at once: each line is a task, with priorities,
suspend, resume, steer and cancel. Its file tools are confined to the
workspace, anything with a side effect outside `-writable` waits for your
approval (see [Approvals](#approvals)), and every task is kept in a SQLite
store, so an interrupted task continues the next time you start `neu`
there.

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
writable = ["out", "docs"]            # changed without asking, whatever -approve says
max_steps = 40                        # model calls per task
context_window = 32768                # tokens; see "Context window" below
approve  = "ask"                      # ask, auto or never; see "Approvals"
sh_timeout = "10m"                    # the longest one sh_run call may run
sh_shell = true                       # sh_run may run bash -lc scripts
sh_deny_env = ["AWS_*", "GH_TOKEN"]   # never passed to sh_run's commands

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

`[[mcp]]` entries and `skills_dir` are described under
[MCP servers](#mcp-servers) and [Skills](#skills).

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

Tool calls show on stderr as they happen, the answer streams on stdout as the
model writes it, the usage line follows on stderr, and the exit status is 0 when
the task finished. Ctrl-C leaves the task in the store; it continues the next
time neu starts in that workspace.

`-no-stream` prints the answer only when the task ends; `-think` also streams
the model's reasoning on stderr (when the provider sends it: Anthropic's
current models send a summary or nothing). In the REPL the newest interactive
task streams; `/bg` tasks and older ones print when they end. If a model call
is retried, neu says so and the answer starts again.

How smooth the stream is depends on the provider. Anthropic's API, measured
from this side with plain curl, sends the text of a turn that follows a tool
result in one burst at the end, while a turn without tool results streams
token by token; OpenAI-compatible endpoints and Ollama stream either way.

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
| `/tools` | every tool the model can call, its effect and its device |
| `/skills` | the skills found, and those skipped and why |
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
refused. No tool writes into `.neu/` or `.git/` (at any depth), the store,
or the change ledger.

| tool | effect | |
|---|---|---|
| `fs_list` | read-only | a tree with sizes |
| `fs_read` | read-only | numbered lines, up to 400 per call |
| `fs_grep` | read-only | `file:line: text` for up to 100 matches, and the total |
| `git_run` | read-only | `log show diff status blame rev-parse ls-files`, no shell |
| `go_test` | idempotent | `go test -count=1 ./...` with a timeout |
| `calc_eval` | read-only | exact arithmetic, `sum avg min max abs round` |
| `fs_write` | side effect | create or replace a file; returns a diff against what it replaced |
| `fs_edit` | side effect | replace exact text in a file: `old_string` must match exactly and once (or `replace_all`); "not found" and "matches N times" come back as errors with a hint; returns a compact unified diff |
| `fs_delete` | side effect | delete a file (not a directory, not through a symlink) |
| `sh_run` | side effect | run a command in a workspace directory: `args` (no shell) or `script` (`bash -lc`); exit status, output capped to its start and end, timeout 2m by default |
| `web_fetch` | read-only | GET an http(s) URL, HTML as readable text with links as `text (url)`, the final URL after at most 5 redirects |

`fs_write`, `fs_edit` and `fs_delete` change files under `-writable`
(default `out/`, created on start) without asking, and anywhere else in the
workspace when the approval mode lets them (`-approve never`: only under
`-writable`, as before). Each change is recorded in a ledger
(`out/.ledger`, or `.neu/.ledger` without a writable directory) by its
call: an intent line with the file's hash before and after, then an
applied line. A call sent again after a crash, under the same key or as a
new attempt, is recognised and not applied twice, even when the crash fell
between the change and its applied line; neu resends such calls itself
when it restarts, so the task does not wait for an operator.

### sh_run

`args` runs a program directly: `{"args": ["go", "test", "./..."], "dir":
"parser"}`. `script` runs a bash command line with `bash -lc`, for pipes
and `&&` (`-sh-shell=false` or `sh_shell = false` turns it off). The
command gets no input, runs in its own process group (a timeout kills its
children too), and its output is kept to the first 8 KiB and the last
16 KiB. A call may ask for `timeout_sec` up to `-sh-timeout` (default 10m).
Its environment is neu's, minus the backends' key variables
(`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `NEU_API_KEY`, `CPA_API_KEY` and
any configured `key_env`) and whatever `-sh-deny-env` / `sh_deny_env`
names (`NAME` or `PREFIX*`).

A short list of read-only commands runs without asking in ask mode: `ls
cat head tail wc grep rg find pwd stat du tree which`, `go build vet test
list env version doc`, read-only `git` (`status log show diff blame
rev-parse ls-files grep describe shortlog cat-file ls-tree rev-list
reflog`) and `make -n`, each with the arguments that would write, delete
or run something else refused (`find -delete/-exec`, `go build -o`, `go
test -exec/-coverprofile`, `git -c ...`, `git diff --output`, `rg --pre`,
...). Everything else, and every `script`, needs approval.

**sh_run is not a sandbox.** An approved command runs with your user's
rights, on your whole machine, not just the workspace; the read-only list
only spares you prompts for commands that look harmless (`go test` runs the
package's test code, for one). You are the security boundary: read what
you approve, and use `-approve auto` only in a workspace you can throw
away or restore (a git checkout).

### web_fetch

`web_fetch` takes `url` and `max_bytes` (default 20 KB of text, at most
48 KB). It follows at most 5 redirects, honours `HTTPS_PROXY` /
`HTTP_PROXY` / `NO_PROXY`, times out after 30s, reads at most 4 MiB, and
returns text only (HTML, plain text, JSON, XML). It refuses `localhost`,
loopback, private, link-local and CGNAT addresses (checked for every
redirect, and for the address a direct connection lands on) unless
`-web-allow-private`.

## Approvals

Every tool call goes through neu's tool policy before it runs (neumann's
`ToolPolicy`; a held call waits in `Waiting(Approval)` and is answered with
`Machine.Approve`, design §9.7). A call that only reads, or changes a file
under `-writable`, always runs. For everything else `-approve` decides:

| `-approve` | side effects outside `-writable` |
|---|---|
| `ask` (default) | held until you answer on the terminal |
| `auto` | run |
| `never` (also `read-only`) | refused: the model gets an error saying so |

This covers every device, not only neu's own tools: a tool whose effect is
read-only or idempotent runs (skills, `tasks_list`, `tasks_send`, an MCP
tool declared `read_only` or `idempotent`); an MCP tool with a side effect
(the default for MCP) is held in ask mode, runs in auto, and is refused in
never.

In ask mode a held call is shown on stderr with what it would do: the
path and a diff for `fs_edit` / `fs_write`, the command and its directory
for `sh_run`, the arguments for anything else. Answer with

| answer | |
|---|---|
| `y` | run it |
| `n`, or `n <reason>` | refuse it; the model gets an error result (with your reason) and adjusts |
| `a` | run it, and every later call of that tool this session without asking |

Calls held at the same time (a round with several calls, or several REPL
tasks) are asked one at a time, and while a prompt waits nothing else is
printed; streamed text and progress lines that arrived meanwhile follow
once you answer. In the REPL the line you type after a prompt is its
answer, not a task. A call still held when neu stops is asked again on the
next start (or run with `-approve auto`, refused with `never`).

A one-shot run whose stdin is not a terminal (a pipe, CI) has nobody to
ask: in ask mode a call that would be held is refused, with a message that
`-approve auto` allows it.

## MCP servers

Each `[[mcp]]` entry of the config file is a Model Context Protocol server,
started with neu and stopped when it exits. Its tools appear as
`<name>_<tool>` (`time_get_current_time`). A server that does not start
within `startup_timeout` (default 30s), or whose tool names collide with
another device's, is left out with one warning and neu goes on. `-no-mcp`
starts none of them; `/tools` lists what is attached. An MCP tool is not
confined to the workspace: it does whatever its server does.

```toml
[[mcp]]                                   # over stdio
name    = "time"                          # letters, digits, '-'
command = "uvx"
args    = ["mcp-server-time", "--local-timezone", "Europe/Vienna"]
env     = { TZ = "Europe/Vienna" }        # $VAR / ${VAR} are expanded from neu's environment
read_only = ["get_current_time", "convert_time"]

[[mcp]]                                   # over streamable HTTP
name    = "docs"
url     = "https://mcp.example/mcp"
headers = { Authorization = "Bearer ${DOCS_TOKEN}" }
effect  = "annotations"                   # trust the server's readOnlyHint / idempotentHint
side_effect = ["publish"]
enabled = false                           # keep the entry, do not start it
startup_timeout = "1m"
```

**Effects.** A tool's effect decides whether a call is sent again after a
crash, and how an approval policy treats it. It is the list that names the
tool (`read_only`, `idempotent`, `side_effect`), else the entry's `effect`:
`side_effect` (the default), `idempotent`, `read_only`, or `annotations`,
which takes the server's own hints. So an MCP tool is a side effect unless
the entry says otherwise: the hints come from the server and are not
trusted by default. A name in a list that the server does not have is a
warning.

**Environment.** A stdio server gets neu's environment except the backend's
keys: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `NEU_API_KEY` and the
configured `key_env` variables are set empty for it, unless its `env` names
them (`env = { OPENAI_API_KEY = "${OPENAI_API_KEY}" }`). Its stderr goes to
`mcp-<name>.log` next to the store, which the warning names when it fails.

## Skills

A skill is a directory with a `SKILL.md`: YAML frontmatter (`name`,
`description`, optionally `when_to_use`) and markdown instructions, plus
any files they refer to. neu reads them from `<root>/.neu/skills` and
`~/.config/neu/skills` (`$XDG_CONFIG_HOME/neu/skills`), those that exist,
the workspace's winning a name; `-skills dir1,dir2` or `skills_dir = "dir"`
(or a list) in the config file replaces those. A relative directory is
relative to the workspace root, and `~` is the home directory.

The catalogue of names and descriptions goes into the system prompt; the
model loads a skill's instructions when a task calls for one (`skills_load`)
and reads its files only when the instructions point to them
(`skills_read`); all three tools are read-only and execute nothing. `/skills`
lists the skills, and those skipped (a bad frontmatter, a duplicate name)
with the reason.

```
$ cat .neu/skills/word-count/SKILL.md
---
name: word-count
description: Count the words of a file of the workspace and report it in the house format.
---
1. Read the file with fs_read.
2. Answer exactly: `WC[<file>] = <n> words`.
$ neu "Use the word-count skill on notes.txt."
neu: 1 skill
[teef16] → skills_load {"name": "word-count"}
[teef16] → fs_read {"path": "notes.txt"}
WC[notes.txt] = 7 words
```

## Task messages

The tasks of one neu process can message each other: `tasks_list` shows
the other unfinished tasks (id, state, the start of the request), and
`tasks_send {task_id, text}` delivers a message to one at its next model
step, marked as coming from `task:<sender>` and not from the user. A task
sees the tasks of its own execution group (with `-backend hybrid`, fast
and strong tasks do not see each other). A message can reach only a task
that is still working; the REPL shows each delivery:

```
> Find every Go file under bus/ whose text mentions SideEffect, and say what each uses it for. If another task sends you a message while you work, quote it at the end of your answer.
[tf5dbb] submitted (priority 0)
> /bg Use tasks_list to find the other task. Count the files directly under bus/tasks with fs_list and send that count to that task with tasks_send. Then answer 'reported'.
[tf5dc0] submitted (priority 2)
[tf5dc0] → tasks_list {}
[tf5dc0] → fs_list {"depth": 1,"path": "bus/tasks"}
[tf5dbb] → fs_grep {"glob": "*.go","pattern": "SideEffect","path": "bus"}
[tf5dc0] → tasks_send {"task_id": "tf5dbb","text": "3"}
[tf5dc0] done:
reported
[tf5dbb] message from task:tf5dc0 delivered
[tf5dbb] → fs_read {"limit_lines": 50,"path": "bus/bus.go","offset_line": 15}
...
[tf5dbb] done:
The following 8 Go files under `bus/` mention `SideEffect`: ...
Message received from task `tf5dc0` during execution:
> 3
```

`tasks_send` is idempotent (a call sent again after a restart is not
delivered twice), and one task may send at most 32 messages.

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
neu "The tests in ./parser fail; fix the code and rerun them"   # asks before each edit
neu -approve auto "Rename Foo to Bar across the module and run go vet"
neu                                    # then: /bg review the diff of HEAD~1 for locking bugs
```
