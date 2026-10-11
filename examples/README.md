# Examples

Each example is a `main` package; run it from the repository root.

| Example | What it shows | Run |
|---|---|---|
| [hello](hello/main.go) | The smallest complete program, offline: a scripted model calls two read-only tools in parallel and one side-effect tool, events stream, the call log is printed. Start here. | `go run ./examples/hello` |
| [approval](approval/main.go) | A tool policy that allows reads, denies transfers to unknown accounts and holds the rest for a person; an operator approves one transfer and edits another through `Machine.Approve`, offline. | `go run ./examples/approval` |
| [decide](decide/main.go) | A decision engine as a coprocessor: a `Router` sends requests to a "fast" or a "strong" group, a `Policy` holds a note deletion for approval and lets a read through, a `Checker` catches an answer that only promises the work and the request is submitted again with its feedback. Offline with a scripted engine; `-live` asks tev1:4b on a local Ollama (`-url`, `-model`, `-key` or `NEUMANN_DECIDE_KEY` for a relay). | `go run ./examples/decide` |
| [messaging](messaging/main.go) | Two tasks talking through the `tasks` device, offline: one asks the other a question with `tasks_send` while it works, the other answers the same way; each message arrives as `[message from task:<id>] …` and each delivery is a `steer_delivered` event with `from`. | `go run ./examples/messaging` |
| [skills](skills/main.go) | A skills device serving skill directories by progressive disclosure, driven by a scripted model, offline. | `go run ./examples/skills` |
| [mcp](mcp/main.go) | An MCP server (this same binary, over stdio) attached as a bus device. Offline with a scripted model; uses a real OpenAI-compatible model when `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` are set. | `go run ./examples/mcp` |
| [observe](observe/main.go) | The `observe` package on an offline machine: the HTTP view (status, queue, tasks, SSE events, Prometheus metrics, pprof), the activity log, and traces printed as a span tree. `-serve` keeps it up; it listens on a free port unless `-addr` is given. | `go run ./examples/observe` |
| [live](live/main.go) | Six concurrent tasks against a real OpenAI-compatible endpoint: parallel tool calls, a slow tool, a steer while it runs, streamed output; checks the invariants at the end. `NEUMANN_DB=path` keeps state in SQLite. | `OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/live` |
| [netbench](netbench/main.go) | What the wire protocol costs against a real endpoint (cold connection, warm round trips, a burst, tiny completions), separate from inference. | `OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/netbench` |

`live` and `netbench` also read `LLM_BASE_URL`, `LLM_API_KEY` and `LLM_MODEL`
when the `OPENAI_` variables are unset. Keep keys in the environment, never in
source.
