# Examples

Each example is a `main` package; run it from the repository root.

| Example | What it shows | Run |
|---|---|---|
| [hello](hello/main.go) | The smallest complete program, offline: a scripted model calls two read-only tools in parallel and one side-effect tool, events stream, the call log is printed. Start here. | `go run ./examples/hello` |
| [skills](skills/main.go) | A skills device serving skill directories by progressive disclosure, driven by a scripted model, offline. | `go run ./examples/skills` |
| [mcp](mcp/main.go) | An MCP server (this same binary, over stdio) attached as a bus device. Offline with a scripted model; uses a real OpenAI-compatible model when `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL` are set. | `go run ./examples/mcp` |
| [live](live/main.go) | Six concurrent tasks against a real OpenAI-compatible endpoint: parallel tool calls, a slow tool, a steer while it runs, streamed output; checks the invariants at the end. `NEUMANN_DB=path` keeps state in SQLite. | `OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/live` |
| [netbench](netbench/main.go) | What the wire protocol costs against a real endpoint (cold connection, warm round trips, a burst, tiny completions), separate from inference. | `OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/netbench` |

`live` and `netbench` also read `LLM_BASE_URL`, `LLM_API_KEY` and `LLM_MODEL`
when the `OPENAI_` variables are unset. Keep keys in the environment, never in
source.

<!-- PENDING: add examples/observe once the observe branch has merged. -->
