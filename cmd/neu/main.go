// Command neu is a workspace and coding assistant built on neumann: one
// process, one machine with a SQLite store, and tools confined to a
// workspace root (fs_list, fs_read, fs_grep, fs_write, fs_edit, fs_delete,
// git_run, go_test, sh_run, web_fetch, calc_eval), plus the config file's
// MCP servers ([[mcp]]), skills (<root>/.neu/skills, ~/.config/neu/skills)
// and messages between tasks (tasks_send, tasks_list). Side effects outside
// -writable wait for the user's approval (-approve ask), run (auto) or are
// refused (never).
//
//	neu "Which files mention preemption?"    one question, then exit
//	neu                                       REPL: each line is a task
//
// Without -backend, the backend comes from the config file
// ($XDG_CONFIG_HOME/neu/config.toml or ~/.config/neu/config.toml), else
// ANTHROPIC_API_KEY, else OPENAI_API_KEY, else a local Ollama. Keys come
// from the environment only and are never printed. Unfinished tasks
// continue after a restart (the store is <root>/.neu/state.db).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// version is set at build time (-ldflags "-X main.version=v0.3.2"); empty:
// the module version from the build info.
var version string

func main() {
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// Version is neu's version: the -ldflags one, else the module's (go
// install ...@v0.3.2), else "devel".
func Version() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "devel"
}

const usageHead = `usage: neu [flags] [question]

With a question: answer it and exit (tool calls on stderr, the answer on
stdout). Without: a REPL, one task per line (/help lists its commands).

Backend, when -backend is not given, the first that applies:
  1. the config file %s (-config), key "backend"
  2. ANTHROPIC_API_KEY set: anthropic at %s
  3. OPENAI_API_KEY set: openai at OPENAI_BASE_URL or %s
  4. Ollama answering at %s: local
There is no default model: give it with -model, model in the config file,
or OPENAI_MODEL / ANTHROPIC_MODEL. Flags override the config file.
HTTPS_PROXY is honoured.

`

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cfg := app.Config{Def: app.UserDefaults()}
	fs := flag.NewFlagSet("neu", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg.Flags(fs)
	cfg.CodingFlags(fs)
	verbose := fs.Bool("v", false, "log the machine's own lines on stderr")
	showVersion := fs.Bool("version", false, "print the version and exit")
	noStream := fs.Bool("no-stream", false, "print each answer when its task ends instead of as the model writes it")
	think := fs.Bool("think", false, "also stream the model's reasoning (on stderr)")
	cfgPath := fs.String("config", DefaultConfigPath(), "config file (TOML); a missing file is not an error")
	var bf busFlags
	bf.register(fs)
	fs.Usage = func() {
		fmt.Fprintf(stderr, usageHead, DefaultConfigPath(), app.AnthropicBaseURL, app.OpenAIBaseURL, OllamaURL)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "neu %s\n", Version())
		return 0
	}
	cfg.MarkSet(fs)
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	src, err := Setup(ctx, &cfg, set, Env{ConfigPath: *cfgPath, OllamaURL: OllamaURL})
	if err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	cfg.Quiet = !*verbose
	if err := cfg.Resolve(); err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	q := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if q != "" && cfg.Coding.Approve == app.ApproveAsk && !stdinIsTerminal(stdin) {
		cfg.Coding.NoPrompt = noPrompt
	}
	cfg.DetectWindows(ctx, func(s string) { fmt.Fprintf(stderr, "neu: %s\n", s) })
	fmt.Fprintf(stderr, "neu: %s; approve %s [%s]\n", cfg.Describe(), cfg.Coding.Approve, src)
	file, err := LoadFile(*cfgPath) // Setup read it already: no error now
	if err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	ex := bf.extras(file, cfg.Root)
	ex.Warnf = func(f string, args ...any) { fmt.Fprintf(stderr, "neu: "+f+"\n", args...) }
	a, err := app.Build(ctx, cfg, app.Options{Extras: ex})
	if err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	if line := busLine(a); line != "" {
		fmt.Fprintf(stderr, "neu: %s\n", line)
	}
	if err := a.Start(ctx); err != nil {
		fmt.Fprintf(stderr, "neu: start: %v\n", err)
		a.Close(5 * time.Second)
		return 1
	}
	defer a.Close(10 * time.Second)
	head, err := observe.Head(ctx, a.M)
	if err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	tr := a.Track(ctx, head)
	con := newConsole(stdout, stderr, *think)
	if !*noStream {
		con.watch(ctx, a.M)
	}
	a.Reconcile(ctx, tr, func(s string) { con.line(stderr, s) })
	if q != "" {
		var read readFunc
		if cfg.Coding.NoPrompt == "" {
			read = readerLines(stdin)
		}
		return oneShot(ctx, a, tr, con, q, newApprover(a, con, read), stdout, stderr)
	}
	in := readLines(stdin)
	newApprover(a, con, in.answer).start(ctx, tr, nil)
	return repl(ctx, a, tr, con, in, stdout)
}

func oneShot(ctx context.Context, a *app.App, tr *app.Tracker, con *console, q string, appr *approver, stdout, stderr io.Writer) int {
	id := newID()
	con.setFocus(id)
	appr.start(ctx, tr, func(task string) bool { return task == id })
	tr.Subscribe(func(ev proto.Event) {
		if ev.TaskID == id || ev.TaskID == "" {
			con.event(ev)
			if line := progress(ev); line != "" {
				con.line(stderr, line)
			}
		}
	})
	pick := a.Route(ctx, q, "")
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: id, Input: q, Class: pick.Class, Budget: &task.Budget{MaxSteps: a.Cfg.MaxSteps}}); err != nil {
		fmt.Fprintf(stderr, "neu: submit: %v\n", err)
		return 1
	}
	if pick.Class != "" {
		fmt.Fprintf(stderr, "[%s] → %s\n", id, pick)
	}
	ev, err := tr.Wait(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "neu: interrupted; task %s continues on the next start with the same -store\n", id)
		return 130
	}
	t, err := a.M.Load(context.WithoutCancel(ctx), id)
	if err != nil {
		fmt.Fprintf(stderr, "neu: %v\n", err)
		return 1
	}
	if !con.answer(id, t.LastAnswer()) {
		fmt.Fprintln(stdout, t.LastAnswer())
	}
	fmt.Fprintln(stderr, taskUsage(tr, t, ev))
	if ev.Kind != proto.EvDone {
		fmt.Fprintf(stderr, "neu: task %s ended %s %s\n", id, ev.Kind, string(ev.Data))
		return 1
	}
	return 0
}

func newID() string {
	return fmt.Sprintf("t%x", time.Now().UnixNano()/1e6%0xfffff)
}

// progress is one short line about an event worth showing while a task
// runs, or "".
func progress(ev proto.Event) string {
	tag := "[" + ev.TaskID + "]"
	switch ev.Kind {
	case proto.EvGroupDown, proto.EvGroupUp:
		class, _ := app.Field[string](ev, "class")
		return fmt.Sprintf("[machine] group %s %s %s", class, strings.TrimPrefix(ev.Kind, "group_"), clip(string(ev.Data), 90))
	case proto.EvMigrated:
		from, _ := app.Field[string](ev, "from")
		to, _ := app.Field[string](ev, "to")
		reason, _ := app.Field[string](ev, "reason")
		return fmt.Sprintf("%s moved %s → %s (%s)", tag, from, to, reason)
	case proto.EvMigrateRefused:
		return fmt.Sprintf("%s could not move to the fallback group: %s", tag, clip(string(ev.Data), 90))
	case proto.EvToolCall:
		tool, _ := app.Field[string](ev, "tool")
		args, _ := app.Field[string](ev, "args")
		return fmt.Sprintf("%s → %s %s", tag, tool, clip(args, 90))
	case proto.EvToolDenied:
		tool, _ := app.Field[string](ev, "tool")
		reason, _ := app.Field[string](ev, "reason")
		if by, _ := app.Field[string](ev, "by"); by == "policy" {
			return fmt.Sprintf("%s ✗ %s refused: %s", tag, tool, clip(reason, 160))
		}
	case proto.EvToolResult:
		if isErr, _ := app.Field[bool](ev, "is_error"); isErr {
			id, _ := app.Field[string](ev, "call_id")
			return fmt.Sprintf("%s ✗ %s returned an error", tag, id)
		}
	case proto.EvModelRetry:
		class, _ := app.Field[string](ev, "class")
		return fmt.Sprintf("%s model retry (%s)", tag, class)
	case proto.EvPreempted:
		by, _ := app.Field[string](ev, "by")
		return fmt.Sprintf("%s preempted by %s", tag, by)
	case proto.EvSuspended:
		return tag + " suspended"
	case proto.EvResumed:
		return tag + " resumed"
	case proto.EvReconcileNeeded:
		tool, _ := app.Field[string](ev, "tool")
		return fmt.Sprintf("%s %s has an unknown outcome after a restart; it waits for an operator", tag, tool)
	case proto.EvSteerDelivered:
		if from, _ := app.Field[string](ev, "from"); from != "" {
			return fmt.Sprintf("%s message from %s delivered", tag, from)
		}
		return tag + " steer delivered"
	case proto.EvSteerDropped:
		if from, _ := app.Field[string](ev, "from"); from != "" {
			return fmt.Sprintf("%s message from %s dropped: the task ended first", tag, from)
		}
	case proto.EvCompacted:
		rounds, _ := app.Field[int](ev, "rounds")
		before, _ := app.Field[int](ev, "tokens_before_est")
		after, _ := app.Field[int](ev, "tokens_after_est")
		return fmt.Sprintf("%s compacted %d round%s: ~%s → ~%s tokens", tag, rounds, plural(rounds), tokens(before), tokens(after))
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
