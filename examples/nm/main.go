// Command nm is a small coding and data assistant on top of neumann: one
// process, one machine with a SQLite store, and workspace tools confined to
// a root directory (fs_list, fs_read, fs_grep, fs_write, git_run, go_test,
// calc_eval). It doubles as a test vehicle: `nm bench` runs a suite of
// realistic, auto-checked tasks in several scenarios and reports what the
// framework did.
//
//	nm "Which files mention preemption?"          one question, then exit
//	nm                                             REPL: each line is a task
//	nm bench -scenario solo -out report.json       the harness
//
// -backend local uses Ollama (qwen3.5:9b-mlx, one request at a time,
// preemption on); -backend cloud uses CPA_BASE_URL, CPA_API_KEY and
// CPA_MODEL; -backend hybrid runs both as two execution groups, fast
// (Ollama, qwen3.5:4b) and strong (the cloud model), and -route picks the
// group of each task. Keys come from the environment only and are never printed.
// Unfinished tasks continue after a restart (the store is ./.nm/state.db
// unless -store says otherwise).
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/examples/nm/bench"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// testdata is the bench's committed fixtures.
//
//go:embed testdata
var testdata embed.FS

func main() {
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 && os.Args[1] == "bench" {
		os.Exit(bench.Main(ctx, os.Args[2:], testdata, os.Stdout, os.Stderr))
	}
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var cfg app.Config
	fs := flag.NewFlagSet("nm", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg.Flags(fs)
	verbose := fs.Bool("v", false, "log the machine's own lines on stderr")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: nm [flags] [question]\n       nm bench [flags]   (nm bench -help)\n\nWith a question: answer it and exit. Without: a REPL, one task per line (/help).\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	cfg.MarkSet(fs)
	cfg.Quiet = !*verbose
	a, err := app.Build(ctx, cfg, app.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "nm: %v\n", err)
		return 1
	}
	if err := a.Start(ctx); err != nil {
		fmt.Fprintf(stderr, "nm: start: %v\n", err)
		a.Close(5 * time.Second)
		return 1
	}
	defer a.Close(10 * time.Second)
	head, err := observe.Head(ctx, a.M)
	if err != nil {
		fmt.Fprintf(stderr, "nm: %v\n", err)
		return 1
	}
	tr := a.Track(ctx, head)
	if q := strings.TrimSpace(strings.Join(fs.Args(), " ")); q != "" {
		return oneShot(ctx, a, tr, q, stdout, stderr)
	}
	return repl(ctx, a, tr, stdin, stdout)
}

func oneShot(ctx context.Context, a *app.App, tr *app.Tracker, q string, stdout, stderr io.Writer) int {
	id := newID()
	tr.Subscribe(func(ev proto.Event) {
		if ev.TaskID == id || ev.TaskID == "" {
			if line := progress(ev); line != "" {
				fmt.Fprintln(stderr, line)
			}
		}
	})
	pick := a.Route(ctx, q, "")
	if _, err := a.M.Submit(ctx, proto.Instruction{TaskID: id, Input: q, Class: pick.Class, Budget: &task.Budget{MaxSteps: a.Cfg.MaxSteps}}); err != nil {
		fmt.Fprintf(stderr, "nm: submit: %v\n", err)
		return 1
	}
	if pick.Class != "" {
		fmt.Fprintf(stderr, "[%s] → %s\n", id, pick)
	}
	ev, err := tr.Wait(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "nm: interrupted; task %s continues on the next start with the same -store\n", id)
		return 130
	}
	t, err := a.M.Load(context.WithoutCancel(ctx), id)
	if err != nil {
		fmt.Fprintf(stderr, "nm: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, t.LastAnswer())
	if ev.Kind != proto.EvDone {
		fmt.Fprintf(stderr, "nm: task %s ended %s %s\n", id, ev.Kind, string(ev.Data))
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
		return tag + " steer delivered"
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
