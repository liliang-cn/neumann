package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

const replHelp = `Each line is a task at priority 0 (interactive). Commands:
  /bg <text>          submit at priority 2 (background)
  /pri <id> <n>       change a task's priority (0 most urgent)
  /suspend <id>       take a task off the machine
  /resume <id>        put it back
  /steer <id> <text>  add a message to a running task
  /cancel <id>        cancel a task
  /status             vCores, queues, groups
  /tasks              unfinished tasks
  /wait <id>          block until a task ends and print its answer
  /quit               stop (unfinished tasks continue on the next start)`

// repl reads one line at a time. Answers are printed as their tasks end,
// from the event stream, prefixed by the task id.
func repl(ctx context.Context, a *app.App, tr *app.Tracker, stdin io.Reader, stdout io.Writer) int {
	var mu sync.Mutex
	say := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(stdout, format+"\n", args...)
	}
	waiting := map[string]bool{} // answered by /wait: not printed twice
	var wmu sync.Mutex
	tr.Subscribe(func(ev proto.Event) {
		if line := progress(ev); line != "" {
			say("%s", line)
		}
		if !ev.Terminal() {
			return
		}
		wmu.Lock()
		w := waiting[ev.TaskID]
		wmu.Unlock()
		if !w {
			go func() { say("%s", outcome(context.WithoutCancel(ctx), a, ev)) }()
		}
	})
	say("nm: %s %s, %d vCore(s), workspace %s. /help for commands.", a.Cfg.Backend, a.Cfg.Model, a.Cfg.Cores, a.Tools.Box.Root())
	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdin)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	submit := func(text string, prio int) {
		id := newID()
		_, err := a.M.Submit(ctx, proto.Instruction{TaskID: id, Input: text, Priority: prio, Budget: &task.Budget{MaxSteps: a.Cfg.MaxSteps}})
		if err != nil {
			say("error: %v", err)
			return
		}
		say("[%s] submitted (priority %d)", id, prio)
	}
	for {
		var line string
		var ok bool
		select {
		case <-ctx.Done():
			return 0
		case line, ok = <-lines:
			if !ok {
				return 0
			}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "/") {
			submit(line, 0)
			continue
		}
		cmd, rest, _ := strings.Cut(line, " ")
		rest = strings.TrimSpace(rest)
		id, arg, _ := strings.Cut(rest, " ")
		arg = strings.TrimSpace(arg)
		var err error
		switch cmd {
		case "/help":
			say("%s", replHelp)
		case "/quit", "/exit":
			return 0
		case "/bg":
			if rest == "" {
				say("usage: /bg <text>")
				continue
			}
			submit(rest, 2)
		case "/pri":
			var n int
			if n, err = strconv.Atoi(arg); err == nil {
				err = retryBusy(ctx, func() error { return a.M.SetPriority(ctx, id, n) })
			}
		case "/suspend":
			err = a.M.Suspend(ctx, id)
		case "/resume":
			err = a.M.ResumeTask(ctx, id)
		case "/steer":
			if arg == "" {
				say("usage: /steer <id> <text>")
				continue
			}
			err = a.M.SteerWith(ctx, id, arg, neumann.SteerOptions{})
		case "/cancel":
			err = a.M.Cancel(ctx, id)
		case "/status":
			say("%s", statusText(a.M.Status()))
		case "/tasks":
			infos := a.M.Tasks()
			if len(infos) == 0 {
				say("no unfinished tasks")
			}
			for _, t := range infos {
				say("  %-10s %-9s %-10s %s", t.ID, t.State, t.Wait, t.Title)
			}
		case "/wait":
			wmu.Lock()
			waiting[id] = true
			wmu.Unlock()
			ev, werr := tr.Wait(ctx, id)
			if werr != nil {
				err = werr
				break
			}
			say("%s", outcome(ctx, a, ev))
		default:
			say("unknown command %s; /help lists them", cmd)
			continue
		}
		if err != nil {
			say("error: %v", err)
		} else if cmd != "/help" && cmd != "/status" && cmd != "/tasks" && cmd != "/wait" && cmd != "/bg" {
			say("ok")
		}
	}
}

// retryBusy retries an operator call that is refused while the task runs
// an instruction (ErrTaskBusy), for up to 30s.
func retryBusy(ctx context.Context, f func() error) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := f()
		if !errors.Is(err, neumann.ErrTaskBusy) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func outcome(ctx context.Context, a *app.App, ev proto.Event) string {
	t, err := a.M.Load(ctx, ev.TaskID)
	if err != nil {
		return fmt.Sprintf("[%s] %s (%v)", ev.TaskID, ev.Kind, err)
	}
	head := fmt.Sprintf("[%s] %s after %d model calls", ev.TaskID, ev.Kind, t.Used.Steps)
	if ev.Kind != proto.EvDone {
		head += " " + string(ev.Data)
	}
	if ans := t.LastAnswer(); ans != "" {
		return head + ":\n" + ans
	}
	return head
}

func statusText(s neumann.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "vCores %d, ready %d, waiting %d, held %d, unfinished %d, suspended %d", s.Cores, s.Ready, s.Waiting, s.Held, s.Unfinished, s.Suspended)
	for _, g := range s.Groups {
		fmt.Fprintf(&b, "\n  group %s (%s): W=%.1f target=%d live=%d busy=%d ready=%d waiting=%d breaker=%s", g.Class, g.Model, g.W, g.Target, g.Live, g.Busy, g.Ready, g.Waiting, g.Breaker)
	}
	return b.String()
}
