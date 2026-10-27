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
	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

const replHelp = `Each line is a task at priority 0 (interactive). Commands:
  /bg <text>          submit at priority 2 (background)
  /fast <text>        hybrid: submit to the fast (local) group, whatever -route says
  /strong <text>      hybrid: submit to the strong (cloud) group
  /pri <id> <n>       change a task's priority (0 most urgent)
  /suspend <id>       take a task off the machine
  /resume <id>        put it back
  /steer <id> <text>  add a message to a running task
  /cancel <id>        cancel a task
  /status             vCores, queues, groups, and the session's usage
  /usage              the session's model calls and tokens
  /tasks              unfinished tasks
  /tools              every tool, its effect and its device (MCP servers included)
  /skills             the skills found, and those skipped
  /wait <id>          block until a task ends and print its answer
  /quit               stop (unfinished tasks continue on the next start)`

// repl reads one line at a time. Answers are printed as their tasks end,
// from the event stream, prefixed by the task id.
func repl(ctx context.Context, a *app.App, tr *app.Tracker, con *console, stdin io.Reader, stdout io.Writer) int {
	say := func(format string, args ...any) { con.line(stdout, fmt.Sprintf(format, args...)) }
	waiting := map[string]bool{} // answered by /wait: not printed twice
	var wmu sync.Mutex
	usage := newTally()
	tr.Subscribe(func(ev proto.Event) {
		usage.add(ev)
		con.event(ev)
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
			go func() { say("%s", outcome(context.WithoutCancel(ctx), a, tr, con, ev)) }()
		}
	})
	say("neu %s, workspace %s. /help for commands.", Version(), a.Tools.Box.Root())
	lines := make(chan string)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdin)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	submitTo := func(text string, prio int, pick app.Pick) {
		id := newID()
		_, err := a.M.Submit(ctx, proto.Instruction{TaskID: id, Input: text, Class: pick.Class, Priority: prio, Budget: &task.Budget{MaxSteps: a.Cfg.MaxSteps}})
		if err != nil {
			say("error: %v", err)
			return
		}
		if prio == 0 {
			con.setFocus(id) // the newest interactive task streams its answer
		}
		if pick.Class != "" {
			say("[%s] submitted (priority %d) → %s", id, prio, pick)
			return
		}
		say("[%s] submitted (priority %d)", id, prio)
	}
	submit := func(text string, prio int) { submitTo(text, prio, a.Route(ctx, text, "")) }
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
		case "/fast", "/strong":
			if rest == "" {
				say("usage: %s <text>", cmd)
				continue
			}
			pick, ferr := a.Force(cmd[1:])
			if ferr != nil {
				say("error: %v", ferr)
				continue
			}
			submitTo(rest, 0, pick)
			continue
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
			say("%s\n%s", statusText(a, a.M.Status()), usage)
		case "/usage":
			say("%s", usage)
		case "/tasks":
			infos := a.M.Tasks()
			if len(infos) == 0 {
				say("no unfinished tasks")
			}
			for _, t := range infos {
				say("  %-10s %-9s %-10s %s", t.ID, t.State, t.Wait, t.Title)
			}
		case "/tools":
			say("%s", toolsText(a))
			continue
		case "/skills":
			say("%s", skillsText(a))
			continue
		case "/wait":
			wmu.Lock()
			waiting[id] = true
			wmu.Unlock()
			ev, werr := tr.Wait(ctx, id)
			if werr != nil {
				err = werr
				break
			}
			say("%s", outcome(ctx, a, tr, con, ev))
		default:
			say("unknown command %s; /help lists them", cmd)
			continue
		}
		if err != nil {
			say("error: %v", err)
		} else if cmd != "/help" && cmd != "/status" && cmd != "/usage" && cmd != "/tasks" && cmd != "/wait" && cmd != "/bg" {
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

// outcome is what the REPL prints when a task ends: how it ended, its
// answer, and its usage line.
func outcome(ctx context.Context, a *app.App, tr *app.Tracker, con *console, ev proto.Event) string {
	t, err := a.M.Load(ctx, ev.TaskID)
	if err != nil {
		return fmt.Sprintf("[%s] %s (%v)", ev.TaskID, ev.Kind, err)
	}
	on := ""
	if a.Hybrid() {
		on = " on " + t.Class
	}
	head := fmt.Sprintf("[%s] %s%s", ev.TaskID, ev.Kind, on)
	if ev.Kind != proto.EvDone {
		head += " " + string(ev.Data)
	}
	if ans := t.LastAnswer(); ans != "" && !con.answer(ev.TaskID, ans) {
		head += ":\n" + ans
	}
	return head + "\n" + taskUsage(tr, t, ev)
}

func statusText(a *app.App, s neumann.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "vCores %d, ready %d, waiting %d, held %d, unfinished %d, suspended %d", s.Cores, s.Ready, s.Waiting, s.Held, s.Unfinished, s.Suspended)
	if m := a.RouteMode(); m != "" {
		fmt.Fprintf(&b, "; route %s", m)
	}
	for _, g := range s.Groups {
		side, _ := a.Side(g.Class)
		fmt.Fprintf(&b, "\n  group %s: %s %s, %d vCore(s): W=%.1f target=%d live=%d busy=%d ready=%d waiting=%d (for the group %d) breaker=%s",
			g.Class, side.Kind, side.Model, side.Cores, g.W, g.Target, g.Live, g.Busy, g.Ready, g.Waiting, g.ForGroup, g.Breaker)
		if g.Breaker != "closed" {
			fmt.Fprintf(&b, " (%s, failures %d", g.Cause, g.Failures)
			if !g.NextProbe.IsZero() {
				fmt.Fprintf(&b, ", next probe in %s", time.Until(g.NextProbe).Round(time.Second))
			}
			b.WriteString(")")
		}
		if g.Fallback != "" {
			fmt.Fprintf(&b, " fallback=%s", g.Fallback)
		}
	}
	return b.String()
}
