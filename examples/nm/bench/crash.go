package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/examples/nm/tools"
	"github.com/liliang-cn/neumann/proto"
)

const crashResults = "crash-results.json"

// crash runs the batch in a child process (`nm bench -scenario
// crash-child`), kills it with SIGKILL after some tool results, starts it
// again on the same store and workspace, and lets it finish. Then every
// task must have finished, and no fs_write may have been applied twice
// for one call key.
func (b *Bench) crash(ctx context.Context, rep *Report) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	killAt := b.Opt.KillAt
	if killAt <= 0 {
		r := rand.New(rand.NewPCG(uint64(b.Opt.Seed), 99))
		killAt = 3 + r.IntN(8)
	}
	var ids []string
	for _, t := range b.tasks {
		ids = append(ids, t.ID)
	}
	args := []string{"bench", "-scenario", "crash-child", "-work", b.Opt.Work, "-run-id", b.Opt.RunID,
		"-backend", b.Cfg.Backend, "-model", b.Cfg.Model, "-base-url", b.Cfg.BaseURL, "-cores", strconv.Itoa(b.Cfg.Cores),
		"-max-steps", strconv.Itoa(b.Cfg.MaxSteps), "-tasks", strings.Join(ids, ","), "-store", b.Cfg.Store, "-repo", b.Opt.Repo}
	if b.Opt.Verbose {
		args = append(args, "-v")
	}
	if b.Opt.Deadline > 0 {
		args = append(args, "-deadline", b.Opt.Deadline.String())
	}
	os.Remove(filepath.Join(b.Opt.Work, crashResults))

	type runInfo struct {
		Run         int     `json:"run"`
		ToolResults int     `json:"tool_results"`
		Killed      bool    `json:"killed"`
		Exit        string  `json:"exit"`
		WallS       float64 `json:"wall_s"`
	}
	var runs []runInfo
	run := func(n int, kill int) (runInfo, error) {
		info := runInfo{Run: n}
		start := time.Now()
		cmd := exec.CommandContext(ctx, exe, args...)
		cmd.Stderr = b.errw
		out, err := cmd.StdoutPipe()
		if err != nil {
			return info, err
		}
		if err := cmd.Start(); err != nil {
			return info, err
		}
		how := ""
		switch {
		case kill > 0 && b.Opt.KillOn != "":
			how = ", kill -9 when " + b.Opt.KillOn + " is dispatched"
		case kill > 0:
			how = fmt.Sprintf(", kill -9 after %d tool results", kill)
		}
		b.logf("crash: run %d started (pid %d)%s", n, cmd.Process.Pid, how)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "TOOL_CALL ") {
				if f := strings.Fields(line); kill > 0 && b.Opt.KillOn != "" && len(f) == 3 && f[2] == b.Opt.KillOn && !info.Killed {
					cmd.Process.Signal(syscall.SIGKILL)
					info.Killed = true
					b.logf("crash: run %d: kill -9 as %s was dispatched (%s)", n, f[2], f[1])
				}
				continue
			}
			if strings.HasPrefix(line, "TOOL_RESULT ") {
				info.ToolResults++
				if kill > 0 && b.Opt.KillOn == "" && info.ToolResults >= kill && !info.Killed {
					cmd.Process.Signal(syscall.SIGKILL)
					info.Killed = true
					b.logf("crash: run %d: kill -9 after tool result %d (%s)", n, info.ToolResults, strings.TrimPrefix(line, "TOOL_RESULT "))
				}
				continue
			}
			b.logf("  child %d| %s", n, line)
		}
		err = cmd.Wait()
		info.WallS = time.Since(start).Seconds()
		info.Exit = "0"
		if err != nil {
			info.Exit = err.Error()
		}
		return info, nil
	}
	first, err := run(1, killAt)
	if err != nil {
		return err
	}
	runs = append(runs, first)
	if !first.Killed {
		rep.Notes = append(rep.Notes, fmt.Sprintf("the batch finished after %d tool results, before the kill point %d", first.ToolResults, killAt))
	} else {
		second, err := run(2, 0)
		if err != nil {
			return err
		}
		runs = append(runs, second)
	}
	data, err := os.ReadFile(filepath.Join(b.Opt.Work, crashResults))
	if err != nil {
		return fmt.Errorf("the last child wrote no results: %w", err)
	}
	var child struct {
		Results    []*Result `json:"results"`
		Reconciled []string  `json:"reconciled"`
	}
	if err := json.Unmarshal(data, &child); err != nil {
		return err
	}
	rep.Results = child.Results
	rep.summarize()
	ledger, err := tools.ReadLedger(filepath.Join(b.Cfg.Root, "out", ".ledger"))
	if err != nil {
		return err
	}
	applied := map[string]int{}
	dups := 0
	for _, e := range ledger {
		switch e.Status {
		case "applied":
			applied[e.Key]++
		case "duplicate":
			dups++
		}
	}
	var twice []string
	for k, n := range applied {
		if n > 1 {
			twice = append(twice, fmt.Sprintf("%s ×%d", k, n))
		}
	}
	finished := 0
	for _, r := range child.Results {
		if r.Outcome != "" {
			finished++
		}
	}
	rep.Summary["kill_after_tool_results"] = killAt
	rep.Summary["runs"] = runs
	rep.Summary["finished"] = fmt.Sprintf("%d/%d", finished, len(b.tasks))
	rep.Summary["ledger_applied"] = len(applied)
	rep.Summary["ledger_applied_twice"] = twice
	rep.Summary["ledger_duplicate_calls"] = dups
	rep.Summary["reconciled"] = child.Reconciled
	var errs []error
	if finished != len(b.tasks) {
		errs = append(errs, fmt.Errorf("%d of %d tasks finished", finished, len(b.tasks)))
	}
	if len(twice) > 0 {
		errs = append(errs, fmt.Errorf("writes applied twice for one key: %v", twice))
	}
	return errors.Join(errs...)
}

// crashChild is one life of the crash batch: it starts the machine on the
// shared store (recovering what the last life left), submits the tasks
// not submitted yet, answers reconcile_needed from the write ledger, and
// waits for all of them. Each tool result is a TOOL_RESULT line on stdout,
// for the parent to count.
func (b *Bench) crashChild(ctx context.Context) int {
	facts, err := Setup(b.Cfg.Root, b.Opt.Repo, b.Testdata)
	if err != nil {
		fmt.Fprintf(b.errw, "crash-child: %v\n", err)
		return 1
	}
	b.Facts = facts
	s, err := b.start(ctx, 0)
	if err != nil {
		fmt.Fprintf(b.errw, "crash-child: %v\n", err)
		return 1
	}
	defer s.close()
	var insts []*Instance
	for _, t := range b.tasks {
		prio := 2
		if t.Quick {
			prio = 0
		}
		inst, err := s.NewInstance(t, prio)
		if err != nil {
			fmt.Fprintf(b.errw, "crash-child: %v\n", err)
			return 1
		}
		insts = append(insts, inst)
	}
	var mu sync.Mutex
	var reconciled []string
	s.Tr.Subscribe(func(ev proto.Event) {
		if s.inst(ev.TaskID) == nil {
			return
		}
		switch ev.Kind {
		case proto.EvToolCall:
			id, _ := app.Field[string](ev, "call_id")
			tool, _ := app.Field[string](ev, "tool")
			fmt.Printf("TOOL_CALL %s/%s %s\n", ev.TaskID, id, tool)
		case proto.EvToolResult:
			id, _ := app.Field[string](ev, "call_id")
			fmt.Printf("TOOL_RESULT %s/%s\n", ev.TaskID, id)
		case proto.EvReconcileNeeded:
			if c, _ := app.Field[bool](ev, "cancelled"); c {
				return
			}
			callID, _ := app.Field[string](ev, "call_id")
			key, _ := app.Field[string](ev, "key")
			go func() {
				o := neumann.Outcome{Retry: true}
				how := "retry"
				if e, ok := s.A.Tools.Ledger.Applied(key); ok {
					o = neumann.Outcome{Result: fmt.Sprintf("wrote %d bytes to %s (confirmed from the write ledger after a restart)", e.Bytes, e.Path)}
					how = "applied"
				}
				err := s.A.M.Resolve(ctx, ev.TaskID, callID, o)
				if errors.Is(err, neumann.ErrNotReconciling) {
					return // answered by an earlier life
				}
				mu.Lock()
				reconciled = append(reconciled, fmt.Sprintf("%s %s: %s (%v)", ev.TaskID, callID, how, err))
				mu.Unlock()
				b.logf("reconciled %s/%s from the ledger: %s (err %v)", ev.TaskID, callID, how, err)
			}()
		}
	})
	for _, inst := range insts {
		if _, err := s.A.M.Load(ctx, inst.ID); errors.Is(err, neumann.ErrNotFound) {
			if err := s.Submit(ctx, inst); err != nil {
				fmt.Fprintf(b.errw, "crash-child: %v\n", err)
				return 1
			}
		} else if err != nil {
			fmt.Fprintf(b.errw, "crash-child: %v\n", err)
			return 1
		} else {
			b.logf("  [%s] already submitted by an earlier life", inst.ID)
		}
	}
	var results []*Result
	for _, inst := range insts {
		r, err := s.Finish(ctx, inst)
		if err != nil {
			fmt.Fprintf(b.errw, "crash-child: %v\n", err)
			return 1
		}
		results = append(results, r)
	}
	mu.Lock()
	out, _ := json.MarshalIndent(map[string]any{"results": results, "reconciled": reconciled}, "", "  ")
	mu.Unlock()
	if err := os.WriteFile(filepath.Join(b.Opt.Work, crashResults), out, 0o644); err != nil {
		fmt.Fprintf(b.errw, "crash-child: %v\n", err)
		return 1
	}
	return 0
}
