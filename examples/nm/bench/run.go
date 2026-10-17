// Package bench is nm's harness: a suite of realistic tasks with objective
// checkers, run against a real backend in several scenarios, measured from
// the machine's committed event stream.
package bench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/examples/nm/tools"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Options are the bench flags beyond the machine's.
type Options struct {
	Scenario string
	Suite    string
	Tasks    string
	Work     string
	Repo     string
	Out      string
	RunID    string

	Every    time.Duration // mixed: probe interval
	Probes   int           // mixed: how many probes
	BG       string        // mixed: background tasks
	Baseline string        // mixed: solo report with the probes' solo latency
	K        int           // burst: copies
	Seed     int64         // crash
	KillAt   int           // crash: tool results before the kill; 0: from the seed
	KillOn   string        // crash: kill when a call of this tool is dispatched
	Rejudge  string        // judge the answers of this report again
	Suspend  time.Duration // ops: how long the task stays suspended
	Deadline time.Duration // per task; 0: none
	Timeout  time.Duration // the whole run
	Verbose  bool
}

// Suite is a -suite file: which tasks, which probes.
type Suite struct {
	Tasks  []string `json:"tasks"`
	Probes []string `json:"probes"`
	BG     []string `json:"background"`
}

// Main is `nm bench`; testdata holds the committed fixtures.
func Main(ctx context.Context, args []string, testdata fs.FS, stdout, stderr io.Writer) int {
	var cfg app.Config
	var o Options
	fset := flag.NewFlagSet("nm bench", flag.ContinueOnError)
	fset.SetOutput(stderr)
	cfg.Flags(fset)
	fset.StringVar(&o.Scenario, "scenario", "solo", "solo, mixed, ops, crash or burst")
	fset.StringVar(&o.Suite, "suite", "", "JSON file {\"tasks\":[...],\"probes\":[...],\"background\":[...]} selecting task ids (a trailing * is a prefix); default: the whole built-in suite")
	fset.StringVar(&o.Tasks, "tasks", "", "comma-separated task ids or kinds to run (overrides the suite's tasks)")
	fset.StringVar(&o.Work, "work", "", "workspace directory (fixtures, store, out/); default: a new directory under $TMPDIR")
	fset.StringVar(&o.Repo, "repo", ".", "git repository to snapshot "+repoTag+" from")
	fset.StringVar(&o.Out, "out", "", "write the JSON report here")
	fset.StringVar(&o.RunID, "run-id", "", "prefix of task ids; default: from the clock")
	fset.DurationVar(&o.Every, "every", 30*time.Second, "mixed: seconds between interactive probes")
	fset.IntVar(&o.Probes, "probes", 0, "mixed: number of probes (default: one per quick task)")
	fset.StringVar(&o.BG, "bg", "", "mixed: background tasks (default: every non-quick task selected)")
	fset.StringVar(&o.Baseline, "baseline", "", "mixed: a solo report whose quick tasks give the probes' solo latency; without it the probes run solo first")
	fset.IntVar(&o.K, "k", 30, "burst: tasks submitted at once")
	fset.Int64Var(&o.Seed, "seed", 1, "crash: seed for the kill point")
	fset.IntVar(&o.KillAt, "kill-after", 0, "crash: kill -9 after this many tool results (0: 3 to 10, from -seed)")
	fset.StringVar(&o.KillOn, "kill-on", "", "crash: kill -9 as soon as a call of this tool (e.g. fs_write) is dispatched, instead of after -kill-after results")
	fset.StringVar(&o.Rejudge, "rejudge", "", "run the checkers again on the answers and files of this report (after a checker fix); writes -out")
	fset.DurationVar(&o.Suspend, "suspend-for", 15*time.Second, "ops: how long the suspended task stays suspended")
	fset.DurationVar(&o.Deadline, "deadline", 0, "deadline per task (0: none)")
	fset.DurationVar(&o.Timeout, "timeout", 3*time.Hour, "give up on the whole run after this long")
	fset.BoolVar(&o.Verbose, "v", false, "log the machine's own lines and every tool call")
	fset.Usage = func() {
		fmt.Fprintf(stderr, "usage: nm bench [flags]\n\nRuns the task suite in a scenario and reports what happened. Task ids:\n")
		for _, t := range Tasks() {
			fmt.Fprintf(stderr, "  %-18s %s\n", t.ID, clip(t.Prompt, 90))
		}
		fmt.Fprintln(stderr)
		fset.PrintDefaults()
	}
	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	cfg.MarkSet(fset)
	storeSet := false
	fset.Visit(func(f *flag.Flag) { storeSet = storeSet || f.Name == "store" })
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	if o.Work == "" {
		d, err := os.MkdirTemp("", "nm-bench-")
		if err != nil {
			fmt.Fprintf(stderr, "nm bench: %v\n", err)
			return 1
		}
		o.Work = d
	}
	if abs, err := filepath.Abs(o.Work); err == nil {
		o.Work = abs
	}
	if o.RunID == "" {
		o.RunID = fmt.Sprintf("r%s", time.Now().Format("0102-150405"))
	}
	if !storeSet {
		cfg.Store = filepath.Join(o.Work, ".nm", o.Scenario+".db")
		if o.Scenario == "crash-child" {
			cfg.Store = filepath.Join(o.Work, ".nm", "crash.db")
		}
	}
	cfg.Root, cfg.Writable = filepath.Join(o.Work, "ws"), "out,fixbug"
	cfg.Quiet = !o.Verbose
	if err := cfg.Resolve(); err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 1
	}
	b := &Bench{Cfg: cfg, Opt: o, Testdata: testdata, out: stdout, errw: stderr}
	if err := b.loadSuite(); err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 2
	}
	if o.Scenario == "crash-child" {
		return b.crashChild(ctx)
	}
	if o.Rejudge != "" {
		return b.rejudge(stdout, stderr)
	}
	b.logf("nm bench: scenario %s, backend %s, workspace %s", o.Scenario, cfg.Describe(), o.Work)
	facts, err := Setup(cfg.Root, o.Repo, testdata)
	if err != nil {
		fmt.Fprintf(stderr, "nm bench: fixtures: %v\n", err)
		return 1
	}
	b.Facts = facts
	rep := newReport(cfg)
	rep.Scenario, rep.Work, rep.Store, rep.RunID, rep.Started = o.Scenario, o.Work, cfg.Store, o.RunID, time.Now()
	switch o.Scenario {
	case "solo":
		err = b.solo(ctx, rep)
	case "mixed":
		err = b.mixed(ctx, rep)
	case "ops":
		err = b.ops(ctx, rep)
	case "crash":
		err = b.crash(ctx, rep)
	case "burst":
		err = b.burst(ctx, rep)
	default:
		err = fmt.Errorf("unknown scenario %q", o.Scenario)
	}
	rep.Finished = time.Now()
	rep.WallS = rep.Finished.Sub(rep.Started).Seconds()
	if err != nil {
		rep.Notes = append(rep.Notes, "error: "+err.Error())
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
	}
	rep.Print(stdout)
	if o.Out != "" {
		if werr := rep.Write(o.Out); werr != nil {
			fmt.Fprintf(stderr, "nm bench: %v\n", werr)
			return 1
		}
		fmt.Fprintf(stdout, "report: %s\n", o.Out)
	}
	if err != nil {
		return 1
	}
	return 0
}

// Bench runs one scenario.
type Bench struct {
	Cfg      app.Config
	Opt      Options
	Testdata fs.FS
	Facts    *Facts
	tasks    []*Task
	probes   []*Task
	bg       []*Task
	out      io.Writer
	errw     io.Writer
	mu       sync.Mutex
	adapter  model.Adapter            // tests: a scripted model instead of the backend
	adapters map[string]model.Adapter // tests: scripted models by class (hybrid)

	lastSession *Session
}

func (b *Bench) logf(format string, args ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Fprintf(b.out, time.Now().Format("15:04:05 ")+format+"\n", args...)
}

func (b *Bench) loadSuite() error {
	var s Suite
	if b.Opt.Suite != "" {
		data, err := os.ReadFile(b.Opt.Suite)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &s); err != nil {
			return fmt.Errorf("suite %s: %v", b.Opt.Suite, err)
		}
	}
	taskList := strings.Join(s.Tasks, ",")
	if b.Opt.Tasks != "" {
		taskList = b.Opt.Tasks
	}
	var err error
	if b.tasks, err = Select(taskList); err != nil {
		return err
	}
	probeList := strings.Join(s.Probes, ",")
	if probeList == "" {
		probeList = "quick"
	}
	if b.probes, err = Select(probeList); err != nil {
		return err
	}
	bgList := strings.Join(s.BG, ",")
	if b.Opt.BG != "" {
		bgList = b.Opt.BG
	}
	if bgList != "" {
		b.bg, err = Select(bgList)
		return err
	}
	for _, t := range b.tasks {
		if !t.Quick {
			b.bg = append(b.bg, t)
		}
	}
	return nil
}

// Session is a started machine with a tracker, for one scenario.
type Session struct {
	b     *Bench
	A     *app.App
	Tr    *app.Tracker
	mu    sync.Mutex
	insts []*Instance
	byID  map[string]*Instance
	n     map[string]int
}

func (b *Bench) start(ctx context.Context, from int64) (*Session, error) {
	s := &Session{b: b, byID: map[string]*Instance{}, n: map[string]int{}}
	a, err := app.Build(ctx, b.Cfg, app.Options{Adapter: b.adapter, Adapters: b.adapters})
	if err != nil {
		return nil, err
	}
	if err := a.Start(ctx); err != nil {
		a.Close(5 * time.Second)
		return nil, err
	}
	s.A = a
	s.Tr = a.Track(ctx, from)
	b.lastSession = s
	s.Tr.Subscribe(func(ev proto.Event) {
		if ev.TaskID == "" {
			switch ev.Kind {
			case proto.EvGroupDown, proto.EvGroupUp:
				b.logf("  machine: %s %s", ev.Kind, clip(string(ev.Data), 120))
			}
			return
		}
		inst := s.inst(ev.TaskID)
		if inst == nil {
			return
		}
		switch ev.Kind {
		case proto.EvToolCall:
			if b.Opt.Verbose {
				tool, _ := app.Field[string](ev, "tool")
				args, _ := app.Field[string](ev, "args")
				b.logf("  [%s] → %s %s", ev.TaskID, tool, clip(args, 100))
			}
		case proto.EvPreempted, proto.EvSuspended, proto.EvResumed, proto.EvModelRetry, proto.EvReconcileNeeded, proto.EvRecovered, proto.EvMigrated, proto.EvMigrateRefused:
			b.logf("  [%s] %s %s", ev.TaskID, ev.Kind, clip(string(ev.Data), 120))
		}
	})
	return s, nil
}

func (s *Session) close() { s.A.Close(30 * time.Second) }

func (s *Session) inst(id string) *Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byID[id]
}

// NewInstance makes the next copy of t, with its own directories.
func (s *Session) NewInstance(t *Task, prio int) (*Instance, error) {
	s.mu.Lock()
	s.n[t.ID]++
	n := s.n[t.ID]
	s.mu.Unlock()
	return s.b.instance(t, prio, n, s)
}

func (b *Bench) instance(t *Task, prio, n int, s *Session) (*Instance, error) {
	id := fmt.Sprintf("%s.%s.%d", b.Opt.RunID, t.ID, n)
	inst := &Instance{ID: id, Task: t, TaskID: t.ID, Priority: prio}
	inst.OutDir = "out/" + id
	if t.NeedsFix {
		inst.FixDir = "fixbug/" + id
		dst := filepath.Join(b.Cfg.Root, inst.FixDir)
		orig, err := fs.ReadFile(b.Testdata, "testdata/fixbug/window_test.go")
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(orig)
		inst.TestSum = hex.EncodeToString(sum[:])
		if _, err := os.Stat(dst); os.IsNotExist(err) {
			if _, err := CopyFixbug(b.Testdata, dst); err != nil {
				return nil, err
			}
		}
	}
	inst.Input = strings.NewReplacer("{{fix}}", inst.FixDir, "{{out}}", inst.OutDir).Replace(t.Prompt)
	if s != nil {
		s.mu.Lock()
		s.insts = append(s.insts, inst)
		s.byID[id] = inst
		s.mu.Unlock()
	}
	return inst, nil
}

// Submit routes an instance to its group (hybrid) and submits it
// (committed when it returns).
func (s *Session) Submit(ctx context.Context, inst *Instance) error {
	pick := s.A.Route(ctx, inst.Input, StaticClass(inst.Task.Kind))
	inst.Route, inst.RouteMS = pick, float64(pick.Took.Microseconds())/1000
	in := proto.Instruction{TaskID: inst.ID, Input: inst.Input, Class: pick.Class, Priority: inst.Priority, Budget: &task.Budget{MaxSteps: s.b.Cfg.MaxSteps}}
	if s.b.Opt.Deadline > 0 {
		in.Budget.Deadline = time.Now().Add(s.b.Opt.Deadline)
	}
	if _, err := s.A.M.Submit(ctx, in); err != nil {
		return fmt.Errorf("submit %s: %w", inst.ID, err)
	}
	if pick.Class != "" {
		s.b.logf("  [%s] submitted (priority %d) → %s", inst.ID, inst.Priority, pick)
	} else {
		s.b.logf("  [%s] submitted (priority %d)", inst.ID, inst.Priority)
	}
	return nil
}

// Result is one finished instance, judged and measured.
type Result struct {
	*Instance
	Kind      string    `json:"kind"`
	Quick     bool      `json:"quick,omitempty"`
	Pass      bool      `json:"pass"`
	Detail    string    `json:"detail"`
	Answer    string    `json:"answer"`
	Submitted time.Time `json:"submitted"`
	Metrics
	Extra map[string]any `json:"extra,omitempty"`
}

// Finish waits for inst and judges it.
func (s *Session) Finish(ctx context.Context, inst *Instance) (*Result, error) {
	if _, err := s.Tr.Wait(ctx, inst.ID); err != nil {
		return nil, fmt.Errorf("wait %s: %w", inst.ID, err)
	}
	return s.judge(ctx, inst), nil
}

func (s *Session) judge(ctx context.Context, inst *Instance) *Result {
	evs := s.Tr.Events(inst.ID)
	r := &Result{Instance: inst, Kind: inst.Task.Kind, Quick: inst.Task.Quick}
	if len(evs) > 0 {
		r.Submitted = evs[0].Time
	}
	r.Metrics = Compute(evs, app.Fingerprint(inst.Input), s.shared(inst), s.A.CallRecords(), s.A.ToolRecords())
	t, err := s.A.M.Load(context.WithoutCancel(ctx), inst.ID)
	if err == nil {
		r.Answer = t.LastAnswer()
	}
	ledger, _ := tools.ReadLedger(s.A.Tools.Ledger.Path())
	var mine []tools.LedgerEntry
	for _, e := range ledger {
		if e.Task == inst.ID {
			mine = append(mine, e)
		}
	}
	v := Verdict{Detail: "task ended " + r.Outcome + " " + r.StopReason}
	if r.Outcome == proto.EvDone && r.StopReason == "budget" {
		v.Detail = fmt.Sprintf("ran out of model steps (MaxSteps %d) without an answer", s.b.Cfg.MaxSteps)
	} else if r.Outcome == proto.EvDone {
		v = inst.Task.Check(&CheckCtx{Answer: r.Answer, Inst: inst, Facts: s.b.Facts, Root: s.b.Cfg.Root, Ledger: mine})
	}
	r.Pass, r.Detail = v.Pass, v.Detail
	mark := "FAIL"
	if r.Pass {
		mark = "pass"
	}
	on := ""
	if r.Class != "" && r.Class != "default" {
		on = " on " + r.Class
		if r.FinalClass != r.Class {
			on += "→" + r.FinalClass
		}
	}
	s.b.logf("  [%s] %s %s%s in %.1fs (%d calls, %d tools): %s", inst.ID, r.Outcome, mark, on, r.WallMS/1000, r.ModelCalls, r.ToolCalls, r.Detail)
	return r
}

// shared reports whether another instance of this session has the same
// input: their adapter calls cannot be told apart.
func (s *Session) shared(inst *Instance) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.insts {
		if o != inst && o.Input == inst.Input {
			return true
		}
	}
	return false
}

// RunAll submits every instance at once and waits for all of them.
func (s *Session) RunAll(ctx context.Context, insts []*Instance) ([]*Result, error) {
	for _, inst := range insts {
		if err := s.Submit(ctx, inst); err != nil {
			return nil, err
		}
	}
	out := make([]*Result, len(insts))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i, inst := range insts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Finish(ctx, inst)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			out[i] = r
		}()
	}
	wg.Wait()
	var res []*Result
	for _, r := range out {
		if r != nil {
			res = append(res, r)
		}
	}
	return res, firstErr
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(p*float64(len(s)-1) + 0.5)
	return s[min(max(i, 0), len(s)-1)]
}

// rejudge runs the checkers again on a finished report: the answers it
// recorded and the files its workspace still holds. Scenario-specific
// checks (ops) are not redone.
func (b *Bench) rejudge(stdout, stderr io.Writer) int {
	data, err := os.ReadFile(b.Opt.Rejudge)
	if err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 1
	}
	var rep Report
	if err := json.Unmarshal(data, &rep); err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 1
	}
	root := filepath.Join(rep.Work, "ws")
	facts, err := Setup(root, b.Opt.Repo, b.Testdata)
	if err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 1
	}
	ledger, _ := tools.ReadLedger(filepath.Join(root, "out", ".ledger"))
	for _, r := range rep.Results {
		r.CacheHitRate = hitRate(r.Usage.CachedTokens, r.Usage.PromptTokens)
		t := TaskByID(r.TaskID)
		if t == nil || r.Outcome != proto.EvDone {
			continue
		}
		r.Instance.Task = t
		var mine []tools.LedgerEntry
		for _, e := range ledger {
			if e.Task == r.ID {
				mine = append(mine, e)
			}
		}
		v := t.Check(&CheckCtx{Answer: r.Answer, Inst: r.Instance, Facts: facts, Root: root, Ledger: mine})
		if v.Pass != r.Pass || v.Detail != r.Detail {
			if r.Extra == nil {
				r.Extra = map[string]any{}
			}
			r.Extra["first_verdict"] = Verdict{r.Pass, r.Detail}
			fmt.Fprintf(stdout, "%s: %v (%s) -> %v (%s)\n", r.ID, r.Pass, r.Detail, v.Pass, v.Detail)
		}
		r.Pass, r.Detail = v.Pass, v.Detail
	}
	rep.Summary = nil
	rep.summarize()
	rep.Notes = append(rep.Notes, "rejudged with the current checkers at "+time.Now().Format(time.RFC3339))
	rep.Print(stdout)
	out := b.Opt.Out
	if out == "" {
		out = b.Opt.Rejudge
	}
	if err := rep.Write(out); err != nil {
		fmt.Fprintf(stderr, "nm bench: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "report: %s\n", out)
	return 0
}
