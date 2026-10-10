// Package core runs vCores: the loop that fetches a ready task, executes one
// instruction, commits, and only then acts on what it committed (design §5).
package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Env is what every vCore shares. The machine builds one.
type Env struct {
	Store mem.Store
	Sched *sched.Sched
	Bus   *bus.Bus
	Model model.Adapter
	Clock sched.Clock

	// Commit is the only write path: commit to the store and, on success, let
	// subscribers see the events. Nothing a commit describes may happen before
	// Commit returns nil.
	Commit func(ctx context.Context, c mem.CommitSet) error
	Logf   func(format string, args ...any)

	// Halt interrupts work that is already running: model calls, tool calls.
	// A commit that already passed its halt check may still land; nothing is
	// committed for work that Halt interrupted, and that task keeps its last
	// committed state. nil means context.Background(), never halted.
	Halt context.Context

	// SteerFormat renders a steer as the user message the model sees. nil
	// sends the text as it is.
	SteerFormat func(text string) string

	// Delta receives a model step's output as it is generated, when the
	// adapter can stream. It is called on the vCore's goroutine and must not
	// block. nil: calls are not streamed.
	Delta func(proto.Delta)

	// Groups are the execution groups by class: window, breaker and
	// limiters. A class with none calls its model with no group control.
	Groups map[string]*Group

	// The model step's retry policy (design §11): consecutive failures a task
	// may have before it fails, and the exponential backoff between them.
	// Zero takes the defaults. Jitter maps a backoff step to the delay used;
	// nil is full jitter (uniform in [0, step]), tests pass the identity.
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Jitter      func(time.Duration) time.Duration

	// Guard admits work started by a timer (the half-open probe timer): ok
	// false means the machine is stopping and the work must not start;
	// release ends it. nil admits everything.
	Guard func() (release func(), ok bool)

	// Policy decides each tool call before its first dispatch is committed
	// (design §9.7). nil allows every call and records no decision.
	Policy Policy

	batches sync.WaitGroup
	cores   cores // elastic vCores: StartCores, Grow, CloseCores, WaitCores

	dispatchMu     sync.Mutex // guards dispatchClosed against batches.Add
	dispatchClosed bool

	failMu     sync.Mutex
	startFails map[string]int // consecutive failed start commits, per task

	refusedMu sync.Mutex
	refused   map[string]bool // parked after migrate_refused: a fallback coming back does not wake them

	lateMu     sync.Mutex // guards lateClosed against lates.Add
	lateClosed bool
	lates      sync.WaitGroup // late results being recorded
}

// maxStartRetries bounds how often a step whose start could not be committed
// is tried again.
const maxStartRetries = 3

func (e *Env) startFailed(id string) int {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	if e.startFails == nil {
		e.startFails = map[string]int{}
	}
	e.startFails[id]++
	return e.startFails[id]
}

func (e *Env) startCommitted(id string) {
	e.failMu.Lock()
	defer e.failMu.Unlock()
	delete(e.startFails, id)
}

func (e *Env) frameSteer(text string) string {
	if e.SteerFormat == nil {
		return text
	}
	return e.SteerFormat(text)
}

type stepData struct {
	PC   string `json:"pc"`
	Core int    `json:"core"`
}

// taskContext is what work for a task runs under: it ends when the task is
// cancelled (its scheduler context) or when the machine halts. release frees
// it.
func (e *Env) taskContext(id string) (ctx context.Context, release func()) {
	ctx, cancel := context.WithCancel(e.Sched.Context(id))
	stop := context.AfterFunc(e.halt(), cancel)
	return ctx, func() { stop(); cancel() }
}

func (e *Env) halt() context.Context {
	if e.Halt == nil {
		return context.Background()
	}
	return e.Halt
}

// RunVCore is one vCore. It returns when the scheduler closes or ctx ends,
// and only ever between two instructions. Cancelling ctx stops fetching; an
// instruction already started runs to its commit, so a shutdown never loses
// or fails the task it was holding. Cancelling Halt interrupts running
// instructions instead: their results are discarded, not committed.
func (e *Env) RunVCore(ctx context.Context, class string, core int) {
	for {
		t, ok := e.Sched.Next(ctx, class)
		if !ok {
			return
		}
		e.step(e.halt(), t, core)
	}
}

// WaitBatches waits for every dispatched tool batch to hand its task back.
func (e *Env) WaitBatches() { e.batches.Wait() }

// CloseDispatch refuses every later tool batch; its calls stay dispatched in
// the call log for recovery. Shutdown calls it after the vCores have
// returned and before WaitBatches, so no batch can start while it waits.
func (e *Env) CloseDispatch() {
	e.dispatchMu.Lock()
	e.dispatchClosed = true
	e.dispatchMu.Unlock()
}

func (e *Env) step(ctx context.Context, t *task.TCB, core int) {
	var (
		c     mem.CommitSet
		batch []Send
		err   error
	)
	if e.halt().Err() != nil {
		e.Sched.Forget(t.ID)
		return
	}
	switch t.PC {
	case task.StepModel, task.StepCompact:
		// A compaction pass decides afresh at each of its instructions
		// (execModel → windowFor → execCompact).
		c, batch, err = e.execModel(ctx, t, core)
	case task.StepTools:
		c, batch, err = e.execTools(ctx, t, core)
	default:
		err = fmt.Errorf("instruction %s is not implemented", t.PC)
	}
	if e.halt().Err() != nil {
		// Halted mid-instruction: the result is not committed, so the task
		// keeps its last committed state for recovery to resume from.
		e.Sched.Forget(t.ID)
		return
	}
	if errors.Is(err, errCannotFinalise) {
		// The task was cancelled, and its final commit could not be put
		// together. Its cancel is committed: recovery finishes it.
		e.Logf("neumann: task %s: %v; it is no longer scheduled", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	if errors.Is(err, errNotStarted) {
		e.handBack(ctx, t, nil)
		return
	}
	if errors.Is(err, errStartNotCommitted) {
		// Nothing happened yet, so the step is retried a few times. A store
		// that keeps failing is not going to record anything else either.
		if e.startFailed(t.ID) <= maxStartRetries {
			e.Logf("neumann: task %s: %v; retrying", t.ID, err)
			e.handBack(ctx, t, nil)
			return
		}
		e.Logf("neumann: task %s: %v; giving up, it is no longer scheduled", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	if err != nil {
		c, batch = e.fail(t, c, err.Error()), nil
	}
	// A commit that has started is allowed to finish.
	if err := e.Commit(context.WithoutCancel(ctx), c); err != nil {
		if errors.Is(err, mem.ErrCancelPending) {
			e.cancelInstead(ctx, t, c) // a cancel committed first: nothing of c happened
			return
		}
		e.Logf("neumann: commit for task %s failed; it is no longer scheduled: %v", t.ID, err)
		e.Sched.Forget(t.ID)
		return
	}
	id := t.ID
	allow := append([]string(nil), t.Allow...)
	deny := append([]string(nil), t.Deny...)
	var parked *Group
	movable := false // parked though it may still move: every group along its fallback chain was down
	if t.State == task.Waiting && t.Wait == task.WaitGroup {
		parked = e.groupOf(t.Class)
		refused := slices.ContainsFunc(c.Events, func(ev proto.Event) bool { return ev.Kind == proto.EvMigrateRefused })
		movable = !t.PinClass && !refused
		e.setRefused(t.ID, refused) // before the hand-back: a group_up right after it must see it
	}
	if !e.handBack(ctx, t, batch) {
		return // cancelled meanwhile and finalised: the batch is never sent
	}
	// t now belongs to the scheduler
	if len(batch) > 0 && e.halt().Err() == nil {
		e.dispatch(id, allow, deny, batch)
	}
	if parked != nil {
		e.afterPark(ctx, id, parked, movable)
	}
}

// ready makes t runnable at its next instruction.
func (e *Env) ready(t *task.TCB, pc task.PC) {
	t.State = task.Ready
	t.Wait = task.WaitNone
	t.PC = pc
	t.ReadySince = e.Clock.Now()
	t.Version++
}

func (e *Env) finish(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Done
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events,
		proto.NewEvent(proto.EvAnswer, t.ID, map[string]string{"text": t.LastAnswer()}),
		proto.NewEvent(proto.EvDone, t.ID, map[string]string{"stop_reason": reason}))
	return c
}

func (e *Env) fail(t *task.TCB, c mem.CommitSet, reason string) mem.CommitSet {
	t.State = task.Failed
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID, map[string]string{"error": reason}))
	return c
}
