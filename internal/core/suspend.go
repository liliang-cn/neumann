package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// ErrNotSuspended is returned by ResumeTask for a task that is neither
// suspended nor has a suspend pending.
var ErrNotSuspended = errors.New("not suspended")

// errSuspended is the cause a model call's context, or its wait for room in
// its limiters, is cancelled with when an operator suspends its task
// (design §9.9): never the task's own context.
var errSuspended = errors.New("model call cut off: its task was suspended")

// What a task was doing when a suspend landed (the suspended event's was).
const (
	wasReady     = "ready"
	wasBackoff   = "backoff"
	wasGroup     = "group"
	wasModel     = "model"
	wasTools     = "tools"
	wasApproval  = "approval"
	wasReconcile = "reconcile"
	wasCheck     = "check"
)

// Suspend commits a suspend of a task, then signals it (design §9.9). A
// task nobody is at work on (ready, waiting for a backoff or its group) is
// suspended here before Suspend returns; a model call in flight for it, or
// its wait for room in a limiter, is cut off and its vCore lands the
// suspend; any other lands at its holder's next commit (a tool round's
// results, an operator's decision). A task already suspended, or with a
// suspend pending that no resume follows, commits nothing. The store
// refuses a suspend of an unknown task (mem.ErrNotFound) or a finished one
// (mem.ErrFinished).
func (e *Env) Suspend(ctx context.Context, id string) error {
	suspended, err := e.suspendedNow(ctx, id)
	if err != nil {
		return err
	}
	if suspended {
		return nil
	}
	in := mem.Interrupt{TaskID: id, Kind: mem.InterruptSuspend}
	if err := e.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
		return err
	}
	e.signal(ctx, id, true)
	return nil
}

// ResumeTask commits a resume of a task, then signals it (design §9.9): a
// suspended task nobody holds is Ready again before ResumeTask returns,
// and the preemption check runs for it; one held for a moment is resumed by
// its holder's hand-back. A suspend that has not landed yet is cancelled by
// it where it would land. A task that is not suspended and has no suspend
// pending gives ErrNotSuspended and commits nothing.
func (e *Env) ResumeTask(ctx context.Context, id string) error {
	suspended, err := e.suspendedNow(ctx, id)
	if err != nil {
		return err
	}
	if !suspended {
		return fmt.Errorf("task %s: %w", id, ErrNotSuspended)
	}
	in := mem.Interrupt{TaskID: id, Kind: mem.InterruptResume}
	if err := e.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{in}}); err != nil {
		return err
	}
	e.signal(ctx, id, false)
	return nil
}

// signal is what follows a committed suspend (cut) or resume: the task is
// taken and landed here when nobody is at work on it, and its holder is
// told otherwise (sched.Nudge); a suspend also cuts off a model call in
// flight for it, or its wait for room in a limiter. Nudging comes first:
// a call that registers after the cut finds the mark (cuttable).
func (e *Env) signal(ctx context.Context, id string, cut bool) {
	ctx = context.WithoutCancel(ctx)
	t, ok := e.Sched.Nudge(id, cut, sched.Landable)
	if !ok {
		if cut {
			e.cut(id)
		}
		return
	}
	err := e.landHeld(ctx, t)
	switch {
	case errors.Is(err, mem.ErrCancelPending):
		e.finalise(ctx, t, mem.CommitSet{}, nil, ReasonCancelled)
		return
	case err != nil:
		e.Logf("neumann: task %s: landing a suspend or resume failed; its next instruction lands it: %v", id, err)
	}
	e.handBack(ctx, t, nil)
}

// suspendedNow reports whether task id is suspended once every suspend and
// resume committed so far has landed: the last of them decides (design
// §9.9), applied or not, since every landing applies all those pending by
// that same rule; a task that has none is suspended when its state says so.
// It decides from one read of the interrupts, the task's state only where
// there are none, so a landing between two reads cannot fool it (a state
// read after the landing, the interrupts it applied gone from a read of the
// pending ones). A suspend or resume committed after that read comes after
// the caller's in their order. A finished task is ErrFinished, an unknown one
// ErrNotFound.
func (e *Env) suspendedNow(ctx context.Context, id string) (bool, error) {
	all, err := e.Store.Interrupts(ctx, id, false)
	if err != nil {
		return false, err
	}
	t, err := e.Store.Load(ctx, id)
	if err != nil {
		return false, err
	}
	if t.State.Terminal() {
		return false, fmt.Errorf("%w: %s", mem.ErrFinished, id)
	}
	if ss := suspensions(all, nil); len(ss) > 0 {
		return ss[len(ss)-1].Kind == mem.InterruptSuspend, nil
	}
	return t.State == task.Suspended, nil
}

// suspensions are the pending suspends and resumes that applied does not
// hold, in seq order.
func suspensions(pending []mem.Interrupt, applied []int64) []mem.Interrupt {
	var out []mem.Interrupt
	for _, in := range pending {
		if (in.Kind == mem.InterruptSuspend || in.Kind == mem.InterruptResume) && !slices.Contains(applied, in.Seq) {
			out = append(out, in)
		}
	}
	return out
}

// landSuspend lands t's pending suspends and resumes (design §9.9) in c,
// the commit its holder is about to make: c.Task is t, in the state that
// commit gives it. Only the last of them counts, and all of them are
// applied where it lands:
//
//   - the last is a suspend, and t is ready or waiting for a backoff or its
//     group: t is Suspended instead (a backoff's wake-up time dropped), and
//     a suspended event {was, kept, kept_think} says what it was doing and
//     what a model call it cut off kept (kept, keptThink);
//   - the last is a suspend, and t waits for its tools, an approval or
//     reconciliation: nothing lands, it lands when that wait is over;
//   - t is Suspended already (the hand-back or recovery of a suspended
//     task): a resume makes it Ready, queued from now, with a resumed
//     event {pending: false}; another suspend is applied with no event;
//   - the last is a resume, and t is not suspended: the task goes on as c
//     says, and a resumed event {pending: true} reports a suspend it
//     cancelled before it landed (a resume with no suspend before it is
//     applied with no event).
//
// Events go after c's own: an observer reads the instruction's step and
// usage first. A terminal c is left alone: the store applies them with no
// event, and a task that ended does not stop to be suspended.
func (e *Env) landSuspend(t *task.TCB, c mem.CommitSet, pending []mem.Interrupt, was, kept, keptThink string) mem.CommitSet {
	ss := suspensions(pending, c.Applied)
	if len(ss) == 0 || t.State.Terminal() {
		return c
	}
	suspend := ss[len(ss)-1].Kind == mem.InterruptSuspend
	switch {
	case t.State == task.Suspended && !suspend:
		t.State, t.Wait = task.Ready, task.WaitNone
		t.ReadySince = e.Clock.Now()
		c.Events = append(c.Events, proto.NewEvent(proto.EvResumed, t.ID, map[string]any{"pending": false}))
	case t.State == task.Suspended:
	case suspend && sched.Landable(t):
		t.State, t.Wait, t.WakeAt = task.Suspended, task.WaitNone, time.Time{}
		c.Events = append(c.Events, proto.NewEvent(proto.EvSuspended, t.ID, map[string]any{
			"was": was, "kept": runes(kept), "kept_think": runes(keptThink),
		}))
	case suspend:
		return c
	case slices.ContainsFunc(ss, func(in mem.Interrupt) bool { return in.Kind == mem.InterruptSuspend }):
		c.Events = append(c.Events, proto.NewEvent(proto.EvResumed, t.ID, map[string]any{"pending": true}))
	}
	applied := slices.Clone(c.Applied)
	for _, in := range ss {
		applied = append(applied, in.Seq)
	}
	c.Applied = applied
	return c
}

// foldSuspend lands, in c (whose task is t), a suspend or resume nudged
// while the caller held t (sched.Nudged): it reads t's interrupts again.
// A read that fails leaves them pending, the task marked: its hand-back
// lands them. So does a suspend that lands only once t's wait is over (its
// tools, an approval): whoever ends that wait folds it.
func (e *Env) foldSuspend(ctx context.Context, t *task.TCB, c mem.CommitSet, was string) mem.CommitSet {
	if c.Task != t || t.State.Terminal() || !e.Sched.Nudged(t.ID) {
		return c
	}
	e.Sched.Heard(t.ID)
	pending, err := e.Store.Interrupts(context.WithoutCancel(ctx), t.ID, true)
	if err != nil {
		e.Sched.Mark(t.ID)
		e.Logf("neumann: task %s: read interrupts to land a suspend: %v", t.ID, err)
		return c
	}
	c = e.landSuspend(t, c, pending, was, "", "")
	if len(suspensions(pending, c.Applied)) > 0 {
		e.Sched.Mark(t.ID) // a suspend that lands when t's wait is over: its holder then folds it
	}
	return c
}

// landAtStart is how an instruction begins when a suspend or resume is
// pending (design §9.9): a suspend that no resume follows lands at once,
// nothing runs, and the commit returned is the instruction's (ok); a
// suspend that a resume cancelled is left for the instruction's own commit
// to apply, the task marked so that it does.
func (e *Env) landAtStart(t *task.TCB, pending []mem.Interrupt) (c mem.CommitSet, ok bool) {
	ss := suspensions(pending, nil)
	if len(ss) == 0 {
		return c, false
	}
	if ss[len(ss)-1].Kind == mem.InterruptResume {
		e.Sched.Mark(t.ID)
		return c, false
	}
	t.Version++
	return e.landSuspend(t, mem.CommitSet{Task: t}, pending, wasReady, "", ""), true
}

// landHeld lands the pending suspends and resumes of a task the caller
// holds between instructions: taken by the suspender or resumer, or handed
// back while it was nudged. It commits only when there is something to
// apply. On error t is unchanged.
func (e *Env) landHeld(ctx context.Context, t *task.TCB) error {
	e.Sched.Heard(t.ID)
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return fmt.Errorf("read interrupts: %w", err)
	}
	moved := t.Clone()
	moved.Version++
	c := e.landSuspend(moved, mem.CommitSet{Task: moved}, pending, heldWas(t), "", "")
	if len(c.Applied) == 0 {
		return nil
	}
	if err := e.Commit(ctx, c); err != nil {
		return err
	}
	*t = *moved
	return nil
}

// heldWas is what a task held between instructions was doing.
func heldWas(t *task.TCB) string {
	if t.State == task.Waiting {
		switch t.Wait {
		case task.WaitBackoff:
			return wasBackoff
		case task.WaitGroup:
			return wasGroup
		}
	}
	return wasReady
}

// suspendedWait is how a model instruction whose wait for room in its
// limiters a suspend cut off ends: nothing was reserved or sent, and the
// suspend lands (was model). A resume that followed it leaves the task
// ready, the two applied.
func (e *Env) suspendedWait(ctx context.Context, t *task.TCB) (mem.CommitSet, error) {
	e.Sched.Heard(t.ID)
	pending, err := e.Store.Interrupts(context.WithoutCancel(ctx), t.ID, true)
	if err != nil {
		e.Sched.Mark(t.ID) // its hand-back lands it
		return mem.CommitSet{}, errNotStarted
	}
	t.Version++
	c := e.landSuspend(t, mem.CommitSet{Task: t}, pending, wasModel, "", "")
	if len(c.Applied) == 0 {
		t.Version--
		return mem.CommitSet{}, errNotStarted
	}
	return c, nil
}

// suspendedCall is the commit of a model call a suspend cut off (design
// §9.9): like a preemption (preempted), what the call produced is kept for
// the next call to continue (keepPartial), and the call counts as no step,
// no retry and nothing unconfirmed; but the task's preemptions are not
// counted, and it is Suspended, its suspended event saying how much was
// kept. A resume that followed the suspend leaves the task ready instead.
func (e *Env) suspendedCall(ctx context.Context, t *task.TCB, c mem.CommitSet, text, think string, caps model.Caps) mem.CommitSet {
	kept, keptThink := keepPartial(t, text, think, caps)
	e.ready(t, task.StepModel)
	c.Task = t
	e.Sched.Heard(t.ID)
	pending, err := e.Store.Interrupts(context.WithoutCancel(ctx), t.ID, true)
	if err != nil {
		e.Sched.Mark(t.ID) // its hand-back lands it
		e.Logf("neumann: task %s: read interrupts to land a suspend: %v", t.ID, err)
		return c
	}
	return e.landSuspend(t, c, pending, wasModel, kept, keptThink)
}

// cuts are what a suspend may cut off of a task on a vCore (design §9.9):
// its model instruction's wait for room in its limiters, then its model
// step's call. A compaction's call is not cut (it lands at its commit), nor
// is the half-open breaker's probe: cut off, it would never be reported,
// and the group would stay down until the breaker gave it up.
type cuts struct {
	mu sync.Mutex
	m  map[string]*cutEntry
}

type cutEntry struct{ cancel context.CancelCauseFunc }

// cuttable registers cancel as what a suspend of task id cuts off, until
// the returned func is called. A suspend nudged before it registered (the
// task's cutting mark) cuts it at once.
func (e *Env) cuttable(id string, cancel context.CancelCauseFunc) (done func()) {
	en := &cutEntry{cancel}
	e.cuts.mu.Lock()
	if e.cuts.m == nil {
		e.cuts.m = map[string]*cutEntry{}
	}
	e.cuts.m[id] = en
	e.cuts.mu.Unlock()
	if e.Sched.Cutting(id) {
		cancel(errSuspended)
	}
	return func() {
		e.cuts.mu.Lock()
		if e.cuts.m[id] == en {
			delete(e.cuts.m, id)
		}
		e.cuts.mu.Unlock()
	}
}

// cut cuts off what task id has registered, if anything.
func (e *Env) cut(id string) {
	e.cuts.mu.Lock()
	en := e.cuts.m[id]
	e.cuts.mu.Unlock()
	if en != nil {
		en.cancel(errSuspended)
	}
}
