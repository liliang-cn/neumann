package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Defaults of a group's preemption (design §6.4).
const (
	DefaultPreemptAfter = 2 * time.Second
	DefaultMaxPreempts  = 2
)

// errPreempted is the cause a preempted model call's context is cancelled
// with: the call's own context, never its task's.
var errPreempted = errors.New("model call preempted for a more urgent task")

// flight is one model call in flight in a group that preempts: what the
// preemption check needs to choose a victim, and how to cut it off.
type flight struct {
	id       string
	priority int // the task's own priority, never aged
	preempts int // the task's Preempts when the call was sent
	core     int // the vCore making the call
	start    time.Time
	cancel   context.CancelCauseFunc
	by       string // the task it was cut off for; empty until chosen
	pre      *Group // the group it is registered with for preemption; nil when none
	uncut    func() // ends its registration for a suspend's cut (cuttable)
}

// preemption is a group's registry of model calls in flight (StepModel
// only: a compaction is never preempted, and neither is the half-open
// breaker's probe), the vCores whose call was chosen as a victim and that
// have not finished their instruction yet, with the task each was freed
// for, and the timer that checks again once a call has run its quantum.
type preemption struct {
	mu       sync.Mutex
	flights  map[string]*flight
	yielding map[int]string // vCore → the task it is freed for
	stop     func() bool
	due      time.Time
}

func (g *Group) preemptAfter() time.Duration {
	if g.PreemptAfter > 0 {
		return g.PreemptAfter
	}
	return DefaultPreemptAfter
}

func (g *Group) maxPreempts() int {
	if g.MaxPreempts > 0 {
		return g.MaxPreempts
	}
	return DefaultMaxPreempts
}

// fly registers t's model call, and returns the context the call runs
// under: ctx (the task's), plus the cut a preemption or a suspend makes.
// land must follow the call. The call is registered for a suspend's cut
// (cuttable, design §9.9), and in its group, when the group preempts. The
// half-open breaker's probe is registered with neither: cut off, it would
// never be reported, and the group would stay down until the breaker gave
// the lost probe up.
//
// It ends with a preemption check: a more urgent task may have become ready
// while this call was on its way (taken ahead of it by aging, or waiting for
// room in a limiter), and no other check would see this call then.
func (e *Env) fly(ctx context.Context, g *Group, t *task.TCB, core int, probe bool) (context.Context, *flight) {
	if probe {
		return ctx, nil
	}
	cctx, cancel := context.WithCancelCause(ctx)
	f := &flight{id: t.ID, priority: t.Priority, preempts: t.Preempts, core: core, start: e.Clock.Now(), cancel: cancel}
	if g != nil && g.Preempt {
		defer e.preemptCheck(g)
		f.pre = g
		g.pre.mu.Lock()
		if g.pre.flights == nil {
			g.pre.flights = map[string]*flight{}
		}
		g.pre.flights[t.ID] = f
		g.pre.mu.Unlock()
	}
	f.uncut = e.cuttable(t.ID, cancel)
	return cctx, f
}

// land takes a call that has returned out of the registries. It reports the
// task the call was cut off for, and whether it was: chosen as a victim and
// its context ended by that choice; and whether a suspend cut it off
// instead. Whichever cut came first is the call's: a victim a suspend had
// cut already was not preempted, and a call chosen just as it answered was
// cut off by neither: its answer stands.
func (e *Env) land(f *flight, ctx context.Context) (by string, ran time.Duration, preempted, suspended bool) {
	if f == nil {
		return "", 0, false, false
	}
	f.uncut()
	if f.pre != nil {
		f.pre.pre.mu.Lock()
		delete(f.pre.pre.flights, f.id)
		by = f.by
		f.pre.pre.mu.Unlock()
	}
	cause := context.Cause(ctx)
	preempted = by != "" && cause == errPreempted
	suspended = cause == errSuspended
	f.cancel(nil)
	return by, e.Clock.Now().Sub(f.start), preempted, suspended
}

// CheckPreempt runs the preemption check of class's group (design §6.4).
// The machine calls it after an operator's SetPriority.
func (e *Env) CheckPreempt(class string) {
	if g := e.groupOf(class); g != nil {
		e.preemptCheck(g)
	}
}

// OnReady is what the scheduler runs when a task joins class's ready queue:
// the group grows its vCores for it (design §6.1), and, when it still has
// none free, may cut off a less urgent call for it (§6.4).
func (e *Env) OnReady(class string) {
	e.Grow(class)
	e.CheckPreempt(class)
}

// preemptCheck cuts off one model call of g for the most urgent ready task
// h of g, when g preempts and no vCore of g is free for h: every vCore g may
// run is alive and busy, and none is about to be freed by an earlier
// preemption. The victim is a call whose task is less urgent than h by the
// tasks' own priorities (aging never counts), that has run at least
// PreemptAfter, and whose task has been preempted fewer than MaxPreempts
// times in this step; the least urgent of them, and among equals the one
// that has run the shortest (it loses the least). A candidate still short of
// its quantum arms a timer that checks again when it is due.
//
// Only one victim is chosen at a time: its vCore counts as free until it has
// finished its instruction, and the check runs again once that vCore takes
// its next task (runCore). The victim's call context is ended with
// errPreempted; its vCore commits the preemption (preempted).
func (e *Env) preemptCheck(g *Group) {
	if !g.Preempt || e.Sched.Stopping().Err() != nil || e.halt().Err() != nil {
		return
	}
	by, prio, ok := e.Sched.Urgent(g.Class)
	if !ok {
		return
	}
	running, busy := g.Ctl.Scaler.Counts()
	target := g.Ctl.Window.Target()
	now := e.Clock.Now()
	after, most := g.preemptAfter(), g.maxPreempts()
	g.pre.mu.Lock()
	if running < target || running-busy+len(g.pre.yielding) > 0 {
		g.pre.mu.Unlock()
		return
	}
	var victim *flight
	var next time.Time
	for _, f := range g.pre.flights {
		if f.by != "" || f.priority <= prio || f.preempts >= most {
			continue
		}
		if due := f.start.Add(after); now.Before(due) {
			if next.IsZero() || due.Before(next) {
				next = due
			}
			continue
		}
		if victim == nil || f.priority > victim.priority ||
			f.priority == victim.priority && (f.start.After(victim.start) || f.start.Equal(victim.start) && f.id < victim.id) {
			victim = f
		}
	}
	switch {
	case victim != nil:
		victim.by = by
		if g.pre.yielding == nil {
			g.pre.yielding = map[int]string{}
		}
		g.pre.yielding[victim.core] = by
	case !next.IsZero() && (g.pre.stop == nil || next.Before(g.pre.due)):
		if g.pre.stop != nil {
			g.pre.stop()
		}
		g.pre.due = next
		g.pre.stop = e.Sched.AfterFunc(next.Sub(now), func() { e.quantumOver(g) })
	}
	g.pre.mu.Unlock()
	if victim != nil {
		victim.cancel(errPreempted)
	}
}

// quantumOver is the group's quantum timer: a call has now run long enough
// to be preempted.
func (e *Env) quantumOver(g *Group) {
	g.pre.mu.Lock()
	g.pre.stop, g.pre.due = nil, time.Time{}
	g.pre.mu.Unlock()
	e.preemptCheck(g)
}

// instructionOver is called by a vCore of g after each instruction, once it
// has released it: a vCore whose call was chosen as a victim no longer
// counts as about to be free. It returns the task that vCore was freed for,
// which its next fetch takes if it is still ready (TakeReady): aging must
// not hand the vCore to another task, or the call was cut off for nothing.
func (e *Env) instructionOver(g *Group, core int) (freedFor string) {
	if g == nil || !g.Preempt {
		return ""
	}
	g.pre.mu.Lock()
	freedFor = g.pre.yielding[core]
	delete(g.pre.yielding, core)
	g.pre.mu.Unlock()
	return freedFor
}

// preempted is the commit of a model call cut off for task by (design §6.4):
// the task is Ready again at the same instruction, queued from now (its
// ReadySince is not kept: an old background task would otherwise be aged
// straight back in front of by), with one more preemption in this step.
// What the call had produced is kept for the next call to continue
// (keepPartial). The preempted event says how much was kept, in
// characters, and how long the call ran. The call was not answered: no
// usage, no step counted, no retry, nothing unconfirmed.
func (e *Env) preempted(t *task.TCB, c mem.CommitSet, by string, ran time.Duration, text, think string, caps model.Caps) mem.CommitSet {
	kept, keptThink := keepPartial(t, text, think, caps)
	t.Preempts++
	e.ready(t, task.StepModel)
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvPreempted, t.ID, map[string]any{
		"by": by, "kept": runes(kept), "kept_think": runes(keptThink), "ran_ms": max(ran.Milliseconds(), 0), "n": t.Preempts,
	}))
	return c
}

// keepPartial keeps what a call that was not answered (preempted, or its
// stream broken off) had produced, for the next call to continue (design
// §6.4), and returns what it kept: text, or thinking.
//
//   - Text continues text: the call continued a Partial, or its answer had
//     begun. The adapter must be able to continue a prefill (caps.Prefill);
//     otherwise nothing new is kept, and a Partial the task already had
//     stays (a turn its pending steers interrupt is delivered all the same).
//     Kept text ends the thinking: PartialThink is cleared.
//   - Thinking: the call had produced no text, only reasoning, and the
//     adapter can continue a reasoning prefix (caps.ThinkPrefill): it is
//     appended to PartialThink. Otherwise the thinking is lost, and the next
//     call thinks afresh.
//
// The caller passes nothing for a call that applied steers: that call was a
// new turn answering them, and goes back with them.
func keepPartial(t *task.TCB, text, think string, caps model.Caps) (keptText, keptThink string) {
	switch {
	case t.Partial != "":
		if caps.Prefill {
			t.Partial += text
			return text, ""
		}
	case text != "" && caps.Prefill:
		t.Partial, t.PartialThink = text, ""
		return text, ""
	case think != "" && caps.ThinkPrefill:
		t.PartialThink += think
		return "", think
	}
	return "", ""
}

// continuedThink is msg, the answer to a request that asked the model to
// continue the reasoning prefix think, with think joined in front of its
// reasoning: in front of its first reasoning part, or as its first part.
func continuedThink(think string, msg task.Message) task.Message {
	parts := make([]task.Part, 0, len(msg.Parts)+1)
	done := false
	for _, p := range msg.Parts {
		if !done && p.Kind == task.PartReasoning {
			p.Text = think + p.Text
			done = true
		}
		parts = append(parts, p)
	}
	if !done {
		parts = append([]task.Part{{Kind: task.PartReasoning, Text: think}}, parts...)
	}
	msg.Parts = parts
	return msg
}

// continued is msg, the answer to a request that asked the model to
// continue prefix, joined onto it: prefix goes in front of msg's first text
// part, or, when msg has none, becomes its first text part, before any tool
// call (after any reasoning, which a provider wants first).
func continued(prefix string, msg task.Message) task.Message {
	parts := make([]task.Part, 0, len(msg.Parts)+1)
	done := false
	for _, p := range msg.Parts {
		if !done && p.Kind == task.PartText {
			p.Text = prefix + p.Text
			done = true
		}
		parts = append(parts, p)
	}
	if !done {
		i := 0
		for i < len(parts) && parts[i].Kind == task.PartReasoning {
			i++
		}
		parts = append(parts[:i], append([]task.Part{{Kind: task.PartText, Text: prefix}}, parts[i:]...)...)
	}
	msg.Parts = parts
	return msg
}

// thinkingOnly reports a turn with nothing in it but thinking: a reasoning
// part, or reasoning tokens reported, and no text or tool call. A backend
// that reports neither (Ollama) is judged by the reasoning it streamed.
func thinkingOnly(msg task.Message, reasoningTokens int) bool {
	if strings.TrimSpace(msg.TextContent()) != "" || len(msg.ToolCalls()) > 0 {
		return false
	}
	for _, p := range msg.Parts {
		if p.Kind == task.PartReasoning {
			return true
		}
	}
	return reasoningTokens > 0
}

// runes is the length of s in characters, as events report text lengths.
func runes(s string) int { return utf8.RuneCountInString(strings.ToValidUTF8(s, "�")) }

// SetPriority gives a task the caller holds (checked out of the scheduler
// between instructions) priority p: the operator's reordering (design §6.4).
// It commits the new priority with a priority_set event {priority, was};
// nothing else about the task changes, its place in the queue (ReadySince)
// included. A priority still pending for the task (PriorityInterrupt, its
// holder at the time failed to land it) came before this one: it is applied
// in the same commit, and p wins. The same priority with nothing pending
// commits nothing. On error t is unchanged; on success the caller hands it
// back, and then runs the preemption check.
func (e *Env) SetPriority(ctx context.Context, t *task.TCB, p int) error {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return fmt.Errorf("read interrupts: %w", err)
	}
	ps := priorities(pending, nil)
	if t.Priority == p && len(ps) == 0 {
		return nil
	}
	moved := t.Clone()
	moved.Version++
	c := mem.CommitSet{Task: moved}
	for _, in := range ps {
		c.Applied = append(c.Applied, in.Seq)
	}
	if p != t.Priority {
		moved.Priority = p
		c.Events = []proto.Event{priorityEvent(t.ID, p, t.Priority)}
	}
	if err := e.Commit(ctx, c); err != nil {
		return err
	}
	*t = *moved
	return nil
}

// PriorityInterrupt is SetPriority for a task its caller could not take:
// one executing an instruction on a vCore or waiting for its tools (design
// §6.4). It commits a priority interrupt carrying p and returns: it never
// waits for the instruction. The task's holder applies it at its next
// boundary, where suspends land (design §9.9: instruction start and commit,
// the end of a tool round, an operator's decision, a hand-back, recovery),
// in that boundary's own commit, with a priority_set event if the task's
// priority changes; the hand-back that follows runs the preemption check
// for a ready task. A task back with the scheduler by the time the interrupt
// is committed is taken and landed here (signal); one its holder hands back
// into a wait for an operator is landed right after that hand-back
// (landWaiting), not left for the operator. The same priority as the
// task will have once what is pending lands commits nothing. A task that
// ends first drops it: the store applies it with the terminal commit,
// silently. A finished task is ErrFinished, an unknown one ErrNotFound.
func (e *Env) PriorityInterrupt(ctx context.Context, id string, p int) error {
	// The pending interrupts first, then the task: a landing between the
	// two reads shows the same priority in both.
	pending, err := e.Store.Interrupts(ctx, id, true)
	if err != nil {
		return err
	}
	t, err := e.Store.Load(ctx, id)
	if err != nil {
		return err
	}
	if t.State.Terminal() {
		return mem.ErrFinished // the machine names the task, as for one it took
	}
	now := t.Priority
	if q, ok := lastPriority(priorities(pending, nil)); ok {
		now = q
	}
	if now == p {
		return nil
	}
	if err := e.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{mem.PriorityInterrupt(id, p)}}); err != nil {
		return err
	}
	e.signal(ctx, id, false, notOnIO)
	return nil
}

// operatorWait reports whether t waits for an operator: an approval, or
// reconciliation.
func operatorWait(t *task.TCB) bool {
	return t.State == task.Waiting && (t.Wait == task.WaitApproval || t.Wait == task.WaitReconcile)
}

// landWaiting lands the priorities of task id, just handed back into a wait
// for an operator, that were nudged while it was held. Such a wait refuses
// no hand-back (sched.Hand refuses one only where a suspend lands at once),
// so without this they would wait for the operator. It takes the task
// (PriorityInterrupt's filter, notOnIO), lands them (landHeld) and hands it
// back, until none is pending. A suspend pending alongside lands only when
// the wait is over: landHeld marks the task again for whoever ends it, and
// since the loop goes on only while priorities are pending, that mark never
// makes it spin. Each turn lands every priority it reads, so it ends unless
// more keep coming; a landing that fails ends it, the task left marked for
// its next boundary.
func (e *Env) landWaiting(ctx context.Context, id string) {
	for e.halt().Err() == nil && e.Sched.Nudged(id) {
		pending, err := e.Store.Interrupts(ctx, id, true)
		if err != nil || len(priorities(pending, nil)) == 0 {
			return
		}
		t, ok := e.Sched.Nudge(id, false, notOnIO)
		if !ok {
			return // held by someone else again: their hand-back lands it
		}
		err = e.landHeld(ctx, t)
		switch {
		case errors.Is(err, mem.ErrCancelPending):
			e.finalise(ctx, t, mem.CommitSet{}, nil, ReasonCancelled)
			return
		case err != nil:
			e.Logf("neumann: task %s: landing a priority failed; its next boundary lands it: %v", id, err)
			e.handBackOnce(ctx, t, nil)
			return
		}
		if !e.handBackOnce(ctx, t, nil) {
			return
		}
	}
}

// notOnIO accepts a task nobody is at work on for a priority to land on at
// once: any but one waiting for its tools, whose batch holds it.
func notOnIO(t *task.TCB) bool { return t.State != task.Waiting || t.Wait != task.WaitIO }

// priorities are the pending priority interrupts that applied does not
// hold, in seq order.
func priorities(pending []mem.Interrupt, applied []int64) []mem.Interrupt {
	var out []mem.Interrupt
	for _, in := range pending {
		if in.Kind == mem.InterruptPriority && !slices.Contains(applied, in.Seq) {
			out = append(out, in)
		}
	}
	return out
}

// lastPriority is the priority the last of ps that carries one gives.
func lastPriority(ps []mem.Interrupt) (int, bool) {
	for i := len(ps) - 1; i >= 0; i-- {
		if p, ok := mem.PriorityOf(ps[i]); ok {
			return p, true
		}
	}
	return 0, false
}

func priorityEvent(id string, p, was int) proto.Event {
	return proto.NewEvent(proto.EvPrioritySet, id, map[string]any{"priority": p, "was": was})
}

// landPriority lands t's pending priority interrupts in c, the commit its
// holder is about to make (c.Task is t). The last of them decides and all
// of them are applied, whatever t is doing: unlike a suspend, a priority
// needs no wait to be over. A priority_set {priority, was} goes after c's
// own events when t's priority changes; several changes, or one undone
// before it landed, are one event or none. A terminal c is left alone: the
// store applies them with no event. One whose number does not parse (a
// store written by hand) is applied and ignored.
func (e *Env) landPriority(t *task.TCB, c mem.CommitSet, pending []mem.Interrupt) mem.CommitSet {
	ps := priorities(pending, c.Applied)
	if len(ps) == 0 || t.State.Terminal() {
		return c
	}
	applied := slices.Clone(c.Applied)
	for _, in := range ps {
		if _, ok := mem.PriorityOf(in); !ok && e.Logf != nil {
			e.Logf("neumann: task %s: priority interrupt %d carries %q, not a priority; ignored", t.ID, in.Seq, in.Text)
		}
		applied = append(applied, in.Seq)
	}
	c.Applied = applied
	if p, ok := lastPriority(ps); ok && p != t.Priority {
		c.Events = append(c.Events, priorityEvent(t.ID, p, t.Priority))
		t.Priority = p
	}
	return c
}
