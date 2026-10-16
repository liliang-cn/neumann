package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/liliang-cn/neumann/mem"
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
}

// preemption is a group's registry of model calls in flight (StepModel
// only: a compaction is never preempted), the vCores whose call was chosen
// as a victim and that have not finished their instruction yet, and the
// timer that checks again once a call has run its quantum.
type preemption struct {
	mu       sync.Mutex
	flights  map[string]*flight
	yielding map[int]bool
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

// fly registers t's model call in its group, when the group preempts, and
// returns the context the call runs under: ctx (the task's), plus the cut a
// preemption makes. land must follow the call.
func (e *Env) fly(ctx context.Context, g *Group, t *task.TCB, core int) (context.Context, *flight) {
	if g == nil || !g.Preempt {
		return ctx, nil
	}
	cctx, cancel := context.WithCancelCause(ctx)
	f := &flight{id: t.ID, priority: t.Priority, preempts: t.Preempts, core: core, start: e.Clock.Now(), cancel: cancel}
	g.pre.mu.Lock()
	if g.pre.flights == nil {
		g.pre.flights = map[string]*flight{}
	}
	g.pre.flights[t.ID] = f
	g.pre.mu.Unlock()
	return cctx, f
}

// land takes a call that has returned out of the registry. It reports the
// task the call was cut off for, and whether it was: chosen as a victim and
// its context ended by that choice. A call chosen just as it answered was
// not preempted: its answer stands.
func (e *Env) land(g *Group, f *flight, ctx context.Context) (by string, ran time.Duration, preempted bool) {
	if f == nil {
		return "", 0, false
	}
	g.pre.mu.Lock()
	delete(g.pre.flights, f.id)
	by = f.by
	g.pre.mu.Unlock()
	preempted = by != "" && context.Cause(ctx) == errPreempted
	f.cancel(nil)
	return by, e.Clock.Now().Sub(f.start), preempted
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
			g.pre.yielding = map[int]bool{}
		}
		g.pre.yielding[victim.core] = true
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
// counts as about to be free, and the check runs again.
func (e *Env) instructionOver(g *Group, core int) {
	if g == nil || !g.Preempt {
		return
	}
	g.pre.mu.Lock()
	was := g.pre.yielding[core]
	delete(g.pre.yielding, core)
	g.pre.mu.Unlock()
	if was {
		e.preemptCheck(g)
	}
}

// preempted is the commit of a model call cut off for task by (design §6.4):
// the task is Ready again at the same instruction, queued from now (its
// ReadySince is not kept: an old background task would otherwise be aged
// straight back in front of by), with one more preemption in this step. The
// text the call had produced, kept, is appended to Partial for the next call
// to continue; nothing is kept when the adapter cannot continue (prefill
// false), and Partial is then cleared. The preempted event says how much was
// kept, in characters, and how long the call ran. The call was not
// answered: no usage, no step counted, no retry, nothing unconfirmed.
func (e *Env) preempted(t *task.TCB, c mem.CommitSet, by string, ran time.Duration, kept string, prefill bool) mem.CommitSet {
	if prefill {
		t.Partial += kept
	} else {
		t.Partial, kept = "", ""
	}
	t.Preempts++
	e.ready(t, task.StepModel)
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvPreempted, t.ID, map[string]any{
		"by": by, "kept": runes(kept), "ran_ms": max(ran.Milliseconds(), 0), "n": t.Preempts,
	}))
	return c
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
	if msg.TextContent() != "" || len(msg.ToolCalls()) > 0 {
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
// included. The same priority commits nothing. On error t is unchanged; on
// success the caller hands it back, and then runs the preemption check.
func (e *Env) SetPriority(ctx context.Context, t *task.TCB, p int) error {
	if t.Priority == p {
		return nil
	}
	moved := t.Clone()
	moved.Priority = p
	moved.Version++
	ev := proto.NewEvent(proto.EvPrioritySet, t.ID, map[string]any{"priority": p, "was": t.Priority})
	if err := e.Commit(ctx, mem.CommitSet{Task: moved, Events: []proto.Event{ev}}); err != nil {
		return err
	}
	*t = *moved
	return nil
}
