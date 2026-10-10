package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// What a task does when its group is down and the fallback cannot take it
// (design §6.3).
const (
	MigrateWait = "wait" // stay in Waiting(Group) until the group is back
	MigrateFail = "fail" // end Failed, StopReason "migrate_incompatible"
)

// StopMigrateIncompatible ends a task whose group is down, whose fallback
// cannot take it, and whose group says MigrateFail.
const StopMigrateIncompatible = "migrate_incompatible"

// The capability checks a move to another group must pass (design §6.3),
// named in the events and errors of a refusal.
const (
	CheckTools     = "tools"      // the task has tools and the target's model calls none
	CheckToolCount = "tool_count" // more tools than the target's model takes in one request
	CheckContext   = "context"    // System + Tools + Regs + the output reserve exceed the target's window
	CheckCaps      = "caps"       // the target's adapter panicked reporting what it can take
)

// MigrateError is a move refused by a capability check.
type MigrateError struct {
	From, To string
	Check    string // CheckTools, CheckToolCount or CheckContext
	Detail   string
}

func (e *MigrateError) Error() string {
	return fmt.Sprintf("cannot move from %q to %q: %s: %s", e.From, e.To, e.Check, e.Detail)
}

func (e *MigrateError) data() map[string]any {
	return map[string]any{"from": e.From, "to": e.To, "check": e.Check, "detail": e.Detail}
}

// fits runs the capability checks of a move of t to target (design §6.3),
// against what target's model reports (model.CapsOf):
//
//  1. a task with tools needs a model that calls tools, and no more tools
//     than it takes in one request;
//  2. the estimated System + Tools + Regs (as the target would be sent them,
//     without foreign reasoning) plus the output reserve (the task's MaxOut,
//     else the target's output cap, else DefaultMaxOut) must fit the target's
//     window. An unknown window (0) is never refused.
//
// The design's third check, whether chunked compaction on the target could
// make the task fit, needs StepCompact, which does not exist yet: a task that
// does not fit is refused.
func (e *Env) fits(t *task.TCB, target *Group) *MigrateError {
	a := e.adapter(target)
	refuse := func(check, detail string, args ...any) *MigrateError {
		return &MigrateError{From: t.Class, To: target.Class, Check: check, Detail: fmt.Sprintf(detail, args...)}
	}
	var caps model.Caps
	if err := guarded("Caps", func() { caps = model.CapsOf(a) }); err != nil {
		return refuse(CheckCaps, "%v", err)
	}
	if len(t.Tools) > 0 && !caps.Tools {
		return refuse(CheckTools, "the task has %d tools and %s calls none", len(t.Tools), a.Provider())
	}
	if caps.MaxTools > 0 && len(t.Tools) > caps.MaxTools {
		return refuse(CheckToolCount, "the task has %d tools and %s takes at most %d", len(t.Tools), a.Provider(), caps.MaxTools)
	}
	if caps.Window > 0 {
		out := t.MaxOut
		if out <= 0 {
			out = caps.MaxOut
		}
		if out <= 0 {
			out = DefaultMaxOut
		}
		in := task.EstimateTokens(t.System, t.Tools, forProvider(t.Regs, a.Provider()))
		if in+int64(out) > int64(caps.Window) {
			return refuse(CheckContext, "about %d tokens with %d reserved for output do not fit the window of %d, and compaction is not available",
				in, out, caps.Window)
		}
	}
	return nil
}

// groupGone is what becomes of t when its group g is down (design §6.2,
// §6.3): with no fallback, or pinned to its class, it waits for the group
// (Waiting(Group)). Otherwise it moves to the first group along the fallback
// chain that is up and passes the capability checks. When every group along
// the chain is down too, it waits; when one was up but could not take it,
// the group's OnMigrateFail decides: wait (with a migrate_refused event
// naming the failed check), or fail with StopReason "migrate_incompatible".
// evs are the step's events so far; they go into whichever commit it is.
func (e *Env) groupGone(t *task.TCB, c mem.CommitSet, g *Group, evs ...proto.Event) mem.CommitSet {
	if g == nil || g.Fallback == "" || t.PinClass {
		return park(t, c, evs...)
	}
	target, refused := e.fallbackFor(t, g)
	switch {
	case target != nil:
		return migrate(t, c, target.Class, "group_down", evs...)
	case refused == nil:
		return park(t, c, evs...)
	case g.OnMigrateFail == MigrateFail:
		c.Events = append(c.Events, evs...)
		t.State = task.Failed
		t.StopReason = StopMigrateIncompatible
		t.Version++
		c.Task = t
		d := refused.data()
		d["error"], d["stop_reason"] = refused.Error(), StopMigrateIncompatible
		c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID, d))
		return c
	}
	return park(t, c, append(evs, proto.NewEvent(proto.EvMigrateRefused, t.ID, refused.data()))...)
}

// fallbackFor walks g's fallback chain (acyclic: the machine checks) for the
// first group that is up and can take t. It returns the first refusal when
// no group could, and neither when every group along the chain is down.
func (e *Env) fallbackFor(t *task.TCB, g *Group) (*Group, *MigrateError) {
	seen := map[string]bool{g.Class: true}
	var first *MigrateError
	for name := g.Fallback; name != "" && !seen[name]; {
		seen[name] = true
		tg := e.groupOf(name)
		if tg == nil {
			break
		}
		if !e.groupDown(tg) {
			err := e.fits(t, tg)
			if err == nil {
				return tg, nil
			}
			if first == nil {
				first = err
			}
		}
		name = tg.Fallback
	}
	return nil, first
}

// migrate describes t moved to class: Ready for its next model step there,
// with its ReadySince kept so it does not go to the back of the new queue.
// Nothing else changes: the first request to the new provider leaves the old
// one's reasoning out (forProvider), so the provider's prompt cache misses
// once.
func migrate(t *task.TCB, c mem.CommitSet, class, reason string, evs ...proto.Event) mem.CommitSet {
	from := t.Class
	t.Class = class
	t.State, t.Wait, t.PC, t.WakeAt = task.Ready, task.WaitNone, task.StepModel, time.Time{}
	t.Version++
	c.Task = t
	c.Events = append(c.Events, evs...)
	c.Events = append(c.Events, proto.NewEvent(proto.EvMigrated, t.ID, map[string]any{"from": from, "to": class, "reason": reason}))
	return c
}

// SetClass moves a task the caller holds (checked out of the scheduler) to
// class: the operator's manual migration (design §9.6), under the same
// capability checks as a fallback move; a refusal is a *MigrateError and
// changes nothing. PinClass does not stop it: it only keeps the machine from
// moving the task on its own. A task waiting for the group it leaves is Ready
// again; any other keeps its state (a backoff keeps its wake-up time). On
// error t is unchanged; on success the caller hands it back.
func (e *Env) SetClass(ctx context.Context, t *task.TCB, class string) error {
	target := e.groupOf(class)
	if target == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	if t.Class == class {
		return nil
	}
	if err := e.fits(t, target); err != nil {
		return err
	}
	moved := t.Clone()
	moved.Class = class
	if moved.State == task.Waiting && moved.Wait == task.WaitGroup {
		moved.State, moved.Wait = task.Ready, task.WaitNone
	}
	moved.Version++
	ev := proto.NewEvent(proto.EvMigrated, t.ID, map[string]any{"from": t.Class, "to": class, "reason": "manual"})
	if err := e.Commit(ctx, mem.CommitSet{Task: moved, Events: []proto.Event{ev}}); err != nil {
		return err
	}
	*t = *moved
	return nil
}

// Rehome gives a recovered task whose class the machine no longer runs to
// class, the default group (design §4.5), before Recover settles it. It
// passes the same capability checks as any move: the task is then committed
// in class, its state unchanged, with a migrated event (reason
// class_removed). Otherwise a cancel whose reason is class_removed is
// committed with a migrate_refused event naming the failed check, and
// Recover's final commit ends the task Failed with that StopReason, settling
// its calls as for any cancel. A task with a cancel already pending is left
// to Recover. t is the caller's, as for Recover.
func (e *Env) Rehome(ctx context.Context, t *task.TCB, class string) error {
	target := e.groupOf(class)
	if target == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	if refused := e.fits(t, target); refused != nil {
		in := mem.Interrupt{TaskID: t.ID, Kind: mem.InterruptCancel, Text: ReasonClassRemoved}
		return e.Commit(ctx, mem.CommitSet{
			NewInterrupts: []mem.Interrupt{in},
			Events:        []proto.Event{proto.NewEvent(proto.EvMigrateRefused, t.ID, refused.data())},
		})
	}
	moved := t.Clone()
	moved.Class = class
	moved.Version++
	ev := proto.NewEvent(proto.EvMigrated, t.ID, map[string]any{"from": t.Class, "to": class, "reason": ReasonClassRemoved})
	err := e.Commit(ctx, mem.CommitSet{Task: moved, Events: []proto.Event{ev}})
	switch {
	case errors.Is(err, mem.ErrCancelPending):
		return nil // cancelled before: Recover finalises it where it is
	case err != nil:
		return err
	}
	*t = *moved
	return nil
}

// forProvider is msgs as a request to provider carries them: a reasoning
// part another provider produced is left out (design §3.1), since its
// signature is bound to that provider and no other can read it. A message
// left with no parts is left out too, and when that puts two messages of one
// role side by side, they are joined into one, so a provider that wants the
// roles to alternate is never sent anything else. The registers keep
// everything: a task moved back finds its reasoning again. msgs is returned
// as it is when there is nothing to leave out.
func forProvider(msgs []task.Message, provider string) []task.Message {
	foreign := func(p task.Part) bool {
		return p.Kind == task.PartReasoning && p.Provider != "" && p.Provider != provider
	}
	has := func(m task.Message) bool { return slices.ContainsFunc(m.Parts, foreign) }
	i := slices.IndexFunc(msgs, has)
	if i < 0 {
		return msgs
	}
	out := make([]task.Message, i, len(msgs))
	copy(out, msgs[:i])
	join := false // a message was left out just before this one
	for _, m := range msgs[i:] {
		if has(m) {
			m.Parts = slices.DeleteFunc(slices.Clone(m.Parts), foreign)
			if len(m.Parts) == 0 {
				join = true
				continue
			}
		}
		if n := len(out); join && n > 0 && out[n-1].Role == m.Role {
			out[n-1].Parts = append(slices.Clone(out[n-1].Parts), m.Parts...)
		} else {
			out = append(out, m)
		}
		join = false
	}
	return out
}
