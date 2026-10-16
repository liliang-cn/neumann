package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Group is one execution group as the vCore loop uses it (design §6): its
// model, its window, scaler and breaker, and the limiters every request of
// the group takes room in (§7). There is one per class; the machine builds
// them. Its breaker state is not durable: a restarted machine starts every
// group closed.
type Group struct {
	Class    string
	Model    model.Adapter // nil: the Env's
	Ctl      *group.Group
	Limiters []*limit.Limiter
	Limits   []string      // the limiters' names, as configured
	OpenFor  time.Duration // the breaker's open time; 0 = group.DefaultBreakerOpenFor

	// Fallback is the class a task of this group moves to while the group
	// is down (design §6.3); empty: it waits. OnMigrateFail is what a task
	// does when the fallback cannot take it: MigrateWait (the default) or
	// MigrateFail.
	Fallback      string
	OnMigrateFail string

	// CompactAt is the share of the model's window above which a task is
	// compacted before its next model step, CompactKeep the share its
	// newest rounds kept verbatim may take (design §5.3); 0 takes
	// DefaultCompactAt and DefaultCompactKeep.
	CompactAt   float64
	CompactKeep float64

	// Preempt lets a model call of the group be cut off for a more urgent
	// task of the group when it has no vCore free (design §6.4): after the
	// call has run PreemptAfter (0: DefaultPreemptAfter), and at most
	// MaxPreempts times in one step of a task (0: DefaultMaxPreempts).
	// With it on, every model call is streamed when the adapter can, so
	// the text a cut-off call produced is kept for the next call to
	// continue.
	Preempt      bool
	PreemptAfter time.Duration
	MaxPreempts  int

	pre preemption

	mu   sync.Mutex
	kick func() bool // the half-open timer, armed while tasks wait for the group
}

func (g *Group) openFor() time.Duration {
	if g.OpenFor > 0 {
		return g.OpenFor
	}
	return group.DefaultBreakerOpenFor
}

// adapter is the model a group's requests go to: its own, else the Env's.
func (e *Env) adapter(g *Group) model.Adapter {
	if g != nil && g.Model != nil {
		return g.Model
	}
	return e.Model
}

// groupOf is the group of a class; nil when the class has none (a task of a
// class this machine does not run, or an Env built without groups).
func (e *Env) groupOf(class string) *Group { return e.Groups[class] }

// groupDown reports whether a request of the group would be refused now:
// the breaker is not closed and no probe is due. It changes nothing, so a
// task fetched while the group is down waits for it without taking room in
// a limiter first; Admit, after the reservation, is the call that decides.
func (e *Env) groupDown(g *Group) bool {
	if g == nil {
		return false
	}
	st := g.Ctl.Breaker.Snapshot()
	if st.State == group.Closed {
		return false
	}
	return st.NextProbe.IsZero() || e.Clock.Now().Before(st.NextProbe)
}

// park describes t waiting for its group to come back (Waiting(Group)): no
// model call is made. Its next instruction is the same model instruction (a
// compaction pass resumes where it was).
func park(t *task.TCB, c mem.CommitSet, evs ...proto.Event) mem.CommitSet {
	t.State, t.Wait, t.PC = task.Waiting, task.WaitGroup, modelPC(t)
	t.WakeAt = time.Time{}
	t.Version++
	c.Task = t
	c.Events = append(c.Events, evs...)
	return c
}

// afterPark runs once a task parked in Waiting(Group) is in the wait set. If
// the group came back between the decision and the hand-back, the group_up
// that woke the others missed it: it is woken here. So is a task that parked
// because every group along its fallback chain was down (movable) when one of
// them has come back since: it gets to move. Otherwise the half-open timer is
// armed, so a probe goes out when one is due.
func (e *Env) afterPark(ctx context.Context, id string, g *Group, movable bool) {
	if g == nil || g.Ctl.Breaker.State() == group.Closed || (movable && e.fallbackUp(g)) {
		e.Wake(ctx, id, task.WaitGroup)
		return
	}
	e.armKick(g, -1)
}

// setRefused records whether a task parked after a fallback refused it.
func (e *Env) setRefused(id string, refused bool) {
	e.refusedMu.Lock()
	defer e.refusedMu.Unlock()
	if !refused {
		delete(e.refused, id)
		return
	}
	if e.refused == nil {
		e.refused = map[string]bool{}
	}
	e.refused[id] = true
}

func (e *Env) isRefused(id string) bool {
	e.refusedMu.Lock()
	defer e.refusedMu.Unlock()
	return e.refused[id]
}

// fallbackUp reports whether a group along g's fallback chain is up.
func (e *Env) fallbackUp(g *Group) bool {
	seen := map[string]bool{g.Class: true}
	for name := g.Fallback; name != "" && !seen[name]; {
		seen[name] = true
		fg := e.groupOf(name)
		if fg == nil {
			return false
		}
		if !e.groupDown(fg) {
			return true
		}
		name = fg.Fallback
	}
	return false
}

// fallsBackTo reports whether up is along h's fallback chain.
func (e *Env) fallsBackTo(h, up *Group) bool {
	seen := map[string]bool{h.Class: true}
	for name := h.Fallback; name != "" && !seen[name]; {
		if name == up.Class {
			return true
		}
		seen[name] = true
		fg := e.groupOf(name)
		if fg == nil {
			return false
		}
		name = fg.Fallback
	}
	return false
}

// armKick sets the group's half-open timer, unless one is set: in d, or when
// the breaker's next probe is due for d < 0. A breaker open for quota never
// half-opens, so nothing is armed for it.
func (e *Env) armKick(g *Group, d time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.kick != nil {
		return
	}
	st := g.Ctl.Breaker.Snapshot()
	if st.State == group.Closed || st.Cause == group.CauseQuota {
		return
	}
	if d < 0 {
		d = st.NextProbe.Sub(e.Clock.Now())
	}
	g.kick = e.Sched.AfterFunc(d, func() { e.onKick(g) })
}

func (e *Env) stopKick(g *Group) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.kick != nil {
		g.kick()
		g.kick = nil
	}
}

// onKick is the half-open timer. The tasks of a down group sit in the wait
// set and nothing fetches them, so when a probe is due the oldest is woken to
// make it; the timer is armed again in case that probe is lost (its task
// cancelled first), and a group_up wakes the rest.
func (e *Env) onKick(g *Group) {
	g.mu.Lock()
	g.kick = nil
	g.mu.Unlock()
	if e.Guard != nil {
		release, ok := e.Guard()
		if !ok {
			return
		}
		defer release()
	}
	ctx := e.halt()
	st := g.Ctl.Breaker.Snapshot()
	switch {
	case st.State == group.Closed:
		e.wakeGroup(ctx, g)
		return
	case st.Cause == group.CauseQuota:
		return
	case !st.NextProbe.IsZero() && e.Clock.Now().Before(st.NextProbe):
		e.armKick(g, -1)
		return
	}
	for _, id := range e.Sched.WaitingFor(g.Class, task.WaitGroup) {
		if e.Wake(ctx, id, task.WaitGroup) {
			e.armKick(g, g.openFor())
			return
		}
	}
}

// wakeGroup makes every task waiting for the group Ready again, and every
// task waiting for a group whose fallback chain reaches it that parked
// because every group along its chain was down: it is woken to try the move
// again (design §6.3). A task pinned to its class is not, and neither is one
// a fallback refused (migrate_refused): what it can take does not change when
// it comes back, so that task waits for its own group, or an operator's
// SetClass.
func (e *Env) wakeGroup(ctx context.Context, g *Group) {
	e.stopKick(g)
	for _, id := range e.Sched.WaitingFor(g.Class, task.WaitGroup) {
		e.Wake(ctx, id, task.WaitGroup)
	}
	for _, h := range e.Groups {
		if h == g || !e.fallsBackTo(h, g) {
			continue
		}
		for _, id := range e.Sched.WaitingFor(h.Class, task.WaitGroup) {
			e.wakeIf(ctx, id, task.WaitGroup, func(t *task.TCB) bool { return !t.PinClass && !e.isRefused(t.ID) })
		}
	}
}

// Wake makes a task in Waiting(why) Ready again: it is checked out of the
// wait set, committed Ready with its ReadySince kept (so it does not lose its
// place in the queue), and handed back. It reports whether it did. A task no
// longer in that wait (cancelled, woken by someone else) is left alone; a
// cancel committed first makes the store refuse the commit, and the task is
// finalised instead.
func (e *Env) Wake(ctx context.Context, id string, why task.WaitReason) bool {
	return e.wakeIf(ctx, id, why, func(*task.TCB) bool { return true })
}

// wakeIf is Wake for a task that also satisfies ok.
func (e *Env) wakeIf(ctx context.Context, id string, why task.WaitReason, ok func(*task.TCB) bool) bool {
	t, found := e.Sched.CheckoutIf(id, func(t *task.TCB) bool { return t.State == task.Waiting && t.Wait == why && ok(t) })
	if !found {
		return false
	}
	e.setRefused(id, false)
	if e.halt().Err() != nil {
		e.Sched.Forget(id)
		return false
	}
	ctx = context.WithoutCancel(ctx)
	wakeAt := t.WakeAt
	t.State, t.Wait, t.WakeAt = task.Ready, task.WaitNone, time.Time{}
	t.Version++
	if err := e.Commit(ctx, mem.CommitSet{Task: t}); err != nil {
		t.State, t.Wait, t.WakeAt = task.Waiting, why, wakeAt
		t.Version--
		if errors.Is(err, mem.ErrCancelPending) {
			e.finalise(ctx, t, mem.CommitSet{}, nil, ReasonCancelled)
			return false
		}
		e.Logf("neumann: waking task %s failed; it is no longer scheduled: %v", id, err)
		e.Sched.Forget(id)
		return false
	}
	return e.handBack(ctx, t, nil)
}

// report applies a call's outcome to its group and acts on what changed: a
// group going down is announced; a group coming back is announced and every
// task waiting for it is woken. The events are machine events, committed on
// their own: the breaker is not durable, so there is no state to commit them
// with.
func (e *Env) report(ctx context.Context, g *Group, tk group.Ticket, o group.Outcome) {
	eff := g.Ctl.Report(tk, o)
	switch {
	case eff.Breaker.Down():
		e.machineEvent(ctx, proto.EvGroupDown, map[string]any{"class": g.Class, "cause": string(eff.Breaker.Cause)})
	case eff.Breaker.Up():
		e.machineEvent(ctx, proto.EvGroupUp, map[string]any{"class": g.Class})
		e.wakeGroup(ctx, g)
	}
}

func (e *Env) machineEvent(ctx context.Context, kind string, data any) error {
	err := e.Commit(context.WithoutCancel(ctx), mem.CommitSet{Events: []proto.Event{proto.NewEvent(kind, "", data)}})
	if err != nil && e.halt().Err() == nil {
		e.Logf("neumann: commit of %s event failed: %v", kind, err)
	}
	return err
}

// GroupReset closes a class's breaker, whatever opened it (the operator's
// group_reset, design §9.6), announces group_up if it was down, and wakes
// every task waiting for the group.
func (e *Env) GroupReset(ctx context.Context, class string) error {
	g := e.groupOf(class)
	if g == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	var err error
	if tr := g.Ctl.Breaker.Reset(); tr.Up() {
		err = e.machineEvent(ctx, proto.EvGroupUp, map[string]any{"class": class, "reset": true})
	}
	e.wakeGroup(ctx, g)
	return err
}
