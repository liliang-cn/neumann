package core

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/proto"
)

// cores is the bookkeeping of elastic vCores (design §6.1): the context they
// fetch under, the goroutines alive, and whether new ones may start. The
// counts that decide when one starts or retires are each group's Scaler's.
type cores struct {
	mu     sync.Mutex // guards open, closed and wg.Add
	open   bool       // StartCores has run
	closed bool       // CloseCores has run: nothing starts any more
	ctx    context.Context
	wg     sync.WaitGroup
	seq    atomic.Int64 // vCore ids, machine-wide, from 1
}

// StartCores lets vCores start, fetching under ctx (cancelling it stops
// fetching, as Stop does), and starts the ones the ready queues ask for.
// From here on a task joining a ready queue grows its class's vCores (the
// scheduler's OnReady calls Grow).
func (e *Env) StartCores(ctx context.Context) {
	e.cores.mu.Lock()
	e.cores.open, e.cores.ctx = true, ctx
	e.cores.mu.Unlock()
	for class := range e.Groups {
		e.Grow(class)
	}
}

// CloseCores stops vCores from starting; the ones alive exit once the
// scheduler closes. WaitCores waits for them.
func (e *Env) CloseCores() {
	e.cores.mu.Lock()
	e.cores.closed = true
	e.cores.mu.Unlock()
}

// WaitCores waits for every vCore to exit. Call it after CloseCores and after
// the scheduler is closed.
func (e *Env) WaitCores() { e.cores.wg.Wait() }

// Grow starts the vCores class's group needs for the work it has: the
// Scaler decides how many (design §6.1: a backlog, every vCore busy, fewer
// vCores than floor(W)), and each counts as live from here. It is called
// when a task joins the class's ready queue, when a vCore takes an
// instruction (there may be more work than idle vCores), and when one
// retires idle. It never blocks: each vCore announces itself (core_up) on its
// own goroutine.
func (e *Env) Grow(class string) {
	g := e.groupOf(class)
	if g == nil {
		return
	}
	e.cores.mu.Lock()
	defer e.cores.mu.Unlock()
	if !e.cores.open || e.cores.closed || e.cores.ctx.Err() != nil {
		return
	}
	k, reason := g.Ctl.Scaler.Grow(e.Sched.ReadyLen(class))
	for range k {
		id := int(e.cores.seq.Add(1))
		e.cores.wg.Add(1)
		go e.runCore(g, id, reason)
	}
}

// runCore is one elastic vCore of group g (design §5, §6.1): fetch, execute,
// commit, and between two instructions ask the group's Scaler whether to go
// on. It retires only there, never in the middle of an instruction (§18 test
// 19):
//
//   - after an instruction (Release), when the group has more vCores than
//     floor(W): reason overload, or manual when an operator pinned W;
//   - after waiting IdleRetire for work (IdleTimeout), when the group has
//     more than Min: reason idle;
//   - when the scheduler closes or the fetch context ends: reason shutdown.
//
// Each start and retirement is a machine event (core_up / core_down),
// committed on its own.
func (e *Env) runCore(g *Group, id int, up group.Reason) {
	defer e.cores.wg.Done()
	sc := g.Ctl.Scaler
	e.coreEvent(proto.EvCoreUp, g, id, up)
	for {
		since := e.Clock.Now()
		t, ok, idle := e.Sched.NextIdle(e.cores.ctx, g.Class, sc.IdleRetire())
		if !ok {
			if !idle {
				e.coreEvent(proto.EvCoreDown, g, id, sc.Stop())
				return
			}
			if retire, reason := sc.IdleTimeout(e.Clock.Now().Sub(since)); retire {
				e.coreEvent(proto.EvCoreDown, g, id, reason)
				e.Grow(g.Class) // work that came as it retired finds a vCore
				return
			}
			continue
		}
		sc.Acquire()
		e.Grow(g.Class)
		e.step(e.halt(), t, id)
		if retire, reason := sc.Release(); retire {
			e.coreEvent(proto.EvCoreDown, g, id, reason)
			return
		}
	}
}

func (e *Env) coreEvent(kind string, g *Group, id int, reason group.Reason) {
	live, _ := g.Ctl.Scaler.Counts()
	e.machineEvent(e.halt(), kind, map[string]any{"class": g.Class, "core": id, "reason": string(reason), "live": live})
}
