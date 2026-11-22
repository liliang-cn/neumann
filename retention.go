package neumann

import (
	"context"
	"time"

	"github.com/liliang-cn/neumann/mem"
)

// The pace of event retention (Config.EventRetention). A pass runs at Start
// and then every pruneEvery, or every EventRetention when that is shorter;
// it deletes at most pruneBatch events per store call and waits pruneGap
// (real time: it spreads load, it means nothing on the machine's clock)
// before the next, until a call deletes fewer. On mem/sqlite a call is one
// short transaction on the writer, so a commit that arrives meanwhile waits
// for at most one batch.
const (
	pruneEvery = time.Minute
	pruneBatch = 500
	pruneGap   = 20 * time.Millisecond
)

// startRetention starts the pruning loop, when Config.EventRetention asks
// for one. Stop stops it.
func (m *Machine) startRetention() {
	if m.cfg.EventRetention <= 0 {
		return
	}
	p := m.store.(mem.EventPruner) // New checked
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	m.retainStop, m.retainDone = cancel, done
	go func() {
		defer close(done)
		every := min(m.cfg.EventRetention, pruneEvery)
		for {
			m.prunePass(ctx, p)
			if !m.sleep(ctx, every) {
				return
			}
		}
	}()
}

// stopRetention cancels the pruning loop and waits for it, or for ctx: a
// store call in flight is short, and one that ignores its context is left
// to finish on its own.
func (m *Machine) stopRetention(ctx context.Context) {
	if m.retainStop == nil {
		return
	}
	m.retainStop()
	select {
	case <-m.retainDone:
	case <-ctx.Done():
	}
}

// prunePass deletes what is older than EventRetention on the machine's
// clock, a batch at a time. An error ends the pass, is logged, and never
// reaches a commit: the next pass tries again.
func (m *Machine) prunePass(ctx context.Context, p mem.EventPruner) {
	before := m.cfg.Clock.Now().Add(-m.cfg.EventRetention)
	for {
		n, err := p.PruneEvents(ctx, before, pruneBatch)
		m.eventsPruned.Add(int64(n))
		if err != nil {
			if ctx.Err() == nil {
				m.cfg.Logf("neumann: event retention: %v", err)
			}
			return
		}
		if n < pruneBatch {
			return
		}
		gap := time.NewTimer(pruneGap)
		select {
		case <-gap.C:
		case <-ctx.Done():
			gap.Stop()
			return
		}
	}
}

// sleep waits d on the machine's clock (a fake clock's timers drive it) and
// reports false if ctx ended first.
func (m *Machine) sleep(ctx context.Context, d time.Duration) bool {
	due := make(chan struct{})
	stop := m.sched.AfterFunc(d, func() { close(due) })
	defer stop()
	select {
	case <-due:
		return true
	case <-ctx.Done():
		return false
	}
}
