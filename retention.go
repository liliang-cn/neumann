package neumann

import (
	"context"
	"time"

	"github.com/liliang-cn/neumann/mem"
)

// The pace of event retention (Config.EventRetention). A pass runs at Start
// and then every pruneEvery, or every EventRetention when that is shorter,
// but never more often than every pruneEveryMin (the cutoff stays exact: a
// pass prunes what is older than EventRetention at the time it runs). A pass
// walks the old part of the log in store calls that each look at no more
// than pruneBatch old events, kept or deleted, going on from where the one
// before stopped, and waits pruneGap (real time: it spreads load, it means
// nothing on the machine's clock) between two calls. On mem/sqlite a call
// is one transaction on the writer whose cost does not depend on how many
// old events are kept before it, so a commit that arrives during it waits
// for that one call, about a millisecond on a laptop when it deletes nothing
// (BenchmarkPruneKeptOldRows), plus the fsync of its deletes when it does.
const (
	pruneEvery    = time.Minute
	pruneEveryMin = time.Second
	pruneBatch    = 500
	pruneGap      = 20 * time.Millisecond
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
		every := max(min(m.cfg.EventRetention, pruneEvery), pruneEveryMin)
		for {
			m.prunePass(ctx, p)
			if !m.sleep(ctx, every) {
				return
			}
		}
	}()
}

// stopRetention cancels the pruning loop and waits for it to end, whatever
// Stop's ctx says: the loop stops between two store calls, so the wait is
// at most one call, and once Stop returns nothing touches the store for
// retention. A store whose PruneEvents ignores its context and blocks holds
// Stop up with it.
func (m *Machine) stopRetention() {
	if m.retainStop == nil {
		return
	}
	m.retainStop()
	<-m.retainDone
}

// prunePass deletes what is older than EventRetention on the machine's
// clock, a call at a time. An error ends the pass, is logged, and never
// reaches a commit: the next pass tries again.
func (m *Machine) prunePass(ctx context.Context, p mem.EventPruner) {
	before := m.cfg.Clock.Now().Add(-m.cfg.EventRetention)
	after := int64(0)
	for {
		n, next, err := p.PruneEvents(ctx, before, after, pruneBatch)
		m.eventsPruned.Add(int64(n))
		if err != nil {
			if ctx.Err() == nil {
				m.cfg.Logf("neumann: event retention: %v", err)
			}
			return
		}
		if next == 0 {
			return
		}
		after = next
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
