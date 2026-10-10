package limit

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// waiter is one ReserveAll call queued on every limiter it needs. Only the
// waiter's own goroutine grants it, and only while it is at the head of all
// of those queues, so no two goroutines ever decide the same waiter's fate.
type waiter struct {
	wake chan struct{} // buffered 1: a notification is never lost or blocked on
}

// Reservation is room taken in one or more limiters. Exactly one of Settle or
// Cancel takes effect; later calls to either are no-ops.
type Reservation struct {
	at    time.Time
	mu    sync.Mutex
	done  bool
	parts []part
}

type part struct {
	l        *Limiter
	entries  []*entry // by dimension index; nil where nothing was reserved
	reserved []int64
}

// Reserve is ReserveAll on this limiter alone.
func (l *Limiter) Reserve(ctx context.Context, amounts map[string]int64) (*Reservation, error) {
	return ReserveAll(ctx, []*Limiter{l}, amounts)
}

// ReserveAll takes amounts from every limiter in ls at once, or nothing.
// Each limiter meters the dimensions it has and ignores the rest. It blocks
// until every dimension of every limiter has room and every caller that
// queued earlier on any of them has been served; it returns ctx.Err() with
// nothing reserved if ctx ends first, and ErrTooLarge at once if an amount
// exceeds a dimension's Limit.
//
// Limiters are locked in a fixed global order and a waiter joins all of its
// queues in one step, so callers naming the same limiters in different orders
// cannot deadlock, and the queues agree on who came first: the earliest
// waiter is at the head of every queue it is in, and always makes progress.
func ReserveAll(ctx context.Context, ls []*Limiter, amounts map[string]int64) (*Reservation, error) {
	set, err := lockOrder(ls)
	if err != nil {
		return nil, err
	}
	needs := make([][]int64, len(set))
	for name, n := range amounts {
		if n < 0 {
			return nil, fmt.Errorf("limit: negative amount %d for %q", n, name)
		}
	}
	for i, l := range set {
		needs[i] = make([]int64, len(l.dims))
		for j, d := range l.dims {
			n := amounts[d.name]
			if n > d.rate.Limit {
				return nil, fmt.Errorf("%w: limiter %q dimension %q: %d > %d", ErrTooLarge, l.name, d.name, n, d.rate.Limit)
			}
			needs[i][j] = n
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(set) == 0 {
		return &Reservation{at: time.Now()}, nil
	}

	lockAll(set)
	if allEmpty(set) {
		if d, _ := waitFor(set, needs); d <= 0 {
			r := grant(set, needs)
			unlockAll(set)
			return r, nil
		}
	}
	// Join every queue in the same locked step that found no room, so no
	// release can slip between the check and the queueing unnoticed.
	w := &waiter{wake: make(chan struct{}, 1)}
	for _, l := range set {
		l.queue = append(l.queue, w)
	}
	for {
		// Locked here. Only the head of every queue may be granted; anyone
		// else waits for the head of some queue to leave, which wakes it.
		var timer <-chan time.Time
		if headOfAll(set, w) {
			d, clk := waitFor(set, needs)
			if d <= 0 {
				r := grant(set, needs)
				for _, l := range set {
					l.dequeue(w)
				}
				unlockAll(set)
				return r, nil
			}
			unlockAll(set)
			timer = clk.After(d)
		} else {
			unlockAll(set)
		}
		select {
		case <-ctx.Done():
			lockAll(set)
			for _, l := range set {
				l.dequeue(w)
			}
			unlockAll(set)
			return nil, ctx.Err()
		case <-w.wake:
		case <-timer:
		}
		lockAll(set)
	}
}

func lockOrder(ls []*Limiter) ([]*Limiter, error) {
	set := make([]*Limiter, 0, len(ls))
	for _, l := range ls {
		if l == nil {
			return nil, errors.New("limit: nil limiter")
		}
		if !slices.Contains(set, l) {
			set = append(set, l)
		}
	}
	slices.SortFunc(set, func(a, b *Limiter) int { return cmp.Compare(a.id, b.id) })
	return set, nil
}

func lockAll(set []*Limiter) {
	for _, l := range set {
		l.mu.Lock()
	}
}

func unlockAll(set []*Limiter) {
	for i := len(set) - 1; i >= 0; i-- {
		set[i].mu.Unlock()
	}
}

func allEmpty(set []*Limiter) bool {
	for _, l := range set {
		if len(l.queue) > 0 {
			return false
		}
	}
	return true
}

func headOfAll(set []*Limiter, w *waiter) bool {
	for _, l := range set {
		if len(l.queue) == 0 || l.queue[0] != w {
			return false
		}
	}
	return true
}

// waitFor is how long until needs fit everywhere, and the clock to wait on.
// Called with every limiter in set locked.
func waitFor(set []*Limiter, needs [][]int64) (time.Duration, Clock) {
	var longest time.Duration
	clk := set[0].clock
	for i, l := range set {
		now := l.clock.Now()
		w := l.paused.Sub(now)
		for j, d := range l.dims {
			d.prune(now)
			w = max(w, d.wait(now, needs[i][j]))
		}
		if w > longest {
			longest, clk = w, l.clock
		}
	}
	return longest, clk
}

// grant charges needs to every limiter. Called with every limiter in set
// locked, after waitFor said they fit.
func grant(set []*Limiter, needs [][]int64) *Reservation {
	r := &Reservation{parts: make([]part, len(set))}
	for i, l := range set {
		now := l.clock.Now()
		if i == 0 {
			r.at = now
		}
		p := part{l: l, entries: make([]*entry, len(l.dims)), reserved: needs[i]}
		for j, d := range l.dims {
			p.entries[j] = d.charge(now, needs[i][j])
		}
		r.parts[i] = p
	}
	return r
}

// dequeue removes w from the queue and, if the head changed, tells the new
// head to look again.
func (l *Limiter) dequeue(w *waiter) {
	i := slices.Index(l.queue, w)
	if i < 0 {
		return
	}
	l.queue = slices.Delete(l.queue, i, i+1)
	if i == 0 {
		l.notifyHead()
	}
}

// notifyHead wakes the waiter at the head of the queue: it is the only one
// that can be granted, so it is the only one that needs to recheck.
func (l *Limiter) notifyHead() {
	if len(l.queue) == 0 {
		return
	}
	select {
	case l.queue[0].wake <- struct{}{}:
	default:
	}
}

// Time is when the reservation was granted.
func (r *Reservation) Time() time.Time { return r.at }

// Settle replaces the reserved amounts with what the request actually used.
// A dimension used less than reserved is refunded at once (if its entry is
// still in the window); one used more is charged the difference now, which
// may push the window over its limit and delay later reservations.
// Dimensions absent from actual keep their reserved amount.
func (r *Reservation) Settle(actual map[string]int64) {
	if !r.finish() {
		return
	}
	for _, p := range r.parts {
		l := p.l
		l.mu.Lock()
		now := l.clock.Now()
		refunded := false
		for j, d := range l.dims {
			a, ok := actual[d.name]
			if !ok {
				continue
			}
			a = max(a, 0)
			d.prune(now)
			switch res := p.reserved[j]; {
			case a < res:
				d.refund(now, p.entries[j], res-a)
				refunded = true
			case a > res:
				d.charge(now, a-res)
			}
		}
		if refunded {
			l.notifyHead()
		}
		l.mu.Unlock()
	}
}

// Cancel refunds the whole reservation: the request never reached the
// provider.
func (r *Reservation) Cancel() {
	if !r.finish() {
		return
	}
	for _, p := range r.parts {
		l := p.l
		l.mu.Lock()
		now := l.clock.Now()
		for j, d := range l.dims {
			d.prune(now)
			d.refund(now, p.entries[j], p.reserved[j])
		}
		l.notifyHead()
		l.mu.Unlock()
	}
}

func (r *Reservation) finish() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	r.done = true
	return true
}
