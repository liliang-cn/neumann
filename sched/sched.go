// Package sched owns every task that is not on a vCore: the ready queues (one
// per class, interactive before background, FIFO by ReadySince, with aging)
// and the wait set. Ownership of a TCB passes only through Next, Checkout and
// Requeue (design §8).
package sched

import (
	"context"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/task"
)

type Clock interface{ Now() time.Time }

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

const DefaultAging = 30 * time.Second

type Sched struct {
	mu      sync.Mutex
	cond    *sync.Cond
	clock   Clock
	aging   time.Duration
	ready   map[string][]*task.TCB
	waiting map[string]*task.TCB
	out     map[string]bool // held by a vCore or a tool batch
	closed  bool
}

func New(clock Clock, aging time.Duration) *Sched {
	if aging <= 0 {
		aging = DefaultAging
	}
	s := &Sched{
		clock:   clock,
		aging:   aging,
		ready:   map[string][]*task.TCB{},
		waiting: map[string]*task.TCB{},
		out:     map[string]bool{},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Add hands a task the caller owns to the scheduler.
func (s *Sched) Add(t *task.TCB) { s.Requeue(t) }

// Requeue returns a task to the scheduler after its holder committed the
// task's new state. Ready tasks join their class's queue, Waiting and
// Suspended tasks the wait set, terminal tasks are forgotten.
func (s *Sched) Requeue(t *task.TCB) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.out, t.ID)
	switch t.State {
	case task.Ready:
		s.ready[t.Class] = append(s.ready[t.Class], t)
	case task.Waiting, task.Suspended:
		s.waiting[t.ID] = t
	}
	s.cond.Broadcast()
}

// Next blocks until a task of class is ready and hands it to the caller. It
// returns false once the scheduler is closed or ctx ends.
func (s *Sched) Next(ctx context.Context, class string) (*task.TCB, bool) {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed || ctx.Err() != nil {
			return nil, false
		}
		if t := s.pickLocked(class); t != nil {
			return t, true
		}
		s.cond.Wait()
	}
}

func (s *Sched) pickLocked(class string) *task.TCB {
	q := s.ready[class]
	if len(q) == 0 {
		return nil
	}
	now := s.clock.Now()
	best := 0
	for i := 1; i < len(q); i++ {
		if s.before(q[i], q[best], now) {
			best = i
		}
	}
	t := q[best]
	s.ready[class] = append(q[:best], q[best+1:]...)
	s.out[t.ID] = true
	return t
}

// effective is the priority used for ordering: a background task that has
// been ready for longer than the aging threshold competes as interactive.
func (s *Sched) effective(t *task.TCB, now time.Time) int {
	if t.Priority > 0 && now.Sub(t.ReadySince) >= s.aging {
		return 0
	}
	return t.Priority
}

func (s *Sched) before(a, b *task.TCB, now time.Time) bool {
	ea, eb := s.effective(a, now), s.effective(b, now)
	if ea != eb {
		return ea < eb
	}
	return a.ReadySince.Before(b.ReadySince)
}

// Checkout takes a waiting task out of the wait set so that its caller can
// commit a transition for it (for example, its tools finished).
func (s *Sched) Checkout(id string) (*task.TCB, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.waiting[id]
	if !ok {
		return nil, false
	}
	delete(s.waiting, id)
	s.out[id] = true
	return t, true
}

// Forget drops a held task whose commit failed: it is no longer scheduled.
func (s *Sched) Forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.out, id)
}

// Close makes every Next return false. Requeue keeps working, so in-flight
// work can still hand its tasks back.
func (s *Sched) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Broadcast()
}

// Counts reports tasks in ready queues, in the wait set, and held elsewhere.
func (s *Sched) Counts() (ready, waiting, held int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.ready {
		ready += len(q)
	}
	return ready, len(s.waiting), len(s.out)
}
