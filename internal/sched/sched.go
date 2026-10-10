// Package sched owns every task that is not on a vCore: the ready queues (one
// per class, interactive before background, FIFO by ReadySince, with aging)
// and the wait set. Ownership of a TCB passes only through Add, Next, Checkout,
// Cancel, Requeue and Forget (design §8). Forget gives a held task up for good:
// it is used after a failed commit and on the halt paths.
//
// The scheduler also holds what may reach a task regardless of who owns it
// (design §8 rule 3, §9.4): its context, which Cancel ends so that a model
// call or tool call in flight stops at once, and its deadline timer. And it
// owns the timer of a task in Waiting(Backoff): due at WakeAt, it calls the
// wake handler, which checks the task out and commits it Ready.
package sched

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/task"
)

type Clock interface{ Now() time.Time }

// Timers is a Clock that also runs timers. A fake clock implements it so that
// deadlines fire when a test moves time; a Clock without it gets real timers,
// set for the time its Now says is left.
type Timers interface {
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

const (
	inReady uint8 = iota + 1
	inWaiting
)

const DefaultAging = 30 * time.Second

type Sched struct {
	mu      sync.Mutex
	cond    *sync.Cond
	clock   Clock
	aging   time.Duration
	ready   map[string][]*task.TCB
	waiting map[string]*task.TCB
	out     map[string]bool // held by a vCore or a tool batch
	loc     map[string]uint8
	closed  bool

	base       context.Context
	stopping   context.Context // ends at Close
	stop       context.CancelFunc
	ctl        map[string]*control
	onDeadline func(id string)
	onWake     func(id string)
}

// control is what reaches a task whoever owns it: its context, whether it has
// been cancelled, and its deadline timer.
type control struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled bool
	deadline  time.Time
	stopTimer func() bool // nil: no timer set
	stopWake  func() bool // the backoff timer while the task is in Waiting(Backoff)
}

func New(clock Clock, aging time.Duration) *Sched {
	if aging <= 0 {
		aging = DefaultAging
	}
	if clock == nil {
		clock = RealClock{}
	}
	s := &Sched{
		clock:   clock,
		aging:   aging,
		ready:   map[string][]*task.TCB{},
		waiting: map[string]*task.TCB{},
		out:     map[string]bool{},
		loc:     map[string]uint8{},
		base:    context.Background(),
		ctl:     map[string]*control{},
	}
	s.cond = sync.NewCond(&s.mu)
	s.stopping, s.stop = context.WithCancel(context.Background())
	return s
}

// Stopping ends when Close is called: work that only waits (a vCore waiting
// for room in a limiter, with nothing committed yet) gives its task back
// instead of holding the shutdown up.
func (s *Sched) Stopping() context.Context { return s.stopping }

// SetContext sets the context every task context derives from: ending it
// ends them all (the machine's halt). Call it before the first Add.
func (s *Sched) SetContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.base = ctx
}

// OnDeadline sets what runs when a task's deadline passes. f runs on a timer's
// goroutine, holds no lock, and owns nothing: it must go through Cancel. Set
// it before the first Add.
func (s *Sched) OnDeadline(f func(id string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDeadline = f
}

// OnWake sets what runs when a task in Waiting(Backoff) reaches its WakeAt.
// Like OnDeadline, f runs on a timer's goroutine, holds no lock and owns
// nothing: the task is still in the wait set, and f must check it out (it
// may be gone by then). Set it before the first Add.
func (s *Sched) OnWake(f func(id string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onWake = f
}

// AfterFunc runs f after d on the scheduler's clock: on its timers when it
// has them, real ones otherwise. stop cancels it.
func (s *Sched) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	if tm, ok := s.clock.(Timers); ok {
		return tm.AfterFunc(d, f)
	}
	return time.AfterFunc(d, f).Stop
}

// Add hands a task the caller owns to the scheduler. It reports false, as
// Requeue does, when the task was cancelled meanwhile.
func (s *Sched) Add(t *task.TCB) bool { return s.Requeue(t) }

// Requeue returns a task to the scheduler after its holder committed the
// task's new state. Ready tasks join their class's queue, Waiting and
// Suspended tasks the wait set, terminal tasks are forgotten.
//
// A task cancelled while it was held is not taken back: Requeue reports false
// and the caller still owns it, and must commit its final state (then Requeue
// it as terminal) or Forget it. Deciding this under the lock that Cancel takes
// means a cancel is never lost between the holder's commit and its hand-back.
func (s *Sched) Requeue(t *task.TCB) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.loc[t.ID]; dup {
		panic(fmt.Sprintf("sched: task %s is already queued or waiting", t.ID))
	}
	c := s.controlLocked(t.ID)
	if c.cancelled && !t.State.Terminal() {
		s.out[t.ID] = true
		return false
	}
	switch t.State {
	case task.Ready:
		s.ready[t.Class] = append(s.ready[t.Class], t)
		s.loc[t.ID] = inReady
	case task.Waiting, task.Suspended:
		// Cancel takes a task in backoff like any other wait that no batch
		// will come back for, and stops its wake-up timer.
		s.waiting[t.ID] = t
		s.loc[t.ID] = inWaiting
		if t.State == task.Waiting && t.Wait == task.WaitBackoff {
			s.armWakeLocked(t, c)
		}
	case task.Done, task.Failed, task.Cancelled:
		// terminal: forgotten
		s.dropLocked(t.ID)
	default:
		panic(fmt.Sprintf("sched: requeue of task %s in state %s", t.ID, t.State))
	}
	if !t.State.Terminal() {
		s.armLocked(t, c)
	}
	delete(s.out, t.ID)
	s.cond.Broadcast()
	return true
}

func (s *Sched) controlLocked(id string) *control {
	c, ok := s.ctl[id]
	if !ok {
		ctx, cancel := context.WithCancel(s.base)
		c = &control{ctx: ctx, cancel: cancel}
		s.ctl[id] = c
	}
	return c
}

// armLocked sets t's deadline timer the first time the scheduler sees it.
// The timer keeps running across every state the task passes through, on a
// vCore or off it, until the task ends.
func (s *Sched) armLocked(t *task.TCB, c *control) {
	c.deadline = t.Budget.Deadline
	if c.stopTimer != nil || t.Budget.Deadline.IsZero() || s.closed {
		return
	}
	id := t.ID
	fire := func() {
		s.mu.Lock()
		f := s.onDeadline
		s.mu.Unlock()
		if f != nil {
			f(id)
		}
	}
	d := t.Budget.Deadline.Sub(s.clock.Now())
	if tm, ok := s.clock.(Timers); ok {
		c.stopTimer = tm.AfterFunc(d, fire)
	} else {
		c.stopTimer = time.AfterFunc(d, fire).Stop
	}
}

// armWakeLocked sets the wake-up timer of a task entering Waiting(Backoff).
func (s *Sched) armWakeLocked(t *task.TCB, c *control) {
	s.disarmWakeLocked(c)
	if s.closed {
		return
	}
	id := t.ID
	c.stopWake = s.AfterFunc(t.WakeAt.Sub(s.clock.Now()), func() {
		s.mu.Lock()
		f := s.onWake
		s.mu.Unlock()
		if f != nil {
			f(id)
		}
	})
}

func (s *Sched) disarmWakeLocked(c *control) {
	if c != nil && c.stopWake != nil {
		c.stopWake()
		c.stopWake = nil
	}
}

// dropLocked releases what the scheduler held for a task that is gone.
func (s *Sched) dropLocked(id string) {
	c, ok := s.ctl[id]
	if !ok {
		return
	}
	if c.stopTimer != nil {
		c.stopTimer()
	}
	s.disarmWakeLocked(c)
	c.cancel()
	delete(s.ctl, id)
}

// Context is the task's context: in-flight work for the task runs under it.
// It ends when the task is cancelled, when the scheduler's own context ends,
// and when the task is gone.
func (s *Sched) Context(id string) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.controlLocked(id).ctx
}

// Deadline is the deadline of a task the scheduler holds, zero for none: a
// tool batch reads it to send nothing once it has passed.
func (s *Sched) Deadline(id string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.ctl[id]; ok {
		return c.deadline
	}
	return time.Time{}
}

// Cancelled reports whether Cancel was called for a task still held here.
func (s *Sched) Cancelled(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ctl[id]
	return ok && c.cancelled
}

// Cancel marks a task cancelled and ends its context, whoever owns it: a
// model call or tool call in flight for it stops at once. The caller must
// have committed the cancel first.
//
// The task's owner finalises it. When nobody is at work on it, that is the
// caller: a task in the ready queue, or in the wait set for anything but its
// tool batch (reconciliation, suspension, later backoff and group waits), is
// checked out and returned, and the caller must commit its final state and
// Requeue it, or Forget it. A task on a vCore, waiting on I/O, or held by a
// batch or an operator is left to its holder, who learns of the cancel from
// the context or when Requeue refuses the task. An unknown task (finished, or
// not handed to the scheduler yet) is left alone: its next instruction reads
// the committed cancel.
func (s *Sched) Cancel(id string) (*task.TCB, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ctl[id]
	if !ok {
		return nil, false
	}
	c.cancelled = true
	c.cancel()
	switch s.loc[id] {
	case inReady:
		for class, q := range s.ready {
			if i := slices.IndexFunc(q, func(t *task.TCB) bool { return t.ID == id }); i >= 0 {
				t := q[i]
				s.ready[class] = slices.Delete(q, i, i+1)
				delete(s.loc, id)
				s.out[id] = true
				return t, true
			}
		}
	case inWaiting:
		t := s.waiting[id]
		if t.State == task.Waiting && t.Wait == task.WaitIO {
			return nil, false // its tool batch checks it out and finalises it
		}
		delete(s.waiting, id)
		delete(s.loc, id)
		s.out[id] = true
		s.disarmWakeLocked(c)
		return t, true
	}
	return nil, false
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
	copy(q[best:], q[best+1:])
	q[len(q)-1] = nil
	s.ready[class] = q[:len(q)-1]
	delete(s.loc, t.ID)
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
	return s.CheckoutIf(id, func(*task.TCB) bool { return true })
}

// CheckoutIf is Checkout for a task that satisfies ok, decided under the
// scheduler's lock. A caller that may only take a task in one particular wait
// (an operator resolving a held call) must not take one whose tool batch is
// still running: that batch would find its task gone.
func (s *Sched) CheckoutIf(id string, ok func(*task.TCB) bool) (*task.TCB, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, found := s.waiting[id]
	if !found || !ok(t) {
		return nil, false
	}
	delete(s.waiting, id)
	delete(s.loc, id)
	s.out[id] = true
	s.disarmWakeLocked(s.ctl[id])
	return t, true
}

// WaitingFor lists the tasks of class in the wait set for reason w, oldest
// ReadySince first. A list, not the tasks: each must still be checked out.
func (s *Sched) WaitingFor(class string, w task.WaitReason) []string {
	// What it sorts by is copied under the lock: once a task leaves the wait
	// set, its new owner writes its TCB.
	type entry struct {
		since time.Time
		id    string
	}
	s.mu.Lock()
	var es []entry
	for _, t := range s.waiting {
		if t.Class == class && t.State == task.Waiting && t.Wait == w {
			es = append(es, entry{t.ReadySince, t.ID})
		}
	}
	s.mu.Unlock()
	slices.SortFunc(es, func(a, b entry) int {
		if c := a.since.Compare(b.since); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	ids := make([]string, len(es))
	for i, e := range es {
		ids[i] = e.id
	}
	return ids
}

// Forget gives up a held task for good: after a failed commit, or on a halt
// path. It is no longer scheduled; its context ends and its deadline timer
// stops.
func (s *Sched) Forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.out, id)
	s.dropLocked(id)
}

// Close makes every Next return false and stops every deadline and backoff
// timer. Requeue
// keeps working, so in-flight work can still hand its tasks back.
func (s *Sched) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.stop()
	for _, c := range s.ctl {
		if c.stopTimer != nil {
			c.stopTimer()
		}
		s.disarmWakeLocked(c)
	}
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
