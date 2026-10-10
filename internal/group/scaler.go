package group

import (
	"sync"
	"time"
)

// DefaultIdleRetire is how long a vCore may sit idle before it retires.
const DefaultIdleRetire = 30 * time.Second

// Reason is why a vCore started or stopped: the reason field of a core_up /
// core_down event (§6.1).
type Reason string

const (
	ReasonDemand   Reason = "demand"   // core_up: backlog, all cores busy, n < floor(W)
	ReasonOverload Reason = "overload" // core_down: n > floor(W) after the window shrank
	ReasonIdle     Reason = "idle"     // core_down: idle for IdleRetire with n > Min
	ReasonManual   Reason = "manual"   // core_down: n > a pinned SetCores value
	ReasonShutdown Reason = "shutdown" // core_down: the machine is stopping
)

// The three pure rules. Scaler applies them under a lock; they are exported so
// the rules can be read, and tested, without any state.

// ScaleUp is how many vCores to start: none unless the ready queue is
// non-empty and every running vCore is busy; then enough to reach target,
// but no more than there is work for.
func ScaleUp(running, busy, ready, target int) int {
	if ready <= 0 || busy < running || running >= target {
		return 0
	}
	return min(target-running, ready)
}

// RetireAfterInstruction is whether a vCore that just finished an instruction
// should retire: the group has more cores than floor(W).
func RetireAfterInstruction(running, target int) bool { return running > target }

// RetireIdle is whether an idle vCore should retire. It never takes the group
// below Min and never touches W.
func RetireIdle(running, min int, idle, after time.Duration) bool {
	return running > min && idle >= after
}

// Scaler owns the live and busy vCore counts of one group and decides, against
// its Window, when a vCore starts or retires. Each decision and the count
// change it implies happen under one lock, so two vCores finishing together
// cannot both take the last retirement, and two Grow calls cannot both fill
// the same gap.
//
// Lifecycle of a vCore against the scaler:
//
//	Grow(ready) → k     reserve k new cores (running += k); start k goroutines
//	Acquire()           the core took an instruction (busy++)
//	Release() → retire  the instruction finished (busy--); on retire the core exits
//	IdleTimeout(d)      an idle core waited d for work; on retire it exits
//	Stop()              the core exits on shutdown
//
// A reserved core counts as running but not busy until it Acquires, so a
// burst of Grow calls before the new cores pick up work starts nothing more.
type Scaler struct {
	mu         sync.Mutex
	w          *Window
	idleRetire time.Duration
	running    int
	busy       int
}

// NewScaler builds a scaler for w. idleRetire <= 0 means DefaultIdleRetire.
func NewScaler(w *Window, idleRetire time.Duration) *Scaler {
	if idleRetire <= 0 {
		idleRetire = DefaultIdleRetire
	}
	return &Scaler{w: w, idleRetire: idleRetire}
}

// Grow reserves the vCores to start for a ready queue of length ready and
// returns how many, with ReasonDemand. The caller must start exactly that
// many; each counts as running immediately.
//
// Min is a floor for retirement only: Grow does not pre-start Min cores with
// no work, so a group with nothing to do costs nothing until its first task.
func (s *Scaler) Grow(ready int) (int, Reason) {
	target := s.w.Target()
	s.mu.Lock()
	defer s.mu.Unlock()
	k := ScaleUp(s.running, s.busy, ready, target)
	s.running += k
	return k, ReasonDemand
}

// Acquire marks a vCore busy. Call it when the core takes an instruction.
func (s *Scaler) Acquire() {
	s.mu.Lock()
	s.busy++
	s.mu.Unlock()
}

// Release marks a vCore idle after an instruction and decides whether it
// retires. When it does, the core is already uncounted and must exit without
// taking another instruction (§18 test 19: vCores retire only between
// instructions). The reason is ReasonManual when the window is pinned and
// ReasonOverload otherwise: W only shrinks below n on an overload halving or
// an operator's SetCores.
func (s *Scaler) Release() (retire bool, reason Reason) {
	target, auto := s.w.control()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy > 0 {
		s.busy--
	}
	if !RetireAfterInstruction(s.running, target) {
		return false, ""
	}
	s.running--
	if !auto {
		return true, ReasonManual
	}
	return true, ReasonOverload
}

// IdleTimeout is called by an idle vCore that has waited idle for work. It
// retires the core (ReasonIdle) when idle >= IdleRetire and the group has
// more than Min cores. W is not changed.
func (s *Scaler) IdleTimeout(idle time.Duration) (retire bool, reason Reason) {
	min := s.w.Snapshot().Min
	s.mu.Lock()
	defer s.mu.Unlock()
	if !RetireIdle(s.running, min, idle, s.idleRetire) {
		return false, ""
	}
	s.running--
	return true, ReasonIdle
}

// IdleRetire is the configured idle timeout, for the vCore's wait.
func (s *Scaler) IdleRetire() time.Duration { return s.idleRetire }

// Stop uncounts one idle vCore that is exiting because the machine stops.
func (s *Scaler) Stop() Reason {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running > 0 {
		s.running--
	}
	return ReasonShutdown
}

// Counts returns the live and busy vCore counts.
func (s *Scaler) Counts() (running, busy int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running, s.busy
}
