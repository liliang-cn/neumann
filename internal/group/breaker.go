package group

import (
	"sync"
	"time"
)

const (
	DefaultBreakerThreshold = 5
	DefaultBreakerOpenFor   = 30 * time.Second
)

// State is a breaker state.
type State int

const (
	Closed   State = iota // requests flow
	Open                  // no requests
	HalfOpen              // one probe is out; everything else waits for its verdict
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Cause is why a breaker opened.
type Cause string

const (
	CauseTransient Cause = "transient" // Threshold consecutive transient failures; half-opens after OpenFor
	CauseQuota     Cause = "quota"     // quota exhausted; stays open until Reset
)

// Transition is a breaker state change. The zero value means nothing changed.
type Transition struct {
	Changed  bool
	From, To State
	Cause    Cause // the open cause after the change; empty when closed
}

// Down reports a closed group going down: emit group_down. Re-opening after a
// failed probe is not a new group_down — the group never came up.
func (t Transition) Down() bool { return t.Changed && t.From == Closed && t.To != Closed }

// Up reports the group coming back: emit group_up.
func (t Transition) Up() bool { return t.Changed && t.From != Closed && t.To == Closed }

// BreakerConfig configures a Breaker. Zero values take the defaults.
type BreakerConfig struct {
	Threshold int           // consecutive transient failures that open it; default 5
	OpenFor   time.Duration // open time before a transient-opened breaker half-opens; default 30s
	Clock     Clock
}

// Breaker is an execution group's circuit breaker (§6.2).
//
//   - Closed: every request is allowed. Threshold consecutive Transient
//     failures open it (CauseTransient); any Success resets the count.
//     QuotaExhausted opens it at once (CauseQuota).
//   - Open (transient): after OpenFor the next Allow half-opens it and
//     returns that one request as the probe.
//   - HalfOpen: every other Allow is refused until the probe reports. Success
//     closes (group_up); Transient reopens for another OpenFor.
//   - Open (quota): never half-opens. Only Reset (the operator's group_reset)
//     closes it.
//
// Results are not tied to a particular request: a straggler that was sent
// before the breaker opened and answers while it is half-open is taken as the
// probe's verdict, since it is just as fresh evidence of the provider's health.
// Results arriving while Open are ignored — a straggler success does not
// close a breaker, and a straggler failure does not extend it.
type Breaker struct {
	mu        sync.Mutex
	clock     Clock
	threshold int
	openFor   time.Duration

	state    State
	cause    Cause
	failures int
	openedAt time.Time
}

// NewBreaker builds a closed breaker from cfg.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = DefaultBreakerThreshold
	}
	if cfg.OpenFor <= 0 {
		cfg.OpenFor = DefaultBreakerOpenFor
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	return &Breaker{clock: cfg.Clock, threshold: cfg.Threshold, openFor: cfg.OpenFor}
}

// Allow reports whether one request may be sent. probe is true for exactly one
// caller per half-open period, however many call concurrently: the open →
// half-open step and handing out the probe happen under one lock.
func (b *Breaker) Allow() (ok, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true, false
	case Open:
		if b.cause == CauseTransient && !b.clock.Now().Before(b.openedAt.Add(b.openFor)) {
			b.state = HalfOpen
			return true, true
		}
	}
	return false, false
}

// Success records a successful call: it resets the consecutive-failure count
// and closes a half-open breaker.
func (b *Breaker) Success() Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		b.failures = 0
	case HalfOpen:
		return b.closeLocked()
	}
	return Transition{}
}

// Transient records a transient failure (other 5xx, network, timeout).
func (b *Breaker) Transient() Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		b.failures++
		if b.failures >= b.threshold {
			return b.openLocked(CauseTransient)
		}
	case HalfOpen:
		return b.openLocked(CauseTransient)
	}
	return Transition{}
}

// QuotaExhausted opens the breaker until Reset, from any state. A breaker
// already open for transient failures changes cause and stops half-opening.
func (b *Breaker) QuotaExhausted() Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && b.cause == CauseQuota {
		return Transition{}
	}
	return b.openLocked(CauseQuota)
}

// Reset closes the breaker whatever its state or cause (the operator's
// group_reset).
func (b *Breaker) Reset() Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Closed {
		b.failures = 0
		return Transition{}
	}
	return b.closeLocked()
}

func (b *Breaker) openLocked(c Cause) Transition {
	t := Transition{Changed: true, From: b.state, To: Open, Cause: c}
	b.state, b.cause, b.failures = Open, c, 0
	b.openedAt = b.clock.Now()
	return t
}

func (b *Breaker) closeLocked() Transition {
	t := Transition{Changed: true, From: b.state, To: Closed}
	b.state, b.cause, b.failures = Closed, "", 0
	b.openedAt = time.Time{}
	return t
}

// State returns the current state. An open breaker whose OpenFor has passed
// still reads Open until an Allow half-opens it.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// BreakerState is a copy of a breaker's state.
type BreakerState struct {
	State    State
	Cause    Cause
	Failures int       // consecutive transient failures while closed
	OpenedAt time.Time // zero when closed
	// HalfOpenAt is when a transient-opened breaker will let a probe through;
	// zero when closed, half-open, or open for quota.
	HalfOpenAt time.Time
}

// Snapshot copies the breaker's state.
func (b *Breaker) Snapshot() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := BreakerState{State: b.state, Cause: b.cause, Failures: b.failures, OpenedAt: b.openedAt}
	if b.state == Open && b.cause == CauseTransient {
		s.HalfOpenAt = b.openedAt.Add(b.openFor)
	}
	return s
}
