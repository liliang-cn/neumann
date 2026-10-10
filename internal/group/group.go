// Package group is the control logic of an execution group (design §6): the
// congestion window that decides how many requests go to one provider at once
// (§6.1), the scaler that turns the window into a number of live vCores, and
// the circuit breaker that stops sending when the provider is failing (§6.2).
//
// The package owns decisions and counts, nothing else. It starts no
// goroutines, emits no events and knows nothing about tasks, adapters or the
// scheduler: every method returns what changed, and the caller (the vCore
// loop) turns that into core_up / core_down / group_down / group_up events.
// Everything is safe for concurrent use and reads time only through Clock.
//
// The parts can be used one by one (Window, Scaler, Breaker), or through Group,
// which wires them together and maps a model call's error class (§11) onto
// them in one place, so the table in §11 lives in exactly one function.
package group

import "time"

// Clock is the only way this package reads time, so tests can drive cooldowns
// and breaker timeouts deterministically. It mirrors sched.Clock.
type Clock interface{ Now() time.Time }

// RealClock reads the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// Outcome is how one model call ended, as classified by the adapter (§11).
// The machine's adapter error class maps onto it one to one.
type Outcome int

const (
	Succeeded      Outcome = iota // the call returned a response
	RateLimited                   // 429 with rate-limit headers: the rate limiter's business, not ours
	QuotaExhausted                // quota or billing exhausted: breaker opens until an operator resets it
	Overload                      // 529 / 503 overloaded / ambiguous 429: halve the window, once per epoch
	Transient                     // other 5xx, network error, timeout: counts toward the breaker
	Permanent                     // other 4xx: the request was wrong, the provider is fine
)

func (o Outcome) String() string {
	switch o {
	case Succeeded:
		return "succeeded"
	case RateLimited:
		return "rate_limited"
	case QuotaExhausted:
		return "quota_exhausted"
	case Overload:
		return "overload"
	case Transient:
		return "transient"
	case Permanent:
		return "permanent"
	}
	return "unknown"
}

// Config configures one execution group's controls. Zero values take the
// design defaults.
type Config struct {
	Min        int           // fewest vCores the group keeps and floor of W; default 1
	Max        int           // ceiling of W and of the vCore count; 0 = unlimited
	Initial    float64       // initial W; default 2
	Cooldown   time.Duration // no growth after a halving; default 10s
	IdleRetire time.Duration // an idle vCore retires after this when n > Min; default 30s

	BreakerThreshold int           // consecutive transient failures that open the breaker; default 5
	BreakerOpenFor   time.Duration // how long a transient-opened breaker stays open; default 30s

	Clock Clock // default RealClock
}

// Group bundles the three controls of one execution group. The fields are
// exported so the caller can reach each part directly (SetCores, Reset, ...);
// Admit and Report are the two calls that touch more than one of them.
type Group struct {
	Window  *Window
	Scaler  *Scaler
	Breaker *Breaker
}

// New builds a group's controls from cfg.
func New(cfg Config) *Group {
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	w := NewWindow(WindowConfig{
		Min: cfg.Min, Max: cfg.Max, Initial: cfg.Initial,
		Cooldown: cfg.Cooldown, Clock: cfg.Clock,
	})
	return &Group{
		Window:  w,
		Scaler:  NewScaler(w, cfg.IdleRetire),
		Breaker: NewBreaker(BreakerConfig{Threshold: cfg.BreakerThreshold, OpenFor: cfg.BreakerOpenFor, Clock: cfg.Clock}),
	}
}

// Admit asks whether one model request may be sent now. When ok, the returned
// ticket must be handed back to Report with the call's outcome; when probe is
// also true this request is the half-open breaker's single trial. When !ok the
// group is down and the task belongs in Waiting(Group) (§6.2).
func (g *Group) Admit() (t Ticket, ok, probe bool) {
	ok, probe = g.Breaker.Allow()
	if !ok {
		return Ticket{}, false, false
	}
	return g.Window.Begin(), true, probe
}

// Effect is what one Report changed.
type Effect struct {
	Halved  bool       // the window was halved: this was the epoch's first overload
	Breaker Transition // the breaker's state change, if any (Down → group_down, Up → group_up)
}

// Report applies one call's outcome to the window and the breaker, following
// the §11 table:
//
//	Succeeded       window grows (unless cooling down); breaker count resets / probe closes
//	RateLimited     nothing — quota is the rate limiter's job; it is not a success either
//	QuotaExhausted  breaker opens, never half-opens until Reset
//	Overload        window halves once per epoch; breaker untouched
//	Transient       counts toward the breaker; window untouched
//	Permanent       nothing
func (g *Group) Report(t Ticket, o Outcome) Effect {
	var e Effect
	switch o {
	case Succeeded:
		g.Window.Success(t)
		e.Breaker = g.Breaker.Success()
	case RateLimited:
		g.Window.RateLimited(t)
	case QuotaExhausted:
		e.Breaker = g.Breaker.QuotaExhausted()
	case Overload:
		e.Halved = g.Window.Overload(t)
	case Transient:
		e.Breaker = g.Breaker.Transient()
	case Permanent:
	}
	return e
}

// Status is a point-in-time copy of a group's controls for Machine.Status (§14).
type Status struct {
	Window  WindowState
	Breaker BreakerState
	Running int // live vCores, including ones started but not yet busy
	Busy    int // vCores executing an instruction
}

// Snapshot copies the group's state. Each part is read under its own lock, so
// the parts are individually consistent but not one atomic cut.
func (g *Group) Snapshot() Status {
	r, b := g.Scaler.Counts()
	return Status{Window: g.Window.Snapshot(), Breaker: g.Breaker.Snapshot(), Running: r, Busy: b}
}
