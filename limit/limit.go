// Package limit meters a model provider's quota: requests per minute, tokens
// per minute, input and output tokens, daily budgets — each one a dimension
// of a Limiter.
//
// # Getting started
//
// Most hosts never call this package directly: they describe limiters in
// neumann.Config.Limiters and name them in each execution group's Limits.
// Every model request of that group then waits until it fits in all of them:
//
//	neumann.Config{
//		Limiters: []limit.Config{{Name: "org", Limits: map[string]limit.Rate{
//			"requests": {Per: time.Minute, Limit: 50},
//			"tokens":   {Per: time.Minute, Limit: 40000},
//		}}},
//		Groups: []neumann.GroupConfig{{Class: "default", Model: adapter, Limits: []string{"org"}}},
//	}
//
// Standalone, [New] builds a Limiter, [Limiter.Reserve] (or [ReserveAll]
// across several) waits for room, and the [Reservation] is settled with what
// was actually used, or cancelled.
//
// A Limiter is not an execution group. A provider's quota scope rarely lines
// up with one model: an organisation shares its token budget across models,
// while one model can be bound by requests, input tokens and output tokens at
// once. So limiters are separate objects that a request references by name,
// and a request needs room in every limiter it references (ReserveAll).
//
// # Accounting
//
// Each dimension is a sliding-window log: every grant is an entry stamped
// with the time it was granted, and an entry counts against its dimension
// until Per has passed. The invariant this buys is the one a test can check
// and a provider can't refute: in any interval of length Per, the amounts
// granted in a dimension add up to at most Limit.
//
// A token bucket was the alternative, and it is what Anthropic documents. It
// was rejected because it promises less: a full bucket can be drained at once
// and refilled over the next Per, so a window can see up to twice the limit.
// Against a provider that counts in fixed or sliding windows — most
// OpenAI-compatible gateways — that is a 429. A sliding window never exceeds a
// token-bucket provider either; what it costs is burst recovery (after a
// burst, room returns as old entries expire rather than continuously), and
// steady-state throughput is the same Limit per Per. The log is one entry per
// grant per dimension and is pruned as it ages, so it holds at most one
// window's worth of grants.
//
// The log is also what makes settlement exact. A refund (Settle below the
// reservation, or Cancel) shrinks the reservation's own entry, which still
// expires when it would have. An extra charge (Settle above the reservation)
// is a new entry stamped when it is reported: a long streaming call that
// overran its estimate used those tokens recently, not when it was sent, and
// recording them at send time would let them expire before they had counted.
//
// # Fairness
//
// Waiters are served strictly first-come first-served: a reservation that
// would fit is still queued if anyone is waiting ahead of it. That is what
// keeps a large request from being starved by a stream of small ones that
// each fit in what is left. The price is head-of-line blocking — small
// requests wait behind a large one even when they would fit — which is the
// point.
//
// # Provider calibration
//
// Observe takes what a response's rate-limit headers said. When the provider
// reports less remaining than local accounting believes, the provider wins
// until the reset time it gave: the dimension's availability is capped at the
// reported figure, less what is reserved after it. A report of more than
// local accounting believes is ignored — responses arrive out of order, and a
// late response's headers describe an older moment. PauseUntil is a 429's
// retry-after: no reservation is granted before it.
//
// Overload (a 529, a 503) is not the limiter's business: that is the
// execution group's congestion window, which governs concurrency.
package limit

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTooLarge is returned when a reservation asks a dimension for more than
// its Limit. It can never fit, so it fails at once rather than waiting.
var ErrTooLarge = errors.New("limit: amount exceeds the limit")

// Rate is Limit units per Per, for example 40000 tokens per minute.
type Rate struct {
	Per   time.Duration
	Limit int64
}

// Clock is the limiter's source of time. Tests inject a fake one.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config describes a Limiter.
type Config struct {
	Name   string
	Limits map[string]Rate // dimension → rate: "requests", "tokens", "input_tokens", …
	Clock  Clock           // nil = the real clock
}

// Limiter meters one quota scope across one or more dimensions. It is safe
// for concurrent use.
type Limiter struct {
	id    uint64 // lock order for ReserveAll
	name  string
	clock Clock

	mu     sync.Mutex
	dims   []*dimension // sorted by name
	index  map[string]int
	queue  []*waiter // FIFO; queue[0] is the only waiter that may be granted
	paused time.Time
}

var nextID atomic.Uint64

// New validates cfg and returns a Limiter with nothing reserved.
func New(cfg Config) (*Limiter, error) {
	if cfg.Name == "" {
		return nil, errors.New("limit: a limiter needs a name")
	}
	if len(cfg.Limits) == 0 {
		return nil, fmt.Errorf("limit: limiter %q has no limits", cfg.Name)
	}
	l := &Limiter{id: nextID.Add(1), name: cfg.Name, clock: cfg.Clock, index: map[string]int{}}
	if l.clock == nil {
		l.clock = realClock{}
	}
	for name, r := range cfg.Limits {
		if r.Per <= 0 || r.Limit <= 0 {
			return nil, fmt.Errorf("limit: limiter %q dimension %q: Per and Limit must be positive, got %v", cfg.Name, name, r)
		}
		l.dims = append(l.dims, &dimension{name: name, rate: r})
	}
	sort.Slice(l.dims, func(i, j int) bool { return l.dims[i].name < l.dims[j].name })
	for i, d := range l.dims {
		l.index[d.name] = i
	}
	return l, nil
}

// Name is the name execution groups reference the limiter by.
func (l *Limiter) Name() string { return l.name }

// Observed is what a provider said about one dimension: how much remains,
// and when the quota is back to full.
type Observed struct {
	Remaining int64
	ResetAt   time.Time
}

// Info is a response's rate-limit headers, by dimension
// (x-ratelimit-remaining-tokens / x-ratelimit-reset-tokens,
// anthropic-ratelimit-input-tokens-remaining / -reset, …).
type Info struct {
	Dims map[string]Observed
}

// Observe calibrates local accounting from provider feedback. For each
// dimension the limiter has, a report of less remaining than local
// accounting believes caps availability at the reported figure until
// ResetAt; reservations granted meanwhile come out of it, and refunds of
// those go back. Reports of more room, reports for dimensions the limiter
// does not have, and reports without a ResetAt in the future are ignored:
// with no reset time there is no telling how long to believe them.
func (l *Limiter) Observe(info Info) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	for name, o := range info.Dims {
		i, ok := l.index[name]
		if !ok || !o.ResetAt.After(now) {
			continue
		}
		d := l.dims[i]
		d.prune(now)
		rem := max(o.Remaining, 0)
		if rem < d.available() {
			d.epoch++
			d.ov = override{active: true, remaining: rem, until: o.ResetAt, epoch: d.epoch}
		}
	}
}

// PauseUntil grants nothing before t: a 429 with retry-after. A pause never
// shortens one already in effect.
func (l *Limiter) PauseUntil(t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.After(l.paused) {
		l.paused = t
	}
}

// DimStatus is one dimension's state at the moment Status was taken.
type DimStatus struct {
	Rate
	// Used is what the current window holds. It can exceed Limit after a
	// Settle charged more than was reserved.
	Used int64
	// Available is what a reservation could take from this dimension now,
	// after provider calibration. It does not account for a pause or for
	// waiters ahead in the queue.
	Available int64
	// NextReset is when Available next grows without anything else
	// happening: the oldest entry in the window expiring, or the provider's
	// reset time while the provider's figure is the binding one. Zero when
	// nothing is in use.
	NextReset time.Time
	// ProviderRemaining and ProviderUntil are the provider's figure, less
	// what was reserved since, while it is in effect; ProviderUntil is zero
	// when it is not.
	ProviderRemaining int64
	ProviderUntil     time.Time
}

// Status is a limiter's state. Taking it holds the limiter's lock for a copy
// and never waits for a reservation.
type Status struct {
	Name        string
	Waiting     int       // reservations queued
	PausedUntil time.Time // zero unless a pause is in effect
	Dims        map[string]DimStatus
}

func (l *Limiter) Status() Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	st := Status{Name: l.name, Waiting: len(l.queue), Dims: make(map[string]DimStatus, len(l.dims))}
	if l.paused.After(now) {
		st.PausedUntil = l.paused
	}
	for _, d := range l.dims {
		d.prune(now)
		ds := DimStatus{Rate: d.rate, Used: d.used, Available: max(d.available(), 0)}
		local := d.rate.Limit - d.used
		if d.ov.active {
			ds.ProviderRemaining, ds.ProviderUntil = d.ov.remaining, d.ov.until
		}
		switch {
		case d.ov.active && d.ov.remaining < local:
			ds.NextReset = d.ov.until
		default:
			for _, e := range d.log {
				if e.amount > 0 {
					ds.NextReset = e.at.Add(d.rate.Per)
					break
				}
			}
		}
		st.Dims[d.name] = ds
	}
	return st
}

// dimension is one rate's sliding-window log.
type dimension struct {
	name  string
	rate  Rate
	log   []*entry // ordered by at
	used  int64    // sum of log amounts
	ov    override
	epoch uint64 // last override epoch issued
}

type entry struct {
	at     time.Time
	amount int64
	epoch  uint64 // the override this was charged to, 0 = none
}

// override is the provider's figure while it is believed.
type override struct {
	active    bool
	remaining int64
	until     time.Time
	epoch     uint64
}

// prune drops entries that have left the window and an override whose reset
// time has come.
func (d *dimension) prune(now time.Time) {
	n := 0
	for n < len(d.log) && !d.log[n].at.Add(d.rate.Per).After(now) {
		d.used -= d.log[n].amount
		d.log[n] = nil
		n++
	}
	d.log = d.log[n:]
	if d.ov.active && !now.Before(d.ov.until) {
		d.ov = override{}
	}
}

// available is what may be granted now; negative when a Settle overran.
// Call after prune.
func (d *dimension) available() int64 {
	a := d.rate.Limit - d.used
	if d.ov.active && d.ov.remaining < a {
		a = d.ov.remaining
	}
	return a
}

// wait is how long until n fits. Call after prune; n ≤ Limit.
func (d *dimension) wait(now time.Time, n int64) time.Duration {
	if n == 0 || d.available() >= n {
		return 0
	}
	var w time.Duration
	if excess := d.used + n - d.rate.Limit; excess > 0 {
		var freed int64
		for _, e := range d.log {
			freed += e.amount
			if freed >= excess {
				w = e.at.Add(d.rate.Per).Sub(now)
				break
			}
		}
	}
	if d.ov.active && d.ov.remaining < n {
		w = max(w, d.ov.until.Sub(now))
	}
	return w
}

// charge records n granted or reported at now.
func (d *dimension) charge(now time.Time, n int64) *entry {
	if n <= 0 {
		return nil
	}
	e := &entry{at: now, amount: n}
	if d.ov.active {
		d.ov.remaining -= n
		e.epoch = d.ov.epoch
	}
	d.log = append(d.log, e)
	d.used += n
	return e
}

// refund gives back up to n of e. An entry that has left the window has
// nothing left to give back. Call after prune(now), which has removed every
// entry that expired by now, so e is live exactly when it has not.
func (d *dimension) refund(now time.Time, e *entry, n int64) {
	if e == nil || n <= 0 || !e.at.Add(d.rate.Per).After(now) {
		return
	}
	n = min(n, e.amount)
	e.amount -= n
	d.used -= n
	if d.ov.active && e.epoch == d.ov.epoch {
		d.ov.remaining += n
	}
}
