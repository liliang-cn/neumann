package group

import (
	"math"
	"sync"
	"time"
)

const (
	DefaultMin      = 1
	DefaultInitial  = 2.0
	DefaultCooldown = 10 * time.Second
)

// WindowConfig configures a Window. Zero values take the defaults.
type WindowConfig struct {
	// Min is the floor of W. Values below 1 are raised to 1: a window that
	// allows no request can never see the success that would grow it.
	Min int
	// Max is the ceiling of W; 0 means unlimited. W itself is capped, not
	// only Target, so that a halving after a long stretch at the ceiling
	// actually reduces concurrency.
	Max      int
	Initial  float64 // starting W, clamped to [Min, Max]; default 2
	Cooldown time.Duration
	Clock    Clock
}

// Ticket records the congestion epoch a request was sent in. Take one with
// Begin immediately before sending and hand it back with the outcome.
type Ticket struct{ Epoch uint64 }

// Window is the AIMD congestion window of one execution group (§6.1). It
// answers one question — how many requests to have in flight to this
// provider — and nothing about quota, which is the rate limiter's.
//
//   - Additive increase: every success adds 1/W, so W grows by about one per
//     round trip of a full window. No growth during a cooldown.
//   - Multiplicative decrease, on Overload only: W = max(Min, W/2), then a
//     cooldown. Once per congestion epoch: only an overload whose ticket
//     carries the current epoch halves, and it advances the epoch, so a burst
//     of in-flight requests that all fail together halves W once.
//   - Manual: SetCores pins W and disables both; AutoScale(true) restores.
type Window struct {
	mu       sync.Mutex
	clock    Clock
	min, max int
	cooldown time.Duration

	w        float64
	epoch    uint64
	coolTill time.Time
	auto     bool
}

// NewWindow builds a window from cfg.
func NewWindow(cfg WindowConfig) *Window {
	if cfg.Min < DefaultMin {
		cfg.Min = DefaultMin
	}
	if cfg.Max < 0 {
		cfg.Max = 0
	}
	if cfg.Max > 0 && cfg.Max < cfg.Min {
		cfg.Max = cfg.Min
	}
	if cfg.Initial <= 0 {
		cfg.Initial = DefaultInitial
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = DefaultCooldown
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	w := &Window{clock: cfg.Clock, min: cfg.Min, max: cfg.Max, cooldown: cfg.Cooldown, auto: true}
	w.w = w.clamp(cfg.Initial)
	return w
}

func (w *Window) clamp(v float64) float64 {
	if v < float64(w.min) {
		v = float64(w.min)
	}
	if w.max > 0 && v > float64(w.max) {
		v = float64(w.max)
	}
	return v
}

// Begin records the current epoch for one request.
func (w *Window) Begin() Ticket {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Ticket{Epoch: w.epoch}
}

// Success grows W by 1/W, unless the window is pinned or cooling down. Any
// success counts, whatever its epoch: an answer is an answer.
func (w *Window) Success(Ticket) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.auto || w.clock.Now().Before(w.coolTill) {
		return
	}
	w.w = w.clamp(w.w + 1/w.w)
}

// Overload halves W if t belongs to the current epoch, starts a cooldown and
// opens a new epoch. It reports whether it halved. Overloads from an earlier
// epoch were already answered by that epoch's halving and are ignored, as is
// every overload while the window is pinned.
func (w *Window) Overload(t Ticket) (halved bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.auto || t.Epoch != w.epoch {
		return false
	}
	w.w = w.clamp(w.w / 2)
	w.epoch++
	w.coolTill = w.clock.Now().Add(w.cooldown)
	return true
}

// RateLimited deliberately changes nothing: a 429 with rate-limit headers says
// the account is out of budget, not that the provider is congested. It exists
// so a caller can route every outcome somewhere and the rule is written down.
func (w *Window) RateLimited(Ticket) {}

// Target is floor(W) clamped to [Min, Max]: the number of vCores the group
// should be running.
func (w *Window) Target() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.targetLocked()
}

func (w *Window) targetLocked() int {
	n := int(math.Floor(w.w))
	if n < w.min {
		n = w.min
	}
	if w.max > 0 && n > w.max {
		n = w.max
	}
	return n
}

// SetCores pins W to n, clamped to [Min, Max], and disables automatic
// adjustment. It returns the value actually pinned.
func (w *Window) SetCores(n int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.w = w.clamp(float64(n))
	w.auto = false
	return w.targetLocked()
}

// AutoScale(true) restores automatic control starting from the current W,
// clears any cooldown and opens a fresh epoch, so overloads from requests
// sent while pinned cannot halve the restored window. AutoScale(false)
// freezes W where it is.
func (w *Window) AutoScale(on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if on && !w.auto {
		w.epoch++
		w.coolTill = time.Time{}
	}
	w.auto = on
}

// control reads the target and the mode under one lock, so a retirement's
// count and its reason come from the same window state.
func (w *Window) control() (target int, auto bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.targetLocked(), w.auto
}

// IsAuto reports whether the window adjusts itself.
func (w *Window) IsAuto() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.auto
}

// WindowState is a copy of a window's state. It is comparable.
type WindowState struct {
	W             float64
	Target        int
	Epoch         uint64
	Min, Max      int
	Auto          bool
	Cooling       bool
	CooldownUntil time.Time // zero when no halving has happened
}

// Snapshot copies the window's state.
func (w *Window) Snapshot() WindowState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WindowState{
		W: w.w, Target: w.targetLocked(), Epoch: w.epoch,
		Min: w.min, Max: w.max, Auto: w.auto,
		Cooling:       w.clock.Now().Before(w.coolTill),
		CooldownUntil: w.coolTill,
	}
}
