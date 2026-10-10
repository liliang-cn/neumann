package group

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerOpensAfterFiveConsecutiveTransients(t *testing.T) {
	b := NewBreaker(BreakerConfig{Clock: newFakeClock()})
	for i := 0; i < 4; i++ {
		if tr := b.Transient(); tr.Changed {
			t.Fatalf("transient %d changed state: %+v", i+1, tr)
		}
	}
	tr := b.Transient()
	if !tr.Changed || !tr.Down() || tr.To != Open || tr.Cause != CauseTransient {
		t.Fatalf("5th transient: %+v", tr)
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("open breaker must refuse")
	}
}

func TestBreakerSuccessResetsCount(t *testing.T) {
	b := NewBreaker(BreakerConfig{Clock: newFakeClock()})
	for i := 0; i < 4; i++ {
		b.Transient()
	}
	b.Success()
	for i := 0; i < 4; i++ {
		if tr := b.Transient(); tr.Changed {
			t.Fatalf("4 + success + %d opened the breaker", i+1)
		}
	}
	if b.State() != Closed {
		t.Fatalf("state = %v, want closed", b.State())
	}
	if tr := b.Transient(); !tr.Down() {
		t.Fatal("fifth in the new run must open")
	}
}

func openTransient(t *testing.T, b *Breaker) {
	t.Helper()
	for i := 0; i < 5; i++ {
		b.Transient()
	}
	if b.State() != Open {
		t.Fatalf("state = %v, want open", b.State())
	}
}

func TestBreakerHalfOpenExactlyOneProbe(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	openTransient(t, b)

	clk.Advance(29 * time.Second)
	if ok, _ := b.Allow(); ok {
		t.Fatal("must stay open for 30s")
	}
	clk.Advance(time.Second)

	var oks, probes atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, probe := b.Allow()
			if ok {
				oks.Add(1)
			}
			if probe {
				probes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if oks.Load() != 1 || probes.Load() != 1 {
		t.Fatalf("allowed %d, probes %d; want exactly one probe", oks.Load(), probes.Load())
	}
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("a second request while the probe is out must be refused")
	}
}

func TestBreakerProbeSuccessCloses(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	openTransient(t, b)
	clk.Advance(30 * time.Second)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatalf("Allow = %v,%v; want probe", ok, probe)
	}
	tr := b.Success()
	if !tr.Up() || tr.From != HalfOpen || tr.To != Closed {
		t.Fatalf("probe success: %+v", tr)
	}
	if ok, probe := b.Allow(); !ok || probe {
		t.Fatalf("closed Allow = %v,%v", ok, probe)
	}
	// Count starts fresh after closing.
	for i := 0; i < 4; i++ {
		b.Transient()
	}
	if b.State() != Closed {
		t.Fatal("count must restart after closing")
	}
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	openTransient(t, b)
	clk.Advance(30 * time.Second)
	b.Allow()
	tr := b.Transient()
	if !tr.Changed || tr.From != HalfOpen || tr.To != Open || tr.Down() {
		t.Fatalf("probe failure: %+v (reopen is not a new group_down)", tr)
	}
	clk.Advance(29 * time.Second)
	if ok, _ := b.Allow(); ok {
		t.Fatal("reopened breaker waits another full 30s")
	}
	clk.Advance(time.Second)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatal("half-opens again after another 30s")
	}
}

// Design §18 test 16 (breaker half).
func TestBreakerQuotaNeverHalfOpensUntilReset(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	tr := b.QuotaExhausted()
	if !tr.Down() || tr.Cause != CauseQuota {
		t.Fatalf("quota: %+v", tr)
	}
	clk.Advance(24 * time.Hour)
	if ok, _ := b.Allow(); ok {
		t.Fatal("quota-opened breaker must never half-open")
	}
	if tr := b.Success(); tr.Changed {
		t.Fatal("a straggler success must not close a quota breaker")
	}
	tr = b.Reset()
	if !tr.Up() || b.State() != Closed {
		t.Fatalf("reset: %+v", tr)
	}
	if ok, _ := b.Allow(); !ok {
		t.Fatal("reset breaker admits")
	}
}

func TestBreakerQuotaOverridesTransientOpen(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	openTransient(t, b)
	tr := b.QuotaExhausted()
	if !tr.Changed || tr.Down() || tr.Cause != CauseQuota {
		t.Fatalf("quota on open: %+v", tr)
	}
	clk.Advance(time.Hour)
	if ok, _ := b.Allow(); ok {
		t.Fatal("quota cause sticks: no half-open")
	}
}

func TestBreakerCustomThresholdAndResetWhenClosed(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Threshold: 2, OpenFor: time.Second, Clock: clk})
	b.Transient()
	if !b.Transient().Down() {
		t.Fatal("threshold 2")
	}
	clk.Advance(time.Second)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatal("OpenFor 1s")
	}
	b.Reset()
	if tr := b.Reset(); tr.Changed {
		t.Fatal("resetting a closed breaker is not a transition")
	}
}

// halfOpen drives a fresh breaker to half-open with its probe handed out.
func halfOpen(t *testing.T) (*Breaker, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	for range DefaultBreakerThreshold {
		b.Transient()
	}
	clk.Advance(DefaultBreakerOpenFor)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatal("no probe after OpenFor")
	}
	return b, clk
}

// A probe the provider answered with anything but a transient failure proves
// it reachable; the breaker must not wait forever for a Success.
func TestBreakerProbeAnsweredOtherwiseCloses(t *testing.T) {
	for _, o := range []Outcome{RateLimited, Overload, Permanent} {
		clk := newFakeClock()
		g := New(Config{Clock: clk})
		for range DefaultBreakerThreshold {
			g.Report(g.Window.Begin(), Transient)
		}
		clk.Advance(DefaultBreakerOpenFor)
		tk, ok, probe := g.Admit()
		if !ok || !probe {
			t.Fatalf("%v: no probe", o)
		}
		if e := g.Report(tk, o); !e.Breaker.Up() {
			t.Fatalf("%v: probe answer did not close the breaker: %+v", o, e.Breaker)
		}
		if ok, _ := g.Breaker.Allow(); !ok {
			t.Fatalf("%v: closed breaker refuses", o)
		}
	}
}

// A probe that never reports (its task was cancelled mid-call) must not wedge
// the group: after another OpenFor a new probe goes out.
func TestBreakerLostProbeIsReplaced(t *testing.T) {
	b, clk := halfOpen(t)
	clk.Advance(DefaultBreakerOpenFor - time.Second)
	if ok, _ := b.Allow(); ok {
		t.Fatal("a second request went out while the probe is pending")
	}
	clk.Advance(time.Second)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatal("a lost probe was never replaced")
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("the replacement probe must be the only one")
	}
}

// Permanent and rate-limited answers while closed neither count toward the
// breaker nor reset its count.
func TestBreakerAnsweredLeavesClosedCountAlone(t *testing.T) {
	b := NewBreaker(BreakerConfig{Clock: newFakeClock()})
	for range DefaultBreakerThreshold - 1 {
		b.Transient()
	}
	if tr := b.Answered(); tr.Changed {
		t.Fatalf("answered changed a closed breaker: %+v", tr)
	}
	if tr := b.Transient(); !tr.Down() {
		t.Fatal("answered reset the consecutive count")
	}
}

// NextProbe says when a probe is next let through: the half-open time while
// open for transients, the lost-probe replacement while half-open, never
// while open for quota.
func TestBreakerNextProbe(t *testing.T) {
	clk := newFakeClock()
	b := NewBreaker(BreakerConfig{Clock: clk})
	if s := b.Snapshot(); !s.NextProbe.IsZero() {
		t.Fatalf("closed: %+v", s)
	}
	for range 5 {
		b.Transient()
	}
	open := clk.Now()
	if s := b.Snapshot(); !s.NextProbe.Equal(open.Add(DefaultBreakerOpenFor)) {
		t.Fatalf("open: %+v", s)
	}
	clk.Advance(DefaultBreakerOpenFor)
	if ok, probe := b.Allow(); !ok || !probe {
		t.Fatal("no probe after OpenFor")
	}
	if s := b.Snapshot(); s.State != HalfOpen || !s.NextProbe.Equal(clk.Now().Add(DefaultBreakerOpenFor)) {
		t.Fatalf("half-open: %+v", s)
	}
	b.QuotaExhausted()
	if s := b.Snapshot(); !s.NextProbe.IsZero() {
		t.Fatalf("quota: %+v", s)
	}
}
