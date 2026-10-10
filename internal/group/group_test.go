package group

import (
	"sync"
	"testing"
	"time"
)

// Design §11: what each error class does to the window and the breaker.
func TestReportByClass(t *testing.T) {
	clk := newFakeClock()
	g := New(Config{Initial: 8, Clock: clk})

	// RateLimited: window unchanged, breaker unchanged (and not a success).
	for i := 0; i < 4; i++ {
		g.Report(g.Window.Begin(), Transient)
	}
	w0 := g.Window.Snapshot()
	e := g.Report(g.Window.Begin(), RateLimited)
	if e.Halved || e.Breaker.Changed || g.Window.Snapshot() != w0 {
		t.Fatalf("RateLimited touched something: %+v", e)
	}
	// Permanent: touches neither — the four transients still count.
	e = g.Report(g.Window.Begin(), Permanent)
	if e.Halved || e.Breaker.Changed || g.Window.Snapshot() != w0 {
		t.Fatalf("Permanent touched something: %+v", e)
	}
	// Overload: halves, does not count toward the breaker.
	e = g.Report(g.Window.Begin(), Overload)
	if !e.Halved || e.Breaker.Changed || g.Window.Snapshot().W != 4 {
		t.Fatalf("Overload: %+v", e)
	}
	// Transient: the fifth consecutive opens the breaker, window untouched.
	w1 := g.Window.Snapshot()
	e = g.Report(g.Window.Begin(), Transient)
	if e.Halved || !e.Breaker.Down() || g.Window.Snapshot() != w1 {
		t.Fatalf("5th Transient: %+v", e)
	}
	if _, ok, _ := g.Admit(); ok {
		t.Fatal("Admit must refuse while open")
	}

	// Success after half-open closes the breaker and grows the window.
	clk.Advance(30 * time.Second)
	tk, ok, probe := g.Admit()
	if !ok || !probe {
		t.Fatal("Admit lets the probe through after 30s")
	}
	e = g.Report(tk, Succeeded)
	if !e.Breaker.Up() || g.Window.Snapshot().W <= 4 {
		t.Fatalf("probe success: %+v W=%v", e, g.Window.Snapshot().W)
	}

	// QuotaExhausted: opens for good, window untouched.
	w2 := g.Window.Snapshot()
	e = g.Report(g.Window.Begin(), QuotaExhausted)
	if !e.Breaker.Down() || e.Breaker.Cause != CauseQuota || g.Window.Snapshot() != w2 {
		t.Fatalf("QuotaExhausted: %+v", e)
	}
	clk.Advance(time.Hour)
	if _, ok, _ := g.Admit(); ok {
		t.Fatal("quota never half-opens")
	}
	if !g.Breaker.Reset().Up() {
		t.Fatal("operator reset brings the group up")
	}
}

func TestGroupSnapshot(t *testing.T) {
	g := New(Config{Initial: 4, Clock: newFakeClock()})
	g.Scaler.Grow(10)
	g.Scaler.Acquire()
	s := g.Snapshot()
	if s.Window.Target != 4 || s.Running != 4 || s.Busy != 1 || s.Breaker.State != Closed {
		t.Fatalf("snapshot: %+v", s)
	}
}

// Everything at once under -race: invariants hold and nothing goes negative.
func TestConcurrentUse(t *testing.T) {
	clk := newFakeClock()
	g := New(Config{Max: 32, Clock: clk})
	outcomes := []Outcome{Succeeded, Succeeded, Succeeded, Overload, RateLimited, Transient, Succeeded, Permanent}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if n, _ := g.Scaler.Grow(3); n > 0 {
					for j := 0; j < n; j++ {
						g.Scaler.Acquire()
						g.Scaler.Release()
					}
				}
				tk, ok, _ := g.Admit()
				if !ok {
					g.Breaker.Reset()
					continue
				}
				g.Report(tk, outcomes[(w+i)%len(outcomes)])
				if i%50 == 0 {
					clk.Advance(time.Second)
					_ = g.Snapshot()
				}
				if i%97 == 0 {
					g.Scaler.IdleTimeout(time.Minute)
				}
			}
		}(w)
	}
	wg.Wait()
	s := g.Snapshot()
	if s.Running < 0 || s.Busy < 0 || s.Busy > s.Running {
		t.Fatalf("bad counts: %+v", s)
	}
	if s.Window.W < 1 || s.Window.W > 32 {
		t.Fatalf("W out of [Min, Max]: %v", s.Window.W)
	}
}
