package group

import (
	"sync"
	"testing"
	"time"
)

func TestScaleUpRule(t *testing.T) {
	cases := []struct {
		name                         string
		running, busy, ready, target int
		want                         int
	}{
		{"empty queue", 1, 1, 0, 4, 0},
		{"a core is idle", 2, 1, 3, 4, 0},
		{"cold start to W", 0, 0, 5, 2, 2},
		{"bounded by backlog", 1, 1, 1, 4, 1},
		{"bounded by window", 2, 2, 10, 4, 2},
		{"at window", 4, 4, 10, 4, 0},
		{"above window", 5, 5, 10, 4, 0},
	}
	for _, c := range cases {
		if got := ScaleUp(c.running, c.busy, c.ready, c.target); got != c.want {
			t.Errorf("%s: ScaleUp(%d,%d,%d,%d) = %d, want %d",
				c.name, c.running, c.busy, c.ready, c.target, got, c.want)
		}
	}
}

func TestRetireRules(t *testing.T) {
	if RetireAfterInstruction(4, 4) || !RetireAfterInstruction(5, 4) {
		t.Fatal("retire after an instruction exactly when n > floor(W)")
	}
	if RetireIdle(2, 1, 29*time.Second, 30*time.Second) {
		t.Fatal("not idle long enough")
	}
	if !RetireIdle(2, 1, 30*time.Second, 30*time.Second) {
		t.Fatal("idle for IdleRetire with n > Min retires")
	}
	if RetireIdle(1, 1, time.Hour, 30*time.Second) {
		t.Fatal("never below Min")
	}
}

func TestScalerGrowReservesAndStartsIdle(t *testing.T) {
	g := New(Config{Clock: newFakeClock()}) // W = 2
	n, reason := g.Scaler.Grow(5)
	if n != 2 || reason != ReasonDemand {
		t.Fatalf("Grow = %d,%q; want 2,demand", n, reason)
	}
	// Reserved cores are not busy yet, so a second Grow starts nothing.
	if n, _ := g.Scaler.Grow(5); n != 0 {
		t.Fatalf("second Grow started %d before the first cores took work", n)
	}
	g.Scaler.Acquire()
	g.Scaler.Acquire()
	if n, _ := g.Scaler.Grow(5); n != 0 {
		t.Fatalf("Grow above floor(W) started %d", n)
	}
	if r, b := g.Scaler.Counts(); r != 2 || b != 2 {
		t.Fatalf("counts = %d,%d", r, b)
	}
}

func TestScalerRetireOnOverloadAndManual(t *testing.T) {
	g := New(Config{Initial: 8, Clock: newFakeClock()})
	n, _ := g.Scaler.Grow(100)
	if n != 8 {
		t.Fatalf("Grow = %d, want 8", n)
	}
	for i := 0; i < n; i++ {
		g.Scaler.Acquire()
	}
	g.Window.Overload(g.Window.Begin()) // W 8 -> 4

	retired := 0
	for i := 0; i < 8; i++ {
		ok, reason := g.Scaler.Release()
		if ok {
			if reason != ReasonOverload {
				t.Fatalf("reason = %q, want overload", reason)
			}
			retired++
		}
	}
	if retired != 4 {
		t.Fatalf("retired %d, want 4", retired)
	}
	if r, b := g.Scaler.Counts(); r != 4 || b != 0 {
		t.Fatalf("counts = %d,%d; want 4,0", r, b)
	}

	g.Window.SetCores(2)
	g.Scaler.Acquire()
	ok, reason := g.Scaler.Release()
	if !ok || reason != ReasonManual {
		t.Fatalf("pinned below n: %v,%q; want retire manual", ok, reason)
	}
}

func TestScalerIdleRetireKeepsWindowAndMin(t *testing.T) {
	g := New(Config{Initial: 4, IdleRetire: 30 * time.Second, Clock: newFakeClock()})
	g.Scaler.Grow(4)
	before := g.Window.Snapshot()
	retired := 0
	for i := 0; i < 4; i++ {
		if ok, reason := g.Scaler.IdleTimeout(31 * time.Second); ok {
			if reason != ReasonIdle {
				t.Fatalf("reason = %q", reason)
			}
			retired++
		}
	}
	if retired != 3 {
		t.Fatalf("retired %d, want 3 (Min 1 stays)", retired)
	}
	if ok, _ := g.Scaler.IdleTimeout(10 * time.Second); ok {
		t.Fatal("short idle must not retire")
	}
	if g.Window.Snapshot() != before {
		t.Fatal("idle retirement must not change W")
	}
	if g.Scaler.Stop() != ReasonShutdown {
		t.Fatal("Stop reports shutdown")
	}
	if r, _ := g.Scaler.Counts(); r != 0 {
		t.Fatalf("running = %d after Stop", r)
	}
}

func TestScalerDefaultIdleRetire(t *testing.T) {
	g := New(Config{Clock: newFakeClock()})
	g.Scaler.Grow(2)
	if ok, _ := g.Scaler.IdleTimeout(29 * time.Second); ok {
		t.Fatal("default IdleRetire is 30s")
	}
	if ok, _ := g.Scaler.IdleTimeout(30 * time.Second); !ok {
		t.Fatal("default IdleRetire is 30s")
	}
}

// Concurrent vCores finishing at once: exactly n - floor(W) retire, never more.
func TestScalerConcurrentReleaseRetiresExactly(t *testing.T) {
	g := New(Config{Initial: 16, Clock: newFakeClock()})
	n, _ := g.Scaler.Grow(100)
	for i := 0; i < n; i++ {
		g.Scaler.Acquire()
	}
	g.Window.Overload(g.Window.Begin()) // 16 -> 8
	var mu sync.Mutex
	retired := 0
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := g.Scaler.Release(); ok {
				mu.Lock()
				retired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if retired != 8 {
		t.Fatalf("retired %d, want 8", retired)
	}
}
