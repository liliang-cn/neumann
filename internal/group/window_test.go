package group

import (
	"math"
	"sync"
	"testing"
	"time"
)

func approx(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func TestWindowDefaults(t *testing.T) {
	w := NewWindow(WindowConfig{Clock: newFakeClock()})
	s := w.Snapshot()
	approx(t, "W", s.W, 2)
	if s.Min != 1 || s.Max != 0 || s.Target != 2 || !s.Auto || s.Epoch != 0 {
		t.Fatalf("defaults: %+v", s)
	}
}

func TestWindowAdditiveIncrease(t *testing.T) {
	w := NewWindow(WindowConfig{Clock: newFakeClock()})
	want := 2.0
	for i := 0; i < 20; i++ {
		w.Success(w.Begin())
		want += 1 / want
		approx(t, "W", w.Snapshot().W, want)
	}
	if got := w.Target(); got != int(math.Floor(want)) {
		t.Fatalf("Target = %d, want floor(%v)", got, want)
	}
}

func TestWindowCooldownBlocksGrowth(t *testing.T) {
	clk := newFakeClock()
	w := NewWindow(WindowConfig{Initial: 8, Clock: clk})
	if !w.Overload(w.Begin()) {
		t.Fatal("first overload of the epoch must halve")
	}
	approx(t, "W after halving", w.Snapshot().W, 4)
	if !w.Snapshot().Cooling {
		t.Fatal("halving must start a cooldown")
	}

	clk.Advance(9 * time.Second)
	w.Success(w.Begin())
	approx(t, "W during cooldown", w.Snapshot().W, 4)

	clk.Advance(time.Second) // exactly 10s: cooldown over
	w.Success(w.Begin())
	approx(t, "W after cooldown", w.Snapshot().W, 4.25)
	if w.Snapshot().Cooling {
		t.Fatal("cooldown should have ended")
	}
}

// Design §18 test 14.
func TestWindowOneHalvingPerEpoch(t *testing.T) {
	w := NewWindow(WindowConfig{Initial: 8, Clock: newFakeClock()})
	tickets := make([]Ticket, 10)
	for i := range tickets {
		tickets[i] = w.Begin()
	}
	var mu sync.Mutex
	halved := 0
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, tk := range tickets {
		wg.Add(1)
		go func(tk Ticket) {
			defer wg.Done()
			<-start
			if w.Overload(tk) {
				mu.Lock()
				halved++
				mu.Unlock()
			}
		}(tk)
	}
	close(start)
	wg.Wait()
	if halved != 1 {
		t.Fatalf("halved %d times, want exactly 1", halved)
	}
	s := w.Snapshot()
	approx(t, "W", s.W, 4)
	if s.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", s.Epoch)
	}

	// A request sent in the new epoch is a new congestion signal.
	if !w.Overload(w.Begin()) {
		t.Fatal("overload from the new epoch must halve")
	}
	approx(t, "W", w.Snapshot().W, 2)
}

func TestWindowMinMaxClamp(t *testing.T) {
	clk := newFakeClock()
	w := NewWindow(WindowConfig{Min: 3, Max: 5, Initial: 4, Clock: clk})
	w.Overload(w.Begin())
	approx(t, "W floored at Min", w.Snapshot().W, 3)
	if w.Target() != 3 {
		t.Fatalf("Target = %d, want 3", w.Target())
	}
	w.Overload(w.Begin())
	approx(t, "W stays at Min", w.Snapshot().W, 3)

	clk.Advance(time.Minute)
	for i := 0; i < 100; i++ {
		w.Success(w.Begin())
	}
	approx(t, "W capped at Max", w.Snapshot().W, 5)
	if w.Target() != 5 {
		t.Fatalf("Target = %d, want 5", w.Target())
	}

	// Initial below Min starts at Min; Initial above Max starts at Max.
	if got := NewWindow(WindowConfig{Min: 4, Clock: clk}).Snapshot().W; got != 4 {
		t.Fatalf("W = %v, want Min 4", got)
	}
	if got := NewWindow(WindowConfig{Max: 1, Clock: clk}).Snapshot().W; got != 1 {
		t.Fatalf("W = %v, want Max 1", got)
	}
}

func TestWindowSetCoresPinsAndAutoScaleRestores(t *testing.T) {
	clk := newFakeClock()
	w := NewWindow(WindowConfig{Clock: clk})
	if got := w.SetCores(6); got != 6 {
		t.Fatalf("SetCores = %d, want 6", got)
	}
	s := w.Snapshot()
	if s.Auto || s.Target != 6 {
		t.Fatalf("pinned: %+v", s)
	}
	tk := w.Begin()
	w.Success(tk)
	if w.Overload(w.Begin()) {
		t.Fatal("a pinned window must not halve")
	}
	approx(t, "pinned W", w.Snapshot().W, 6)

	w.AutoScale(true)
	s = w.Snapshot()
	if !s.Auto {
		t.Fatal("AutoScale(true) must restore automatic control")
	}
	approx(t, "W kept on restore", s.W, 6)
	w.Success(w.Begin())
	approx(t, "W grows again", w.Snapshot().W, 6+1.0/6)
	if !w.Overload(w.Begin()) {
		t.Fatal("restored window halves on overload")
	}

	// SetCores is clamped to [Min, Max].
	w2 := NewWindow(WindowConfig{Min: 2, Max: 4, Clock: clk})
	if got := w2.SetCores(10); got != 4 {
		t.Fatalf("SetCores(10) = %d, want 4", got)
	}
	if got := w2.SetCores(0); got != 2 {
		t.Fatalf("SetCores(0) = %d, want 2", got)
	}

	// AutoScale(false) freezes the current window.
	w3 := NewWindow(WindowConfig{Clock: clk})
	w3.AutoScale(false)
	w3.Success(w3.Begin())
	approx(t, "frozen W", w3.Snapshot().W, 2)
}

func TestWindowRateLimitedLeavesWindowAlone(t *testing.T) {
	w := NewWindow(WindowConfig{Initial: 8, Clock: newFakeClock()})
	before := w.Snapshot()
	for i := 0; i < 10; i++ {
		w.RateLimited(w.Begin())
	}
	after := w.Snapshot()
	if before != after {
		t.Fatalf("RateLimited changed the window: %+v -> %+v", before, after)
	}
}
