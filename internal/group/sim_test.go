package group

import (
	"testing"
	"time"
)

// Design §18 test 13. A scripted provider answers Overload to any request that
// arrives while K or more are already in flight. A discrete-event loop drives
// simulated vCores through exactly the calls a real vCore loop makes:
// Grow → Acquire → Begin → (provider) → Report → Release. The backlog is
// infinite, so the only thing limiting the core count is the window.
//
// After warm-up the live core count must oscillate around K (it keeps probing
// up to K and beyond, and a halving brings it back to about K/2) and never
// exceed 2K.
func TestCoresOscillateAroundProviderCapacity(t *testing.T) {
	const (
		K       = 8
		step    = 50 * time.Millisecond
		total   = 10 * time.Minute
		warmup  = time.Minute
		backlog = 1 << 20
	)
	clk := newFakeClock()
	g := New(Config{Clock: clk})
	start := clk.Now()

	type request struct {
		done     time.Time
		ticket   Ticket
		overload bool
	}
	type core struct{ req *request }

	var (
		cores    []*core
		inflight int
		seq      int
		overload int
	)
	send := func(c *core) {
		g.Scaler.Acquire()
		tk, ok, _ := g.Admit()
		if !ok {
			t.Fatal("breaker must stay closed: overload never trips it")
		}
		seq++
		// 1.0–1.4s latency, deterministic jitter so the cores desynchronise.
		lat := time.Second + time.Duration(seq%5)*100*time.Millisecond
		c.req = &request{done: clk.Now().Add(lat), ticket: tk, overload: inflight >= K}
		inflight++
	}

	minN, maxN, sum, samples := 1<<30, 0, 0, 0
	maxW := 0.0
	for clk.Now().Sub(start) < total {
		now := clk.Now()
		kept := cores[:0]
		for _, c := range cores {
			if c.req != nil && !c.req.done.After(now) {
				inflight--
				if c.req.overload {
					overload++
					g.Report(c.req.ticket, Overload)
				} else {
					g.Report(c.req.ticket, Succeeded)
				}
				c.req = nil
				if retire, _ := g.Scaler.Release(); retire {
					continue
				}
				send(c)
			}
			kept = append(kept, c)
		}
		cores = kept

		k, _ := g.Scaler.Grow(backlog)
		for i := 0; i < k; i++ {
			c := &core{}
			cores = append(cores, c)
			send(c)
		}

		running, busy := g.Scaler.Counts()
		if running != len(cores) || busy != inflight {
			t.Fatalf("scaler counts %d/%d disagree with sim %d/%d", running, busy, len(cores), inflight)
		}
		if now.Sub(start) >= warmup {
			minN = min(minN, running)
			maxN = max(maxN, running)
			sum += running
			samples++
			maxW = max(maxW, g.Window.Snapshot().W)
		}
		clk.Advance(step)
	}

	mean := float64(sum) / float64(samples)
	t.Logf("K=%d stable phase: cores min=%d max=%d mean=%.2f, max W=%.2f, overloads=%d, final epoch=%d",
		K, minN, maxN, mean, maxW, overload, g.Window.Snapshot().Epoch)

	if maxN > 2*K {
		t.Fatalf("cores reached %d, must never exceed 2K=%d", maxN, 2*K)
	}
	if maxN < K {
		t.Fatalf("cores peaked at %d: never probed up to K=%d", maxN, K)
	}
	if minN < K/2-1 {
		t.Fatalf("cores fell to %d: collapsed well below K/2", minN)
	}
	if mean < float64(K)/2 || mean > float64(K)*1.5 {
		t.Fatalf("mean %.2f not around K=%d", mean, K)
	}
	if g.Window.Snapshot().Epoch < 5 {
		t.Fatal("expected repeated congestion epochs (oscillation), saw too few")
	}
}
