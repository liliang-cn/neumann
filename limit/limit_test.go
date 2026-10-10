package limit_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/limit"
)

func newLimiter(t *testing.T, clk limit.Clock, name string, limits map[string]limit.Rate) *limit.Limiter {
	t.Helper()
	l, err := limit.New(limit.Config{Name: name, Limits: limits, Clock: clk})
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	return l
}

func perMinute(n int64) limit.Rate { return limit.Rate{Per: time.Minute, Limit: n} }

type result struct {
	r   *limit.Reservation
	err error
}

func reserveAsync(ctx context.Context, ls []*limit.Limiter, amounts map[string]int64) <-chan result {
	ch := make(chan result, 1)
	go func() {
		r, err := limit.ReserveAll(ctx, ls, amounts)
		ch <- result{r, err}
	}()
	return ch
}

// waitUntil polls cond; it is how a test learns that a goroutine has reached
// a state it cannot otherwise announce (queued, sleeping on a timer).
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func expectBlocked(t *testing.T, ch <-chan result) {
	t.Helper()
	// Give a goroutine that would wrongly be granted a moment to say so.
	time.Sleep(2 * time.Millisecond)
	select {
	case res := <-ch:
		t.Fatalf("reservation should still be waiting, got r=%v err=%v", res.r, res.err)
	default:
	}
}

func expectResult(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("reservation never returned")
		return result{}
	}
}

func dim(t *testing.T, l *limit.Limiter, name string) limit.DimStatus {
	t.Helper()
	d, ok := l.Status().Dims[name]
	if !ok {
		t.Fatalf("no dimension %q in status", name)
	}
	return d
}

func waiting(l *limit.Limiter) func() bool {
	return func() bool { return l.Status().Waiting > 0 }
}

func TestNewValidates(t *testing.T) {
	cases := []limit.Config{
		{Name: "", Limits: map[string]limit.Rate{"requests": perMinute(1)}},
		{Name: "x"},
		{Name: "x", Limits: map[string]limit.Rate{"requests": {Per: 0, Limit: 1}}},
		{Name: "x", Limits: map[string]limit.Rate{"requests": {Per: time.Second, Limit: 0}}},
	}
	for i, c := range cases {
		if _, err := limit.New(c); err == nil {
			t.Errorf("case %d: New accepted %+v", i, c)
		}
	}
}

func TestAllOrNothingAcrossDimensions(t *testing.T) {
	clk := newFakeClock()
	l := newLimiter(t, clk, "org", map[string]limit.Rate{"requests": perMinute(10), "tokens": perMinute(100)})
	if _, err := l.Reserve(context.Background(), map[string]int64{"requests": 1, "tokens": 90}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := reserveAsync(ctx, []*limit.Limiter{l}, map[string]int64{"requests": 1, "tokens": 20})
	waitUntil(t, "second reservation to queue", waiting(l))
	expectBlocked(t, ch)

	// requests had room, tokens did not: nothing may have been taken.
	if d := dim(t, l, "requests"); d.Used != 1 || d.Available != 9 {
		t.Fatalf("requests: used=%d available=%d, want 1/9", d.Used, d.Available)
	}
	if d := dim(t, l, "tokens"); d.Used != 90 || d.Available != 10 {
		t.Fatalf("tokens: used=%d available=%d, want 90/10", d.Used, d.Available)
	}
	cancel()
	res := expectResult(t, ch)
	if !errors.Is(res.err, context.Canceled) || res.r != nil {
		t.Fatalf("got r=%v err=%v, want nil, context.Canceled", res.r, res.err)
	}
	if st := l.Status(); st.Waiting != 0 || st.Dims["requests"].Used != 1 || st.Dims["tokens"].Used != 90 {
		t.Fatalf("after cancel: %+v", st)
	}
}

func TestAllOrNothingAcrossLimiters(t *testing.T) {
	clk := newFakeClock()
	org := newLimiter(t, clk, "org", map[string]limit.Rate{"tokens": perMinute(100)})
	mdl := newLimiter(t, clk, "model", map[string]limit.Rate{"tokens": perMinute(50)})
	if _, err := mdl.Reserve(context.Background(), map[string]int64{"tokens": 40}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := reserveAsync(ctx, []*limit.Limiter{org, mdl}, map[string]int64{"tokens": 20})
	waitUntil(t, "reservation to queue on both limiters", func() bool {
		return org.Status().Waiting == 1 && mdl.Status().Waiting == 1
	})
	expectBlocked(t, ch)
	if d := dim(t, org, "tokens"); d.Used != 0 {
		t.Fatalf("org charged %d while the model limiter had no room", d.Used)
	}
	clk.Advance(time.Minute)
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if org.Status().Dims["tokens"].Used != 20 || mdl.Status().Dims["tokens"].Used != 20 {
		t.Fatalf("after grant: org=%+v model=%+v", org.Status(), mdl.Status())
	}
}

func TestBlocksUntilWindowFrees(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	if _, err := l.Reserve(context.Background(), map[string]int64{"tokens": 100}); err != nil {
		t.Fatal(err)
	}
	ch := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 50})
	waitUntil(t, "waiter to sleep on a timer", func() bool { return clk.Pending() == 1 })
	if d := dim(t, l, "tokens"); !d.NextReset.Equal(t0.Add(time.Minute)) {
		t.Fatalf("NextReset = %v, want %v", d.NextReset, t0.Add(time.Minute))
	}
	clk.Advance(59 * time.Second)
	expectBlocked(t, ch)
	clk.Advance(time.Second)
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.r.Time().Equal(t0.Add(time.Minute)) {
		t.Fatalf("granted at %v, want %v", res.r.Time(), t0.Add(time.Minute))
	}
}

func TestContextAlreadyDone(t *testing.T) {
	l := newLimiter(t, newFakeClock(), "m", map[string]limit.Rate{"requests": perMinute(10)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := l.Reserve(ctx, map[string]int64{"requests": 1})
	if !errors.Is(err, context.Canceled) || r != nil {
		t.Fatalf("got r=%v err=%v", r, err)
	}
	if d := dim(t, l, "requests"); d.Used != 0 {
		t.Fatalf("used %d after a cancelled reserve", d.Used)
	}
}

func TestCancelledHeadLetsNextThrough(t *testing.T) {
	clk := newFakeClock()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	if _, err := l.Reserve(context.Background(), map[string]int64{"tokens": 50}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	big := reserveAsync(ctx, []*limit.Limiter{l}, map[string]int64{"tokens": 100})
	waitUntil(t, "big to queue", waiting(l))
	small := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 10})
	waitUntil(t, "small to queue", func() bool { return l.Status().Waiting == 2 })
	expectBlocked(t, small) // behind big, though it would fit
	cancel()
	if res := expectResult(t, big); !errors.Is(res.err, context.Canceled) {
		t.Fatalf("big: %v", res.err)
	}
	// No time passes: the small one fits and is now at the head.
	if res := expectResult(t, small); res.err != nil {
		t.Fatal(res.err)
	}
}

func TestFIFOBigRequestNotStarved(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	if _, err := l.Reserve(context.Background(), map[string]int64{"tokens": 60}); err != nil {
		t.Fatal(err)
	}
	big := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 100})
	waitUntil(t, "big to queue", waiting(l))

	// 40 tokens are free, so each of these would fit on its own; FIFO says
	// none may pass the big request.
	var smalls []<-chan result
	for i := range 5 {
		smalls = append(smalls, reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 10}))
		waitUntil(t, "small to queue", func() bool { return l.Status().Waiting == 2+i })
	}
	for _, ch := range smalls {
		expectBlocked(t, ch)
	}

	clk.Advance(time.Minute)
	bres := expectResult(t, big)
	if bres.err != nil {
		t.Fatal(bres.err)
	}
	if !bres.r.Time().Equal(t0.Add(time.Minute)) {
		t.Fatalf("big granted at %v", bres.r.Time())
	}
	for _, ch := range smalls {
		expectBlocked(t, ch)
	}
	clk.Advance(time.Minute)
	for _, ch := range smalls {
		res := expectResult(t, ch)
		if res.err != nil {
			t.Fatal(res.err)
		}
		if !res.r.Time().After(bres.r.Time()) {
			t.Fatalf("small granted at %v, not after big at %v", res.r.Time(), bres.r.Time())
		}
	}
}

func TestReserveAllNoLimiters(t *testing.T) {
	r, err := limit.ReserveAll(context.Background(), nil, map[string]int64{"requests": 1})
	if err != nil || r == nil {
		t.Fatalf("got r=%v err=%v", r, err)
	}
	r.Settle(map[string]int64{"requests": 1})
	if _, err := limit.ReserveAll(context.Background(), []*limit.Limiter{nil}, nil); err == nil {
		t.Fatal("nil limiter accepted")
	}
}

func TestTooLarge(t *testing.T) {
	clk := newFakeClock()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	start := time.Now()
	r, err := l.Reserve(context.Background(), map[string]int64{"tokens": 101})
	if !errors.Is(err, limit.ErrTooLarge) || r != nil {
		t.Fatalf("got r=%v err=%v, want ErrTooLarge", r, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("ErrTooLarge was not immediate")
	}

	big := newLimiter(t, clk, "org", map[string]limit.Rate{"tokens": perMinute(1000)})
	if _, err := limit.ReserveAll(context.Background(), []*limit.Limiter{big, l}, map[string]int64{"tokens": 500}); !errors.Is(err, limit.ErrTooLarge) {
		t.Fatalf("ReserveAll: %v, want ErrTooLarge", err)
	}
	if big.Status().Dims["tokens"].Used != 0 {
		t.Fatal("ErrTooLarge charged the other limiter")
	}
	if _, err := l.Reserve(context.Background(), map[string]int64{"tokens": -1}); err == nil {
		t.Fatal("negative amount accepted")
	}
}

func TestSettleRefundWakesWaiter(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	r, err := l.Reserve(context.Background(), map[string]int64{"tokens": 100})
	if err != nil {
		t.Fatal(err)
	}
	ch := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 50})
	waitUntil(t, "waiter to queue", waiting(l))
	expectBlocked(t, ch)
	r.Settle(map[string]int64{"tokens": 30})
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.r.Time().Equal(t0) {
		t.Fatalf("granted at %v, want %v (no time had to pass)", res.r.Time(), t0)
	}
	if d := dim(t, l, "tokens"); d.Used != 80 {
		t.Fatalf("used %d, want 80", d.Used)
	}
}

func TestSettleExtraDelaysLaterReservations(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})
	r, err := l.Reserve(context.Background(), map[string]int64{"tokens": 50})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second)
	// The extra 100 was used during the call; it counts from when it is
	// reported, not from when the request was sent.
	r.Settle(map[string]int64{"tokens": 150})
	if d := dim(t, l, "tokens"); d.Used != 150 || d.Available != 0 {
		t.Fatalf("after extra: used=%d available=%d", d.Used, d.Available)
	}
	ch := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 10})
	waitUntil(t, "waiter to sleep", func() bool { return clk.Pending() == 1 })
	clk.Advance(30 * time.Second) // t0+60: the original 50 expires, 100 still in window
	expectBlocked(t, ch)
	if d := dim(t, l, "tokens"); d.Used != 100 {
		t.Fatalf("used=%d at t0+60s, want 100", d.Used)
	}
	clk.Advance(30 * time.Second)
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.r.Time().Equal(t0.Add(90 * time.Second)) {
		t.Fatalf("granted at %v, want t0+90s", res.r.Time().Sub(t0))
	}
}

func TestSettleKeepsUnreportedDimensions(t *testing.T) {
	l := newLimiter(t, newFakeClock(), "m", map[string]limit.Rate{"requests": perMinute(10), "tokens": perMinute(100)})
	r, err := l.Reserve(context.Background(), map[string]int64{"requests": 1, "tokens": 50})
	if err != nil {
		t.Fatal(err)
	}
	r.Settle(map[string]int64{"tokens": 20})
	if st := l.Status(); st.Dims["requests"].Used != 1 || st.Dims["tokens"].Used != 20 {
		t.Fatalf("status %+v", st.Dims)
	}
}

func TestCancelRefundsEverything(t *testing.T) {
	clk := newFakeClock()
	org := newLimiter(t, clk, "org", map[string]limit.Rate{"tokens": perMinute(1000)})
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"requests": perMinute(10), "tokens": perMinute(100)})
	r, err := limit.ReserveAll(context.Background(), []*limit.Limiter{l, org}, map[string]int64{"requests": 1, "tokens": 100})
	if err != nil {
		t.Fatal(err)
	}
	r.Cancel()
	for _, x := range []*limit.Limiter{l, org} {
		for name, d := range x.Status().Dims {
			if d.Used != 0 || d.Available != d.Limit {
				t.Fatalf("%s/%s after cancel: %+v", x.Name(), name, d)
			}
		}
	}
}

func TestSettleAndCancelAreOnce(t *testing.T) {
	l := newLimiter(t, newFakeClock(), "m", map[string]limit.Rate{"tokens": perMinute(100)})
	r, err := l.Reserve(context.Background(), map[string]int64{"tokens": 100})
	if err != nil {
		t.Fatal(err)
	}
	r.Settle(map[string]int64{"tokens": 30})
	r.Settle(map[string]int64{"tokens": 100})
	r.Cancel()
	if d := dim(t, l, "tokens"); d.Used != 30 {
		t.Fatalf("used %d, want 30 (second settle and cancel must be no-ops)", d.Used)
	}

	r2, err := l.Reserve(context.Background(), map[string]int64{"tokens": 50})
	if err != nil {
		t.Fatal(err)
	}
	r2.Cancel()
	r2.Cancel()
	r2.Settle(map[string]int64{"tokens": 70})
	if d := dim(t, l, "tokens"); d.Used != 30 {
		t.Fatalf("used %d, want 30", d.Used)
	}
}

func TestObserveLowersAvailabilityUntilReset(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"tokens": perMinute(100)})

	// Stale or reset-less observations, and ones that agree there is more
	// room than local accounting thinks, change nothing.
	l.Observe(limit.Info{Dims: map[string]limit.Observed{"tokens": {Remaining: 5, ResetAt: t0.Add(-time.Second)}}})
	l.Observe(limit.Info{Dims: map[string]limit.Observed{"tokens": {Remaining: 5}}})
	l.Observe(limit.Info{Dims: map[string]limit.Observed{"unknown": {Remaining: 0, ResetAt: t0.Add(time.Hour)}}})
	if d := dim(t, l, "tokens"); d.Available != 100 || !d.ProviderUntil.IsZero() {
		t.Fatalf("ignored observations changed state: %+v", d)
	}

	l.Observe(limit.Info{Dims: map[string]limit.Observed{"tokens": {Remaining: 10, ResetAt: t0.Add(20 * time.Second)}}})
	d := dim(t, l, "tokens")
	if d.Available != 10 || !d.ProviderUntil.Equal(t0.Add(20*time.Second)) || !d.NextReset.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("after observe: %+v", d)
	}
	// A later, more optimistic report (an older response arriving late)
	// does not raise it.
	l.Observe(limit.Info{Dims: map[string]limit.Observed{"tokens": {Remaining: 60, ResetAt: t0.Add(20 * time.Second)}}})
	if d := dim(t, l, "tokens"); d.Available != 10 {
		t.Fatalf("optimistic observe raised availability to %d", d.Available)
	}

	r, err := l.Reserve(context.Background(), map[string]int64{"tokens": 10})
	if err != nil {
		t.Fatal(err)
	}
	if d := dim(t, l, "tokens"); d.Available != 0 {
		t.Fatalf("available %d after taking the provider's 10", d.Available)
	}
	r.Cancel() // never reached the provider: the provider's 10 come back
	if d := dim(t, l, "tokens"); d.Available != 10 {
		t.Fatalf("available %d after cancel, want 10", d.Available)
	}
	if _, err := l.Reserve(context.Background(), map[string]int64{"tokens": 10}); err != nil {
		t.Fatal(err)
	}

	ch := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"tokens": 5})
	waitUntil(t, "waiter to sleep", func() bool { return clk.Pending() == 1 })
	clk.Advance(19 * time.Second)
	expectBlocked(t, ch)
	clk.Advance(time.Second)
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.r.Time().Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("granted at t0+%v, want t0+20s", res.r.Time().Sub(t0))
	}
	if d := dim(t, l, "tokens"); d.Used != 15 || d.Available != 85 || !d.ProviderUntil.IsZero() {
		t.Fatalf("after reset: %+v", d)
	}
}

func TestPauseUntil(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	l := newLimiter(t, clk, "m", map[string]limit.Rate{"requests": perMinute(100)})
	l.PauseUntil(t0.Add(5 * time.Second))
	l.PauseUntil(t0.Add(time.Second)) // an earlier pause does not shorten it
	if st := l.Status(); !st.PausedUntil.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("PausedUntil = %v", st.PausedUntil)
	}
	ch := reserveAsync(context.Background(), []*limit.Limiter{l}, map[string]int64{"requests": 1})
	waitUntil(t, "waiter to sleep", func() bool { return clk.Pending() == 1 })
	clk.Advance(4 * time.Second)
	expectBlocked(t, ch)
	clk.Advance(time.Second)
	res := expectResult(t, ch)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if !res.r.Time().Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("granted at t0+%v", res.r.Time().Sub(t0))
	}
	if st := l.Status(); !st.PausedUntil.IsZero() {
		t.Fatalf("pause still reported after it ended: %v", st.PausedUntil)
	}
}

// grant is what a test records about a reservation that was granted: when,
// and how much it finally held in each limiter's dimensions.
type grant struct {
	at      time.Time
	amounts map[string]int64
}

type ledger struct {
	mu     sync.Mutex
	grants map[*limit.Limiter][]*grant
}

func (g *ledger) add(ls []*limit.Limiter, x *grant) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, l := range ls {
		g.grants[l] = append(g.grants[l], x)
	}
}

// checkWindows asserts the sliding-window invariant: in every interval of
// length Per, the amounts granted in a dimension add up to at most Limit.
func (g *ledger) checkWindows(t *testing.T, l *limit.Limiter, limits map[string]limit.Rate) {
	t.Helper()
	gs := append([]*grant(nil), g.grants[l]...)
	sort.Slice(gs, func(i, j int) bool { return gs[i].at.Before(gs[j].at) })
	for name, rate := range limits {
		var sum int64
		lo := 0
		for hi := range gs {
			sum += gs[hi].amounts[name]
			for gs[lo].at.Add(rate.Per).Compare(gs[hi].at) <= 0 {
				sum -= gs[lo].amounts[name]
				lo++
			}
			if sum > rate.Limit {
				t.Fatalf("%s/%s: %d granted in the window ending %v, limit %d", l.Name(), name, sum, gs[hi].at, rate.Limit)
			}
		}
	}
}

// drive advances the fake clock to each next timer until done closes.
func drive(t *testing.T, clk *fakeClock, done <-chan struct{}) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-deadline:
			t.Fatal("reservations did not finish: deadlock?")
		default:
		}
		if !clk.AdvanceToNext() {
			time.Sleep(20 * time.Microsecond)
		}
	}
}

func TestReserveAllOppositeOrders(t *testing.T) {
	clk := newFakeClock()
	aLimits := map[string]limit.Rate{"requests": {Per: time.Second, Limit: 3}}
	bLimits := map[string]limit.Rate{"requests": {Per: time.Second, Limit: 2}, "tokens": {Per: time.Second, Limit: 10}}
	a := newLimiter(t, clk, "a", aLimits)
	b := newLimiter(t, clk, "b", bLimits)
	led := &ledger{grants: map[*limit.Limiter][]*grant{}}

	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ls := []*limit.Limiter{a, b}
			if i%2 == 1 {
				ls = []*limit.Limiter{b, a}
			}
			amounts := map[string]int64{"requests": 1, "tokens": int64(1 + i%5)}
			r, err := limit.ReserveAll(context.Background(), ls, amounts)
			if err != nil {
				t.Error(err)
				return
			}
			led.add(ls, &grant{at: r.Time(), amounts: amounts})
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	drive(t, clk, done)
	led.checkWindows(t, a, aLimits)
	led.checkWindows(t, b, bLimits)
	if len(led.grants[a]) != 40 || len(led.grants[b]) != 40 {
		t.Fatalf("grants: a=%d b=%d, want 40 each", len(led.grants[a]), len(led.grants[b]))
	}
}

func TestStressWindowsNeverExceeded(t *testing.T) {
	clk := newFakeClock()
	orgLimits := map[string]limit.Rate{"tokens": {Per: time.Second, Limit: 80}}
	mdlLimits := map[string]limit.Rate{"requests": {Per: time.Second, Limit: 7}, "tokens": {Per: time.Second, Limit: 50}}
	org := newLimiter(t, clk, "org", orgLimits)
	mdl := newLimiter(t, clk, "model", mdlLimits)
	led := &ledger{grants: map[*limit.Limiter][]*grant{}}

	const workers, each = 64, 20
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for range each {
				var ls []*limit.Limiter
				switch rng.IntN(3) {
				case 0:
					ls = []*limit.Limiter{mdl}
				case 1:
					ls = []*limit.Limiter{org, mdl}
				default:
					ls = []*limit.Limiter{mdl, org}
				}
				amounts := map[string]int64{"requests": 1, "tokens": 1 + rng.Int64N(50)}
				ctx, cancel := context.WithCancel(context.Background())
				if rng.IntN(10) == 0 {
					cancel() // some callers give up
				}
				r, err := limit.ReserveAll(ctx, ls, amounts)
				cancel()
				if errors.Is(err, context.Canceled) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				net := map[string]int64{"requests": 1, "tokens": amounts["tokens"]}
				switch rng.IntN(4) {
				case 0:
					r.Cancel()
					net = map[string]int64{}
				case 1:
					net["tokens"] = rng.Int64N(amounts["tokens"] + 1)
					r.Settle(map[string]int64{"tokens": net["tokens"]})
				}
				led.add(ls, &grant{at: r.Time(), amounts: net})
				if rng.IntN(8) == 0 {
					_ = org.Status()
					mdl.Observe(limit.Info{Dims: map[string]limit.Observed{
						"tokens": {Remaining: rng.Int64N(50), ResetAt: clk.Now().Add(100 * time.Millisecond)},
					}})
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	drive(t, clk, done)
	led.checkWindows(t, org, orgLimits)
	led.checkWindows(t, mdl, mdlLimits)
	t.Logf("grants: model=%d org=%d over %v of fake time", len(led.grants[mdl]), len(led.grants[org]), clk.Now().Sub(newFakeClock().Now()))
	for _, l := range []*limit.Limiter{org, mdl} {
		if st := l.Status(); st.Waiting != 0 {
			t.Fatalf("%s: %d waiters left behind", l.Name(), st.Waiting)
		}
	}
}

func ExampleReserveAll() {
	org, _ := limit.New(limit.Config{Name: "org", Limits: map[string]limit.Rate{
		"tokens": {Per: time.Minute, Limit: 400_000},
	}})
	mdl, _ := limit.New(limit.Config{Name: "sonnet", Limits: map[string]limit.Rate{
		"requests": {Per: time.Minute, Limit: 50},
		"tokens":   {Per: time.Minute, Limit: 40_000},
	}})
	r, err := limit.ReserveAll(context.Background(), []*limit.Limiter{org, mdl}, map[string]int64{"requests": 1, "tokens": 9_000})
	if err != nil {
		panic(err)
	}
	r.Settle(map[string]int64{"tokens": 6_500}) // the response said what it really used
	fmt.Println(mdl.Status().Dims["tokens"].Used, org.Status().Dims["tokens"].Used)
	// Output: 6500 6500
}
