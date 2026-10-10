package core

import (
	"math"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// limitClock runs a limiter on a sim.Clock.
type limitClock struct{ *sim.Clock }

func (c limitClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.AfterFunc(d, func() { ch <- c.Now() })
	return ch
}

func tokenLimiter(t *testing.T, clk *sim.Clock) *limit.Limiter {
	t.Helper()
	l, err := limit.New(limit.Config{Name: "tpm", Limits: map[string]limit.Rate{"tokens": {Per: time.Minute, Limit: 1000}}, Clock: limitClock{clk}})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// design §7.3: a response is settled before its rate headers are observed.
// The provider's remaining figure already counts this request; observed
// first, the settle would charge the request's extra usage a second time.
func TestSettleBeforeObserve(t *testing.T) {
	clk := sim.NewClock(time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC))
	resp := model.Response{Usage: task.Usage{PromptTokens: 200, CompletionTokens: 100}}
	// The provider saw this request use 300 of 1000.
	info := &model.RateInfo{Dims: map[string]model.RateDim{"tokens": {Remaining: 700, ResetAt: clk.Now().Add(30 * time.Second)}}}

	e, _, _ := newTestEnv(t, reply)
	l := tokenLimiter(t, clk)
	g := &Group{Class: "default", Limiters: []*limit.Limiter{l}}
	res, err := l.Reserve(bg, map[string]int64{"tokens": 100})
	if err != nil {
		t.Fatal(err)
	}
	e.settleCall(g, res, settleActual, model.Request{}, resp, info)
	if got := l.Status().Dims["tokens"].Available; got != 700 {
		t.Fatalf("available %d after settle then observe, want 700", got)
	}

	// The other order counts the 200 extra tokens twice.
	wrong := tokenLimiter(t, clk)
	res, _ = wrong.Reserve(bg, map[string]int64{"tokens": 100})
	wrong.Observe(limit.Info{Dims: map[string]limit.Observed{"tokens": {Remaining: 700, ResetAt: clk.Now().Add(30 * time.Second)}}})
	res.Settle(map[string]int64{"tokens": 300})
	if got := wrong.Status().Dims["tokens"].Available; got != 500 {
		t.Fatalf("observe then settle left %d; the test no longer tells the orders apart", got)
	}
}

// Without a model.Metered adapter: one request, the input estimated from the
// request's size, the output cap for output tokens; settled at the reported
// usage, or at the reservation when none was reported.
func TestDefaultAmounts(t *testing.T) {
	e, _, _ := newTestEnv(t, reply)
	req := model.Request{System: "be brief", Messages: []task.Message{task.Text(task.RoleUser, "what is two plus two, exactly?")}}
	got := reserveAmounts(e.Model, req, 4096)
	if got["requests"] != 1 || got["output_tokens"] != 4096 || got["tokens"] < 10 || got["tokens"] != got["input_tokens"] {
		t.Fatalf("reserve %v", got)
	}
	s := settleAmounts(e.Model, req, model.Response{Usage: task.Usage{PromptTokens: 40, CompletionTokens: 7}})
	if s["requests"] != 1 || s["tokens"] != 47 || s["input_tokens"] != 40 || s["output_tokens"] != 7 {
		t.Fatalf("settle %v", s)
	}
	if s := settleAmounts(e.Model, req, model.Response{}); len(s) != 1 || s["requests"] != 1 {
		t.Fatalf("settle without usage %v", s)
	}
	if n := e.maxOut(&task.TCB{}); n != DefaultMaxOut {
		t.Fatalf("maxOut %d", n)
	}
}

// The backoff step doubles from the base up to the cap; the jitter maps it
// to the delay, which never undercuts the provider's retry-after.
func TestBackoffDelay(t *testing.T) {
	e, _, _ := newTestEnv(t, reply)
	e.Jitter = func(d time.Duration) time.Duration { return d }
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 7: 60 * time.Second, 50: 60 * time.Second} {
		if got := e.backoffDelay(attempt, 0); got != want {
			t.Fatalf("attempt %d: %v, want %v", attempt, got, want)
		}
	}
	if got := e.backoffDelay(1, 5*time.Second); got != 5*time.Second {
		t.Fatalf("retry-after floor: %v", got)
	}
	e.BackoffBase, e.BackoffMax = time.Duration(math.MaxInt64/3), time.Duration(math.MaxInt64)
	for _, attempt := range []int{2, 3, 64} {
		if got := e.backoffDelay(attempt, 0); got <= 0 {
			t.Fatalf("attempt %d near the largest duration: %v", attempt, got)
		}
	}
	e.BackoffBase, e.BackoffMax = 0, 0
	e.Jitter = nil
	if d := fullJitter(time.Duration(math.MaxInt64)); d < 0 {
		t.Fatalf("full jitter of the largest duration: %v", d)
	}
	for range 100 {
		if d := e.backoffDelay(3, 0); d < 0 || d > 4*time.Second {
			t.Fatalf("full jitter out of range: %v", d)
		}
	}
}
