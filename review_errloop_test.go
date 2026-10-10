package neumann_test

import (
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

func TestReviewTruncationRedoBecomesTooLarge(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{
		Limiters: []limit.Config{{Name: "gw", Limits: map[string]limit.Rate{"output_tokens": {Per: time.Minute, Limit: 30000}}}},
		Limits:   []string{"gw"},
	})
	trunc := func() (model.Response, error) { return sim.Truncated(), nil }
	sc.plan["A"] = []func() (model.Response, error){trunc, trunc, trunc}
	a := submit(t, m, proto.Instruction{Input: "A"})
	runUntilDone(t, m, clk, time.Minute, a)
	got := loadTask(t, m, a)
	var caps []int
	for _, c := range sc.of("A") {
		caps = append(caps, c.maxOut)
	}
	t.Logf("A ended %s %q, calls with caps %v, truncations %d", got.State, got.StopReason, caps, got.Truncations)
	if got.StopReason == "too_large" {
		t.Errorf("BUG: the truncation redo made the request too large for its limiter")
	}
	// The second redo is capped at what the limiter can ever grant.
	if got.StopReason != "truncated" || len(caps) != 3 || caps[2] != 30000 {
		t.Errorf("A ended %q with caps %v, want truncated after caps [0 16384 30000]", got.StopReason, caps)
	}
}
