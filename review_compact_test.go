package neumann_test

import (
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// A reasoning model spends a compaction's small output cap on reasoning and
// returns finish_reason "length" with no text, every time: the model step
// would redo such a turn with a larger cap (redoTruncated), but a
// compaction treats it as a transient failure at the same cap, so the task
// fails model_error after the retry limit although the model works.
func TestReviewTruncatedSummaryNeverRedone(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	var caps []int
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			caps = append(caps, req.MaxOut)
			if req.MaxOut < 2048 {
				return sim.Truncated(), nil // the cap went to reasoning
			}
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}, BackoffBase: time.Millisecond, BackoffJitter: identity})
	id := submit(t, m, proto.Instruction{Input: "go"})
	got := waitDone(t, m, id)
	t.Logf("task %s %q; compaction caps %v", got.State, got.StopReason, caps)
	if got.State == task.Failed && got.StopReason == "model_error" {
		t.Errorf("BUG: an empty truncated summary is retried at the same cap until the task fails")
	}
}

// A summary cut off by its cap with some text is accepted as it is: the
// rest of the span is gone from the model's context.
func TestReviewCutOffSummaryAccepted(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	cut := 0
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			resp := sim.Summarize(req)
			resp.FinishReason = "length"
			resp.Message = task.Text(task.RoleAssistant, resp.Message.TextContent()+" and th")
			cut++
			return resp, nil
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
	id := submit(t, m, proto.Instruction{Input: "go"})
	got := waitDone(t, m, id)
	sum := ""
	for _, msg := range got.Regs {
		if msg.Summary {
			sum = task.SummaryText(msg)
		}
	}
	t.Logf("task %s %q after %d cut-off summaries; summary in regs %q", got.State, got.StopReason, cut, sum)
	if cut > 0 && strings.HasSuffix(sum, "and th") {
		t.Errorf("NOTE: a summary cut off by finish_reason length was kept without notice")
	}
}
