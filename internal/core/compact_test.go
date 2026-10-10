package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// round is an assistant turn calling one tool and its result, padded.
func padRound(id, pad string) []task.Message {
	return []task.Message{
		{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: id, Name: "dev_x", Args: json.RawMessage(fmt.Sprintf(`{"pad":%q}`, pad))}}},
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: id, Text: pad}}},
	}
}

func regsOf(goal string, parts ...[]task.Message) []task.Message {
	out := []task.Message{task.Text(task.RoleUser, goal)}
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// A chunk's request is measured from its units' bytes, not marshalled
// whole: the figure must be exactly what task.EstimateTokens gives the
// request, whatever the text holds (quotes, newlines, HTML, non-Latin,
// invalid UTF-8).
func TestCompactRequestEstimateIsExact(t *testing.T) {
	odd := []string{`plain`, `"quoted" \ back`, "new\nlines\ttabs", "<html> & </html>", "日本語ではない中文", "bad \xff utf8", strings.Repeat("x", 3001)}
	var parts [][]task.Message
	for i, s := range odd {
		parts = append(parts, padRound(fmt.Sprintf("c-%d", i+1), s), []task.Message{task.Text(task.RoleUser, s)})
	}
	parts = append(parts, []task.Message{task.SummaryMessage("rounds=2 \"so far\"")})
	regs := regsOf("goal", parts...)
	sp := measure(regs)
	for lo := range sp.units {
		for hi := lo + 1; hi <= len(sp.units); hi++ {
			b := 0
			for i, u := range sp.units[lo:hi] {
				if i > 0 {
					b += sepBytes
				}
				b += u.bytes
			}
			msgs := regs[sp.units[lo].Start:sp.units[hi-1].End]
			if got, want := compactTokens(b), task.EstimateTokens(task.CompactSystem, nil, task.CompactMessages(msgs)); got != want {
				t.Fatalf("units [%d,%d): measured %d, estimate %d", lo, hi, got, want)
			}
		}
	}
}

func sizingOf(window int64) sizing {
	out := int(min(2048, window/8))
	return sizing{window: window, at: DefaultCompactAt, keep: int64(DefaultCompactKeep * float64(window)), out: out, sum: out, reserve: 256}
}

// A pass's chunks: the longest run from the span's start that fits a
// request, carrying the summary before it; the newest rounds within keep
// stay; a summary that cannot take the next round with it lets that round
// go alone; a span of one summary is done; two summaries are joined.
func TestNextChunk(t *testing.T) {
	pad := strings.Repeat("p", 800) // a round is about 420 tokens, its transcript about 430
	var rounds [][]task.Message
	for i := range 12 {
		rounds = append(rounds, padRound(fmt.Sprintf("c-%d", i+1), pad))
	}
	s := sizingOf(4096)
	us := measure(regsOf("goal", rounds...)).units
	lo, hi, ok, err := nextChunk(us, s, s.keep, 0)
	if err != nil || !ok || lo != 0 || hi < 2 || hi >= len(us)-2 {
		t.Fatalf("first chunk [%d,%d) ok %v err %v of %d units", lo, hi, ok, err, len(us))
	}
	if !chunkFits(us[lo:hi], s) || chunkFits(us[lo:hi+1], s) {
		t.Fatalf("chunk [%d,%d) is not the longest that fits", lo, hi)
	}
	if k := keptFrom(us, s.keep); k != len(us)-2 {
		t.Fatalf("kept from %d of %d: 30%% of 4096 holds two rounds", k, len(us))
	}

	if lo, hi, ok, _ := nextChunk(us, s, s.keep, 2); !ok || lo != 0 || hi != 2 {
		t.Fatalf("a split chunk carries at most the limit: [%d,%d) %v", lo, hi, ok)
	}

	summary := []task.Message{task.SummaryMessage("rounds=5")}
	us = measure(regsOf("goal", summary, rounds[0], rounds[1], rounds[2])).units
	if lo, hi, ok, _ := nextChunk(us, s, 0, 0); !ok || lo != 0 || hi != 3 {
		t.Fatalf("the summary goes with the next rounds: [%d,%d) %v", lo, hi, ok)
	}

	big := []task.Message{task.SummaryMessage(strings.Repeat("s", 12400))} // about 3100 tokens
	us = measure(regsOf("goal", big, rounds[0], rounds[1], rounds[2])).units
	if lo, hi, ok, _ := nextChunk(us, s, 0, 0); !ok || lo != 1 || hi != 2 {
		t.Fatalf("a round that cannot join the summary goes alone: [%d,%d) %v", lo, hi, ok)
	}

	us = measure(regsOf("goal", summary, rounds[0])).units
	if _, _, ok, _ := nextChunk(us, s, 0, 0); ok {
		t.Fatal("one summary before the kept round: nothing to compact")
	}
	us = measure(regsOf("goal", summary, summary, rounds[0])).units
	if lo, hi, ok, _ := nextChunk(us, s, 0, 0); !ok || lo != 0 || hi != 2 {
		t.Fatalf("two summaries are joined: [%d,%d) %v", lo, hi, ok)
	}

	huge := padRound("c-99", strings.Repeat("h", 16000))
	us = measure(regsOf("goal", huge, rounds[0])).units
	if _, _, ok, err := nextChunk(us, s, 0, 0); ok || err == nil || !strings.Contains(err.Error(), "does not fit a compaction request") {
		t.Fatalf("a round too large for a request: ok %v, err %v", ok, err)
	}

	pending := []task.Message{{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-50", Name: "dev_x"}}}}
	us = measure(regsOf("goal", rounds[0], pending, task.CompactMessages(nil), rounds[1])).units
	if k := keptFrom(us, 0); k != 1 {
		t.Fatalf("a round waiting for results is never swapped out: kept from %d", k)
	}
}

// A compaction whose result commit is refused because a cancel was
// committed first (behind the vCore's back, before it was signalled): the
// cancel's final commit is made instead, and nothing of the compaction is:
// the registers are as they were, nothing is archived, and the answered
// call still counts as a step.
func TestCompactionYieldsToACommittedCancel(t *testing.T) {
	var store *mem.Memory
	pad := strings.Repeat("p", 800)
	e, s, sm := newTestEnv(t, func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled}}}); err != nil {
				t.Error(err)
			}
			return sim.Summarize(req), nil
		}
		return sim.Reply("x"), nil
	})
	store = s
	sm.Info = &model.Caps{Window: 4096, MaxOut: 256, Tools: true}
	tc := newTask(t, e, s, "goal")
	var regs []task.Message
	for i := range 9 {
		regs = append(regs, padRound(fmt.Sprintf("c-%d", i+1), pad)...)
	}
	tc.Regs = append(tc.Regs, regs...)
	tc.Version++
	if _, err := s.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	e.Sched.Add(tc.Clone())
	e.step(bg, take(t, e), 0)
	got := load(t, s)
	if got.State != task.Cancelled || len(got.Regs) != 19 || got.Archived != 0 || got.Used.Steps != 1 {
		t.Fatalf("task %s, %d registers, archived %d, steps %d", got.State, len(got.Regs), got.Archived, got.Used.Steps)
	}
	if a, _ := s.Archive(bg, "t1"); len(a) != 0 {
		t.Fatalf("archived %d messages", len(a))
	}
	evs, _ := s.Events(bg, 0, 0)
	for _, ev := range evs {
		if ev.Kind == proto.EvCompacted {
			t.Fatal("a compacted event for a compaction that never landed")
		}
	}
}
