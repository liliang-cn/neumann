package neumann_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// smallWindow is a model with a 4k-token window and a 256-token output cap:
// a tool round of padCall(n, 400) is about 260 tokens, so a task outgrows
// it after a dozen rounds.
var smallWindow = model.Caps{Window: 4096, MaxOut: 256, Tools: true}

// padCall asks for dev_echo with args {"n":n,"pad":"xxx…"} (size x's): the
// echo device answers with the same bytes, so a round costs about twice
// size.
func padCall(n, size int) model.Response {
	args := fmt.Sprintf(`{"n":%d,"pad":%q}`, n, strings.Repeat("x", size))
	msg := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{
		Kind: task.PartToolCall, ProviderCallID: fmt.Sprintf("p-%d", n), Name: "dev_echo", Args: json.RawMessage(args),
	}}}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: task.Usage{PromptTokens: 10, CompletionTokens: 5}}
}

// rounder calls dev_echo for rounds rounds, then answers "done after N"
// with the rounds it counts (sim.Rounds: summaries included), and
// summarises any compaction request with sim.Summarize. It records every
// request.
type rounder struct {
	rounds, size int
	mu           sync.Mutex
	reqs         []model.Request
}

func (r *rounder) fn(req model.Request) (model.Response, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	if sim.IsCompaction(req) {
		return sim.Summarize(req), nil
	}
	if n := sim.Rounds(req); n < r.rounds {
		return padCall(n, r.size), nil
	}
	return sim.Reply(fmt.Sprintf("done after %d", sim.Rounds(req))), nil
}

func (r *rounder) requests() (model, compact []model.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, req := range r.reqs {
		if sim.IsCompaction(req) {
			compact = append(compact, req)
		} else {
			model = append(model, req)
		}
	}
	return model, compact
}

type compactedData struct {
	Rounds         int   `json:"rounds"`
	MessagesBefore int   `json:"messages_before"`
	MessagesAfter  int   `json:"messages_after"`
	TokensBefore   int64 `json:"tokens_before_est"`
	TokensAfter    int64 `json:"tokens_after_est"`
	Chunk          int   `json:"chunk"`
	Chunks         int   `json:"chunks"`
}

func compactedEvents(t *testing.T, m *neumann.Machine, id string) []compactedData {
	t.Helper()
	var out []compactedData
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvCompacted {
			var d compactedData
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				t.Fatal(err)
			}
			out = append(out, d)
		}
	}
	return out
}

func stepPCs(t *testing.T, m *neumann.Machine, id string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvStep {
			var d struct{ PC string }
			json.Unmarshal(ev.Data, &d)
			out[d.PC]++
		}
	}
	return out
}

func est(req model.Request) int64 { return task.EstimateTokens(req.System, req.Tools, req.Messages) }

// checkHistory checks what Machine.History gives for a task that ran
// rounds tool rounds of dev_echo and answered: the goal, every round's call
// and result in order, ids c-1… with nothing missing or repeated, the
// answer, and no summary.
func checkHistory(t *testing.T, m *neumann.Machine, id, goal string, rounds int) []task.Message {
	t.Helper()
	h, err := m.History(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.CheckPairing(h); err != nil {
		t.Fatalf("history breaks the pairing: %v", err)
	}
	if len(h) != 2*rounds+2 || h[0].TextContent() != goal {
		t.Fatalf("history has %d messages (want %d), first %q", len(h), 2*rounds+2, h[0].TextContent())
	}
	for i := range rounds {
		call, res := h[1+2*i], h[2+2*i]
		if call.Role != task.RoleAssistant || res.Role != task.RoleTool || call.Summary || res.Summary {
			t.Fatalf("round %d is %s/%s", i, call.Role, res.Role)
		}
		want := fmt.Sprintf("c-%d", i+1)
		if call.Parts[0].CallID != want || res.Parts[0].CallID != want || !strings.HasPrefix(res.Parts[0].Text, fmt.Sprintf(`{"n":%d,`, i)) {
			t.Fatalf("round %d: call %s, result %s %.20q", i, call.Parts[0].CallID, res.Parts[0].CallID, res.Parts[0].Text)
		}
	}
	return h
}

// A task that outgrows its window is compacted before the model step that
// would cross the threshold (0.8 of the window, with the output reserve):
// no model request it sends is over it, every compaction request fits the
// window with its bounded output, carries no tools and the fixed prompt,
// and the model still counts every round. Compaction calls are steps like
// any other. A group's own CompactAt moves the threshold.
func TestCompactionTriggersAtTheThreshold(t *testing.T) {
	for _, at := range []float64{0, 0.5} {
		t.Run(fmt.Sprintf("at=%v", at), func(t *testing.T) {
			r := &rounder{rounds: 20, size: 400}
			sm := &sim.Model{Fn: r.fn, Info: &smallWindow}
			m := startWith(t, neumann.Config{Devices: []bus.Device{echoDev()},
				Groups: []neumann.GroupConfig{{Class: "default", Model: sm, CompactAt: at}}})
			id := submit(t, m, proto.Instruction{Input: "count to twenty"})
			got := waitDone(t, m, id)
			if got.State != task.Done || got.LastAnswer() != "done after 20" {
				t.Fatalf("task ended %s %q: %q", got.State, got.StopReason, got.LastAnswer())
			}
			threshold := 0.8
			if at > 0 {
				threshold = at
			}
			models, compacts := r.requests()
			if len(compacts) == 0 {
				t.Fatal("no compaction")
			}
			for i, req := range models {
				if e := est(req) + 256; float64(e) > threshold*4096 {
					t.Errorf("model request %d is about %d tokens with its reserve, over %v of the window", i, e, threshold)
				}
			}
			for i, req := range compacts {
				if req.MaxOut <= 0 || req.MaxOut > 4096/8 || est(req)+int64(req.MaxOut) > 4096 || len(req.Tools) != 0 || len(req.Messages) != 1 {
					t.Errorf("compaction request %d: %d tokens, max_out %d, %d tools, %d messages", i, est(req), req.MaxOut, len(req.Tools), len(req.Messages))
				}
			}
			evs := compactedEvents(t, m, id)
			if len(evs) != len(compacts) {
				t.Fatalf("%d compacted events for %d compaction calls", len(evs), len(compacts))
			}
			for _, d := range evs {
				if d.Rounds < 1 || d.MessagesAfter >= d.MessagesBefore || d.TokensAfter >= d.TokensBefore || d.Chunk < 1 || d.Chunks < d.Chunk {
					t.Errorf("compacted %+v", d)
				}
			}
			pcs := stepPCs(t, m, id)
			if pcs["compact"] != len(compacts) || pcs["model"] != len(models) {
				t.Fatalf("step events %v for %d model and %d compaction calls", pcs, len(models), len(compacts))
			}
			if got.Used.Steps != sm.Calls() || got.PC != task.StepModel || got.CompactChunk != 0 {
				t.Fatalf("steps %d for %d calls; pc %s, chunk %d", got.Used.Steps, sm.Calls(), got.PC, got.CompactChunk)
			}
			checkHistory(t, m, id, "count to twenty", 20)
		})
	}
	t.Run("lower threshold compacts sooner", func(t *testing.T) {
		first := map[float64]int{}
		for _, at := range []float64{0.8, 0.5} {
			r := &rounder{rounds: 20, size: 400}
			m := startWith(t, neumann.Config{Devices: []bus.Device{echoDev()},
				Groups: []neumann.GroupConfig{{Class: "default", Model: &sim.Model{Fn: r.fn, Info: &smallWindow}, CompactAt: at}}})
			id := submit(t, m, proto.Instruction{Input: "go"})
			waitDone(t, m, id)
			r.mu.Lock()
			for i, req := range r.reqs {
				if sim.IsCompaction(req) {
					first[at] = i
					break
				}
			}
			r.mu.Unlock()
		}
		if first[0.5] >= first[0.8] {
			t.Fatalf("first compaction at call %d with 0.5, %d with 0.8", first[0.5], first[0.8])
		}
	})
}

// A long task against a small window: 60 tool rounds against 4k tokens,
// about 15k tokens of conversation. It completes; the model counts all 60
// rounds; every request it is sent keeps the pairing invariant (no result
// without its call, no call without its result) and starts with the task's
// goal verbatim; the registers end with the goal, a summary, and the latest
// rounds verbatim; and History gives back every original message.
func TestLongTaskCompactsAndCompletes(t *testing.T) {
	r := &rounder{rounds: 60, size: 400}
	m := startWith(t, neumann.Config{Model: &sim.Model{Fn: r.fn, Info: &smallWindow}, Devices: []bus.Device{echoDev()}})
	id := submit(t, m, proto.Instruction{Input: "the goal: count to sixty"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "done after 60" {
		t.Fatalf("task ended %s %q: %q", got.State, got.StopReason, got.LastAnswer())
	}
	models, compacts := r.requests()
	if len(compacts) < 3 {
		t.Fatalf("%d compactions in 60 rounds", len(compacts))
	}
	for i, req := range models {
		if err := task.CheckPairing(req.Messages); err != nil {
			t.Fatalf("model request %d: %v", i, err)
		}
		if req.Messages[0].TextContent() != "the goal: count to sixty" || req.Messages[0].Summary {
			t.Fatalf("model request %d does not start with the goal: %q", i, req.Messages[0].TextContent())
		}
	}
	if err := task.CheckPairing(got.Regs); err != nil {
		t.Fatalf("registers: %v", err)
	}
	if !got.Regs[1].Summary || got.Regs[2].Summary || len(got.Regs) > 20 {
		t.Fatalf("registers are not goal, summary, latest rounds: %d messages", len(got.Regs))
	}
	h := checkHistory(t, m, id, "the goal: count to sixty", 60)
	tail := got.Regs[2:]
	for i, msg := range tail {
		if j := len(h) - len(tail) + i; msg.Role != h[j].Role || msg.TextContent() != h[j].TextContent() ||
			(len(msg.Parts) > 0 && msg.Parts[0].CallID != h[j].Parts[0].CallID) {
			t.Fatalf("kept message %d differs from the original", i)
		}
	}
	if got.Archived != len(h)-1-len(tail) {
		t.Fatalf("archived %d, history has %d before the %d kept", got.Archived, len(h)-1, len(tail))
	}
	if sum := compactedEvents(t, m, id); len(sum) != len(compacts) {
		t.Fatalf("%d compacted events, %d compactions", len(sum), len(compacts))
	}
}

// The span between the goal and the rounds kept is cut into chunks whose
// requests each fit the window, one instruction per chunk: a task moved
// from a large window to a small one is compacted in several. Each chunk's
// event names it, i of n; the pass ends at the last.
func TestMultiChunkCompactionAfterAMigration(t *testing.T) {
	var quota atomic.Bool
	big := &rounder{rounds: 40, size: 400}
	bigModel := &sim.Model{Name: "big", Info: &model.Caps{Window: 64000, MaxOut: 256, Tools: true}, Fn: func(req model.Request) (model.Response, error) {
		if sim.Rounds(req) >= 30 && quota.CompareAndSwap(false, true) || quota.Load() {
			return model.Response{}, sim.QuotaExhausted()
		}
		return big.fn(req)
	}}
	little := &rounder{rounds: 40, size: 400}
	m := startWith(t, neumann.Config{Devices: []bus.Device{echoDev()}, Groups: []neumann.GroupConfig{
		{Class: "big", Model: bigModel, Fallback: "little"},
		{Class: "little", Model: &sim.Model{Name: "little", Fn: little.fn, Info: &smallWindow}},
	}})
	id := submit(t, m, proto.Instruction{Input: "count to forty"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.Class != "little" || got.LastAnswer() != "done after 40" {
		t.Fatalf("task ended %s %q in %q: %q", got.State, got.StopReason, got.Class, got.LastAnswer())
	}
	if _, c := big.requests(); len(c) != 0 {
		t.Fatalf("big compacted %d times below its threshold", len(c))
	}
	little.mu.Lock()
	first := little.reqs[0]
	little.mu.Unlock()
	if !sim.IsCompaction(first) {
		t.Fatal("the first instruction on the small group was not a compaction")
	}
	if len(migratedEvents(t, m, id)) != 1 {
		t.Fatal("not migrated once")
	}
	evs := compactedEvents(t, m, id)
	if len(evs) < 2 || evs[0].Chunk != 1 || evs[1].Chunk != 2 {
		t.Fatalf("compacted %+v: want a pass of several chunks", evs)
	}
	pass := 0
	for pass < len(evs) && evs[pass].Chunk == pass+1 {
		pass++
	}
	if pass < 2 || evs[pass-1].Chunks != pass {
		t.Fatalf("the first pass ran %d chunks: %+v", pass, evs)
	}
	_, compacts := little.requests()
	for i, req := range compacts {
		if est(req)+int64(req.MaxOut) > 4096 {
			t.Fatalf("chunk request %d does not fit the window: %d + %d", i, est(req), req.MaxOut)
		}
	}
	checkHistory(t, m, id, "count to forty", 40)
}

// A single round larger than the window can never be sent, nor fit a
// compaction request: the task fails with context_overflow, without
// sending it, and without compacting what cannot help. So does a goal
// larger than the window, before any call; and so does an old round too
// large for a compaction request, once newer ones push the task over the
// window (while it still fits, the task goes on uncompacted).
func TestOversizedMessageOverflows(t *testing.T) {
	cases := []struct {
		name  string
		goal  string
		sizes []int // the pad of each round
		calls int   // model calls before the failure
	}{
		{"goal", strings.Repeat("g", 20000), nil, 0},
		{"latest round", "go", []int{100, 9000}, 2},
		// A round that fits a model request in an 8k window, but not a
		// compaction request (oldRoundPad), then small ones.
		{"old round", "go", []int{0, 100, 100, 100, 100}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			window := model.Caps{Window: 4096, MaxOut: 256, Tools: true}
			if c.name == "old round" {
				window.Window = 8192
				c.sizes[0] = oldRoundPad(t, c.goal, window)
			}
			var compactions atomic.Int32
			sm := &sim.Model{Info: &window, Fn: func(req model.Request) (model.Response, error) {
				if sim.IsCompaction(req) {
					compactions.Add(1)
					return sim.Summarize(req), nil
				}
				if n := sim.Rounds(req); n < len(c.sizes) {
					return padCall(n, c.sizes[n]), nil
				}
				return sim.Reply("done"), nil
			}}
			m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
			id := submit(t, m, proto.Instruction{Input: c.goal})
			got := waitDone(t, m, id)
			if got.State != task.Failed || got.StopReason != "context_overflow" {
				t.Fatalf("task ended %s %q", got.State, got.StopReason)
			}
			if c.calls >= 0 && sm.Calls() != c.calls {
				t.Fatalf("%d model calls, want %d", sm.Calls(), c.calls)
			}
			if n := compactions.Load(); n != 0 {
				t.Fatalf("%d compactions that could not help", n)
			}
			if fe := failedEvent(t, m, id); fe["stop_reason"] != "context_overflow" || !strings.Contains(fe["error"], "window") {
				t.Fatalf("failed event %v", fe)
			}
		})
	}
}

// oldRoundPad is the smallest pad for which a first round of padCall fits a
// model request in w (with its output reserve) but not, on its own, a
// compaction request (with a summary as long as the model's output cap): the
// compaction prompt is the difference. It measures with the tools a machine
// actually sends.
func oldRoundPad(t *testing.T, goal string, w model.Caps) int {
	t.Helper()
	var tools []task.ToolSpec
	probe := startWith(t, neumann.Config{Devices: []bus.Device{echoDev()}, Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		tools = req.Tools
		return sim.Reply("x"), nil
	}}})
	waitDone(t, probe, submit(t, probe, proto.Instruction{Input: goal}))
	measure := func(pad int) (asModel, asChunk int64) {
		call := padCall(0, pad).Message
		call.Parts[0].CallID = "c-1"
		round := []task.Message{call, {Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: string(call.Parts[0].Args)}}}}
		asModel = task.EstimateTokens("", tools, append([]task.Message{task.Text(task.RoleUser, goal)}, round...)) + int64(w.MaxOut)
		asChunk = task.EstimateTokens(task.CompactSystem, nil, task.CompactMessages(round)) + int64(w.MaxOut)
		return asModel, asChunk
	}
	// Both grow with the pad: find the smallest pad whose chunk is too
	// large, then check that its model request still fits.
	lo, hi := 0, 8*w.Window
	for lo < hi {
		mid := (lo + hi) / 2
		if _, c := measure(mid); c > int64(w.Window) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if m, _ := measure(lo); m <= int64(w.Window) {
		return lo
	}
	t.Fatal("no pad fits a model request and not a compaction request")
	return 0
}

// windowed is a provider that does not say how large its window is, and
// rejects any request estimated above limit tokens as too long, as a real
// one does with context_length_exceeded.
func windowed(limit int64, r *rounder) *sim.Model {
	return &sim.Model{Info: &model.Caps{MaxOut: 256, Tools: true}, Fn: func(req model.Request) (model.Response, error) {
		if est(req) > limit {
			return model.Response{}, sim.ContextTooLong()
		}
		return r.fn(req)
	}}
}

// A provider rejecting a request as too long for its window, with no window
// known beforehand, makes the task compact (a model_retry of class
// context_length, waiting for "compact") and go on, instead of failing:
// the window it learned is kept on the task, so it compacts ahead of the
// next rejection. A rejection with nothing left to compact fails the task
// with context_overflow, not model_error.
func TestContextLengthRejectionCompactsThenSucceeds(t *testing.T) {
	r := &rounder{rounds: 25, size: 400}
	sm := windowed(2500, r)
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
	id := submit(t, m, proto.Instruction{Input: "count to twenty-five"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "done after 25" {
		t.Fatalf("task ended %s %q: %q", got.State, got.StopReason, got.LastAnswer())
	}
	var rejected int
	for _, rd := range retries(t, m, id) {
		if rd.Class == "context_length" {
			rejected++
			if rd.Wait != "compact" {
				t.Fatalf("retry %+v", rd)
			}
		}
	}
	if rejected == 0 || len(compactedEvents(t, m, id)) == 0 {
		t.Fatalf("%d rejections, %d compactions", rejected, len(compactedEvents(t, m, id)))
	}
	if got.Window <= 0 || got.Window >= 2500 {
		t.Fatalf("learned window %d", got.Window)
	}
	if got.Used.Steps+rejected != sm.Calls() {
		t.Fatalf("steps %d + %d rejected != %d calls", got.Used.Steps, rejected, sm.Calls())
	}
	checkHistory(t, m, id, "count to twenty-five", 25)

	t.Run("nothing to compact", func(t *testing.T) {
		sm := windowed(10, &rounder{rounds: 1, size: 10})
		m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
		id := submit(t, m, proto.Instruction{Input: "too long already"})
		got := waitDone(t, m, id)
		if got.State != task.Failed || got.StopReason != "context_overflow" || sm.Calls() != 1 {
			t.Fatalf("task ended %s %q after %d calls", got.State, got.StopReason, sm.Calls())
		}
	})
}

// A compaction call fails like a model call: a transient failure backs off
// and the compaction is tried again (the pass resumes at StepCompact); the
// task then completes.
func TestCompactionFailureBacksOff(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	var failed atomic.Bool
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) && failed.CompareAndSwap(false, true) {
			return model.Response{}, sim.Transient()
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}, BackoffBase: time.Millisecond, BackoffJitter: identity})
	id := submit(t, m, proto.Instruction{Input: "go"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.LastAnswer() != "done after 20" || got.Retries != 0 {
		t.Fatalf("task ended %s %q: %q, retries %d", got.State, got.StopReason, got.LastAnswer(), got.Retries)
	}
	rs := retries(t, m, id)
	if len(rs) != 1 || rs[0].Class != "transient" {
		t.Fatalf("retries %+v", rs)
	}
	evs := eventsOf(t, m, id)
	for i, ev := range evs {
		if ev.Kind == proto.EvModelRetry {
			var next struct{ PC string }
			for _, e := range evs[i+1:] {
				if e.Kind == proto.EvStep {
					json.Unmarshal(e.Data, &next)
					break
				}
			}
			if next.PC != "compact" {
				t.Fatalf("the step after the failed compaction is %q", next.PC)
			}
		}
	}
	if got.Used.Steps+1 != sm.Calls() {
		t.Fatalf("steps %d, calls %d", got.Used.Steps, sm.Calls())
	}
}

// A cancel or a deadline during a compaction call ends the task at once:
// the call is interrupted (unconfirmed), nothing of the compaction is
// committed, and the registers and archive are as they were.
func TestCancelAndDeadlineDuringCompaction(t *testing.T) {
	for _, how := range []string{"cancel", "deadline"} {
		t.Run(how, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			r := &rounder{rounds: 20, size: 400}
			entered := make(chan struct{}, 1)
			a := &adapter{name: "small", caps: &smallWindow, fn: func(ctx context.Context, req model.Request) (model.Response, error) {
				if sim.IsCompaction(req) {
					entered <- struct{}{}
					<-ctx.Done()
					return model.Response{}, ctx.Err()
				}
				return r.fn(req)
			}}
			m := startWith(t, neumann.Config{Model: a, Devices: []bus.Device{echoDev()}, Clock: clk})
			in := proto.Instruction{Kind: proto.KindSubmit, Input: "go"}
			if how == "deadline" {
				in.Budget = &task.Budget{Deadline: clk.Now().Add(time.Hour)}
			}
			id, err := m.Submit(bg, in)
			if err != nil {
				t.Fatal(err)
			}
			<-entered
			before := loadTask(t, m, id)
			if before.State != task.Running || before.PC != task.StepCompact {
				t.Fatalf("during compaction the task is %s at %s", before.State, before.PC)
			}
			if how == "cancel" {
				if err := m.Cancel(bg, id); err != nil {
					t.Fatal(err)
				}
			} else {
				clk.Advance(2 * time.Hour)
			}
			got := waitDone(t, m, id)
			want := map[string]task.State{"cancel": task.Cancelled, "deadline": task.Failed}[how]
			if got.State != want || got.Unconfirmed != 1 || len(got.Regs) != len(before.Regs) || got.Archived != 0 {
				t.Fatalf("task ended %s %q, unconfirmed %d, %d registers (had %d), archived %d",
					got.State, got.StopReason, got.Unconfirmed, len(got.Regs), len(before.Regs), got.Archived)
			}
			if len(compactedEvents(t, m, id)) != 0 {
				t.Fatal("a compacted event for an interrupted compaction")
			}
			if h, err := m.History(bg, id); err != nil || len(h) != len(got.Regs) {
				t.Fatalf("history %d messages (%v), registers %d", len(h), err, len(got.Regs))
			}
		})
	}
}

// Compaction calls are answered steps: the step budget counts them, and a
// task out of budget before a compaction ends "budget" without making it.
func TestCompactionCountsAgainstTheBudget(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	sm := &sim.Model{Info: &smallWindow, Fn: r.fn}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}})
	probe := submit(t, m, proto.Instruction{Input: "go"})
	waitDone(t, m, probe)
	_, compacts := r.requests()
	r.mu.Lock()
	firstCompact := 0
	for i, req := range r.reqs {
		if sim.IsCompaction(req) {
			firstCompact = i
			break
		}
	}
	r.mu.Unlock()
	if len(compacts) == 0 {
		t.Fatal("no compaction")
	}
	id := submit(t, m, proto.Instruction{Input: "go", Budget: &task.Budget{MaxSteps: firstCompact + 1}})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.StopReason != "budget" || got.Used.Steps != firstCompact+1 {
		t.Fatalf("task ended %s %q after %d steps", got.State, got.StopReason, got.Used.Steps)
	}
	if pcs := stepPCs(t, m, id); pcs["compact"] != 1 {
		t.Fatalf("steps %v: the budget's last step should be the compaction", pcs)
	}
}

// A steer sent while a compaction runs is not applied by it: it stays
// pending through the compaction's commit, and the next model step delivers
// it as an ordinary user message after the summary.
func TestSteerDuringCompactionReachesTheNextStep(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var steered atomic.Bool
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			once.Do(func() { close(entered); <-gate })
		} else if texts := sim.UserTexts(req); texts[len(texts)-1] == "also say hello" {
			steered.Store(true)
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}, SteerFormat: func(s string) string { return s }})
	id := submit(t, m, proto.Instruction{Input: "go"})
	<-entered
	if err := m.Steer(bg, id, "also say hello"); err != nil {
		t.Fatal(err)
	}
	close(gate)
	got := waitDone(t, m, id)
	if got.State != task.Done || !steered.Load() {
		t.Fatalf("task ended %s; the steer reached the model: %v", got.State, steered.Load())
	}
	evs := eventsOf(t, m, id)
	compacted, applied := -1, -1
	for i, ev := range evs {
		switch {
		case ev.Kind == proto.EvCompacted && compacted < 0:
			compacted = i
		case ev.Kind == proto.EvSteerDropped:
			t.Fatal("the steer was dropped")
		}
	}
	h, _ := m.History(bg, id)
	for i, msg := range h {
		if msg.Role == task.RoleUser && msg.TextContent() == "also say hello" {
			applied = i
		}
	}
	if compacted < 0 || applied < 0 {
		t.Fatalf("compacted at event %d, steer at history %d", compacted, applied)
	}
}

// A group's compaction settings: shares of the window, the kept share below
// the threshold (else every step would compact again).
func TestCompactionSettingsAreValidated(t *testing.T) {
	a := &adapter{name: "a", fn: reply("a")}
	for name, g := range map[string]neumann.GroupConfig{
		"negative at":   {CompactAt: -0.1},
		"at above one":  {CompactAt: 1.5},
		"negative keep": {CompactKeep: -0.1},
		"keep at at":    {CompactAt: 0.5, CompactKeep: 0.5},
		"keep above":    {CompactKeep: 0.9},
	} {
		g.Class, g.Model = "x", a
		if _, err := neumann.New(neumann.Config{Groups: []neumann.GroupConfig{g}}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, g := range []neumann.GroupConfig{{CompactAt: 1, CompactKeep: 0.9}, {CompactKeep: 0.5}, {CompactAt: 0.4}} {
		g.Class, g.Model = "x", a
		if _, err := neumann.New(neumann.Config{Groups: []neumann.GroupConfig{g}}); err != nil {
			t.Errorf("%+v: %v", g, err)
		}
	}
}
