package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Compaction (design §5.3): defaults and the stop reason of a task that
// cannot be made to fit its window.
const (
	// DefaultCompactAt is the share of the window above which a task is
	// compacted before its next model step.
	DefaultCompactAt = 0.8
	// DefaultCompactKeep is the share of the window the newest rounds kept
	// verbatim may take (at least the newest round is always kept).
	DefaultCompactKeep = 0.3
	// DefaultCompactOut caps a summary's output tokens; a window under
	// 8×DefaultCompactOut caps it at an eighth of the window.
	DefaultCompactOut = 2048

	StopContextOverflow = "context_overflow"
)

// errEmptySummary is a compaction answered with no text: there is nothing
// to put in place of the span, so it fails like a transient error.
var errEmptySummary = errors.New("compaction: the model answered with an empty summary")

// sizing is what compaction measures a task by in one group.
type sizing struct {
	window  int64   // tokens, input and output together
	at      float64 // the threshold, a share of the window
	keep    int64   // tokens the newest rounds kept verbatim may take
	out     int     // a summary's output cap
	reserve int64   // the output reserve of the task's model step
}

// sizeFor is the sizing of a task on a model with caps, in a group g (nil:
// the defaults), when the provider was seen to reject requests at learned
// tokens (0: never). sent is the output cap the task's model requests
// carry (0: the adapter's default). ok is false with no window known:
// such a task is never compacted ahead of a rejection.
func sizeFor(g *Group, caps model.Caps, learned, sent int) (sizing, bool) {
	w := int64(caps.Window)
	if learned > 0 && (w <= 0 || int64(learned) < w) {
		w = int64(learned)
	}
	if w <= 0 {
		return sizing{}, false
	}
	at, keep := DefaultCompactAt, DefaultCompactKeep
	if g != nil && g.CompactAt > 0 {
		at = g.CompactAt
	}
	if g != nil && g.CompactKeep > 0 {
		keep = g.CompactKeep
	}
	out := min(int64(DefaultCompactOut), w/8)
	if caps.MaxOut > 0 && int64(caps.MaxOut) < out {
		out = int64(caps.MaxOut)
	}
	return sizing{window: w, at: at, keep: int64(keep * float64(w)), out: int(max(out, 1)), reserve: outReserve(caps, sent)}, true
}

// outReserve is the output a model request is taken to use of the window:
// the cap it carries (sent), else the model's default (Caps.MaxOut), else
// DefaultMaxOut.
func outReserve(caps model.Caps, sent int) int64 {
	switch {
	case sent > 0:
		return int64(sent)
	case caps.MaxOut > 0:
		return int64(caps.MaxOut)
	}
	return DefaultMaxOut
}

// capsOf is what a reports, read under the adapter-panic guard; a panic
// reads as nothing known.
func capsOf(a model.Adapter) model.Caps {
	var caps model.Caps
	if guarded("Caps", func() { caps = model.CapsOf(a) }) != nil {
		return model.Caps{}
	}
	return caps
}

// unitCost is one task.Unit with what it costs: as messages in a model
// request (tokens), and as text in a compaction request (bytes: its
// transcript, JSON-escaped, which is what the request's estimate counts).
type unitCost struct {
	task.Unit
	tokens int64
	bytes  int
}

// span is a task's registers measured for compaction.
type span struct {
	head  int // regs[:head] is kept verbatim always: the task's request
	units []unitCost
}

func escapedLen(s string) int {
	b, _ := json.Marshal(s)
	return len(b) - 2
}

var (
	// compactBase is the estimate's byte count of a compaction request with
	// an empty transcript, and sepBytes what joining two units' transcripts
	// adds: so a request's estimate is computed from its units' bytes
	// exactly as task.EstimateTokens would count the whole request, without
	// marshalling every candidate chunk.
	compactBase = func() int {
		b, _ := json.Marshal(struct {
			S string
			T []task.ToolSpec
			M []task.Message
		}{task.CompactSystem, nil, task.CompactMessages([]task.Message{task.Text(task.RoleUser, "x")})})
		return len(b) - escapedLen(task.Transcript([]task.Message{task.Text(task.RoleUser, "x")}))
	}()
	sepBytes = escapedLen("\n\n")
)

// compactTokens is task.EstimateTokens of a compaction request whose
// transcript is bytes long once escaped.
func compactTokens(bytes int) int64 { return int64((compactBase+bytes)/4) + 1 }

func measure(regs []task.Message) span {
	head, units := task.Units(regs)
	sp := span{head: head, units: make([]unitCost, len(units))}
	for i, u := range units {
		msgs := regs[u.Start:u.End]
		sp.units[i] = unitCost{Unit: u, tokens: task.EstimateTokens("", nil, msgs), bytes: escapedLen(task.Transcript(msgs))}
	}
	return sp
}

// keptFrom is the index of the first unit kept verbatim: the newest units
// whose tokens fit in keep, at least the newest one, and never one before
// an incomplete unit (a round still waiting for results is never cut
// from its calls).
func keptFrom(us []unitCost, keep int64) int {
	if len(us) == 0 {
		return 0
	}
	k := len(us) - 1
	sum := us[k].tokens
	for k > 0 && sum+us[k-1].tokens <= keep {
		k--
		sum += us[k].tokens
	}
	for i := range k {
		if !us[i].Complete {
			return i
		}
	}
	return k
}

// chunkFits reports whether a compaction request for us fits the window
// with the summary's output cap.
func chunkFits(us []unitCost, sz sizing) bool {
	b := 0
	for i, u := range us {
		if i > 0 {
			b += sepBytes
		}
		b += u.bytes
	}
	return compactTokens(b)+int64(sz.out) <= sz.window
}

// nextChunk picks the next chunk of a compaction pass: units [lo, hi) of
// sp, oldest first, between the head and the units kept (keep). A span is
// worth compacting while it holds an original round or message, or two
// summaries or more; otherwise ok is false. The chunk is the longest run
// from the span's start whose request fits the window; it carries the
// summary of the chunk before, so the pass ends with one summary. When a
// summary at the start cannot take the next unit with it, that unit goes
// alone (its summary then sits after the earlier one, and a later chunk
// joins them). A unit that does not fit a request even alone is an error:
// it can never be compacted.
func nextChunk(us []unitCost, sz sizing, keep int64) (lo, hi int, ok bool, err error) {
	k := keptFrom(us, keep)
	spanUnits := us[:k]
	originals, summaries := 0, 0
	for _, u := range spanUnits {
		if u.Summary {
			summaries++
		} else {
			originals++
		}
	}
	if originals == 0 && summaries < 2 {
		return 0, 0, false, nil
	}
	tooLarge := func(u unitCost) error {
		return fmt.Errorf("a round of about %d tokens does not fit a compaction request in the window of %d tokens",
			compactTokens(u.bytes), sz.window)
	}
	j := 0
	for j < len(spanUnits) && chunkFits(spanUnits[:j+1], sz) {
		j++
	}
	if j >= 2 || (j == 1 && !spanUnits[0].Summary) {
		return 0, j, true, nil
	}
	if j == 0 && !spanUnits[0].Summary {
		return 0, 0, false, tooLarge(spanUnits[0])
	}
	i := slices.IndexFunc(spanUnits, func(u unitCost) bool { return !u.Summary })
	if i < 0 {
		return 0, 0, false, nil // summaries that cannot be joined: nothing gains
	}
	if !chunkFits(spanUnits[i:i+1], sz) {
		return 0, 0, false, tooLarge(spanUnits[i])
	}
	return i, i + 1, true, nil
}

// projectChunks is how many chunks the pass starting with [lo, hi) is
// expected to take: each summary is taken to be as long as its cap.
func projectChunks(us []unitCost, sz sizing, keep int64, lo, hi int) int {
	ph := unitCost{Unit: task.Unit{Summary: true, Complete: true}, tokens: int64(sz.out), bytes: escapedLen("### summary of earlier work\n") + 4*sz.out}
	n := 0
	for ok := true; ok && n <= 2*len(us)+2; {
		n++
		us = slices.Concat(us[:lo], []unitCost{ph}, us[hi:])
		lo, hi, ok, _ = nextChunk(us, sz, keep)
	}
	return n
}

// canCompact reports whether chunked compaction under sz could bring a task
// with these registers into the window: what is always kept (its request and
// the newest rounds within keep, or failing that its newest round alone),
// with a summary at its cap and the output reserve, fits the window, and
// every unit to be swapped out fits a compaction request. The registers are
// measured as provider would be sent them.
func canCompact(system string, tools []task.ToolSpec, regs []task.Message, provider string, sz sizing) error {
	sp := measure(regs)
	if len(sp.units) == 0 {
		return errors.New("nothing but the task's request is left to compact")
	}
	var last error
	for _, keep := range []int64{sz.keep, 0} {
		k := keptFrom(sp.units, keep)
		rest := slices.Concat(regs[:sp.head], regs[sp.units[k].Start:])
		after := task.EstimateTokens(system, tools, forProvider(rest, provider)) + int64(sz.out) + sz.reserve
		if after > sz.window {
			last = fmt.Errorf("the request and the newest round alone take about %d tokens with a summary and the output reserve", after)
			continue
		}
		for _, u := range sp.units[:k] {
			if compactTokens(u.bytes)+int64(sz.out) > sz.window {
				return fmt.Errorf("a round of about %d tokens does not fit a compaction request", compactTokens(u.bytes))
			}
		}
		return nil
	}
	return last
}

// decision is what the next model instruction of a task is, as far as its
// window goes.
type decision struct {
	compact  bool
	overflow string // the task cannot be made to fit: why

	from, to  int // the chunk: regs[from:to]
	keep      int64
	sz        sizing
	projected int // chunks the pass is expected to take, this one included
}

// windowFor measures t against its group's window before a model
// instruction (design §5.3). With no window known (the adapter's Caps, or
// one a rejection taught the task), nothing is compacted. Otherwise the
// task compacts when it is in a pass (PC StepCompact), when its request with
// the output reserve is over the threshold, or when must (the provider
// rejected it as too long). A task that does not fit the window must
// compact, keeping only its newest round if keeping more is not enough; if
// even that cannot make it fit, or a round to swap out cannot fit a
// compaction request, it overflows. A task over the threshold that still
// fits the window and has nothing to compact is sent as it is.
func (e *Env) windowFor(t *task.TCB, g *Group, a model.Adapter, must bool) decision {
	sz, ok := sizeFor(g, capsOf(a), t.Window, e.sendOut(g, t))
	if !ok {
		return decision{}
	}
	in := task.EstimateTokens(t.System, t.Tools, forProvider(t.Regs, a.Provider()))
	if !must && t.PC != task.StepCompact && float64(in+sz.reserve) <= sz.at*float64(sz.window) {
		return decision{}
	}
	must = must || in+sz.reserve > sz.window
	overflow := func(why error) decision {
		return decision{overflow: fmt.Sprintf("about %d tokens with %d reserved for output do not fit the window of %d tokens, and compaction cannot make them fit: %v",
			in, sz.reserve, sz.window, why)}
	}
	if must {
		if err := canCompact(t.System, t.Tools, t.Regs, a.Provider(), sz); err != nil {
			return overflow(err)
		}
	}
	sp := measure(t.Regs)
	for _, keep := range []int64{sz.keep, 0} {
		if keep == 0 && !must {
			break
		}
		lo, hi, ok, err := nextChunk(sp.units, sz, keep)
		if err != nil {
			if must {
				return overflow(err)
			}
			return decision{} // it fits as it is
		}
		if ok {
			return decision{compact: true, from: sp.units[lo].Start, to: sp.units[hi-1].End, keep: keep, sz: sz,
				projected: projectChunks(sp.units, sz, keep, lo, hi)}
		}
	}
	if must {
		return overflow(errors.New("nothing more can be compacted"))
	}
	return decision{}
}

// execCompact is one compaction instruction (design §5.3): a model call
// through the group exactly as a model step's (modelCall), sending the
// chunk d chose as a transcript under the fixed compaction prompt, with no
// tools and the summary's output cap. Its answer replaces the chunk with one
// summary message; the chunk's original messages (an earlier summary aside)
// go to the task's archive in the same commit, with a compacted event. The
// task stays at StepCompact while the pass has more to swap out, then goes
// back to StepModel. Pending steers are not applied: the next model step
// does that.
func (e *Env) execCompact(ctx context.Context, t *task.TCB, core int, g *Group, a model.Adapter, d decision) (mem.CommitSet, []Send, error) {
	chunk := task.CloneMessages(t.Regs[d.from:d.to])
	req := model.Request{System: task.CompactSystem, Messages: task.CompactMessages(chunk), MaxOut: d.sz.out}
	resp, c, out := e.modelCall(ctx, t, core, g, a, task.StepCompact, req, nil)
	if out != nil {
		return out.c, out.batch, out.err
	}
	text := strings.TrimSpace(resp.Message.TextContent())
	if text == "" {
		return e.modelFailed(t, c, model.Transient, errEmptySummary, nil, false), nil, nil
	}
	t.Retries = 0

	before, inBefore := len(t.Regs), task.EstimateTokens(t.System, t.Tools, forProvider(t.Regs, a.Provider()))
	rounds := 0
	var archive []task.Message
	for _, m := range chunk {
		if m.Summary {
			continue
		}
		if m.Role == task.RoleAssistant {
			rounds++
		}
		archive = append(archive, m)
	}
	t.Regs = slices.Concat(t.Regs[:d.from], []task.Message{task.SummaryMessage(text)}, t.Regs[d.to:])
	t.Archived += len(archive)

	n := t.CompactChunk + 1
	chunks, pc := n, task.StepModel
	t.CompactChunk = 0
	if _, _, more, _ := nextChunk(measure(t.Regs).units, d.sz, d.keep); more {
		chunks, pc = max(n+1, n-1+d.projected), task.StepCompact
		t.CompactChunk = n
	}
	e.ready(t, pc)
	c.Task = t
	c.Archive = archive
	c.Events = append(c.Events, proto.NewEvent(proto.EvCompacted, t.ID, map[string]any{
		"rounds": rounds, "messages_before": before, "messages_after": len(t.Regs),
		"tokens_before_est": inBefore, "tokens_after_est": task.EstimateTokens(t.System, t.Tools, forProvider(t.Regs, a.Provider())),
		"chunk": n, "chunks": chunks,
	}))
	return c, nil, nil
}

// contextRejected is what a request the provider rejected as longer than
// its window (model.ContextLength) means for its task: the window is
// smaller than that request, so the task learns a window three quarters of
// the request's estimate with its output cap (kept if smaller than one it
// learned before) and compacts to it, with a model_retry event (wait
// "compact"). A task that compaction cannot bring under that window fails
// with context_overflow. A compaction request rejected so goes on with
// smaller chunks.
func (e *Env) contextRejected(t *task.TCB, c mem.CommitSet, g *Group, a model.Adapter, req model.Request, err error) mem.CommitSet {
	out := outReserve(capsOf(a), req.MaxOut)
	learned := max((estimateInput(req)+out)*3/4, 1)
	if t.Window == 0 || learned < int64(t.Window) {
		t.Window = int(learned)
	}
	d := e.windowFor(t, g, a, true)
	if !d.compact {
		why := d.overflow
		if why == "" {
			why = "nothing can be compacted"
		}
		return failWith(t, c, StopContextOverflow, err.Error()+": "+why)
	}
	c.Events = append(c.Events, retryEvent(t, model.ContextLength.String(), 0, "compact", err.Error()))
	e.ready(t, task.StepCompact)
	c.Task = t
	return c
}

// modelPC is the model instruction t resumes at: a compaction pass in
// progress resumes as one, anything else at its model step.
func modelPC(t *task.TCB) task.PC {
	if t.PC == task.StepCompact {
		return task.StepCompact
	}
	return task.StepModel
}
