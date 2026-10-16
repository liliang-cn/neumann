package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// execModel is one model instruction: apply pending steers, call the model
// once, and describe the result as a commit set. When the model asks for
// tools, the same commit records the calls as dispatched and the task as
// waiting on I/O, and the returned batch is dispatched right after it: a tool
// round does not queue for a vCore a second time just to hand calls over.
//
// Before the call, the task's context is measured against its group's
// window (windowFor, design §5.3): a task over the threshold, or in the middle
// of a compaction pass (PC StepCompact), runs a compaction instead
// (execCompact), and one that cannot be made to fit fails with
// context_overflow without a call.
//
// Around the call (modelCall, design §5.1, §6, §7): a task whose group is
// down waits for it instead (Waiting(Group)); otherwise room is reserved in
// the group's limiters, waiting while the vCore holds the task and before
// anything is committed; the group admits the request (or the task waits for
// it after all); the request is committed Running and sent; and its outcome
// is settled with the limiters and reported to the group before the task's
// own commit is put together. A failed call is retried, waited out or fails
// the task by its error class (modelFailed); a turn cut off before it
// produced anything is asked again with a larger output cap (redoTruncated).
//
// Each steer with a sender that the step's commit applies is reported by a
// steer_delivered event ({seq, from}) at the head of that commit's events:
// the steer went into the request at the step's start (delivered).
func (e *Env) execModel(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []Send, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, nil, fmt.Errorf("read interrupts: %w", err)
	}
	c, batch, err := e.modelStep(ctx, t, core, pending)
	return delivered(t.ID, c, pending), batch, err
}

// delivered puts a steer_delivered event for each steer with a sender among
// c.Applied in front of c's events, in seq order. A steer without a sender
// is delivered silently, as before senders existed. c need not carry its
// task yet: a step that ends in an error is committed by the run loop, which
// sets it (fail) and keeps Applied and these events.
func delivered(id string, c mem.CommitSet, pending []mem.Interrupt) mem.CommitSet {
	if len(c.Applied) == 0 {
		return c
	}
	var evs []proto.Event
	for _, in := range pending {
		if in.Kind == mem.InterruptSteer && in.From != "" && slices.Contains(c.Applied, in.Seq) {
			evs = append(evs, proto.NewEvent(proto.EvSteerDelivered, id, map[string]any{"seq": in.Seq, "from": in.From}))
		}
	}
	if len(evs) > 0 {
		c.Events = append(evs, c.Events...)
	}
	return c
}

// modelStep is execModel once the pending interrupts are read.
func (e *Env) modelStep(ctx context.Context, t *task.TCB, core int, pending []mem.Interrupt) (mem.CommitSet, []Send, error) {
	if reason, ok := e.stopping(t, pending); ok {
		// Cancelled, or out of time, before this instruction began: nothing
		// runs.
		c, err := e.cancelSet(ctx, t, mem.CommitSet{}, nil, reason)
		return c, nil, err
	}
	var c mem.CommitSet
	// Unconfirmed requests count too: they may have reached the provider, and
	// a task that keeps crashing mid-call must not re-send past its budget.
	if t.Budget.MaxSteps > 0 && t.Used.Steps+t.Unconfirmed >= t.Budget.MaxSteps {
		// Not an instruction: no model call happens, so no step event.
		if e.Check != nil && len(t.Regs) > 0 && t.Regs[len(t.Regs)-1].Feedback {
			// The answer it ends with is one the answer check rejected and
			// sent back (design §9.8): a request for another answer was lost
			// and spent the last step. Say so, as a final rejection would.
			return e.finishWith(t, c, "budget", map[string]any{"checked": true, "check_failed": true}), nil, nil
		}
		return e.finish(t, c, "budget"), nil, nil
	}
	g := e.groupOf(t.Class)
	if e.groupDown(g) {
		// The group is down: no request, no reservation. The task moves to
		// its group's fallback, or waits for group_up (or a half-open probe)
		// in the wait set.
		return e.groupGone(t, c, g), nil, nil
	}
	a := e.adapter(g)
	caps := capsOf(a)
	// A task cut off mid-answer (design §6.4) carries the text it had in
	// Partial, or, cut off while still thinking, its thinking in
	// PartialThink. With steers to deliver, the text becomes the turn the
	// steers interrupt, a whole assistant message ahead of them, and
	// thinking about the question before them is dropped; otherwise a model
	// that can continue the prefix is asked to, and one that cannot starts
	// afresh.
	partial, think := t.Partial, t.PartialThink
	steers := steerMessages(pending, e.frameSteer)
	var prefill *task.Message // what the request ends with for the model to continue
	extra := steers           // what the request carries after the registers
	switch {
	case partial == "" && think == "":
	case len(steers) > 0:
		t.PartialThink, think = "", ""
		if partial != "" {
			extra = append([]task.Message{task.Text(task.RoleAssistant, partial)}, steers...)
		}
	case partial != "" && caps.Prefill:
		m := task.Text(task.RoleAssistant, partial)
		prefill = &m
	case partial == "" && caps.ThinkPrefill:
		prefill = &task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartReasoning, Text: think}}}
	default:
		t.Partial, t.PartialThink, partial, think = "", "", "", ""
	}
	if prefill != nil {
		extra = []task.Message{*prefill}
	}
	switch d := e.windowFor(t, g, a, false, extra); {
	case d.overflow != "":
		// Not an instruction either: nothing is sent.
		return failWith(t, c, StopContextOverflow, d.overflow), nil, nil
	case d.compact:
		return e.execCompact(ctx, t, core, g, a, d)
	}
	t.CompactChunk, t.CompactRaise, t.CompactLimit = 0, 0, 0 // a pass that found nothing more to swap out is over

	base := len(t.Regs)
	withPrefill := func(msgs []task.Message) []task.Message {
		if prefill == nil {
			return msgs
		}
		return append(msgs[:len(msgs):len(msgs)], *prefill)
	}
	req := model.Request{System: t.System, Tools: t.Tools, Messages: withPrefill(forProvider(t.Regs, a.Provider())), MaxOut: e.sendOut(g, t), NoThink: t.NoThink}
	resp, c, thought, out := e.modelCall(ctx, t, core, g, a, task.StepModel, req, func(c *mem.CommitSet) model.Request {
		// Steers are applied after the Running commit, so a crash cannot
		// leave a steer in the registers that the interrupt table still
		// lists as pending. So is a Partial they interrupt: modelCall puts
		// it back if the call is not answered.
		if partial != "" && len(steers) > 0 {
			t.Regs = append(t.Regs, task.Text(task.RoleAssistant, partial))
			t.Partial = ""
		}
		c.Applied = applySteers(t, pending, e.frameSteer)
		req.Messages = withPrefill(forProvider(t.Regs, a.Provider()))
		if prefill != nil {
			amendStep(c, func(d *stepData) {
				if partial != "" {
					d.Prefill = runes(partial)
				} else {
					d.PrefillThink = runes(think)
				}
			})
		}
		return req
	})
	if out != nil {
		return out.c, out.batch, out.err
	}
	t.Retries = 0
	// Answered: the step's preemptions are over.
	t.Preempts = 0
	if truncated(resp) {
		// Cut off with nothing in it: judged on what this call produced, not
		// on the prefix it continued, which is not an answer by itself. The
		// redo sends the same turn, steers included; a Partial they
		// interrupted goes back with them, as does a Partial the call was
		// continuing. Thinking that ran into the cap is not continued: the
		// redo thinks afresh, or not at all.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		t.Partial, t.PartialThink = partial, ""
		if caps.ThinkOff && !t.NoThink && (thought || think != "" || thinkingOnly(resp.Message, resp.Usage.ReasoningTokens)) {
			// It spent its whole cap thinking (design §13): asked again with
			// thinking off, at the same cap, before any raise.
			return e.redoWithoutThinking(g, t, c), nil, nil
		}
		return e.redoTruncated(g, t, c), nil, nil
	}
	// A continued answer is joined onto what it continues: the text before
	// its text, the thinking before its reasoning.
	if prefill != nil && partial != "" {
		resp.Message = continued(partial, resp.Message)
	} else if prefill != nil {
		resp.Message = continuedThink(think, resp.Message)
	}
	// Answered, and not cut off: a cap raised for a truncated turn is
	// dropped, and the next call goes back to the task's own, thinking
	// included.
	t.Truncations = 0
	t.NoThink = false
	t.Partial, t.PartialThink = "", ""

	msg := resp.Message
	msg.Parts = slices.Clone(msg.Parts) // the adapter's slice is not ours to write
	msg.Role = task.RoleAssistant
	if len(msg.ToolCalls()) > 0 {
		recs := e.plan(t, &msg)
		for _, r := range recs {
			// Args go into the event as a string: a model can emit malformed
			// JSON, and marshalling it as raw JSON would fail.
			c.Events = append(c.Events, proto.NewEvent(proto.EvToolCall, t.ID,
				map[string]any{"call_id": r.CallID, "tool": r.Tool, "args": string(r.Args)}))
		}
		t.Regs = append(t.Regs, msg)
		// The tool policy decides each call here, before the commit that
		// dispatches it, and its decisions go into that commit (design §9.7).
		// It runs under the task's context, as the call did.
		tctx, release := e.taskContext(t.ID)
		c.Events = append(c.Events, e.decide(tctx, t, recs)...)
		release()
		batch := e.startRound(t, recs)
		c.Calls = recs
		c.Task = t
		// No re-read of the interrupts here: a cancel committed since then
		// makes the store refuse this commit, and the step finalises.
		return c, sends(batch), nil
	}

	t.Regs = append(t.Regs, msg)
	if out := e.arrived(ctx, t, c, base, resp); out != nil {
		return out.c, out.batch, out.err
	}
	if e.Check == nil {
		return e.finish(t, c, "end_turn"), nil, nil
	}
	// The answer check sees the answer before the commit that finishes the
	// task (design §9.8); its verdict exists only through that commit. What
	// arrived while it ran counts as if it had arrived during the call: a
	// cancel ends the task and a steer makes the answer a turn, and the
	// verdict is dropped either way.
	began := e.Clock.Now()
	v := e.checkAnswer(t, msg.TextContent())
	checkTook(&c, e.Clock.Now().Sub(began))
	if out := e.arrived(ctx, t, c, base, resp); out != nil {
		return out.c, out.batch, out.err
	}
	return e.answerChecked(t, c, v), nil, nil
}

// checkTook notes on the step event in c how long the answer check held the
// step, so that an observer can tell the provider's latency from it.
func checkTook(c *mem.CommitSet, d time.Duration) {
	amendStep(c, func(s *stepData) { s.CheckMS = max(d.Milliseconds(), 0) })
}

// arrived ends a model step whose final answer is in hand (the last of t's
// registers, appended after base) when outside input arrived for t during
// it; nil lets the answer stand. A cancel ends the task, the answer
// discarded. A steer turns the answer into a turn: the task goes on instead
// of finishing over it. The steer stays pending: the next model step
// applies it at its start and sends it, or that step's terminal commit
// reports it dropped.
func (e *Env) arrived(ctx context.Context, t *task.TCB, c mem.CommitSet, base int, resp model.Response) *stepOut {
	late, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return &stepOut{c: c, err: fmt.Errorf("read interrupts: %w", err)}
	}
	if _, ok := cancelReason(late); ok {
		cs, batch, err := e.cancelledStep(ctx, t, c, base, resp, nil, true, false)
		return &stepOut{cs, batch, err}
	}
	if late = withoutApplied(late, c.Applied); hasSteer(late) {
		e.ready(t, task.StepModel)
		c.Task = t
		return &stepOut{c: c}
	}
	return nil
}

// stepOut is how a model call's instruction ends when the call did not
// come back with an answer to use: the commit to make (or the error that
// stops the step), and the batch to dispatch after it.
type stepOut struct {
	c     mem.CommitSet
	batch []Send
	err   error
}

// modelCall is one request to t's group, for a model step or a compaction
// alike (pc): reserve room in the group's limiters, be admitted by the
// group, commit Running (with pc as the task's PC), send the request, and
// settle its outcome with the limiters and the group. apply runs after the
// Running commit and before the call, and returns the request to send (a
// model step applies its steers there); nil sends req as it is.
//
// It returns the response and the commit set so far (the step and usage
// events, the steers applied) when the call was answered, and whether the
// call streamed any reasoning (a backend that streams its thinking may
// report it nowhere else). Otherwise it
// returns how the instruction ends: not started, cancelled, waiting for
// the group or a backoff, failed, or compacting after a context-length
// rejection.
func (e *Env) modelCall(ctx context.Context, t *task.TCB, core int, g *Group, a model.Adapter, pc task.PC, req model.Request,
	apply func(c *mem.CommitSet) model.Request) (model.Response, mem.CommitSet, bool, *stepOut) {
	var c mem.CommitSet
	base := len(t.Regs)
	// apply may move the task's Partial into its registers (a preempted
	// answer interrupted by steers, design §6.4); a call that is not answered
	// puts it back, as it takes the steers back.
	partial0 := t.Partial
	end := func(c mem.CommitSet, err error) (model.Response, mem.CommitSet, bool, *stepOut) {
		return model.Response{}, mem.CommitSet{}, false, &stepOut{c: c, err: err}
	}

	// The call runs under the task's own context: a cancel or deadline ends
	// it at once, whoever holds the task. So does the wait for room in the
	// limiters before it.
	tctx, release := e.taskContext(t.ID)
	defer release()
	// The wait for room also ends when the machine stops fetching: nothing
	// is committed yet, so the task goes back as it is.
	rctx, stopWaiting := context.WithCancel(tctx)
	defer stopWaiting()
	unhook := context.AfterFunc(e.Sched.Stopping(), stopWaiting)
	out := req.MaxOut
	if out == 0 {
		out = e.maxOut(t)
	}
	res, pre, err := e.reserve(rctx, g, t.ID, req, out)
	unhook()
	if err != nil {
		switch {
		case errors.Is(err, limit.ErrTooLarge):
			return end(failWith(t, c, StopTooLarge, err.Error()), nil)
		case e.halt().Err() != nil:
			return end(mem.CommitSet{}, nil) // the step's halt check drops the task
		case tctx.Err() == nil && e.Sched.Stopping().Err() != nil:
			return end(mem.CommitSet{}, errNotStarted)
		case tctx.Err() != nil:
			// Cancelled, or out of time, while waiting for room: nothing ran.
			return end(e.cancelSet(ctx, t, mem.CommitSet{}, nil, ReasonCancelled))
		}
		return end(failWith(t, c, StopModelError, "reserve: "+err.Error()), nil)
	}
	var ticket *group.Ticket
	probe := false // the half-open breaker's probe: never preempted
	if g != nil {
		tk, ok, isProbe := g.Ctl.Admit()
		probe = isProbe
		if !ok {
			// Went down while this task waited for room (or another task took
			// the probe): the room goes back, and the task moves or waits for
			// the group. A group with no limiters has no room to give back.
			if res != nil {
				res.Cancel()
			}
			return end(e.groupGone(t, c, g, pre...), nil)
		}
		ticket = &tk
	}
	// A request admitted and then not sent (cancelled, the start not
	// committed) leaves its ticket unreported; a probe lost this way is
	// replaced by the breaker after its open time.
	unsent := func() {
		if res != nil {
			res.Cancel()
		}
	}

	// Committed before the call: a restart that finds the task Running knows
	// a request may have reached the provider, and redoes the instruction
	// its PC names.
	// Running means "a request may have been sent": a halt that lands after
	// this commit and before the call leaves it Running though nothing went
	// out, and recovery then counts one request too many as unconfirmed.
	prevPC := t.PC
	t.State, t.PC = task.Running, pc
	t.Version++
	call := proto.NewEvent(proto.EvModelCall, t.ID, stepData{PC: pc.String(), Core: core})
	if err := e.Commit(context.WithoutCancel(ctx), mem.CommitSet{Task: t, Events: append(pre, call)}); err != nil {
		t.State, t.PC = task.Ready, prevPC
		t.Version--
		unsent()
		if errors.Is(err, mem.ErrCancelPending) {
			// Cancelled after the interrupts were read: nothing runs.
			return end(e.cancelSet(ctx, t, mem.CommitSet{}, nil, ReasonCancelled))
		}
		return end(mem.CommitSet{}, fmt.Errorf("%w: %v", errStartNotCommitted, err))
	}
	e.startCommitted(t.ID)
	if e.halt().Err() != nil {
		unsent()
		return end(mem.CommitSet{}, nil) // the step's halt check drops the task
	}
	if e.Sched.Cancelled(t.ID) {
		// Signalled since the start commit: no request goes out.
		unsent()
		cs, batch, err := e.cancelledStep(ctx, t, c, base, model.Response{}, nil, false, false)
		return model.Response{}, mem.CommitSet{}, false, &stepOut{cs, batch, err}
	}
	c.Events = append(c.Events, proto.NewEvent(proto.EvStep, t.ID, stepData{PC: pc.String(), Core: core}))
	if apply != nil {
		req = apply(&c)
	}

	// The text the call streams is kept as it comes: a call cut off by a
	// preemption, or whose stream breaks off, leaves it for the next call to
	// continue (design §6.4). A group that preempts streams every model call
	// its adapter can, whether or not anyone watches the deltas.
	var (
		emit     func(model.Delta)
		got      strings.Builder // text
		gotThink strings.Builder // reasoning
		first    time.Time
	)
	force := g != nil && g.Preempt
	if pc == task.StepModel {
		// A compaction's summary is not an answer: it is not streamed.
		stepNo := t.Used.Steps + 1
		emit = func(d model.Delta) {
			if first.IsZero() {
				first = e.Clock.Now()
			}
			got.WriteString(d.Text)
			gotThink.WriteString(d.Reasoning)
			if e.Delta != nil {
				e.Delta(proto.Delta{TaskID: t.ID, Step: stepNo, Text: d.Text, Reasoning: d.Reasoning})
			}
		}
	}
	callCtx, fl := tctx, (*flight)(nil)
	if pc == task.StepModel {
		callCtx, fl = e.fly(tctx, g, t, core, probe)
	}
	sent := e.Clock.Now()
	resp, err := e.callModel(callCtx, a, req, emit, force)
	by, ran, preempted := e.land(g, fl, callCtx)
	// Cut off by a preemption, and not by its task's cancel or the halt:
	// the machine ended the call itself, and knows how it ended.
	preempted = preempted && err != nil && tctx.Err() == nil
	if !first.IsZero() {
		amendStep(&c, func(d *stepData) { ms := max(first.Sub(sent).Milliseconds(), 0); d.FirstMS = &ms })
	}
	// What a call that was not answered produced is kept (keepPartial) only
	// when the call carried no steers: one that did was a new turn
	// answering them, and goes back with them.
	keep, keepThink := got.String(), gotThink.String()
	if len(c.Applied) > 0 || t.Partial != partial0 {
		keep, keepThink = "", ""
	}
	thought := gotThink.Len() > 0
	class := model.Unknown
	var p *panicError
	panicked := errors.As(err, &p)
	lost := err != nil && errors.Is(err, model.ErrResponseLost) && !preempted
	// The adapter's hooks run under the same guard as the call: a panic in
	// any of them is the adapter panicking, and fails the task (the call's
	// response, if it had one, is not used).
	hookPanicked := func(perr error) bool {
		if perr == nil {
			return false
		}
		err, panicked, class = perr, true, model.Unknown
		return true
	}
	if preempted && !panicked {
		// Settled as a call interrupted by a cancel, and not reported to the
		// group: it says nothing about the provider.
		class = model.Canceled
	} else if err != nil && !panicked && !hookPanicked(guarded("Classify", func() { class = model.ClassifyWith(a, err) })) {
		if class == model.Canceled && tctx.Err() == nil {
			// The task was not cancelled: the adapter gave up on its own (a
			// timeout it reported as a bare deadline). The provider was slow.
			class = model.Transient
		}
		if class == model.Unknown {
			class = model.Transient
		}
	}
	var info *model.RateInfo
	if !panicked {
		hookPanicked(guarded("RateInfo", func() { info = rateInfo(a, resp, err) }))
	}
	how := refundOf(err, class)
	if panicked {
		how = cancelAll
	}
	hookPanicked(e.settleCall(g, res, how, req, resp, info))
	if class == model.RateLimited {
		if at, ok := e.retryAt(err, info); ok {
			pause(g, at)
		}
	}
	if ticket != nil && !panicked && class != model.Canceled {
		o := group.Succeeded
		if err != nil {
			o = outcomeOf(class)
		}
		e.report(ctx, g, *ticket, o)
	}
	if err == nil {
		// Used.Steps counts answered calls only (design §4.6): a failed call
		// is bounded by the retry limit, a lost one is unconfirmed, and
		// neither spends the task's step budget. A compaction is a call like
		// any other.
		t.Used.Steps++
		t.Used.Add(resp.Usage)
		e.windowAnswered(t, a, req)
		c.Events = append(c.Events, proto.NewEvent(proto.EvUsage, t.ID, resp.Usage))
	}
	if lost {
		// The request reached the provider and its answer never came whole:
		// it may have been done and billed (design §4.6, §18 test 12). A
		// request that never got out (a refused connection) is not counted.
		t.Unconfirmed++
	}
	if e.halt().Err() == nil && e.Sched.Cancelled(t.ID) {
		kept := resp
		if pc != task.StepModel {
			kept = model.Response{} // a summary is never kept
		}
		t.Partial = partial0
		cs, batch, cerr := e.cancelledStep(ctx, t, c, base, kept, err, true, err != nil && !lost && tctx.Err() != nil)
		return model.Response{}, mem.CommitSet{}, false, &stepOut{cs, batch, cerr}
	}
	if err != nil {
		// Steers applied for this call were never answered: take them back.
		// They stay pending: the retry applies them again, and a terminal
		// commit reports them dropped.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		t.Partial = partial0
		if preempted && !panicked {
			return end(e.preempted(t, c, by, ran, keep, keepThink, capsOf(a)), nil)
		}
		if lost && !panicked && pc == task.StepModel {
			// The stream broke off: what came is continued by the retry
			// (design §6.4), committed with the backoff.
			keepPartial(t, keep, keepThink, capsOf(a))
		}
		if class == model.ContextLength && !panicked {
			return end(e.contextRejected(t, c, g, a, req, err), nil)
		}
		return end(e.modelFailed(t, c, class, err, info, panicked), nil)
	}
	return resp, c, thought, nil
}

// plan gives each tool call in msg its id and returns their planned call
// records, with what recovery needs to know about each tool.
func (e *Env) plan(t *task.TCB, msg *task.Message) []mem.CallRecord {
	var recs []mem.CallRecord
	for i := range msg.Parts {
		p := &msg.Parts[i]
		if p.Kind != task.PartToolCall {
			continue
		}
		t.CallSeq++
		p.CallID = fmt.Sprintf("c-%d", t.CallSeq)
		device, effect, report := e.Bus.Recovery(p.Name)
		recs = append(recs, mem.CallRecord{
			TaskID: t.ID, CallID: p.CallID, Index: t.CallSeq, Tool: p.Name, Args: p.Args, Status: mem.CallPlanned,
			Device: device, Effect: effect, ReportUnknown: report,
		})
	}
	return recs
}

// cancelledStep is the final commit of a model step whose task was cancelled
// around its model call. The steers applied for the call go back to pending,
// so the final commit reports them dropped. A call cut off by the cancel
// (interrupted) may have reached the provider: it is unconfirmed; with no
// call made (called false) nothing is. A response that did come back has
// already counted its usage; its tool calls are recorded cancelled and never
// dispatched; an answer is discarded.
func (e *Env) cancelledStep(ctx context.Context, t *task.TCB, c mem.CommitSet, base int, resp model.Response, err error, called, interrupted bool) (mem.CommitSet, []Send, error) {
	t.Regs = t.Regs[:base]
	c.Applied = nil
	switch {
	case !called:
	case err != nil:
		if interrupted {
			t.Unconfirmed++
		}
	default:
		msg := resp.Message
		msg.Parts = slices.Clone(msg.Parts)
		msg.Role = task.RoleAssistant
		if len(msg.ToolCalls()) > 0 {
			for _, r := range e.plan(t, &msg) {
				r.Status = mem.CallCancelled
				c.Calls = append(c.Calls, r)
			}
			t.Regs = append(t.Regs, msg)
		}
	}
	c, cerr := e.cancelSet(ctx, t, c, nil, ReasonCancelled)
	return c, nil, cerr
}

// errNotStarted marks a step given up before it changed anything: the
// machine stopped while it waited for room in a limiter. The task goes back
// to the scheduler as it was.
var errNotStarted = errors.New("step not started: the machine is stopping")

// errStartNotCommitted marks a step whose start could not be recorded.
// Nothing was sent and the task is unchanged, so the step can be tried again.
var errStartNotCommitted = errors.New("start of step not committed")

// panicError is a model adapter's panic, recovered into a failure of its
// task. in names the hook it happened in; empty for the call itself.
type panicError struct {
	v  any
	in string
}

func (p *panicError) Error() string {
	if p.in != "" {
		return fmt.Sprintf("model panicked in %s: %v", p.in, p.v)
	}
	return fmt.Sprintf("model panicked: %v", p.v)
}

// guarded runs one of the adapter's hooks (Classify, RateInfo, Reserve,
// Settle), turning a panic in it into a *panicError.
func guarded(in string, f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{v: r, in: in}
		}
	}()
	f()
	return nil
}

// callModel calls the adapter, streaming through emit when the adapter can,
// emit is not nil, and either a Delta sink is set or force (a group that
// preempts keeps what a call streams); a panic in it fails the task, not the
// process.
func (e *Env) callModel(ctx context.Context, a model.Adapter, req model.Request, emit func(model.Delta), force bool) (resp model.Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp, err = model.Response{}, &panicError{v: r}
		}
	}()
	if s, ok := a.(model.Streamer); ok && (e.Delta != nil || force) && emit != nil {
		return s.Stream(ctx, req, emit)
	}
	return a.Call(ctx, req)
}

// steerMessages are the user messages applySteers would add for pending.
func steerMessages(pending []mem.Interrupt, frame func(mem.Interrupt) string) []task.Message {
	var out []task.Message
	for _, in := range pending {
		if in.Kind == mem.InterruptSteer {
			out = append(out, task.Text(task.RoleUser, frame(in)))
		}
	}
	return out
}

// applySteers appends each pending steer as a user message, rendered by
// frame, and returns the interrupt sequence numbers it consumed. Other kinds
// stay pending.
func applySteers(t *task.TCB, pending []mem.Interrupt, frame func(mem.Interrupt) string) []int64 {
	var seqs []int64
	for _, in := range pending {
		if in.Kind != mem.InterruptSteer {
			continue
		}
		t.Regs = append(t.Regs, task.Text(task.RoleUser, frame(in)))
		seqs = append(seqs, in.Seq)
	}
	return seqs
}

func withoutApplied(list []mem.Interrupt, applied []int64) []mem.Interrupt {
	var out []mem.Interrupt
	for _, in := range list {
		if !slices.Contains(applied, in.Seq) {
			out = append(out, in)
		}
	}
	return out
}

func hasSteer(list []mem.Interrupt) bool {
	return slices.ContainsFunc(list, func(in mem.Interrupt) bool { return in.Kind == mem.InterruptSteer })
}
