package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
func (e *Env) execModel(ctx context.Context, t *task.TCB, core int) (mem.CommitSet, []Send, error) {
	pending, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return mem.CommitSet{}, nil, fmt.Errorf("read interrupts: %w", err)
	}
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
	switch d := e.windowFor(t, g, a, false, steerMessages(pending, e.frameSteer)); {
	case d.overflow != "":
		// Not an instruction either: nothing is sent.
		return failWith(t, c, StopContextOverflow, d.overflow), nil, nil
	case d.compact:
		return e.execCompact(ctx, t, core, g, a, d)
	}
	t.CompactChunk, t.CompactRaise, t.CompactLimit = 0, 0, 0 // a pass that found nothing more to swap out is over

	base := len(t.Regs)
	req := model.Request{System: t.System, Tools: t.Tools, Messages: forProvider(t.Regs, a.Provider()), MaxOut: e.sendOut(g, t)}
	resp, c, out := e.modelCall(ctx, t, core, g, a, task.StepModel, req, func(c *mem.CommitSet) model.Request {
		// Steers are applied after the Running commit, so a crash cannot
		// leave a steer in the registers that the interrupt table still
		// lists as pending.
		c.Applied = applySteers(t, pending, e.frameSteer)
		req.Messages = forProvider(t.Regs, a.Provider())
		return req
	})
	if out != nil {
		return out.c, out.batch, out.err
	}
	t.Retries = 0
	if truncated(resp) {
		// Nothing came back to keep: the redo sends the same turn, steers
		// included, with a larger output cap.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		return e.redoTruncated(g, t, c), nil, nil
	}
	// Answered, and not cut off: a cap raised for a truncated turn is
	// dropped, and the next call goes back to the task's own.
	t.Truncations = 0

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
	// A steer committed while the model was answering turns this answer into
	// a turn: the task goes on instead of finishing over it. The steer stays
	// pending: the next model step applies it at its start and sends it, or
	// that step's terminal commit reports it dropped.
	late, err := e.Store.Interrupts(ctx, t.ID, true)
	if err != nil {
		return c, nil, fmt.Errorf("read interrupts: %w", err)
	}
	if _, ok := cancelReason(late); ok {
		return e.cancelledStep(ctx, t, c, base, resp, nil, true, false)
	}
	if late = withoutApplied(late, c.Applied); hasSteer(late) {
		e.ready(t, task.StepModel)
		c.Task = t
		return c, nil, nil
	}
	return e.finish(t, c, "end_turn"), nil, nil
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
// events, the steers applied) when the call was answered. Otherwise it
// returns how the instruction ends: not started, cancelled, waiting for
// the group or a backoff, failed, or compacting after a context-length
// rejection.
func (e *Env) modelCall(ctx context.Context, t *task.TCB, core int, g *Group, a model.Adapter, pc task.PC, req model.Request,
	apply func(c *mem.CommitSet) model.Request) (model.Response, mem.CommitSet, *stepOut) {
	var c mem.CommitSet
	base := len(t.Regs)
	end := func(c mem.CommitSet, err error) (model.Response, mem.CommitSet, *stepOut) {
		return model.Response{}, mem.CommitSet{}, &stepOut{c: c, err: err}
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
	if g != nil {
		tk, ok, _ := g.Ctl.Admit()
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
		return model.Response{}, mem.CommitSet{}, &stepOut{cs, batch, err}
	}
	c.Events = append(c.Events, proto.NewEvent(proto.EvStep, t.ID, stepData{PC: pc.String(), Core: core}))
	if apply != nil {
		req = apply(&c)
	}

	var emit func(model.Delta)
	if pc == task.StepModel {
		// A compaction's summary is not an answer: it is not streamed.
		stepNo := t.Used.Steps + 1
		emit = func(d model.Delta) {
			e.Delta(proto.Delta{TaskID: t.ID, Step: stepNo, Text: d.Text, Reasoning: d.Reasoning})
		}
	}
	resp, err := e.callModel(tctx, a, req, emit)
	class := model.Unknown
	var p *panicError
	panicked := errors.As(err, &p)
	lost := err != nil && errors.Is(err, model.ErrResponseLost)
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
	if err != nil && !panicked && !hookPanicked(guarded("Classify", func() { class = model.ClassifyWith(a, err) })) {
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
		cs, batch, cerr := e.cancelledStep(ctx, t, c, base, kept, err, true, err != nil && !lost && tctx.Err() != nil)
		return model.Response{}, mem.CommitSet{}, &stepOut{cs, batch, cerr}
	}
	if err != nil {
		// Steers applied for this call were never answered: take them back.
		// They stay pending: the retry applies them again, and a terminal
		// commit reports them dropped.
		t.Regs = t.Regs[:base]
		c.Applied = nil
		if class == model.ContextLength && !panicked {
			return end(e.contextRejected(t, c, g, a, req, err), nil)
		}
		return end(e.modelFailed(t, c, class, err, info, panicked), nil)
	}
	return resp, c, nil
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
// a Delta sink is set and emit is not nil; a panic in it fails the task, not the process.
func (e *Env) callModel(ctx context.Context, a model.Adapter, req model.Request, emit func(model.Delta)) (resp model.Response, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp, err = model.Response{}, &panicError{v: r}
		}
	}()
	if s, ok := a.(model.Streamer); ok && e.Delta != nil && emit != nil {
		return s.Stream(ctx, req, emit)
	}
	return a.Call(ctx, req)
}

// steerMessages are the user messages applySteers would add for pending.
func steerMessages(pending []mem.Interrupt, frame func(string) string) []task.Message {
	var out []task.Message
	for _, in := range pending {
		if in.Kind == mem.InterruptSteer {
			out = append(out, task.Text(task.RoleUser, frame(in.Text)))
		}
	}
	return out
}

// applySteers appends each pending steer as a user message, rendered by
// frame, and returns the interrupt sequence numbers it consumed. Other kinds
// stay pending.
func applySteers(t *task.TCB, pending []mem.Interrupt, frame func(string) string) []int64 {
	var seqs []int64
	for _, in := range pending {
		if in.Kind != mem.InterruptSteer {
			continue
		}
		t.Regs = append(t.Regs, task.Text(task.RoleUser, frame(in.Text)))
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
