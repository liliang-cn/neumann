package observe

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// data is the union of the fields observe reads from events' Data. Fields
// that carry task content (tool arguments, answers, steer text) are left
// out on purpose: nothing observe exports can leak them.
type data struct {
	Class      string `json:"class"`
	Priority   int    `json:"priority"`
	StopReason string `json:"stop_reason"`
	Error      string `json:"error"`
	PC         string `json:"pc"`
	Core       int    `json:"core"`
	CallID     string `json:"call_id"`
	Tool       string `json:"tool"`
	IsError    bool   `json:"is_error"`
	Cached     bool   `json:"cached"`
	Resolved   bool   `json:"resolved"`
	Unknown    bool   `json:"unknown"`
	Cancelled  bool   `json:"cancelled"`
	Retry      bool   `json:"retry"`
	Task       string `json:"task"`
	WaitedMS   int64  `json:"waited_ms"`
	Cause      string `json:"cause"`
	Reason     string `json:"reason"`
	Live       int    `json:"live"`
	From       string `json:"from"`
	To         string `json:"to"`
	Check      string `json:"check"`
	Attempt    int    `json:"attempt"`
	DelayMS    int64  `json:"delay_ms"`
	Wait       string `json:"wait"`
	Reset      bool   `json:"reset"`
	MaxOut     int    `json:"max_out"`
	By         string `json:"by"`        // tool_denied: policy or operator
	Device     string `json:"device"`    // approval_needed
	Effect     string `json:"effect"`    // approval_needed
	Recovered  bool   `json:"recovered"` // approval_needed announced again by a restart
	Edited     bool   `json:"edited"`    // approved with new arguments
	// the answer check (design §9.8): answer_rejected, and done
	CheckMS     int64  `json:"check_ms"`     // step: how long the answer check held it after the call answered
	Final       bool   `json:"final"`        // answer_rejected: no retry was left
	Checked     bool   `json:"checked"`      // done: the answer check saw the answer
	CheckFailed bool   `json:"check_failed"` // done: and rejected it, past the retries
	CheckError  string `json:"check_error"`  // done: and gave no verdict (timed out, panicked)
	// compacted
	Rounds         int   `json:"rounds"`
	MessagesBefore int   `json:"messages_before"`
	MessagesAfter  int   `json:"messages_after"`
	TokensBefore   int64 `json:"tokens_before_est"`
	TokensAfter    int64 `json:"tokens_after_est"`
	Chunk          int   `json:"chunk"`
	Chunks         int   `json:"chunks"`
	// preemption and continuation (design §6.4)
	FirstMS      *int64 `json:"first_ms"`      // step: ms from the request to its first streamed chunk; nil when none came
	Prefill      int    `json:"prefill"`       // step: chars of the continued prefix
	PrefillThink int    `json:"prefill_think"` // step: chars of the continued thought
	Kept         int    `json:"kept"`          // preempted, suspended: chars kept for the continuation
	KeptThink    int    `json:"kept_think"`    // preempted, suspended: chars of thinking kept for the continuation
	RanMS        int64  `json:"ran_ms"`        // preempted: how long the call ran
	Dropped      int    `json:"dropped"`       // model_retry: chars of the prefix a failed continuation dropped
	DroppedThink int    `json:"dropped_think"` // model_retry: chars of the thought a failed continuation dropped
	N            int    `json:"n"`             // preempted: how many times this step was preempted
	Was          int    `json:"was"`           // priority_set: the priority before
	SteerSeq     int64  `json:"seq"`           // steer_delivered, steer_dropped: the steer's seq
	// suspend and resume (design §9.9)
	WasDoing string `json:"-"`       // suspended: what the task was doing (its "was", a string there)
	Pending  bool   `json:"pending"` // resumed: the resume cancelled a suspend that had not landed
	task.Usage
}

func decode(ev proto.Event) data {
	var d data
	if len(ev.Data) > 0 {
		// A field of an unexpected type is skipped; the rest still decode.
		_ = json.Unmarshal(ev.Data, &d)
		if ev.Kind == proto.EvSuspended {
			// suspended's was names a state; priority_set's is a number.
			var s struct {
				Was string `json:"was"`
			}
			_ = json.Unmarshal(ev.Data, &s)
			d.WasDoing = s.Was
		}
	}
	return d
}

// Outcomes of instructions, tool calls and tasks, as metric labels and span
// attributes.
const (
	outOK        = "ok"
	outError     = "error"
	outCancelled = "cancelled"
	outCached    = "cached"
	outLate      = "late"
	outUnknown   = "unknown"
	outDenied    = "denied"
	outResolved  = "resolved"
	// A model call cut off for a more urgent task (design §6.4): not an
	// error, the task runs it again and continues the kept text.
	outPreempted = "preempted"
	// A model call cut off by an operator's suspend (design §9.9): not an
	// error either; after the resume the task runs it again, continuing the
	// kept text.
	outSuspended = "suspended"
	// A suspension that ended with a resume (the others end with their
	// task: cancelled, failed).
	outResumed = "resumed"
)

// Verdicts of a call held for approval (design §9.7), as metric labels and
// span attributes: what an operator answered, or cancelled when its task
// ended first.
const (
	verdictAllow = "allow"
	verdictEdit  = "edit"
	verdictDeny  = "deny"
)

type factKind int

const (
	factTaskStart      factKind = iota // accepted, or the first event of a task seen mid-way
	factTaskEnd                        // done, failed, cancelled
	factStep                           // an instruction (model, compact, tools) finished
	factToolEnd                        // a tool call ended: a result, an unknown outcome, a late result, a cancel
	factBatchEnd                       // every call of a tool round ended
	factMark                           // anything else worth noting on the task (span event)
	factAfter                          // an event committed after its task ended (steer_dropped, tool_late)
	factApprovalNeeded                 // a call was held for an operator, announced for the first time
	factApprovalEnd                    // a held call was decided, or its task ended first (outcome: the verdict)
	factSuspendEnd                     // a suspension ended: resumed, or its task ended first (outcome: resumed or the task's)
)

// fact is what one event means once the events before it are known: the
// unit metrics, traces and the log are built from.
type fact struct {
	kind  factKind
	ev    proto.Event
	d     data
	task  string
	class string // the task's execution group (or a machine event's)

	seq          int64 // the event that opened the span: step, tool_call, first tool_call of a batch
	start, end   time.Time
	name         string // step pc; tool name; terminal kind
	outcome      string
	errClass     string // a failed step's error class (model_retry class or the task's stop reason)
	usage        *task.Usage
	core         int
	partial      bool           // the task's start was not seen
	timed        bool           // tool end: start is when the call was made
	batch        int64          // tool end: the Seq of its round (the round's first tool_call)
	total        task.Usage     // task end: the task's confirmed usage
	steps        int            // task end: answered model calls
	priority     *int           // task end: the task's last priority, nil when unknown (a partial task)
	firstByte    *time.Duration // step: from the request to its first streamed chunk
	prefill      int            // step: chars of the continued prefix
	prefillThink int            // step: chars of the continued thought
	kept         int            // suspension: chars of text kept from the call it interrupted
	keptThink    int            // suspension: chars of thinking kept
}

type openStep struct {
	seq          int64
	pc           string
	core         int
	at, start    time.Time
	usage        *task.Usage
	check        time.Duration // the answer check's share of the step, after the call answered
	firstByte    *time.Duration
	prefill      int
	prefillThink int
}

type openCall struct {
	seq   int64
	tool  string
	at    time.Time
	batch *openBatch
}

// openApproval is a call held for an operator, from its first announcement.
type openApproval struct {
	seq  int64 // the approval_needed that announced it first
	tool string
	at   time.Time
}

// openSuspend is a task an operator suspended (design §9.9), from the
// suspended event on.
type openSuspend struct {
	seq             int64
	at              time.Time
	was             string
	kept, keptThink int
}

type openBatch struct {
	seq     int64
	at      time.Time
	pending int
	last    time.Time
}

type taskState struct {
	class    string
	priority *int // nil until accepted or priority_set says
	partial  bool
	start    time.Time
	// ready is when the task could next run an instruction: its last
	// commit, or the end of a backoff. A model step that follows spans from
	// here, unless a throttled event says when its call really began (call).
	ready, call time.Time
	step        *openStep
	open        map[string]*openCall // calls without an outcome yet
	held        map[string]*openApproval
	calls       map[string]*openCall // every call seen, for late results
	batch       *openBatch
	suspend     *openSuspend // suspended and not yet resumed
	usage       task.Usage
	steps       int
	ended       bool
}

// endedTasks remembers the calls of recently finished tasks, so that a late
// result (committed after its task ended) still knows its tool and start.
const endedTasks = 256

// interp folds the event stream into facts. It is not safe for concurrent
// use: one Follow goroutine feeds it.
type interp struct {
	tasks map[string]*taskState
	ended map[string]*taskState
	order []string // ended, oldest first
	// lookup gives the class of a task first seen mid-way; nil: unknown.
	lookup func(id string) string
	// suspended counts the tasks with an open suspension: suspended, and
	// neither resumed nor ended since.
	suspended int
}

func newInterp(lookup func(id string) string) *interp {
	return &interp{tasks: map[string]*taskState{}, ended: map[string]*taskState{}, lookup: lookup}
}

// feed interprets ev and calls emit for each fact, in order. It returns the
// class the event belongs to, for observers that label raw events, and the
// event's decoded data.
func (in *interp) feed(ev proto.Event, emit func(fact)) (string, data) {
	d := decode(ev)
	if ev.TaskID == "" {
		return in.machine(ev, d, emit), d
	}
	if t := in.ended[ev.TaskID]; t != nil {
		in.after(ev, d, t, emit)
		return t.class, d
	}
	t := in.tasks[ev.TaskID]
	f := func(k factKind) fact {
		return fact{kind: k, ev: ev, d: d, task: ev.TaskID, class: t.class}
	}
	if t == nil {
		t = &taskState{start: ev.Time, ready: ev.Time, open: map[string]*openCall{}, calls: map[string]*openCall{}, held: map[string]*openApproval{}}
		in.tasks[ev.TaskID] = t
		if ev.Kind == proto.EvAccepted {
			t.class, t.priority = d.Class, &d.Priority
		} else {
			t.partial = true
			if in.lookup != nil {
				t.class = in.lookup(ev.TaskID)
			}
		}
		s := f(factTaskStart)
		s.start, s.partial = t.start, t.partial
		emit(s)
	}

	// The step a commit opened ends with the first event after it (in the
	// same commit, as the core commits a step with its result). A resume
	// that cancelled a suspend that had not landed rides on the commit of
	// the instruction it let go on, which it neither ends nor times.
	pendingResume := ev.Kind == proto.EvResumed && d.Pending
	if t.step != nil && ev.Kind != proto.EvUsage && ev.Kind != proto.EvStep && !pendingResume {
		in.endStep(ev, d, t, emit)
	}
	switch ev.Kind {
	case proto.EvStep:
		if t.step != nil {
			in.endStep(ev, d, t, emit)
		}
		if t.batch != nil && t.batch.pending > 0 {
			in.endBatch(ev, t, ev.Time, emit) // a new round: whatever is left of the old one is over
		}
		start := t.ready
		if !t.call.IsZero() {
			start = t.call
		}
		t.step = &openStep{seq: ev.Seq, pc: d.PC, core: d.Core, at: ev.Time, start: start, check: time.Duration(d.CheckMS) * time.Millisecond, prefill: d.Prefill, prefillThink: d.PrefillThink}
		if d.FirstMS != nil {
			fb := time.Duration(*d.FirstMS) * time.Millisecond
			t.step.firstByte = &fb
		}
		t.call = time.Time{}
		if d.PC == task.StepTools.String() {
			// Dispatching planned calls: the instruction is the commit.
			in.endStep(ev, d, t, emit)
		}
	case proto.EvUsage:
		u := d.Usage
		t.usage.Add(u)
		if t.step != nil {
			t.step.usage = &u
			t.steps++
			if t.step.pc != task.StepCompact.String() {
				in.endStep(ev, d, t, emit)
			}
			// An answered compaction ends with the event after its usage,
			// in the same commit: compacted, whose figures its fact carries
			// (or a retry or the task's end, when it was not applied).
		}
	case proto.EvToolCall:
		if t.batch == nil {
			t.batch = &openBatch{seq: ev.Seq, at: ev.Time}
		}
		c := &openCall{seq: ev.Seq, tool: d.Tool, at: ev.Time, batch: t.batch}
		t.batch.pending++
		t.open[d.CallID], t.calls[d.CallID] = c, c
	case proto.EvToolResult:
		out := outOK
		switch {
		case d.Unknown:
			out = outUnknown
		case d.Resolved:
			out = outResolved
		case d.IsError:
			out = outError
		case d.Cached:
			out = outCached
		}
		in.endCall(ev, d, t, d.CallID, out, emit)
	case proto.EvReconcileNeeded:
		emit(f(factMark))
		in.endCall(ev, d, t, d.CallID, outUnknown, emit)
	case proto.EvToolDenied:
		// Refused before it ran, by the policy or by an operator: its
		// result, as far as the model is concerned.
		emit(f(factMark))
		if d.By == "operator" {
			in.endApproval(ev, d, t, d.CallID, verdictDeny, emit)
		}
		in.endCall(ev, d, t, d.CallID, outDenied, emit)
	case proto.EvApprovalNeeded:
		emit(f(factMark))
		if t.held[d.CallID] == nil {
			// A restart announces a held call again: it is still the same
			// wait, counted once and timed from its first announcement.
			t.held[d.CallID] = &openApproval{seq: ev.Seq, tool: d.Tool, at: ev.Time}
			n := f(factApprovalNeeded)
			n.name, n.seq, n.start = d.Tool, ev.Seq, ev.Time
			emit(n)
		}
	case proto.EvApproved:
		emit(f(factMark))
		verdict := verdictAllow
		if d.Edited {
			verdict = verdictEdit
		}
		in.endApproval(ev, d, t, d.CallID, verdict, emit)
	case proto.EvModelRetry, proto.EvPreempted:
		// A preempted call's step ended above, outcome preempted; the task
		// is Ready again from this commit on.
		emit(f(factMark))
	case proto.EvPrioritySet:
		emit(f(factMark))
		t.priority = &d.Priority
	case proto.EvSuspended:
		// A model call it interrupted ended above, outcome suspended. The
		// task is held until a resume; its next instruction is timed from
		// the resume's commit (ready, below) or its own model_call, never
		// from here.
		emit(f(factMark))
		if t.suspend == nil {
			// A second suspended without a resume (a restart landing it
			// again) is the same suspension, timed from the first.
			t.suspend = &openSuspend{seq: ev.Seq, at: ev.Time, was: d.WasDoing, kept: d.Kept, keptThink: d.KeptThink}
			in.suspended++
		}
	case proto.EvResumed:
		emit(f(factMark))
		if d.Pending {
			// The resume cancelled a suspend that never landed: there is
			// no suspension to end, and the task went on as it was.
			return t.class, d
		}
		// Ready again from this commit: the next instruction is timed from
		// here (or its model_call), not from the suspend or a backoff the
		// suspend dropped.
		in.endSuspend(ev, d, t, outResumed, emit)
	case proto.EvSteerDelivered:
		// At the head of the commit of the step that sent the steer: that
		// step's call began at its model_call, not here.
		emit(f(factMark))
		return t.class, d
	case proto.EvModelCall:
		// Committed as the request goes out: the step that follows spans
		// the provider's latency only, not the queue or the limiters.
		t.call = ev.Time
		return t.class, d
	case proto.EvMigrated:
		emit(f(factMark))
		t.class = d.To
	case proto.EvDone, proto.EvFailed, proto.EvCancelled:
		in.endTask(ev, d, t, emit)
		return t.class, d
	case proto.EvAccepted, proto.EvAnswer:
	default:
		// recovered, resolved, migrate_refused, and kinds this package does
		// not know yet: noted on the task as they are.
		emit(f(factMark))
	}
	t.ready = ev.Time
	// A throttled wait counts only for the step that follows it; any other
	// commit of the task in between means its call never went out.
	t.call = time.Time{}
	if ev.Kind == proto.EvModelRetry && d.Wait == "backoff" && d.DelayMS > 0 {
		t.ready = ev.Time.Add(time.Duration(d.DelayMS) * time.Millisecond)
	}
	return t.class, d
}

// machine interprets an event without a task.
func (in *interp) machine(ev proto.Event, d data, emit func(fact)) string {
	if ev.Kind == proto.EvThrottled {
		// Committed when the limiters let the call go: that is when the call
		// began, whatever came before.
		if t := in.tasks[d.Task]; t != nil {
			t.call = ev.Time
			emit(fact{kind: factMark, ev: ev, d: d, task: d.Task, class: t.class})
		}
	}
	return d.Class
}

// after interprets an event committed after its task's terminal event.
func (in *interp) after(ev proto.Event, d data, t *taskState, emit func(fact)) {
	f := fact{kind: factAfter, ev: ev, d: d, task: ev.TaskID, class: t.class, name: ev.Kind, start: ev.Time, end: ev.Time}
	if ev.Kind == proto.EvToolLate {
		f.name = d.Tool
		if c := t.calls[d.CallID]; c != nil {
			f.name, f.start, f.timed = c.tool, c.at, true
		}
		f.kind, f.outcome, f.seq = factToolEnd, outLate, ev.Seq
	}
	emit(f)
}

func (in *interp) endStep(ev proto.Event, d data, t *taskState, emit func(fact)) {
	s := t.step
	t.step = nil
	f := fact{kind: factStep, ev: ev, d: d, task: ev.TaskID, class: t.class, seq: s.seq, name: s.pc, core: s.core, usage: s.usage, outcome: outOK,
		firstByte: s.firstByte, prefill: s.prefill, prefillThink: s.prefillThink}
	f.start, f.end = s.start, ev.Time
	if s.at.Before(ev.Time) {
		// The step was committed before its result: the call ran in between.
		f.start = s.at
	}
	if s.check > 0 {
		// The answer check ran between the answer and the commit: the call
		// ended that much earlier.
		f.end = f.end.Add(-s.check)
		if f.end.Before(f.start) {
			f.end = f.start
		}
	}
	if s.usage == nil && s.pc != task.StepTools.String() {
		switch ev.Kind {
		case proto.EvModelRetry:
			f.outcome, f.errClass = outError, d.Class
		case proto.EvFailed:
			f.outcome, f.errClass = outError, d.StopReason
			if f.errClass == "" {
				f.errClass = "error"
			}
		case proto.EvCancelled:
			f.outcome = outCancelled
		case proto.EvPreempted:
			f.outcome = outPreempted
		case proto.EvSuspended:
			f.outcome = outSuspended
		}
	}
	emit(f)
}

func (in *interp) endCall(ev proto.Event, d data, t *taskState, id, out string, emit func(fact)) {
	c := t.open[id]
	f := fact{kind: factToolEnd, ev: ev, d: d, task: ev.TaskID, class: t.class, outcome: out, end: ev.Time}
	if c == nil {
		// A call this interpreter never saw start (recovery re-sent it, or
		// following began mid-round): a span of its own, at its result.
		f.seq, f.start, f.name = ev.Seq, ev.Time, d.Tool
		if old := t.calls[id]; old != nil {
			f.name = old.tool
		}
		emit(f)
		return
	}
	delete(t.open, id)
	f.seq, f.start, f.name, f.timed = c.seq, c.at, c.tool, true
	if c.batch != nil {
		f.batch = c.batch.seq
	}
	emit(f)
	if b := c.batch; b != nil && b == t.batch {
		b.pending--
		b.last = ev.Time
		if b.pending == 0 {
			in.endBatch(ev, t, ev.Time, emit)
		}
	}
}

// endApproval ends the wait of a held call with verdict.
func (in *interp) endApproval(ev proto.Event, d data, t *taskState, id, verdict string, emit func(fact)) {
	a := t.held[id]
	if a == nil {
		return // held before this interpreter began following
	}
	delete(t.held, id)
	emit(fact{kind: factApprovalEnd, ev: ev, d: d, task: ev.TaskID, class: t.class, seq: a.seq, name: a.tool,
		outcome: verdict, start: a.at, end: ev.Time, timed: true})
}

// endSuspend ends a task's suspension, if it has one, with outcome.
func (in *interp) endSuspend(ev proto.Event, d data, t *taskState, outcome string, emit func(fact)) {
	s := t.suspend
	if s == nil {
		return // suspended before this interpreter began following, or not at all
	}
	t.suspend = nil
	in.suspended--
	emit(fact{kind: factSuspendEnd, ev: ev, d: d, task: ev.TaskID, class: t.class, seq: s.seq, name: s.was,
		outcome: outcome, start: s.at, end: ev.Time, timed: true, kept: s.kept, keptThink: s.keptThink})
}

func (in *interp) endBatch(ev proto.Event, t *taskState, end time.Time, emit func(fact)) {
	b := t.batch
	t.batch = nil
	emit(fact{kind: factBatchEnd, ev: ev, task: ev.TaskID, class: t.class, seq: b.seq, start: b.at, end: end})
}

func (in *interp) endTask(ev proto.Event, d data, t *taskState, emit func(fact)) {
	outcome := outCancelled
	switch ev.Kind {
	case proto.EvDone:
		outcome = "done"
	case proto.EvFailed:
		outcome = "failed"
	}
	// A task cancelled, or out of time, while suspended: the suspension ends
	// with it.
	in.endSuspend(ev, d, t, outcome, emit)
	// Calls still held for approval when the task ends were never decided.
	for _, id := range heldInOrder(t) {
		in.endApproval(ev, d, t, id, outCancelled, emit)
	}
	// Calls still open when the task ends never got a result: cancelled.
	for id := range sortedOpen(t) {
		in.endCall(ev, d, t, id, outCancelled, emit)
	}
	if t.batch != nil {
		in.endBatch(ev, t, ev.Time, emit)
	}
	f := fact{kind: factTaskEnd, ev: ev, d: d, task: ev.TaskID, class: t.class, name: ev.Kind, start: t.start, end: ev.Time,
		partial: t.partial, total: t.usage, steps: t.steps, priority: t.priority, outcome: outcome}
	emit(f)
	delete(in.tasks, ev.TaskID)
	t.ended, t.open, t.batch, t.step, t.held = true, nil, nil, nil, nil
	in.ended[ev.TaskID] = t
	in.order = append(in.order, ev.TaskID)
	if len(in.order) > endedTasks {
		delete(in.ended, in.order[0])
		in.order = in.order[1:]
	}
}

// heldInOrder is a task's held calls in the order they were announced.
func heldInOrder(t *taskState) []string {
	ids := make([]string, 0, len(t.held))
	for id := range t.held {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b string) int { return cmp.Compare(t.held[a].seq, t.held[b].seq) })
	return ids
}

// sortedOpen yields a task's open calls in the order they were made, so the
// facts of a cancel come out the same on every replay.
func sortedOpen(t *taskState) func(func(string) bool) {
	return func(yield func(string) bool) {
		for len(t.open) > 0 {
			var first string
			var seq int64
			for id, c := range t.open {
				if first == "" || c.seq < seq {
					first, seq = id, c.seq
				}
			}
			if !yield(first) {
				return
			}
			if _, still := t.open[first]; still {
				delete(t.open, first) // the caller did not end it: do not loop forever
			}
		}
	}
}
