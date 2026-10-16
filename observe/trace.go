package observe

import (
	"context"
	"crypto/sha256"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// TracerOptions configure a Tracer.
type TracerOptions struct {
	// Resource describes the process; nil is service.name "neumann".
	Resource *resource.Resource
	// BatchSize is how many finished spans are exported at once; 0 = 256.
	// Follow flushes whatever is buffered each time it catches up.
	BatchSize int
}

// Tracer rebuilds OpenTelemetry traces from committed events, one trace per
// task, and hands the spans to an exporter (OTLP, or tracetest's in-memory
// exporter in tests). Spans start and end at the events' commit times, so
// they are the same whether the Tracer follows the machine live or replays
// its log later, and their ids are derived from the task id and the Seq of
// the event that opened them (TraceID, TaskSpanID): replaying the log from
// seq 0 after a crash exports exactly the spans a live Tracer did.
//
// A trace has:
//
//   - "task": the root span, accepted to done / failed / cancelled. Status
//     error with the error text for a failed task. Attributes: task id,
//     class, priority (its last, after any priority_set; left out when the
//     task's start was not seen), outcome, stop reason, steps, confirmed
//     tokens. Retries, throttling, compactions, migrations, refusals,
//     recovery, reconcile and operator resolutions are events on it, and so
//     are the tool policy's and the operators' decisions: tool_denied,
//     tool_rewritten, approval_needed (again after a restart, marked
//     neumann.recovered) and approved, and the answer check's rejections
//     (answer_rejected, with neumann.attempt and neumann.final). So are
//     preemptions (preempted: neumann.preempt.by, the preempting task's
//     id, .kept, .kept_think, .ran_ms, .n), priority changes (priority_set:
//     neumann.priority, neumann.priority_was), steers with a sender
//     reaching the model (steer_delivered: neumann.steer.seq, neumann.from),
//     and an operator's suspends and resumes (design §9.9: suspended with
//     neumann.was and, when it kept any, neumann.suspend.kept and
//     .kept_think; resumed, with neumann.pending when it cancelled a
//     suspend that had not landed).
//   - "model" / "compact": one per model instruction, child of the task.
//     It spans from its model_call, committed as the request goes out (in
//     a log written before model_call existed: from when the task could
//     run, its previous commit, after any backoff, or when the limiters let
//     its call go), to the commit of the step's result, less the answer
//     check's time (check_ms on the step).
//     Status error with the error class for a failed call. A call
//     preempted for a more urgent task (design §6.4) ends at its preempted
//     event with neumann.outcome preempted, which is not an error, and
//     carries neumann.preempt.kept, .kept_think and .n and a link to the
//     preempting task's root span; a call that continues kept text carries
//     neumann.prefill (its chars), one that continues a kept thought
//     neumann.prefill_think. A call cut off by a suspend ends at its
//     suspended event with neumann.outcome suspended, not an error, and
//     neumann.suspend.kept / .kept_think when it kept any; after the resume
//     the task's next call is timed from its own model_call, so the time
//     suspended is in no model span. A step with first_ms has a first_byte
//     event that many milliseconds after the span's start (the request),
//     telling the backend's queue from its generation.
//     A compaction that was applied carries its compacted event's figures
//     (neumann.compact.rounds, .messages_before, .messages_after,
//     .tokens_before_est, .tokens_after_est, .chunk, .chunks), which are
//     also an event on the task span.
//   - "dispatch": the instruction that sends planned calls after a
//     restart, child of the task.
//   - "tools": one per round of tool calls, child of the task, from the
//     commit that made the calls to the last result.
//   - "tool <name>": one per call, child of its round, from the call to its
//     result. Status error for an error result, an unknown outcome, or a
//     denial (outcome denied, ending at tool_denied).
//   - "approval <tool>": one per call held for an operator (design §9.7),
//     child of the task, from its first approval_needed to its decision,
//     with neumann.verdict allow, edit, deny, or cancelled when the task
//     ended first.
//   - "suspended": one per suspension (design §9.9), child of the task,
//     from its suspended event to the resume (neumann.outcome resumed) or
//     the task's end while suspended (cancelled, failed), with neumann.was
//     (ready, backoff, group, model, tools, approval, reconcile, check)
//     and what it kept, as on the event. A resume that cancelled a suspend
//     that never landed (resumed with pending) opens none.
//   - "steer_dropped", "tool_late": events committed after the task ended,
//     zero-length children of the task.
//
// Spans carry no task content: no prompts, answers, tool arguments or
// results. A task first seen mid-way through the log (a Tracer resumed from
// a saved seq) gets a root span from that event on, marked neumann.partial;
// its spans keep the ids a full replay gives. Events without a time (a
// store that kept none) produce no spans.
type Tracer struct {
	exp     sdktrace.SpanExporter
	res     *resource.Resource
	batch   int
	in      *interp
	tasks   map[string]*traced
	pending []sdktrace.ReadOnlySpan
}

type traced struct {
	events []sdktrace.Event
}

// NewTracer returns a Tracer exporting to exp.
func NewTracer(exp sdktrace.SpanExporter, o TracerOptions) *Tracer {
	if o.Resource == nil {
		o.Resource = resource.NewSchemaless(attribute.String("service.name", "neumann"))
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 256
	}
	return &Tracer{exp: exp, res: o.Resource, batch: o.BatchSize, in: newInterp(nil), tasks: map[string]*traced{}}
}

// TraceID is the trace of a task: the first 16 bytes of
// SHA-256("neumann/trace/" + taskID).
func TraceID(taskID string) trace.TraceID {
	var id trace.TraceID
	sum := sha256.Sum256([]byte("neumann/trace/" + taskID))
	copy(id[:], sum[:])
	return id
}

// TaskSpanID is the id of a task's root span: the first 8 bytes of
// SHA-256("neumann/span/" + taskID). Every other span's id is the first 8
// bytes of SHA-256("neumann/span/" + taskID + "/" + kind + "/" + seq), kind
// one of step, batch, tool, result, approval, suspend, after, and seq the Seq of the event that opened
// the span.
func TaskSpanID(taskID string) trace.SpanID {
	return hashSpan("neumann/span/" + taskID)
}

func spanID(taskID, kind string, seq int64) trace.SpanID {
	return hashSpan("neumann/span/" + taskID + "/" + kind + "/" + strconv.FormatInt(seq, 10))
}

func hashSpan(s string) trace.SpanID {
	var id trace.SpanID
	sum := sha256.Sum256([]byte(s))
	copy(id[:], sum[:])
	if !id.IsValid() {
		id[7] = 1
	}
	return id
}

func spanContext(taskID string, id trace.SpanID) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: TraceID(taskID), SpanID: id, TraceFlags: trace.FlagsSampled})
}

// Observe interprets ev; spans that ended are buffered for export.
func (t *Tracer) Observe(ev proto.Event) {
	t.in.feed(ev, t.fact)
	if len(t.pending) >= t.batch {
		t.Flush(context.Background())
	}
}

// Flush exports the buffered spans. An export error goes to otel.Handle;
// the spans are dropped either way, so a dead collector cannot grow memory.
func (t *Tracer) Flush(ctx context.Context) error {
	if len(t.pending) == 0 {
		return nil
	}
	spans := t.pending
	t.pending = nil
	err := t.exp.ExportSpans(ctx, spans)
	if err != nil {
		otel.Handle(err)
	}
	return err
}

// Shutdown flushes and shuts the exporter down.
func (t *Tracer) Shutdown(ctx context.Context) error {
	ferr := t.Flush(ctx)
	if err := t.exp.Shutdown(ctx); err != nil {
		return err
	}
	return ferr
}

var scope = instrumentation.Scope{Name: ScopeName}

func (t *Tracer) emit(taskID string, id, parent trace.SpanID, name string, start, end time.Time, st sdktrace.Status, attrs []attribute.KeyValue, events []sdktrace.Event, links ...sdktrace.Link) {
	if start.IsZero() || end.IsZero() {
		return
	}
	if end.Before(start) {
		end = start
	}
	stub := tracetest.SpanStub{
		Name:                 name,
		SpanContext:          spanContext(taskID, id),
		SpanKind:             trace.SpanKindInternal,
		StartTime:            start,
		EndTime:              end,
		Attributes:           attrs,
		Events:               events,
		Links:                links,
		Status:               st,
		Resource:             t.res,
		InstrumentationScope: scope,
	}
	if parent.IsValid() {
		stub.Parent = spanContext(taskID, parent)
	}
	t.pending = append(t.pending, stub.Snapshot())
}

func (t *Tracer) fact(f fact) {
	switch f.kind {
	case factTaskStart:
		t.tasks[f.task] = &traced{}
	case factMark:
		tt := t.tasks[f.task]
		if tt == nil || f.ev.Time.IsZero() {
			return
		}
		tt.events = append(tt.events, sdktrace.Event{Name: f.ev.Kind, Time: f.ev.Time, Attributes: markAttrs(f)})
	case factStep:
		name := f.name
		if name == task.StepTools.String() {
			name = "dispatch"
		}
		attrs := []attribute.KeyValue{
			attribute.String("neumann.task.id", f.task), attribute.String("neumann.class", f.class),
			attribute.String("neumann.pc", f.name), attribute.Int("neumann.core", f.core),
			attribute.String("neumann.outcome", f.outcome), attribute.Int64("neumann.seq", f.seq),
		}
		var st sdktrace.Status
		if f.outcome == outError {
			attrs = append(attrs, attribute.String("neumann.error.class", f.errClass))
			st = sdktrace.Status{Code: codes.Error, Description: f.errClass}
		}
		if u := f.usage; u != nil {
			attrs = append(attrs, tokenAttrs(*u)...)
		}
		if f.ev.Kind == proto.EvCompacted {
			attrs = append(attrs, compactAttrs(f.d)...)
		}
		if f.prefill > 0 {
			attrs = append(attrs, attribute.Int("neumann.prefill", f.prefill))
		}
		if f.prefillThink > 0 {
			attrs = append(attrs, attribute.Int("neumann.prefill_think", f.prefillThink))
		}
		var links []sdktrace.Link
		if f.outcome == outPreempted {
			attrs = append(attrs, attribute.Int("neumann.preempt.kept", f.d.Kept), attribute.Int("neumann.preempt.kept_think", f.d.KeptThink),
				attribute.Int("neumann.preempt.n", f.d.N))
			if f.d.By != "" {
				// The task that took the backend: its root span, in its own trace.
				links = append(links, sdktrace.Link{SpanContext: spanContext(f.d.By, TaskSpanID(f.d.By)),
					Attributes: []attribute.KeyValue{attribute.String("neumann.link", "preempted_by")}})
			}
		}
		if f.outcome == outSuspended {
			attrs = append(attrs, keptAttrs(f.d.Kept, f.d.KeptThink)...)
		}
		var events []sdktrace.Event
		if fb := f.firstByte; fb != nil && !f.start.IsZero() {
			// From the request, which the span starts with (its model_call),
			// and never past the span's end.
			at := f.start.Add(max(*fb, 0))
			if at.After(f.end) {
				at = f.end
			}
			if at.Before(f.start) {
				at = f.start
			}
			events = append(events, sdktrace.Event{Name: "first_byte", Time: at,
				Attributes: []attribute.KeyValue{attribute.Int64("neumann.first_ms", fb.Milliseconds())}})
		}
		t.emit(f.task, spanID(f.task, "step", f.seq), TaskSpanID(f.task), name, f.start, f.end, st, attrs, events, links...)
	case factApprovalEnd:
		// The wait for an operator, from the call's first announcement.
		t.emit(f.task, spanID(f.task, "approval", f.seq), TaskSpanID(f.task), "approval "+f.name, f.start, f.end, sdktrace.Status{},
			[]attribute.KeyValue{
				attribute.String("neumann.task.id", f.task), attribute.String("neumann.tool", f.name),
				attribute.String("neumann.call.id", f.d.CallID), attribute.String("neumann.verdict", f.outcome),
			}, nil)
	case factSuspendEnd:
		// The time an operator held the task off the CPU (design §9.9).
		attrs := []attribute.KeyValue{
			attribute.String("neumann.task.id", f.task), attribute.String("neumann.was", f.name),
			attribute.String("neumann.outcome", f.outcome), attribute.Int64("neumann.seq", f.seq),
		}
		attrs = append(attrs, keptAttrs(f.kept, f.keptThink)...)
		t.emit(f.task, spanID(f.task, "suspend", f.seq), TaskSpanID(f.task), "suspended", f.start, f.end, sdktrace.Status{}, attrs, nil)
	case factBatchEnd:
		t.emit(f.task, spanID(f.task, "batch", f.seq), TaskSpanID(f.task), "tools", f.start, f.end, sdktrace.Status{},
			[]attribute.KeyValue{attribute.String("neumann.task.id", f.task), attribute.Int64("neumann.seq", f.seq)}, nil)
	case factToolEnd:
		attrs := []attribute.KeyValue{
			attribute.String("neumann.task.id", f.task), attribute.String("neumann.tool", f.name),
			attribute.String("neumann.call.id", f.d.CallID), attribute.String("neumann.outcome", f.outcome),
		}
		var st sdktrace.Status
		if f.outcome == outError || f.outcome == outUnknown || f.outcome == outDenied {
			st = sdktrace.Status{Code: codes.Error, Description: f.outcome}
		}
		if f.outcome == outLate {
			t.emit(f.task, spanID(f.task, "after", f.ev.Seq), TaskSpanID(f.task), "tool_late", f.ev.Time, f.ev.Time, st, attrs, nil)
			return
		}
		parent := TaskSpanID(f.task)
		kind := "result" // a call never seen made: its span is opened by its result
		if f.timed {
			kind = "tool"
			if f.batch != 0 {
				parent = spanID(f.task, "batch", f.batch)
			}
		}
		t.emit(f.task, spanID(f.task, kind, f.seq), parent, "tool "+f.name, f.start, f.end, st, attrs, nil)
	case factAfter:
		t.emit(f.task, spanID(f.task, "after", f.ev.Seq), TaskSpanID(f.task), f.name, f.ev.Time, f.ev.Time, sdktrace.Status{},
			[]attribute.KeyValue{attribute.String("neumann.task.id", f.task)}, nil)
	case factTaskEnd:
		tt := t.tasks[f.task]
		delete(t.tasks, f.task)
		if tt == nil {
			tt = &traced{}
		}
		attrs := []attribute.KeyValue{
			attribute.String("neumann.task.id", f.task), attribute.String("neumann.class", f.class),
			attribute.String("neumann.outcome", f.outcome), attribute.String("neumann.stop_reason", f.d.StopReason),
			attribute.Int("neumann.steps", f.steps),
		}
		attrs = append(attrs, tokenAttrs(f.total)...)
		if f.priority != nil {
			attrs = append(attrs, attribute.Int("neumann.priority", *f.priority))
		}
		if f.partial {
			attrs = append(attrs, attribute.Bool("neumann.partial", true))
		}
		var st sdktrace.Status
		if f.outcome == "failed" {
			st = sdktrace.Status{Code: codes.Error, Description: f.d.Error}
		}
		t.emit(f.task, TaskSpanID(f.task), trace.SpanID{}, "task", f.start, f.end, st, attrs, tt.events)
	}
}

func tokenAttrs(u task.Usage) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("neumann.tokens.prompt", u.PromptTokens), attribute.Int("neumann.tokens.completion", u.CompletionTokens),
		attribute.Int("neumann.tokens.reasoning", u.ReasoningTokens), attribute.Int("neumann.tokens.cached", u.CachedTokens),
	}
}

// markAttrs are the scalar facts of a task-span event; never its content.
func markAttrs(f fact) []attribute.KeyValue {
	d := f.d
	a := []attribute.KeyValue{attribute.Int64("neumann.seq", f.ev.Seq)}
	add := func(k, v string) {
		if v != "" {
			a = append(a, attribute.String(k, v))
		}
	}
	switch f.ev.Kind {
	case proto.EvModelRetry:
		add("neumann.error.class", d.Class)
		add("neumann.wait", d.Wait)
		a = append(a, attribute.Int("neumann.attempt", d.Attempt), attribute.Int64("neumann.delay_ms", d.DelayMS))
	case proto.EvThrottled:
		add("neumann.class", d.Class)
		a = append(a, attribute.Int64("neumann.waited_ms", d.WaitedMS))
	case proto.EvMigrated:
		add("neumann.from", d.From)
		add("neumann.to", d.To)
		add("neumann.reason", d.Reason)
	case proto.EvCompacted:
		a = append(a, compactAttrs(d)...)
	case proto.EvMigrateRefused:
		add("neumann.from", d.From)
		add("neumann.to", d.To)
		add("neumann.check", d.Check)
	case proto.EvReconcileNeeded, proto.EvResolved:
		add("neumann.call.id", d.CallID)
		add("neumann.tool", d.Tool)
		if d.Cancelled {
			a = append(a, attribute.Bool("neumann.cancelled", true))
		}
	case proto.EvRecovered:
		add("neumann.from", d.From)
	case proto.EvPreempted:
		// by is the preempting task's id, as on the task span: not content.
		add("neumann.preempt.by", d.By)
		a = append(a, attribute.Int("neumann.preempt.kept", d.Kept), attribute.Int("neumann.preempt.kept_think", d.KeptThink), attribute.Int64("neumann.preempt.ran_ms", d.RanMS),
			attribute.Int("neumann.preempt.n", d.N))
	case proto.EvPrioritySet:
		a = append(a, attribute.Int("neumann.priority", d.Priority), attribute.Int("neumann.priority_was", d.Was))
	case proto.EvSuspended:
		add("neumann.was", d.WasDoing)
		a = append(a, keptAttrs(d.Kept, d.KeptThink)...)
	case proto.EvResumed:
		if d.Pending {
			a = append(a, attribute.Bool("neumann.pending", true))
		}
	case proto.EvSteerDelivered:
		// The sender, a name; never the steer's text.
		a = append(a, attribute.Int64("neumann.steer.seq", d.SteerSeq))
		add("neumann.from", d.From)
	case proto.EvAnswerRejected:
		// Never the reason or the feedback: they may quote the answer.
		a = append(a, attribute.Int("neumann.attempt", d.Attempt))
		if d.Final {
			a = append(a, attribute.Bool("neumann.final", true))
		}
	case proto.EvToolDenied, proto.EvToolRewritten, proto.EvApprovalNeeded, proto.EvApproved:
		// Never the arguments, nor a reason: a policy may quote them.
		add("neumann.call.id", d.CallID)
		add("neumann.tool", d.Tool)
		add("neumann.by", d.By)
		add("neumann.effect", d.Effect)
		if d.Recovered {
			a = append(a, attribute.Bool("neumann.recovered", true))
		}
		if d.Edited {
			a = append(a, attribute.Bool("neumann.edited", true))
		}
	}
	return a
}

// keptAttrs are what a suspend kept of the model call it interrupted, when
// it kept anything.
func keptAttrs(kept, think int) []attribute.KeyValue {
	var a []attribute.KeyValue
	if kept != 0 {
		a = append(a, attribute.Int("neumann.suspend.kept", kept))
	}
	if think != 0 {
		a = append(a, attribute.Int("neumann.suspend.kept_think", think))
	}
	return a
}

// compactAttrs are a compacted event's figures (design §5.3).
func compactAttrs(d data) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("neumann.compact.rounds", d.Rounds),
		attribute.Int("neumann.compact.messages_before", d.MessagesBefore), attribute.Int("neumann.compact.messages_after", d.MessagesAfter),
		attribute.Int64("neumann.compact.tokens_before_est", d.TokensBefore), attribute.Int64("neumann.compact.tokens_after_est", d.TokensAfter),
		attribute.Int("neumann.compact.chunk", d.Chunk), attribute.Int("neumann.compact.chunks", d.Chunks),
	}
}
