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
//     class, priority, outcome, stop reason, steps, confirmed tokens.
//     Retries, throttling, compactions, migrations, refusals, recovery,
//     reconcile and operator resolutions are events on it, and so are the
//     tool policy's and the operators' decisions: tool_denied,
//     tool_rewritten, approval_needed (again after a restart, marked
//     neumann.recovered) and approved.
//   - "model" / "compact": one per model instruction, child of the task.
//     It spans from when the task could run (its previous commit, after any
//     backoff, or when the limiters let its call go) to the commit of the
//     step's result. Status error with the error class for a failed call.
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
// one of step, batch, tool, after, and seq the Seq of the event that opened
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

func (t *Tracer) emit(taskID string, id, parent trace.SpanID, name string, start, end time.Time, st sdktrace.Status, attrs []attribute.KeyValue, events []sdktrace.Event) {
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
		t.emit(f.task, spanID(f.task, "step", f.seq), TaskSpanID(f.task), name, f.start, f.end, st, attrs, nil)
	case factApprovalEnd:
		// The wait for an operator, from the call's first announcement.
		t.emit(f.task, spanID(f.task, "approval", f.seq), TaskSpanID(f.task), "approval "+f.name, f.start, f.end, sdktrace.Status{},
			[]attribute.KeyValue{
				attribute.String("neumann.task.id", f.task), attribute.String("neumann.tool", f.name),
				attribute.String("neumann.call.id", f.d.CallID), attribute.String("neumann.verdict", f.outcome),
			}, nil)
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

// compactAttrs are a compacted event's figures (design §5.3).
func compactAttrs(d data) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("neumann.compact.rounds", d.Rounds),
		attribute.Int("neumann.compact.messages_before", d.MessagesBefore), attribute.Int("neumann.compact.messages_after", d.MessagesAfter),
		attribute.Int64("neumann.compact.tokens_before_est", d.TokensBefore), attribute.Int64("neumann.compact.tokens_after_est", d.TokensAfter),
		attribute.Int("neumann.compact.chunk", d.Chunk), attribute.Int("neumann.compact.chunks", d.Chunks),
	}
}
