package observe

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/liliang-cn/neumann/proto"
)

// span is what a test compares of an exported span: everything a backend
// would show, without the resource and scope pointers.
type span struct {
	Name          string
	Trace         trace.TraceID
	ID, Parent    trace.SpanID
	Start, End    time.Time
	Status        codes.Code
	Desc          string
	Attrs         map[string]string
	Events        []string            // name@offset-from-t0
	EventAttrs    []map[string]string // each event's attributes, in order
	Links         []string            // trace/span of each link
	TraceSampled  bool
	ParentIsValid bool
}

func spans(exp *tracetest.InMemoryExporter) []span {
	var out []span
	for _, s := range exp.GetSpans() {
		sp := span{
			Name: s.Name, Trace: s.SpanContext.TraceID(), ID: s.SpanContext.SpanID(), Parent: s.Parent.SpanID(),
			Start: s.StartTime, End: s.EndTime, Status: s.Status.Code, Desc: s.Status.Description,
			Attrs: map[string]string{}, TraceSampled: s.SpanContext.IsSampled(), ParentIsValid: s.Parent.IsValid(),
		}
		for _, kv := range s.Attributes {
			sp.Attrs[string(kv.Key)] = kv.Value.Emit()
		}
		for _, e := range s.Events {
			sp.Events = append(sp.Events, fmt.Sprintf("%s@%v", e.Name, e.Time.Sub(t0)))
			a := map[string]string{}
			for _, kv := range e.Attributes {
				a[string(kv.Key)] = kv.Value.Emit()
			}
			sp.EventAttrs = append(sp.EventAttrs, a)
		}
		for _, l := range s.Links {
			sp.Links = append(sp.Links, l.SpanContext.TraceID().String()+"/"+l.SpanContext.SpanID().String())
		}
		out = append(out, sp)
	}
	slices.SortFunc(out, func(a, b span) int {
		if c := strings.Compare(a.Trace.String(), b.Trace.String()); c != 0 {
			return c
		}
		if c := a.Start.Compare(b.Start); c != 0 {
			return c
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
	return out
}

func byName(ss []span, task, name string) []span {
	var out []span
	for _, s := range ss {
		if s.Trace == TraceID(task) && s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func one(t *testing.T, ss []span, task, name string) span {
	t.Helper()
	got := byName(ss, task, name)
	if len(got) != 1 {
		t.Fatalf("%d %q spans for %s, want 1: %+v", len(got), name, task, ss)
	}
	return got[0]
}

// A scripted run, traced live: one trace per task, a task span from accepted
// to done, a model span per step, a tools span per batch with a span per
// call, all timed by the events' commit times. Replaying the log from seq 0
// into a second tracer exports exactly the same spans.
func TestTracesFromAScriptedRun(t *testing.T) {
	r := newRig(t)
	live := tracetest.NewInMemoryExporter()
	tr := NewTracer(live, TracerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Follow(ctx, r.m, 0, tr)
	}()
	r.run(t, "a", "b")
	head := r.log(t)
	waitFor(t, func() bool { return len(live.GetSpans()) == 2*5 })
	cancel()
	<-done
	if err := tr.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := spans(live)

	for i, id := range []string{"a", "b"} {
		start := t0.Add(time.Duration(i) * (2*modelTime + toolTime))
		root := one(t, got, id, "task")
		if root.ID != TaskSpanID(id) || root.ParentIsValid || !root.TraceSampled {
			t.Fatalf("task span %+v", root)
		}
		if !root.Start.Equal(start) || !root.End.Equal(start.Add(2*modelTime+toolTime)) {
			t.Fatalf("task %s spans %v..%v, want from %v", id, root.Start, root.End, start)
		}
		if root.Status != codes.Unset || root.Attrs["neumann.task.id"] != id || root.Attrs["neumann.class"] != "default" ||
			root.Attrs["neumann.outcome"] != "done" || root.Attrs["neumann.stop_reason"] != "end_turn" ||
			root.Attrs["neumann.tokens.prompt"] != "20" || root.Attrs["neumann.steps"] != "2" {
			t.Fatalf("task span %+v", root)
		}
		models := byName(got, id, "model")
		if len(models) != 2 {
			t.Fatalf("model spans %+v", models)
		}
		for j, m := range models {
			from := start.Add(time.Duration(j) * (modelTime + toolTime))
			if m.Parent != root.ID || !m.Start.Equal(from) || !m.End.Equal(from.Add(modelTime)) ||
				m.Attrs["neumann.outcome"] != "ok" || m.Attrs["neumann.tokens.prompt"] != "10" || m.Attrs["neumann.core"] == "" {
				t.Fatalf("model span %d of %s: %+v", j, id, m)
			}
		}
		batch := one(t, got, id, "tools")
		call := one(t, got, id, "tool util_echo")
		if batch.Parent != root.ID || call.Parent != batch.ID {
			t.Fatalf("tools %+v, call %+v", batch, call)
		}
		at := start.Add(modelTime)
		if !batch.Start.Equal(at) || !batch.End.Equal(at.Add(toolTime)) || !call.Start.Equal(at) || !call.End.Equal(at.Add(toolTime)) {
			t.Fatalf("tools %+v, call %+v", batch, call)
		}
		if call.Attrs["neumann.tool"] != "util_echo" || call.Attrs["neumann.call.id"] != "c-1" || call.Attrs["neumann.outcome"] != "ok" {
			t.Fatalf("call %+v", call)
		}
	}

	// A host rebuilding traces after a crash replays the log from seq 0.
	replayed := tracetest.NewInMemoryExporter()
	rt := NewTracer(replayed, TracerOptions{})
	feed(rt, head)
	if err := rt.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if again := spans(replayed); !reflect.DeepEqual(again, got) {
		t.Fatalf("replay differs:\nlive   %+v\nreplay %+v", got, again)
	}
}

// Failures: a step that errors carries its error class, a failed task is an
// error span with the error text, and retries, throttling, migrations,
// reconcile and steers are events on the task span.
func TestTraceOfAFailingTask(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	s.add(3*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(3*time.Second, proto.EvModelRetry, "a", map[string]any{"class": "overload", "attempt": 1, "delay_ms": 1000, "wait": "backoff"})
	s.add(5*time.Second, proto.EvThrottled, "", map[string]any{"task": "a", "class": "big", "waited_ms": 250})
	s.add(9*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(9*time.Second, proto.EvUsage, "a", usage(100, 20, 0, 0))
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-1", "tool": "pay_send"})
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-2", "tool": "fs_read"})
	s.add(11*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-2", "is_error": true})
	s.add(12*time.Second, proto.EvMigrated, "a", map[string]any{"from": "big", "to": "little", "reason": "group_down"})
	s.add(20*time.Second, proto.EvReconcileNeeded, "a", map[string]any{"call_id": "c-1", "tool": "pay_send", "cancelled": true})
	s.add(20*time.Second, proto.EvFailed, "a", map[string]any{"error": "the deadline passed", "stop_reason": "deadline"})
	s.add(20*time.Second, proto.EvSteerDropped, "a", map[string]any{"seq": 1})
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)

	root := one(t, got, "a", "task")
	if root.Status != codes.Error || root.Desc != "the deadline passed" || root.Attrs["neumann.stop_reason"] != "deadline" ||
		root.Attrs["neumann.class"] != "little" || !root.End.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("task span %+v", root)
	}
	wantEvents := []string{"model_retry@3s", "throttled@5s", "migrated@12s", "reconcile_needed@20s"}
	if !reflect.DeepEqual(root.Events, wantEvents) {
		t.Fatalf("task span events %v, want %v", root.Events, wantEvents)
	}
	// What the store commits after the terminal event is a span of its own,
	// so a trace never depends on when the tracer happened to flush.
	if d := one(t, got, "a", "steer_dropped"); d.Parent != root.ID || !d.Start.Equal(root.End) || !d.End.Equal(root.End) {
		t.Fatalf("steer_dropped span %+v", d)
	}
	models := byName(got, "a", "model")
	if len(models) != 2 {
		t.Fatalf("model spans %+v", models)
	}
	if m := models[0]; m.Status != codes.Error || m.Attrs["neumann.error.class"] != "overload" || !m.Start.Equal(t0) || !m.End.Equal(t0.Add(3*time.Second)) {
		t.Fatalf("failed step %+v", m)
	}
	if m := models[1]; m.Status != codes.Unset || !m.Start.Equal(t0.Add(5*time.Second)) || !m.End.Equal(t0.Add(9*time.Second)) {
		t.Fatalf("throttled step %+v", m)
	}
	if c := one(t, got, "a", "tool fs_read"); c.Status != codes.Error || !c.End.Equal(t0.Add(11*time.Second)) {
		t.Fatalf("failed call %+v", c)
	}
	if c := one(t, got, "a", "tool pay_send"); c.Status != codes.Error || c.Attrs["neumann.outcome"] != "unknown" || !c.End.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("call of unknown outcome %+v", c)
	}
	if b := one(t, got, "a", "tools"); !b.End.Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("batch %+v", b)
	}
}

// A tracer that starts in the middle of the log (a host resuming from a
// saved seq) still parents every span under the task's deterministic ids,
// the same ones a full replay gives, and marks the task span partial.
func TestTraceResumedMidway(t *testing.T) {
	r := newRig(t)
	r.run(t, "a")
	evs := r.log(t)
	full := tracetest.NewInMemoryExporter()
	ft := NewTracer(full, TracerOptions{})
	feed(ft, evs)
	ft.Flush(context.Background())

	cut := slices.IndexFunc(evs, func(e proto.Event) bool { return e.Kind == proto.EvToolResult })
	half := tracetest.NewInMemoryExporter()
	ht := NewTracer(half, TracerOptions{})
	feed(ht, evs[cut:])
	ht.Flush(context.Background())

	fs, hs := spans(full), spans(half)
	root := one(t, hs, "a", "task")
	if root.ID != TaskSpanID("a") || root.Attrs["neumann.partial"] != "true" || !root.Start.Equal(evs[cut].Time) {
		t.Fatalf("resumed task span %+v", root)
	}
	last := byName(fs, "a", "model")[1]
	if m := one(t, hs, "a", "model"); m.ID != last.ID || m.Parent != root.ID || !m.Start.Equal(last.Start) {
		t.Fatalf("resumed model span %+v, full replay %+v", m, last)
	}
}

// Events of a store that kept no times (zero) never become spans with a
// year-one start; the trace is skipped rather than invented.
func TestTraceToleratesEventsWithoutTimes(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", nil)
	s.add(0, proto.EvStep, "a", map[string]any{"pc": "model"})
	s.add(0, proto.EvUsage, "a", usage(1, 1, 0, 0))
	s.add(0, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn"})
	for i := range s.evs {
		s.evs[i].Time = time.Time{}
	}
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("%d spans from events without times", n)
	}
}

func TestSpanIDsAreDeterministic(t *testing.T) {
	if TraceID("a") != TraceID("a") || TraceID("a") == TraceID("b") || !TraceID("a").IsValid() {
		t.Fatal("trace ids")
	}
	if TaskSpanID("a") == spanID("a", "step", 1) || spanID("a", "step", 1) == spanID("a", "step", 2) || spanID("a", "step", 1) == spanID("a", "tool", 1) {
		t.Fatal("span ids collide")
	}
	_ = attribute.String
}

// A throttled wait whose call never went out (the group went down while the
// task waited, and it parked) does not set the start of a later step.
func TestThrottleWithoutACall(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", nil)
	s.add(2*time.Second, proto.EvThrottled, "", map[string]any{"task": "a", "class": "default", "waited_ms": 2000})
	s.add(3*time.Second, proto.EvMigrateRefused, "a", map[string]any{"from": "default", "to": "small", "check": "context"})
	s.add(9*time.Second, proto.EvStep, "a", map[string]any{"pc": "model"})
	s.add(9*time.Second, proto.EvUsage, "a", usage(1, 1, 0, 0))
	s.add(9*time.Second, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn"})
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	if m := one(t, spans(exp), "a", "model"); !m.Start.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("model span starts at %v, want when the task parked", m.Start.Sub(t0))
	}
}

// A model_call event marks when the request went out, so the model span
// covers the provider's latency only, not the queue or the limiters before it.
func TestModelSpanStartsAtTheCall(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", nil)
	s.add(4*time.Second, proto.EvModelCall, "a", map[string]any{"pc": "model", "core": 1})
	s.add(6*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(6*time.Second, proto.EvUsage, "a", usage(1, 1, 0, 0))
	s.add(6*time.Second, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn"})
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	m := one(t, spans(exp), "a", "model")
	if !m.Start.Equal(t0.Add(4*time.Second)) || !m.End.Equal(t0.Add(6*time.Second)) {
		t.Fatalf("model span %v → %v, want 4s → 6s", m.Start.Sub(t0), m.End.Sub(t0))
	}
}
