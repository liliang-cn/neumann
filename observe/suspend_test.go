package observe

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/proto"
)

// suspension is five tasks an operator suspends, the way the core commits
// them (design §9.9):
//
//	r: suspended while ready at 1s, resumed at 5s; its call goes out at 6s
//	   and answers at 8s.
//	m: its call goes out at 1s and is cut off by the suspend at 3s, keeping
//	   80 chars (step and suspended, one commit, no usage); resumed at 10s,
//	   its continuation goes out at 11s with the 80 chars as prefill and
//	   answers at 14s.
//	p: a resume at once cancels a suspend that never landed: resumed
//	   {pending} rides on the commit of the call that went on (out at 1s,
//	   answered at 3s).
//	c: suspended at 3s on the commit that ends its tool round, cancelled
//	   at 7s while suspended.
//	b: a call fails at 1s and backs off 10s; suspended at 2s in the backoff,
//	   resumed at 4s; its next step, in a log without model_call, is timed
//	   from the resume (4s), not the suspend, nor the end of the backoff
//	   the suspend dropped; it answers at 6s.
func suspension() script {
	var s script
	s.add(0, proto.EvAccepted, "r", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvSuspended, "r", map[string]any{"was": "ready", "kept": 0, "kept_think": 0})
	s.add(5*time.Second, proto.EvResumed, "r", map[string]any{"pending": false})
	s.add(6*time.Second, proto.EvModelCall, "r", map[string]any{"pc": "model", "core": 1})
	s.add(8*time.Second, proto.EvStep, "r", map[string]any{"pc": "model", "core": 1})
	s.add(8*time.Second, proto.EvUsage, "r", usage(10, 5, 0, 0))
	s.add(8*time.Second, proto.EvAnswer, "r", map[string]any{"text": "secret answer"})
	s.add(8*time.Second, proto.EvDone, "r", map[string]any{"stop_reason": "end_turn"})

	s.add(0, proto.EvAccepted, "m", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvModelCall, "m", map[string]any{"pc": "model", "core": 2})
	s.add(3*time.Second, proto.EvStep, "m", map[string]any{"pc": "model", "core": 2, "first_ms": 500})
	s.add(3*time.Second, proto.EvSuspended, "m", map[string]any{"was": "model", "kept": 80, "kept_think": 0})
	s.add(10*time.Second, proto.EvResumed, "m", map[string]any{"pending": false})
	s.add(11*time.Second, proto.EvModelCall, "m", map[string]any{"pc": "model", "core": 2})
	s.add(14*time.Second, proto.EvStep, "m", map[string]any{"pc": "model", "core": 2, "prefill": 80})
	s.add(14*time.Second, proto.EvUsage, "m", usage(40, 30, 0, 0))
	s.add(14*time.Second, proto.EvAnswer, "m", map[string]any{"text": "kept text and the rest"})
	s.add(14*time.Second, proto.EvDone, "m", map[string]any{"stop_reason": "end_turn"})

	s.add(0, proto.EvAccepted, "p", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvModelCall, "p", map[string]any{"pc": "model", "core": 3})
	s.add(3*time.Second, proto.EvStep, "p", map[string]any{"pc": "model", "core": 3})
	s.add(3*time.Second, proto.EvResumed, "p", map[string]any{"pending": true})
	s.add(3*time.Second, proto.EvUsage, "p", usage(10, 5, 0, 0))
	s.add(3*time.Second, proto.EvDone, "p", map[string]any{"stop_reason": "end_turn"})

	s.add(0, proto.EvAccepted, "c", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvModelCall, "c", map[string]any{"pc": "model", "core": 4})
	s.add(2*time.Second, proto.EvStep, "c", map[string]any{"pc": "model", "core": 4})
	s.add(2*time.Second, proto.EvUsage, "c", usage(10, 5, 0, 0))
	s.add(2*time.Second, proto.EvToolCall, "c", map[string]any{"call_id": "c1", "tool": "util_echo"})
	s.add(3*time.Second, proto.EvToolResult, "c", map[string]any{"call_id": "c1", "tool": "util_echo"})
	s.add(3*time.Second, proto.EvSuspended, "c", map[string]any{"was": "tools", "kept": 0, "kept_think": 0})
	s.add(7*time.Second, proto.EvCancelled, "c", map[string]any{"stop_reason": "cancelled"})

	s.add(0, proto.EvAccepted, "b", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvStep, "b", map[string]any{"pc": "model", "core": 5})
	s.add(time.Second, proto.EvModelRetry, "b", map[string]any{"class": "transient", "attempt": 1, "wait": "backoff", "delay_ms": 10000})
	s.add(2*time.Second, proto.EvSuspended, "b", map[string]any{"was": "backoff", "kept": 0, "kept_think": 0})
	s.add(4*time.Second, proto.EvResumed, "b", map[string]any{"pending": false})
	s.add(6*time.Second, proto.EvStep, "b", map[string]any{"pc": "model", "core": 5})
	s.add(6*time.Second, proto.EvUsage, "b", usage(10, 5, 0, 0))
	s.add(6*time.Second, proto.EvDone, "b", map[string]any{"stop_reason": "end_turn"})
	return s
}

// at is a span's start and end, from t0.
func at(s span) [2]time.Duration { return [2]time.Duration{s.Start.Sub(t0), s.End.Sub(t0)} }

func span2(a, b time.Duration) [2]time.Duration { return [2]time.Duration{a, b} }

// A suspension is a "suspended" span under the task, from its suspended
// event to the resume, or to the task's end while suspended; the task span
// notes both events. A call cut off by the suspend ends there with outcome
// suspended, not an error, and its continuation is timed from its own
// model_call: the time suspended is in no model span.
func TestTraceOfSuspendedTasks(t *testing.T) {
	s := suspension()
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)

	// Suspended while ready.
	root := one(t, got, "r", "task")
	if want := []string{"suspended@1s", "resumed@5s"}; !reflect.DeepEqual(root.Events, want) {
		t.Fatalf("r task span events %v, want %v", root.Events, want)
	}
	if a := root.EventAttrs[0]; a["neumann.was"] != "ready" || a["neumann.suspend.kept"] != "" || a["neumann.suspend.kept_think"] != "" {
		t.Fatalf("suspended event %v", a)
	}
	if a := root.EventAttrs[1]; a["neumann.pending"] != "" {
		t.Fatalf("resumed event %v", a)
	}
	sp := one(t, got, "r", "suspended")
	if sp.ID != spanID("r", "suspend", s.seqOf("r", proto.EvSuspended, 0)) || sp.Parent != root.ID || at(sp) != span2(time.Second, 5*time.Second) ||
		sp.Status != codes.Unset || sp.Attrs["neumann.was"] != "ready" || sp.Attrs["neumann.outcome"] != "resumed" ||
		sp.Attrs["neumann.suspend.kept"] != "" {
		t.Fatalf("r suspended span %+v", sp)
	}
	if m := one(t, got, "r", "model"); at(m) != span2(6*time.Second, 8*time.Second) || m.Attrs["neumann.outcome"] != "ok" {
		t.Fatalf("r model span %v %+v, want 6s → 8s", at(m), m)
	}

	// Suspended mid-call, keeping 80 chars, then resumed and continued.
	root = one(t, got, "m", "task")
	if want := []string{"suspended@3s", "resumed@10s"}; !reflect.DeepEqual(root.Events, want) {
		t.Fatalf("m task span events %v, want %v", root.Events, want)
	}
	if a := root.EventAttrs[0]; a["neumann.was"] != "model" || a["neumann.suspend.kept"] != "80" || a["neumann.suspend.kept_think"] != "" {
		t.Fatalf("suspended event %v", a)
	}
	if root.Attrs["neumann.outcome"] != "done" || root.Attrs["neumann.steps"] != "1" || at(root) != span2(0, 14*time.Second) {
		t.Fatalf("m task span %+v", root)
	}
	models := byName(got, "m", "model")
	if len(models) != 2 {
		t.Fatalf("m model spans %+v", models)
	}
	cut, cont := models[0], models[1]
	if cut.ID != spanID("m", "step", s.seqOf("m", proto.EvStep, 0)) || at(cut) != span2(time.Second, 3*time.Second) ||
		cut.Status != codes.Unset || cut.Attrs["neumann.outcome"] != "suspended" || cut.Attrs["neumann.suspend.kept"] != "80" ||
		cut.Attrs["neumann.error.class"] != "" || cut.Attrs["neumann.tokens.prompt"] != "" || cut.Attrs["neumann.preempt.kept"] != "" ||
		len(cut.Links) != 0 || !reflect.DeepEqual(cut.Events, []string{"first_byte@1.5s"}) {
		t.Fatalf("suspended model span %v %+v", at(cut), cut)
	}
	if at(cont) != span2(11*time.Second, 14*time.Second) || cont.Attrs["neumann.outcome"] != "ok" || cont.Attrs["neumann.prefill"] != "80" ||
		cont.Attrs["neumann.suspend.kept"] != "" || cont.Attrs["neumann.tokens.completion"] != "30" {
		t.Fatalf("continued model span %v %+v", at(cont), cont)
	}
	sp = one(t, got, "m", "suspended")
	if at(sp) != span2(3*time.Second, 10*time.Second) || sp.Attrs["neumann.was"] != "model" || sp.Attrs["neumann.suspend.kept"] != "80" ||
		sp.Attrs["neumann.suspend.kept_think"] != "" || sp.Attrs["neumann.outcome"] != "resumed" {
		t.Fatalf("m suspended span %v %+v", at(sp), sp)
	}

	// A pending resume: an event on the task span, no suspended span, and
	// the call it rode on keeps its usage and its outcome.
	root = one(t, got, "p", "task")
	if want := []string{"resumed@3s"}; !reflect.DeepEqual(root.Events, want) || root.EventAttrs[0]["neumann.pending"] != "true" {
		t.Fatalf("p task span events %v %v, want %v pending", root.Events, root.EventAttrs, want)
	}
	if n := len(byName(got, "p", "suspended")); n != 0 {
		t.Fatalf("%d suspended spans for a suspend that never landed", n)
	}
	if m := one(t, got, "p", "model"); at(m) != span2(time.Second, 3*time.Second) || m.Attrs["neumann.outcome"] != "ok" ||
		m.Attrs["neumann.tokens.prompt"] != "10" {
		t.Fatalf("p model span %v %+v", at(m), m)
	}
	if root.Attrs["neumann.steps"] != "1" {
		t.Fatalf("p task span %+v", root)
	}

	// Cancelled while suspended: the suspension ends with the task.
	root = one(t, got, "c", "task")
	if want := []string{"suspended@3s"}; !reflect.DeepEqual(root.Events, want) || root.EventAttrs[0]["neumann.was"] != "tools" {
		t.Fatalf("c task span events %v %v", root.Events, root.EventAttrs)
	}
	sp = one(t, got, "c", "suspended")
	if at(sp) != span2(3*time.Second, 7*time.Second) || sp.Attrs["neumann.outcome"] != "cancelled" || sp.Attrs["neumann.was"] != "tools" ||
		sp.Status != codes.Unset {
		t.Fatalf("c suspended span %v %+v", at(sp), sp)
	}
	if b := one(t, got, "c", "tools"); at(b) != span2(2*time.Second, 3*time.Second) {
		t.Fatalf("c tool round %v", at(b))
	}
	if tl := one(t, got, "c", "tool util_echo"); at(tl) != span2(2*time.Second, 3*time.Second) || tl.Attrs["neumann.outcome"] != "ok" {
		t.Fatalf("c tool span %v %+v", at(tl), tl)
	}
	if at(root) != span2(0, 7*time.Second) || root.Attrs["neumann.outcome"] != "cancelled" {
		t.Fatalf("c task span %v %+v", at(root), root)
	}

	// Suspended in a backoff: the next step, without a model_call, starts at
	// the resume.
	sp = one(t, got, "b", "suspended")
	if at(sp) != span2(2*time.Second, 4*time.Second) || sp.Attrs["neumann.was"] != "backoff" {
		t.Fatalf("b suspended span %v %+v", at(sp), sp)
	}
	models = byName(got, "b", "model")
	if len(models) != 2 || models[0].Attrs["neumann.outcome"] != "error" || at(models[1]) != span2(4*time.Second, 6*time.Second) {
		t.Fatalf("b model spans %+v", models)
	}

	for _, sp := range got {
		for k, v := range sp.Attrs {
			if strings.Contains(v, "secret") || strings.Contains(v, "kept text") {
				t.Errorf("task content in %s %s=%s", sp.Name, k, v)
			}
		}
	}
}

// A task that runs out of time while suspended fails, and its suspension
// ends with it, outcome failed; a second suspended without a resume (a
// restart landing the suspend again) is the same suspension.
func TestSuspensionEndsWithAFailedTask(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "d", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvSuspended, "d", map[string]any{"was": "ready"})
	s.add(2*time.Second, proto.EvRecovered, "d", map[string]any{"from": "suspended"})
	s.add(2*time.Second, proto.EvSuspended, "d", map[string]any{"was": "ready"})
	s.add(3*time.Second, proto.EvFailed, "d", map[string]any{"stop_reason": "deadline", "error": "deadline exceeded"})
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)
	sp := one(t, got, "d", "suspended")
	if sp.ID != spanID("d", "suspend", s.seqOf("d", proto.EvSuspended, 0)) || at(sp) != span2(time.Second, 3*time.Second) ||
		sp.Attrs["neumann.outcome"] != "failed" || sp.Status != codes.Unset {
		t.Fatalf("d suspended span %v %+v", at(sp), sp)
	}
	if root := one(t, got, "d", "task"); root.Status != codes.Error || at(root) != span2(0, 3*time.Second) {
		t.Fatalf("d task span %+v", root)
	}

	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(m, s.evs[:4])
	collect(t, reader).must("neumann.tasks.suspended", nil, 1)
	feed(m, s.evs[4:])
	collect(t, reader).must("neumann.tasks.suspended", nil, 0)
}

// A tracer that followed the suspensions live, flushing after every event,
// exports field for field what a tracer replaying the log from seq 0 does.
// One resumed mid-way, after m's suspended event, sees no suspension to end
// (no suspended span for m) and gives its continuation the same id.
func TestSuspensionTraceReplays(t *testing.T) {
	s := suspension()
	live := tracetest.NewInMemoryExporter()
	if _, err := Follow(context.Background(), liveSource(s.evs), 0, NewTracer(live, TracerOptions{})); err != nil {
		t.Fatal(err)
	}
	replayed := tracetest.NewInMemoryExporter()
	rt := NewTracer(replayed, TracerOptions{})
	feed(rt, s.evs)
	rt.Flush(context.Background())
	got := spans(live)
	if again := spans(replayed); !reflect.DeepEqual(again, got) {
		t.Fatalf("replay differs:\nlive   %+v\nreplay %+v", got, again)
	}
	// 5 tasks; model calls r 1, m 2, p 1, c 1, b 2; c's round and its call;
	// suspensions r, m, c, b.
	if n := len(got); n != 5+7+2+4 {
		t.Fatalf("%d spans: %+v", n, got)
	}

	cut := slices.IndexFunc(s.evs, func(e proto.Event) bool { return e.TaskID == "m" && e.Kind == proto.EvSuspended })
	half := tracetest.NewInMemoryExporter()
	ht := NewTracer(half, TracerOptions{})
	feed(ht, s.evs[cut+1:])
	ht.Flush(context.Background())
	hs := spans(half)
	if n := len(byName(hs, "m", "suspended")); n != 0 {
		t.Fatalf("%d suspended spans for a suspension never seen begin", n)
	}
	if m := one(t, hs, "m", "model"); m.ID != byName(got, "m", "model")[1].ID || at(m) != span2(11*time.Second, 14*time.Second) {
		t.Fatalf("resumed model span %+v", m)
	}
}

// Metrics: a call cut off by a suspend is a model call of outcome
// suspended, timed and not a failure; neumann.tasks.suspended counts the
// tasks suspended and not yet resumed or ended, from the events, with or
// without a Source.
func TestMetricsOfSuspension(t *testing.T) {
	s := suspension()
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	upTo := func(task, kind string) int {
		return slices.IndexFunc(s.evs, func(e proto.Event) bool { return e.TaskID == task && e.Kind == kind }) + 1
	}
	fed := 0
	feedTo := func(n int) {
		feed(m, s.evs[fed:n])
		fed = n
	}
	feedTo(upTo("r", proto.EvSuspended))
	collect(t, reader).must("neumann.tasks.suspended", nil, 1)
	feedTo(upTo("r", proto.EvResumed))
	collect(t, reader).must("neumann.tasks.suspended", nil, 0)
	feedTo(upTo("m", proto.EvSuspended))
	collect(t, reader).must("neumann.tasks.suspended", nil, 1)
	feedTo(upTo("c", proto.EvSuspended))
	collect(t, reader).must("neumann.tasks.suspended", nil, 1) // m resumed, p never landed, c suspended
	feedTo(upTo("b", proto.EvSuspended))
	collect(t, reader).must("neumann.tasks.suspended", nil, 1) // c cancelled, b suspended
	feedTo(len(s.evs))

	c := collect(t, reader)
	local := map[string]string{"class": "local"}
	c.must("neumann.tasks.suspended", nil, 0)
	c.must("neumann.model.calls", map[string]string{"class": "local", "pc": "model", "outcome": "suspended"}, 1)
	c.must("neumann.model.calls", map[string]string{"class": "local", "pc": "model", "outcome": "ok"}, 5)
	c.must("neumann.model.calls", map[string]string{"class": "local", "pc": "model", "outcome": "error"}, 1)
	c.mustHist("neumann.model.duration", map[string]string{"class": "local", "outcome": "suspended"}, 1, 2)
	// r 2s, m's continuation 3s, p 2s, c 1s, b's second 2s (from the resume)
	c.mustHist("neumann.model.duration", map[string]string{"class": "local", "outcome": "ok"}, 5, 2+3+2+1+2)
	c.must("neumann.model.failures", local, 1) // b's transient error, not the suspend
	c.must("neumann.events", map[string]string{"kind": "suspended"}, 4)
	c.must("neumann.events", map[string]string{"kind": "resumed"}, 4)
	c.must("neumann.tasks.terminal", map[string]string{"class": "local", "outcome": "cancelled"}, 1)
	if _, ok := c.num("neumann.model.preemptions", nil); ok {
		t.Error("a suspend counted as a preemption")
	}
}

// suspendedSource is a machine whose Status reports n suspended tasks.
type suspendedSource struct {
	*neumann.Machine
	n int
}

func (s suspendedSource) Status() neumann.Status {
	st := s.Machine.Status()
	st.Suspended = s.n
	return st
}

// With a Source the gauge is Status.Suspended, registered with the others,
// whatever the events this observer read say.
func TestSuspendedGaugeWithASource(t *testing.T) {
	r := newRig(t)
	reader, mp := newReader(t)
	m, err := NewMetrics(suspendedSource{r.m, 3}, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s := suspension()
	cut := slices.IndexFunc(s.evs, func(e proto.Event) bool { return e.TaskID == "m" && e.Kind == proto.EvSuspended })
	feed(m, s.evs[:cut+1]) // the events alone would say 1
	c := collect(t, reader)
	c.must("neumann.tasks.suspended", nil, 3)
	c.must("neumann.tasks.unfinished", nil, 0)
}

// The activity log: suspended and resumed at info, with what the task was
// doing, what it kept, whether the resume was pending, and how long the
// task was suspended; never the kept or answered text.
func TestLogOfSuspension(t *testing.T) {
	s := suspension()
	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=INFO", "msg=suspended", "task=r", "class=local", "was=ready", "at=2026-10-10T12:00:01Z")
	find(t, ls, "level=INFO", "msg=resumed", "task=r", "was=ready", "suspended=4s", "at=2026-10-10T12:00:05Z")
	find(t, ls, "level=INFO", "msg=suspended", "task=m", "was=model", "kept=80")
	find(t, ls, "level=INFO", "msg=resumed", "task=m", "was=model", "suspended=7s")
	find(t, ls, "level=INFO", "msg=resumed", "task=p", "pending=true")
	find(t, ls, "level=INFO", "msg=suspended", "task=c", "was=tools")
	find(t, ls, "level=INFO", "msg=cancelled", "task=c")
	for _, line := range ls {
		if strings.Contains(line, "msg=suspended") && (strings.Contains(line, "kept=0") || strings.Contains(line, "kept_think=0")) {
			t.Errorf("a suspend that kept nothing logs what it kept: %s", line)
		}
		if strings.Contains(line, "task=p") && strings.Contains(line, "msg=resumed") && strings.Contains(line, "suspended=") {
			t.Errorf("a pending resume logs a suspension: %s", line)
		}
	}
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "kept text") {
		t.Errorf("task content in the log:\n%s", buf.String())
	}
}
