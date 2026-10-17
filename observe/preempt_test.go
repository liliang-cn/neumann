package observe

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// preemption is two tasks of a local group with one backend slot, the way
// the core commits them (design §6.4, §12):
//
//	bg (priority 5): its call goes out at 1s, its first chunk at 2.5s; at 4s
//	   fg preempts it, keeping 120 chars (step and preempted, one commit);
//	   an operator raises it to priority 2 at 6s; its continuation goes out
//	   at 7s with the 120 chars as prefill, answers at 10s.
//	fg (priority 0): accepted at 3s, its call goes out at 4s and answers at
//	   6s, its first chunk at once.
//	s: a steer from the operator reaches the model with the step at 23s,
//	   whose call went out at 21s.
func preemption() script {
	var s script
	s.add(0, proto.EvAccepted, "bg", map[string]any{"class": "local", "priority": 5})
	s.add(time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(3*time.Second, proto.EvAccepted, "fg", map[string]any{"class": "local", "priority": 0})
	s.add(4*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "first_ms": 1500})
	s.add(4*time.Second, proto.EvPreempted, "bg", map[string]any{"by": "fg", "kept": 120, "ran_ms": 3000, "n": 1})
	s.add(4*time.Second, proto.EvModelCall, "fg", map[string]any{"pc": "model", "core": 1})
	s.add(6*time.Second, proto.EvStep, "fg", map[string]any{"pc": "model", "core": 1, "first_ms": 0})
	s.add(6*time.Second, proto.EvUsage, "fg", usage(10, 5, 0, 0))
	s.add(6*time.Second, proto.EvAnswer, "fg", map[string]any{"text": "urgent and private"})
	s.add(6*time.Second, proto.EvDone, "fg", map[string]any{"stop_reason": "end_turn"})
	s.add(6*time.Second, proto.EvPrioritySet, "bg", map[string]any{"priority": 2, "was": 5})
	s.add(7*time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(10*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "first_ms": 200, "prefill": 120})
	s.add(10*time.Second, proto.EvUsage, "bg", usage(40, 30, 0, 0))
	s.add(10*time.Second, proto.EvAnswer, "bg", map[string]any{"text": "kept text and the rest"})
	s.add(10*time.Second, proto.EvDone, "bg", map[string]any{"stop_reason": "end_turn"})
	s.add(20*time.Second, proto.EvAccepted, "s", map[string]any{"class": "local", "priority": 1})
	s.add(21*time.Second, proto.EvModelCall, "s", map[string]any{"pc": "model", "core": 2})
	s.add(23*time.Second, proto.EvSteerDelivered, "s", map[string]any{"seq": 1, "from": "operator"})
	s.add(23*time.Second, proto.EvStep, "s", map[string]any{"pc": "model", "core": 2})
	s.add(23*time.Second, proto.EvUsage, "s", usage(5, 5, 0, 0))
	s.add(23*time.Second, proto.EvDone, "s", map[string]any{"stop_reason": "end_turn"})
	return s
}

// seqOf is the Seq of the n-th (from 0) event of kind for task.
func (s script) seqOf(task, kind string, n int) int64 {
	for _, ev := range s.evs {
		if ev.TaskID == task && ev.Kind == kind {
			if n == 0 {
				return ev.Seq
			}
			n--
		}
	}
	return 0
}

// A preempted call is a model span that ends at its preempted event with
// outcome preempted and no error, keeps what it kept, and links to the task
// that took the backend; its continuation is a second model span carrying
// the prefill. Each streamed step has a first_byte event first_ms after its
// request. The task span notes the preemption and the priority change and
// ends with the priority it had last.
func TestTraceOfAPreemptedAndContinuedTask(t *testing.T) {
	s := preemption()
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)

	root := one(t, got, "bg", "task")
	if root.Status != codes.Unset || root.Attrs["neumann.outcome"] != "done" || root.Attrs["neumann.priority"] != "2" ||
		root.Attrs["neumann.steps"] != "1" || !root.Start.Equal(t0) || !root.End.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("task span %+v", root)
	}
	if want := []string{"preempted@4s", "priority_set@6s"}; !reflect.DeepEqual(root.Events, want) {
		t.Fatalf("task span events %v, want %v", root.Events, want)
	}
	if a := root.EventAttrs[0]; a["neumann.preempt.by"] != "fg" || a["neumann.preempt.kept"] != "120" ||
		a["neumann.preempt.ran_ms"] != "3000" || a["neumann.preempt.n"] != "1" {
		t.Fatalf("preempted event %v", a)
	}
	if a := root.EventAttrs[1]; a["neumann.priority"] != "2" || a["neumann.priority_was"] != "5" {
		t.Fatalf("priority_set event %v", a)
	}

	models := byName(got, "bg", "model")
	if len(models) != 2 {
		t.Fatalf("model spans %+v", models)
	}
	cut, cont := models[0], models[1]
	if cut.ID != spanID("bg", "step", s.seqOf("bg", proto.EvStep, 0)) || cut.Parent != root.ID ||
		!cut.Start.Equal(t0.Add(time.Second)) || !cut.End.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("preempted span %+v", cut)
	}
	if cut.Status != codes.Unset || cut.Attrs["neumann.outcome"] != "preempted" || cut.Attrs["neumann.preempt.kept"] != "120" ||
		cut.Attrs["neumann.preempt.n"] != "1" || cut.Attrs["neumann.error.class"] != "" || cut.Attrs["neumann.prefill"] != "" ||
		cut.Attrs["neumann.tokens.prompt"] != "" {
		t.Fatalf("preempted span %+v", cut)
	}
	if want := []string{TraceID("fg").String() + "/" + TaskSpanID("fg").String()}; !reflect.DeepEqual(cut.Links, want) {
		t.Fatalf("preempted span links %v, want the root of fg %v", cut.Links, want)
	}
	if want := []string{"first_byte@2.5s"}; !reflect.DeepEqual(cut.Events, want) || cut.EventAttrs[0]["neumann.first_ms"] != "1500" {
		t.Fatalf("preempted span events %v %v, want %v", cut.Events, cut.EventAttrs, want)
	}
	if cont.ID != spanID("bg", "step", s.seqOf("bg", proto.EvStep, 1)) || cont.Parent != root.ID ||
		!cont.Start.Equal(t0.Add(7*time.Second)) || !cont.End.Equal(t0.Add(10*time.Second)) ||
		cont.Status != codes.Unset || cont.Attrs["neumann.outcome"] != "ok" || cont.Attrs["neumann.prefill"] != "120" ||
		cont.Attrs["neumann.preempt.kept"] != "" || len(cont.Links) != 0 || cont.Attrs["neumann.tokens.completion"] != "30" {
		t.Fatalf("continued span %+v", cont)
	}
	if want := []string{"first_byte@7.2s"}; !reflect.DeepEqual(cont.Events, want) {
		t.Fatalf("continued span events %v, want %v", cont.Events, want)
	}

	// The preempting task: a first chunk at once is still a first_byte, at
	// the span's start.
	fg := one(t, got, "fg", "model")
	if !fg.Start.Equal(t0.Add(4*time.Second)) || !reflect.DeepEqual(fg.Events, []string{"first_byte@4s"}) || fg.Attrs["neumann.prefill"] != "" {
		t.Fatalf("fg model span %+v", fg)
	}
	if r := one(t, got, "fg", "task"); r.Attrs["neumann.priority"] != "0" || len(r.Events) != 0 {
		t.Fatalf("fg task span %+v", r)
	}
	for _, sp := range got {
		for k, v := range sp.Attrs {
			if strings.Contains(v, "private") || strings.Contains(v, "kept text") {
				t.Errorf("task content in %s %s=%s", sp.Name, k, v)
			}
		}
	}
}

// A steer with a sender that reached the model is an event on the task
// span with its seq and sender; committed at the head of its step's commit,
// it does not move the start of that step's model span off its model_call.
func TestTraceOfADeliveredSteer(t *testing.T) {
	s := preemption()
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)
	root := one(t, got, "s", "task")
	if !reflect.DeepEqual(root.Events, []string{"steer_delivered@23s"}) {
		t.Fatalf("task span events %v", root.Events)
	}
	if a := root.EventAttrs[0]; a["neumann.steer.seq"] != "1" || a["neumann.from"] != "operator" ||
		a["neumann.seq"] != strconv.FormatInt(s.seqOf("s", proto.EvSteerDelivered, 0), 10) {
		t.Fatalf("steer_delivered event %v", a)
	}
	m := one(t, got, "s", "model")
	if !m.Start.Equal(t0.Add(21*time.Second)) || !m.End.Equal(t0.Add(23*time.Second)) || len(m.Events) != 0 {
		t.Fatalf("model span %v → %v %v, want 21s → 23s and no first_byte", m.Start.Sub(t0), m.End.Sub(t0), m.Events)
	}
}

// The same against the core: an operator steers a task while its tool runs;
// the second model step, whose call goes out at 2.5s and answers at 4.5s,
// delivers it. Its span covers that call, the steer is an event on the task
// span, and neumann.steers.delivered counts it.
func TestDeliveredSteerFromTheCore(t *testing.T) {
	clock := sim.NewClock(t0)
	store := mem.NewMemory()
	store.Now = clock.Now
	var m *neumann.Machine
	echo := builtin.New("util").Add(bus.ToolDef{Name: "echo", Description: "echo", Effect: bus.ReadOnly},
		func(ctx context.Context, _ bus.DeviceCall) (string, error) {
			if err := m.SteerWith(ctx, "a", "also this", neumann.SteerOptions{From: "operator"}); err != nil {
				return "", err
			}
			clock.Advance(toolTime)
			return "ok", nil
		})
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
			clock.Advance(modelTime)
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("util_echo"), nil
			}
			return sim.Reply("done"), nil
		}},
		Devices: []bus.Device{echo},
		Store:   store,
		Logf:    t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) })
	r := &rig{m: m, clock: clock, store: store}
	r.run(t, "a")
	evs := r.log(t)
	if !slices.ContainsFunc(evs, func(e proto.Event) bool { return e.Kind == proto.EvSteerDelivered }) {
		t.Fatalf("no steer_delivered in %+v", evs)
	}

	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	reader, mp := newReader(t)
	mt, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(tr, evs)
	feed(mt, evs)
	tr.Flush(context.Background())
	got := spans(exp)
	root := one(t, got, "a", "task")
	if !reflect.DeepEqual(root.Events, []string{"steer_delivered@4.5s"}) || root.EventAttrs[0]["neumann.from"] != "operator" ||
		root.EventAttrs[0]["neumann.steer.seq"] == "" {
		t.Fatalf("task span events %v %v", root.Events, root.EventAttrs)
	}
	models := byName(got, "a", "model")
	if len(models) != 2 {
		t.Fatalf("model spans %+v", models)
	}
	if s := models[1]; !s.Start.Equal(t0.Add(modelTime+toolTime)) || !s.End.Equal(t0.Add(2*modelTime+toolTime)) {
		t.Fatalf("second model span %v → %v, want %v → %v", s.Start.Sub(t0), s.End.Sub(t0), modelTime+toolTime, 2*modelTime+toolTime)
	}
	collect(t, reader).must("neumann.steers.delivered", map[string]string{"class": "default"}, 1)
}

// liveSource hands out its events one at a time, as a machine committing
// them would: Follow catches up, and flushes, after every event.
type liveSource []proto.Event

func (l liveSource) Events(ctx context.Context, from int64) <-chan proto.Event {
	ch := make(chan proto.Event)
	go func() {
		defer close(ch)
		for _, ev := range l {
			if ev.Seq <= from {
				continue
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// A tracer that followed the preemption live, flushing after every event,
// exports field for field what a tracer replaying the log from seq 0 does;
// one resumed mid-way gives the same ids and the priority it learnt.
func TestPreemptionTraceReplays(t *testing.T) {
	s := preemption()
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
	if len(got) != 3+4 {
		t.Fatalf("%d spans, want 3 tasks and 4 model calls: %+v", len(got), got)
	}

	cut := slices.IndexFunc(s.evs, func(e proto.Event) bool { return e.Kind == proto.EvPreempted })
	half := tracetest.NewInMemoryExporter()
	ht := NewTracer(half, TracerOptions{})
	feed(ht, s.evs[cut:])
	ht.Flush(context.Background())
	hs := spans(half)
	root := one(t, hs, "bg", "task")
	if root.Attrs["neumann.partial"] != "true" || root.Attrs["neumann.priority"] != "2" {
		t.Fatalf("resumed task span %+v", root)
	}
	if m := one(t, hs, "bg", "model"); m.ID != byName(got, "bg", "model")[1].ID || m.Attrs["neumann.prefill"] != "120" {
		t.Fatalf("resumed model span %+v", m)
	}
	// Resumed after accepted and before any priority_set: unknown, left out.
	start := slices.IndexFunc(s.evs, func(e proto.Event) bool { return e.TaskID == "s" && e.Kind == proto.EvModelCall })
	part := tracetest.NewInMemoryExporter()
	pt := NewTracer(part, TracerOptions{})
	feed(pt, s.evs[start:])
	pt.Flush(context.Background())
	if r := one(t, spans(part), "s", "task"); r.Attrs["neumann.priority"] != "" {
		t.Fatalf("partial task span with a priority it never saw: %+v", r)
	}
}

// Metrics: a preemption counts once by class and its call is a model call
// of outcome preempted, timed and not a failure; every step that carries
// first_ms is in neumann.model.first_byte, a zero among them; a delivered
// steer counts by class. The preempting task is never a label.
func TestMetricsOfPreemption(t *testing.T) {
	s := preemption()
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(m, s.evs)
	c := collect(t, reader)
	local := map[string]string{"class": "local"}
	c.must("neumann.model.preemptions", local, 1)
	c.must("neumann.model.calls", map[string]string{"class": "local", "pc": "model", "outcome": "preempted"}, 1)
	c.must("neumann.model.calls", map[string]string{"class": "local", "pc": "model", "outcome": "ok"}, 3)
	c.mustHist("neumann.model.duration", map[string]string{"class": "local", "outcome": "preempted"}, 1, 3)
	c.mustHist("neumann.model.duration", map[string]string{"class": "local", "outcome": "ok"}, 3, 3+2+2)
	c.mustHist("neumann.model.first_byte", map[string]string{"class": "local", "pc": "model"}, 3, 1.5+0+0.2)
	c.must("neumann.steers.delivered", local, 1)
	c.must("neumann.events", map[string]string{"kind": "preempted"}, 1)
	c.must("neumann.events", map[string]string{"kind": "priority_set"}, 1)
	if _, ok := c.num("neumann.model.failures", nil); ok {
		t.Error("a preemption counted as a failure")
	}
	if _, ok := c.num("neumann.steers.dropped", nil); ok {
		t.Error("a delivered steer counted as dropped")
	}
	for _, md := range c.by {
		var sets []string
		switch d := md.Data.(type) {
		case metricdata.Sum[int64]:
			for _, p := range d.DataPoints {
				sets = append(sets, p.Attributes.Encoded(nil))
			}
		case metricdata.Histogram[float64]:
			for _, p := range d.DataPoints {
				sets = append(sets, p.Attributes.Encoded(nil))
			}
		}
		for _, set := range sets {
			if strings.Contains(set, "fg") || strings.Contains(set, "bg") {
				t.Errorf("%s labelled by a task: %s", md.Name, set)
			}
		}
	}
}

// The activity log: preempted and priority_set at info with their figures,
// steer_delivered at info with its sender, first_ms and prefill on the
// step's debug line; never the kept or answered text.
func TestLogOfPreemption(t *testing.T) {
	s := preemption()
	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=INFO", "msg=preempted", "task=bg", "class=local", "by=fg", "kept=120", "ran_ms=3000", "n=1", "at=2026-10-10T12:00:04Z")
	find(t, ls, "level=INFO", "msg=priority_set", "task=bg", "class=local", "priority=2", "was=5")
	find(t, ls, "level=INFO", "msg=steer_delivered", "task=s", "from=operator")

	buf.Reset()
	l = NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	feed(l, s.evs)
	find(t, lines(&buf), "level=DEBUG", "msg=step", "task=bg", "first_ms=200", "prefill=120")
	find(t, lines(&buf), "level=DEBUG", "msg=step", "task=fg", "first_ms=0")
	if strings.Contains(buf.String(), "private") || strings.Contains(buf.String(), "kept text") {
		t.Errorf("task content in the log:\n%s", buf.String())
	}
}

// A call preempted while the model was still thinking keeps its thought
// (kept_think, design §6.4) and the next call continues it (prefill_think):
// both reach the spans, the task span's preempted event and the log.
func TestPreemptedThought(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "bg", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(3*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "first_ms": 500})
	s.add(3*time.Second, proto.EvPreempted, "bg", map[string]any{"by": "fg", "kept": 0, "kept_think": 160, "ran_ms": 2000, "n": 1})
	s.add(5*time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(9*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "first_ms": 400, "prefill_think": 160})
	s.add(9*time.Second, proto.EvUsage, "bg", usage(40, 30, 0, 0))
	s.add(9*time.Second, proto.EvDone, "bg", map[string]any{"stop_reason": "end_turn"})

	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)
	root := one(t, got, "bg", "task")
	if a := root.EventAttrs[0]; a["neumann.preempt.kept"] != "0" || a["neumann.preempt.kept_think"] != "160" {
		t.Fatalf("preempted event %v", a)
	}
	models := byName(got, "bg", "model")
	if len(models) != 2 {
		t.Fatalf("model spans %+v", models)
	}
	if cut := models[0]; cut.Attrs["neumann.preempt.kept_think"] != "160" || cut.Attrs["neumann.prefill_think"] != "" {
		t.Fatalf("preempted span %+v", cut)
	}
	if cont := models[1]; cont.Attrs["neumann.prefill_think"] != "160" || cont.Attrs["neumann.prefill"] != "" {
		t.Fatalf("continued span %+v", cont)
	}

	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=INFO", "msg=preempted", "task=bg", "kept=0", "kept_think=160")
	find(t, ls, "level=DEBUG", "msg=step", "task=bg", "prefill_think=160")
}

// A continuation that failed drops the prefix it continued (design §6.4):
// its model_retry says how much, and so does the log line.
func TestFailedContinuationDropsItsPrefix(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "bg", map[string]any{"class": "local", "priority": 1})
	s.add(time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(2*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "prefill_think": 160})
	s.add(2*time.Second, proto.EvModelRetry, "bg", map[string]any{"class": "transient", "attempt": 2, "delay_ms": 2000, "wait": "backoff", "error": "empty", "dropped_think": 160})
	s.add(4*time.Second, proto.EvModelCall, "bg", map[string]any{"pc": "model", "core": 1})
	s.add(4*time.Second, proto.EvStep, "bg", map[string]any{"pc": "model", "core": 1, "prefill": 30})
	s.add(4*time.Second, proto.EvModelRetry, "bg", map[string]any{"class": "transient", "attempt": 3, "delay_ms": 4000, "wait": "backoff", "error": "empty", "dropped": 30})

	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	feed(l, s.evs)
	ls := lines(&buf)
	if line := find(t, ls, "level=WARN", "msg=model_retry", "task=bg", "attempt=2", "dropped_think=160"); strings.Contains(line, "dropped=") {
		t.Errorf("thought drop logged as text: %s", line)
	}
	if line := find(t, ls, "level=WARN", "msg=model_retry", "task=bg", "attempt=3", "dropped=30"); strings.Contains(line, "dropped_think") {
		t.Errorf("text drop logged as thought: %s", line)
	}
}
