package observe

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// compaction is a task that compacts once between two model steps, the way
// the core commits it: step (pc compact), usage and compacted in one commit.
func compaction() script {
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "small"})
	s.add(time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(time.Second, proto.EvUsage, "a", usage(100, 10, 0, 0))
	s.add(time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-1", "tool": "dev_echo"})
	s.add(2*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-1"})
	s.add(5*time.Second, proto.EvStep, "a", map[string]any{"pc": "compact", "core": 2})
	s.add(5*time.Second, proto.EvUsage, "a", usage(900, 50, 0, 0))
	s.add(5*time.Second, proto.EvCompacted, "a", map[string]any{
		"rounds": 7, "messages_before": 16, "messages_after": 4, "tokens_before_est": 3300, "tokens_after_est": 900, "chunk": 1, "chunks": 1,
	})
	s.add(6*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(6*time.Second, proto.EvUsage, "a", usage(300, 10, 0, 0))
	s.add(6*time.Second, proto.EvAnswer, "a", map[string]any{"text": "x"})
	s.add(6*time.Second, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn"})
	return s
}

// Metrics count compactions, the rounds they swapped out and the tokens
// they took off, by class; the compaction call is a model call of pc
// compact, timed from the task's previous commit to its result.
func TestMetricsOfACompaction(t *testing.T) {
	s := compaction()
	s.add(9*time.Second, proto.EvCompacted, "b", map[string]any{"rounds": 2, "tokens_before_est": 500, "tokens_after_est": 600})
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(m, s.evs)
	c := collect(t, reader)
	c.must("neumann.compactions", map[string]string{"class": "small"}, 1)
	c.must("neumann.compaction.rounds", map[string]string{"class": "small"}, 7)
	c.must("neumann.compaction.tokens", map[string]string{"class": "small"}, 2400)
	c.must("neumann.compaction.tokens", map[string]string{"class": ""}, 0) // never negative
	c.must("neumann.model.calls", map[string]string{"class": "small", "pc": "compact", "outcome": "ok"}, 1)
	c.must("neumann.model.calls", map[string]string{"class": "small", "pc": "model", "outcome": "ok"}, 2)
	c.mustHist("neumann.model.duration", map[string]string{"pc": "compact"}, 1, sec(3*time.Second))
	c.must("neumann.events", map[string]string{"kind": "compacted"}, 2)
}

// The activity log has a compacted line at info with its figures.
func TestLogOfACompaction(t *testing.T) {
	s := compaction()
	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	feed(l, s.evs)
	find(t, lines(&buf), "level=INFO", "msg=compacted", "task=a", "class=small", "rounds=7", "messages_before=16", "messages_after=4",
		"tokens_before_est=3300", "tokens_after_est=900", "chunk=1", "chunks=1", "at=2026-10-10T12:00:05Z")
}

// A compaction is a "compact" span, child of the task, from the task's
// previous commit to the compaction's commit, carrying its figures and its
// tokens; the task span notes it as an event. A replay gives the same spans.
func TestTraceOfACompaction(t *testing.T) {
	s := compaction()
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)
	root := one(t, got, "a", "task")
	if !reflect.DeepEqual(root.Events, []string{"compacted@5s"}) {
		t.Fatalf("task span events %v", root.Events)
	}
	c := one(t, got, "a", "compact")
	want := map[string]string{
		"neumann.pc": "compact", "neumann.outcome": "ok", "neumann.compact.rounds": "7", "neumann.compact.messages_before": "16",
		"neumann.compact.messages_after": "4", "neumann.compact.tokens_before_est": "3300", "neumann.compact.tokens_after_est": "900",
		"neumann.compact.chunk": "1", "neumann.compact.chunks": "1", "neumann.core": "2",
	}
	for k, v := range want {
		if c.Attrs[k] != v {
			t.Errorf("compact span %s = %q, want %q", k, c.Attrs[k], v)
		}
	}
	if c.Parent != root.ID || c.Status != codes.Unset || !c.Start.Equal(t0.Add(2*time.Second)) || !c.End.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("compact span %+v", c)
	}
	if n := len(byName(got, "a", "model")); n != 2 {
		t.Fatalf("%d model spans", n)
	}
	again := tracetest.NewInMemoryExporter()
	tr2 := NewTracer(again, TracerOptions{})
	feed(tr2, s.evs)
	tr2.Flush(context.Background())
	if !reflect.DeepEqual(spans(again), got) {
		t.Fatal("a replay gives other spans")
	}
}

// A compaction whose result never landed (a cancel committed first: the
// final commit carries its step and usage, then cancelled) is still a
// compact span, ending with the task, and no compaction is counted.
func TestCompactionOvertakenByACancel(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "small"})
	s.add(4*time.Second, proto.EvStep, "a", map[string]any{"pc": "compact", "core": 1})
	s.add(4*time.Second, proto.EvUsage, "a", usage(900, 50, 0, 0))
	s.add(4*time.Second, proto.EvCancelled, "a", map[string]any{"stop_reason": "cancelled"})
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(tr, s.evs)
	feed(m, s.evs)
	tr.Flush(context.Background())
	c := one(t, spans(exp), "a", "compact")
	if c.Attrs["neumann.compact.rounds"] != "" || !c.End.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("compact span %+v", c)
	}
	col := collect(t, reader)
	if _, found := col.num("neumann.compactions", nil); found {
		t.Fatal("a compaction counted that never landed")
	}
	col.must("neumann.model.calls", map[string]string{"pc": "compact"}, 1)
}

// A live machine whose model has a small window compacts a long task; the
// metrics fed from its log count every compacted event, and the tracer
// exports one compact span per compaction call.
func TestCompactionOfALiveMachine(t *testing.T) {
	sm := &sim.Model{Info: &model.Caps{Window: 1024, MaxOut: 64, Tools: true}, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			return sim.Summarize(req), nil
		}
		if sim.Rounds(req) < 12 {
			return sim.CallTools("util_page"), nil
		}
		return sim.Reply("done"), nil
	}}
	page := builtin.New("util").Add(bus.ToolDef{Name: "page", Effect: bus.ReadOnly}, func(context.Context, bus.DeviceCall) (string, error) {
		return strings.Repeat("a long page of text. ", 20), nil
	})
	m, err := neumann.New(neumann.Config{Model: sm, Devices: []bus.Device{page}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	evs := m.Events(ctx, 0)
	if _, err := m.Submit(ctx, proto.Instruction{TaskID: "long", Input: "read twelve pages"}); err != nil {
		t.Fatal(err)
	}
	for ev := range evs {
		if ev.TaskID == "long" && ev.Terminal() {
			break
		}
	}
	log, err := m.EventLog(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	compacted := 0
	for _, ev := range log {
		if ev.Kind == proto.EvCompacted {
			compacted++
		}
	}
	if compacted < 2 {
		t.Fatalf("%d compactions", compacted)
	}
	reader, mp := newReader(t)
	met, err := NewMetrics(m, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(met, log)
	feed(tr, log)
	tr.Flush(context.Background())
	collect(t, reader).must("neumann.compactions", map[string]string{"class": "default"}, float64(compacted))
	cs := byName(spans(exp), "long", "compact")
	if len(cs) != compacted {
		t.Fatalf("%d compact spans for %d compactions", len(cs), compacted)
	}
	for _, c := range cs {
		if c.Attrs["neumann.compact.rounds"] == "" || c.Attrs["neumann.outcome"] != "ok" {
			t.Fatalf("compact span %+v", c)
		}
	}
}
