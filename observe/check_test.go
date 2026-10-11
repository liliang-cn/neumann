package observe

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liliang-cn/neumann/proto"
)

// checkScript is three tasks under an answer check (design §9.8):
//
//	a: the first answer rejected and sent back at 2s, the second passed at 4s;
//	b: rejected at 6s, rejected again past the retry at 8s, done check_failed;
//	c: the check timed out at 10s, and the answer passed.
func checkScript() script {
	var s script
	answer := func(at time.Duration, task string, done map[string]any) {
		s.add(at, proto.EvStep, task, map[string]any{"pc": "model", "core": 1})
		s.add(at, proto.EvUsage, task, usage(10, 5, 0, 0))
		s.add(at, proto.EvAnswer, task, map[string]any{"text": "the secret answer", "checked": true})
		s.add(at, proto.EvDone, task, done)
	}
	reject := func(at time.Duration, task string, attempt int, final bool) {
		s.add(at, proto.EvStep, task, map[string]any{"pc": "model", "core": 1})
		s.add(at, proto.EvUsage, task, usage(10, 5, 0, 0))
		d := map[string]any{"reason": "quotes the secret answer", "attempt": attempt}
		if final {
			d["final"] = true
		} else {
			d["feedback"] = "the secret feedback"
		}
		s.add(at, proto.EvAnswerRejected, task, d)
	}
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	reject(2*time.Second, "a", 0, false)
	answer(4*time.Second, "a", map[string]any{"stop_reason": "end_turn", "checked": true, "check_reason": "fine"})
	s.add(5*time.Second, proto.EvAccepted, "b", map[string]any{"class": "big"})
	reject(6*time.Second, "b", 0, false)
	reject(8*time.Second, "b", 1, true)
	s.add(8*time.Second, proto.EvAnswer, "b", map[string]any{"text": "the secret answer", "checked": true, "check_failed": true})
	s.add(8*time.Second, proto.EvDone, "b", map[string]any{"stop_reason": "end_turn", "checked": true, "check_failed": true, "check_reason": "quotes the secret answer"})
	s.add(9*time.Second, proto.EvAccepted, "c", map[string]any{"class": "big"})
	answer(10*time.Second, "c", map[string]any{"stop_reason": "end_turn", "checked": true, "check_error": "check timed out"})
	return s
}

// Every answer the check saw is counted once by its outcome: a rejection
// sent back, one past the retries (failed, not counted again at done), a
// pass and a pass for want of a verdict. Every rejection's step is an
// answered model call.
func TestMetricsOfAnswerChecks(t *testing.T) {
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s := checkScript()
	feed(m, s.evs)
	c := collect(t, reader)
	c.must("neumann.answers.checked", map[string]string{"class": "big", "outcome": "rejected"}, 2)
	c.must("neumann.answers.checked", map[string]string{"class": "big", "outcome": "failed"}, 1)
	c.must("neumann.answers.checked", map[string]string{"class": "big", "outcome": "passed"}, 1)
	c.must("neumann.answers.checked", map[string]string{"class": "big", "outcome": "failed_open"}, 1)
	c.must("neumann.model.calls", map[string]string{"class": "big", "pc": "model", "outcome": "ok"}, 5)
	c.must("neumann.tasks.terminal", map[string]string{"class": "big", "outcome": "done", "stop_reason": "end_turn"}, 3)
}

// A rejection is an event on the task span, with its attempt; no reason,
// feedback or answer reaches a span.
func TestTraceOfAnswerChecks(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	s := checkScript()
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)
	if root := one(t, got, "b", "task"); !reflect.DeepEqual(root.Events, []string{"answer_rejected@6s", "answer_rejected@8s"}) {
		t.Fatalf("task span events %v", root.Events)
	}
	if n := len(byName(got, "b", "model")); n != 2 {
		t.Fatalf("%d model spans", n)
	}
	for _, sp := range got {
		for k, v := range sp.Attrs {
			if strings.Contains(v, "secret") {
				t.Errorf("span %s carries task content in %s", sp.Name, k)
			}
		}
	}
}

// answer_rejected is info, with its attempt and whether it was final; done
// says whether the answer was checked and how; no reason, feedback or
// answer is logged.
func TestLogOfAnswerChecks(t *testing.T) {
	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	s := checkScript()
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=INFO", "msg=answer_rejected", "task=a", "class=big", "attempt=0")
	find(t, ls, "level=INFO", "msg=answer_rejected", "task=b", "attempt=1", "final=true")
	find(t, ls, "level=INFO", "msg=done", "task=a", "checked=true")
	find(t, ls, "level=INFO", "msg=done", "task=b", "checked=true", "check_failed=true")
	find(t, ls, "level=INFO", "msg=done", "task=c", "checked=true", `check_error="check timed out"`)
	if out := buf.String(); strings.Contains(out, "secret") {
		t.Errorf("task content in the log:\n%s", out)
	}
}

// The check's time is not the provider's: a step whose answer the check held
// for 400ms after an instant call ends 400ms before its commit, in the
// duration histogram and in its span.
func TestModelLatencyLeavesTheCheckOut(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	s.add(time.Second, proto.EvModelCall, "a", map[string]any{"pc": "model", "core": 1})
	s.add(time.Second+401*time.Millisecond, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1, "check_ms": 400})
	s.add(time.Second+401*time.Millisecond, proto.EvUsage, "a", usage(10, 5, 0, 0))
	s.add(time.Second+401*time.Millisecond, proto.EvAnswer, "a", map[string]any{"text": "x", "checked": true})
	s.add(time.Second+401*time.Millisecond, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn", "checked": true})

	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	feed(m, s.evs)
	collect(t, reader).mustHist("neumann.model.duration", map[string]string{"class": "big", "pc": "model", "outcome": "ok"}, 1, 0.001)

	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	feed(tr, s.evs)
	tr.Flush(context.Background())
	if sp := one(t, spans(exp), "a", "model"); !sp.Start.Equal(t0.Add(time.Second)) || !sp.End.Equal(t0.Add(time.Second+time.Millisecond)) {
		t.Fatalf("model span %v → %v", sp.Start.Sub(t0), sp.End.Sub(t0))
	}
}
