package observe

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// pruned is a machine with event retention whose tasks a and b have had
// their events pruned, and whose task c's are kept; saved is the seq of a's
// first event, where a reader that stopped early would resume.
func pruned(t *testing.T) (m *neumann.Machine, saved int64) {
	t.Helper()
	clock := sim.NewClock(t0)
	store := mem.NewMemory()
	store.Now = clock.Now
	m, err := neumann.New(neumann.Config{
		Model:          &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store:          store,
		Clock:          clock,
		EventRetention: time.Hour,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) })
	run := func(id string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		evs := m.Events(ctx, 0)
		if _, err := m.Submit(ctx, proto.Instruction{TaskID: id, Input: id}); err != nil {
			t.Fatal(err)
		}
		for ev := range evs {
			if ev.TaskID == id && ev.Terminal() {
				return
			}
		}
		t.Fatalf("task %s did not finish", id)
	}
	has := func(id string) bool {
		log, err := m.EventLog(context.Background(), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range log {
			if e.TaskID == id {
				return true
			}
		}
		return false
	}
	run("a")
	run("b")
	log, _ := m.EventLog(context.Background(), 0, 0)
	for _, e := range log {
		if e.TaskID == "a" {
			saved = e.Seq
			break
		}
	}
	clock.Advance(2 * time.Hour)
	run("c")
	deadline := time.Now().Add(10 * time.Second)
	for has("a") || has("b") {
		if time.Now().After(deadline) {
			t.Fatal("a's and b's events were not pruned")
		}
		clock.Advance(time.Minute) // fires the pruning loop's timer, wherever it is set
		time.Sleep(2 * time.Millisecond)
	}
	if !has("c") {
		t.Fatal("c's events were pruned")
	}
	// The pass that pruned them counts them once its store call returns.
	for m.Status().EventsPruned == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Status counts no pruned events")
		}
		time.Sleep(time.Millisecond)
	}
	return m, saved
}

// /events from a seq whose next events were pruned says so once, with a
// "pruned" event that has no id, then streams what is kept.
func TestHandlerMarksAPrunedGap(t *testing.T) {
	m, saved := pruned(t)
	kept, err := m.EventLog(context.Background(), saved, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHandler(m, HandlerOptions{}))
	t.Cleanup(srv.Close) // after the streams' bodies are closed

	s := openSSE(t, srv, "/events?from_seq="+strconv.FormatInt(saved, 10))
	e := s.next()
	var gap struct {
		After int64 `json:"after_seq"`
		Next  int64 `json:"next_seq"`
	}
	if e.event != "pruned" || e.id != "" || json.Unmarshal([]byte(e.data), &gap) != nil || gap.After != saved || gap.Next != kept[0].Seq {
		t.Fatalf("first event %+v, want pruned after %d up to %d", e, saved, kept[0].Seq)
	}
	for _, want := range kept {
		if e := s.next(); e.event != want.Kind || e.ev.Seq != want.Seq {
			t.Fatalf("streamed %+v, want seq %d %s", e, want.Seq, want.Kind)
		}
	}
	// From 0 the client asked for whatever is kept: no marker.
	s = openSSE(t, srv, "/events?from_seq=0")
	all, _ := m.EventLog(context.Background(), 0, 0)
	if e := s.next(); e.event == "pruned" || e.ev.Seq != all[0].Seq {
		t.Fatalf("from 0: first event %+v, want seq %d", e, all[0].Seq)
	}
}

// Follow from a pruned seq feeds the kept events after it and returns the
// last seq; Head finds the end of a pruned log; Metrics counts what was
// pruned.
func TestFollowHeadAndMetricsOnAPrunedLog(t *testing.T) {
	m, saved := pruned(t)
	reader, prom, err := PrometheusReader()
	if err != nil {
		t.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	if _, err := NewMetrics(m, mp, MetricsOptions{}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHandler(m, HandlerOptions{Metrics: prom}))
	t.Cleanup(srv.Close)
	_, b := get(t, srv, "/metrics")
	p := expfmt.NewTextParser(prommodel.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(string(b)))
	if err != nil {
		t.Fatalf("exposition does not parse: %v", err)
	}
	f := fams["neumann_events_pruned_total"]
	if f == nil || f.GetMetric()[0].GetCounter().GetValue() != float64(m.Status().EventsPruned) || m.Status().EventsPruned == 0 {
		t.Fatalf("neumann_events_pruned_total %v, status %d", f, m.Status().EventsPruned)
	}

	m.Stop(context.Background())
	kept, _ := m.EventLog(context.Background(), saved, 0)
	last := kept[len(kept)-1].Seq
	if n, err := Head(context.Background(), m); err != nil || n != last {
		t.Fatalf("Head %d %v, want %d", n, err, last)
	}
	var seen []int64
	n, err := Follow(context.Background(), m, saved, ObserverFunc(func(ev proto.Event) { seen = append(seen, ev.Seq) }))
	if err != nil || n != last || len(seen) != len(kept) || seen[0] != kept[0].Seq {
		t.Fatalf("Follow returned %d %v after %v, want the %d kept events through %d", n, err, seen, len(kept), last)
	}
}
