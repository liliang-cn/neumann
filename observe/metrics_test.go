package observe

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/liliang-cn/neumann/proto"
)

// collected is one collection from a ManualReader, looked up by name and
// attributes.
type collected struct {
	t  *testing.T
	by map[string]metricdata.Metrics
}

func collect(t *testing.T, r *sdkmetric.ManualReader) collected {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	c := collected{t: t, by: map[string]metricdata.Metrics{}}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			c.by[m.Name] = m
		}
	}
	return c
}

func match(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.Emit() != v {
			return false
		}
	}
	return true
}

// num is the value of a sum or gauge point with these attributes (summed
// over the points that match), and whether any matched.
func (c collected) num(name string, attrs map[string]string) (float64, bool) {
	c.t.Helper()
	m, ok := c.by[name]
	if !ok {
		return 0, false
	}
	var v float64
	found := false
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			if match(p.Attributes, attrs) {
				v, found = v+float64(p.Value), true
			}
		}
	case metricdata.Sum[float64]:
		for _, p := range d.DataPoints {
			if match(p.Attributes, attrs) {
				v, found = v+p.Value, true
			}
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			if match(p.Attributes, attrs) {
				v, found = v+float64(p.Value), true
			}
		}
	case metricdata.Gauge[float64]:
		for _, p := range d.DataPoints {
			if match(p.Attributes, attrs) {
				v, found = v+p.Value, true
			}
		}
	default:
		c.t.Fatalf("%s is a %T, not a sum or gauge", name, m.Data)
	}
	return v, found
}

func (c collected) must(name string, attrs map[string]string, want float64) {
	c.t.Helper()
	got, ok := c.num(name, attrs)
	if !ok || got != want {
		c.t.Errorf("%s%v = %v (found %v), want %v", name, attrs, got, ok, want)
	}
}

// hist is the count and sum of a histogram's points with these attributes.
func (c collected) hist(name string, attrs map[string]string) (uint64, float64) {
	c.t.Helper()
	m, ok := c.by[name]
	if !ok {
		c.t.Fatalf("no metric %s", name)
	}
	d, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		c.t.Fatalf("%s is a %T, not a histogram", name, m.Data)
	}
	var n uint64
	var sum float64
	for _, p := range d.DataPoints {
		if match(p.Attributes, attrs) {
			n += p.Count
			sum += p.Sum
		}
	}
	return n, sum
}

func (c collected) mustHist(name string, attrs map[string]string, count uint64, sum float64) {
	c.t.Helper()
	n, s := c.hist(name, attrs)
	if n != count || s < sum-1e-9 || s > sum+1e-9 {
		c.t.Errorf("%s%v: count %d sum %v, want %d and %v", name, attrs, n, s, count, sum)
	}
}

func newReader(t *testing.T) (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	t.Cleanup(func() { mp.Shutdown(context.Background()) })
	return r, mp
}

// A scripted run, followed live: counts, latencies from event times, token
// sums, and the gauges read from Status and Queue at collection.
func TestMetricsFromAScriptedRun(t *testing.T) {
	r := newRig(t)
	reader, mp := newReader(t)
	m, err := NewMetrics(r.m, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	followed := make(chan int64)
	go func() {
		seq, _ := Follow(ctx, r.m, 0, m)
		followed <- seq
	}()
	r.run(t, ids(3)...)
	last := r.log(t)
	head := last[len(last)-1].Seq
	waitFor(t, func() bool { return m.Seq() == head })
	cancel()
	if seq := <-followed; seq != head {
		t.Fatalf("Follow stopped at %d, want %d", seq, head)
	}

	c := collect(t, reader)
	def := map[string]string{"class": "default"}
	c.must("neumann.tasks.accepted", def, 3)
	c.must("neumann.tasks.terminal", map[string]string{"class": "default", "outcome": "done", "stop_reason": "end_turn"}, 3)
	c.mustHist("neumann.task.duration", map[string]string{"class": "default", "outcome": "done"}, 3, 3*sec(2*modelTime+toolTime))
	c.must("neumann.model.calls", map[string]string{"class": "default", "pc": "model", "outcome": "ok"}, 6)
	c.mustHist("neumann.model.duration", map[string]string{"class": "default", "pc": "model", "outcome": "ok"}, 6, 6*sec(modelTime))
	c.must("neumann.model.tokens", map[string]string{"class": "default", "type": "prompt"}, 60)
	c.must("neumann.model.tokens", map[string]string{"class": "default", "type": "completion"}, 30)
	c.must("neumann.tool.calls", map[string]string{"tool": "util_echo", "outcome": "ok"}, 3)
	c.mustHist("neumann.tool.duration", map[string]string{"tool": "util_echo", "outcome": "ok"}, 3, 3*sec(toolTime))
	c.must("neumann.events", map[string]string{"kind": "accepted"}, 3)
	c.must("neumann.events", map[string]string{"kind": "step"}, 6)
	if _, ok := c.num("neumann.model.failures", nil); ok {
		t.Errorf("failures counted in a run without any")
	}

	// Gauges come from Status and Queue when the reader collects.
	c.must("neumann.observe.seq", nil, float64(head))
	c.must("neumann.tasks.unfinished", nil, 0)
	c.must("neumann.queue.ready", def, 0)
	c.must("neumann.group.target", def, 2)
	c.must("neumann.group.window", def, 2)
	c.must("neumann.group.breaker", map[string]string{"class": "default", "state": "closed"}, 1)
	c.must("neumann.group.breaker", map[string]string{"class": "default", "state": "open"}, 0)
	if live, ok := c.num("neumann.vcores.live", def); !ok || live < 0 || live > 2 {
		t.Errorf("vcores.live %v %v", live, ok)
	}
	c.must("neumann.cache.misses", nil, 0) // the echo tool is not cacheable
	if g, ok := c.num("neumann.process.goroutines", nil); !ok || g < 1 {
		t.Errorf("goroutines %v %v", g, ok)
	}
	if h, ok := c.num("neumann.process.heap", nil); !ok || h <= 0 {
		t.Errorf("heap %v %v", h, ok)
	}
}

// The paths a plain run does not take, from hand-built events: failures by
// class, retries, throttling, the machine events, migrations, reconcile,
// late and cached results, and the cardinality guard.
func TestMetricsForEveryOtherEvent(t *testing.T) {
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{MaxTools: 2})
	if err != nil {
		t.Fatal(err)
	}
	var s script
	s.add(0, proto.EvCoreUp, "", map[string]any{"class": "big", "core": 1, "reason": "demand", "live": 1})
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	// A model call that overloads, backs off 1s, waits 250ms for room, then
	// answers with three tool calls.
	s.add(3*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(3*time.Second, proto.EvModelRetry, "a", map[string]any{"class": "overload", "attempt": 1, "delay_ms": 1000, "wait": "backoff"})
	s.add(5*time.Second, proto.EvThrottled, "", map[string]any{"task": "a", "class": "big", "waited_ms": 250})
	s.add(9*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(9*time.Second, proto.EvUsage, "a", usage(100, 20, 7, 50))
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-1", "tool": "fs_read"})
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-2", "tool": "fs_read"})
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-3", "tool": "web_get"})
	s.add(9*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-4", "tool": "hallucinated_tool"})
	s.add(10*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-1", "is_error": false, "cached": true})
	s.add(12*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-3", "is_error": true})
	s.add(12*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-4", "is_error": true})
	s.add(13*time.Second, proto.EvGroupDown, "", map[string]any{"class": "big", "cause": "transient"})
	s.add(14*time.Second, proto.EvMigrated, "a", map[string]any{"from": "big", "to": "little", "reason": "group_down"})
	s.add(15*time.Second, proto.EvReconcileNeeded, "a", map[string]any{"call_id": "c-2", "tool": "fs_read", "cancelled": true})
	s.add(15*time.Second, proto.EvCancelled, "a", map[string]any{"stop_reason": "cancelled"})
	s.add(15*time.Second, proto.EvSteerDropped, "a", map[string]any{"seq": 3})
	s.add(17*time.Second, proto.EvToolLate, "a", map[string]any{"call_id": "c-2", "is_error": false})
	s.add(18*time.Second, proto.EvGroupUp, "", map[string]any{"class": "big", "reset": true})
	s.add(18*time.Second, proto.EvCoreDown, "", map[string]any{"class": "big", "core": 1, "reason": "idle", "live": 0})
	// A second task, failing on a permanent error after its first call.
	s.add(20*time.Second, proto.EvAccepted, "b", map[string]any{"class": "little"})
	s.add(20*time.Second, proto.EvMigrateRefused, "b", map[string]any{"from": "little", "to": "big", "check": "context"})
	s.add(21*time.Second, proto.EvStep, "b", map[string]any{"pc": "model", "core": 2})
	s.add(21*time.Second, proto.EvFailed, "b", map[string]any{"error": "model: 400 bad request", "stop_reason": "model_error"})
	feed(m, s.evs)

	c := collect(t, reader)
	// The first call failed after 3s; the second ran from the end of its
	// backoff (4s) but started its call when the limiter let it (5s).
	c.mustHist("neumann.model.duration", map[string]string{"class": "big", "outcome": "error"}, 1, 3)
	c.mustHist("neumann.model.duration", map[string]string{"class": "big", "outcome": "ok"}, 1, 4)
	c.must("neumann.model.failures", map[string]string{"class": "big", "error_class": "overload"}, 1)
	c.must("neumann.model.failures", map[string]string{"class": "little", "error_class": "model_error"}, 1)
	c.must("neumann.model.calls", map[string]string{"class": "little", "outcome": "error"}, 1)
	c.must("neumann.model.retries", map[string]string{"class": "big", "error_class": "overload", "wait": "backoff"}, 1)
	c.must("neumann.model.tokens", map[string]string{"class": "big", "type": "prompt"}, 100)
	c.must("neumann.model.tokens", map[string]string{"class": "big", "type": "completion"}, 20)
	c.must("neumann.model.tokens", map[string]string{"class": "big", "type": "reasoning"}, 7)
	c.must("neumann.model.tokens", map[string]string{"class": "big", "type": "cached"}, 50)
	c.mustHist("neumann.limiter.throttled", map[string]string{"class": "big"}, 1, 0.25)

	c.must("neumann.tool.calls", map[string]string{"tool": "fs_read", "outcome": "cached"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "web_get", "outcome": "error"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "fs_read", "outcome": "unknown"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "fs_read", "outcome": "late"}, 1)
	// Two distinct tool names are labelled; the third becomes _other.
	c.must("neumann.tool.calls", map[string]string{"tool": "_other", "outcome": "error"}, 1)
	c.mustHist("neumann.tool.duration", map[string]string{"tool": "web_get", "outcome": "error"}, 1, 3)
	c.mustHist("neumann.tool.duration", map[string]string{"tool": "fs_read", "outcome": "late"}, 1, 8)

	c.must("neumann.tasks.terminal", map[string]string{"class": "little", "outcome": "cancelled", "stop_reason": "cancelled"}, 1)
	c.must("neumann.tasks.terminal", map[string]string{"class": "little", "outcome": "failed", "stop_reason": "model_error"}, 1)
	c.must("neumann.vcore.changes", map[string]string{"class": "big", "direction": "up", "reason": "demand"}, 1)
	c.must("neumann.vcore.changes", map[string]string{"class": "big", "direction": "down", "reason": "idle"}, 1)
	c.must("neumann.group.transitions", map[string]string{"class": "big", "direction": "down", "cause": "transient"}, 1)
	c.must("neumann.group.transitions", map[string]string{"class": "big", "direction": "up", "cause": "reset"}, 1)
	c.must("neumann.migrations", map[string]string{"from": "big", "to": "little", "reason": "group_down"}, 1)
	c.must("neumann.migrations.refused", map[string]string{"from": "little", "to": "big", "check": "context"}, 1)
	c.must("neumann.reconcile.needed", map[string]string{"class": "little", "cancelled": "true"}, 1)
	c.must("neumann.steers.dropped", map[string]string{"class": "little"}, 1)
	if _, ok := c.num("neumann.tasks.unfinished", nil); ok {
		t.Errorf("gauges registered without a source")
	}
	for _, m := range c.by {
		if _, ok := m.Data.(metricdata.Sum[int64]); !ok {
			continue
		}
		for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				if kv.Key == "task" || kv.Key == "task_id" {
					t.Errorf("%s labelled by task", m.Name)
				}
			}
		}
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
