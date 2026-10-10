package observe

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// ScopeName is the instrumentation scope of observe's metrics and spans.
const ScopeName = "github.com/liliang-cn/neumann/observe"

// MetricsOptions bound the label values Metrics creates. A label value past
// its bound is reported as "_other", so a model that invents tool names, or
// a cancel reason that carries free text, cannot grow the series without
// limit. Metrics never labels by task id.
type MetricsOptions struct {
	MaxTools   int // distinct tool names; 0 = 64
	MaxClasses int // distinct execution groups; 0 = 32
	MaxValues  int // distinct values of any other label; 0 = 64
}

// Other is the label value that stands for every value past a bound.
const Other = "_other"

// durationBuckets suit everything from a cached tool result to a long task.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}

// Metrics is OpenTelemetry metrics for one machine: counters and histograms
// from the committed events it is fed (an Observer: run it under Follow),
// and gauges read from Status and Queue each time the meter provider
// collects. It costs nothing until it is fed or collected. Every metric is
// listed in the package documentation.
type Metrics struct {
	src    Source
	in     *interp
	seq    atomic.Int64
	reg    metric.Registration
	labels map[string]*bounded
	o      MetricsOptions

	events, accepted, terminal, recovered      metric.Int64Counter
	modelCalls, modelFailures, retries, tokens metric.Int64Counter
	toolCalls                                  metric.Int64Counter
	vcores, transitions, migrations, refused   metric.Int64Counter
	reconcile, steers                          metric.Int64Counter
	taskDur, modelDur, toolDur, throttle       metric.Float64Histogram
}

// NewMetrics creates the instruments on mp's meter. src gives the gauges
// and the class of a task first seen mid-way through the log; nil leaves
// the gauges out and such tasks unlabelled.
func NewMetrics(src Source, mp metric.MeterProvider, o MetricsOptions) (*Metrics, error) {
	if o.MaxTools <= 0 {
		o.MaxTools = 64
	}
	if o.MaxClasses <= 0 {
		o.MaxClasses = 32
	}
	if o.MaxValues <= 0 {
		o.MaxValues = 64
	}
	m := &Metrics{src: src, o: o, labels: map[string]*bounded{}}
	var lookup func(string) string
	if src != nil {
		lookup = func(id string) string {
			t, err := src.Load(context.Background(), id)
			if err != nil {
				return ""
			}
			return t.Class
		}
	}
	m.in = newInterp(lookup)
	meter := mp.Meter(ScopeName)
	var errs []error
	counter := func(name, unit, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		return c
	}
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(durationBuckets...))
		errs = append(errs, err)
		return h
	}
	m.events = counter("neumann.events", "{event}", "Committed events read, by kind.")
	m.accepted = counter("neumann.tasks.accepted", "{task}", "Tasks accepted, by class.")
	m.terminal = counter("neumann.tasks.terminal", "{task}", "Tasks ended, by class, outcome (done, failed, cancelled) and stop reason.")
	m.recovered = counter("neumann.tasks.recovered", "{task}", "Tasks recovered at a restart, by class and the state they were found in.")
	m.modelCalls = counter("neumann.model.calls", "{call}", "Model instructions, by class, pc (model, compact) and outcome (ok, error, cancelled).")
	m.modelFailures = counter("neumann.model.failures", "{call}", "Model instructions that failed, by class and error class.")
	m.retries = counter("neumann.model.retries", "{retry}", "Model retries decided, by class, error class and what the task waits for.")
	m.tokens = counter("neumann.model.tokens", "{token}", "Confirmed tokens, by class and type (prompt, completion, reasoning, cached).")
	m.toolCalls = counter("neumann.tool.calls", "{call}", "Tool calls ended, by tool and outcome.")
	m.vcores = counter("neumann.vcore.changes", "{change}", "vCores started (up) and retired (down), by class and reason.")
	m.transitions = counter("neumann.group.transitions", "{transition}", "Breakers opening (down) and closing (up), by class and cause.")
	m.migrations = counter("neumann.migrations", "{migration}", "Tasks moved to another group, by from, to and reason.")
	m.refused = counter("neumann.migrations.refused", "{refusal}", "Migrations refused by the capability check, by from, to and check.")
	m.reconcile = counter("neumann.reconcile.needed", "{call}", "Calls of unknown outcome held for an operator, by class and whether the task was cancelled.")
	m.steers = counter("neumann.steers.dropped", "{steer}", "Steers never delivered because the task ended, by class.")
	m.taskDur = hist("neumann.task.duration", "Task lifetime, accepted to terminal, by class and outcome.")
	m.modelDur = hist("neumann.model.duration", "Model instruction latency, from the moment the task could run (its previous commit, after any backoff, or when the limiters let its call go) to the commit of the step's result, by class, pc and outcome.")
	m.toolDur = hist("neumann.tool.duration", "Tool call latency, tool_call to its result, by tool and outcome.")
	m.throttle = hist("neumann.limiter.throttled", "Waits for room in the limiters longer than 50ms, by class.")
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if src != nil {
		if err := m.gauges(meter); err != nil {
			return nil, err
		}
	} else {
		g, err := meter.Int64ObservableGauge("neumann.observe.seq", metric.WithUnit("{event}"),
			metric.WithDescription("Seq of the last event this observer read."))
		if err != nil {
			return nil, err
		}
		m.reg, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(g, m.seq.Load())
			return nil
		}, g)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Seq is the Seq of the last event observed.
func (m *Metrics) Seq() int64 { return m.seq.Load() }

// Close unregisters the gauges.
func (m *Metrics) Close() error {
	if m.reg == nil {
		return nil
	}
	return m.reg.Unregister()
}

var bg = context.Background()

func (m *Metrics) label(key, v string) attribute.KeyValue {
	b := m.labels[key]
	if b == nil {
		limit := m.o.MaxValues
		switch key {
		case "tool":
			limit = m.o.MaxTools
		case "class", "from", "to":
			limit = m.o.MaxClasses
		}
		b = &bounded{max: limit, seen: map[string]bool{}}
		m.labels[key] = b
	}
	return attribute.String(key, b.get(v))
}

func (m *Metrics) attrs(kv ...string) metric.MeasurementOption {
	out := make([]attribute.KeyValue, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, m.label(kv[i], kv[i+1]))
	}
	return metric.WithAttributes(out...)
}

// Observe counts ev. Feed events in Seq order, from one goroutine.
func (m *Metrics) Observe(ev proto.Event) {
	class, d := m.in.feed(ev, m.fact)
	m.events.Add(bg, 1, m.attrs("kind", ev.Kind))
	switch ev.Kind {
	case proto.EvAccepted:
		m.accepted.Add(bg, 1, m.attrs("class", class))
	case proto.EvUsage:
		for _, t := range []struct {
			name string
			n    int
		}{{"prompt", d.PromptTokens}, {"completion", d.CompletionTokens}, {"reasoning", d.ReasoningTokens}, {"cached", d.CachedTokens}} {
			m.tokens.Add(bg, int64(t.n), m.attrs("class", class, "type", t.name))
		}
	case proto.EvModelRetry:
		m.retries.Add(bg, 1, m.attrs("class", class, "error_class", d.Class, "wait", d.Wait))
	case proto.EvThrottled:
		m.throttle.Record(bg, float64(d.WaitedMS)/1000, m.attrs("class", d.Class))
	case proto.EvCoreUp, proto.EvCoreDown:
		dir := "up"
		if ev.Kind == proto.EvCoreDown {
			dir = "down"
		}
		m.vcores.Add(bg, 1, m.attrs("class", d.Class, "direction", dir, "reason", d.Reason))
	case proto.EvGroupDown:
		m.transitions.Add(bg, 1, m.attrs("class", d.Class, "direction", "down", "cause", d.Cause))
	case proto.EvGroupUp:
		cause := "probe"
		if d.Reset {
			cause = "reset"
		}
		m.transitions.Add(bg, 1, m.attrs("class", d.Class, "direction", "up", "cause", cause))
	case proto.EvMigrated:
		m.migrations.Add(bg, 1, m.attrs("from", d.From, "to", d.To, "reason", d.Reason))
	case proto.EvMigrateRefused:
		m.refused.Add(bg, 1, m.attrs("from", d.From, "to", d.To, "check", d.Check))
	case proto.EvReconcileNeeded:
		m.reconcile.Add(bg, 1, m.attrs("class", class, "cancelled", strconv.FormatBool(d.Cancelled)))
	case proto.EvSteerDropped:
		m.steers.Add(bg, 1, m.attrs("class", class))
	case proto.EvRecovered:
		m.recovered.Add(bg, 1, m.attrs("class", class, "from", d.From))
	}
	m.seq.Store(ev.Seq)
}

func timed(start, end time.Time) (float64, bool) {
	if start.IsZero() || end.IsZero() {
		return 0, false // a store that kept no times: nothing to measure
	}
	return max(end.Sub(start), 0).Seconds(), true
}

func (m *Metrics) fact(f fact) {
	switch f.kind {
	case factTaskEnd:
		reason := f.d.StopReason
		if reason == "" && f.outcome == "failed" {
			reason = "error" // a panic's failure carries its text, not a reason
		}
		m.terminal.Add(bg, 1, m.attrs("class", f.class, "outcome", f.outcome, "stop_reason", reason))
		if s, ok := timed(f.start, f.end); ok && !f.partial {
			m.taskDur.Record(bg, s, m.attrs("class", f.class, "outcome", f.outcome))
		}
	case factStep:
		if f.name != task.StepModel.String() && f.name != task.StepCompact.String() {
			return
		}
		m.modelCalls.Add(bg, 1, m.attrs("class", f.class, "pc", f.name, "outcome", f.outcome))
		if f.outcome == outError {
			m.modelFailures.Add(bg, 1, m.attrs("class", f.class, "error_class", f.errClass))
		}
		if s, ok := timed(f.start, f.end); ok {
			m.modelDur.Record(bg, s, m.attrs("class", f.class, "pc", f.name, "outcome", f.outcome))
		}
	case factToolEnd:
		tool := f.name
		if tool == "" {
			tool = "_unknown"
		}
		m.toolCalls.Add(bg, 1, m.attrs("tool", tool, "outcome", f.outcome))
		if s, ok := timed(f.start, f.end); ok && f.timed {
			m.toolDur.Record(bg, s, m.attrs("tool", tool, "outcome", f.outcome))
		}
	}
}

// bounded admits the first max distinct values of a label and maps every
// later one to Other.
type bounded struct {
	max  int
	seen map[string]bool
}

func (b *bounded) get(v string) string {
	if b.seen[v] {
		return v
	}
	if len(b.seen) >= b.max {
		return Other
	}
	b.seen[v] = true
	return v
}
