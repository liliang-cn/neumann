package observe

import (
	"context"
	"errors"
	"runtime/metrics"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// gauges registers the observable instruments: one callback, run by the
// meter provider at each collection, reads Status, Queue and the runtime
// once for all of them. Status and Queue are lock-protected copies; no
// store read, no model call.
func (m *Metrics) gauges(meter metric.Meter) error {
	var errs []error
	var all []metric.Observable
	ig := func(name, unit, desc string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		all = append(all, g)
		return g
	}
	fg := func(name, unit, desc string) metric.Float64ObservableGauge {
		g, err := meter.Float64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		all = append(all, g)
		return g
	}
	ic := func(name, unit, desc string) metric.Int64ObservableCounter {
		c, err := meter.Int64ObservableCounter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		all = append(all, c)
		return c
	}
	seq := ig("neumann.observe.seq", "{event}", "Seq of the last event this observer read.")
	unfinished := ig("neumann.tasks.unfinished", "{task}", "Tasks admitted and not yet terminal.")
	held := ig("neumann.tasks.held", "{task}", "Tasks on a vCore or being completed by a tool batch.")
	awaiting := ig("neumann.tasks.awaiting_approval", "{task}", "Tasks whose tool round waits for an operator's approval (Waiting(Approval)).")
	live := ig("neumann.vcores.live", "{vcore}", "vCores alive, by class.")
	busy := ig("neumann.vcores.busy", "{vcore}", "vCores executing an instruction, by class.")
	window := fg("neumann.group.window", "{request}", "Congestion window W, by class.")
	target := ig("neumann.group.target", "{vcore}", "floor(W) within [Min, Max]: the vCores a group runs at most, by class.")
	epoch := ig("neumann.group.epoch", "{epoch}", "Congestion epoch, by class.")
	breaker := ig("neumann.group.breaker", "1", "1 for the breaker's current state (closed, open, half_open), 0 for the others, by class.")
	failures := ig("neumann.group.failures", "{failure}", "Consecutive transient failures while the breaker is closed, by class.")
	ready := ig("neumann.queue.ready", "{task}", "Tasks in the ready queue, by class.")
	background := ig("neumann.queue.background", "{task}", "Background tasks in the ready queue, by class.")
	waiting := ig("neumann.queue.waiting", "{task}", "Tasks in the wait set, by class.")
	forGroup := ig("neumann.queue.waiting_group", "{task}", "Tasks waiting for their group to come back, by class.")
	forApproval := ig("neumann.queue.waiting_approval", "{task}", "Tasks waiting for an operator's approval, by class.")
	longest := fg("neumann.queue.longest_wait", "s", "How long the oldest ready task has been ready, by class.")
	used := ig("neumann.limiter.used", "{unit}", "What a limiter's window holds, by limiter and dimension.")
	available := ig("neumann.limiter.available", "{unit}", "What a reservation could take now, by limiter and dimension.")
	limit := ig("neumann.limiter.limit", "{unit}", "A limiter's limit per window, by limiter and dimension.")
	queued := ig("neumann.limiter.waiting", "{reservation}", "Reservations queued at a limiter, by limiter.")
	entries := ig("neumann.cache.entries", "{entry}", "Entries in the tool result cache (L2).")
	bytes := ig("neumann.cache.bytes", "By", "Estimated size of the tool result cache.")
	hits := ic("neumann.cache.hits", "{lookup}", "Cache lookups served from L2.")
	misses := ic("neumann.cache.misses", "{lookup}", "Cache lookups L2 could not serve.")
	evictions := ic("neumann.cache.evictions", "{entry}", "Cache entries dropped by a cap.")
	dropped := ic("neumann.deltas.dropped", "{delta}", "Streamed deltas a slow subscriber missed.")
	goroutines := ig("neumann.process.goroutines", "{goroutine}", "Goroutines in the process.")
	heap := ig("neumann.process.heap", "By", "Bytes of live and unswept heap objects.")
	rss := ig("neumann.process.rss", "By", "Resident set size (Linux only).")
	if err := errors.Join(errs...); err != nil {
		return err
	}
	samples := []metrics.Sample{{Name: "/sched/goroutines:goroutines"}, {Name: "/memory/classes/heap/objects:bytes"}}
	var err error
	m.reg, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(seq, m.seq.Load())
		st, q := m.src.Status(), m.src.Queue()
		o.ObserveInt64(unfinished, int64(st.Unfinished))
		o.ObserveInt64(held, int64(st.Held))
		o.ObserveInt64(awaiting, int64(st.Approval))
		for _, g := range st.Groups {
			c := metric.WithAttributes(attribute.String("class", g.Class))
			o.ObserveInt64(live, int64(g.Live), c)
			o.ObserveInt64(busy, int64(g.Busy), c)
			o.ObserveFloat64(window, g.W, c)
			o.ObserveInt64(target, int64(g.Target), c)
			o.ObserveInt64(epoch, int64(g.Epoch), c)
			o.ObserveInt64(failures, int64(g.Failures), c)
			for _, state := range []string{"closed", "open", "half_open"} {
				v := int64(0)
				if g.Breaker == state {
					v = 1
				}
				o.ObserveInt64(breaker, v, metric.WithAttributes(attribute.String("class", g.Class), attribute.String("state", state)))
			}
		}
		for _, cq := range q.Classes {
			c := metric.WithAttributes(attribute.String("class", cq.Class))
			o.ObserveInt64(ready, int64(cq.Ready), c)
			o.ObserveInt64(background, int64(cq.Background), c)
			o.ObserveInt64(waiting, int64(cq.Waiting), c)
			o.ObserveInt64(forGroup, int64(cq.ForGroup), c)
			o.ObserveInt64(forApproval, int64(cq.Approval), c)
			o.ObserveFloat64(longest, cq.Longest.Seconds(), c)
		}
		for _, l := range st.Limiters {
			o.ObserveInt64(queued, int64(l.Waiting), metric.WithAttributes(attribute.String("limiter", l.Name)))
			for dim, ds := range l.Dims {
				c := metric.WithAttributes(attribute.String("limiter", l.Name), attribute.String("dimension", dim))
				o.ObserveInt64(used, ds.Used, c)
				o.ObserveInt64(available, ds.Available, c)
				o.ObserveInt64(limit, ds.Limit, c)
			}
		}
		o.ObserveInt64(entries, int64(st.Cache.Entries))
		o.ObserveInt64(bytes, st.Cache.Bytes)
		o.ObserveInt64(hits, int64(st.Cache.Hits))
		o.ObserveInt64(misses, int64(st.Cache.Misses))
		o.ObserveInt64(evictions, int64(st.Cache.Evictions))
		o.ObserveInt64(dropped, st.DeltasDropped)
		metrics.Read(samples)
		o.ObserveInt64(goroutines, sampleInt(samples[0]))
		o.ObserveInt64(heap, sampleInt(samples[1]))
		if n, ok := residentBytes(); ok {
			o.ObserveInt64(rss, n)
		}
		return nil
	}, all...)
	return err
}

func sampleInt(s metrics.Sample) int64 {
	if s.Value.Kind() == metrics.KindUint64 {
		return int64(s.Value.Uint64())
	}
	return 0
}
