// Package observe watches a running machine from the outside, using only
// its public API (Events, EventLog, Status, Queue, Load, Calls), so it
// works for any host:
//
//   - Metrics: OpenTelemetry metrics — counters and histograms from the
//     committed events, gauges read from Status and Queue at collection.
//   - Tracer: OpenTelemetry traces rebuilt from the events, one trace per
//     task, timed by the events' commit times (proto.Event.Time).
//   - Log: an slog activity log, one line per notable event, and
//     LogResources, a periodic resource line.
//   - NewHandler: a read-only http.Handler — /status, /queue, /tasks/{id},
//     /events (server-sent events), and optionally /metrics and
//     /debug/pprof/.
//
// Metrics, Tracer and Log are Observers: Follow feeds them the committed
// events from a seq on, replaying the durable log and then following it
// live. Nothing here runs unless a host attaches it, and nothing here can
// hold the machine up: events are read from the log at the observer's pace,
// gauges are lock-protected copies. A host that saves the seq Follow
// returns (or Metrics.Seq) resumes from it after a restart; Head gives the
// end of the log, for observers that should count only from now on.
//
//	reader, prom, _ := observe.PrometheusReader()
//	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
//	metrics, _ := observe.NewMetrics(m, mp, observe.MetricsOptions{})
//	tracer := observe.NewTracer(otlpExporter, observe.TracerOptions{})
//	from, _ := observe.Head(ctx, m)
//	go observe.Follow(ctx, m, from, metrics, tracer, observe.NewLog(slog.Default()))
//	http.ListenAndServe("127.0.0.1:43517", observe.NewHandler(m, observe.HandlerOptions{Metrics: prom}))
//
// # Metrics
//
// Names are under neumann., with OTel units; the Prometheus exporter turns
// them into neumann_tasks_accepted_total, neumann_model_duration_seconds,
// and so on. Durations are seconds; histograms use buckets from 5ms to 1h.
// Labels never include a task id. Tool names, classes and every other
// label are bounded (MetricsOptions); a value past its bound is "_other".
//
// From events (counters and histograms):
//
//	name                         type       unit         labels                          from
//	neumann.events               counter    {event}      kind                            every event read
//	neumann.tasks.accepted       counter    {task}       class                           accepted
//	neumann.tasks.terminal       counter    {task}       class, outcome, stop_reason     done / failed / cancelled
//	neumann.tasks.recovered      counter    {task}       class, from                     recovered
//	neumann.task.duration        histogram  s            class, outcome                  accepted → terminal
//	neumann.model.calls          counter    {call}       class, pc, outcome              step (pc model, compact) and its result
//	neumann.model.failures       counter    {call}       class, error_class              a step ending in model_retry or failed
//	neumann.model.duration       histogram  s            class, pc, outcome              see below
//	neumann.model.retries        counter    {retry}      class, error_class, wait        model_retry
//	neumann.model.preemptions    counter    {preemption} class                           preempted
//	neumann.model.first_byte     histogram  s            class, pc                       step with first_ms: request → first streamed chunk
//	neumann.model.tokens         counter    {token}      class, type                     usage: prompt, completion, reasoning, cached
//	neumann.tool.calls           counter    {call}       tool, outcome                   tool_result, tool_denied, reconcile_needed, tool_late, task end
//	neumann.tool.duration        histogram  s            tool, outcome                   tool_call → its result
//	neumann.limiter.throttled    histogram  s            class                           throttled (waits over 50ms)
//	neumann.vcore.changes        counter    {change}     class, direction, reason        core_up (up) / core_down (down)
//	neumann.group.transitions    counter    {transition} class, direction, cause         group_down (down, cause) / group_up (up, probe or reset)
//	neumann.migrations           counter    {migration}  from, to, reason                migrated
//	neumann.migrations.refused   counter    {refusal}    from, to, check                 migrate_refused
//	neumann.reconcile.needed     counter    {call}       class, cancelled                reconcile_needed
//	neumann.steers.dropped       counter    {steer}      class                           steer_dropped
//	neumann.steers.delivered     counter    {steer}      class                           steer_delivered
//	neumann.tool.denied          counter    {call}       tool, by                        tool_denied (by policy or operator)
//	neumann.tool.rewritten       counter    {call}       tool                            tool_rewritten
//	neumann.approvals.needed     counter    {call}       class, tool                     approval_needed (first announcement only)
//	neumann.approvals.decided    counter    {call}       class, verdict                  approved, tool_denied by operator, task end
//	neumann.approval.wait        histogram  s            class, verdict                  first approval_needed → its decision
//	neumann.compactions          counter    {chunk}      class                           compacted (one per chunk swapped out)
//	neumann.compaction.rounds    counter    {round}      class                           compacted: rounds swapped out
//	neumann.compaction.tokens    counter    {token}      class                           compacted: tokens_before_est - tokens_after_est
//	neumann.answers.checked      counter    {answer}     class, outcome                  answer_rejected, done of a checked answer
//
// outcome of a model step: ok, error, cancelled, preempted (cut off for a
// more urgent task of its group, design §6.4: not a failure, so not in
// neumann.model.failures; the task runs the step again, continuing the
// kept text), suspended (cut off by an operator's suspend, design §9.9:
// not a failure either; after the resume the task runs the step again,
// continuing the kept text); error_class is the
// model_retry class (rate_limited, quota_exhausted, overload, transient,
// truncated, thought: the model thought and stopped without an answer, ...) or, for a call that failed its task, the stop reason. A
// tool call's outcome: ok, error, cached (served by L2), denied (tool_denied:
// refused by the tool policy or an operator, never run), unknown
// (reconcile_needed, or reported unknown at recovery), resolved, late,
// cancelled (no result when its task ended, a call still held for approval
// among them). A task's: done, failed, cancelled. A held call's verdict
// (design §9.7): allow, edit, deny, or cancelled when its task ended first;
// a restart that announces a held call again neither counts it again nor
// restarts its wait. A final answer the answer check saw (design §9.8):
// passed, failed_open (the check timed out or panicked, and the answer
// passed), rejected (sent back to the model with feedback), or failed
// (rejected past the retries, and kept).
//
// neumann.model.duration: from the model_call event, committed with the
// task's Running state as the request goes out, to the commit of the result:
// the provider's latency, without the ready-queue or limiter wait before it.
// A step whose answer the answer check saw (design §9.8) ends check_ms (on
// its step event) before its commit: the check's time is not the
// provider's.
// A log written before model_call existed falls back to the moment the task
// could run (its previous commit, the end of its backoff, or the throttled
// event that let its call go), which includes those waits. A preempted
// call's duration (outcome preempted, or suspended) is the time it held the
// backend before it was cut off; its continuation is another call, timed
// from its own model_call, so the time a task spends suspended counts in no
// model call (in a log without model_call, the continuation is timed from
// the resume).
//
// neumann.model.first_byte: first_ms of a streamed step, from the request
// to its first chunk, for every outcome. On a local backend it tells the
// time spent queued in the backend and reading the prompt from the time
// spent generating.
//
// Read at collection (gauges, and counters the machine keeps):
//
//	name                          type     unit          labels
//	neumann.observe.seq           gauge    {event}       —                  seq of the last event read
//	neumann.tasks.unfinished      gauge    {task}        —
//	neumann.tasks.held            gauge    {task}        —                  on a vCore or a tool batch
//	neumann.tasks.awaiting_approval gauge  {task}        —                  Status.Approval
//	neumann.tasks.suspended       gauge    {task}        —                  suspended, not yet resumed or ended (see below)
//	neumann.vcores.live           gauge    {vcore}       class
//	neumann.vcores.busy           gauge    {vcore}       class
//	neumann.group.window          gauge    {request}     class              congestion window W
//	neumann.group.target          gauge    {vcore}       class              floor(W) in [Min, Max]
//	neumann.group.epoch           gauge    {epoch}       class
//	neumann.group.breaker         gauge    1             class, state       1 for the current state of closed / open / half_open
//	neumann.group.failures        gauge    {failure}     class              consecutive transient failures
//	neumann.queue.ready           gauge    {task}        class
//	neumann.queue.background      gauge    {task}        class
//	neumann.queue.waiting         gauge    {task}        class
//	neumann.queue.waiting_group   gauge    {task}        class
//	neumann.queue.waiting_approval gauge   {task}        class              Waiting(Approval)
//	neumann.queue.longest_wait    gauge    s             class              oldest ready task's wait
//	neumann.limiter.used          gauge    {unit}        limiter, dimension
//	neumann.limiter.available     gauge    {unit}        limiter, dimension
//	neumann.limiter.limit         gauge    {unit}        limiter, dimension
//	neumann.limiter.waiting       gauge    {reservation} limiter
//	neumann.cache.entries         gauge    {entry}       —
//	neumann.cache.bytes           gauge    By            —
//	neumann.cache.hits            counter  {lookup}      —
//	neumann.cache.misses          counter  {lookup}      —
//	neumann.cache.evictions       counter  {entry}       —
//	neumann.deltas.dropped        counter  {delta}       —
//	neumann.process.goroutines    gauge    {goroutine}   —
//	neumann.process.heap          gauge    By            —                  live and unswept heap objects
//	neumann.process.rss           gauge    By            —                  Linux only
//
// neumann.tasks.suspended (design §9.9) is Status.Suspended. A Metrics
// without a Source has it too, counted from the events it read instead: a
// task's suspended event adds it, its resumed (not a pending one) or its
// terminal event takes it off, so a task suspended before the seq Follow
// began from is not counted there.
//
// # Traces
//
// See Tracer for the spans. Their ids are derived from the task id and the
// Seq of the event that opened each span (TraceID, TaskSpanID), and their
// times are commit times, so replaying the log from seq 0 rebuilds the same
// traces a live Tracer exported.
//
// # Task content
//
// Metrics, traces and the activity log carry no task content. /tasks/{id}
// and /events do: prompts, answers, tool arguments and results. Serve the
// handler only where the operator alone can reach it, or with
// HandlerOptions.Token over an encrypted connection.
package observe
