package observe

import (
	"context"
	"log/slog"
	"runtime/metrics"
	"time"

	"github.com/liliang-cn/neumann/proto"
)

// Log writes one slog line per notable committed event: the message is the
// event kind, the attributes are greppable keys (seq, task, class, and what
// the kind carries), and "at" is the commit time. Levels:
//
//   - info: lifecycle — accepted, done, cancelled, compacted, migrated,
//     recovered, resolved, approved, tool_rewritten, answer_rejected,
//     group_up, core_up, core_down, and kinds this package does not know;
//   - warn: model_retry, throttled, group_down, migrate_refused,
//     steer_dropped, tool_late, tool_denied, and approval_needed (an
//     operator must answer it: Machine.Approve);
//   - error: failed, reconcile_needed;
//   - debug: step, usage, tool_call, tool_result, answer.
//
// Lines carry no task content: no prompts, answers, tool arguments or
// results, nor the reasons a tool policy, an operator or an answer check
// gave (they may quote the arguments or the answer). A failed task's error text is logged; it is the provider's or
// the machine's, not the user's.
type Log struct {
	l  *slog.Logger
	in *interp
}

// NewLog returns a Log writing to l (nil: slog.Default()).
func NewLog(l *slog.Logger) *Log {
	if l == nil {
		l = slog.Default()
	}
	return &Log{l: l, in: newInterp(nil)}
}

// Observe logs ev if it is notable at the logger's level.
func (g *Log) Observe(ev proto.Event) {
	var decided *fact // the approval this event decided
	class, d := g.in.feed(ev, func(f fact) {
		if f.kind == factApprovalEnd {
			decided = &f
		}
	})
	level, ok := levelOf(ev.Kind)
	if !ok || !g.l.Enabled(context.Background(), level) {
		return
	}
	a := []slog.Attr{slog.Int64("seq", ev.Seq)}
	if ev.TaskID != "" {
		a = append(a, slog.String("task", ev.TaskID))
	} else if d.Task != "" {
		a = append(a, slog.String("task", d.Task))
	}
	if class != "" {
		a = append(a, slog.String("class", class))
	}
	str := func(k, v string) {
		if v != "" {
			a = append(a, slog.String(k, v))
		}
	}
	switch ev.Kind {
	case proto.EvAccepted:
		a = append(a, slog.Int("priority", d.Priority))
	case proto.EvStep:
		a = append(a, slog.String("pc", d.PC), slog.Int("core", d.Core))
	case proto.EvUsage:
		a = append(a, slog.Int("prompt_tokens", d.PromptTokens), slog.Int("completion_tokens", d.CompletionTokens),
			slog.Int("reasoning_tokens", d.ReasoningTokens), slog.Int("cached_tokens", d.CachedTokens))
	case proto.EvToolCall, proto.EvToolResult, proto.EvToolLate, proto.EvReconcileNeeded, proto.EvResolved:
		str("call", d.CallID)
		str("tool", d.Tool)
		if d.IsError {
			a = append(a, slog.Bool("is_error", true))
		}
		if d.Cached {
			a = append(a, slog.Bool("cached", true))
		}
		if d.Cancelled {
			a = append(a, slog.Bool("cancelled", true))
		}
	case proto.EvToolDenied, proto.EvToolRewritten, proto.EvApprovalNeeded, proto.EvApproved:
		str("call", d.CallID)
		str("tool", d.Tool)
		str("by", d.By)
		str("device", d.Device)
		str("effect", d.Effect)
		if d.Recovered {
			a = append(a, slog.Bool("recovered", true))
		}
		if f := decided; f != nil {
			a = append(a, slog.String("verdict", f.outcome))
			if s, ok := timed(f.start, f.end); ok {
				a = append(a, slog.Duration("waited", time.Duration(s*float64(time.Second))))
			}
		}
	case proto.EvModelRetry:
		str("error_class", d.Class)
		a = append(a, slog.Int("attempt", d.Attempt), slog.Int64("delay_ms", d.DelayMS))
		str("wait", d.Wait)
		str("error", d.Error)
	case proto.EvThrottled:
		a = append(a, slog.Int64("waited_ms", d.WaitedMS))
	case proto.EvCoreUp, proto.EvCoreDown:
		a = append(a, slog.Int("core", d.Core), slog.String("reason", d.Reason), slog.Int("live", d.Live))
	case proto.EvGroupDown:
		str("cause", d.Cause)
	case proto.EvGroupUp:
		if d.Reset {
			a = append(a, slog.Bool("reset", true))
		}
	case proto.EvMigrated, proto.EvMigrateRefused:
		str("from", d.From)
		str("to", d.To)
		str("reason", d.Reason)
		str("check", d.Check)
	case proto.EvRecovered:
		str("from", d.From)
	case proto.EvCompacted:
		a = append(a, slog.Int("rounds", d.Rounds), slog.Int("messages_before", d.MessagesBefore), slog.Int("messages_after", d.MessagesAfter),
			slog.Int64("tokens_before_est", d.TokensBefore), slog.Int64("tokens_after_est", d.TokensAfter),
			slog.Int("chunk", d.Chunk), slog.Int("chunks", d.Chunks))
	case proto.EvAnswerRejected:
		a = append(a, slog.Int("attempt", d.Attempt))
		if d.Final {
			a = append(a, slog.Bool("final", true))
		}
	case proto.EvDone, proto.EvFailed, proto.EvCancelled:
		str("stop_reason", d.StopReason)
		str("error", d.Error)
		if d.Checked {
			a = append(a, slog.Bool("checked", true))
		}
		if d.CheckFailed {
			a = append(a, slog.Bool("check_failed", true))
		}
		str("check_error", d.CheckError)
	}
	if !ev.Time.IsZero() {
		a = append(a, slog.String("at", ev.Time.Format(time.RFC3339Nano)))
	}
	g.l.LogAttrs(context.Background(), level, ev.Kind, a...)
}

func levelOf(kind string) (slog.Level, bool) {
	switch kind {
	case proto.EvStep, proto.EvUsage, proto.EvToolCall, proto.EvToolResult, proto.EvAnswer:
		return slog.LevelDebug, true
	case proto.EvModelRetry, proto.EvThrottled, proto.EvGroupDown, proto.EvMigrateRefused, proto.EvSteerDropped, proto.EvToolLate,
		proto.EvToolDenied, proto.EvApprovalNeeded:
		return slog.LevelWarn, true
	case proto.EvFailed, proto.EvReconcileNeeded:
		return slog.LevelError, true
	}
	return slog.LevelInfo, true
}

// LogResources writes a "resources" line at info every interval until ctx
// ends: the machine's load (vCores, ready, waiting, held, unfinished tasks,
// groups whose breaker is not closed, cache entries) and the process's
// (goroutines, heap bytes). It reads Status and the runtime only when it
// writes.
func LogResources(ctx context.Context, l *slog.Logger, src Source, every time.Duration) {
	if l == nil {
		l = slog.Default()
	}
	if every <= 0 {
		every = time.Minute
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	samples := []metrics.Sample{{Name: "/sched/goroutines:goroutines"}, {Name: "/memory/classes/heap/objects:bytes"}}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		st := src.Status()
		down := 0
		for _, g := range st.Groups {
			if g.Breaker != "closed" {
				down++
			}
		}
		metrics.Read(samples)
		l.LogAttrs(ctx, slog.LevelInfo, "resources",
			slog.Int("cores", st.Cores), slog.Int("ready", st.Ready), slog.Int("waiting", st.Waiting), slog.Int("held", st.Held),
			slog.Int("unfinished", st.Unfinished), slog.Int("groups_down", down), slog.Int("cache_entries", st.Cache.Entries),
			slog.Int64("goroutines", sampleInt(samples[0])), slog.Int64("heap_bytes", sampleInt(samples[1])))
	}
}
