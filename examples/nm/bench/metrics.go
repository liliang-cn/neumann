package bench

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/examples/nm/tools"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Metrics of one task, from its committed events (commit times), the
// adapter's own call times and the devices' own call times.
//
//   - WallMS: accepted → the terminal event.
//   - QueueMS: time the task was ready and not running: from a commit
//     that made it ready (accepted, the last result of a tool round, a
//     preemption, a resume, a retry's backoff end) to the model_call
//     commit that started its next call. With free vCores this is the
//     scheduler's and the store's latency.
//   - ModelMS: model_call → the step that committed the result, per call:
//     the provider's latency as the machine sees it.
//   - AdapterMS: the adapter's own time in those calls (no commits).
//   - ToolMS: the commit that dispatched a round (the model step that
//     asked for the calls) → the round's last result, per round.
//   - DeviceMS: the devices' own time in those calls, per round the
//     longest call (calls of a round run in parallel).
//   - SuspendedMS: suspended → resumed.
//   - OverheadMS: WallMS − AdapterMS − DeviceMS − QueueMS − SuspendedMS:
//     what the framework added around the calls (commits, dispatch,
//     wake-ups). It is meaningful only when AdapterExact.
type Metrics struct {
	WallMS      float64 `json:"wall_ms"`
	QueueMS     float64 `json:"queue_ms"`
	ModelMS     float64 `json:"model_ms"`
	AdapterMS   float64 `json:"adapter_ms"`
	ToolMS      float64 `json:"tool_ms"`
	DeviceMS    float64 `json:"device_ms"`
	SuspendedMS float64 `json:"suspended_ms"`
	OverheadMS  float64 `json:"overhead_ms"`
	// AdapterExact: every model call of the task was matched to exactly
	// one adapter call (no other live task had the same request text).
	AdapterExact bool `json:"adapter_exact"`

	ModelCalls   int        `json:"model_calls"`   // model_call events
	Steps        int        `json:"steps"`         // answered calls (usage events)
	ToolCalls    int        `json:"tool_calls"`    // tool_call events
	ToolErrors   int        `json:"tool_errors"`   // tool results that were errors
	ToolRounds   int        `json:"tool_rounds"`   // tools steps
	FirstMS      []int64    `json:"first_ms"`      // per step: request → first streamed piece
	Retries      int        `json:"retries"`       // model_retry events
	RetryClasses []string   `json:"retry_classes"` // their classes
	Throttled    int        `json:"throttled"`     // limiter waits for this task
	Preempted    int        `json:"preempted"`     // times its call was cut off for another task
	PreemptedMS  float64    `json:"preempted_ms"`  // model time lost in preempted calls
	Suspends     int        `json:"suspends"`      // suspended events
	Prefill      int        `json:"prefill"`       // continued calls (prefill or prefill_think)
	Usage        task.Usage `json:"usage"`
	CacheHitRate float64    `json:"cache_hit_rate"` // cached / prompt tokens, percent
	Recovered    int        `json:"recovered"`      // recovered events (restarts)
	Reconcile    int        `json:"reconcile"`      // reconcile_needed events
	Outcome      string     `json:"outcome"`        // done, failed, cancelled, or "" if unfinished
	StopReason   string     `json:"stop_reason,omitempty"`

	// Execution groups: the class the task was accepted in, the one it
	// ended in, every move between them ("fast→strong group_down"), and
	// the usage and model calls of each class it ran in.
	Class        string                `json:"class,omitempty"`
	FinalClass   string                `json:"final_class,omitempty"`
	Migrations   []string              `json:"migrations,omitempty"`
	UsageByClass map[string]task.Usage `json:"usage_by_class,omitempty"`
	CallsByClass map[string]int        `json:"calls_by_class,omitempty"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// Compute derives a task's metrics. fp is the fingerprint of its input
// (app.Fingerprint); shared reports whether another task live at the same
// time had the same input, so adapter calls cannot be told apart.
func Compute(evs []proto.Event, fp string, shared bool, calls []app.CallRecord, toolStats []tools.CallStat) Metrics {
	var m Metrics
	if len(evs) == 0 {
		return m
	}
	var (
		start, end   time.Time
		ready        time.Time // when the task last became ready; zero while running/waiting
		callAt       time.Time // model_call commit of the call in flight
		toolsAt      time.Time // dispatch of the round in flight
		pending      int       // calls of the round without a result yet
		suspAt       time.Time
		modelWindows [][2]time.Time
		roundKeys    []map[string]bool
		curKeys      map[string]bool
	)
	for _, ev := range evs {
		d := decode(ev)
		switch ev.Kind {
		case proto.EvAccepted:
			start, ready = ev.Time, ev.Time
			m.Class, m.FinalClass = d.Class, d.Class
		case proto.EvMigrated:
			m.Migrations = append(m.Migrations, d.From+"→"+d.To+" "+d.Reason)
			m.FinalClass = d.To
		case proto.EvModelCall:
			m.ModelCalls++
			if m.CallsByClass == nil {
				m.CallsByClass = map[string]int{}
			}
			m.CallsByClass[m.FinalClass]++
			if !ready.IsZero() {
				m.QueueMS += ms(ev.Time.Sub(ready))
				ready = time.Time{}
			}
			callAt = ev.Time
		case proto.EvStep:
			switch d.PC {
			case "model", "compact":
				if !callAt.IsZero() {
					m.ModelMS += ms(ev.Time.Sub(callAt))
					modelWindows = append(modelWindows, [2]time.Time{callAt, ev.Time})
				}
				if d.FirstMS != nil {
					m.FirstMS = append(m.FirstMS, *d.FirstMS)
				}
				if d.Prefill > 0 || d.PrefillThink > 0 {
					m.Prefill++
				}
				callAt = time.Time{}
			case "tools":
				// Planned calls dispatched by an instruction of their own
				// (after a restart); normally a model step dispatches its
				// calls in its own commit.
				if !ready.IsZero() {
					m.QueueMS += ms(ev.Time.Sub(ready))
					ready = time.Time{}
				}
				if pending > 0 && toolsAt.IsZero() {
					toolsAt = ev.Time
				}
			}
		case proto.EvUsage:
			m.Steps++
			m.Usage.Add(d.Usage)
			if m.UsageByClass == nil {
				m.UsageByClass = map[string]task.Usage{}
			}
			u := m.UsageByClass[m.FinalClass]
			u.Add(d.Usage)
			m.UsageByClass[m.FinalClass] = u
		case proto.EvToolCall:
			m.ToolCalls++
			if pending == 0 {
				// A new round, dispatched by the commit of the model step
				// that asked for it.
				m.ToolRounds++
				toolsAt, ready = ev.Time, time.Time{}
				curKeys = map[string]bool{}
				roundKeys = append(roundKeys, curKeys)
			}
			pending++
			curKeys[d.CallID] = true
		case proto.EvToolResult, proto.EvToolDenied, proto.EvReconcileNeeded:
			if ev.Kind == proto.EvReconcileNeeded {
				m.Reconcile++
			}
			if d.IsError {
				m.ToolErrors++
			}
			if pending > 0 {
				pending--
				if pending == 0 && !toolsAt.IsZero() {
					m.ToolMS += ms(ev.Time.Sub(toolsAt))
					toolsAt = time.Time{}
					ready = ev.Time
				}
			}
		case proto.EvModelRetry:
			m.Retries++
			m.RetryClasses = append(m.RetryClasses, d.Class)
			if !callAt.IsZero() {
				m.ModelMS += ms(ev.Time.Sub(callAt))
				modelWindows = append(modelWindows, [2]time.Time{callAt, ev.Time})
				callAt = time.Time{}
			}
			ready = ev.Time // backoff counts as queueing
		case proto.EvPreempted:
			m.Preempted++
			m.PreemptedMS += float64(d.RanMS)
			ready = ev.Time
		case proto.EvSuspended:
			m.Suspends++
			if !callAt.IsZero() {
				m.ModelMS += ms(ev.Time.Sub(callAt))
				modelWindows = append(modelWindows, [2]time.Time{callAt, ev.Time})
				callAt = time.Time{}
			}
			if !ready.IsZero() {
				m.QueueMS += ms(ev.Time.Sub(ready))
			}
			ready, suspAt = time.Time{}, ev.Time
		case proto.EvResumed:
			if d.Pending {
				continue
			}
			if !suspAt.IsZero() {
				m.SuspendedMS += ms(ev.Time.Sub(suspAt))
				suspAt = time.Time{}
			}
			ready = ev.Time
		case proto.EvRecovered:
			m.Recovered++
			if !callAt.IsZero() {
				callAt = time.Time{}
			}
			ready = ev.Time
		case proto.EvDone, proto.EvFailed, proto.EvCancelled:
			if m.Outcome == "" {
				end = ev.Time
				m.Outcome, m.StopReason = ev.Kind, d.StopReason
				if !suspAt.IsZero() {
					m.SuspendedMS += ms(ev.Time.Sub(suspAt))
				}
			}
		}
	}
	if !start.IsZero() && !end.IsZero() {
		m.WallMS = ms(end.Sub(start))
	}
	// Adapter time: the calls with this task's fingerprint that started in
	// one of its model windows.
	matched := 0
	for _, w := range modelWindows {
		var best *app.CallRecord
		for i := range calls {
			c := &calls[i]
			if c.Fingerprint != fp || c.Start.Before(w[0].Add(-50*time.Millisecond)) || c.Start.After(w[1]) {
				continue
			}
			if best == nil || c.Start.Before(best.Start) {
				best = c
			}
		}
		if best != nil {
			m.AdapterMS += ms(best.End.Sub(best.Start))
			matched++
		}
	}
	m.AdapterExact = !shared && matched == len(modelWindows)
	// Device time: per round, the longest call of this task in it.
	byRound := map[int]time.Duration{}
	roundOf := func(s tools.CallStat) int {
		// Key is task/call/attempt; the round is found by call id.
		_, rest, _ := strings.Cut(s.Key, "/")
		id, _, _ := strings.Cut(rest, "/")
		for i, keys := range roundKeys {
			if keys[id] {
				return i
			}
		}
		return -1
	}
	if len(evs) > 0 {
		id := evs[0].TaskID
		for _, s := range toolStats {
			if s.Task != id {
				continue
			}
			r := roundOf(s)
			byRound[r] = max(byRound[r], s.Dur)
		}
	}
	for _, d := range byRound {
		m.DeviceMS += ms(d)
	}
	m.CacheHitRate = hitRate(m.Usage.CachedTokens, m.Usage.PromptTokens)
	if m.WallMS > 0 {
		m.OverheadMS = m.WallMS - m.AdapterMS - m.DeviceMS - m.QueueMS - m.SuspendedMS
	}
	return m
}

// evData is the union of the event fields metrics read.
type evData struct {
	PC           string `json:"pc"`
	FirstMS      *int64 `json:"first_ms"`
	Prefill      int    `json:"prefill"`
	PrefillThink int    `json:"prefill_think"`
	CallID       string `json:"call_id"`
	IsError      bool   `json:"is_error"`
	Class        string `json:"class"`
	RanMS        int64  `json:"ran_ms"`
	Pending      bool   `json:"pending"`
	StopReason   string `json:"stop_reason"`
	Task         string `json:"task"`
	From         string `json:"from"`
	To           string `json:"to"`
	Reason       string `json:"reason"`
	task.Usage
}

func decode(ev proto.Event) evData {
	var d evData
	if len(ev.Data) > 0 {
		_ = json.Unmarshal(ev.Data, &d)
	}
	return d
}

// hitRate is cached as a percentage of prompt; 0 when there was no prompt.
func hitRate(cached, prompt int) float64 {
	if prompt <= 0 {
		return 0
	}
	return 100 * float64(cached) / float64(prompt)
}
