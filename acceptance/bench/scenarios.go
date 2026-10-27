package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/proto"
)

// solo runs each selected task alone, one after another.
func (b *Bench) solo(ctx context.Context, rep *Report) error {
	s, err := b.start(ctx, 0)
	if err != nil {
		return err
	}
	defer s.close()
	for _, t := range b.tasks {
		inst, err := s.NewInstance(t, 0)
		if err != nil {
			return err
		}
		if err := s.Submit(ctx, inst); err != nil {
			return err
		}
		r, err := s.Finish(ctx, inst)
		if err != nil {
			return err
		}
		rep.Results = append(rep.Results, r)
	}
	rep.summarize()
	return nil
}

// mixed starts the background tasks at priority 2, then submits an
// interactive probe (priority 0) every -every; it compares the probes'
// latency with their solo latency.
func (b *Bench) mixed(ctx context.Context, rep *Report) error {
	nProbes := b.Opt.Probes
	if nProbes <= 0 {
		nProbes = len(b.probes)
	}
	solo := map[string]float64{}
	if b.Opt.Baseline != "" {
		data, err := os.ReadFile(b.Opt.Baseline)
		if err != nil {
			return err
		}
		var base Report
		if err := json.Unmarshal(data, &base); err != nil {
			return fmt.Errorf("baseline: %v", err)
		}
		for _, r := range base.Results {
			if r.Pass || solo[r.TaskID] == 0 {
				solo[r.TaskID] = r.WallMS
			}
		}
	}
	s, err := b.start(ctx, 0)
	if err != nil {
		return err
	}
	defer s.close()
	probeTask := func(i int) *Task { return b.probes[i%len(b.probes)] }
	var soloRes []*Result
	for i := 0; i < nProbes && i < len(b.probes); i++ {
		t := probeTask(i)
		if _, ok := solo[t.ID]; ok {
			continue
		}
		b.logf("mixed: solo baseline for %s", t.ID)
		inst, err := s.NewInstance(t, 0)
		if err != nil {
			return err
		}
		if err := s.Submit(ctx, inst); err != nil {
			return err
		}
		r, err := s.Finish(ctx, inst)
		if err != nil {
			return err
		}
		r.Extra = map[string]any{"phase": "solo-baseline"}
		soloRes = append(soloRes, r)
		solo[t.ID] = r.WallMS
	}
	rep.Results = append(rep.Results, soloRes...)

	b.logf("mixed: %d background tasks at priority 2, then %d probes every %s", len(b.bg), nProbes, b.Opt.Every)
	var bg []*Instance
	for _, t := range b.bg {
		inst, err := s.NewInstance(t, 2)
		if err != nil {
			return err
		}
		bg = append(bg, inst)
	}
	bgStart := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var bgRes, probeRes []*Result
	var errs []error
	collect := func(inst *Instance, into *[]*Result, phase string) {
		defer wg.Done()
		r, err := s.Finish(ctx, inst)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			errs = append(errs, err)
			return
		}
		r.Extra = map[string]any{"phase": phase}
		*into = append(*into, r)
	}
	for _, inst := range bg {
		if err := s.Submit(ctx, inst); err != nil {
			return err
		}
		wg.Add(1)
		go collect(inst, &bgRes, "background")
	}
	for i := range nProbes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.Opt.Every):
		}
		inst, err := s.NewInstance(probeTask(i), 0)
		if err != nil {
			return err
		}
		if err := s.Submit(ctx, inst); err != nil {
			return err
		}
		wg.Add(1)
		go collect(inst, &probeRes, "probe")
	}
	wg.Wait()
	bgEnd := time.Now()
	rep.Results = append(rep.Results, bgRes...)
	rep.Results = append(rep.Results, probeRes...)
	rep.summarize()
	var mixedLat, soloLat, e2eLat []float64
	type probeRow struct {
		Task    string  `json:"task"`
		Class   string  `json:"class,omitempty"`
		MixedMS float64 `json:"mixed_ms"`
		RouteMS float64 `json:"route_ms,omitempty"`
		SoloMS  float64 `json:"solo_ms"`
		QueueMS float64 `json:"queue_ms"`
		Pass    bool    `json:"pass"`
	}
	var rows []probeRow
	for _, r := range probeRes {
		mixedLat = append(mixedLat, r.WallMS)
		e2eLat = append(e2eLat, r.RouteMS+r.WallMS)
		soloLat = append(soloLat, solo[r.TaskID])
		rows = append(rows, probeRow{r.TaskID, r.Class, r.WallMS, r.RouteMS, solo[r.TaskID], r.QueueMS, r.Pass})
	}
	preempts, bgPass := 0, 0
	for _, r := range bgRes {
		preempts += r.Preempted
		if r.Pass {
			bgPass++
		}
	}
	for _, r := range probeRes {
		preempts += r.Preempted
	}
	rep.Summary["probes"] = rows
	rep.Summary["probe_mixed_p50_ms"] = percentile(mixedLat, 0.5)
	rep.Summary["probe_mixed_p95_ms"] = percentile(mixedLat, 0.95)
	rep.Summary["probe_mixed_max_ms"] = percentile(mixedLat, 1)
	// What the user waits for: the routing decision, then the task.
	rep.Summary["probe_e2e_p50_ms"] = percentile(e2eLat, 0.5)
	rep.Summary["probe_e2e_p95_ms"] = percentile(e2eLat, 0.95)
	probePass := 0
	for _, r := range probeRes {
		if r.Pass {
			probePass++
		}
	}
	rep.Summary["probes_passed"] = fmt.Sprintf("%d/%d", probePass, len(probeRes))
	rep.Summary["probe_solo_p50_ms"] = percentile(soloLat, 0.5)
	rep.Summary["probe_solo_p95_ms"] = percentile(soloLat, 0.95)
	rep.Summary["probe_solo_max_ms"] = percentile(soloLat, 1)
	rep.Summary["preemptions"] = preempts
	rep.Summary["background_passed"] = fmt.Sprintf("%d/%d", bgPass, len(bgRes))
	rep.Summary["background_wall_s"] = bgEnd.Sub(bgStart).Seconds()
	return errors.Join(errs...)
}

// burst submits K tasks (the selected mix, cycled) at once and samples the
// machine's status while they run.
func (b *Bench) burst(ctx context.Context, rep *Report) error {
	if b.Cfg.Backend == "local" {
		rep.Notes = append(rep.Notes, "burst is meant for a cloud backend")
	}
	s, err := b.start(ctx, 0)
	if err != nil {
		return err
	}
	defer s.close()
	var insts []*Instance
	for i := range b.Opt.K {
		t := b.tasks[i%len(b.tasks)]
		prio := 2
		if t.Quick {
			prio = 0
		}
		inst, err := s.NewInstance(t, prio)
		if err != nil {
			return err
		}
		insts = append(insts, inst)
	}
	type sample struct {
		At      float64 `json:"t_s"`
		W       float64 `json:"w"`
		Target  int     `json:"target"`
		Live    int     `json:"live"`
		Busy    int     `json:"busy"`
		Ready   int     `json:"ready"`
		Waiting int     `json:"waiting"`
		Breaker string  `json:"breaker"`
	}
	var samples []sample
	sctx, stop := context.WithCancel(ctx)
	start := time.Now()
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			st := s.A.M.Status()
			for _, g := range st.Groups {
				samples = append(samples, sample{time.Since(start).Seconds(), g.W, g.Target, g.Live, g.Busy, g.Ready, g.Waiting, g.Breaker})
			}
			select {
			case <-sctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	res, err := s.RunAll(ctx, insts)
	end := time.Now()
	stop()
	<-sampled
	rep.Results = res
	rep.summarize()
	var lat []float64
	done, passed, retries := 0, 0, 0
	classes := map[string]int{}
	var last time.Time
	for _, r := range res {
		lat = append(lat, r.WallMS)
		if r.Outcome == proto.EvDone {
			done++
		}
		if r.Pass {
			passed++
		}
		retries += r.Retries
		for _, c := range r.RetryClasses {
			classes[c]++
		}
		if e := r.Submitted.Add(time.Duration(r.WallMS * float64(time.Millisecond))); e.After(last) {
			last = e
		}
	}
	machine := map[string]int{}
	for _, ev := range s.Tr.Machine() {
		machine[ev.Kind]++
	}
	wMin, wMax := 1e9, 0.0
	breakers := map[string]int{}
	for _, sm := range samples {
		wMin, wMax = min(wMin, sm.W), max(wMax, sm.W)
		breakers[sm.Breaker]++
	}
	span := end.Sub(start).Minutes()
	rep.Summary["k"] = b.Opt.K
	rep.Summary["span_s"] = end.Sub(start).Seconds()
	rep.Summary["throughput_tasks_per_min"] = float64(done) / span
	rep.Summary["latency_p50_ms"] = percentile(lat, 0.5)
	rep.Summary["latency_p95_ms"] = percentile(lat, 0.95)
	rep.Summary["latency_max_ms"] = percentile(lat, 1)
	rep.Summary["done"] = done
	rep.Summary["passed"] = passed
	rep.Summary["retries"] = retries
	rep.Summary["retry_classes"] = classes
	rep.Summary["machine_events"] = machine
	rep.Summary["window_min"] = wMin
	rep.Summary["window_max"] = wMax
	rep.Summary["breaker_samples"] = breakers
	rep.Summary["status_samples"] = samples
	return err
}

// ops exercises the operator controls on three tasks at once: one is
// suspended mid model call (after its first streamed piece) and resumed;
// one is steered with an extra question after its first tool result; one
// is cancelled at its first tool call.
func (b *Bench) ops(ctx context.Context, rep *Report) error {
	s, err := b.start(ctx, 0)
	if err != nil {
		return err
	}
	defer s.close()
	pick := func(id string) *Task {
		for _, t := range b.tasks {
			if t.ID == id {
				return t
			}
		}
		return TaskByID(id)
	}
	susT, steerT, cancelT := pick("log-triage"), pick("csv-stats"), pick("changelog")
	sus, err := s.NewInstance(susT, 1)
	if err != nil {
		return err
	}
	st, err := s.NewInstance(steerT, 1)
	if err != nil {
		return err
	}
	cn, err := s.NewInstance(cancelT, 1)
	if err != nil {
		return err
	}
	steerText := "Also tell me how many Q3 orders that region had."

	type opLog struct {
		mu                   sync.Mutex
		suspendedAt          time.Time
		resumedAt            time.Time
		modelCallsSuspended  int // model_call events of the suspended task while suspended
		statusSuspended      int
		steeredAt            time.Time
		cancelReturned       time.Time
		toolCallsAfterCancel int
		devCallsAfterCancel  int
		errs                 []string
	}
	ol := &opLog{}
	note := func(f string, a ...any) {
		ol.mu.Lock()
		ol.errs = append(ol.errs, fmt.Sprintf(f, a...))
		ol.mu.Unlock()
		b.logf("ops: "+f, a...)
	}

	// Suspend after the first streamed piece of a model call.
	deltas := s.A.M.Deltas(ctx)
	suspendOnce, steerOnce, cancelOnce := sync.Once{}, sync.Once{}, sync.Once{}
	var opsWG sync.WaitGroup
	go func() {
		for d := range deltas {
			if d.TaskID != sus.ID {
				continue
			}
			suspendOnce.Do(func() {
				opsWG.Add(1)
				go func() {
					defer opsWG.Done()
					if err := s.A.M.Suspend(ctx, sus.ID); err != nil {
						note("suspend: %v", err)
						return
					}
					ol.mu.Lock()
					ol.suspendedAt = time.Now()
					ol.mu.Unlock()
					b.logf("ops: suspended %s after its first streamed piece; resuming in %s", sus.ID, b.Opt.Suspend)
					time.Sleep(b.Opt.Suspend / 2)
					ol.mu.Lock()
					ol.statusSuspended = s.A.M.Status().Suspended
					ol.mu.Unlock()
					time.Sleep(b.Opt.Suspend / 2)
					if err := s.A.M.ResumeTask(ctx, sus.ID); err != nil {
						note("resume: %v", err)
						return
					}
					ol.mu.Lock()
					ol.resumedAt = time.Now()
					ol.mu.Unlock()
				}()
			})
		}
	}()
	s.Tr.Subscribe(func(ev proto.Event) {
		switch {
		case ev.TaskID == sus.ID && ev.Kind == proto.EvModelCall:
			ol.mu.Lock()
			if !ol.suspendedAt.IsZero() && ol.resumedAt.IsZero() {
				ol.modelCallsSuspended++
			}
			ol.mu.Unlock()
		case ev.TaskID == st.ID && ev.Kind == proto.EvToolResult:
			steerOnce.Do(func() {
				opsWG.Add(1)
				go func() {
					defer opsWG.Done()
					if err := s.A.M.Steer(ctx, st.ID, steerText); err != nil {
						note("steer: %v", err)
						return
					}
					ol.mu.Lock()
					ol.steeredAt = time.Now()
					ol.mu.Unlock()
					b.logf("ops: steered %s: %q", st.ID, steerText)
				}()
			})
		case ev.TaskID == cn.ID && ev.Kind == proto.EvToolCall:
			cancelOnce.Do(func() {
				opsWG.Add(1)
				go func() {
					defer opsWG.Done()
					if err := s.A.M.Cancel(ctx, cn.ID); err != nil {
						note("cancel: %v", err)
						return
					}
					ol.mu.Lock()
					ol.cancelReturned = time.Now()
					ol.mu.Unlock()
					b.logf("ops: cancelled %s at its first tool call", cn.ID)
				}()
			})
		}
	})
	res, err := s.RunAll(ctx, []*Instance{sus, st, cn})
	opsWG.Wait()
	if err != nil {
		return err
	}
	byID := map[string]*Result{}
	for _, r := range res {
		byID[r.ID] = r
	}
	ol.mu.Lock()
	defer ol.mu.Unlock()
	for _, c := range s.A.ToolRecords() {
		if c.Task == cn.ID && !ol.cancelReturned.IsZero() && c.Start.After(ol.cancelReturned) {
			ol.devCallsAfterCancel++
		}
	}
	// Suspend: it was suspended mid-call, held, resumed and still passed.
	if r := byID[sus.ID]; r != nil {
		evs := s.Tr.Events(sus.ID)
		var susEv proto.Event
		for _, ev := range evs {
			if ev.Kind == proto.EvSuspended {
				susEv = ev
				break
			}
		}
		was, _ := app.Field[string](susEv, "was")
		kept, _ := app.Field[int](susEv, "kept")
		keptThink, _ := app.Field[int](susEv, "kept_think")
		ok := r.Suspends > 0 && was == "model" && ol.modelCallsSuspended == 0 && ol.statusSuspended == 1
		r.Extra = map[string]any{"op": "suspend", "was": was, "kept": kept, "kept_think": keptThink, "suspended_ms": r.SuspendedMS,
			"model_calls_while_suspended": ol.modelCallsSuspended, "status_suspended": ol.statusSuspended, "op_ok": ok}
		if !ok {
			r.Detail += fmt.Sprintf("; suspend check failed (was=%q, calls while suspended %d, Status.Suspended %d)", was, ol.modelCallsSuspended, ol.statusSuspended)
		} else {
			r.Detail += fmt.Sprintf("; suspend ok (was=model, kept %d/%d, held %.1fs)", kept, keptThink, r.SuspendedMS/1000)
		}
		r.Pass = ok && r.Pass
	}
	// Steer: the answer still passes and answers the extra question.
	if r := byID[st.ID]; r != nil {
		dropped := 0
		for _, ev := range s.Tr.Events(st.ID) {
			if ev.Kind == proto.EvSteerDropped {
				dropped++
			}
		}
		extra := wantNumber(r.Answer, float64(b.Facts.Sales.Q3TopOrders), 0)
		ok := extra.Pass && dropped == 0 && !ol.steeredAt.IsZero()
		r.Extra = map[string]any{"op": "steer", "steer": steerText, "want_orders": b.Facts.Sales.Q3TopOrders, "answered_extra": extra.Pass, "steer_dropped": dropped, "op_ok": ok}
		if !extra.Pass {
			r.Detail += fmt.Sprintf("; the steer's question (%d orders) is not answered", b.Facts.Sales.Q3TopOrders)
		}
		r.Pass = ok && r.Pass
	}
	// Cancel: it ended cancelled, with no tool call and no write after.
	if r := byID[cn.ID]; r != nil {
		var cancelSeq int64
		evs := s.Tr.Events(cn.ID)
		for _, ev := range evs {
			if ev.Kind == proto.EvCancelled {
				cancelSeq = ev.Seq
			}
		}
		for _, ev := range evs {
			if ev.Kind == proto.EvToolCall && cancelSeq > 0 && ev.Seq > cancelSeq {
				ol.toolCallsAfterCancel++
			}
		}
		_, statErr := os.Stat(b.Cfg.Root + "/" + cn.OutDir + "/CHANGELOG.md")
		wrote := statErr == nil
		ok := r.Outcome == proto.EvCancelled && ol.toolCallsAfterCancel == 0 && ol.devCallsAfterCancel == 0 && !wrote
		r.Extra = map[string]any{"op": "cancel", "tool_calls_after_cancel": ol.toolCallsAfterCancel, "device_calls_after_cancel": ol.devCallsAfterCancel, "wrote_changelog": wrote, "op_ok": ok}
		r.Pass, r.Detail = ok, fmt.Sprintf("ended %s; %d tool calls and %d device calls after the cancel; changelog written: %v", r.Outcome, ol.toolCallsAfterCancel, ol.devCallsAfterCancel, wrote)
	}
	rep.Results = res
	rep.summarize()
	if len(ol.errs) > 0 {
		rep.Notes = append(rep.Notes, ol.errs...)
	}
	rep.Summary["suspend_for_s"] = b.Opt.Suspend.Seconds()
	return nil
}
