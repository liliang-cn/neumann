// Package load is design §19 acceptance 3 against a real gateway: N short
// tasks (default 200) submitted at once to one execution group that scales
// itself (Min 1, Max 0, the default initial window). Every task must finish
// Done, and the requests a billing proxy in front of the gateway counted must
// equal what the machine accounts for (design §4.6):
//
//	proxy requests = Σ Used.Steps + Σ Unconfirmed + failed calls that got an answer
//
// A request whose connection was refused never reaches the proxy, so it is
// on neither side; a lost response reached it and is counted in Unconfirmed.
// When the gateway signals overload, the window must fall and then grow again.
//
// The proxy is an in-process httptest server on a random loopback port that
// forwards every request to the gateway unchanged and streams responses
// through chunk by chunk; the machine's adapter points at it. The run's
// timeline (Status every 200ms and the vCore, retry and throttle events),
// every proxied request and a summary are written under NEUMANN_LOAD_OUT
// (default: the test's temporary directory).
//
// Skipped unless NEUMANN_ACCEPT_LIVE=1. The endpoint comes from
// OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL or the LLM_ equivalents;
// the key goes from the environment into the adapter's header and nowhere
// else, and the test fails if it shows up in anything the run captured.
//
//	NEUMANN_ACCEPT_LIVE=1 go test -v -count=1 -timeout 40m ./acceptance/load
//	NEUMANN_LOAD_TASKS=500 ...   a larger burst
package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

const class = "load"

func env(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func envInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return def
}

var capitals = map[string]string{
	"France": "Paris", "Japan": "Tokyo", "Austria": "Vienna", "Kenya": "Nairobi", "Peru": "Lima",
	"Canada": "Ottawa", "Norway": "Oslo", "Egypt": "Cairo", "Chile": "Santiago", "Vietnam": "Hanoi",
	"Poland": "Warsaw", "Ghana": "Accra", "Cuba": "Havana", "Nepal": "Kathmandu", "Iceland": "Reykjavik",
}

func devices() []bus.Device {
	num := json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
	country := json.RawMessage(`{"type":"object","properties":{"country":{"type":"string"}},"required":["country"]}`)
	calc := builtin.New("calc").
		Add(bus.ToolDef{Name: "mul", Description: "Multiply two numbers.", Schema: num, Effect: bus.ReadOnly}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			var v struct{ A, B float64 }
			if err := json.Unmarshal(c.Args, &v); err != nil {
				return "", err
			}
			return strconv.FormatFloat(v.A*v.B, 'f', -1, 64), nil
		})
	atlas := builtin.New("atlas").
		Add(bus.ToolDef{Name: "capital", Description: "Look up the capital city of a country.", Schema: country, Effect: bus.ReadOnly}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			var v struct{ Country string }
			if err := json.Unmarshal(c.Args, &v); err != nil {
				return "", err
			}
			for k, city := range capitals {
				if strings.EqualFold(k, strings.TrimSpace(v.Country)) {
					return city, nil
				}
			}
			return "", fmt.Errorf("no country %q in the atlas", v.Country)
		})
	return []bus.Device{calc, atlas}
}

// work is task i: a one-tool multiplication, a one-tool lookup, or a sum the
// model answers directly. Each takes one to three model calls.
func work(i int) (prompt, want string) {
	a, b := 100+(i*37)%900, 10+(i*53)%90
	switch i % 3 {
	case 0:
		return fmt.Sprintf("Use the mul tool to compute %d times %d. Reply with only the resulting number.", a, b), strconv.Itoa(a * b)
	case 1:
		names := make([]string, 0, len(capitals))
		for k := range capitals {
			names = append(names, k)
		}
		sort.Strings(names)
		c := names[i%len(names)]
		return fmt.Sprintf("Use the capital tool to look up the capital of %s. Reply with only the city name.", c), capitals[c]
	default:
		return fmt.Sprintf("What is %d plus %d? Reply with only the number.", a, b), strconv.Itoa(a + b)
	}
}

// logbook is everything the run writes to the test log, kept so the key can
// be looked for in it.
type logbook struct {
	t  *testing.T
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logbook) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logbook) logf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	l.Write([]byte(s + "\n"))
	l.t.Log(s)
}

func (l *logbook) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// sample is the group's Status at one instant, and the proxy's load then.
type sample struct {
	T        float64 `json:"t_ms"`
	Type     string  `json:"type"`
	W        float64 `json:"w"`
	Target   int     `json:"target"`
	Live     int     `json:"live"`
	Busy     int     `json:"busy"`
	Ready    int     `json:"ready"`
	Waiting  int     `json:"waiting"`
	Epoch    uint64  `json:"epoch"`
	Cooling  bool    `json:"cooling"`
	Breaker  string  `json:"breaker"`
	Inflight int64   `json:"proxy_inflight"`
	Proxied  int     `json:"proxy_total"`
	Finished int     `json:"finished"`
}

// mark is a timestamped event from the machine's stream.
type mark struct {
	T    float64         `json:"t_ms"`
	Type string          `json:"type"`
	Kind string          `json:"kind"`
	Task string          `json:"task,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func TestLoadLive(t *testing.T) {
	if os.Getenv("NEUMANN_ACCEPT_LIVE") != "1" {
		t.Skip("live acceptance against a real gateway: set NEUMANN_ACCEPT_LIVE=1 and the endpoint variables")
	}
	base, key, name := env("OPENAI_BASE_URL", "LLM_BASE_URL"), env("OPENAI_API_KEY", "LLM_API_KEY"), env("OPENAI_MODEL", "LLM_MODEL")
	if base == "" || key == "" || name == "" {
		t.Fatal("set OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL (or the LLM_ equivalents)")
	}
	n := envInt("NEUMANN_LOAD_TASKS", 200)
	timeout := time.Duration(envInt("NEUMANN_LOAD_TIMEOUT_MIN", 30)) * time.Minute
	outRoot := os.Getenv("NEUMANN_LOAD_OUT")
	if outRoot == "" {
		outRoot = t.TempDir()
	}
	out := filepath.Join(outRoot, fmt.Sprintf("run-%d-%s", n, time.Now().Format("20060102T150405")))
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	lb := &logbook{t: t}
	target, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil {
		t.Fatalf("base URL: %v", err)
	}

	start := time.Now()
	prx := newMeter(target, openai.NewTransport(openai.HTTP2), lb, start)
	srv := httptest.NewServer(prx) // 127.0.0.1, a random ephemeral port
	defer srv.Close()

	ad := openai.New(srv.URL+target.Path, key, name)
	wr := &wire{inner: ad.HTTP.Transport, answered: map[int]int{}}
	ad.HTTP.Transport = wr
	m, err := neumann.New(neumann.Config{
		Groups:  []neumann.GroupConfig{{Class: class, Model: ad, Min: 1, Max: 0}},
		Devices: devices(),
		System:  "You are a terse assistant. Use the provided tools when asked to; never guess what a tool would return. Reply with only the answer.",
		Logf:    func(f string, a ...any) { lb.logf("machine: "+f, a...) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer scancel()
		if err := m.Stop(sctx); err != nil {
			lb.logf("stop: %v", err)
		}
	}
	defer stop()
	gs := m.Status().Groups[0]
	lb.logf("load: %d tasks on %s via proxy %s; group %s W=%.2f min=%d max=%d auto=%v", n, name, srv.URL, gs.Class, gs.W, gs.Min, gs.Max, gs.Auto)
	if !gs.Auto || gs.Max != 0 || gs.Min != 1 {
		t.Fatalf("group is not autoscaling with Min 1, Max 0: %+v", gs)
	}
	// Events: timestamped as they are committed; the terminal ones counted.
	var (
		evMu     sync.Mutex
		marks    []mark
		finished = map[string]bool{}
		quota    string
	)
	ids := make([]string, n)
	want := make(map[string]string, n)
	idx := make(map[string]int, n)
	allDone := make(chan struct{})
	quotaHit := make(chan struct{})
	evCtx, evCancel := context.WithCancel(ctx)
	defer evCancel()
	evs := m.Events(evCtx, 0)
	go func() {
		closedDone, closedQuota := false, false
		for ev := range evs {
			at := ms(time.Since(start))
			evMu.Lock()
			switch ev.Kind {
			case proto.EvCoreUp, proto.EvCoreDown, proto.EvModelRetry, proto.EvThrottled, proto.EvGroupDown, proto.EvGroupUp, proto.EvMigrated:
				marks = append(marks, mark{T: at, Type: "event", Kind: ev.Kind, Task: ev.TaskID, Data: ev.Data})
			}
			if ev.Kind == proto.EvGroupDown && strings.Contains(string(ev.Data), `"cause":"quota"`) && !closedQuota {
				quota = string(ev.Data)
				closedQuota = true
				close(quotaHit)
			}
			if ev.Terminal() {
				finished[ev.TaskID] = true
				if len(finished) >= n && !closedDone {
					closedDone = true
					close(allDone)
				}
			}
			evMu.Unlock()
		}
	}()

	// Status every 200ms.
	var (
		smMu    sync.Mutex
		samples []sample
	)
	sampCtx, sampCancel := context.WithCancel(ctx)
	sampDone := make(chan struct{})
	go func() {
		defer close(sampDone)
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			st := m.Status()
			g := st.Groups[0]
			evMu.Lock()
			fin := len(finished)
			evMu.Unlock()
			prx.mu.Lock()
			proxied := len(prx.reqs)
			prx.mu.Unlock()
			smMu.Lock()
			samples = append(samples, sample{
				T: ms(time.Since(start)), Type: "sample", W: g.W, Target: g.Target, Live: g.Live, Busy: g.Busy, Ready: g.Ready,
				Waiting: g.Waiting, Epoch: g.Epoch, Cooling: g.Cooling, Breaker: g.Breaker, Inflight: prx.inflight.Load(), Proxied: proxied, Finished: fin,
			})
			smMu.Unlock()
			select {
			case <-tk.C:
			case <-sampCtx.Done():
				return
			}
		}
	}()

	// Submit all at once.
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, w := work(i)
			id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: p, Budget: &task.Budget{MaxSteps: 8}})
			ids[i], errs[i] = id, err
			evMu.Lock()
			want[id], idx[id] = w, i
			evMu.Unlock()
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	lb.logf("load: %d tasks submitted in %v", n, time.Since(start).Round(time.Millisecond))

	aborted := ""
	select {
	case <-allDone:
	case <-quotaHit:
		aborted = "gateway quota exhausted (group_down cause quota): " + quota
	case <-ctx.Done():
		aborted = fmt.Sprintf("timed out after %v", timeout)
	}
	wall := time.Since(start)
	if aborted != "" {
		lb.logf("load: ABORTED: %s; cancelling the unfinished tasks", aborted)
		for _, id := range ids {
			m.Cancel(context.Background(), id)
		}
		select {
		case <-allDone:
		case <-time.After(2 * time.Minute):
			lb.logf("load: tasks still unfinished two minutes after the cancel")
		}
	}
	sampCancel()
	<-sampDone
	stop() // no more requests after this
	srv.Close()

	// The machine's account.
	bg := context.Background()
	var (
		used                        task.Usage
		steps, unconfirmed, correct int
		states                      = map[string]int{}
		notDone                     []string
		perTaskSteps                = map[int]int{}
	)
	for _, id := range ids {
		tc, err := m.Load(bg, id)
		if err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
		used.Add(tc.Used)
		steps += tc.Used.Steps
		unconfirmed += tc.Unconfirmed
		perTaskSteps[tc.Used.Steps]++
		key := tc.State.String()
		if tc.StopReason != "" {
			key += "/" + tc.StopReason
		}
		states[key]++
		if tc.State != task.Done || tc.StopReason != "end_turn" {
			notDone = append(notDone, fmt.Sprintf("%s #%d %s stop=%q steps=%d answer=%q", id, idx[id], tc.State, tc.StopReason, tc.Used.Steps, clip(tc.LastAnswer(), 80)))
		} else if strings.Contains(strings.ReplaceAll(tc.LastAnswer(), ",", ""), want[id]) {
			correct++
		}
	}
	log, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	statusRe := regexp.MustCompile(`status \d{3}\b`)
	var (
		modelSteps                    int
		failedAnswered, failedLost    int
		failedNoResponse              int
		retryByClass                  = map[string]int{}
		noResponseErrs                []string
		coreUps, coreDowns, throttled int
		downReasons                   = map[string]int{}
		groupDowns                    int
	)
	failedCall := func(text string) {
		switch {
		case strings.Contains(text, "response lost"):
			failedLost++
		case statusRe.MatchString(text):
			failedAnswered++
		default:
			failedNoResponse++
			noResponseErrs = append(noResponseErrs, clip(text, 160))
		}
	}
	for _, ev := range log {
		var d map[string]any
		json.Unmarshal(ev.Data, &d)
		switch ev.Kind {
		case proto.EvStep:
			if d["pc"] == "model" {
				modelSteps++
			}
		case proto.EvModelRetry:
			c, _ := d["class"].(string)
			retryByClass[c]++
			if c != "truncated" {
				e, _ := d["error"].(string)
				failedCall(e)
			}
		case proto.EvFailed:
			if d["stop_reason"] == "model_error" {
				e, _ := d["error"].(string)
				failedCall(e)
			}
		case proto.EvCoreUp:
			coreUps++
		case proto.EvCoreDown:
			coreDowns++
			r, _ := d["reason"].(string)
			downReasons[r]++
		case proto.EvThrottled:
			throttled++
		case proto.EvGroupDown:
			groupDowns++
		}
	}

	// The proxy's account.
	xs := prx.snapshot()
	chatPath := target.Path + "/chat/completions"
	chat := summarize(xs, chatPath)
	paths := map[string]int{}
	for _, x := range xs {
		paths[x.Path]++
	}
	wr.mu.Lock()
	wireSent, wireAnswered, wireNoResp, wireBodyErr := wr.sent, cloneCounts(wr.answered), append([]string(nil), wr.noResp...), wr.bodyErr
	wr.mu.Unlock()

	// The window over time.
	smMu.Lock()
	ss := append([]sample(nil), samples...)
	smMu.Unlock()
	peakLive, peakW, finalEpoch := 0, 0.0, uint64(0)
	for _, s := range ss {
		peakLive, peakW, finalEpoch = max(peakLive, s.Live), max(peakW, s.W), max(finalEpoch, s.Epoch)
	}
	wtl := windowTimeline(ss)
	falls := halvings(ss)
	overloadSignals := retryByClass["overload"] + chat.ByStatus["529"] + chat.ByStatus["503"]
	overloaded := finalEpoch > 0 || retryByClass["overload"] > 0

	// Write the timeline, the requests and the summary.
	evMu.Lock()
	ms2 := append([]mark(nil), marks...)
	evMu.Unlock()
	writeTimeline(t, filepath.Join(out, "timeline.jsonl"), ss, ms2)
	writeRequests(t, filepath.Join(out, "requests.csv"), xs)
	sum := map[string]any{
		"tasks": n, "model": name, "wall_s": wall.Seconds(), "aborted": aborted,
		"states": states, "not_done": notDone, "correct_answers": correct, "steps_per_task": perTaskSteps,
		"tokens":      map[string]int{"prompt": used.PromptTokens, "cached": used.CachedTokens, "completion": used.CompletionTokens, "reasoning": used.ReasoningTokens},
		"machine":     map[string]any{"sum_steps": steps, "sum_unconfirmed": unconfirmed, "failed_answered": failedAnswered, "failed_lost": failedLost, "failed_no_response": failedNoResponse, "model_step_events": modelSteps, "retry_by_class": retryByClass, "no_response_errors": noResponseErrs},
		"proxy":       map[string]any{"by_path": paths, "chat": chat, "peak_inflight": prx.peak.Load()},
		"client_wire": map[string]any{"sent": wireSent, "answered_by_status": wireAnswered, "no_response": wireNoResp, "body_errors": wireBodyErr},
		"window":      map[string]any{"peak_live": peakLive, "peak_w": peakW, "final_epoch": finalEpoch, "core_up": coreUps, "core_down": coreDowns, "core_down_reasons": downReasons, "halvings": falls, "timeline": wtl},
		"throttled":   throttled, "group_down": groupDowns, "overload_signals": overloadSignals,
	}
	sb, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(filepath.Join(out, "summary.json"), sb, 0o644); err != nil {
		t.Fatal(err)
	}

	// Report.
	lb.logf("== %d tasks, wall %.1fs ==", n, wall.Seconds())
	lb.logf("states %v; correct answers %d/%d; answered steps per task %v", states, correct, n, perTaskSteps)
	lb.logf("tokens: prompt %d (cached %d), completion %d, reasoning %d", used.PromptTokens, used.CachedTokens, used.CompletionTokens, used.ReasoningTokens)
	lb.logf("proxy: by path %v; %s: received %d, by status %v, body errors %d, client hangups %d, peak in flight %d; latency p50 %.0fms p90 %.0fms p99 %.0fms max %.0fms",
		paths, chatPath, chat.Received, chat.ByStatus, chat.BodyErr, chat.Hangups, prx.peak.Load(), chat.P50, chat.P90, chat.P99, chat.Max)
	lb.logf("client wire: sent %d, answered %v, no response %d, body errors %d", wireSent, wireAnswered, len(wireNoResp), wireBodyErr)
	lb.logf("machine: Σsteps %d, Σunconfirmed %d, failed calls answered %d / lost %d / no response %d; model-step events %d; model_retry by class %v",
		steps, unconfirmed, failedAnswered, failedLost, failedNoResponse, modelSteps, retryByClass)
	lb.logf("window: peak live %d, peak W %.2f, final epoch %d, core_up %d, core_down %d %v, throttled %d, group_down %d",
		peakLive, peakW, finalEpoch, coreUps, coreDowns, downReasons, throttled, groupDowns)
	lb.logf("W timeline: %s", strings.Join(wtl, " "))
	lb.logf("halvings: %v", falls)
	lb.logf("output: %s", out)

	// Clause 1: every task Done.
	if aborted != "" {
		t.Errorf("run aborted: %s", aborted)
	}
	if len(notDone) > 0 {
		t.Errorf("%d of %d tasks did not finish Done:\n  %s", len(notDone), n, strings.Join(notDone, "\n  "))
	}

	// Clause 3: the accounting identity, and §4.6's own consistency.
	machineSide := steps + unconfirmed + failedAnswered
	lb.logf("identity: proxy %d == Σsteps %d + Σunconfirmed %d + answered failures %d = %d", chat.Received, steps, unconfirmed, failedAnswered, machineSide)
	if chat.Received != machineSide {
		t.Errorf("accounting identity broken: the proxy forwarded %d model requests, the machine accounts for %d", chat.Received, machineSide)
	}
	if modelSteps != steps+failedAnswered+failedLost+failedNoResponse {
		t.Errorf("model-step events %d != Σsteps %d + failed calls %d", modelSteps, steps, failedAnswered+failedLost+failedNoResponse)
	}
	if wireSent != modelSteps {
		t.Errorf("the adapter's client sent %d requests for %d model-step events", wireSent, modelSteps)
	}
	if unconfirmed != failedLost {
		t.Logf("note: Σunconfirmed %d != lost responses %d (a cancel can also leave a call unconfirmed)", unconfirmed, failedLost)
	}

	// Clause 2: after overload, the window falls and grows again.
	switch {
	case !overloaded:
		lb.logf("overload: none observed at %d tasks (no overload retry, epoch stayed 0; non-2xx %v)", n, chat.ByStatus)
	default:
		lb.logf("overload: observed (overload retries %d, epoch %d)", retryByClass["overload"], finalEpoch)
		if len(falls) == 0 {
			t.Errorf("overload was signalled but no fall of W was sampled")
		}
		grew := false
		for _, f := range falls {
			if f.GrewTo > f.To {
				grew = true
			}
		}
		if !grew && workAfterCooldown(ss, falls) {
			t.Errorf("W fell after overload and never grew again although work remained after the cooldown: %v", falls)
		} else if !grew {
			lb.logf("overload: W fell but the work ran out before the cooldown ended; regrowth not observable")
		}
	}

	// The key is nowhere in what the run captured.
	captured := []string{lb.String(), string(sb), readFile(filepath.Join(out, "timeline.jsonl")), readFile(filepath.Join(out, "requests.csv"))}
	for _, ev := range log {
		captured = append(captured, string(ev.Data))
	}
	for _, s := range captured {
		if strings.Contains(s, key) {
			t.Fatal("the API key appears in the captured output")
		}
	}
}

// fall is one halving of W seen in the samples (its epoch went up), and how
// the window did afterwards.
type fall struct {
	At     float64 `json:"at_ms"`
	From   float64 `json:"from"`
	To     float64 `json:"to"`
	Epoch  uint64  `json:"epoch"`
	GrewAt float64 `json:"grew_at_ms,omitempty"` // first sample after it with W above To
	GrewTo float64 `json:"grew_to,omitempty"`    // the largest W before the next fall
}

func halvings(ss []sample) []fall {
	var out []fall
	for i := 1; i < len(ss); i++ {
		if ss[i].Epoch <= ss[i-1].Epoch {
			continue
		}
		f := fall{At: ss[i].T, From: ss[i-1].W, To: ss[i].W, Epoch: ss[i].Epoch}
		for j := i + 1; j < len(ss) && ss[j].Epoch == ss[i].Epoch; j++ {
			f.To = min(f.To, ss[j].W)
		}
		for j := i + 1; j < len(ss) && ss[j].Epoch == ss[i].Epoch; j++ {
			if ss[j].W > f.To {
				if f.GrewAt == 0 {
					f.GrewAt = ss[j].T
				}
				f.GrewTo = max(f.GrewTo, ss[j].W)
			}
		}
		out = append(out, f)
	}
	return out
}

// workAfterCooldown reports whether, after the last fall's cooldown, the
// group still had work to grow on.
func workAfterCooldown(ss []sample, falls []fall) bool {
	last := falls[len(falls)-1]
	for _, s := range ss {
		if s.T > last.At && !s.Cooling && s.Busy+s.Ready > 0 {
			return true
		}
	}
	return false
}

// windowTimeline is W, live vCores and in-flight requests every five seconds.
func windowTimeline(ss []sample) []string {
	var out []string
	next := 0.0
	for _, s := range ss {
		if s.T < next {
			continue
		}
		next = s.T + 5000
		c := ""
		if s.Cooling {
			c = "c"
		}
		out = append(out, fmt.Sprintf("%.0fs:W%.1f/n%d/f%d%s", s.T/1000, s.W, s.Live, s.Inflight, c))
	}
	return out
}

func writeTimeline(t *testing.T, path string, ss []sample, marks []mark) {
	var lines []struct {
		t float64
		b []byte
	}
	for _, s := range ss {
		b, _ := json.Marshal(s)
		lines = append(lines, struct {
			t float64
			b []byte
		}{s.T, b})
	}
	for _, m := range marks {
		b, _ := json.Marshal(m)
		lines = append(lines, struct {
			t float64
			b []byte
		}{m.T, b})
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].t < lines[j].t })
	var buf bytes.Buffer
	for _, l := range lines {
		buf.Write(l.b)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeRequests(t *testing.T, path string, xs []exchange) {
	var buf bytes.Buffer
	buf.WriteString("start_ms,path,status,header_ms,total_ms,bytes,upstream_err,body_err,client_closed\n")
	for _, x := range xs {
		fmt.Fprintf(&buf, "%.1f,%s,%d,%.1f,%.1f,%d,%q,%q,%v\n", x.Start, x.Path, x.Status, x.Header, x.Total, x.Bytes, x.Upstream, x.Body, x.ClientClosed)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

func cloneCounts(m map[int]int) map[int]int {
	out := make(map[int]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func clip(s string, n int) string {
	r := []rune(strings.Join(strings.Fields(s), " "))
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return string(r)
}
