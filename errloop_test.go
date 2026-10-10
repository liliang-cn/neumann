package neumann_test

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// script answers each task (named by its first user message) with its own
// list of failures, one per call, then with "<name> done". It records when
// each call was made, on the machine's clock, and the output cap it carried.
type script struct {
	clk   *sim.Clock
	mu    sync.Mutex
	plan  map[string][]func() (model.Response, error)
	calls map[string][]call
}

type call struct {
	at     time.Time
	maxOut int
}

func newScript(clk *sim.Clock) *script {
	return &script{clk: clk, plan: map[string][]func() (model.Response, error){}, calls: map[string][]call{}}
}

func fails(errs ...error) []func() (model.Response, error) {
	var out []func() (model.Response, error)
	for _, err := range errs {
		out = append(out, func() (model.Response, error) { return model.Response{}, err })
	}
	return out
}

func (s *script) fn(req model.Request) (model.Response, error) {
	name := sim.FirstUser(req)
	s.mu.Lock()
	s.calls[name] = append(s.calls[name], call{at: s.clk.Now(), maxOut: req.MaxOut})
	n := len(s.calls[name])
	plan := s.plan[name]
	s.mu.Unlock()
	if n <= len(plan) {
		return plan[n-1]()
	}
	return sim.Reply(name + " done"), nil
}

func (s *script) of(name string) []call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls[name])
}

func (s *script) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		n += len(c)
	}
	return n
}

// errMachine is a machine on a fake clock whose backoff has no jitter, so
// every delay is the exponential step itself.
func errMachine(t *testing.T, cfg neumann.Config) (*neumann.Machine, *sim.Clock, *script) {
	t.Helper()
	clk := sim.NewClock(clock0)
	sc := newScript(clk)
	cfg.Clock = clk
	if cfg.BackoffJitter == nil {
		cfg.BackoffJitter = func(d time.Duration) time.Duration { return d }
	}
	if cfg.Cores == 0 {
		cfg.Cores = 2
	}
	m, _ := start(t, cfg, sc.fn)
	return m, clk, sc
}

// quiet waits until no task is ready or on a vCore, except vCores waiting
// for room in a limiter: everything is waiting (for a timer, for its group,
// for room) or finished, so moving the clock is the only thing that can make
// progress.
func quiet(t *testing.T, m *neumann.Machine) {
	t.Helper()
	still := func() bool {
		st := m.Status()
		throttled := 0
		for _, l := range st.Limiters {
			throttled += l.Waiting
		}
		return st.Ready == 0 && st.Held <= throttled
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if still() {
			time.Sleep(2 * time.Millisecond)
			if still() {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the machine never went quiet: %+v", m.Status())
}

// runUntilDone moves the fake clock in steps until every task has ended.
func runUntilDone(t *testing.T, m *neumann.Machine, clk *sim.Clock, step time.Duration, ids ...string) {
	t.Helper()
	for range 2000 {
		quiet(t, m)
		done := true
		for _, id := range ids {
			if tc, err := m.Load(bg, id); err != nil || !tc.State.Terminal() {
				done = false
			}
		}
		if done {
			return
		}
		clk.Advance(step)
	}
	t.Fatalf("tasks %v never finished", ids)
}

func loadTask(t *testing.T, m *neumann.Machine, id string) *task.TCB {
	t.Helper()
	tc, err := m.Load(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	return tc
}

type retryData struct {
	Class   string `json:"class"`
	Attempt int    `json:"attempt"`
	DelayMS int64  `json:"delay_ms"`
	Wait    string `json:"wait"`
	MaxOut  int    `json:"max_out"`
	Error   string `json:"error"`
}

func retries(t *testing.T, m *neumann.Machine, id string) []retryData {
	t.Helper()
	var out []retryData
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvModelRetry {
			var d retryData
			json.Unmarshal(ev.Data, &d)
			out = append(out, d)
		}
	}
	return out
}

// machineEvents are the events of kind that belong to no task.
func machineEvents(t *testing.T, m *neumann.Machine, kind string) []map[string]any {
	t.Helper()
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, ev := range all {
		if ev.Kind == kind && ev.TaskID == "" {
			var d map[string]any
			json.Unmarshal(ev.Data, &d)
			out = append(out, d)
		}
	}
	return out
}

func failedEvent(t *testing.T, m *neumann.Machine, id string) map[string]string {
	t.Helper()
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvFailed {
			var d map[string]string
			json.Unmarshal(ev.Data, &d)
			return d
		}
	}
	return nil
}

func gateway(n int64, per time.Duration) []limit.Config {
	return []limit.Config{{Name: "gw", Limits: map[string]limit.Rate{"requests": {Per: per, Limit: n}}}}
}

// RateLimited (design §11): the task waits for the provider's retry time,
// which does not count against its retries, and the limiter pauses until
// then, so the group's other tasks wait instead of earning their own 429.
// The window is not touched (§18 test 16).
func TestRateLimitedWaitsWithoutSpendingRetries(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{MaxModelRetries: 2, Limiters: gateway(100, time.Minute), Limits: []string{"gw"}})
	sc.plan["A"] = fails(sim.RateLimited(10*time.Second), sim.RateLimited(10*time.Second), sim.RateLimited(10*time.Second))
	a := submit(t, m, proto.Instruction{Input: "A"})
	quiet(t, m)
	if got := loadTask(t, m, a); got.State != task.Waiting || got.Wait != task.WaitBackoff || !got.WakeAt.Equal(clk.Now().Add(10*time.Second)) {
		t.Fatalf("A is %s/%s until %v", got.State, got.Wait, got.WakeAt)
	}
	if st := m.Status().Limiters[0]; !st.PausedUntil.Equal(clk.Now().Add(10 * time.Second)) {
		t.Fatalf("limiter %+v", st)
	}

	// B arrives during the pause: it waits for room, holding a vCore, and
	// changes no state while it does.
	b := submit(t, m, proto.Instruction{Input: "B"})
	waitFor(t, func() bool { return m.Status().Limiters[0].Waiting == 1 })
	if n := len(sc.of("B")); n != 0 {
		t.Fatalf("B called the model %d times during the pause", n)
	}
	if got := loadTask(t, m, b); got.State != task.Ready {
		t.Fatalf("B is %s while throttled", got.State)
	}

	runUntilDone(t, m, clk, 10*time.Second, a, b)
	got := loadTask(t, m, a)
	if got.State != task.Done || got.Retries != 0 || got.Used.Steps != 1 {
		t.Fatalf("A ended %s %q, retries %d, steps %d", got.State, got.StopReason, got.Retries, got.Used.Steps)
	}
	rs := retries(t, m, a)
	if len(rs) != 3 {
		t.Fatalf("model_retry events %+v", rs)
	}
	for _, r := range rs {
		if r.Class != "rate_limited" || r.Attempt != 0 || r.DelayMS != 10000 || r.Wait != "backoff" {
			t.Fatalf("model_retry %+v", r)
		}
	}
	if got := loadTask(t, m, b); got.State != task.Done {
		t.Fatalf("B ended %s", got.State)
	}
	th := machineEvents(t, m, proto.EvThrottled)
	if len(th) == 0 || th[0]["task"] != b || th[0]["waited_ms"].(float64) < 10000 {
		t.Fatalf("throttled events %v", th)
	}
	g := m.Status().Groups[0]
	if g.Epoch != 0 || g.Breaker != "closed" {
		t.Fatalf("a rate limit moved the group: %+v", g)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(time.Millisecond)
	}
}

// Overload and Transient back off exponentially (base 1s, doubling), never
// shorter than the provider's retry-after, and succeed when the provider
// recovers; the retry count resets on success.
func TestOverloadAndTransientBackOff(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{Cores: 1})
	sc.plan["A"] = fails(sim.Fail(model.Overload, 5*time.Second), sim.Transient(), sim.Transient())
	a := submit(t, m, proto.Instruction{Input: "A"})
	runUntilDone(t, m, clk, time.Second, a)
	got := loadTask(t, m, a)
	if got.State != task.Done || got.Retries != 0 || got.LastAnswer() != "A done" {
		t.Fatalf("A ended %s %q, retries %d", got.State, got.StopReason, got.Retries)
	}
	rs := retries(t, m, a)
	want := []retryData{{Class: "overload", Attempt: 1, DelayMS: 5000}, {Class: "transient", Attempt: 2, DelayMS: 2000}, {Class: "transient", Attempt: 3, DelayMS: 4000}}
	if len(rs) != len(want) {
		t.Fatalf("model_retry events %+v", rs)
	}
	for i, r := range rs {
		if r.Class != want[i].Class || r.Attempt != want[i].Attempt || r.DelayMS != want[i].DelayMS || r.Error == "" {
			t.Fatalf("retry %d: %+v, want %+v", i, r, want[i])
		}
	}
	var at []time.Duration
	for _, c := range sc.of("A") {
		at = append(at, c.at.Sub(clock0))
	}
	if !slices.Equal(at, []time.Duration{0, 5 * time.Second, 7 * time.Second, 11 * time.Second}) {
		t.Fatalf("calls at %v", at)
	}
}

// After MaxModelRetries consecutive failures the task fails with model_error
// and the provider's error in its failed event.
func TestModelRetriesRunOut(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{MaxModelRetries: 3})
	sc.plan["A"] = fails(sim.Transient(), sim.Transient(), sim.Transient(), sim.Transient())
	a := submit(t, m, proto.Instruction{Input: "A"})
	runUntilDone(t, m, clk, time.Second, a)
	got := loadTask(t, m, a)
	if got.State != task.Failed || got.StopReason != "model_error" || len(sc.of("A")) != 3 {
		t.Fatalf("A ended %s %q after %d calls", got.State, got.StopReason, len(sc.of("A")))
	}
	if d := failedEvent(t, m, a); !strings.Contains(d["error"], "scripted transient") || d["stop_reason"] != "model_error" {
		t.Fatalf("failed event %v", d)
	}
	if n := len(retries(t, m, a)); n != 2 {
		t.Fatalf("%d model_retry events", n)
	}
}

// A permanent rejection fails the task at once.
func TestPermanentFailsAtOnce(t *testing.T) {
	m, _, sc := errMachine(t, neumann.Config{})
	sc.plan["A"] = fails(sim.Permanent())
	a := submit(t, m, proto.Instruction{Input: "A"})
	got := waitDone(t, m, a)
	if got.State != task.Failed || got.StopReason != "model_error" || len(sc.of("A")) != 1 || len(retries(t, m, a)) != 0 {
		t.Fatalf("A ended %s %q after %d calls", got.State, got.StopReason, len(sc.of("A")))
	}
}

// design §18 test 12: Unconfirmed grows only for a response that was lost
// after the request reached the provider, never for a request that never got
// out; Used holds only what responses reported.
func TestOnlyALostResponseIsUnconfirmed(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{})
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	sc.plan["A"] = fails(sim.Lost(), refused, sim.Transient())
	a := submit(t, m, proto.Instruction{Input: "A"})
	runUntilDone(t, m, clk, time.Second, a)
	got := loadTask(t, m, a)
	if got.State != task.Done || got.Unconfirmed != 1 || got.Used.Steps != 1 ||
		got.Used.PromptTokens != 10 || got.Used.CompletionTokens != 5 {
		t.Fatalf("A ended %s, unconfirmed %d, used %+v", got.State, got.Unconfirmed, got.Used)
	}
}

// Used.Steps counts answered calls only: twenty rate-limited replies do not
// spend a budget of three steps, and the task finishes.
func TestRateLimitStormDoesNotSpendTheBudget(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{})
	var storm []error
	for range 20 {
		storm = append(storm, sim.RateLimited(time.Second))
	}
	sc.plan["A"] = fails(storm...)
	a := submit(t, m, proto.Instruction{Input: "A", Budget: &task.Budget{MaxSteps: 3}})
	runUntilDone(t, m, clk, time.Second, a)
	got := loadTask(t, m, a)
	if got.State != task.Done || got.StopReason != "end_turn" || got.Used.Steps != 1 || len(sc.of("A")) != 21 {
		t.Fatalf("A ended %s %q, steps %d, calls %d", got.State, got.StopReason, got.Used.Steps, len(sc.of("A")))
	}
}

// design §13: a turn cut off by its output cap with nothing in it is asked
// again with four times the cap, at most twice; then the task fails with
// "truncated". Each redo is a counted step.
func TestTruncatedTurnIsRedoneWithALargerCap(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{})
	trunc := func() (model.Response, error) { return sim.Truncated(), nil }
	sc.plan["A"] = []func() (model.Response, error){trunc, trunc, trunc}
	sc.plan["B"] = []func() (model.Response, error){trunc}
	a := submit(t, m, proto.Instruction{Input: "A"})
	b := submit(t, m, proto.Instruction{Input: "B"})
	runUntilDone(t, m, clk, time.Second, a, b)

	got := loadTask(t, m, a)
	if got.State != task.Failed || got.StopReason != "truncated" || got.Used.Steps != 3 {
		t.Fatalf("A ended %s %q after %d steps", got.State, got.StopReason, got.Used.Steps)
	}
	var caps []int
	for _, c := range sc.of("A") {
		caps = append(caps, c.maxOut)
	}
	if !slices.Equal(caps, []int{0, 16384, 65536}) {
		t.Fatalf("output caps %v", caps)
	}
	rs := retries(t, m, a)
	if len(rs) != 2 || rs[0].Class != "truncated" || rs[0].MaxOut != 16384 || rs[1].MaxOut != 65536 {
		t.Fatalf("model_retry events %+v", rs)
	}
	if got := loadTask(t, m, b); got.State != task.Done || got.MaxOut != 16384 || got.LastAnswer() != "B done" {
		t.Fatalf("B ended %s with cap %d", got.State, got.MaxOut)
	}
}

// design §18 test 16, through a machine: an exhausted quota opens the
// group's breaker for good. Its tasks wait for the group without calling the
// model, an hour later still, until an operator's GroupReset brings it back.
func TestQuotaExhaustedHoldsTheGroupUntilReset(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{})
	sc.plan["A"] = fails(sim.QuotaExhausted())
	a := submit(t, m, proto.Instruction{Input: "A"})
	quiet(t, m)
	b := submit(t, m, proto.Instruction{Input: "B"})
	quiet(t, m)
	clk.Advance(time.Hour)
	quiet(t, m)
	for _, id := range []string{a, b} {
		if got := loadTask(t, m, id); got.State != task.Waiting || got.Wait != task.WaitGroup {
			t.Fatalf("%s is %s/%s", id, got.State, got.Wait)
		}
	}
	if n := sc.total(); n != 1 {
		t.Fatalf("%d model calls while the group was down", n)
	}
	if d := machineEvents(t, m, proto.EvGroupDown); len(d) != 1 || d[0]["cause"] != "quota" || d[0]["class"] != neumann.DefaultClass {
		t.Fatalf("group_down %v", d)
	}
	if g := m.Status().Groups[0]; g.Breaker != "open" || g.Cause != "quota" || !g.NextProbe.IsZero() {
		t.Fatalf("group %+v", g)
	}
	if err := m.GroupReset(bg, "nope"); err == nil {
		t.Fatal("reset of an unknown class")
	}
	if err := m.GroupReset(bg, neumann.DefaultClass); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a, b} {
		if got := waitDone(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s", id, got.State)
		}
	}
	if d := machineEvents(t, m, proto.EvGroupUp); len(d) != 1 {
		t.Fatalf("group_up %v", d)
	}
}

// Five consecutive transient failures open the breaker (design §6.2): the
// group sends nothing for 30s of the machine's clock, then lets one probe
// through, and its success brings the group back and wakes everyone.
func TestTransientFailuresOpenTheBreaker(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{Cores: 1, MaxModelRetries: 20})
	sc.plan["A"] = fails(sim.Transient(), sim.Transient(), sim.Transient(), sim.Transient(), sim.Transient())
	a := submit(t, m, proto.Instruction{Input: "A"})
	var b string
	for range 60 {
		quiet(t, m)
		if b == "" && len(machineEvents(t, m, proto.EvGroupDown)) > 0 {
			// Arrives while the group is down: it waits without a call.
			b = submit(t, m, proto.Instruction{Input: "B"})
			quiet(t, m)
			if got := loadTask(t, m, b); got.Wait != task.WaitGroup {
				t.Fatalf("B is %s/%s", got.State, got.Wait)
			}
		}
		clk.Advance(time.Second)
	}
	runUntilDone(t, m, clk, time.Second, a, b)
	var at []time.Duration
	for _, c := range sc.of("A") {
		at = append(at, c.at.Sub(clock0))
	}
	// Failures at 0, 1, 3, 7 and 15s; the fifth opens the breaker; the probe
	// goes out 30s later.
	want := []time.Duration{0, time.Second, 3 * time.Second, 7 * time.Second, 15 * time.Second, 45 * time.Second}
	if !slices.Equal(at, want) {
		t.Fatalf("A's calls at %v, want %v", at, want)
	}
	if c := sc.of("B"); len(c) != 1 || c[0].at.Before(clock0.Add(45*time.Second)) {
		t.Fatalf("B's calls %v", c)
	}
	down, up := machineEvents(t, m, proto.EvGroupDown), machineEvents(t, m, proto.EvGroupUp)
	if len(down) != 1 || down[0]["cause"] != "transient" || len(up) != 1 {
		t.Fatalf("group_down %v group_up %v", down, up)
	}
	for _, id := range []string{a, b} {
		if got := loadTask(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s", id, got.State)
		}
	}
	if g := m.Status().Groups[0]; g.Breaker != "closed" {
		t.Fatalf("group %+v", g)
	}
}

// Two tasks sharing a limiter of one request per minute are served one after
// the other; the second one's wait is reported.
func TestSharedLimiterSerialisesCalls(t *testing.T) {
	m, clk, sc := errMachine(t, neumann.Config{Limiters: gateway(1, time.Minute), Limits: []string{"gw"}})
	a := submit(t, m, proto.Instruction{Input: "A"})
	b := submit(t, m, proto.Instruction{Input: "B"})
	waitFor(t, func() bool { return sc.total() == 1 && m.Status().Limiters[0].Waiting == 1 })
	clk.Advance(30 * time.Second)
	time.Sleep(10 * time.Millisecond)
	if n := sc.total(); n != 1 {
		t.Fatalf("%d calls within the minute", n)
	}
	clk.Advance(30 * time.Second)
	for _, id := range []string{a, b} {
		if got := waitDone(t, m, id); got.State != task.Done {
			t.Fatalf("%s ended %s", id, got.State)
		}
	}
	first, second := a, b
	if len(sc.of("B")) > 0 && sc.of("B")[0].at.Before(sc.of("A")[0].at) {
		first, second = b, a
	}
	_ = first
	th := machineEvents(t, m, proto.EvThrottled)
	if len(th) != 1 || th[0]["task"] != second || th[0]["waited_ms"].(float64) != 60000 || th[0]["class"] != neumann.DefaultClass {
		t.Fatalf("throttled events %v", th)
	}
}

// A request that can never fit its limiters fails the task at once.
func TestTooLargeForTheLimiterFails(t *testing.T) {
	m, _, sc := errMachine(t, neumann.Config{
		Limiters: []limit.Config{{Name: "gw", Limits: map[string]limit.Rate{"output_tokens": {Per: time.Minute, Limit: 1000}}}},
		Limits:   []string{"gw"},
	})
	a := submit(t, m, proto.Instruction{Input: "A"})
	if got := waitDone(t, m, a); got.State != task.Failed || got.StopReason != "too_large" || sc.total() != 0 {
		t.Fatalf("A ended %s %q after %d calls", got.State, got.StopReason, sc.total())
	}
}

func TestConfigValidatesLimiters(t *testing.T) {
	for _, cfg := range []neumann.Config{
		{Limits: []string{"missing"}},
		{Limiters: append(gateway(1, time.Minute), gateway(2, time.Minute)...)},
		{Limiters: []limit.Config{{Name: "bad"}}},
	} {
		cfg.Model = &sim.Model{Fn: payModel}
		if _, err := neumann.New(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
}

// design §18 test 10, for the waits this step adds: a cancel and a deadline
// end a task in Waiting(Backoff), in Waiting(Group), and while its vCore
// waits for room in a limiter. None of them calls the model again.
func TestCancelAndDeadlineWhileWaiting(t *testing.T) {
	type setup struct {
		cfg  neumann.Config
		plan []func() (model.Response, error)
		// waiting reports that A has reached the wait under test.
		waiting func(m *neumann.Machine, a string) bool
	}
	inWait := func(w task.WaitReason) func(*neumann.Machine, string) bool {
		return func(m *neumann.Machine, a string) bool {
			tc, err := m.Load(bg, a)
			return err == nil && tc.State == task.Waiting && tc.Wait == w
		}
	}
	setups := map[string]setup{
		// The provider asks for two hours: the deadline falls inside them.
		"backoff": {plan: fails(sim.Fail(model.Overload, 2*time.Hour)), waiting: inWait(task.WaitBackoff)},
		"group":   {plan: fails(sim.QuotaExhausted()), waiting: inWait(task.WaitGroup)},
		// "first" takes the only request of the minute; A waits for room.
		"reservation": {cfg: neumann.Config{Limiters: gateway(1, time.Hour*24), Limits: []string{"gw"}},
			waiting: func(m *neumann.Machine, a string) bool { return m.Status().Limiters[0].Waiting == 1 }},
	}
	for name, s := range setups {
		for _, how := range []string{"cancel", "deadline"} {
			t.Run(name+"/"+how, func(t *testing.T) {
				m, clk, sc := errMachine(t, s.cfg)
				sc.plan["A"] = s.plan
				if name == "reservation" {
					first := submit(t, m, proto.Instruction{Input: "first"})
					waitDone(t, m, first)
				}
				in := proto.Instruction{Input: "A"}
				if how == "deadline" {
					in.Budget = inAnHour(clk)
				}
				a := submit(t, m, in)
				waitFor(t, func() bool { return s.waiting(m, a) })
				calls := len(sc.of("A"))
				if how == "cancel" {
					if err := m.Cancel(bg, a); err != nil {
						t.Fatal(err)
					}
					if got := waitDone(t, m, a); got.State != task.Cancelled || got.StopReason != "cancelled" {
						t.Fatalf("A ended %s %q", got.State, got.StopReason)
					}
				} else {
					clk.Advance(time.Hour)
					failedByDeadline(t, m, a)
				}
				if n := len(sc.of("A")); n != calls {
					t.Fatalf("the model was called %d more times", n-calls)
				}
				if st := m.Status(); st.Ready+st.Waiting+st.Held != 0 {
					t.Fatalf("left behind: %+v", st)
				}
			})
		}
	}
}

// A machine restarted with a task in backoff or waiting for its group runs it
// at once: timers and the breaker are not durable.
func TestRestartRunsBackoffAndGroupWaitsAtOnce(t *testing.T) {
	store := mem.NewMemory()
	for i, w := range []task.WaitReason{task.WaitBackoff, task.WaitGroup} {
		tc := &task.TCB{
			ID: []string{"backoff", "group"}[i], State: task.Waiting, Wait: w, PC: task.StepModel, Class: neumann.DefaultClass,
			WakeAt: clock0.Add(time.Hour), Retries: 2, Regs: []task.Message{task.Text(task.RoleUser, "A")}, Version: 1,
		}
		if _, err := store.Commit(bg, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
			t.Fatal(err)
		}
	}
	m, _, _ := errMachine(t, neumann.Config{Store: store})
	for _, id := range []string{"backoff", "group"} {
		if got := waitDone(t, m, id); got.State != task.Done || got.Retries != 0 {
			t.Fatalf("%s ended %s, retries %d", id, got.State, got.Retries)
		}
	}
}

// A graceful Stop does not wait out a throttled vCore: the wait committed
// nothing, so the task goes back unchanged and the next start runs it.
func TestStopDoesNotWaitForAThrottledTask(t *testing.T) {
	store := mem.NewMemory()
	clk := sim.NewClock(clock0)
	sm := &sim.Model{Fn: func(req model.Request) (model.Response, error) { return sim.Reply("ok"), nil }}
	m, err := neumann.New(neumann.Config{Model: sm, Store: store, Clock: clk, Logf: t.Logf,
		Limiters: gateway(1, 24*time.Hour), Limits: []string{"gw"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	first := submit(t, m, proto.Instruction{Input: "first"})
	waitDone(t, m, first)
	b := submit(t, m, proto.Instruction{Input: "B"})
	waitFor(t, func() bool { return m.Status().Limiters[0].Waiting == 1 })
	ctx, cancel := context.WithTimeout(bg, 2*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got, _ := store.Load(bg, b); got.State != task.Ready || got.Version != 1 || len(machineEventsIn(t, store, proto.EvThrottled)) != 0 {
		t.Fatalf("B left %s at version %d", got.State, got.Version)
	}
	m2, _, _ := errMachine(t, neumann.Config{Store: store})
	if got := waitDone(t, m2, b); got.State != task.Done {
		t.Fatalf("B ended %s", got.State)
	}
}

func machineEventsIn(t *testing.T, s mem.Store, kind string) []proto.Event {
	t.Helper()
	all, _ := s.Events(bg, 0, 0)
	var out []proto.Event
	for _, ev := range all {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}
