package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var errCrashed = errors.New("process died")

// crashStore is a process that dies after a number of commits: from then on
// nothing it does is recorded, though the work it already started (a tool
// call) may still happen in the world. With after set, the commit that kills
// it is durable, but the process dies before its caller hears so: the caller
// never acts on it (design §18 test 7, a crash after a commit point).
type crashStore struct {
	mem.Store
	after   bool
	left    atomic.Int64
	crashed atomic.Bool
	commits atomic.Int64
}

func (c *crashStore) Commit(ctx context.Context, cs mem.CommitSet) ([]proto.Event, error) {
	if c.crashed.Load() {
		return nil, errCrashed
	}
	if n := c.left.Add(-1); n < 0 {
		c.crashed.Store(true)
		if c.after && n == -1 {
			if _, err := c.Store.Commit(ctx, cs); err == nil {
				c.commits.Add(1)
			}
		}
		return nil, errCrashed
	}
	c.commits.Add(1)
	return c.Store.Commit(ctx, cs)
}

// crash kills the process now: the next commit, and every one after it, fails.
func (c *crashStore) crash() { c.crashed.Store(true) }

// world is what the tools do outside the machine; it outlives both processes.
type world struct {
	pays  atomic.Int32
	mu    sync.Mutex
	keys  map[string][]string // call -> idempotency keys it was sent with
	m     atomic.Pointer[neumann.Machine]
	store mem.Store // the running process's store, guarded by mu
	clock *sim.Clock

	deadline  time.Time    // the task's, if it has one
	afterDead atomic.Int32 // device calls that started once the deadline had passed

	flaky bool         // the machines meter their calls through a limiter, and time runs
	fails atomic.Int32 // model calls after the tool round so far (flakyModel)
}

func (w *world) setStore(s mem.Store) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.store = s
}

// cancelBehindTheMachine commits a cancel straight to the store: durable, but
// no owner has been signalled yet (the gap inside Machine.Cancel).
func (w *world) cancelBehindTheMachine(id string) {
	w.mu.Lock()
	s := w.store
	w.mu.Unlock()
	s.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: id, Kind: mem.InterruptCancel, Text: "cancelled"}}})
}

func newWorld() *world {
	return &world{keys: map[string][]string{}, clock: sim.NewClock(time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC))}
}

func (w *world) note(c bus.DeviceCall) {
	if !w.deadline.IsZero() && !w.clock.Now().Before(w.deadline) {
		w.afterDead.Add(1)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	call := c.Key[:strings.LastIndex(c.Key, "/")] // task/call, without /attempt
	w.keys[call] = append(w.keys[call], c.Key)
}

func (w *world) devices() []bus.Device {
	return []bus.Device{builtin.New("dev").
		Add(bus.ToolDef{Name: "pay"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			w.note(c)
			w.pays.Add(1)
			return "paid", nil
		}).
		Add(bus.ToolDef{Name: "look", Effect: bus.ReadOnly}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			w.note(c)
			return "balance 10", nil
		}).
		// cpay pays, then the user cancels the task while the payment is
		// still in flight; it answers only once the machine has let go.
		Add(bus.ToolDef{Name: "cpay"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
			w.note(c)
			w.pays.Add(1)
			if err := w.m.Load().Cancel(bg, c.Key[:strings.Index(c.Key, "/")]); err != nil {
				return "paid", nil // the process died: nothing will cancel this call
			}
			<-ctx.Done()
			return "paid", nil
		}).
		// xlook reads, and takes so long that the task's deadline passes.
		Add(bus.ToolDef{Name: "xlook", Effect: bus.ReadOnly}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			w.note(c)
			w.clock.Advance(time.Hour)
			return "balance 10", nil
		}).
		// dpay pays and takes so long that the task's deadline passes.
		Add(bus.ToolDef{Name: "dpay"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			w.note(c)
			w.pays.Add(1)
			// The deadline fires, and (unless the process died) cancels this
			// call, before Advance returns.
			w.clock.Advance(time.Hour)
			return "paid", nil
		})}
}

func payModel(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) == 0 {
		return sim.CallTools("dev_pay", "dev_look"), nil
	}
	return sim.Reply("done"), nil
}

// lookThenPay reads, then pays with tool, then answers.
func lookThenPay(tool string) modelFn {
	return func(req model.Request) (model.Response, error) {
		switch sim.ToolRounds(req) {
		case 0:
			return sim.CallTools("dev_look"), nil
		case 1:
			return sim.CallTools(tool), nil
		}
		return sim.Reply("done"), nil
	}
}

// crashScenario is one task the crash sweep runs, and how it may end.
type crashScenario struct {
	name     string
	model    func(w *world) modelFn
	deadline time.Duration // from the world clock's start; 0 = none
	ends     map[task.State]string
	maxPays  int32
	flaky    bool   // time runs, and calls go through a limiter
	retries  string // the model_retry classes a clean run must show, comma-separated
}

func fixed(fn modelFn) func(*world) modelFn { return func(*world) modelFn { return fn } }

// cancelInTheGap is a model step during which the task is cancelled, the
// cancel committed but its owner not yet told; the step then answers with
// resp.
func cancelInTheGap(resp model.Response) func(*world) modelFn {
	return func(w *world) modelFn {
		return func(req model.Request) (model.Response, error) {
			if sim.ToolRounds(req) > 0 {
				return sim.Reply("done"), nil
			}
			w.cancelBehindTheMachine("pay-1")
			return resp, nil
		}
	}
}

var (
	payScenario    = crashScenario{name: "pay", model: fixed(payModel), maxPays: 1, ends: map[task.State]string{task.Done: "end_turn"}}
	cancelScenario = crashScenario{name: "cancel", model: fixed(lookThenPay("dev_cpay")), maxPays: 1,
		ends: map[task.State]string{task.Done: "end_turn", task.Cancelled: "cancelled"}}
	deadlineScenario = crashScenario{name: "deadline", model: fixed(lookThenPay("dev_dpay")), deadline: 30 * time.Minute, maxPays: 1,
		ends: map[task.State]string{task.Done: "end_turn", task.Failed: "deadline"}}
	// The cancel lands while the model is asked for a payment: the payment
	// must never be sent.
	gapToolsScenario = crashScenario{name: "gap_tools", model: cancelInTheGap(sim.CallTools("dev_pay")), maxPays: 0,
		ends: map[task.State]string{task.Cancelled: "cancelled"}}
	// The cancel lands while the model writes its answer: it must not finish
	// over the cancel.
	gapAnswerScenario = crashScenario{name: "gap_answer", model: cancelInTheGap(sim.Reply("done")),
		ends: map[task.State]string{task.Cancelled: "cancelled"}}
	// The deadline passes inside a read-only call: a restart finds that call
	// re-sendable, and must not send it.
	expireScenario = crashScenario{name: "expire", model: fixed(lookThenPay("dev_xlook")), deadline: 30 * time.Minute,
		ends: map[task.State]string{task.Failed: "deadline"}}
	// After the payment, the provider fails once transiently and then rate
	// limits the task: two backoffs, a limiter pause and two wake-ups, each a
	// commit point.
	flakyScenario = crashScenario{name: "flaky", model: flakyModel, flaky: true, maxPays: 1, retries: "transient,rate_limited",
		ends: map[task.State]string{task.Done: "end_turn"}}
	// After the payment, the account's quota runs out: the group goes down
	// and the task waits for it, so a restart recovers it from
	// Waiting(Group). A machine that sees the group go down resets it, as an
	// operator would.
	quotaScenario = crashScenario{name: "quota", model: quotaModel, maxPays: 1, retries: "quota_exhausted",
		ends: map[task.State]string{task.Done: "end_turn"}}
)

// flakyModel pays, then fails transiently, then is rate limited, then
// answers. The failures are counted in the world, so a restart does not
// repeat the ones already seen.
func flakyModel(w *world) modelFn {
	return func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_pay", "dev_look"), nil
		}
		switch w.fails.Add(1) {
		case 1:
			return model.Response{}, sim.Transient()
		case 2:
			return model.Response{}, sim.RateLimited(time.Second)
		}
		return sim.Reply("done"), nil
	}
}

// quotaModel pays, then finds the quota exhausted once, then answers.
func quotaModel(w *world) modelFn {
	return func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_pay", "dev_look"), nil
		}
		if w.fails.Add(1) == 1 {
			return model.Response{}, sim.QuotaExhausted()
		}
		return sim.Reply("done"), nil
	}
}

// tick moves the world's clock a second every millisecond, so backoffs and
// limiter pauses run out, until stop is called.
func (w *world) tick() (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
				w.clock.Advance(time.Second)
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

func newMachine(t *testing.T, store mem.Store, w *world, fn modelFn) *neumann.Machine {
	t.Helper()
	if fn == nil {
		fn = payModel
	}
	w.setStore(store)
	cfg := neumann.Config{
		Model: &sim.Model{Fn: fn}, Store: store, Devices: w.devices(), Cores: 2, Clock: w.clock,
		SteerFormat: func(s string) string { return s }, Logf: func(string, ...any) {},
		BackoffJitter: func(d time.Duration) time.Duration { return d },
	}
	if w.flaky {
		cfg.Limiters = []limit.Config{{Name: "gw", Limits: map[string]limit.Rate{"requests": {Per: time.Minute, Limit: 100}}}}
		cfg.Limits = []string{"gw"}
	}
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w.m.Store(m)
	return m
}

// quiesce waits until the dead machine has nothing left in flight.
func quiesce(t *testing.T, m *neumann.Machine, store mem.Store, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tc, err := store.Load(bg, id); err == nil && tc.State.Terminal() {
			return
		}
		st := m.Status()
		if st.Ready+st.Waiting+st.Held == 0 {
			return
		}
		// Waiting for a group that only an operator brings back: nothing
		// more happens in this process.
		if tc, err := store.Load(bg, id); err == nil && tc.State == task.Waiting && tc.Wait == task.WaitGroup && st.Ready+st.Held == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("machine never went quiet: %+v", m.Status())
}

func stop(t *testing.T, m *neumann.Machine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// crashRun is what one run of the sweep saw.
type crashRun struct {
	commits   int64    // made by the first machine
	resolved  bool     // the second machine needed an operator
	recovered bool     // the second machine finished a cancel the first one committed
	unsent    bool     // a read-only call it may have re-sent was not, and is unknown
	fromGroup bool     // the second machine recovered the task from Waiting(Group)
	retries   []string // the classes of the model_retry events, in order
}

// runAndCrash runs sc on a machine that dies after n commits (after: right
// after the n+1-th lands), then starts a fresh machine on the same database
// and lets it finish, resolving any held call as an operator would.
func runAndCrash(t *testing.T, sc crashScenario, n int64, after bool) (run crashRun) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	w := newWorld()
	if sc.flaky {
		w.flaky = true
		defer w.tick()()
	}

	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cs := &crashStore{Store: s1, after: after}
	cs.left.Store(n)
	m1 := newMachine(t, cs, w, sc.model(w))
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	in := proto.Instruction{Kind: proto.KindSubmit, TaskID: "pay-1", Input: "pay the bill"}
	if sc.deadline > 0 {
		w.deadline = w.clock.Now().Add(sc.deadline)
		in.Budget = &task.Budget{Deadline: w.deadline}
	}
	id, submitErr := m1.Submit(bg, in)
	if submitErr != nil && !errors.Is(submitErr, errCrashed) {
		t.Fatalf("submit: %v", submitErr)
	}
	if submitErr == nil {
		quiesce(t, m1, s1, id)
	}
	stop(t, m1)
	s1.Close()
	run.commits = cs.commits.Load()

	s2, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, lerr := s2.Load(bg, in.TaskID); errors.Is(lerr, mem.ErrNotFound) {
		if submitErr == nil {
			t.Fatalf("crash after %d commits: an accepted task is gone", n)
		}
		return run // never accepted, never there: nothing to recover
	}
	id = in.TaskID
	m2 := newMachine(t, s2, w, sc.model(w))
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	events := m2.Events(ctx, 0)
	if err := m2.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m2)
	for ev := range events {
		if ev.Kind == proto.EvGroupDown {
			if err := m2.GroupReset(bg, neumann.DefaultClass); err != nil {
				t.Fatalf("group reset: %v", err)
			}
		}
		if ev.TaskID != id {
			continue
		}
		var d struct {
			CallID    string `json:"call_id"`
			Cancelled bool   `json:"cancelled"`
			Cancel    string `json:"cancel"`
			From      string `json:"from"`
		}
		json.Unmarshal(ev.Data, &d)
		if ev.Kind == proto.EvRecovered && d.Cancel != "" {
			run.recovered = true
		}
		if ev.Kind == proto.EvRecovered && d.From == "waiting_group" {
			run.fromGroup = true
		}
		if ev.Kind == proto.EvReconcileNeeded && !d.Cancelled {
			// The operator checks the bank: the payment went through exactly
			// when the device ran.
			result := "not paid"
			if w.pays.Load() > 0 {
				result = "paid"
			}
			err := m2.Resolve(bg, id, d.CallID, neumann.Outcome{Result: result})
			switch {
			case err == nil:
				run.resolved = true
			case errors.Is(err, neumann.ErrNotReconciling):
				// The deadline took the task first.
			default:
				t.Fatalf("resolve: %v", err)
			}
		}
		if ev.Terminal() {
			break
		}
	}

	got, err := s2.Load(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	if reason, ok := sc.ends[got.State]; !ok || reason != got.StopReason {
		t.Fatalf("crash after %d commits (after=%v): task ended %s %q", n, after, got.State, got.StopReason)
	}
	if got.State == task.Done && (got.LastAnswer() != "done" || got.Retries != 0) {
		t.Fatalf("crash after %d commits: answer %q, retries %d", n, got.LastAnswer(), got.Retries)
	}
	if p := w.pays.Load(); p > sc.maxPays {
		t.Fatalf("crash after %d commits (after=%v): the payment ran %d times", n, after, p)
	}
	if k := w.afterDead.Load(); k > 0 {
		t.Fatalf("crash after %d commits (after=%v): %d calls started after the deadline", n, after, k)
	}
	recs, _ := s2.Calls(bg, id)
	for _, r := range recs {
		if r.Status == mem.CallUnknown && r.Effect == task.ReadOnly {
			run.unsent = true
		}
		w.mu.Lock()
		sent := len(w.keys[id+"/"+r.CallID])
		w.mu.Unlock()
		if r.Status == mem.CallCancelled && sent > 0 {
			t.Fatalf("crash after %d commits (after=%v): call %s is recorded as never sent, but a device saw it %d times", n, after, r.CallID, sent)
		}
	}
	w.mu.Lock()
	for call, keys := range w.keys {
		for _, k := range keys[1:] {
			if k != keys[0] {
				t.Fatalf("crash after %d commits (after=%v): %s was resent under a new key: %v", n, after, call, keys)
			}
		}
	}
	w.mu.Unlock()
	checkEventsHaveState(t, s2, id)
	all, _ := s2.Events(bg, 0, 0)
	for _, ev := range all {
		if ev.Kind == proto.EvModelRetry {
			var d struct {
				Class string `json:"class"`
			}
			json.Unmarshal(ev.Data, &d)
			run.retries = append(run.retries, d.Class)
		}
	}
	return run
}

// checkEventsHaveState is the other half of design §18 test 7: every event in
// the log has the state it describes, so no event was ever visible without it.
func checkEventsHaveState(t *testing.T, s mem.Store, id string) {
	t.Helper()
	// Events first: a late result may still be landing, and the state read
	// after them is at least as new as they are.
	all, _ := s.Events(bg, 0, 0)
	tc, err := s.Load(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	recs, _ := s.Calls(bg, id)
	byID := map[string]mem.CallRecord{}
	for _, r := range recs {
		byID[r.CallID] = r
	}
	results := map[string]int{}
	terminal := 0
	for _, ev := range all {
		if ev.TaskID != id {
			continue
		}
		var d struct {
			CallID string `json:"call_id"`
		}
		json.Unmarshal(ev.Data, &d)
		r, known := byID[d.CallID]
		switch ev.Kind {
		case proto.EvToolCall:
			if !known {
				t.Fatalf("tool_call %s has no call record", d.CallID)
			}
		case proto.EvToolResult:
			results[d.CallID]++
			if !known || r.Result == nil {
				t.Fatalf("tool_result %s, but the record is %+v", d.CallID, r)
			}
		case proto.EvToolLate:
			if !known || r.Status != mem.CallLate || r.Result == nil {
				t.Fatalf("tool_late %s, but the record is %+v", d.CallID, r)
			}
		case proto.EvReconcileNeeded:
			if !known || r.Attempt < 1 {
				t.Fatalf("reconcile_needed %s, but the record is %+v", d.CallID, r)
			}
		case proto.EvDone, proto.EvFailed, proto.EvCancelled:
			terminal++
			want := map[string]task.State{proto.EvDone: task.Done, proto.EvFailed: task.Failed, proto.EvCancelled: task.Cancelled}[ev.Kind]
			if tc.State != want {
				t.Fatalf("%s event, but the task is %s", ev.Kind, tc.State)
			}
		}
	}
	if terminal != 1 {
		t.Fatalf("%d terminal events", terminal)
	}
	for call, k := range results {
		if k != 1 {
			t.Fatalf("%d tool_result events for %s", k, call)
		}
	}
	for _, r := range recs {
		switch r.Status {
		case mem.CallPlanned, mem.CallDispatched:
			t.Fatalf("finished task left call %s %s", r.CallID, r.Status)
		}
	}
}

// sweep is design §18 test 7: the machine dies before, and in a second pass
// right after, each possible number of commits, and a fresh machine on the
// same database always finishes the task without paying twice.
func sweep(t *testing.T, sc crashScenario) (points int, resolved, recovered int) {
	points, resolved, recovered, _, _ = sweepAll(t, sc)
	return points, resolved, recovered
}

func sweepAll(t *testing.T, sc crashScenario) (points int, resolved, recovered, unsent, fromGroup int) {
	clean := runAndCrash(t, sc, 1<<30, false) // a clean run: how many commits there are
	if clean.commits < 3 {
		t.Fatalf("a clean run made only %d commits", clean.commits)
	}
	if strings.Join(clean.retries, ",") != sc.retries {
		t.Fatalf("the clean run retried %v", clean.retries)
	}
	var mu sync.Mutex
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%v", after), func(t *testing.T) {
			for n := int64(0); n <= clean.commits; n++ {
				t.Run(fmt.Sprintf("crash_%d", n), func(t *testing.T) {
					t.Parallel()
					run := runAndCrash(t, sc, n, after)
					mu.Lock()
					defer mu.Unlock()
					points++
					if run.resolved {
						resolved++
					}
					if run.recovered {
						recovered++
					}
					if run.unsent {
						unsent++
					}
					if run.fromGroup {
						fromGroup++
					}
				})
			}
		})
	}
	return points, resolved, recovered, unsent, fromGroup
}

func TestCrashAtEveryCommitPoint(t *testing.T) {
	points, held, _ := sweep(t, payScenario)
	if held == 0 {
		t.Fatal("no crash point left the payment's outcome unknown: the test does not reach the dangerous window")
	}
	t.Logf("%d crash points; the operator was needed at %d of them", points, held)
}

// The same sweep with the task cancelled while its payment is in flight.
func TestCrashAtEveryCommitPointWhileCancelling(t *testing.T) {
	points, _, recovered := sweep(t, cancelScenario)
	if recovered == 0 {
		t.Fatal("no crash point fell between the cancel and its final commit")
	}
	t.Logf("%d crash points; recovery finished the cancel at %d of them", points, recovered)
}

// The same sweep with the task's deadline passing while its payment is in
// flight.
func TestCrashAtEveryCommitPointWithADeadline(t *testing.T) {
	points, _, recovered := sweep(t, deadlineScenario)
	if recovered == 0 {
		t.Fatal("no crash point fell between the deadline and its final commit")
	}
	t.Logf("%d crash points; recovery finished the deadline at %d of them", points, recovered)
}

func TestStopHonoursItsDeadlineWithASubmitInFlight(t *testing.T) {
	m, _ := start(t, neumann.Config{Store: slowStore{mem.NewMemory(), 1500 * time.Millisecond}},
		func(model.Request) (model.Response, error) { return sim.Reply("x"), nil })
	go m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "slow"})
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	m.Stop(ctx)
	if d := time.Since(begin); d > time.Second {
		t.Fatalf("Stop took %v with a 100ms deadline", d)
	}
}

func TestNothingIsAcceptedBeforeStart(t *testing.T) {
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: payModel}, Store: mem.NewMemory(), Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x"}); !errors.Is(err, neumann.ErrNotStarted) {
		t.Fatalf("submit: %v", err)
	}
	if err := m.Steer(bg, "t1", "x"); !errors.Is(err, neumann.ErrNotStarted) {
		t.Fatalf("steer: %v", err)
	}
	if err := m.Resolve(bg, "t1", "c-1", neumann.Outcome{Result: "x"}); !errors.Is(err, neumann.ErrNotStarted) {
		t.Fatalf("resolve: %v", err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	if _, err := m.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, Input: "x"}); err != nil {
		t.Fatalf("submit after start: %v", err)
	}
}

// The same sweep with the cancel landing inside a model step, between its
// commit and the signal to the step's vCore: once while the model asks for a
// payment, once while it answers.
func TestCrashAtEveryCommitPointWithACancelInTheGap(t *testing.T) {
	for _, sc := range []crashScenario{gapToolsScenario, gapAnswerScenario} {
		t.Run(sc.name, func(t *testing.T) {
			points, _, recovered := sweep(t, sc)
			t.Logf("%d crash points; recovery finished the cancel at %d of them", points, recovered)
		})
	}
}

// The same sweep with the model failing transiently and then rate limited
// after the payment: a crash at every commit of the backoffs and wake-ups,
// before and after it lands, still finishes the task once, paying once.
func TestCrashAtEveryCommitPointWithModelFailures(t *testing.T) {
	points, held, _ := sweep(t, flakyScenario)
	if held == 0 {
		t.Fatal("no crash point left the payment's outcome unknown")
	}
	t.Logf("%d crash points; the operator was needed at %d of them", points, held)
}

// The same sweep with the quota running out after the payment: the first
// process leaves the task in Waiting(Group), and every crash point, before
// and after its commit, still finishes the task once, paying once.
func TestCrashAtEveryCommitPointWithTheGroupDown(t *testing.T) {
	points, held, _, _, fromGroup := sweepAll(t, quotaScenario)
	if held == 0 {
		t.Fatal("no crash point left the payment's outcome unknown")
	}
	if fromGroup == 0 {
		t.Fatal("no crash point left the task in Waiting(Group) for the restart")
	}
	t.Logf("%d crash points; the operator was needed at %d; recovered from Waiting(Group) at %d", points, held, fromGroup)
}

// The same sweep with the deadline passing inside a read-only call, so that
// a restart finds the call re-sendable after the deadline.
func TestCrashAtEveryCommitPointPastTheDeadline(t *testing.T) {
	points, _, recovered, unsent, _ := sweepAll(t, expireScenario)
	if unsent == 0 {
		t.Fatal("no crash point left a re-sendable call behind a passed deadline")
	}
	t.Logf("%d crash points; recovery finished the deadline at %d; a re-send was held back at %d", points, recovered, unsent)
}
