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
	clock *sim.Clock
}

func newWorld() *world {
	return &world{keys: map[string][]string{}, clock: sim.NewClock(time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC))}
}

func (w *world) note(c bus.DeviceCall) {
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
	model    modelFn
	deadline time.Duration // from the world clock's start; 0 = none
	ends     map[task.State]string
}

var (
	payScenario    = crashScenario{name: "pay", model: payModel, ends: map[task.State]string{task.Done: "end_turn"}}
	cancelScenario = crashScenario{name: "cancel", model: lookThenPay("dev_cpay"),
		ends: map[task.State]string{task.Done: "end_turn", task.Cancelled: "cancelled"}}
	deadlineScenario = crashScenario{name: "deadline", model: lookThenPay("dev_dpay"), deadline: 30 * time.Minute,
		ends: map[task.State]string{task.Done: "end_turn", task.Failed: "deadline"}}
)

func newMachine(t *testing.T, store mem.Store, w *world, fn modelFn) *neumann.Machine {
	t.Helper()
	if fn == nil {
		fn = payModel
	}
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: fn}, Store: store, Devices: w.devices(), Cores: 2, Clock: w.clock,
		SteerFormat: func(s string) string { return s }, Logf: func(string, ...any) {},
	})
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
	commits   int64 // made by the first machine
	resolved  bool  // the second machine needed an operator
	recovered bool  // the second machine finished a cancel the first one committed
}

// runAndCrash runs sc on a machine that dies after n commits (after: right
// after the n+1-th lands), then starts a fresh machine on the same database
// and lets it finish, resolving any held call as an operator would.
func runAndCrash(t *testing.T, sc crashScenario, n int64, after bool) (run crashRun) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	w := newWorld()

	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cs := &crashStore{Store: s1, after: after}
	cs.left.Store(n)
	m1 := newMachine(t, cs, w, sc.model)
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	in := proto.Instruction{Kind: proto.KindSubmit, TaskID: "pay-1", Input: "pay the bill"}
	if sc.deadline > 0 {
		in.Budget = &task.Budget{Deadline: w.clock.Now().Add(sc.deadline)}
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
	m2 := newMachine(t, s2, w, sc.model)
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	events := m2.Events(ctx, 0)
	if err := m2.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m2)
	for ev := range events {
		if ev.TaskID != id {
			continue
		}
		var d struct {
			CallID    string `json:"call_id"`
			Cancelled bool   `json:"cancelled"`
			Cancel    string `json:"cancel"`
		}
		json.Unmarshal(ev.Data, &d)
		if ev.Kind == proto.EvRecovered && d.Cancel != "" {
			run.recovered = true
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
	if got.State == task.Done && got.LastAnswer() != "done" {
		t.Fatalf("crash after %d commits: answer %q", n, got.LastAnswer())
	}
	if p := w.pays.Load(); p > 1 {
		t.Fatalf("crash after %d commits (after=%v): the payment ran %d times", n, after, p)
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
	clean := runAndCrash(t, sc, 1<<30, false) // a clean run: how many commits there are
	if clean.commits < 5 {
		t.Fatalf("a clean run made only %d commits", clean.commits)
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
				})
			}
		})
	}
	return points, resolved, recovered
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
