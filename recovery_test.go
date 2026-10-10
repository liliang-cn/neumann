package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
// call) may still happen in the world.
type crashStore struct {
	mem.Store
	left    atomic.Int64
	crashed atomic.Bool
	commits atomic.Int64
}

func (c *crashStore) Commit(ctx context.Context, cs mem.CommitSet) ([]proto.Event, error) {
	if c.crashed.Load() || c.left.Add(-1) < 0 {
		c.crashed.Store(true)
		return nil, errCrashed
	}
	c.commits.Add(1)
	return c.Store.Commit(ctx, cs)
}

// world is what the tools do outside the machine; it outlives both processes.
type world struct {
	pays atomic.Int32
	mu   sync.Mutex
	keys map[string][]string // call -> idempotency keys it was sent with
}

func (w *world) devices() []bus.Device {
	note := func(c bus.DeviceCall) {
		w.mu.Lock()
		defer w.mu.Unlock()
		call := c.Key[:len(c.Key)-2] // task/call, without /attempt
		w.keys[call] = append(w.keys[call], c.Key)
	}
	return []bus.Device{builtin.New("dev").
		Add(bus.ToolDef{Name: "pay"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			note(c)
			w.pays.Add(1)
			return "paid", nil
		}).
		Add(bus.ToolDef{Name: "look", Effect: bus.ReadOnly}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			note(c)
			return "balance 10", nil
		})}
}

func payModel(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) == 0 {
		return sim.CallTools("dev_pay", "dev_look"), nil
	}
	return sim.Reply("done"), nil
}

func newMachine(t *testing.T, store mem.Store, w *world) *neumann.Machine {
	t.Helper()
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: payModel}, Store: store, Devices: w.devices(), Cores: 2,
		SteerFormat: func(s string) string { return s }, Logf: func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
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

// runAndCrash runs the payment task on a machine that dies after n commits,
// then starts a fresh machine on the same database and lets it finish,
// resolving any held call as an operator would. It returns the commits the
// first machine made, and whether the second one needed an operator.
func runAndCrash(t *testing.T, n int64) (commits int64, resolved bool) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	w := &world{keys: map[string][]string{}}

	s1, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cs := &crashStore{Store: s1}
	cs.left.Store(n)
	m1 := newMachine(t, cs, w)
	if err := m1.Start(bg); err != nil {
		t.Fatal(err)
	}
	id, err := m1.Submit(bg, proto.Instruction{Kind: proto.KindSubmit, TaskID: "pay-1", Input: "pay the bill"})
	if err == nil {
		quiesce(t, m1, s1, id)
	}
	stop(t, m1)
	s1.Close()
	commits = cs.commits.Load()
	if err != nil {
		if !errors.Is(err, errCrashed) {
			t.Fatalf("submit: %v", err)
		}
		return commits, false
	}

	s2, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2 := newMachine(t, s2, w)
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
		if ev.Kind == proto.EvReconcileNeeded {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(ev.Data, &d)
			// The operator checks the bank: the payment went through exactly
			// when the device ran.
			result := "not paid"
			if w.pays.Load() > 0 {
				result = "paid"
			}
			if err := m2.Resolve(bg, id, d.CallID, neumann.Outcome{Result: result}); err != nil {
				t.Fatalf("resolve: %v", err)
			}
			resolved = true
		}
		if ev.Terminal() {
			break
		}
	}

	got, err := s2.Load(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != task.Done || got.LastAnswer() != "done" {
		t.Fatalf("crash after %d commits: task ended %s %q", n, got.State, got.LastAnswer())
	}
	if p := w.pays.Load(); p > 1 {
		t.Fatalf("crash after %d commits: the payment ran %d times", n, p)
	}
	w.mu.Lock()
	for call, keys := range w.keys {
		for _, k := range keys[1:] {
			if k != keys[0] && call == id+"/c-2" {
				t.Fatalf("crash after %d commits: the read-only call was resent under a new key: %v", n, keys)
			}
		}
	}
	w.mu.Unlock()
	all, _ := s2.Events(bg, 0, 0)
	results := map[string]int{}
	for _, ev := range all {
		if ev.TaskID == id && ev.Kind == proto.EvToolResult {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(ev.Data, &d)
			results[d.CallID]++
		}
	}
	for call, k := range results {
		if k != 1 {
			t.Fatalf("crash after %d commits: %d tool_result events for %s", n, k, call)
		}
	}
	return commits, resolved
}

// TestCrashAtEveryCommitPoint is design §18 test 7: the machine dies after
// each possible number of commits, and a fresh machine on the same database
// always finishes the task without paying twice.
func TestCrashAtEveryCommitPoint(t *testing.T) {
	total, _ := runAndCrash(t, 1<<30) // a clean run: how many commits there are
	if total < 5 {
		t.Fatalf("a clean run made only %d commits", total)
	}
	held := 0
	for n := int64(0); n <= total; n++ {
		t.Run(fmt.Sprintf("after_%d_commits", n), func(t *testing.T) {
			if _, resolved := runAndCrash(t, n); resolved {
				held++
			}
		})
	}
	if held == 0 {
		t.Fatal("no crash point left the payment's outcome unknown: the test does not reach the dangerous window")
	}
	t.Logf("%d commit points; the operator was needed at %d of them", total+1, held)
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
