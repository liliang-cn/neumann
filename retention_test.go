package neumann_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// logLines is a Logf that keeps what it is given, and passes it on to t.
type logLines struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (l *logLines) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	l.t.Log(line)
}

func (l *logLines) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

// retentionStores are the built-in stores, on the test's clock.
func retentionStores(t *testing.T, clk *sim.Clock) map[string]func() mem.Store {
	return map[string]func() mem.Store{
		"memory": func() mem.Store {
			s := mem.NewMemory()
			s.Now = clk.Now
			return s
		},
		"sqlite": func() mem.Store {
			s, err := sqlite.Open(filepath.Join(t.TempDir(), "neumann.db"), sqlite.WithClock(clk.Now))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		},
	}
}

// ageUntil moves the clock a minute at a time, so that the pruning loop's
// timer fires whenever it was set, until cond holds.
func ageUntil(t *testing.T, clk *sim.Clock, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		clk.Advance(time.Minute)
		time.Sleep(2 * time.Millisecond)
	}
}

func logOf(t *testing.T, m *neumann.Machine) []proto.Event {
	t.Helper()
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// Retention deletes a finished task's events and old machine-level ones,
// keeps every event of a task that runs longer than the retention age, and
// deletes that task's too once it has finished and its last event is old.
func TestRetentionPrunesFinishedTasksOnly(t *testing.T) {
	for _, name := range []string{"memory", "sqlite"} {
		t.Run(name, func(t *testing.T) {
			clk := sim.NewClock(clock0)
			newStore := retentionStores(t, clk)[name]
			release := make(chan struct{})
			var once sync.Once
			free := func() { once.Do(func() { close(release) }) }
			fn := func(ctx context.Context, req model.Request) (model.Response, error) {
				if sim.FirstUser(req) == "long" {
					select {
					case <-release:
					case <-ctx.Done():
						return model.Response{}, ctx.Err()
					}
				}
				return sim.Reply("ok"), nil
			}
			m := startWith(t, neumann.Config{Model: ctxModel{fn: fn}, Cores: 2, Store: newStore(), Clock: clk, EventRetention: time.Hour})
			t.Cleanup(free) // before the machine stops

			short := submit(t, m, proto.Instruction{Input: "short"})
			waitDone(t, m, short)
			long := submit(t, m, proto.Instruction{Input: "long"})
			waitFor(t, func() bool {
				return slices.ContainsFunc(eventsOf(t, m, long), func(e proto.Event) bool { return e.Kind == proto.EvModelCall })
			})
			longEvents := eventsOf(t, m, long)

			clk.Advance(2 * time.Hour)
			ageUntil(t, clk, func() bool { return len(eventsOf(t, m, short)) == 0 })
			if got := eventsOf(t, m, long); !slices.EqualFunc(got, longEvents, func(a, b proto.Event) bool { return a.Seq == b.Seq && a.Kind == b.Kind }) {
				t.Fatalf("the running task's events: %v, want %v", got, longEvents)
			}
			cut := clk.Now().Add(-time.Hour)
			all := logOf(t, m)
			for i, e := range all {
				if e.TaskID == "" && e.Time.Before(cut) && i != len(all)-1 {
					t.Fatalf("an old machine-level event is left: %+v", e)
				}
			}
			// Counted once the store call that pruned them returns.
			waitFor(t, func() bool { return m.Status().EventsPruned > 0 })

			free()
			waitDone(t, m, long)
			after := submit(t, m, proto.Instruction{Input: "after"})
			waitDone(t, m, after)
			clk.Advance(2 * time.Hour)
			ageUntil(t, clk, func() bool { return len(eventsOf(t, m, long)) == 0 })

			// What is left is in seq order, the newest event among it.
			all = logOf(t, m)
			if len(all) == 0 {
				t.Fatal("the newest event was pruned")
			}
			for i := 1; i < len(all); i++ {
				if all[i].Seq <= all[i-1].Seq {
					t.Fatalf("events out of order: %d after %d", all[i].Seq, all[i-1].Seq)
				}
			}
			// The head was kept, so no seq is handed out again.
			next := submit(t, m, proto.Instruction{Input: "next"})
			if evs := eventsOf(t, m, next); evs[0].Seq <= all[len(all)-1].Seq {
				t.Fatalf("new task's first seq %d, not after the head %d", evs[0].Seq, all[len(all)-1].Seq)
			}
		})
	}
}

// With no retention nothing is deleted, however old.
func TestNoRetentionKeepsEverything(t *testing.T) {
	clk := sim.NewClock(clock0)
	store := mem.NewMemory()
	store.Now = clk.Now
	m := startWith(t, neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }}, Store: store, Clock: clk})
	for i := range 3 {
		waitDone(t, m, submit(t, m, proto.Instruction{Input: fmt.Sprint(i)}))
		clk.Advance(30 * 24 * time.Hour)
	}
	time.Sleep(20 * time.Millisecond)
	all := logOf(t, m)
	for i, e := range all {
		if e.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d: the log has a gap", i, e.Seq)
		}
	}
	if st := m.Status(); st.EventsPruned != 0 {
		t.Fatalf("pruned %d", st.EventsPruned)
	}
}

// keepAll is a Store without pruning.
type keepAll struct{ mem.Store }

func TestRetentionConfig(t *testing.T) {
	sm := &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }}
	if _, err := neumann.New(neumann.Config{Model: sm, EventRetention: -time.Second}); err == nil {
		t.Fatal("negative retention accepted")
	}
	_, err := neumann.New(neumann.Config{Model: sm, Store: keepAll{mem.NewMemory()}, EventRetention: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "EventPruner") {
		t.Fatalf("retention over a store that cannot prune: %v", err)
	}
	if _, err := neumann.New(neumann.Config{Model: sm, Store: keepAll{mem.NewMemory()}}); err != nil {
		t.Fatalf("a store that cannot prune, without retention: %v", err)
	}
}

// A subscriber that resumes from a seq whose events were pruned gets the
// kept events after it, in order, without hanging, and the machine logs the
// gap once; one that starts from 0 gets what is kept, with no warning.
func TestEventsAfterAPrunedSeq(t *testing.T) {
	clk := sim.NewClock(clock0)
	store := mem.NewMemory()
	store.Now = clk.Now
	logs := &logLines{t: t}
	m := startWith(t, neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store: store, Clock: clk, EventRetention: time.Hour, Logf: logs.logf})
	var first []string
	for i := range 3 {
		id := submit(t, m, proto.Instruction{Input: fmt.Sprint(i)})
		waitDone(t, m, id)
		first = append(first, id)
	}
	saved := eventsOf(t, m, first[1])[0].Seq // a reader stopped here
	clk.Advance(2 * time.Hour)
	young := submit(t, m, proto.Instruction{Input: "young"})
	waitDone(t, m, young)
	ageUntil(t, clk, func() bool { return len(eventsOf(t, m, first[2])) == 0 })

	kept, err := m.EventLog(bg, saved, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) == 0 || kept[0].Seq <= saved+1 {
		t.Fatalf("EventLog after %d: %v, want the kept events after a gap", saved, kept)
	}
	read := func(from int64) []proto.Event {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		var got []proto.Event
		for ev := range m.Events(ctx, from) {
			got = append(got, ev)
			if len(got) == len(kept) {
				break
			}
		}
		return got
	}
	got := read(saved)
	if !slices.EqualFunc(got, kept, func(a, b proto.Event) bool { return a.Seq == b.Seq }) {
		t.Fatalf("Events from %d: %d events, want the %d kept", saved, len(got), len(kept))
	}
	if n := logs.count("were pruned"); n != 1 {
		t.Fatalf("logged the gap %d times, want once", n)
	}
	kept = logOf(t, m) // read from 0 until as many
	if got := read(0); len(got) != len(kept) {
		t.Fatalf("Events from 0: %d events, want %d", len(got), len(kept))
	}
	if n := logs.count("were pruned"); n != 1 {
		t.Fatalf("reading from 0 logged a gap (%d lines)", n)
	}
}

// slowPruner holds every prune until its context ends, then takes linger
// more to return, as a store call in flight finishing its transaction
// would; it fails every one when failing is set, and returns at once,
// pruning nothing, when quick is set.
type slowPruner struct {
	*mem.Memory
	failing bool
	quick   bool
	linger  time.Duration
	entered chan struct{}
	once    sync.Once
	active  atomic.Int32
	calls   atomic.Int32
}

func (s *slowPruner) PruneEvents(ctx context.Context, before time.Time, after int64, limit int) (int, int64, error) {
	s.calls.Add(1)
	switch {
	case s.failing:
		return 0, 0, errors.New("disk on fire")
	case s.quick:
		return 0, 0, nil
	}
	s.active.Add(1)
	defer s.active.Add(-1)
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	time.Sleep(s.linger)
	return 0, 0, ctx.Err()
}

// Stop waits for the store call in flight even when its own context has
// already ended: once it returns, retention touches the store no more.
func TestStopWaitsForThePruneWhateverItsContext(t *testing.T) {
	store := &slowPruner{Memory: mem.NewMemory(), entered: make(chan struct{}), linger: 50 * time.Millisecond}
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store: store, EventRetention: time.Hour, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	<-store.entered
	ctx, cancel := context.WithCancel(bg)
	cancel()
	m.Stop(ctx) // its error is about the deadline, not retention
	if n := store.active.Load(); n != 0 {
		t.Fatalf("%d prunes still running after Stop", n)
	}
}

// However short EventRetention is, passes are at least a second apart.
func TestRetentionPassesAreASecondApart(t *testing.T) {
	store := &slowPruner{Memory: mem.NewMemory(), quick: true}
	startWith(t, neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store: store, EventRetention: time.Microsecond})
	time.Sleep(300 * time.Millisecond)
	if n := store.calls.Load(); n != 1 {
		t.Fatalf("%d passes in 300ms, want the one Start runs", n)
	}
}

// Stop cancels a prune in flight and waits for it: none runs once Stop has
// returned, and none starts after.
func TestStopDuringAPrune(t *testing.T) {
	store := &slowPruner{Memory: mem.NewMemory(), entered: make(chan struct{})}
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store: store, EventRetention: time.Hour, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	<-store.entered // the pass Start runs
	id := submit(t, m, proto.Instruction{Input: "q"})
	waitDone(t, m, id) // commits go on while a prune is in flight
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	began := time.Now()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if d := time.Since(began); d > time.Second {
		t.Fatalf("stop took %v", d)
	}
	if n := store.active.Load(); n != 0 {
		t.Fatalf("%d prunes still running after Stop", n)
	}
	calls := store.calls.Load()
	time.Sleep(20 * time.Millisecond)
	if store.calls.Load() != calls {
		t.Fatal("a prune started after Stop")
	}
}

// A store that fails every prune fails no commit: the error is logged and
// the machine goes on.
func TestPruneErrorsFailNoCommit(t *testing.T) {
	clk := sim.NewClock(clock0)
	store := &slowPruner{Memory: mem.NewMemory(), failing: true}
	logs := &logLines{t: t}
	m := startWith(t, neumann.Config{Model: &sim.Model{Fn: func(model.Request) (model.Response, error) { return sim.Reply("ok"), nil }},
		Store: store, Clock: clk, EventRetention: time.Hour, Logf: logs.logf})
	for i := range 3 {
		waitDone(t, m, submit(t, m, proto.Instruction{Input: fmt.Sprint(i)}))
	}
	ageUntil(t, clk, func() bool { return logs.count("disk on fire") >= 2 })
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "after"}))
}
