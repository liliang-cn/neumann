package neumann_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// readKey marks the reads of the Suspend or ResumeTask call a test puts a
// landing in the middle of.
type readKey struct{}

// landingStore lets a test place another holder's landing between the two
// reads a Suspend or ResumeTask makes (the task's state and its
// interrupts, in whichever order): the first read made under readKey
// returns only once until holds, and closes first as it starts waiting.
// pre and post run once, before and after the commit that suspends task
// B.
type landingStore struct {
	mem.Store
	pre, post atomic.Pointer[func()]
	first     chan struct{}
	until     func() bool
	once      sync.Once
}

func newLandingStore() *landingStore {
	return &landingStore{Store: mem.NewMemory(), first: make(chan struct{})}
}

func (s *landingStore) hold(ctx context.Context) {
	if ctx.Value(readKey{}) == nil {
		return
	}
	s.once.Do(func() {
		close(s.first)
		for !s.until() {
			time.Sleep(time.Millisecond)
		}
	})
}

func (s *landingStore) Load(ctx context.Context, id string) (*task.TCB, error) {
	t, err := s.Store.Load(ctx, id)
	s.hold(ctx)
	return t, err
}

func (s *landingStore) Interrupts(ctx context.Context, id string, unapplied bool) ([]mem.Interrupt, error) {
	ins, err := s.Store.Interrupts(ctx, id, unapplied)
	s.hold(ctx)
	return ins, err
}

func (s *landingStore) Commit(ctx context.Context, c mem.CommitSet) ([]proto.Event, error) {
	suspends := c.Task != nil && c.Task.ID == "B" && c.Task.State == task.Suspended
	if suspends {
		if f := s.pre.Swap(nil); f != nil {
			(*f)()
		}
	}
	evs, err := s.Store.Commit(ctx, c)
	if suspends && err == nil {
		if f := s.post.Swap(nil); f != nil {
			(*f)()
		}
	}
	return evs, err
}

// stateOf is B's committed state, read past the hooks.
func (s *landingStore) stateOf(id string) task.State {
	tc, err := s.Store.Load(context.Background(), id)
	if err != nil {
		return task.Ready
	}
	return tc.State
}

// Suspend, resume, suspend, each after the one before returned: the task
// ends suspended, though the resume lands (at its holder's refused
// hand-back) between the two reads of the second suspend. Judged from a
// state read before that landing and pending interrupts read after it,
// the second suspend took the task for suspended already and committed
// nothing, and the task ran to its end (found in review).
func TestSuspendAfterAResumeStillLanding(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	ls := newLandingStore()
	ls.until = func() bool { return ls.stateOf("B") != task.Suspended }
	m := localMachine(t, neumann.Config{Store: ls}, lm, clk)
	again := make(chan error, 1)
	// The vCore is about to commit the cut call's suspend: the operator
	// resumes B (it returns; the vCore holds B, its hand-back lands the
	// resume). Once the suspend is committed, the operator suspends B again,
	// and the vCore goes on to that landing only after the second
	// suspend's first read.
	pre := func() {
		if err := m.ResumeTask(bg, "B"); err != nil {
			t.Errorf("resume: %v", err)
		}
	}
	post := func() {
		go func() { again <- m.Suspend(context.WithValue(bg, readKey{}, 1), "B") }()
		<-ls.first
	}
	ls.pre.Store(&pre)
	ls.post.Store(&post)
	submitAs(t, m, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	select {
	case err := <-again:
		if err != nil {
			t.Fatalf("the second suspend: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second suspend never returned")
	}
	waitSuspended(t, m, "B")
	clk.Advance(time.Hour)
	time.Sleep(10 * time.Millisecond)
	if b := loadTask(t, m, "B"); b.State != task.Suspended {
		t.Fatalf("B after suspend, resume, suspend: %s", b.State)
	}
	resume(t, m, "B")
	drive(t, m, clk, time.Minute, "B")
}

// Suspend, then resume once Suspend has returned: the resume is taken,
// though the suspend lands (the vCore's commit of the cut call) between
// the resume's two reads. Judged from a state read before that landing
// (Running) and pending interrupts read after it (none), the resume was
// refused with ErrNotSuspended and the task stayed suspended (found in
// review).
func TestResumeOfASuspendStillLanding(t *testing.T) {
	clk := sim.NewClock(clock0)
	lm := newLocal(clk, prefillCaps, nil)
	ls := newLandingStore()
	ls.until = func() bool { return ls.stateOf("B") == task.Suspended }
	m := localMachine(t, neumann.Config{Store: ls}, lm, clk)
	res := make(chan error, 1)
	returned := make(chan struct{})
	pre := func() {
		<-returned
		go func() { res <- m.ResumeTask(context.WithValue(bg, readKey{}, 1), "B") }()
		<-ls.first
	}
	ls.pre.Store(&pre)
	submitAs(t, m, "B", 0)
	onAir(t, lm, "B", 1)
	suspend(t, m, "B")
	close(returned)
	if err := <-res; err != nil {
		t.Fatalf("resume after suspend: %v", err)
	}
	drive(t, m, clk, time.Minute, "B")
	if b := loadTask(t, m, "B"); b.State != task.Done || b.LastAnswer() != story("B") {
		t.Fatalf("B ended %s %q", b.State, b.LastAnswer())
	}
}

// Suspends and resumes race every boundary of three tasks on one
// preempting vCore while the clock runs (found nothing in review but the
// two above; kept as a regression). Each task whose last instruction is a
// resume ends Done with its answer whole; one whose last is a suspend
// comes to rest Suspended, or done.
func TestSuspendResumeStress(t *testing.T) {
	for iter := range 6 {
		clk := sim.NewClock(clock0)
		lm := newLocal(clk, prefillCaps, nil)
		m := localMachine(t, neumann.Config{Preempt: true}, lm, clk)
		ids := []string{"A", "B", "C"}
		for i, id := range ids {
			submitAs(t, m, id, i)
		}
		var stopClock atomic.Bool
		var clock sync.WaitGroup
		clock.Add(1)
		go func() {
			defer clock.Done()
			for !stopClock.Load() {
				clk.Advance(700 * time.Millisecond)
				time.Sleep(50 * time.Microsecond)
			}
		}()
		var ops sync.WaitGroup
		for i, id := range ids {
			ops.Add(1)
			go func() {
				defer ops.Done()
				r := rand.New(rand.NewSource(int64(iter*10 + i)))
				for range 20 {
					if err := m.Suspend(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("suspend %s: %v", id, err)
					}
					time.Sleep(time.Duration(r.Intn(300)) * time.Microsecond)
					if err := m.ResumeTask(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("resume %s right after its suspend: %v", id, err)
					}
					time.Sleep(time.Duration(r.Intn(300)) * time.Microsecond)
				}
				if id == "C" {
					if err := m.Suspend(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("final suspend: %v", err)
					}
				}
			}()
		}
		ops.Wait()
		waitFor(t, func() bool {
			a, b, c := loadTask(t, m, "A"), loadTask(t, m, "B"), loadTask(t, m, "C")
			return a.State == task.Done && b.State == task.Done && (c.State == task.Done || c.State == task.Suspended)
		})
		stopClock.Store(true)
		clock.Wait()
		for _, id := range ids {
			if tc := loadTask(t, m, id); tc.State == task.Done && tc.LastAnswer() != story(id) {
				t.Fatalf("iter %d: %s answered %q", iter, id, tc.LastAnswer())
			}
		}
		if c := loadTask(t, m, "C"); c.State == task.Suspended {
			resume(t, m, "C")
			drive(t, m, clk, time.Minute, "C")
		}
		if st := m.Status(); st.Suspended != 0 {
			t.Fatalf("iter %d: status %+v", iter, st)
		}
	}
}

// Suspends and resumes at random on the real clock, against tasks that run
// three tool rounds each through transient model failures: every task ends
// Done, never left suspended (each operator's last instruction is a
// resume), and every tool call ran exactly once.
func TestSuspendResumeStressWithTools(t *testing.T) {
	for iter := range 4 {
		var runs sync.Map // idempotency key -> runs
		dev := builtin.New("dev").Add(bus.ToolDef{Name: "work"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			n, _ := runs.LoadOrStore(c.Key, new(atomic.Int32))
			n.(*atomic.Int32).Add(1)
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
			return "ok", nil
		})
		var fails atomic.Int32
		fn := func(req model.Request) (model.Response, error) {
			time.Sleep(time.Duration(rand.Intn(1500)) * time.Microsecond)
			if fails.Add(1)%5 == 0 {
				return model.Response{}, sim.Transient()
			}
			if sim.ToolRounds(req) < 3 {
				return sim.CallTools("dev_work"), nil
			}
			return sim.Reply("done " + sim.FirstUser(req)), nil
		}
		m, _ := start(t, neumann.Config{Cores: 2, Devices: []bus.Device{dev}, MaxModelRetries: 50,
			BackoffBase: time.Millisecond, BackoffMax: 2 * time.Millisecond, BackoffJitter: func(d time.Duration) time.Duration { return d }}, fn)
		var ids []string
		for i := range 4 {
			ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("t%d", i)}))
		}
		var ops sync.WaitGroup
		for i, id := range ids {
			ops.Add(1)
			go func() {
				defer ops.Done()
				r := rand.New(rand.NewSource(int64(iter*10 + i)))
				for range 15 {
					if err := m.Suspend(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("suspend: %v", err)
					}
					time.Sleep(time.Duration(r.Intn(1500)) * time.Microsecond)
					if err := m.ResumeTask(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("resume right after its suspend: %v", err)
					}
					time.Sleep(time.Duration(r.Intn(1500)) * time.Microsecond)
				}
			}()
		}
		ops.Wait()
		for i, id := range ids {
			tc := waitDone(t, m, id)
			if tc.State != task.Done || tc.LastAnswer() != fmt.Sprintf("done t%d", i) {
				t.Fatalf("iter %d: %s ended %s %q (%s)", iter, id, tc.State, tc.LastAnswer(), tc.StopReason)
			}
		}
		runs.Range(func(k, v any) bool {
			if n := v.(*atomic.Int32).Load(); n != 1 {
				t.Errorf("iter %d: tool call %v ran %d times", iter, k, n)
			}
			return true
		})
		stop(t, m)
	}
}

// A cancel races suspends and resumes: the cancel always wins, nothing
// hangs, and nothing is left counted suspended.
func TestCancelRacesSuspendAndResume(t *testing.T) {
	for iter := range 5 {
		dev := builtin.New("dev").Add(bus.ToolDef{Name: "work"}, func(context.Context, bus.DeviceCall) (string, error) {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
			return "ok", nil
		})
		fn := func(req model.Request) (model.Response, error) {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
			if sim.ToolRounds(req) < 50 {
				return sim.CallTools("dev_work"), nil
			}
			return sim.Reply("done"), nil
		}
		m, _ := start(t, neumann.Config{Cores: 2, Devices: []bus.Device{dev}}, fn)
		var ids []string
		for i := range 4 {
			ids = append(ids, submit(t, m, proto.Instruction{Input: fmt.Sprintf("t%d", i)}))
		}
		var ops sync.WaitGroup
		for i, id := range ids {
			ops.Add(2)
			r := rand.New(rand.NewSource(int64(iter*10 + i)))
			d := time.Duration(r.Intn(10000)) * time.Microsecond
			go func() {
				defer ops.Done()
				for range 30 {
					if err := m.Suspend(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
						t.Errorf("suspend: %v", err)
					}
					time.Sleep(time.Duration(r.Intn(400)) * time.Microsecond)
					m.ResumeTask(bg, id)
					time.Sleep(time.Duration(r.Intn(400)) * time.Microsecond)
				}
			}()
			go func() {
				defer ops.Done()
				time.Sleep(d)
				if err := m.Cancel(bg, id); err != nil && !errors.Is(err, neumann.ErrFinished) {
					t.Errorf("cancel: %v", err)
				}
			}()
		}
		ops.Wait()
		for _, id := range ids {
			if tc := waitDone(t, m, id); tc.StopReason != "cancelled" {
				t.Fatalf("iter %d: %s ended %s %q", iter, id, tc.State, tc.StopReason)
			}
		}
		if st := m.Status(); st.Suspended != 0 || st.Unfinished != 0 {
			t.Fatalf("iter %d: status %+v", iter, st)
		}
		stop(t, m)
	}
}
