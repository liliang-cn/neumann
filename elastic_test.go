package neumann_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"runtime"
	"slices"
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
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// adapter is a scripted model with a name, caps, and a context; it records
// every request it is sent.
type adapter struct {
	name string
	caps *model.Caps
	fn   func(ctx context.Context, req model.Request) (model.Response, error)

	mu   sync.Mutex
	reqs []model.Request
}

func (a *adapter) Provider() string { return a.name }

func (a *adapter) Caps() model.Caps {
	if a.caps != nil {
		return *a.caps
	}
	return model.Caps{Tools: true}
}

func (a *adapter) Call(ctx context.Context, req model.Request) (model.Response, error) {
	a.mu.Lock()
	a.reqs = append(a.reqs, req)
	a.mu.Unlock()
	return a.fn(ctx, req)
}

func (a *adapter) requests() []model.Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.reqs)
}

func reply(text string) func(context.Context, model.Request) (model.Response, error) {
	return func(context.Context, model.Request) (model.Response, error) { return sim.Reply(text), nil }
}

// after is a channel closed once clk has moved d on.
func after(clk *sim.Clock, d time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	clk.AfterFunc(d, func() { close(ch) })
	return ch
}

// ticking moves clk by step every real tick until the returned stop is
// called.
func ticking(clk *sim.Clock, step, tick time.Duration) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(tick):
				clk.Advance(step)
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

func identity(d time.Duration) time.Duration { return d }

func groupOf(t *testing.T, m *neumann.Machine, class string) neumann.GroupStatus {
	t.Helper()
	for _, g := range m.Status().Groups {
		if g.Class == class {
			return g
		}
	}
	t.Fatalf("no group %q", class)
	return neumann.GroupStatus{}
}

type coreData struct {
	Class  string `json:"class"`
	Core   int    `json:"core"`
	Reason string `json:"reason"`
	Live   int    `json:"live"`
	seq    int64
}

func coreEvents(t *testing.T, m *neumann.Machine, kind string) []coreData {
	t.Helper()
	all, err := m.EventLog(bg, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []coreData
	for _, ev := range all {
		if ev.Kind == kind && ev.TaskID == "" {
			var d coreData
			json.Unmarshal(ev.Data, &d)
			d.seq = ev.Seq
			out = append(out, d)
		}
	}
	return out
}

func reasons(cs []coreData) map[string]int {
	out := map[string]int{}
	for _, c := range cs {
		out[c.Reason]++
	}
	return out
}

func doneAll(t *testing.T, m *neumann.Machine, ids []string) {
	t.Helper()
	waitFor(t, func() bool {
		for _, id := range ids {
			if tc, err := m.Load(bg, id); err != nil || !tc.State.Terminal() {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		if got := loadTask(t, m, id); got.State != task.Done {
			t.Fatalf("task %s ended %s %q", id, got.State, got.StopReason)
		}
	}
}

// design §18 test 13, through the machine: a provider that answers Overload
// to any request arriving while K are in flight. The group's live vCores
// climb to about K, are halved by the overloads, climb again, and never
// exceed 2K; every task finishes.
func TestCoresTrackProviderCapacity(t *testing.T) {
	const K = 4
	clk := sim.NewClock(clock0)
	var inflight atomic.Int32
	var overloads atomic.Int32
	var mp atomic.Pointer[neumann.Machine]
	var mu sync.Mutex
	var samples []int
	sample := func() {
		if m := mp.Load(); m != nil {
			live := m.Status().Groups[0].Live
			mu.Lock()
			samples = append(samples, live)
			mu.Unlock()
		}
	}
	a := &adapter{name: "cap", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		sample()
		if n > K {
			overloads.Add(1)
			return model.Response{}, sim.Overload()
		}
		select {
		case <-after(clk, time.Second):
		case <-ctx.Done():
			return model.Response{}, ctx.Err()
		}
		return sim.Reply("ok"), nil
	}}
	rng := rand.New(rand.NewPCG(1, 2))
	var rmu sync.Mutex
	m := startWith(t, neumann.Config{
		Clock:           clk,
		Groups:          []neumann.GroupConfig{{Class: "g", Model: a, Cooldown: 2 * time.Second, IdleRetire: time.Hour}},
		MaxQueued:       10000,
		MaxModelRetries: 1000,
		BackoffBase:     500 * time.Millisecond,
		BackoffMax:      4 * time.Second,
		BackoffJitter: func(d time.Duration) time.Duration {
			rmu.Lock()
			defer rmu.Unlock()
			return d/2 + time.Duration(rng.Int64N(int64(d/2)+1))
		},
	})
	mp.Store(m)
	defer ticking(clk, 50*time.Millisecond, 300*time.Microsecond)()
	var ids []string
	for range 300 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
	}
	waitForLong(t, 60*time.Second, func() bool {
		for _, id := range ids {
			if tc, err := m.Load(bg, id); err != nil || !tc.State.Terminal() {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		if got := loadTask(t, m, id); got.State != task.Done {
			t.Fatalf("task %s ended %s %q", id, got.State, got.StopReason)
		}
	}
	mu.Lock()
	maxLive, sum := 0, 0
	for _, s := range samples {
		maxLive = max(maxLive, s)
		sum += s
	}
	mean := float64(sum) / float64(len(samples))
	mu.Unlock()
	g := m.Status().Groups[0]
	downs := reasons(coreEvents(t, m, proto.EvCoreDown))
	t.Logf("K=%d: live vCores max %d, mean %.2f over %d calls; %d overloads; epoch %d, W %.2f; core_down %v",
		K, maxLive, mean, len(samples), overloads.Load(), g.Epoch, g.W, downs)
	if maxLive > 2*K {
		t.Fatalf("%d live vCores, more than 2K = %d", maxLive, 2*K)
	}
	if maxLive < K {
		t.Fatalf("live vCores peaked at %d, never reached K = %d", maxLive, K)
	}
	if overloads.Load() == 0 || g.Epoch < 2 || downs["overload"] == 0 {
		t.Fatalf("the window never oscillated: %d overloads, epoch %d, core_down %v", overloads.Load(), g.Epoch, downs)
	}
	if mean < K/2 || mean > 2*K {
		t.Fatalf("mean live vCores %.2f is not near K = %d", mean, K)
	}
}

func waitForLong(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(time.Millisecond)
	}
}

// design §18 test 14, through the machine: ten requests in flight in one
// congestion epoch all come back Overload. The window halves once (10 → 5),
// the epoch moves once, and the five vCores above the new window retire as
// their instructions finish (core_down, overload).
func TestOneEpochOfOverloadsHalvesTheWindowOnce(t *testing.T) {
	clk := sim.NewClock(clock0)
	var arrived atomic.Int32
	all := make(chan struct{})
	a := &adapter{name: "o", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		if arrived.Add(1) == 10 {
			close(all)
		}
		select {
		case <-all:
		case <-ctx.Done():
			return model.Response{}, ctx.Err()
		}
		return model.Response{}, sim.Overload()
	}}
	m := startWith(t, neumann.Config{
		Clock: clk, BackoffJitter: identity,
		Groups: []neumann.GroupConfig{{Class: "g", Model: a, Initial: 10}},
	})
	var ids []string
	for range 10 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
	}
	waitFor(t, func() bool {
		for _, id := range ids {
			if tc, err := m.Load(bg, id); err != nil || tc.State != task.Waiting || tc.Wait != task.WaitBackoff {
				return false
			}
		}
		return groupOf(t, m, "g").Busy == 0
	})
	g := groupOf(t, m, "g")
	if g.W != 5 || g.Target != 5 || g.Epoch != 1 || g.Live != 5 {
		t.Fatalf("after one epoch of overloads: W %.2f, target %d, epoch %d, live %d", g.W, g.Target, g.Epoch, g.Live)
	}
	ups, downs := coreEvents(t, m, proto.EvCoreUp), coreEvents(t, m, proto.EvCoreDown)
	if len(ups) != 10 || reasons(ups)["demand"] != 10 {
		t.Fatalf("core_up %+v", ups)
	}
	if len(downs) != 5 || reasons(downs)["overload"] != 5 {
		t.Fatalf("core_down %+v", downs)
	}
}

// design §18 test 15: two groups, each with its own model, share one limiter
// of three requests a minute. Together they never send more than three in
// any minute, and both get their turn.
func TestTwoGroupsShareALimiter(t *testing.T) {
	clk := sim.NewClock(clock0)
	var mu sync.Mutex
	var at []time.Time
	calls := map[string]int{}
	record := func(name string) func(context.Context, model.Request) (model.Response, error) {
		return func(context.Context, model.Request) (model.Response, error) {
			mu.Lock()
			at = append(at, clk.Now())
			calls[name]++
			mu.Unlock()
			return sim.Reply(name + " done"), nil
		}
	}
	m := startWith(t, neumann.Config{
		Clock:    clk,
		Limiters: []limit.Config{{Name: "shared", Limits: map[string]limit.Rate{"requests": {Per: time.Minute, Limit: 3}}}},
		Groups: []neumann.GroupConfig{
			{Class: "a", Model: &adapter{name: "a", fn: record("a")}, Limits: []string{"shared"}, Initial: 6},
			{Class: "b", Model: &adapter{name: "b", fn: record("b")}, Limits: []string{"shared"}, Initial: 6},
		},
	})
	var ids []string
	for i := range 12 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q", Class: []string{"a", "b"}[i%2]}))
	}
	runUntilDone(t, m, clk, 10*time.Second, ids...)
	mu.Lock()
	defer mu.Unlock()
	if calls["a"] != 6 || calls["b"] != 6 {
		t.Fatalf("calls %v", calls)
	}
	slices.SortFunc(at, func(x, y time.Time) int { return x.Compare(y) })
	for i, s := range at {
		n := 0
		for _, u := range at[i:] {
			if u.Sub(s) < time.Minute {
				n++
			}
		}
		if n > 3 {
			t.Fatalf("%d requests in the minute from %v: %v", n, s.Sub(clock0), at)
		}
	}
	if span := at[len(at)-1].Sub(at[0]); span < 3*time.Minute {
		t.Fatalf("12 requests at 3 a minute took only %v", span)
	}
}

// echo2 is a device with two tools, so a task snapshots two.
func echo2() bus.Device {
	echo := func(_ context.Context, c bus.DeviceCall) (string, error) { return string(c.Args), nil }
	return builtin.New("dev").Add(bus.ToolDef{Name: "echo"}, echo).Add(bus.ToolDef{Name: "other"}, echo)
}

func migratedEvents(t *testing.T, m *neumann.Machine, id string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvMigrated {
			var d map[string]any
			json.Unmarshal(ev.Data, &d)
			out = append(out, d)
		}
	}
	return out
}

// design §18 test 17, success: the task's group runs out of quota mid-task.
// It moves to the fallback group: its class changes, its ReadySince is kept,
// a migrated event says so, and the request the new provider receives has
// none of the old provider's reasoning, while the old provider had been sent
// it back.
func TestMigrationToTheFallbackGroup(t *testing.T) {
	clk := sim.NewClock(clock0)
	big := &adapter{name: "big-provider", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			r := sim.CallTools("dev_echo")
			r.Message.Parts = append([]task.Part{{Kind: task.PartReasoning, Text: "thinking", Provider: "big-provider", Opaque: json.RawMessage(`{"sig":"x"}`)}}, r.Message.Parts...)
			return r, nil
		}
		clk.Advance(time.Minute) // the migration happens a minute after the task was last ready
		return model.Response{}, sim.QuotaExhausted()
	}}
	var readySince atomic.Pointer[time.Time]
	var mp atomic.Pointer[neumann.Machine]
	little := &adapter{name: "little-provider", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		if tc, err := mp.Load().Load(bg, "mig-1"); err == nil {
			rs := tc.ReadySince
			readySince.Store(&rs)
		}
		return sim.Reply("little done"), nil
	}}
	m := startWith(t, neumann.Config{
		Clock: clk, Devices: []bus.Device{echoDev()},
		Groups: []neumann.GroupConfig{
			{Class: "big", Model: big, Fallback: "little"},
			{Class: "little", Model: little},
		},
	})
	mp.Store(m)
	id := submit(t, m, proto.Instruction{TaskID: "mig-1", Input: "go", Class: "big"})
	got := waitDone(t, m, id)
	if got.State != task.Done || got.Class != "little" || got.LastAnswer() != "little done" {
		t.Fatalf("task ended %s in %q: %q", got.State, got.Class, got.LastAnswer())
	}
	if rs := readySince.Load(); rs == nil || !rs.Equal(clock0) {
		t.Fatalf("ReadySince on the new group %v, want %v (kept from before the move)", rs, clock0)
	}
	mig := migratedEvents(t, m, id)
	if len(mig) != 1 || mig[0]["from"] != "big" || mig[0]["to"] != "little" || mig[0]["reason"] != "group_down" {
		t.Fatalf("migrated events %v", mig)
	}
	hasReasoning := func(req model.Request) bool {
		for _, msg := range req.Messages {
			for _, p := range msg.Parts {
				if p.Kind == task.PartReasoning {
					return true
				}
			}
		}
		return false
	}
	breqs, lreqs := big.requests(), little.requests()
	if len(breqs) != 2 || !hasReasoning(breqs[1]) {
		t.Fatalf("the old provider's own reasoning was not sent back to it: %d requests", len(breqs))
	}
	if len(lreqs) != 1 || hasReasoning(lreqs[0]) || sim.ToolRounds(lreqs[0]) != 1 {
		t.Fatalf("the new provider got %d requests; first carries reasoning %v", len(lreqs), len(lreqs) > 0 && hasReasoning(lreqs[0]))
	}
	if g := groupOf(t, m, "big"); g.Breaker != "open" || g.Cause != "quota" {
		t.Fatalf("big group %+v", g)
	}
	// The task's own record of its reasoning is kept: only the request to
	// another provider leaves it out.
	if !slices.ContainsFunc(got.Regs, func(msg task.Message) bool {
		return slices.ContainsFunc(msg.Parts, func(p task.Part) bool { return p.Kind == task.PartReasoning })
	}) {
		t.Fatal("the registers lost the reasoning")
	}
}

// design §18 test 17, refusals: the fallback cannot take the task because it
// calls no tools, takes fewer tools than the task has, or has a window the
// task does not fit (chunked compaction is not available). With
// OnMigrateFail "fail" the task fails, migrate_incompatible, naming the
// check; with "wait" it waits for its own group (a migrate_refused event
// names the check) and finishes there after a reset. A pinned task never
// moves.
func TestMigrationRefusedByEachCheck(t *testing.T) {
	cases := []struct {
		check string
		caps  model.Caps
	}{
		{"tools", model.Caps{}},
		{"tool_count", model.Caps{Tools: true, MaxTools: 1}},
		{"context", model.Caps{Tools: true, Window: 200}},
	}
	for _, c := range cases {
		for _, mode := range []string{neumann.MigrateFail, neumann.MigrateWait} {
			t.Run(c.check+"/"+mode, func(t *testing.T) {
				var quota atomic.Bool
				quota.Store(true)
				big := &adapter{name: "big", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
					if quota.Load() {
						return model.Response{}, sim.QuotaExhausted()
					}
					return sim.Reply("big done"), nil
				}}
				caps := c.caps
				little := &adapter{name: "little", caps: &caps, fn: reply("little done")}
				m := startWith(t, neumann.Config{
					Devices: []bus.Device{echo2()},
					Groups: []neumann.GroupConfig{
						{Class: "big", Model: big, Fallback: "little", OnMigrateFail: mode},
						{Class: "little", Model: little},
					},
				})
				id := submit(t, m, proto.Instruction{Input: "go"})
				var check string
				if mode == neumann.MigrateFail {
					got := waitDone(t, m, id)
					if got.State != task.Failed || got.StopReason != "migrate_incompatible" || got.Class != "big" {
						t.Fatalf("task ended %s %q in %q", got.State, got.StopReason, got.Class)
					}
					check = failedEvent(t, m, id)["check"]
				} else {
					waitFor(t, func() bool {
						tc, err := m.Load(bg, id)
						return err == nil && tc.State == task.Waiting && tc.Wait == task.WaitGroup
					})
					for _, ev := range eventsOf(t, m, id) {
						if ev.Kind == proto.EvMigrateRefused {
							var d map[string]string
							json.Unmarshal(ev.Data, &d)
							check = d["check"]
						}
					}
					quota.Store(false)
					if err := m.GroupReset(bg, "big"); err != nil {
						t.Fatal(err)
					}
					if got := waitDone(t, m, id); got.State != task.Done || got.Class != "big" || got.LastAnswer() != "big done" {
						t.Fatalf("after reset the task ended %s in %q: %q", got.State, got.Class, got.LastAnswer())
					}
				}
				if check != c.check {
					t.Fatalf("refused by %q, want %q", check, c.check)
				}
				if n := len(little.requests()); n != 0 {
					t.Fatalf("the fallback was called %d times", n)
				}
				if mig := migratedEvents(t, m, id); len(mig) != 0 {
					t.Fatalf("migrated %v", mig)
				}
			})
		}
	}

	t.Run("pinned", func(t *testing.T) {
		big := &adapter{name: "big", fn: func(context.Context, model.Request) (model.Response, error) {
			return model.Response{}, sim.QuotaExhausted()
		}}
		little := &adapter{name: "little", fn: reply("little done")}
		m := startWith(t, neumann.Config{Groups: []neumann.GroupConfig{
			{Class: "big", Model: big, Fallback: "little"},
			{Class: "little", Model: little},
		}})
		id := submit(t, m, proto.Instruction{Input: "go", PinClass: true})
		waitFor(t, func() bool {
			tc, err := m.Load(bg, id)
			return err == nil && tc.State == task.Waiting && tc.Wait == task.WaitGroup
		})
		if tc := loadTask(t, m, id); !tc.PinClass || tc.Class != "big" || len(little.requests()) != 0 {
			t.Fatalf("pinned task moved: %+v", tc)
		}
	})
}

// SetClass moves a task between instructions: from Waiting(Group) or the
// ready queue, with the capability checks (a refusal changes nothing), never
// while it executes, and not a finished or unknown one.
func TestSetClassMovesATask(t *testing.T) {
	gate, open := newGate()
	defer open()
	busy := make(chan struct{}, 1)
	big := &adapter{name: "big", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		switch sim.FirstUser(req) {
		case "busy":
			busy <- struct{}{}
			<-gate
			return sim.Reply("busy done"), nil
		}
		return model.Response{}, sim.QuotaExhausted()
	}}
	notools := &adapter{name: "notools", caps: &model.Caps{}, fn: reply("notools done")}
	little := &adapter{name: "little", fn: reply("little done")}
	m := startWith(t, neumann.Config{
		Devices: []bus.Device{echoDev()},
		Groups: []neumann.GroupConfig{
			{Class: "big", Model: big},
			{Class: "notools", Model: notools},
			{Class: "little", Model: little},
		},
	})
	x := submit(t, m, proto.Instruction{Input: "x"})
	waitFor(t, func() bool {
		tc, err := m.Load(bg, x)
		return err == nil && tc.State == task.Waiting && tc.Wait == task.WaitGroup
	})
	before := loadTask(t, m, x)

	var me *neumann.MigrateError
	if err := m.SetClass(bg, x, "notools"); !errors.As(err, &me) || me.Check != "tools" || me.To != "notools" {
		t.Fatalf("SetClass to a model without tools: %v", err)
	}
	if tc := loadTask(t, m, x); tc.Class != "big" || tc.Version != before.Version {
		t.Fatalf("a refused move changed the task: %+v", tc)
	}
	if err := m.SetClass(bg, x, "nope"); err == nil {
		t.Fatal("SetClass to an unknown class")
	}
	if err := m.SetClass(bg, "missing", "little"); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}

	if err := m.SetClass(bg, x, "little"); err != nil {
		t.Fatal(err)
	}
	got := waitDone(t, m, x)
	if got.State != task.Done || got.Class != "little" || got.LastAnswer() != "little done" || !got.ReadySince.Equal(before.ReadySince) {
		t.Fatalf("moved task ended %s in %q: %q, ReadySince %v (was %v)", got.State, got.Class, got.LastAnswer(), got.ReadySince, before.ReadySince)
	}
	if mig := migratedEvents(t, m, x); len(mig) != 1 || mig[0]["reason"] != "manual" || mig[0]["from"] != "big" {
		t.Fatalf("migrated %v", mig)
	}
	if err := m.SetClass(bg, x, "big"); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("finished task: %v", err)
	}

	m.GroupReset(bg, "big")
	y := submit(t, m, proto.Instruction{Input: "busy"})
	<-busy
	if err := m.SetClass(bg, y, "little"); !errors.Is(err, neumann.ErrTaskBusy) {
		t.Fatalf("task on a vCore: %v", err)
	}
	open()
	if got := waitDone(t, m, y); got.Class != "big" || got.State != task.Done {
		t.Fatalf("busy task ended %s in %q", got.State, got.Class)
	}
}

// design §18 test 19: vCores retire only between instructions. Four vCores
// each hold a model call when an operator pins the window at one: each
// finishes its instruction and commits it, and only then do three of them
// retire (core_down, manual). No task is lost and none runs twice.
func TestCoresRetireOnlyBetweenInstructions(t *testing.T) {
	gate, open := newGate()
	defer open()
	var in atomic.Int32
	a := &adapter{name: "g", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		in.Add(1)
		<-gate
		return sim.Reply("done"), nil
	}}
	m := startWith(t, neumann.Config{Groups: []neumann.GroupConfig{{Class: "g", Model: a, Initial: 4}}})
	var ids []string
	for range 4 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
	}
	waitFor(t, func() bool { return in.Load() == 4 })
	if err := m.SetCores("g", 1); err != nil {
		t.Fatal(err)
	}
	if g := groupOf(t, m, "g"); g.Live != 4 || g.Busy != 4 || g.Target != 1 || g.Auto {
		t.Fatalf("right after SetCores: %+v", g)
	}
	open()
	doneAll(t, m, ids)
	waitFor(t, func() bool { return groupOf(t, m, "g").Live == 1 })
	downs := coreEvents(t, m, proto.EvCoreDown)
	if len(downs) != 3 || reasons(downs)["manual"] != 3 {
		t.Fatalf("core_down %+v", downs)
	}
	if n := len(a.requests()); n != 4 {
		t.Fatalf("%d model calls for 4 one-step tasks", n)
	}
	// Each retiring vCore committed its instruction before it retired: the
	// task it ran ended at a lower seq than its core_down.
	all, _ := m.EventLog(bg, 0, 0)
	ranOn := map[string]int{}
	ended := map[string]int64{}
	for _, ev := range all {
		switch ev.Kind {
		case proto.EvStep:
			var d struct{ Core int }
			json.Unmarshal(ev.Data, &d)
			ranOn[ev.TaskID] = d.Core
		case proto.EvDone:
			ended[ev.TaskID] = ev.Seq
		}
	}
	for _, d := range downs {
		found := false
		for id, c := range ranOn {
			if c == d.Core {
				found = true
				if ended[id] == 0 || ended[id] > d.seq {
					t.Fatalf("vCore %d retired at seq %d, its task %s ended at %d", d.Core, d.seq, id, ended[id])
				}
			}
		}
		if !found {
			t.Fatalf("vCore %d retired without having run a task", d.Core)
		}
	}
}

// SetCores pins the window and starts vCores for the backlog; AutoScale
// gives control back. Both refuse an unknown class, SetCores a count below 1.
func TestSetCoresAndAutoScale(t *testing.T) {
	gate, open := newGate()
	defer open()
	a := &adapter{name: "g", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		<-gate
		return sim.Reply("done"), nil
	}}
	m := startWith(t, neumann.Config{Groups: []neumann.GroupConfig{{Class: "g", Model: a, Initial: 1}}})
	var ids []string
	for range 6 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
	}
	waitFor(t, func() bool { g := groupOf(t, m, "g"); return g.Live == 1 && g.Busy == 1 && g.Ready == 5 })
	if err := m.SetCores("g", 3); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { g := groupOf(t, m, "g"); return g.Live == 3 && g.Busy == 3 })
	if g := groupOf(t, m, "g"); g.Auto || g.Target != 3 || g.W != 3 {
		t.Fatalf("pinned: %+v", g)
	}
	if q := m.Queue(); len(q.Classes) != 1 || q.Classes[0].Class != "g" || q.Classes[0].Ready != 3 {
		t.Fatalf("queue %+v", q)
	}
	if err := m.AutoScale("g", true); err != nil {
		t.Fatal(err)
	}
	if g := groupOf(t, m, "g"); !g.Auto {
		t.Fatalf("auto: %+v", g)
	}
	if m.SetCores("nope", 2) == nil || m.AutoScale("nope", true) == nil || m.SetCores("g", 0) == nil {
		t.Fatal("bad SetCores or AutoScale accepted")
	}
	open()
	doneAll(t, m, ids)
	ups := coreEvents(t, m, proto.EvCoreUp)
	if len(ups) < 3 || reasons(ups)["demand"] != len(ups) {
		t.Fatalf("core_up %+v", ups)
	}
}

// An idle vCore retires after IdleRetire on the machine's clock, never taking
// the group below Min, and without touching the window.
func TestIdleCoresRetireDownToMin(t *testing.T) {
	clk := sim.NewClock(clock0)
	gate, open := newGate()
	defer open()
	var in atomic.Int32
	a := &adapter{name: "g", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		in.Add(1)
		<-gate
		return sim.Reply("done"), nil
	}}
	m := startWith(t, neumann.Config{Clock: clk, Groups: []neumann.GroupConfig{{Class: "g", Model: a, Initial: 4, Min: 2, IdleRetire: 30 * time.Second}}})
	var ids []string
	for range 4 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q"}))
	}
	waitFor(t, func() bool { return in.Load() == 4 })
	open()
	doneAll(t, m, ids)
	waitFor(t, func() bool { return groupOf(t, m, "g").Busy == 0 })
	time.Sleep(20 * time.Millisecond) // every vCore back waiting for work
	w := groupOf(t, m, "g").W
	clk.Advance(29 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if g := groupOf(t, m, "g"); g.Live != 4 {
		t.Fatalf("retired before IdleRetire: %+v", g)
	}
	for range 5 {
		clk.Advance(31 * time.Second)
		time.Sleep(10 * time.Millisecond)
	}
	g := groupOf(t, m, "g")
	downs := coreEvents(t, m, proto.EvCoreDown)
	if g.Live != 2 || len(downs) != 2 || reasons(downs)["idle"] != 2 || g.W != w {
		t.Fatalf("after idling: live %d, W %.2f (was %.2f), core_down %+v", g.Live, g.W, w, downs)
	}
}

// Stop retires every vCore (core_down, shutdown), and nothing of the machine
// is left running.
func TestStopShutsEveryCoreDown(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()
	m, err := neumann.New(neumann.Config{Logf: t.Logf, Groups: []neumann.GroupConfig{
		{Class: "a", Model: &adapter{name: "a", fn: reply("a")}, Initial: 3},
		{Class: "b", Model: &adapter{name: "b", fn: reply("b")}, Initial: 3},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := range 20 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "q", Class: []string{"a", "b"}[i%2]}))
	}
	doneAll(t, m, ids)
	live := m.Status().Cores
	if live == 0 {
		t.Fatal("no vCore ran")
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if st := m.Status(); st.Cores != 0 {
		t.Fatalf("%d vCores after Stop", st.Cores)
	}
	ups, downs := coreEvents(t, m, proto.EvCoreUp), coreEvents(t, m, proto.EvCoreDown)
	if len(ups) != len(downs) || reasons(downs)["shutdown"] != live {
		t.Fatalf("%d core_up, core_down %v; %d were live at Stop", len(ups), reasons(downs), live)
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("%d goroutines after Stop, %d before:\n%s", runtime.NumGoroutine(), base, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Groups are checked at New: classes unique and named, a model each,
// fallbacks naming a group and never coming back around, limiters that
// exist, OnMigrateFail one of the two, and no shorthand fields beside them.
func TestGroupConfigIsValidated(t *testing.T) {
	a := &adapter{name: "a", fn: reply("a")}
	for name, cfg := range map[string]neumann.Config{
		"duplicate":      {Groups: []neumann.GroupConfig{{Class: "x", Model: a}, {Class: "x", Model: a}}},
		"no class":       {Groups: []neumann.GroupConfig{{Model: a}}},
		"no model":       {Groups: []neumann.GroupConfig{{Class: "x"}}},
		"fallback":       {Groups: []neumann.GroupConfig{{Class: "x", Model: a, Fallback: "y"}}},
		"self":           {Groups: []neumann.GroupConfig{{Class: "x", Model: a, Fallback: "x"}}},
		"cycle":          {Groups: []neumann.GroupConfig{{Class: "x", Model: a, Fallback: "y"}, {Class: "y", Model: a, Fallback: "z"}, {Class: "z", Model: a, Fallback: "y"}}},
		"limiter":        {Groups: []neumann.GroupConfig{{Class: "x", Model: a, Limits: []string{"none"}}}},
		"on fail":        {Groups: []neumann.GroupConfig{{Class: "x", Model: a, OnMigrateFail: "retry"}}},
		"max below min":  {Groups: []neumann.GroupConfig{{Class: "x", Model: a, Min: 3, Max: 2}}},
		"shorthand too":  {Model: a, Groups: []neumann.GroupConfig{{Class: "x", Model: a}}},
		"default":        {Default: "y", Groups: []neumann.GroupConfig{{Class: "x", Model: a}}},
		"nothing at all": {},
	} {
		if _, err := neumann.New(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	m := startWith(t, neumann.Config{Default: "y", Groups: []neumann.GroupConfig{
		{Class: "x", Model: &adapter{name: "x", fn: reply("x")}, Fallback: "y"},
		{Class: "y", Model: &adapter{name: "y", fn: reply("y")}, MaxQueued: 1},
	}})
	if got := waitDone(t, m, submit(t, m, proto.Instruction{Input: "q"})); got.Class != "y" || got.LastAnswer() != "y" {
		t.Fatalf("default class: %q answered %q", got.Class, got.LastAnswer())
	}
	if _, err := m.Submit(bg, proto.Instruction{Input: "q", Class: "z"}); err == nil {
		t.Fatal("unknown class accepted")
	}
}

// A group's MaxQueued bounds its own unfinished tasks; the machine's other
// groups still admit theirs.
func TestGroupMaxQueued(t *testing.T) {
	gate, open := newGate()
	defer open()
	slow := &adapter{name: "s", fn: func(context.Context, model.Request) (model.Response, error) { <-gate; return sim.Reply("s"), nil }}
	m := startWith(t, neumann.Config{Groups: []neumann.GroupConfig{
		{Class: "s", Model: slow, MaxQueued: 2},
		{Class: "f", Model: &adapter{name: "f", fn: reply("f")}},
	}})
	a := submit(t, m, proto.Instruction{Input: "1", Class: "s"})
	submit(t, m, proto.Instruction{Input: "2", Class: "s"})
	_, err := m.Submit(bg, proto.Instruction{Input: "3", Class: "s"})
	var ce *neumann.CapacityError
	if !errors.As(err, &ce) || ce.Class != "s" || ce.Limit != 2 || !errors.Is(err, neumann.ErrBusy) {
		t.Fatalf("third task of s: %v", err)
	}
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "4", Class: "f"}))
	open()
	waitDone(t, m, a)
	waitFor(t, func() bool { return m.Status().Unfinished == 0 })
	submit(t, m, proto.Instruction{Input: "5", Class: "s"})
}

// L2 through the machine: a read-only, cacheable tool called twice with the
// same arguments in one task runs once; the second result is served from the
// cache and its tool_result says cached. The task's cache is forgotten when
// it ends.
func TestCachedToolResultThroughTheMachine(t *testing.T) {
	var runs atomic.Int32
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "look", Effect: bus.ReadOnly, Cacheable: true},
		func(context.Context, bus.DeviceCall) (string, error) { runs.Add(1); return "balance 10", nil })
	m, _ := start(t, neumann.Config{Devices: []bus.Device{dev}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) < 2 {
			return sim.CallTools("dev_look"), nil
		}
		return sim.Reply("done"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "look twice"})
	if got := waitDone(t, m, id); got.State != task.Done {
		t.Fatalf("task ended %s", got.State)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("the tool ran %d times", n)
	}
	var cached []bool
	for _, ev := range eventsOf(t, m, id) {
		if ev.Kind == proto.EvToolResult {
			var d struct{ Cached bool }
			json.Unmarshal(ev.Data, &d)
			cached = append(cached, d.Cached)
		}
	}
	if !slices.Equal(cached, []bool{false, true}) {
		t.Fatalf("tool_result cached flags %v", cached)
	}
	st := m.Status().Cache
	if st.Hits != 1 || st.Fills != 1 || st.Tasks != 0 || st.Entries != 0 {
		t.Fatalf("cache %+v", st)
	}
}

// Status reports each group's vCores, queue and model.
func TestStatusReportsGroups(t *testing.T) {
	m := startWith(t, neumann.Config{Groups: []neumann.GroupConfig{
		{Class: "x", Model: &adapter{name: "px", fn: reply("x")}, Fallback: "y", Min: 1, Max: 8},
		{Class: "y", Model: &adapter{name: "py", fn: reply("y")}},
	}})
	waitDone(t, m, submit(t, m, proto.Instruction{Input: "q"}))
	st := m.Status()
	if len(st.Groups) != 2 || st.Cores != st.Groups[0].Live+st.Groups[1].Live {
		t.Fatalf("status %+v", st)
	}
	x := st.Groups[0]
	if x.Class != "x" || x.Model != "px" || x.Fallback != "y" || x.Max != 8 || !x.Auto || x.Live != 1 || x.W < 2 {
		t.Fatalf("group x %+v", x)
	}
	if !strings.Contains(st.Groups[1].Model, "py") {
		t.Fatalf("group y %+v", st.Groups[1])
	}
}

// A task parked on its own group because its fallback was down too moves
// once the fallback comes back: group_up on the fallback also wakes the
// waiting tasks of every group whose fallback chain reaches it, though the
// task's own group, open for quota, never half-opens.
func TestParkedTaskMigratesWhenItsFallbackReturns(t *testing.T) {
	clk := sim.NewClock(clock0)
	var littleQuota atomic.Bool
	littleQuota.Store(true)
	big := &adapter{name: "big", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		return model.Response{}, sim.QuotaExhausted()
	}}
	little := &adapter{name: "little", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		if littleQuota.Load() {
			return model.Response{}, sim.QuotaExhausted()
		}
		return sim.Reply("little done"), nil
	}}
	m := startWith(t, neumann.Config{
		Clock: clk,
		Groups: []neumann.GroupConfig{
			{Class: "big", Model: big, Fallback: "little"},
			{Class: "little", Model: little},
		},
	})
	l := submit(t, m, proto.Instruction{TaskID: "L", Input: "l", Class: "little"})
	waitFor(t, func() bool { tc, _ := m.Load(bg, l); return tc.State == task.Waiting && tc.Wait == task.WaitGroup })
	b := submit(t, m, proto.Instruction{TaskID: "B", Input: "b", Class: "big"})
	waitFor(t, func() bool { tc, _ := m.Load(bg, b); return tc.State == task.Waiting && tc.Wait == task.WaitGroup })

	littleQuota.Store(false)
	if err := m.GroupReset(bg, "little"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { tc, _ := m.Load(bg, l); return tc.State == task.Done })
	for range 50 {
		clk.Advance(time.Hour)
		time.Sleep(2 * time.Millisecond)
	}
	got := loadTask(t, m, b)
	t.Logf("B: %s/%s in class %q; little is %s", got.State, got.Wait, got.Class, groupOf(t, m, "little").Breaker)
	if got.State == task.Waiting && got.Class == "big" {
		t.Fatalf("B is still parked on its quota-open group 50h after its fallback came back")
	}
	if got.State != task.Done || got.Class != "little" || got.LastAnswer() != "little done" {
		t.Fatalf("B ended %s in %q: %q", got.State, got.Class, got.LastAnswer())
	}
}

// The same with the task's own group open for transient failures: a backlog
// parked there while the fallback was down too moves as soon as the fallback
// comes back (its own half-open probe succeeds), not one task per open period
// of the task's group.
func TestParkedBacklogMovesWhenTheFallbackComesBack(t *testing.T) {
	clk := sim.NewClock(clock0)
	var littleDown atomic.Bool
	littleDown.Store(true)
	big := &adapter{name: "big", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		return model.Response{}, sim.Transient()
	}}
	little := &adapter{name: "little", fn: func(ctx context.Context, req model.Request) (model.Response, error) {
		if littleDown.Load() {
			return model.Response{}, sim.Transient()
		}
		return sim.Reply("little done"), nil
	}}
	m := startWith(t, neumann.Config{
		Clock: clk, BackoffJitter: identity, MaxModelRetries: 100,
		Groups: []neumann.GroupConfig{
			{Class: "big", Model: big, Fallback: "little", BreakerThreshold: 1, BreakerOpenFor: time.Hour},
			{Class: "little", Model: little, BreakerThreshold: 1, BreakerOpenFor: time.Minute},
		},
	})
	l := submit(t, m, proto.Instruction{Input: "l", Class: "little"})
	waitFor(t, func() bool { return groupOf(t, m, "little").Breaker == "open" })
	var ids []string
	for range 5 {
		ids = append(ids, submit(t, m, proto.Instruction{Input: "b", Class: "big"}))
	}
	waitFor(t, func() bool { return groupOf(t, m, "big").Breaker == "open" })
	// The backoffs of the calls that failed before the breaker opened run
	// out, and every task parks.
	parked := func() bool {
		for _, id := range ids {
			if tc := loadTask(t, m, id); tc.State != task.Waiting || tc.Wait != task.WaitGroup {
				return false
			}
		}
		return true
	}
	for range 10 {
		quiet(t, m)
		if parked() {
			break
		}
		clk.Advance(2 * time.Second)
	}
	for _, id := range ids {
		if tc := loadTask(t, m, id); tc.State != task.Waiting || tc.Wait != task.WaitGroup || tc.Class != "big" {
			t.Fatalf("%s is %s/%s in %q before the fallback is back", id, tc.State, tc.Wait, tc.Class)
		}
	}
	littleDown.Store(false)
	begin := clk.Now()
	runUntilDone(t, m, clk, time.Second, append(ids, l)...)
	if took := clk.Now().Sub(begin); took > 5*time.Minute {
		t.Fatalf("the backlog took %v to move", took)
	}
	for _, id := range ids {
		if tc := loadTask(t, m, id); tc.State != task.Done || tc.Class != "little" {
			t.Fatalf("%s ended %s in %q", id, tc.State, tc.Class)
		}
	}
}

// A restart under a configuration without the task's class moves the task to
// the default group (migrated, reason class_removed), when that group can
// take it; otherwise the task ends Failed, StopReason class_removed.
func TestRecoveredTaskOfARemovedClass(t *testing.T) {
	for _, fits := range []bool{true, false} {
		t.Run(map[bool]string{true: "moved", false: "failed"}[fits], func(t *testing.T) {
			store := mem.NewMemory()
			old := &adapter{name: "old", fn: func(context.Context, model.Request) (model.Response, error) {
				return model.Response{}, sim.QuotaExhausted()
			}}
			m1, err := neumann.New(neumann.Config{Store: store, Logf: t.Logf, Devices: []bus.Device{echoDev()},
				Groups: []neumann.GroupConfig{{Class: "old", Model: old}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := m1.Start(bg); err != nil {
				t.Fatal(err)
			}
			id := submit(t, m1, proto.Instruction{Input: "q"})
			waitFor(t, func() bool { tc, _ := m1.Load(bg, id); return tc.State == task.Waiting && tc.Wait == task.WaitGroup })
			stop(t, m1)

			var caps *model.Caps
			if !fits {
				caps = &model.Caps{} // calls no tools; the task has one
			}
			m2 := startWith(t, neumann.Config{Store: store, Devices: []bus.Device{echoDev()},
				Groups: []neumann.GroupConfig{{Class: "new", Model: &adapter{name: "new", caps: caps, fn: reply("new done")}}}})
			got := waitDone(t, m2, id)
			mig := migratedEvents(t, m2, id)
			if fits {
				if got.State != task.Done || got.Class != "new" || got.LastAnswer() != "new done" {
					t.Fatalf("task ended %s in %q: %q", got.State, got.Class, got.LastAnswer())
				}
				if len(mig) != 1 || mig[0]["from"] != "old" || mig[0]["to"] != "new" || mig[0]["reason"] != "class_removed" {
					t.Fatalf("migrated %v", mig)
				}
				return
			}
			if got.State != task.Failed || got.StopReason != "class_removed" || len(mig) != 0 {
				t.Fatalf("task ended %s %q, migrated %v", got.State, got.StopReason, mig)
			}
			var check string
			for _, ev := range eventsOf(t, m2, id) {
				if ev.Kind == proto.EvMigrateRefused {
					var d map[string]string
					json.Unmarshal(ev.Data, &d)
					check = d["check"]
				}
			}
			if d := failedEvent(t, m2, id); d["stop_reason"] != "class_removed" || check != "tools" {
				t.Fatalf("failed event %v, refused by %q", d, check)
			}
			if st := m2.Status(); st.Unfinished != 0 {
				t.Fatalf("status %+v", st)
			}
		})
	}
}

// A task parked because its fallback was up but could not take it
// (migrate_refused) is not woken again each time the fallback comes back:
// nothing about the fallback's caps changed, so it waits for its own group.
func TestRefusedTaskIsNotWokenByItsFallback(t *testing.T) {
	var quota atomic.Bool
	quota.Store(true)
	big := &adapter{name: "big", fn: func(context.Context, model.Request) (model.Response, error) {
		if quota.Load() {
			return model.Response{}, sim.QuotaExhausted()
		}
		return sim.Reply("big done"), nil
	}}
	little := &adapter{name: "little", caps: &model.Caps{}, fn: reply("little done")}
	m := startWith(t, neumann.Config{Devices: []bus.Device{echoDev()}, Groups: []neumann.GroupConfig{
		{Class: "big", Model: big, Fallback: "little"},
		{Class: "little", Model: little},
	}})
	id := submit(t, m, proto.Instruction{Input: "q"})
	waitFor(t, func() bool { tc, _ := m.Load(bg, id); return tc.State == task.Waiting && tc.Wait == task.WaitGroup })
	refusals := func() int {
		n := 0
		for _, ev := range eventsOf(t, m, id) {
			if ev.Kind == proto.EvMigrateRefused {
				n++
			}
		}
		return n
	}
	for range 3 {
		if err := m.GroupReset(bg, "little"); err != nil {
			t.Fatal(err)
		}
		quiet(t, m)
	}
	if n := refusals(); n != 1 {
		t.Fatalf("%d migrate_refused events after the fallback came back three times", n)
	}
	quota.Store(false)
	if err := m.GroupReset(bg, "big"); err != nil {
		t.Fatal(err)
	}
	if got := waitDone(t, m, id); got.State != task.Done || got.Class != "big" {
		t.Fatalf("task ended %s in %q", got.State, got.Class)
	}
}
