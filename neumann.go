// Package neumann is a resident agent runtime organised like a von Neumann
// machine: vCores fetch, execute and commit one instruction at a time; tools
// sit on a bus and run as asynchronous I/O; every state change is committed
// before anything acts on it.
//
// To start: build a Machine with New (a model adapter, devices, optionally a
// durable store such as mem/sqlite), call Start once, Submit tasks, read their
// progress with Events (a replayable log) or Load, and Stop once at the end.
// See Example, examples/hello (offline), README.md and docs/overview.md; the
// full design is docs/design.md (in Chinese).
package neumann

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/cache"
	"github.com/liliang-cn/neumann/internal/core"
	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

const (
	DefaultClass     = "default"
	DefaultCores     = 4
	DefaultMaxQueued = 1000
	DefaultPerDevice = 4
)

// Config of a machine. Its execution groups are Groups; with none, Model,
// Class, Cores and Limits describe one group (the shorthand every machine
// before execution groups used).
type Config struct {
	// Groups are the execution groups, one per class (design §6, §15): each
	// a model adapter with elastic vCores, a congestion window, a breaker,
	// the limiters it takes room in, and the fallback its tasks move to
	// while it is down. Classes are unique; a Fallback names another group,
	// and following fallbacks never comes back to where it started.
	Groups []GroupConfig
	// Default is the class of a task submitted with none; empty: the first
	// group's.
	Default string

	// The one-group shorthand, used only when Groups is empty: a group of
	// class Class (default DefaultClass) on Model, taking room in Limits,
	// whose window is pinned at Cores (default DefaultCores) — at most Cores
	// vCores, started as work arrives, as before execution groups existed.
	// AutoScale(class, true) lets it adapt. Setting any of them together
	// with Groups is an error.
	Model  model.Adapter
	Cores  int
	Class  string
	Limits []string

	Devices      []bus.Device
	Store        mem.Store // nil: in-memory, not durable
	System       string
	MaxQueued    int // unfinished tasks admitted at once
	MaxPerDevice int // concurrent calls per device
	AgingAfter   time.Duration
	Clock        Clock
	Logf         func(format string, args ...any)

	// SteerFormat renders a steer as the user message the model sees. nil
	// uses DefaultSteerFormat; pass an identity function to send the text as
	// it is.
	SteerFormat func(text string) string

	// ToolPolicy, when set, decides every tool call before its first
	// dispatch is committed (design §9.7): allow it, deny it, rewrite its
	// arguments, or hold its round for an operator (Approve). See ToolPolicy:
	// it has no timeout, and one that blocks stalls a vCore, or Start.
	ToolPolicy ToolPolicy

	// AnswerCheck, when set, is asked about every final answer before its
	// task is finished (design §9.8): pass it, or reject it with feedback
	// the model answers again from. nil: answers are not checked, and
	// nothing about checks is recorded. AnswerCheckTimeout bounds one call
	// of it (default 3s, wall-clock time); a check that times out or panics
	// passes the answer. MaxAnswerRetries is how many rejected answers of a
	// task go back to the model (default 1; negative: none); a rejection
	// past them finishes the task with that answer anyway, marked
	// check_failed. See AnswerCheck.
	AnswerCheck        AnswerCheck
	AnswerCheckTimeout time.Duration
	MaxAnswerRetries   int

	// Limiters are the machine's rate limiters (design §7). A group names
	// the ones every one of its model requests takes room in (all of them at
	// once, or it waits); groups that name the same limiter share it. A
	// limiter with no Clock runs on the machine's.
	Limiters []limit.Config

	// The model step's retry policy (design §11): a task fails with
	// "model_error" after MaxModelRetries consecutive overloaded or transient
	// failures (default 8); between them it waits BackoffBase·2^(n-1), capped
	// at BackoffMax (defaults 1s and 60s), with full jitter. BackoffJitter
	// maps that step to the delay used; nil is full jitter, uniform in
	// [0, step]. Rate limits wait for the provider's retry time and do not
	// count.
	MaxModelRetries int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	BackoffJitter   func(step time.Duration) time.Duration
}

// GroupConfig is one execution group (design §6).
type GroupConfig struct {
	Class  string
	Model  model.Adapter
	Limits []string // names in Config.Limiters

	// The vCores (design §6.1): the window W starts at Initial (default 2)
	// and stays within [Min, Max] (Min default 1, Max 0 = no limit); the
	// group runs at most floor(W) vCores, started as work arrives, and
	// retires one that has waited IdleRetire (default 30s) for work while it
	// has more than Min. Cooldown is how long a halving stops the window
	// growing (default 10s).
	Min        int
	Max        int
	Initial    float64
	IdleRetire time.Duration
	Cooldown   time.Duration

	// The breaker (design §6.2): BreakerThreshold consecutive transient
	// failures open it (default 5), for BreakerOpenFor (default 30s).
	BreakerThreshold int
	BreakerOpenFor   time.Duration

	// Fallback is the class a task moves to while this group is down
	// (design §6.3); empty: it waits. OnMigrateFail is what a task does when
	// the fallback cannot take it: MigrateWait (default) or MigrateFail.
	Fallback      string
	OnMigrateFail string

	// MaxQueued bounds this class's unfinished tasks, as Config.MaxQueued
	// bounds the machine's; 0 = no bound of its own. A task moved here by
	// migration is never refused.
	MaxQueued int

	// Compaction (design §5.3): a task whose request, with its output
	// reserve, takes more than CompactAt of the model's window (Caps.Window)
	// is compacted before its next model step, keeping its newest rounds
	// within CompactKeep of the window verbatim. 0 takes the defaults, 0.8
	// and 0.3; CompactKeep must stay below CompactAt. A model whose window
	// is unknown is compacted only once its provider rejects a request as
	// too long.
	CompactAt   float64
	CompactKeep float64
}

// What a task does when its group is down and the fallback cannot take it.
const (
	MigrateWait = core.MigrateWait
	MigrateFail = core.MigrateFail
)

// MigrateError is a move to another group refused by a capability check
// (design §6.3); SetClass returns it. Check is "tools", "tool_count",
// "context", or "caps" when the target's adapter panicked reporting them.
type MigrateError = core.MigrateError

// ErrTaskBusy is returned by SetClass for a task that is executing an
// instruction or waiting for its tools: it can be moved between instructions
// only, and the caller may try again.
var ErrTaskBusy = errors.New("neumann: task is executing an instruction or waiting for its tools")

// DefaultSteerFormat tells the model that a message arrived while it was
// working, so it treats the message as part of the request in progress, not
// as a new question that replaces it. Measured on gemini-3.8-flash-high: sent
// bare, "Also tell me the weather there." made the model answer the weather
// and drop the population it had just looked up.
func DefaultSteerFormat(text string) string {
	return "[The user sent this while you were working on the request above. " +
		"Take it into account together with that request.]\n" + text
}

// Clock is the machine's notion of now: scheduling order, aging and
// deadlines read it. A Clock that also has AfterFunc(d, f) (stop func() bool)
// runs the deadline timers too, so a fake clock can make them fire; any other
// gets real timers set for the time it says is left.
type Clock interface{ Now() time.Time }

// ErrNotStarted is returned by Submit, Steer, Cancel and Resolve before Start.
var ErrNotStarted = errors.New("neumann: machine not started")

var (
	// ErrBusy is matched by every *CapacityError: admission refused a new
	// task because MaxQueued (machine or group) unfinished tasks exist.
	ErrBusy = errors.New("neumann: at capacity")
	// ErrStopped is returned by every instruction once Stop has begun.
	ErrStopped  = errors.New("neumann: machine stopped")
	ErrFinished = mem.ErrFinished
	ErrNotFound = mem.ErrNotFound
)

// CapacityError is returned by Submit when admission would exceed MaxQueued,
// the machine's (Class empty) or a group's. errors.Is(err, ErrBusy) is true
// for it.
type CapacityError struct {
	Class        string
	Count, Limit int
}

func (e *CapacityError) Error() string {
	if e.Class != "" {
		return fmt.Sprintf("neumann: at capacity: %d unfinished tasks of class %q, limit %d", e.Count, e.Class, e.Limit)
	}
	return fmt.Sprintf("neumann: at capacity: %d unfinished tasks, limit %d", e.Count, e.Limit)
}

func (e *CapacityError) Is(target error) bool { return target == ErrBusy }

// Machine is a running agent runtime: execution groups of vCores over one
// store and one bus. Build it with New, Start it once and Stop it once; every
// method is safe for concurrent use.
type Machine struct {
	cfg    Config
	store  mem.Store
	sched  *sched.Sched
	bus    *bus.Bus
	env    *core.Env
	hub    *hub
	deltas *deltaHub

	// admit guards active, perClass, classOf and stopped. active counts
	// unfinished tasks, perClass the same by class, and classOf is each
	// unfinished task's class; a task whose commit failed is forgotten by its
	// vCore and keeps its slot (a bug path only).
	admit     sync.Mutex
	active    int
	perClass  map[string]int
	classOf   map[string]string
	stopped   bool
	accepting bool           // set by Start once recovery is done
	inflight  sync.WaitGroup // Submit and Steer calls admitted and still committing

	haltCtx    context.Context
	haltCancel context.CancelFunc

	life    sync.Mutex
	started bool
	cancel  context.CancelFunc

	groups   []*core.Group // in Config order
	byClass  map[string]*core.Group
	queued   map[string]int // each class's MaxQueued; 0 = none
	defClass string
	limiters []*limit.Limiter // in Config order
}

// New validates cfg and attaches the devices to a bus (closing them if
// attaching fails). It does not touch the store or start anything: Start
// recovers unfinished tasks and starts the vCores. A config error found before
// the bus is built leaves the devices to the caller.
func New(cfg Config) (*Machine, error) {
	if cfg.Store == nil {
		cfg.Store = mem.NewMemory()
	}
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = DefaultMaxQueued
	}
	if cfg.MaxPerDevice <= 0 {
		cfg.MaxPerDevice = DefaultPerDevice
	}
	if cfg.AgingAfter <= 0 {
		cfg.AgingAfter = sched.DefaultAging
	}
	if cfg.Clock == nil {
		cfg.Clock = sched.RealClock{}
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.SteerFormat == nil {
		cfg.SteerFormat = DefaultSteerFormat
	}
	groups, pinned, err := groupConfigs(cfg)
	if err != nil {
		return nil, err
	}
	limiters, err := buildLimiters(cfg, groups)
	if err != nil {
		return nil, err
	}
	b, err := bus.New(context.Background(), cfg.MaxPerDevice, cfg.Devices...)
	if err != nil {
		return nil, err
	}
	haltCtx, haltCancel := context.WithCancel(context.Background())
	m := &Machine{
		haltCtx: haltCtx, haltCancel: haltCancel,
		cfg:      cfg,
		store:    cfg.Store,
		sched:    sched.New(cfg.Clock, cfg.AgingAfter),
		bus:      b,
		hub:      newHub(),
		deltas:   newDeltaHub(),
		perClass: map[string]int{},
		classOf:  map[string]string{},
		byClass:  map[string]*core.Group{},
		queued:   map[string]int{},
		defClass: cfg.Default,
	}
	if m.defClass == "" {
		m.defClass = groups[0].Class
	}
	// Each group's window, scaler and breaker read the machine's clock, so a
	// fake clock drives cooldowns, idle retirement and the breaker's open
	// time too.
	for _, gc := range groups {
		g := &core.Group{
			Class: gc.Class,
			Model: gc.Model,
			Ctl: group.New(group.Config{
				Min: gc.Min, Max: gc.Max, Initial: gc.Initial, Cooldown: gc.Cooldown, IdleRetire: gc.IdleRetire,
				BreakerThreshold: gc.BreakerThreshold, BreakerOpenFor: gc.BreakerOpenFor, Clock: cfg.Clock,
			}),
			Limits:        slices.Clone(gc.Limits),
			OpenFor:       gc.BreakerOpenFor,
			Fallback:      gc.Fallback,
			OnMigrateFail: gc.OnMigrateFail,
			CompactAt:     gc.CompactAt,
			CompactKeep:   gc.CompactKeep,
		}
		for _, name := range gc.Limits {
			g.Limiters = append(g.Limiters, limiters[name])
		}
		if pinned > 0 {
			g.Ctl.Window.SetCores(pinned)
		}
		m.groups = append(m.groups, g)
		m.byClass[g.Class] = g
		m.queued[g.Class] = gc.MaxQueued
	}
	m.limiters = make([]*limit.Limiter, 0, len(cfg.Limiters))
	for _, lc := range cfg.Limiters {
		m.limiters = append(m.limiters, limiters[lc.Name])
	}
	m.env = &core.Env{
		Store: m.store, Sched: m.sched, Bus: b, Model: m.byClass[m.defClass].Model, Clock: cfg.Clock,
		Commit: m.commit, Logf: cfg.Logf, SteerFormat: cfg.SteerFormat, Delta: m.deltas.publish, Halt: haltCtx,
		Groups:     m.byClass,
		MaxRetries: cfg.MaxModelRetries, BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax, Jitter: cfg.BackoffJitter,
		Guard: m.guard,
	}
	m.env.Policy = cfg.ToolPolicy
	m.env.Check, m.env.CheckTimeout, m.env.CheckRetries = cfg.AnswerCheck, cfg.AnswerCheckTimeout, cfg.MaxAnswerRetries
	m.sched.SetContext(haltCtx)
	m.sched.OnDeadline(m.deadline)
	m.sched.OnWake(m.wake)
	m.sched.OnReady(m.env.Grow)
	return m, nil
}

// groupConfigs is cfg's groups, checked: Groups, or the one group the
// shorthand describes, whose window is then pinned at the returned count.
func groupConfigs(cfg Config) ([]GroupConfig, int, error) {
	groups := slices.Clone(cfg.Groups)
	pinned := 0
	if len(groups) == 0 {
		if cfg.Model == nil {
			return nil, 0, errors.New("neumann: Config.Model or Config.Groups is required")
		}
		class := cfg.Class
		if class == "" {
			class = DefaultClass
		}
		pinned = cfg.Cores
		if pinned <= 0 {
			pinned = DefaultCores
		}
		groups = []GroupConfig{{Class: class, Model: cfg.Model, Limits: slices.Clone(cfg.Limits), Initial: float64(pinned)}}
	} else if cfg.Model != nil || cfg.Cores != 0 || cfg.Class != "" || len(cfg.Limits) > 0 {
		return nil, 0, errors.New("neumann: Config.Model, Cores, Class and Limits describe one group; with Groups, set them in each GroupConfig")
	}
	byClass := map[string]GroupConfig{}
	for _, g := range groups {
		switch {
		case g.Class == "":
			return nil, 0, errors.New("neumann: a group has no Class")
		case strings.Contains(g.Class, "/"):
			return nil, 0, fmt.Errorf("neumann: class %q must not contain '/'", g.Class)
		case g.Model == nil:
			return nil, 0, fmt.Errorf("neumann: group %q has no Model", g.Class)
		case g.Min < 0 || g.Max < 0 || g.Initial < 0 || g.MaxQueued < 0 || g.IdleRetire < 0 || g.Cooldown < 0 || g.BreakerThreshold < 0 || g.BreakerOpenFor < 0:
			return nil, 0, fmt.Errorf("neumann: group %q has a negative setting", g.Class)
		case g.Max > 0 && g.Max < g.Min:
			return nil, 0, fmt.Errorf("neumann: group %q has Max %d below Min %d", g.Class, g.Max, g.Min)
		case g.CompactAt < 0 || g.CompactAt > 1 || g.CompactKeep < 0 || g.CompactKeep >= compactAt(g):
			return nil, 0, fmt.Errorf("neumann: group %q: CompactAt %v and CompactKeep %v must satisfy 0 <= CompactKeep < CompactAt <= 1", g.Class, g.CompactAt, g.CompactKeep)
		case g.OnMigrateFail != "" && g.OnMigrateFail != MigrateWait && g.OnMigrateFail != MigrateFail:
			return nil, 0, fmt.Errorf("neumann: group %q: OnMigrateFail %q is neither %q nor %q", g.Class, g.OnMigrateFail, MigrateWait, MigrateFail)
		}
		if _, dup := byClass[g.Class]; dup {
			return nil, 0, fmt.Errorf("neumann: two groups of class %q", g.Class)
		}
		byClass[g.Class] = g
	}
	for _, g := range groups {
		seen := map[string]bool{g.Class: true}
		for next := g.Fallback; next != ""; next = byClass[next].Fallback {
			if _, ok := byClass[next]; !ok {
				return nil, 0, fmt.Errorf("neumann: group %q falls back to %q, which is not a group", g.Class, next)
			}
			if seen[next] {
				return nil, 0, fmt.Errorf("neumann: the fallbacks of group %q come back to %q", g.Class, next)
			}
			seen[next] = true
		}
	}
	if cfg.Default != "" {
		if _, ok := byClass[cfg.Default]; !ok {
			return nil, 0, fmt.Errorf("neumann: Default names %q, which is not a group", cfg.Default)
		}
	}
	return groups, pinned, nil
}

// compactAt is a group's compaction threshold, the default for 0.
func compactAt(g GroupConfig) float64 {
	if g.CompactAt > 0 {
		return g.CompactAt
	}
	return core.DefaultCompactAt
}

// buildLimiters builds cfg.Limiters by name and checks that every name a
// group gives is one of them.
func buildLimiters(cfg Config, groups []GroupConfig) (map[string]*limit.Limiter, error) {
	out := make(map[string]*limit.Limiter, len(cfg.Limiters))
	for _, lc := range cfg.Limiters {
		if _, dup := out[lc.Name]; dup {
			return nil, fmt.Errorf("neumann: two limiters named %q", lc.Name)
		}
		if lc.Clock == nil {
			lc.Clock = limitClock{cfg.Clock}
		}
		l, err := limit.New(lc)
		if err != nil {
			return nil, err
		}
		out[lc.Name] = l
	}
	for _, g := range groups {
		for _, name := range g.Limits {
			if out[name] == nil {
				return nil, fmt.Errorf("neumann: group %q: Limits names %q, which is not in Limiters", g.Class, name)
			}
		}
	}
	return out, nil
}

// limitClock runs a limiter on the machine's clock: on its timers when it has
// them (a fake clock moves limiter waits too), real ones otherwise.
type limitClock struct{ c Clock }

func (l limitClock) Now() time.Time { return l.c.Now() }

func (l limitClock) After(d time.Duration) <-chan time.Time {
	tm, ok := l.c.(sched.Timers)
	if !ok {
		return time.After(d)
	}
	ch := make(chan time.Time, 1)
	tm.AfterFunc(d, func() { ch <- l.c.Now() })
	return ch
}

// guard admits work a timer starts: it is refused once the machine is
// stopping, and Stop waits for what it admitted.
func (m *Machine) guard() (func(), bool) {
	m.admit.Lock()
	defer m.admit.Unlock()
	if m.stopped {
		return nil, false
	}
	m.inflight.Add(1)
	return m.inflight.Done, true
}

// wake runs when a task's backoff is over: it is committed Ready and queued
// with its old ReadySince, so it does not lose its place. Like a deadline, it
// does nothing once the machine is stopping: the task stays Waiting(Backoff)
// in the store, and the next start runs it.
func (m *Machine) wake(id string) {
	release, ok := m.guard()
	if !ok {
		return
	}
	defer release()
	m.env.Wake(m.haltCtx, id, task.WaitBackoff)
}

// GroupReset brings a class's execution group back after its breaker opened
// (design §6.2, §9.6): the breaker closes whatever opened it — an exhausted
// quota never closes by itself — group_up is announced, and every task
// waiting for the group runs again.
func (m *Machine) GroupReset(ctx context.Context, class string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	return m.env.GroupReset(ctx, class)
}

// commit is the machine's single write path for vCores and tool batches.
func (m *Machine) commit(ctx context.Context, c mem.CommitSet) error {
	evs, err := m.store.Commit(ctx, c)
	if err != nil {
		return err
	}
	var ended []string
	for _, ev := range evs {
		if ev.Terminal() {
			ended = append(ended, ev.TaskID)
		}
	}
	if len(ended) > 0 || c.Task != nil {
		m.admit.Lock()
		m.active -= len(ended)
		for _, id := range ended {
			if class, ok := m.classOf[id]; ok {
				m.perClass[class]--
				delete(m.classOf, id)
			}
		}
		if t := c.Task; t != nil && !t.State.Terminal() {
			// A migration: the task now counts against its new class.
			if class, ok := m.classOf[t.ID]; ok && class != t.Class {
				m.perClass[class]--
				m.perClass[t.Class]++
				m.classOf[t.ID] = t.Class
			}
		}
		m.admit.Unlock()
	}
	for _, id := range ended {
		m.bus.ForgetTask(id) // its L2 entries go with it (design §10)
	}
	m.hub.notify()
	return nil
}

// Start launches the vCores. ctx bounds only fetching: cancelling it stops
// vCores taking new instructions, but the machine keeps running in-flight
// work and holds its resources until Stop. A stopped machine cannot restart.
//
// Before any vCore runs, Start recovers every unfinished task in the store
// (design §4.5): a task caught mid model call is counted unconfirmed and
// runs the step again; a tool round caught in flight is settled call by call
// — finished calls keep their results, read-only and idempotent calls are
// sent again under the same idempotency key, and a call with side effects
// whose outcome is now unknown is either reported to the model (if its tool
// allows it) or held for an operator (see Resolve). If recovery fails, Start
// returns the error and may be called again.
func (m *Machine) Start(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	m.admit.Lock()
	stopped := m.stopped
	m.admit.Unlock()
	if stopped {
		return ErrStopped
	}
	if m.started {
		return errors.New("neumann: already started")
	}
	sends, err := m.recover(ctx)
	if err != nil {
		return err
	}
	m.started = true
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	warmed := map[model.Adapter]bool{}
	for _, g := range m.groups {
		if _, ok := g.Model.(model.Streamer); !ok {
			// Not an error, but easy to cause by accident: a wrapper around an
			// adapter that forwards only Call hides the adapter's Stream.
			m.cfg.Logf("neumann: model %s of class %s does not implement model.Streamer; Deltas will stay silent for it", g.Model.Provider(), g.Class)
		}
		w, ok := g.Model.(model.Warmer)
		if !ok || warmed[g.Model] {
			continue
		}
		warmed[g.Model] = true
		// Open the connection (and its TLS or QUIC handshake) now, so the
		// first task does not pay for it. In the background: a slow or failed
		// warm-up only means the first call connects as it would have anyway.
		go func() {
			wctx, cancel := context.WithTimeout(runCtx, 10*time.Second)
			defer cancel()
			if err := w.Warm(wctx); err != nil {
				m.cfg.Logf("neumann: warming the model connection failed: %v", err)
			}
		}()
	}
	for _, d := range sends {
		m.env.Dispatch(d.id, d.allow, d.deny, d.batch)
	}
	// vCores start as work arrives (design §6.1): now for the recovered
	// ready queues, later whenever a task joins one.
	m.env.StartCores(runCtx)
	return nil
}

// Stop shuts the machine down. It first refuses new work and closes the
// scheduler, then waits for the vCores and in-flight tool batches to commit
// and hand their tasks back. If ctx ends first it halts what is left (running
// calls are cancelled and their results are not committed; those tasks keep
// their last committed state) and waits at most 2s more. In every case it
// then closes the event hub and the devices. It returns ctx.Err() when the
// deadline was hit and is idempotent: later calls return nil. A model adapter
// or device that ignores its context can outlive Stop, but whatever it
// returns after the halt is not committed.
func (m *Machine) Stop(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	m.admit.Lock()
	already := m.stopped
	m.stopped = true
	m.admit.Unlock()
	if already {
		return nil
	}
	// No instruction is fetched, and no vCore started, from here on. Close
	// only stops fetching: tool batches and admitted submits can still hand
	// tasks back. Every vCore retires (core_down, shutdown) once its
	// instruction is committed.
	m.env.CloseCores()
	m.sched.Close()
	// Submits, steers and resolves admitted before the flag finish their
	// commits, within the deadline. One that finishes later is durable all
	// the same, and recovery picks it up on the next start.
	admitted := make(chan struct{})
	go func() { m.inflight.Wait(); close(admitted) }()
	select {
	case <-admitted:
	case <-ctx.Done():
	}
	var err error
	if m.started {
		done := make(chan struct{})
		go func() {
			m.env.WaitCores()
			m.env.CloseDispatch() // a late Resolve cannot start a batch while we wait
			m.env.WaitBatches()
			m.env.CloseLate() // late results being recorded land before the store is let go
			close(done)
		}()
		select {
		case <-done:
		default:
			select {
			case <-done:
			case <-ctx.Done():
				select {
				case <-done: // drained as the deadline hit: still a clean stop
				default:
					err = ctx.Err()
					m.haltCancel()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
					}
				}
			}
		}
		m.cancel()
	}
	m.haltCancel()
	m.hub.close()
	m.deltas.close()
	return errors.Join(err, m.bus.Close())
}

// Submit admits a task. It returns only after the task is committed, so a
// returned id is a task that exists (design §4.3).
func (m *Machine) Submit(ctx context.Context, in proto.Instruction) (string, error) {
	if in.Kind != "" && in.Kind != proto.KindSubmit {
		return "", fmt.Errorf("neumann: Submit got an instruction of kind %q", in.Kind)
	}
	if strings.TrimSpace(in.Input) == "" {
		return "", errors.New("neumann: submit needs input")
	}
	if in.Priority < 0 || in.Priority > 1 {
		return "", fmt.Errorf("neumann: priority %d out of range 0..1", in.Priority)
	}
	class := in.Class
	if class == "" {
		class = m.defClass
	}
	if m.byClass[class] == nil {
		return "", fmt.Errorf("neumann: unknown class %q", class)
	}
	id := in.TaskID
	if strings.Contains(id, "/") {
		return "", fmt.Errorf("neumann: task id %q must not contain '/'", id)
	}
	if id == "" {
		id = uuid.NewString()
	}
	allow, deny := slices.Clone(in.Allow), slices.Clone(in.Deny)
	t := &task.TCB{
		ID:         id,
		State:      task.Ready,
		PC:         task.StepModel,
		Class:      class,
		PinClass:   in.PinClass,
		Priority:   in.Priority,
		ReadySince: m.cfg.Clock.Now(),
		System:     m.cfg.System,
		Tools:      m.bus.Specs(allow, deny),
		Regs:       []task.Message{task.Text(task.RoleUser, in.Input)},
		Allow:      allow,
		Deny:       deny,
		Version:    1,
	}
	if in.Budget != nil {
		t.Budget = *in.Budget
	}

	// The slot is reserved under the lock, so concurrent submits cannot all
	// slip past the limit together; the commit happens outside it, so submits
	// do not queue behind each other's disk writes. Stop waits for reserved
	// submits to finish before it closes anything.
	if err := m.enterTask(id, class); err != nil {
		return "", err
	}
	defer m.inflight.Done()
	accepted := proto.NewEvent(proto.EvAccepted, id, map[string]any{"class": class, "priority": in.Priority, "pin_class": in.PinClass})
	if _, err := m.store.Commit(ctx, mem.CommitSet{Task: t, NewTask: true, Events: []proto.Event{accepted}}); err != nil {
		m.admit.Lock()
		m.active--
		m.perClass[class]--
		delete(m.classOf, id)
		m.admit.Unlock()
		return "", err
	}
	m.sched.Add(t)
	m.hub.notify()
	return id, nil
}

// Cancel stops a task (design §9.5). It returns once the cancel is committed:
// from then on the task ends Cancelled, with StopReason "cancelled", even
// across a restart. A model call in flight for it is interrupted at once and
// in-flight tool calls see their context cancelled. The final commit marks
// calls that never reached their device cancelled; a call that did is marked
// unknown with a reconcile_needed event, and the task does not wait for it. A
// result that arrives after that is kept on its call record as late (a
// tool_late event) and never reaches the model. Each undelivered steer is
// reported by a steer_dropped event.
//
// Cancel returns ErrFinished for a task that has already ended and
// ErrNotFound for an unknown one. A task that ends on its own while the cancel
// is being committed ends as it ended.
func (m *Machine) Cancel(ctx context.Context, taskID string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	return m.env.Cancel(ctx, taskID, core.ReasonCancelled)
}

// deadline runs when a task's deadline passes (design §9.4): a cancel whose
// reason is the deadline, so the task ends Failed with StopReason "deadline".
// It runs on a timer's goroutine, during recovery as well, and does nothing
// once the machine is stopping: the cancel is then left to the next start,
// which finds the deadline passed and fires it at once.
func (m *Machine) deadline(id string) {
	m.admit.Lock()
	if m.stopped {
		m.admit.Unlock()
		return
	}
	m.inflight.Add(1)
	m.admit.Unlock()
	defer m.inflight.Done()
	err := m.env.Cancel(m.haltCtx, id, core.ReasonDeadline)
	if err != nil && !errors.Is(err, ErrFinished) && !errors.Is(err, ErrNotFound) && m.haltCtx.Err() == nil {
		m.cfg.Logf("neumann: deadline of task %s: %v", id, err)
	}
}

// enter admits one outside request: it fails once the machine is stopped.
// The caller must call m.inflight.Done when its commit is over.
func (m *Machine) enter() error { return m.enterTask("", "") }

// enterTask is enter for a new task id of class (id empty: not a new task):
// it also fails when the machine, or the class, is full, and reserves the
// task's slot otherwise.
func (m *Machine) enterTask(id, class string) error {
	m.admit.Lock()
	defer m.admit.Unlock()
	if m.stopped {
		return ErrStopped
	}
	if !m.accepting {
		// Before Start has recovered the store, a new task would be read back
		// by recovery and scheduled twice.
		return ErrNotStarted
	}
	if id != "" {
		if m.active >= m.cfg.MaxQueued {
			return &CapacityError{Count: m.active, Limit: m.cfg.MaxQueued}
		}
		if limit := m.queued[class]; limit > 0 && m.perClass[class] >= limit {
			return &CapacityError{Class: class, Count: m.perClass[class], Limit: limit}
		}
		if _, dup := m.classOf[id]; dup {
			return fmt.Errorf("neumann: task %s: %w", id, mem.ErrExists)
		}
		m.active++
		m.perClass[class]++
		m.classOf[id] = class
	}
	m.inflight.Add(1)
	return nil
}

// Steer queues a message for a task. It returns nil only after the steer is
// committed; it is then either sent to the model by the task's next model call
// or reported by a steer_dropped event (the task ended first: budget, model
// failure). A steer that arrives while the final answer is being produced stays
// pending, and the task takes one more model step to answer it.
func (m *Machine) Steer(ctx context.Context, taskID, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("neumann: steer needs text")
	}
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	_, err := m.store.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{
		{TaskID: taskID, Kind: mem.InterruptSteer, Text: text},
	}})
	if err == nil {
		m.hub.notify()
	}
	return err
}

// Events streams committed events with Seq > fromSeq, in order. The stream
// ends when ctx ends, or after the machine stops and the subscriber has read
// everything committed. A slow subscriber never blocks the machine and never
// misses an event: it reads the log at its own pace.
func (m *Machine) Events(ctx context.Context, fromSeq int64) <-chan proto.Event {
	out := make(chan proto.Event, 64)
	go func() {
		defer close(out)
		after := fromSeq
		for {
			wake, closed := m.hub.wait()
			evs, err := m.store.Events(ctx, after, 256)
			if err != nil {
				if ctx.Err() == nil {
					m.cfg.Logf("neumann: read events: %v", err)
				}
				return
			}
			for _, ev := range evs {
				select {
				case out <- ev:
					after = ev.Seq
				case <-ctx.Done():
					return
				}
			}
			if len(evs) > 0 {
				continue
			}
			if closed {
				return
			}
			select {
			case <-wake:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// EventLog reads committed events with Seq > afterSeq; limit 0 means all.
func (m *Machine) EventLog(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	return m.store.Events(ctx, afterSeq, limit)
}

// History is every message a task has had in its registers, in order, as
// if nothing had ever been compacted: its request, the messages compaction
// swapped out (from the store's archive), then the registers after the
// summary, with no summary in it (design §5.3). It reads the committed
// task first and takes as many archived messages as that task says were
// archived, so a compaction committed between the two reads changes
// nothing.
func (m *Machine) History(ctx context.Context, id string) ([]task.Message, error) {
	t, err := m.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	archived, err := m.store.Archive(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(archived) < t.Archived {
		return nil, fmt.Errorf("neumann: task %s has archived %d messages, the store holds %d", id, t.Archived, len(archived))
	}
	head, _ := task.Units(t.Regs)
	out := slices.Concat(t.Regs[:head], archived[:t.Archived])
	for _, msg := range t.Regs[head:] {
		if !msg.Summary {
			out = append(out, msg)
		}
	}
	return out, nil
}

// Load returns the committed state of a task.
func (m *Machine) Load(ctx context.Context, id string) (*task.TCB, error) {
	return m.store.Load(ctx, id)
}

// Calls returns a task's tool call records.
func (m *Machine) Calls(ctx context.Context, id string) ([]mem.CallRecord, error) {
	return m.store.Calls(ctx, id)
}

// Status is a cheap snapshot of the machine (design §14), returned by
// Machine.Status: no model call, no store read.
type Status struct {
	Cores      int `json:"cores"`      // live vCores, every group
	Ready      int `json:"ready"`      // in ready queues
	Waiting    int `json:"waiting"`    // in the wait set
	Held       int `json:"held"`       // on a vCore or being completed by a tool batch
	Unfinished int `json:"unfinished"` // admitted and not yet terminal
	Approval   int `json:"approval"`   // waiting for an operator's approval (Approve)

	DeltasDropped int64 `json:"deltas_dropped"` // streamed pieces a slow subscriber missed

	Groups   []GroupStatus  `json:"groups"`
	Limiters []limit.Status `json:"limiters"`
	Cache    cache.Stats    `json:"cache"` // L2, the tool result cache (design §10)
}

// GroupStatus is one execution group (design §6, §14): its window, its
// vCores, its queue and its breaker.
type GroupStatus struct {
	Class    string   `json:"class"`
	Model    string   `json:"model"` // the adapter's Provider
	Limits   []string `json:"limits,omitempty"`
	Fallback string   `json:"fallback,omitempty"`

	W        float64       `json:"w"`      // congestion window
	Target   int           `json:"target"` // floor(W) within [Min, Max]: the vCores the group runs at most
	Min      int           `json:"min"`
	Max      int           `json:"max"`   // 0 = no limit
	Auto     bool          `json:"auto"`  // false while an operator pins W (SetCores)
	Epoch    uint64        `json:"epoch"` // congestion epoch: one halving each
	Cooling  bool          `json:"cooling"`
	Live     int           `json:"live"` // vCores alive
	Busy     int           `json:"busy"` // ...executing an instruction
	Ready    int           `json:"ready"`
	Waiting  int           `json:"waiting"`       // in the wait set, for any reason
	ForGroup int           `json:"waiting_group"` // ...of which for this group to come back
	Approval int           `json:"approval"`      // ...of which for an operator's approval
	Longest  time.Duration `json:"longest_wait"`  // how long the oldest ready task has been ready

	Breaker   string    `json:"breaker"`         // closed, open, half_open
	Cause     string    `json:"cause,omitempty"` // why it is open: transient, quota
	Failures  int       `json:"failures"`        // consecutive transient failures while closed
	OpenedAt  time.Time `json:"opened_at,omitzero"`
	NextProbe time.Time `json:"next_probe,omitzero"` // when a probe is next let through; zero for quota
}

// Status is a cheap snapshot: no model call, no store read.
func (m *Machine) Status() Status {
	r, w, h := m.sched.Counts()
	m.admit.Lock()
	u := m.active
	m.admit.Unlock()
	st := Status{Ready: r, Waiting: w, Held: h, Unfinished: u, DeltasDropped: m.deltas.dropped.Load(), Cache: m.bus.CacheStats()}
	queues, now := m.sched.Queues(), m.cfg.Clock.Now()
	for _, g := range m.groups {
		gs, q := g.Ctl.Snapshot(), queues[g.Class]
		win, br := gs.Window, gs.Breaker
		st.Cores += gs.Running
		st.Groups = append(st.Groups, GroupStatus{
			Class: g.Class, Model: g.Model.Provider(), Limits: slices.Clone(g.Limits), Fallback: g.Fallback,
			W: win.W, Target: win.Target, Min: win.Min, Max: win.Max, Auto: win.Auto, Epoch: win.Epoch, Cooling: win.Cooling,
			Live: gs.Running, Busy: gs.Busy, Ready: q.Ready, Waiting: q.Waiting, ForGroup: q.ForGroup, Approval: q.Approval, Longest: longest(q, now),
			Breaker: br.State.String(), Cause: string(br.Cause), Failures: br.Failures, OpenedAt: br.OpenedAt, NextProbe: br.NextProbe,
		})
	}
	for _, q := range queues {
		st.Approval += q.Approval
	}
	for _, l := range m.limiters {
		st.Limiters = append(st.Limiters, l.Status())
	}
	return st
}

func longest(q sched.Queue, now time.Time) time.Duration {
	if q.Oldest.IsZero() {
		return 0
	}
	return max(now.Sub(q.Oldest), 0)
}

// QueueStatus is the scheduler's queues by class (design §9.1, §9.6).
type QueueStatus struct {
	Classes []ClassQueue `json:"classes"`
}

// ClassQueue is one class's queues: its ready queue (and how much of it is
// background work), its wait set, and how long its oldest ready task has
// been ready.
type ClassQueue struct {
	Class      string        `json:"class"`
	Ready      int           `json:"ready"`
	Background int           `json:"background"`
	Waiting    int           `json:"waiting"`
	ForGroup   int           `json:"waiting_group"`
	Approval   int           `json:"approval"`
	Longest    time.Duration `json:"longest_wait"`
}

// Queue reports the queues of every class the machine runs, in Config order,
// and of any other class a recovered task still has. Like Status, it is a
// cheap copy: no store read.
func (m *Machine) Queue() QueueStatus {
	queues, now := m.sched.Queues(), m.cfg.Clock.Now()
	var out QueueStatus
	add := func(class string, q sched.Queue) {
		out.Classes = append(out.Classes, ClassQueue{
			Class: class, Ready: q.Ready, Background: q.Background, Waiting: q.Waiting, ForGroup: q.ForGroup, Approval: q.Approval, Longest: longest(q, now),
		})
	}
	for _, g := range m.groups {
		add(g.Class, queues[g.Class])
		delete(queues, g.Class)
	}
	others := slices.Sorted(maps.Keys(queues))
	for _, class := range others {
		add(class, queues[class])
	}
	return out
}

// SetCores pins class's window at n (design §6.1, §9.6): the group runs at
// most n vCores (n is clamped to the group's [Min, Max]) and the window no
// longer adapts. More vCores start as work asks for them; extra ones retire
// once their current instruction is committed (core_down, manual).
func (m *Machine) SetCores(class string, n int) error {
	g := m.byClass[class]
	if g == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	if n < 1 {
		return fmt.Errorf("neumann: SetCores(%q, %d): a group needs at least one vCore", class, n)
	}
	g.Ctl.Window.SetCores(n)
	m.env.Grow(class)
	return nil
}

// AutoScale turns class's adaptive window back on (from where SetCores left
// it), or freezes it where it is.
func (m *Machine) AutoScale(class string, on bool) error {
	g := m.byClass[class]
	if g == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	g.Ctl.Window.AutoScale(on)
	m.env.Grow(class)
	return nil
}

// SetClass moves a task to another execution group (design §6.3, §9.6): the
// operator's migration. The same capability checks as a fallback move apply,
// and a refusal is a *MigrateError that changes nothing; PinClass does not
// stop an operator. It returns once the move is committed: the task keeps its
// ReadySince, a migrated event (reason manual) records it, and its next model
// step goes to the new group, leaving the old provider's reasoning out. A
// task waiting for the group it leaves is ready again.
//
// A task can be moved between instructions only: one executing an
// instruction or waiting for its tools gives ErrTaskBusy, and the caller may
// try again. A finished task gives ErrFinished, an unknown one ErrNotFound.
func (m *Machine) SetClass(ctx context.Context, taskID, class string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	if m.byClass[class] == nil {
		return fmt.Errorf("neumann: unknown class %q", class)
	}
	t, ok := m.sched.Take(taskID, func(t *task.TCB) bool {
		switch t.State {
		case task.Ready, task.Suspended:
			return true
		case task.Waiting:
			return t.Wait != task.WaitIO
		}
		return false
	})
	if !ok {
		tc, err := m.store.Load(ctx, taskID)
		switch {
		case err != nil:
			return err
		case tc.State.Terminal():
			return fmt.Errorf("neumann: task %s: %w", taskID, ErrFinished)
		}
		return fmt.Errorf("neumann: task %s: %w", taskID, ErrTaskBusy)
	}
	err := m.env.SetClass(ctx, t, class)
	if errors.Is(err, mem.ErrCancelPending) {
		m.env.Finalise(ctx, t) // cancelled first: it ends where it is
		return fmt.Errorf("neumann: task %s was cancelled before it moved: %w", taskID, ErrFinished)
	}
	m.env.HandBack(ctx, t, nil) // t is unchanged when err != nil
	return err
}

type recoveredDispatch struct {
	id          string
	allow, deny []string
	batch       []core.Send
}

// recover settles every unfinished task and hands them to the scheduler. It
// commits every decision before any task is scheduled, so a failure part way
// leaves nothing scheduled and can be retried by calling Start again.
func (m *Machine) recover(ctx context.Context) ([]recoveredDispatch, error) {
	tasks, err := m.store.Unfinished(ctx)
	if err != nil {
		return nil, fmt.Errorf("neumann: read unfinished tasks: %w", err)
	}
	// Tasks are independent, so they recover concurrently and their commits
	// share transactions (group commit) instead of paying one fsync each.
	batches := make([][]core.Send, len(tasks))
	errs := make([]error, len(tasks))
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup
	for i, t := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			if m.byClass[t.Class] == nil && !t.State.Terminal() {
				// Its class is gone from the configuration: it moves to the
				// default group, or fails (design §4.5).
				m.cfg.Logf("neumann: recovered task %s belongs to class %q, which this machine does not run; moving it to %q", t.ID, t.Class, m.defClass)
				if errs[i] = m.env.Rehome(ctx, t, m.defClass); errs[i] != nil {
					return
				}
			}
			batches[i], errs[i] = m.env.Recover(ctx, t)
		}()
	}
	wg.Wait()
	var sends []recoveredDispatch
	unfinished := 0
	for i, t := range tasks {
		if errs[i] != nil {
			return nil, fmt.Errorf("neumann: recover task %s: %w", t.ID, errs[i])
		}
		if len(batches[i]) > 0 {
			sends = append(sends, recoveredDispatch{t.ID, slices.Clone(t.Allow), slices.Clone(t.Deny), batches[i]})
		}
		if !t.State.Terminal() { // a committed cancel was finished
			unfinished++
		}
	}
	// Counted before the tasks are scheduled: a deadline that has passed
	// fires as soon as its task is, and its final commit counts itself out.
	m.admit.Lock()
	m.active = unfinished
	clear(m.perClass)
	clear(m.classOf)
	for _, t := range tasks {
		if !t.State.Terminal() {
			m.perClass[t.Class]++
			m.classOf[t.ID] = t.Class
		}
	}
	m.admit.Unlock()
	for _, t := range tasks {
		m.sched.Add(t) // a finished one is forgotten; deadlines are armed again
	}
	m.admit.Lock()
	m.accepting = true
	m.admit.Unlock()
	if len(tasks) > 0 {
		m.cfg.Logf("neumann: recovered %d unfinished tasks; %d resume with calls in flight", len(tasks), len(sends))
	}
	return sends, nil
}

// Outcome is an operator's answer for a call whose outcome a restart made
// unknown: Retry sends it again (a new attempt under a new idempotency key);
// otherwise Result is what the call returned and IsError marks a failure.
type Outcome = core.Outcome

// ErrNotReconciling is returned by Resolve for a task or call that is not
// held for an operator.
var ErrNotReconciling = core.ErrNotReconciling

// Resolve answers a call held for reconciliation (a reconcile_needed event
// names it). When the task holds no other unknown call, its round is settled
// and the task resumes.
func (m *Machine) Resolve(ctx context.Context, taskID, callID string, o Outcome) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.inflight.Done()
	t, ok := m.sched.CheckoutIf(taskID, func(t *task.TCB) bool {
		return t.State == task.Waiting && t.Wait == task.WaitReconcile
	})
	if !ok {
		if _, err := m.store.Load(ctx, taskID); err != nil {
			return err
		}
		return fmt.Errorf("neumann: task %s: %w", taskID, ErrNotReconciling)
	}
	batch, err := m.env.Resolve(ctx, t, callID, o)
	if errors.Is(err, mem.ErrCancelPending) {
		// Cancelled first: the outcome is not recorded, and the task ends.
		m.env.Finalise(ctx, t)
		return fmt.Errorf("neumann: task %s was cancelled before the outcome was recorded: %w", taskID, ErrNotReconciling)
	}
	allow, deny := slices.Clone(t.Allow), slices.Clone(t.Deny)
	// t is unchanged when err != nil. Cancelled while the operator held it:
	// finalised instead, and the batch is never sent.
	queued := m.env.HandBack(ctx, t, batch)
	if err != nil {
		return err
	}
	if !queued {
		return nil
	}
	if len(batch) > 0 {
		m.env.Dispatch(taskID, allow, deny, batch)
	}
	return nil
}
