// Package neumann is a resident agent runtime organised like a von Neumann
// machine: vCores fetch, execute and commit one instruction at a time; tools
// sit on a bus and run as asynchronous I/O; every state change is committed
// before anything acts on it. See docs/design.md.
package neumann

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/core"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sched"
	"github.com/liliang-cn/neumann/task"
)

const (
	DefaultClass     = "default"
	DefaultCores     = 4
	DefaultMaxQueued = 1000
	DefaultPerDevice = 4
)

// Config of a phase-1 machine: one execution group with a fixed vCore count.
type Config struct {
	Model        model.Adapter
	Devices      []bus.Device
	Store        mem.Store // nil: in-memory, not durable
	Cores        int
	Class        string
	System       string
	MaxQueued    int // unfinished tasks admitted at once
	MaxPerDevice int // concurrent calls per device
	AgingAfter   time.Duration
	Clock        sched.Clock
	Logf         func(format string, args ...any)
}

var (
	ErrBusy     = errors.New("neumann: at capacity")
	ErrStopped  = errors.New("neumann: machine stopped")
	ErrFinished = mem.ErrFinished
	ErrNotFound = mem.ErrNotFound
)

// CapacityError is returned by Submit when admission would exceed MaxQueued.
// errors.Is(err, ErrBusy) is true for it.
type CapacityError struct{ Count, Limit int }

func (e *CapacityError) Error() string {
	return fmt.Sprintf("neumann: at capacity: %d unfinished tasks, limit %d", e.Count, e.Limit)
}

func (e *CapacityError) Is(target error) bool { return target == ErrBusy }

type Machine struct {
	cfg   Config
	store mem.Store
	sched *sched.Sched
	bus   *bus.Bus
	env   *core.Env
	hub   *hub

	// admit guards active and stopped. active counts unfinished tasks; a task
	// whose commit failed is forgotten by its vCore and keeps its slot (a bug
	// path only, in phase 1).
	admit   sync.Mutex
	active  int
	stopped bool

	haltCtx    context.Context
	haltCancel context.CancelFunc

	life    sync.Mutex
	started bool
	cancel  context.CancelFunc
	cores   sync.WaitGroup
}

func New(cfg Config) (*Machine, error) {
	if cfg.Model == nil {
		return nil, errors.New("neumann: Config.Model is required")
	}
	if cfg.Store == nil {
		cfg.Store = mem.NewMemory()
	}
	if cfg.Cores <= 0 {
		cfg.Cores = DefaultCores
	}
	if cfg.Class == "" {
		cfg.Class = DefaultClass
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
	b, err := bus.New(context.Background(), cfg.MaxPerDevice, cfg.Devices...)
	if err != nil {
		return nil, err
	}
	haltCtx, haltCancel := context.WithCancel(context.Background())
	m := &Machine{
		haltCtx: haltCtx, haltCancel: haltCancel,
		cfg:   cfg,
		store: cfg.Store,
		sched: sched.New(cfg.Clock, cfg.AgingAfter),
		bus:   b,
		hub:   newHub(),
	}
	m.env = &core.Env{
		Store: m.store, Sched: m.sched, Bus: b, Model: cfg.Model, Clock: cfg.Clock,
		Commit: m.commit, Logf: cfg.Logf, Halt: haltCtx,
	}
	return m, nil
}

// commit is the machine's single write path for vCores and tool batches.
func (m *Machine) commit(ctx context.Context, c mem.CommitSet) error {
	evs, err := m.store.Commit(ctx, c)
	if err != nil {
		return err
	}
	ended := 0
	for _, ev := range evs {
		if ev.Terminal() {
			ended++
		}
	}
	if ended > 0 {
		m.admit.Lock()
		m.active -= ended
		m.admit.Unlock()
	}
	m.hub.notify()
	return nil
}

// Start launches the vCores. ctx bounds only fetching: cancelling it stops
// vCores taking new instructions, but the machine keeps running in-flight
// work and holds its resources until Stop. A stopped machine cannot restart.
// Phase 1 keeps nothing across processes, so there is nothing to recover.
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
	m.started = true
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	for i := range m.cfg.Cores {
		m.cores.Add(1)
		go func() {
			defer m.cores.Done()
			m.env.RunVCore(runCtx, m.cfg.Class, i)
		}()
	}
	return nil
}

// Stop shuts the machine down. It first refuses new work and closes the
// scheduler, then waits for the vCores and in-flight tool batches to commit
// and hand their tasks back. If ctx ends first it halts what is left (running
// calls are cancelled and their results are not committed; those tasks keep
// their last committed state) and waits at most 2s more. In every case it
// then closes the event hub and the devices. It returns ctx.Err() when the
// deadline was hit and is idempotent: later calls return nil.
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
	m.sched.Close()
	var err error
	if m.started {
		done := make(chan struct{})
		go func() {
			m.cores.Wait()
			m.env.WaitBatches()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
			m.haltCancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
		m.cancel()
	}
	m.haltCancel()
	m.hub.close()
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
		class = m.cfg.Class
	}
	if class != m.cfg.Class {
		return "", fmt.Errorf("neumann: unknown class %q", class)
	}
	id := in.TaskID
	if id == "" {
		id = uuid.NewString()
	}
	allow, deny := slices.Clone(in.Allow), slices.Clone(in.Deny)
	t := &task.TCB{
		ID:         id,
		State:      task.Ready,
		PC:         task.StepModel,
		Class:      class,
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

	// Admission is decided, committed and recorded under one lock, so
	// concurrent submits cannot all slip past the limit together.
	m.admit.Lock()
	defer m.admit.Unlock()
	if m.stopped {
		return "", ErrStopped
	}
	if m.active >= m.cfg.MaxQueued {
		return "", &CapacityError{Count: m.active, Limit: m.cfg.MaxQueued}
	}
	accepted := proto.NewEvent(proto.EvAccepted, id, map[string]any{"class": class, "priority": in.Priority})
	if _, err := m.store.Commit(ctx, mem.CommitSet{Task: t, NewTask: true, Events: []proto.Event{accepted}}); err != nil {
		return "", err
	}
	m.active++
	m.sched.Add(t)
	m.hub.notify()
	return id, nil
}

// Steer queues a message for a task. It returns nil only after the steer is
// committed; it is then either delivered before the task's next model call or
// reported by a steer_dropped event.
func (m *Machine) Steer(ctx context.Context, taskID, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("neumann: steer needs text")
	}
	m.admit.Lock()
	stopped := m.stopped
	m.admit.Unlock()
	if stopped {
		return ErrStopped
	}
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

// Load returns the committed state of a task.
func (m *Machine) Load(ctx context.Context, id string) (*task.TCB, error) {
	return m.store.Load(ctx, id)
}

// Calls returns a task's tool call records.
func (m *Machine) Calls(ctx context.Context, id string) ([]mem.CallRecord, error) {
	return m.store.Calls(ctx, id)
}

type Status struct {
	Cores      int `json:"cores"`
	Ready      int `json:"ready"`      // in ready queues
	Waiting    int `json:"waiting"`    // in the wait set
	Held       int `json:"held"`       // on a vCore or being completed by a tool batch
	Unfinished int `json:"unfinished"` // admitted and not yet terminal
}

// Status is a cheap snapshot: no model call, no store read.
func (m *Machine) Status() Status {
	r, w, h := m.sched.Counts()
	m.admit.Lock()
	u := m.active
	m.admit.Unlock()
	return Status{Cores: m.cfg.Cores, Ready: r, Waiting: w, Held: h, Unfinished: u}
}
