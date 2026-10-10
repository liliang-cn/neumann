// Package bus is the system bus: every tool the machine can call is a device
// attached here, addressed by a full name, behind a per-device queue.
package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann/cache"
	"github.com/liliang-cn/neumann/task"
)

// Effect is the declared effect of a tool (see task.Effect). The zero value
// is SideEffect: an undeclared tool is assumed to have one.
type Effect = task.Effect

const (
	SideEffect = task.SideEffect
	ReadOnly   = task.ReadOnly
	Idempotent = task.Idempotent
)

type ToolDef struct {
	Name        string
	Description string
	Schema      json.RawMessage
	Effect      Effect
	// Cacheable: a successful result may be served again, from L2, to a
	// call with the same arguments in the same task. Only ReadOnly tools are
	// cached; on any other effect it is ignored.
	Cacheable bool
	CacheTTL  time.Duration // how long a cached result is served; 0 = until the task's cache is cleared
	Timeout   time.Duration // 0 = DefaultTimeout
	// ReportUnknown: when a restart leaves this SideEffect tool's outcome
	// unknown, tell the model so instead of waiting for an operator to
	// resolve it. Leave false for anything a person must check.
	ReportUnknown bool
}

// DeviceCall is one call as a device receives it. Key is the idempotency key
// (task/call/attempt); a device may use it to drop a repeat.
type DeviceCall struct {
	Tool string
	Args json.RawMessage
	Key  string
}

type Device interface {
	Name() string
	Tools(ctx context.Context) ([]ToolDef, error)
	Call(ctx context.Context, call DeviceCall) (task.ToolResult, error)
	Close() error
}

const DefaultTimeout = 120 * time.Second

type route struct {
	dev  Device
	tool string
	def  ToolDef
	slot chan struct{} // shared by all tools of one device
}

type Bus struct {
	routes map[string]route
	specs  []task.ToolSpec
	devs   []Device
	l2     atomic.Pointer[cache.L2]
}

// New attaches devices. perDevice bounds concurrent calls per device; calls
// beyond it wait in that device's queue. The bus starts with an L2 cache with
// the default caps (see SetCache); only tools that declare Cacheable use it.
func New(ctx context.Context, perDevice int, devs ...Device) (*Bus, error) {
	if perDevice <= 0 {
		perDevice = 4
	}
	b := &Bus{routes: map[string]route{}, devs: devs}
	b.l2.Store(cache.New(cache.Config{}))
	fail := func(err error) (*Bus, error) {
		for _, d := range devs {
			_ = d.Close()
		}
		return nil, err
	}
	for _, d := range devs {
		if d.Name() == "" {
			return fail(errors.New("bus: device with empty name"))
		}
		defs, err := d.Tools(ctx)
		if err != nil {
			return fail(fmt.Errorf("bus: list tools of %s: %w", d.Name(), err))
		}
		slot := make(chan struct{}, perDevice)
		for _, def := range defs {
			if def.Name == "" {
				return fail(fmt.Errorf("bus: device %q has a tool with an empty name", d.Name()))
			}
			name := FullName(d.Name(), def.Name)
			if !def.Effect.Valid() {
				return fail(fmt.Errorf("bus: tool %q of device %q declares an unknown effect %d", def.Name, d.Name(), int(def.Effect)))
			}
			if def.CacheTTL < 0 {
				return fail(fmt.Errorf("bus: tool %q of device %q declares a negative CacheTTL", def.Name, d.Name()))
			}
			if _, dup := b.routes[name]; dup {
				return fail(fmt.Errorf("bus: duplicate tool name %q (device %q, tool %q)", name, d.Name(), def.Name))
			}
			def.Schema = bytes.Clone(def.Schema)
			b.routes[name] = route{dev: d, tool: def.Name, def: def, slot: slot}
			b.specs = append(b.specs, task.ToolSpec{Name: name, Description: def.Description, Schema: def.Schema})
		}
	}
	sort.Slice(b.specs, func(i, j int) bool { return b.specs[i].Name < b.specs[j].Name })
	return b, nil
}

// FullName is the name the model sees: <device>_<tool>, every character
// outside [A-Za-z0-9_-] replaced by '_'.
func FullName(device, tool string) string {
	return sanitize(device) + "_" + sanitize(tool)
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '_' || r == '-', 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
			return r
		}
		return '_'
	}, s)
}

// Permitted applies a page table to a full tool name. Allow and Deny are
// prefix lists; an empty Allow permits whatever Deny does not name. Prefixes
// match full names, so "dev_" also matches a device named "dev_extra".
func Permitted(name string, allow, deny []string) bool {
	for _, p := range deny {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	if len(allow) == 0 {
		return true
	}
	for _, p := range allow {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// Specs is the tool list a task with this page table sees, sorted by name so
// that it is byte-identical on every request (it sits in the cached prefix).
func (b *Bus) Specs(allow, deny []string) []task.ToolSpec {
	var out []task.ToolSpec
	for _, s := range b.specs {
		if Permitted(s.Name, allow, deny) {
			s.Schema = bytes.Clone(s.Schema)
			out = append(out, s)
		}
	}
	return out
}

// Effect is a tool's declared effect; unknown names are SideEffect.
func (b *Bus) Effect(name string) Effect {
	_, e, _ := b.Recovery(name)
	return e
}

// Recovery is what a restart needs to know about a tool, recorded when a
// call is planned: the device that runs it, its effect, and whether an
// unknown outcome may be reported to the model. Unknown names have no
// device, are SideEffect and are not reported.
func (b *Bus) Recovery(name string) (device string, effect Effect, reportUnknown bool) {
	if r, ok := b.routes[name]; ok {
		return r.dev.Name(), r.def.Effect, r.def.ReportUnknown
	}
	return "", SideEffect, false
}

// Outcome is how a call ended, for a caller that must know whether the
// device ran: a cancelled task records a call that never reached its device
// as cancelled, and one that did as unknown.
type Outcome int

const (
	// Returned: the result is the call's own: what the device returned, the
	// bus's refusal (not permitted, unknown tool), or its timeout.
	Returned Outcome = iota
	// NotRun: ctx ended before the device was invoked; it did not run.
	NotRun
	// Abandoned: ctx ended while the device was running, and the bus stopped
	// waiting. Whether the call took effect is unknown; whatever the device
	// returns later goes to the late callback.
	Abandoned
)

func (o Outcome) String() string {
	switch o {
	case Returned:
		return "returned"
	case NotRun:
		return "not_run"
	case Abandoned:
		return "abandoned"
	}
	return "unknown"
}

// SetCache replaces the L2 cache; nil turns caching off. Calls already past
// their lookup finish against the cache they started with.
func (b *Bus) SetCache(c *cache.L2) { b.l2.Store(c) }

// Cache is the L2 cache in use, nil when caching is off.
func (b *Bus) Cache() *cache.L2 { return b.l2.Load() }

// CacheStats reads L2's counters (hits, misses, evictions, ...); zero when
// caching is off.
func (b *Bus) CacheStats() cache.Stats {
	if c := b.l2.Load(); c != nil {
		return c.Stats()
	}
	return cache.Stats{}
}

// ForgetTask drops taskID's L2 entries. The machine calls it when a task
// ends; until then, a finished task's entries are only bounded by the caps
// and their TTLs.
func (b *Bus) ForgetTask(taskID string) {
	if c := b.l2.Load(); c != nil {
		c.Forget(taskID)
	}
}

// keyTask is the task scope of an idempotency key task/call/attempt: all of
// it before the last two '/'. A key without one names no task, and its call
// neither reads, fills nor clears any cache.
func keyTask(key string) (string, bool) {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		return "", false
	}
	j := strings.LastIndexByte(key[:i], '/')
	if j <= 0 {
		return "", false
	}
	return key[:j], true
}

// Call runs one tool call, waiting in the device's queue for a free slot. It
// never returns a Go error: every failure becomes an error result, because
// each call the model made must get exactly one result.
//
// The bus enforces cancellation and the timeout itself: the device runs in its
// own goroutine and Call returns when ctx ends or the timeout fires, whether
// or not the device cooperates. A device that ignores its context keeps its
// slot until its call actually returns, so the per-device bound counts real
// work. A device panic becomes an error result.
func (b *Bus) Call(ctx context.Context, name string, args json.RawMessage, key string, allow, deny []string) task.ToolResult {
	res, _ := b.Run(ctx, name, args, key, allow, deny, nil)
	return res
}

// Run is Call that also says how the call ended. When ctx ends while the
// device runs (Abandoned) and the device later returns a result of its own,
// not merely an echo of the cancellation, late receives it on the device's
// goroutine after its slot is released. A timeout is a result (Returned), and
// what the device returns after it is dropped.
//
// L2: a ReadOnly && Cacheable call whose key names a task is first looked up
// in that task's cache; a hit is Returned with the result an earlier call
// returned, and the device does not run (RunCached says it was a hit). A miss
// runs the device, and its result is stored if it is not an error and no
// write in the task completed meanwhile. Identical misses in flight together
// each run. When a non-ReadOnly call in a task finishes on its device, or the
// bus stops waiting for it, the task's cache is cleared. Late results and
// timeouts are never stored; a refused call never reaches the cache.
func (b *Bus) Run(ctx context.Context, name string, args json.RawMessage, key string, allow, deny []string, late func(task.ToolResult)) (task.ToolResult, Outcome) {
	res, how, _ := b.RunCached(ctx, name, args, key, allow, deny, late)
	return res, how
}

// RunCached is Run that also reports whether the result was served from L2.
// A hit is an ordinary Returned result: the call is recorded like any other,
// with exactly one result.
func (b *Bus) RunCached(ctx context.Context, name string, args json.RawMessage, key string, allow, deny []string, late func(task.ToolResult)) (task.ToolResult, Outcome, bool) {
	if !Permitted(name, allow, deny) {
		return task.ToolResult{Content: "tool " + name + " is not permitted for this task", IsError: true}, Returned, false
	}
	r, ok := b.routes[name]
	if !ok {
		return task.ToolResult{Content: "unknown tool " + name, IsError: true}, Returned, false
	}
	cancelled := func() (task.ToolResult, Outcome, bool) {
		return task.ToolResult{Content: "cancelled before the tool ran: " + ctx.Err().Error(), IsError: true}, NotRun, false
	}
	if ctx.Err() != nil {
		return cancelled()
	}
	l2 := b.l2.Load()
	taskID, scoped := keyTask(key)
	scoped = scoped && l2 != nil
	var miss *cache.Miss // nil unless this call may fill the cache
	if scoped && r.def.Effect == ReadOnly && r.def.Cacheable {
		res, hit, m := l2.Lookup(taskID, name, args)
		if hit {
			return res, Returned, true
		}
		miss = m
		defer miss.Drop() // no-op once filled
	}
	clears := scoped && r.def.Effect != ReadOnly // a write clears its task's cache once it ran
	invalidate := func() {
		if clears {
			l2.Invalidate(taskID)
		}
	}
	select {
	case r.slot <- struct{}{}:
	case <-ctx.Done():
		return cancelled()
	}
	if ctx.Err() != nil {
		<-r.slot
		return cancelled()
	}

	timeout := r.def.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type outcome struct {
		res task.ToolResult
		err error
	}
	finish := func(o outcome) task.ToolResult {
		if o.err != nil {
			return task.ToolResult{Content: o.err.Error(), IsError: true}
		}
		return o.res
	}
	// echo: the device only repeated the cancellation or timeout back.
	echo := func(o outcome) bool {
		for _, e := range []error{context.Canceled, context.DeadlineExceeded} {
			if (o.err != nil && errors.Is(o.err, e)) || (o.res.IsError && o.res.Content == e.Error()) {
				return true
			}
		}
		return false
	}
	var (
		mu       sync.Mutex // orders the device's hand-over against the bus giving up
		gaveUp   bool
		toCancel bool // it gave up because ctx ended, not on the timeout
	)
	ch := make(chan outcome, 1)
	go func() {
		o := func() (o outcome) {
			defer func() {
				if p := recover(); p != nil {
					o = outcome{res: task.ToolResult{Content: fmt.Sprintf("tool %s panicked: %v", name, p), IsError: true}}
				}
			}()
			res, err := r.dev.Call(cctx, DeviceCall{Tool: r.tool, Args: args, Key: key})
			return outcome{res, err}
		}()
		invalidate() // before the result is handed over: the write is done
		<-r.slot
		mu.Lock()
		if !gaveUp {
			ch <- o
			mu.Unlock()
			return
		}
		mu.Unlock()
		if late != nil && toCancel && !echo(o) {
			late(finish(o))
		}
	}()
	// stopped reports a call the bus stopped waiting for. byCancel is read
	// once by the caller, so the outcome and the late hand-off agree even
	// when the timeout and a cancel land together.
	stopped := func(byCancel bool) (task.ToolResult, Outcome, bool) {
		invalidate() // a write given up on may still take effect
		if !byCancel {
			return task.ToolResult{Content: fmt.Sprintf("tool %s timed out after %v", name, timeout), IsError: true}, Returned, false
		}
		return task.ToolResult{Content: "cancelled while the tool ran: " + context.Canceled.Error(), IsError: true}, Abandoned, false
	}
	var o outcome
	select {
	case o = <-ch:
	case <-cctx.Done():
		// A device that finished at the same moment as the timeout wins: its
		// side effect happened, so reporting a timeout would invite a retry.
		mu.Lock()
		select {
		case o = <-ch:
			mu.Unlock()
		default:
			gaveUp, toCancel = true, ctx.Err() != nil
			byCancel := toCancel
			mu.Unlock()
			return stopped(byCancel)
		}
	}
	// A device that merely echoes the context error is a timeout, not a result.
	if cctx.Err() != nil && echo(o) {
		return stopped(ctx.Err() != nil)
	}
	res := finish(o)
	miss.Fill(res, r.def.CacheTTL) // stores only a non-error result, and only if no write completed since the lookup
	return res, Returned, false
}

func (b *Bus) Close() error {
	var errs []error
	for _, d := range b.devs {
		errs = append(errs, d.Close())
	}
	return errors.Join(errs...)
}
