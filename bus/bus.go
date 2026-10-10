// Package bus is the system bus: every tool the machine can call is a device
// attached here, addressed by a full name, behind a per-device queue.
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/task"
)

// Effect is what calling a tool does to the world; recovery depends on it.
// The zero value is SideEffect: an undeclared tool is assumed to have one.
type Effect int

const (
	SideEffect Effect = iota
	ReadOnly
	Idempotent
)

type ToolDef struct {
	Name        string
	Description string
	Schema      json.RawMessage
	Effect      Effect
	Timeout     time.Duration // 0 = DefaultTimeout
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
}

// New attaches devices. perDevice bounds concurrent calls per device; calls
// beyond it wait in that device's queue.
func New(ctx context.Context, perDevice int, devs ...Device) (*Bus, error) {
	if perDevice <= 0 {
		perDevice = 4
	}
	b := &Bus{routes: map[string]route{}, devs: devs}
	for _, d := range devs {
		defs, err := d.Tools(ctx)
		if err != nil {
			return nil, fmt.Errorf("bus: list tools of %s: %w", d.Name(), err)
		}
		slot := make(chan struct{}, perDevice)
		for _, def := range defs {
			name := FullName(d.Name(), def.Name)
			if _, dup := b.routes[name]; dup {
				return nil, fmt.Errorf("bus: duplicate tool name %q", name)
			}
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
// prefix lists; an empty Allow permits whatever Deny does not name.
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
			out = append(out, s)
		}
	}
	return out
}

// Effect is a tool's declared effect; unknown names are SideEffect.
func (b *Bus) Effect(name string) Effect {
	if r, ok := b.routes[name]; ok {
		return r.def.Effect
	}
	return SideEffect
}

// Call runs one tool call, waiting in the device's queue for a free slot. It
// never returns a Go error: every failure becomes an error result, because
// each call the model made must get exactly one result.
func (b *Bus) Call(ctx context.Context, name string, args json.RawMessage, key string, allow, deny []string) task.ToolResult {
	if !Permitted(name, allow, deny) {
		return task.ToolResult{Content: "tool " + name + " is not permitted for this task", IsError: true}
	}
	r, ok := b.routes[name]
	if !ok {
		return task.ToolResult{Content: "unknown tool " + name, IsError: true}
	}
	select {
	case r.slot <- struct{}{}:
	case <-ctx.Done():
		return task.ToolResult{Content: "cancelled before the tool ran: " + ctx.Err().Error(), IsError: true}
	}
	defer func() { <-r.slot }()

	timeout := r.def.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := r.dev.Call(cctx, DeviceCall{Tool: r.tool, Args: args, Key: key})
	if err != nil {
		return task.ToolResult{Content: err.Error(), IsError: true}
	}
	return res
}

func (b *Bus) Close() error {
	var errs []error
	for _, d := range b.devs {
		errs = append(errs, d.Close())
	}
	return errors.Join(errs...)
}
