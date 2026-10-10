// Package builtin is a device made of Go functions.
package builtin

import (
	"context"
	"fmt"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

// Func implements one tool. A returned error becomes an error result for the
// model, not a failure of the device.
type Func func(ctx context.Context, call bus.DeviceCall) (string, error)

type Device struct {
	name string
	defs []bus.ToolDef
	fns  map[string]Func
}

func New(name string) *Device {
	return &Device{name: name, fns: map[string]Func{}}
}

// Add registers a tool and returns the device, for chaining. Call Add only
// before the device is attached to a bus.
func (d *Device) Add(def bus.ToolDef, fn Func) *Device {
	d.defs = append(d.defs, def)
	d.fns[def.Name] = fn
	return d
}

func (d *Device) Name() string { return d.name }

func (d *Device) Tools(context.Context) ([]bus.ToolDef, error) {
	return append([]bus.ToolDef(nil), d.defs...), nil
}

func (d *Device) Call(ctx context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	fn, ok := d.fns[c.Tool]
	if !ok {
		return task.ToolResult{}, fmt.Errorf("builtin %s: no tool %q", d.name, c.Tool)
	}
	out, err := fn(ctx, c)
	if err != nil {
		return task.ToolResult{Content: err.Error(), IsError: true}, nil
	}
	return task.ToolResult{Content: out}, nil
}

func (d *Device) Close() error { return nil }
