// Package builtin is a device made of Go functions: the quickest way to give a
// machine tools.
//
// Create a device, add one [Func] per tool, and pass the device in
// neumann.Config.Devices. The model sees each tool as <device>_<tool>:
//
//	calc := builtin.New("calc").Add(bus.ToolDef{
//		Name:        "add",
//		Description: "Add two numbers.",
//		Schema:      json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}}}`),
//		Effect:      bus.ReadOnly, // safe to repeat after a crash
//	}, func(ctx context.Context, call bus.DeviceCall) (string, error) {
//		var in struct{ A, B float64 }
//		if err := json.Unmarshal(call.Args, &in); err != nil {
//			return "", err // becomes an error result for the model
//		}
//		return fmt.Sprint(in.A + in.B), nil
//	})
//
// A Func's error is reported to the model as an error result; it never fails
// the task. Leave Effect at its zero value, bus.SideEffect, for anything that
// changes the world, and use call.Key to make it idempotent where you can.
// examples/hello is a complete program.
package builtin
