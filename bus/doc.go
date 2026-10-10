// Package bus is the system bus: every tool the machine can call is a device
// attached here, addressed by a full name, behind a per-device queue.
//
// # Getting started
//
// A host rarely builds a Bus itself. It hands its devices to the machine,
// which attaches them when neumann.New runs:
//
//	m, err := neumann.New(neumann.Config{
//		Model:   adapter,
//		Devices: []bus.Device{calc, notes},
//	})
//
// Ready-made devices: [github.com/liliang-cn/neumann/bus/builtin] (Go
// functions), [github.com/liliang-cn/neumann/bus/mcp] (a Model Context
// Protocol server) and [github.com/liliang-cn/neumann/bus/skills] (skill
// directories). Anything else implements [Device]: a name, a list of
// [ToolDef]s, and Call.
//
// # What the bus guarantees
//
//   - Names. The model sees <device>_<tool> ([FullName]), with every character
//     outside [A-Za-z0-9_-] replaced by '_'. An empty or duplicate name fails
//     New, which then closes every device it was given.
//   - Effects. Each tool declares an [Effect]. The zero value is [SideEffect],
//     so a tool that says nothing is never sent twice by recovery. Declare
//     [ReadOnly] or [Idempotent] only when a repeat is harmless; recovery after
//     a crash relies on it (design §4.2).
//   - Exactly one result. [Bus.Call] never returns a Go error: a refusal, a
//     timeout ([ToolDef.Timeout], default [DefaultTimeout]), a cancellation or
//     a device panic becomes an error result for the model. Timeouts and
//     cancellation are enforced by the bus, not left to the device.
//   - Arbitration. Each device runs at most perDevice calls at once (the
//     machine's Config.MaxPerDevice, default 4); the rest wait in FIFO order.
//   - Page table. A task's Allow and Deny prefix lists ([Permitted]) filter the
//     tools it sees and are checked again at dispatch.
//   - L2. A successful result of a ReadOnly tool that sets Cacheable is served
//     again, within the same task, to a call with the same arguments (package
//     [github.com/liliang-cn/neumann/cache]).
//
// Every call carries an idempotency key, task/call/attempt
// ([DeviceCall].Key). A device whose far side can deduplicate should pass it
// on: that is what turns "dispatched at most once" into "happened at most
// once" for a SideEffect tool.
package bus
