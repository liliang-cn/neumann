// Package proto is the machine's wire format: instructions in, events out.
//
// An [Instruction] is what a host asks for; Machine.Submit takes one of kind
// [KindSubmit]. An [Event] is something that happened, published only after
// it was committed, with a global Seq and a per-task TaskSeq. A host that
// remembers the last Seq it handled can always resume from there with
// Machine.Events, and sees exactly what it missed.
//
// Task events (TaskID set) include accepted, step, tool_call, tool_result,
// usage, answer, and one terminal event: done, failed or cancelled (see
// [Event.Terminal]). Recovery adds recovered, reconcile_needed and resolved.
// Machine events (TaskID empty) report vCores (core_up, core_down), execution
// groups (group_down, group_up) and limiter waits (throttled). The constants
// below list every kind; design §12 gives their data.
//
// A [Delta] is different: a live, uncommitted piece of a model's output
// (Machine.Deltas), a preview that may be lost; the committed answer event is
// the record.
//
// The stdio frontend's JSON Lines format is in package
// [github.com/liliang-cn/neumann/proto/jsonl].
package proto
