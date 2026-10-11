// Package tasks is a device that lets the tasks of one machine talk to each
// other. Two tools:
//
//   - send {task_id, text}: steers text into another task, as
//     Machine.SteerWith with From "task:<caller>" (design §10.1). The other
//     task's model sees it at its next model step as a user message headed
//     "[message from task:<caller>]", and its steer_delivered event (or
//     steer_dropped, if it ends first) carries from. Sending to an unknown
//     task, to one that has finished, to the caller itself, an empty text or
//     one over the limit (WithMaxText, default DefaultMaxText bytes) is an
//     error result for the model, not a failure. send is Idempotent: the
//     call's idempotency key goes with the steer (SteerOptions.Key), so a
//     call that recovery sends again after a restart does not send twice; an
//     operator's retry (Resolve) is a new attempt, under a new key, and does.
//   - list: the other unfinished tasks the caller may see, as a JSON array of
//     {id, class, state, wait, title}: wait only for a waiting task, title the
//     start of the task's request (Title). ReadOnly.
//
// Wiring. The device needs the machine, and the machine is built from its
// devices, so the device is bound after New and before Start:
//
//	dev := tasks.New("tasks")
//	m, err := neumann.New(neumann.Config{Model: adapter, Devices: []bus.Device{dev}})
//	dev.Bind(m)
//	err = m.Start(ctx)
//
// The caller's identity is the task part of the call's idempotency key
// (bus.DeviceCall.Task), which the machine sets; the model cannot choose it.
//
// Visibility. By default every task sees, and can send to, every unfinished
// task of the machine, and list shows the start of each one's request: fine
// for tasks that work for one user, not for tasks of different users on one
// machine. WithVisible narrows it per pair of tasks (same class, an id
// prefix per tenant, ...); a task the caller may not see is reported as
// unknown, whether or not it exists. Which tasks get the tools at all is the
// page table's (Instruction.Allow / Deny, prefix "tasks_"), and a
// Config.ToolPolicy can deny or hold any tasks_send for an operator, as for
// any tool.
//
// A message reaches a task that is still running. A task that finishes its
// answer while a message is on its way takes one more step to answer it, but
// one that has finished cannot be sent to, so a task that waits for an
// answer must still be working when it arrives (examples/messaging).
package tasks
