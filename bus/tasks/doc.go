// Package tasks is a device that lets the tasks of one machine talk to each
// other. Two tools:
//
//   - send {task_id, text}: steers text into another task, as
//     Machine.SteerWith with From "task:<caller>" (design §10.1). The other
//     task's model gets it at its next model step framed by
//     Config.MessageFormat: by default, as a message from task:<caller> that
//     is not the user's and is information rather than instructions, with
//     its text quoted line by line so that it cannot pass for the frame. Its
//     steer_delivered event (or steer_dropped, if it ends first) carries
//     from. Sending to an unknown task or one the caller may not see, to one
//     that has finished, to the caller itself, an empty text, one over the
//     limit (WithMaxText, default DefaultMaxText bytes), or past the
//     caller's quota of messages (WithMaxSends, default DefaultMaxSends) is
//     an error result for the model, not a failure.
//   - list: the other unfinished tasks the caller may see, as a JSON array of
//     {id, class, state, wait, title}: wait only for a waiting task, title the
//     start of the task's request. ReadOnly; it reads Machine.Tasks, which
//     the machine keeps in memory, not the store.
//
// send is Idempotent. The call's idempotency key goes with the steer
// (SteerOptions.Key), and a call is first looked up by it (Machine.HasSteer):
// a call that recovery sends again after a restart is answered "sent" if its
// steer is committed, whatever changed since, and is not sent twice. A key
// survives a restart only if the store keeps mem.Interrupt.Key, as mem/sqlite
// does. A keyed steer is admitted while Stop drains the tool batches, so a
// send that runs then is committed, not lost.
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
// A task whose id has characters a sender may not have (ValidFrom) cannot
// send.
//
// Visibility. By default a task sees, and can send to, the unfinished tasks
// of its own class (SameClass), and list shows the start of each one's
// request. WithVisible widens it (All) or narrows it (an id prefix per
// tenant, ...); a task the caller may not see is reported as unknown,
// whether or not it exists. Which tasks get the tools at all is the page
// table's (Instruction.Allow / Deny, prefix "tasks_"), and a
// Config.ToolPolicy can deny or hold any tasks_send for an operator, as for
// any tool. A message is another task's words, and that task may have read
// untrusted content: keep the default MessageFormat, or one that says as
// much, for any task with tools that act.
//
// A message reaches a task that is still running. A task that finishes its
// answer while a message is on its way takes one more step to answer it, but
// one that has finished cannot be sent to, so a task that waits for an
// answer must still be working when it arrives (examples/messaging).
package tasks
