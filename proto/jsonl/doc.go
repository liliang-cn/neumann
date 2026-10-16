// Package jsonl is the machine's stdio frontend (design §12): a JSON Lines
// codec, and a Server that drives a started neumann.Machine with it.
// cmd/neumann runs a Server over stdin and stdout.
//
// The codec half (Decode, Marshal and the wire types) needs no machine; the
// Server imports package neumann, which never imports this package.
//
// # Streams
//
// Input is one instruction per line, a JSON object. Output is one JSON object
// per line, and its first field, "type", says what it is: "reply" answers one
// instruction, "event" is a committed event. Replies and events share one
// stream because that is what a host spawning the process gets for free (a
// Node child_process or a Python subprocess has stdout; an extra file
// descriptor is awkward in both and does not exist on every platform), and
// because one stream gives an order: a subscribe reply sits exactly between
// the old stream's last event and the new stream's first. Logs go to stderr.
//
// # Instructions and replies
//
// Every instruction may carry an "id", a string or a number, which its reply
// echoes. Instructions run one at a time in the order received, so replies
// also come in that order; the id is for matching all the same.
//
//	{"id":1,"kind":"submit","input":"What is 2+2?"}
//	{"type":"reply","id":1,"kind":"submit","ok":true,"task_id":"8c0f…"}
//
// An ok reply to submit, steer, cancel, resolve, approve, class, priority, suspend and
// resume means the instruction is committed: it survives a crash from that moment on. The
// kinds and their fields (anything else is refused by name):
//
//	submit       input (required), task_id, class, pin_class, priority, budget {max_steps, deadline (RFC 3339)}, allow, deny
//	             → task_id
//	steer        task_id, input (both required), from (the sender, Machine.SteerWith;
//	             the peer is the operator, so from is taken as given)
//	cancel       task_id (required)
//	resolve      task_id, call_id, outcome (all required); outcome is {"result":"…"},
//	             {"result":"…","is_error":true} or {"retry":true}
//	approve      task_id, call_id, decision (all required): answers a call held for approval
//	             (an approval_needed event names it); decision is {"verdict":"allow"},
//	             {"verdict":"deny","reason":"…"} or {"verdict":"edit","args":{…}}, args an object (Machine.Approve)
//	status       → status: the machine's snapshot (neumann.Status)
//	subscribe    from_seq (default 0) → from_seq; starts or restarts the event stream
//	unsubscribe  stops the event stream
//	class        task_id, class (both required): moves the task to that group (Machine.SetClass)
//	priority     task_id, priority (both required, priority >= 0: 0 is interactive, larger is
//	             less urgent): changes the task's priority, then a call of a less urgent task
//	             may be preempted for it (Machine.SetPriority)
//	suspend      task_id (required): takes the task off the machine until a resume; a ready task
//	             is suspended before the reply, a model call in flight is cut off, and a tool
//	             round or a round held for approval finishes first (Machine.Suspend); suspending
//	             a suspended task is ok and changes nothing
//	resume       task_id (required): puts a suspended task back, or cancels a suspend that has not
//	             landed yet (Machine.ResumeTask)
//	cores        class, cores (both required, cores >= 1): pins the group's window (Machine.SetCores)
//	autoscale    class, on (both required): the group's window adapts again, or freezes (Machine.AutoScale)
//	group_reset  class (required): closes the group's breaker (Machine.GroupReset)
//	queue        → queue: the queues by class (Machine.Queue)
//
// A failed reply has "ok":false and an error with a code and a message:
//
//	{"type":"reply","id":2,"kind":"steer","ok":false,"error":{"code":"finished","message":"mem: task already finished: t1"}}
//
// Codes: bad_request (malformed JSON, an object missing, unknown or ill-typed
// field; the message names it), unknown_kind, unsupported (a designed
// operator control the machine does not have yet; none at present),
// not_found, finished, not_reconciling, not_awaiting_approval
// (approve: the task is not waiting for approval, or the call is not held),
// not_suspended (resume: the task is not suspended and has no suspend pending),
// busy (with "count" and "limit"), task_busy (class, priority: the task is
// executing an instruction or waiting for its tools; try again), incompatible (class: the group cannot take the task; "check"
// names the failed check: tools, tool_count, context or caps), stopped,
// not_started, failed (anything else, an unknown class among them). A
// bad line costs one error reply; the server carries on. Blank lines are
// skipped; lines longer than Server.MaxLine (16 MiB) are refused.
//
// # Events
//
// Nothing is streamed until a subscribe. subscribe {"from_seq":N} streams
// every committed event with seq > N, read from the store, then live ones as
// they are committed; a second subscribe replaces the first. A client that
// reconnects (or restarts) with the last seq it handled gets exactly what it
// missed. An event line is the event itself with "type" in front:
//
//	{"type":"event","kind":"answer","task_id":"8c0f…","seq":12,"task_seq":5,"data":{"text":"4"}}
//
// A task's terminal event (done, failed, cancelled) is not always its last:
// the same commit may report undelivered steers as steer_dropped after it.
//
// A slow reader never blocks the machine: events are read from the store at
// the pace the output is written, so falling behind only means being behind
// in the log. Replies jump the queue: a reply waiting to be written goes out
// before the next event, so a backlog of events does not delay answers.
//
// # Shutdown
//
// End of input, or cancelling Serve's context (cmd/neumann does on SIGINT and
// SIGTERM), stops the machine within Server.StopTimeout, then writes the
// events the subscription still owes within Server.DrainTimeout, and Serve
// returns nil. An instruction being executed when the signal arrives finishes
// first, so its reply is true.
package jsonl
