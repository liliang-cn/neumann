// Command hello is the smallest complete neumann program, and it runs offline:
// no API key, no network. A scripted model (package sim) plays the part of a
// real one.
//
//	go run ./examples/hello
//
// What happens:
//
//  1. Two devices are attached to the machine's bus: "calc", with two
//     read-only tools, and "notes", with one tool that has a side effect.
//  2. One task is submitted: "add 19 and 23, multiply 6 by 7, keep a note".
//  3. The scripted model asks for calc_add and calc_multiply in one turn (the
//     two calls run in parallel), reads their results, asks for notes_save,
//     then answers.
//  4. Every committed event is printed as it streams, then the task's tool
//     call log (the write-ahead log that makes crash recovery possible).
//
// To use a real model instead, replace the sim.Model with an adapter from
// model/openai or model/anthropic; nothing else changes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func main() {
	res, err := run(context.Background(), os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nfinal answer: %s\n", res.Answer)
}

// result is what one run produced; the test checks it.
type result struct {
	Answer string   // the task's committed answer
	Notes  []string // what notes_save actually stored, once per real call
}

// run builds a machine, runs one task to completion and prints what it sees
// to out.
func run(ctx context.Context, out io.Writer) (result, error) {
	var res result

	// --- Step 1: devices. -------------------------------------------------
	//
	// A device is a named group of tools on the bus. The model sees each tool
	// as <device>_<tool>: calc_add, calc_multiply, notes_save.
	//
	// Every tool declares its Effect, which decides what happens if the
	// process dies while the call is in flight:
	//   - ReadOnly / Idempotent: safe to send again after a crash, with the
	//     same idempotency key.
	//   - SideEffect (the zero value, so an undeclared tool gets it): sent at
	//     most once automatically; after a crash with an unknown outcome the
	//     call is held for an operator (Machine.Resolve) instead of repeated.
	numbers := json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
	calc := builtin.New("calc").
		Add(bus.ToolDef{Name: "add", Description: "Add two numbers.", Schema: numbers, Effect: bus.ReadOnly},
			arith(func(a, b float64) float64 { return a + b })).
		Add(bus.ToolDef{Name: "multiply", Description: "Multiply two numbers.", Schema: numbers, Effect: bus.ReadOnly},
			arith(func(a, b float64) float64 { return a * b }))

	var mu sync.Mutex
	notes := builtin.New("notes").Add(bus.ToolDef{
		Name:        "save",
		Description: "Save a note. Each call stores one more note.",
		Schema:      json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
		Effect:      bus.SideEffect,
	}, func(_ context.Context, call bus.DeviceCall) (string, error) {
		var in struct{ Text string }
		if err := json.Unmarshal(call.Args, &in); err != nil {
			return "", fmt.Errorf("bad arguments: %v", err) // an error result for the model, not a crash
		}
		// call.Key is the idempotency key (task/call/attempt). A real device
		// that talks to the outside world would send it along, so the far
		// side can drop a repeat.
		mu.Lock()
		res.Notes = append(res.Notes, in.Text)
		mu.Unlock()
		return "saved", nil
	})

	// --- Step 2: the machine. ---------------------------------------------
	//
	// Config.Model with no Groups is the one-group shorthand: one execution
	// group on one model with up to Config.Cores vCores (default 4). With no
	// Store the state lives in memory; pass mem/sqlite's Store to survive a
	// restart.
	m, err := neumann.New(neumann.Config{
		Model:   &sim.Model{Name: "scripted", Fn: script},
		Devices: []bus.Device{calc, notes},
		System:  "You are a careful assistant. Use the tools for arithmetic.",
	})
	if err != nil {
		return res, err
	}
	// Start recovers whatever an earlier run left unfinished in the store
	// (nothing, here), then starts vCores as work arrives.
	if err := m.Start(ctx); err != nil {
		return res, err
	}
	defer func() {
		// Stop waits for in-flight instructions and tool calls to commit.
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Stop(sctx); err != nil {
			log.Printf("stop: %v", err)
		}
	}()

	// --- Step 3: submit a task. -------------------------------------------
	//
	// Submit returns only after the task is committed: the id names a task
	// that exists, and would be recovered after a crash. TaskID is optional
	// (a UUID is assigned when it is empty); MaxSteps bounds the model calls
	// that got an answer.
	id, err := m.Submit(ctx, proto.Instruction{
		Kind:   proto.KindSubmit,
		TaskID: "hello",
		Input:  "Add 19 and 23, multiply 6 by 7, and keep a note of both results.",
		Budget: &task.Budget{MaxSteps: 10},
	})
	if err != nil {
		return res, err
	}
	fmt.Fprintf(out, "submitted task %s\n\n", id)

	// --- Step 4: watch the events. ----------------------------------------
	//
	// Events streams every committed event with Seq > fromSeq, in order; a
	// subscriber that reconnects with the last Seq it saw misses nothing.
	// Events with an empty TaskID are machine events (vCores starting, a
	// provider's breaker opening, ...).
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for ev := range m.Events(wctx, 0) {
		who := "task"
		if ev.TaskID == "" {
			who = "machine"
		}
		fmt.Fprintf(out, "#%-3d %-7s %-12s %s\n", ev.Seq, who, ev.Kind, ev.Data)
		if ev.TaskID == id && ev.Terminal() { // done, failed or cancelled
			break
		}
	}
	if err := wctx.Err(); err != nil {
		return res, fmt.Errorf("task %s did not finish: %w", id, err)
	}

	// --- Step 5: read the committed state. --------------------------------
	t, err := m.Load(ctx, id)
	if err != nil {
		return res, err
	}
	if t.State != task.Done {
		return res, fmt.Errorf("task %s ended %s (%s)", id, t.State, t.StopReason)
	}
	res.Answer = t.LastAnswer()
	fmt.Fprintf(out, "\ntask %s: %s, %d answered model calls\n", t.State, t.StopReason, t.Used.Steps)

	// The call log: one record per tool call, written before the call was
	// dispatched and updated when its result came back.
	calls, err := m.Calls(ctx, id)
	if err != nil {
		return res, err
	}
	fmt.Fprintln(out, "\ncall log:")
	for _, c := range calls {
		var content string
		if c.Result != nil { // nil until a result is recorded
			content = c.Result.Content
		}
		fmt.Fprintf(out, "  %-4s %-14s %-11s %-5s key=%s result=%q\n", c.CallID, c.Tool, c.Effect, c.Status, c.Key, content)
	}
	return res, nil
}

// arith adapts a two-number function to a builtin tool.
func arith(f func(a, b float64) float64) builtin.Func {
	return func(_ context.Context, call bus.DeviceCall) (string, error) {
		var in struct{ A, B float64 }
		if err := json.Unmarshal(call.Args, &in); err != nil {
			return "", fmt.Errorf("bad arguments: %v", err)
		}
		return strconv.FormatFloat(f(in.A, in.B), 'f', -1, 64), nil
	}
}

// script plays the model. It is called once per model step with the full
// request (system prompt, tools, every message so far) and decides from it,
// the way a real model would: by how many tool rounds have completed.
func script(req model.Request) (model.Response, error) {
	switch sim.ToolRounds(req) {
	case 0:
		// Two calls in one turn: the machine dispatches both at once and the
		// task waits for both, without holding a vCore.
		return toolTurn(
			toolCall("calc_add", map[string]float64{"a": 19, "b": 23}),
			toolCall("calc_multiply", map[string]float64{"a": 6, "b": 7}),
		), nil
	case 1:
		// The last message holds the results, in the order of the calls.
		r := lastResults(req)
		if len(r) != 2 {
			return sim.Reply("I did not get both results back."), nil
		}
		return toolTurn(toolCall("notes_save", map[string]string{
			"text": fmt.Sprintf("19 + 23 = %s; 6 × 7 = %s", r[0], r[1]),
		})), nil
	default:
		// The note was saved (the last message is its result): answer.
		return sim.Reply("19 + 23 = 42 and 6 × 7 = 42; both results are in your notes."), nil
	}
}

// toolTurn is an assistant turn that calls tools.
func toolTurn(calls ...task.Part) model.Response {
	return model.Response{
		Message:      task.Message{Role: task.RoleAssistant, Parts: calls},
		FinishReason: "tool_calls",
		Usage:        task.Usage{PromptTokens: 40, CompletionTokens: 12},
	}
}

// toolCall is one tool_call part. ProviderCallID stands in for the id a real
// provider would return; the machine assigns its own CallID ("c-1", ...).
func toolCall(name string, args any) task.Part {
	b, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return task.Part{Kind: task.PartToolCall, ProviderCallID: "call_" + name, Name: name, Args: b}
}

// lastResults is the text of each tool result in the request's last message.
func lastResults(req model.Request) []string {
	if len(req.Messages) == 0 {
		return nil
	}
	var out []string
	for _, p := range req.Messages[len(req.Messages)-1].Parts {
		if p.Kind == task.PartToolResult {
			out = append(out, p.Text)
		}
	}
	return out
}
