// Command messaging shows two tasks of one machine talking to each other
// through the tasks device (package bus/tasks), offline: a scripted model
// (package sim) plays both.
//
//	go run ./examples/messaging
//
// What happens:
//
//  1. Task "expert" starts a long job (desk_work).
//  2. Task "planner" looks up the other tasks (tasks_list), sends expert a
//     question (tasks_send), and starts a long job of its own.
//  3. When its job is done, expert's next model step finds the question in
//     its context as "[message from task:planner] ...". It answers with
//     tasks_send to planner, and finishes.
//  4. planner's next model step finds "[message from task:expert] ...", and
//     planner answers its user with it.
//
// Each delivery is a steer_delivered event that names its sender (from);
// the events are printed as they stream.
//
// desk_work stands in for real work. Here it returns when the example sees
// that the message for its task is on its way, so the run is the same every
// time; with a real model it would take as long as the work takes. A message
// can only reach a task that has not finished: a task that waits for an
// answer must still be working when it arrives.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/bus/tasks"
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
	fmt.Printf("\nplanner's answer: %s\n", res.Answers["planner"])
}

// result is what one run produced; the test checks it.
type result struct {
	Answers   map[string]string // each task's committed answer
	Delivered []string          // "task from" of each steer_delivered event
	Saw       map[string]string // the message each task's model received from the other
}

func run(ctx context.Context, out io.Writer) (result, error) {
	res := result{Answers: map[string]string{}, Saw: map[string]string{}}

	// --- The devices. -------------------------------------------------------
	//
	// tasks gives the model tasks_send and tasks_list. It needs the machine,
	// which is built from its devices, so it is bound after New.
	dev := tasks.New("tasks")
	// desk_work holds a task until the example releases it.
	release := map[string]chan struct{}{"planner": make(chan struct{}), "expert": make(chan struct{})}
	released := map[string]bool{}
	desk := builtin.New("desk").Add(bus.ToolDef{
		Name:        "work",
		Description: "Work on your own job for a while.",
		Schema:      json.RawMessage(`{"type":"object","properties":{}}`),
		Effect:      bus.ReadOnly,
	}, func(ctx context.Context, call bus.DeviceCall) (string, error) {
		// call.Task is the id of the task that made the call.
		select {
		case <-release[call.Task]:
			return "the job is done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	// --- The machine. -------------------------------------------------------
	var mu sync.Mutex // guards res.Saw, which the model writes
	m, err := neumann.New(neumann.Config{
		Model:   &sim.Model{Name: "scripted", Fn: script(&mu, res.Saw)},
		Devices: []bus.Device{dev, desk},
		System:  "You are one of several agents. Use the tasks tools to talk to the others.",
	})
	if err != nil {
		return res, err
	}
	dev.Bind(m) // before Start: recovery may send a tasks call again as it starts
	if err := m.Start(ctx); err != nil {
		return res, err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Stop(sctx); err != nil {
			log.Printf("stop: %v", err)
		}
	}()

	// --- The tasks. ---------------------------------------------------------
	for _, in := range []proto.Instruction{
		{TaskID: "expert", Input: "You are the expert on Paris landmarks. Work on your catalogue, and answer questions other agents send you."},
		{TaskID: "planner", Input: "Plan a visit to the Eiffel Tower; ask the expert when it was completed."},
	} {
		in.Kind = proto.KindSubmit
		if _, err := m.Submit(ctx, in); err != nil {
			return res, err
		}
	}

	// --- The events. --------------------------------------------------------
	//
	// A message is sent once the tool_result of its tasks_send is committed:
	// then the receiver may finish its job and read it.
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	sendCall := map[string]string{} // task -> call id of its tasks_send
	finished := 0
	for ev := range m.Events(wctx, 0) {
		if ev.TaskID == "" {
			continue // machine events: vCores starting
		}
		fmt.Fprintf(out, "#%-3d %-8s %-16s %s\n", ev.Seq, ev.TaskID, ev.Kind, ev.Data)
		var d struct {
			CallID string `json:"call_id"`
			Tool   string `json:"tool"`
			From   string `json:"from"`
		}
		json.Unmarshal(ev.Data, &d)
		switch ev.Kind {
		case proto.EvToolCall:
			if d.Tool == "tasks_send" {
				sendCall[ev.TaskID] = d.CallID
			}
		case proto.EvToolResult:
			if d.CallID != "" && d.CallID == sendCall[ev.TaskID] {
				to := map[string]string{"planner": "expert", "expert": "planner"}[ev.TaskID]
				if !released[to] {
					released[to] = true
					close(release[to])
				}
			}
		case proto.EvSteerDelivered:
			res.Delivered = append(res.Delivered, ev.TaskID+" "+d.From)
		}
		if ev.Terminal() {
			if finished++; finished == 2 {
				break
			}
		}
	}
	if err := wctx.Err(); err != nil {
		return res, fmt.Errorf("the tasks did not finish: %w", err)
	}
	for _, id := range []string{"expert", "planner"} {
		t, err := m.Load(ctx, id)
		if err != nil {
			return res, err
		}
		if t.State != task.Done {
			return res, fmt.Errorf("task %s ended %s (%s)", id, t.State, t.StopReason)
		}
		res.Answers[id] = t.LastAnswer()
	}
	return res, nil
}

// script plays both models, telling them apart by their request. The
// message each model got from the other is recorded in saw, as it saw it.
func script(mu *sync.Mutex, saw map[string]string) func(model.Request) (model.Response, error) {
	return func(req model.Request) (model.Response, error) {
		who := "planner"
		if strings.HasPrefix(sim.FirstUser(req), "You are the expert") {
			who = "expert"
		}
		users := sim.UserTexts(req)
		last := users[len(users)-1]
		from, text, isMessage := message(last)
		if isMessage {
			mu.Lock()
			saw[who] = last
			mu.Unlock()
		}
		rounds := sim.ToolRounds(req)
		if who == "expert" {
			switch {
			case rounds == 0:
				return turn(call("desk_work", map[string]string{})), nil
			case rounds == 1 && isMessage:
				// Reply to the sender, whose id the message names.
				return turn(call("tasks_send", map[string]string{
					"task_id": strings.TrimPrefix(from, "task:"),
					"text":    "The Eiffel Tower was completed in 1889.",
				})), nil
			}
			return sim.Reply("Catalogue updated; answered " + from + "."), nil
		}
		switch rounds {
		case 0:
			return turn(call("tasks_list", map[string]string{})), nil
		case 1:
			// The list (the last tool result) names the expert's task id.
			return turn(call("tasks_send", map[string]string{"task_id": "expert", "text": "When was the Eiffel Tower completed?"})), nil
		case 2:
			return turn(call("desk_work", map[string]string{})), nil
		}
		return sim.Reply("Plan: visit the Eiffel Tower in the morning. The expert says: " + text), nil
	}
}

// message reads a message from another task out of a user message: the
// machine puts "[message from <sender>] " before its text, and
// Config.SteerFormat (by default, a note that it arrived while the task was
// working) frames the whole.
func message(s string) (from, text string, ok bool) {
	const tag = "[message from "
	i := strings.Index(s, tag)
	if i < 0 {
		return "", "", false
	}
	rest := s[i+len(tag):]
	j := strings.Index(rest, "] ")
	if j < 0 {
		return "", "", false
	}
	return rest[:j], rest[j+2:], true
}

func turn(calls ...task.Part) model.Response {
	return model.Response{
		Message:      task.Message{Role: task.RoleAssistant, Parts: calls},
		FinishReason: "tool_calls",
		Usage:        task.Usage{PromptTokens: 40, CompletionTokens: 12},
	}
}

func call(name string, args any) task.Part {
	b, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return task.Part{Kind: task.PartToolCall, ProviderCallID: "call_" + name, Name: name, Args: b}
}
