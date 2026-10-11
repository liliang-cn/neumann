// Command decide shows a decision engine working as the machine's
// coprocessor: a small model that answers closed questions in one pass,
// asked by the host and by the tool policy, never scheduled as a task step.
//
//   - a Router sends each request to the "fast" or the "strong" group;
//
//   - a Policy lets a read through without asking, and holds a delete for a
//     person, with the engine's answer as the committed reason;
//
//   - a Checker reads each answer next to its request; an answer that only
//     promises the work is caught, and the request is submitted again with
//     the checker's feedback.
//
//     go run ./examples/decide                 # offline: a scripted engine
//     go run ./examples/decide -live           # tev1:4b on the local Ollama
//     NEUMANN_DECIDE_KEY=... go run ./examples/decide -live -url https://relay.example/v1 -model tev1:4b
//
// The task models are sim.Model in both modes; -live replaces only the
// decision engine. -key defaults to $NEUMANN_DECIDE_KEY and is never
// printed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/decide"
	"github.com/liliang-cn/neumann/decide/decidetest"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func main() {
	live := flag.Bool("live", false, "ask a real System One server instead of the scripted engine")
	url := flag.String("url", decide.DefaultURL, "System One server (a trailing /v1 is fine)")
	modelName := flag.String("model", decide.DefaultModel, "decision model")
	key := flag.String("key", "", "bearer key for a relay (default $NEUMANN_DECIDE_KEY)")
	flag.Parse()
	if *key == "" {
		*key = os.Getenv("NEUMANN_DECIDE_KEY") // not the flag's default: -help would print it
	}

	ctx := context.Background()
	var engine decide.Engine = scripted()
	if *live {
		so := decide.NewSystemOne(decide.SystemOneConfig{URL: *url, Model: *modelName, APIKey: *key, KeepAlive: 10 * time.Minute})
		if err := so.Ready(ctx); err != nil { // the server lists the model
			log.Fatal(err)
		}
		start := time.Now()
		wctx, cancel := context.WithTimeout(ctx, time.Minute) // a cold load takes seconds; a stuck server, forever
		err := so.Warm(wctx)                                  // and has it loaded
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("warmed %s in %s\n", so.Name(), time.Since(start).Round(time.Millisecond))
		engine = so
	}
	if _, err := run(ctx, os.Stdout, engine); err != nil {
		log.Fatal(err)
	}
}

// result is what a run did, for the test.
type result struct {
	Routes   map[string]string // input -> class
	Held     []string          // approval_needed reasons
	Deleted  *atomic.Int32     // notes_delete calls that ran
	Feedback string            // the checker's feedback on the plan-only answer
	Answers  []string          // final answers, in order
}

const (
	sum     = "What is 17 + 25?"
	tidy    = "Go through my notes, delete the outdated one and summarise what is left in a short paragraph."
	summary = "Summarise my notes in one line."
)

func run(ctx context.Context, w io.Writer, engine decide.Engine) (result, error) {
	res := result{Routes: map[string]string{}, Deleted: new(atomic.Int32)}
	notes := builtin.New("notes").
		Add(bus.ToolDef{Name: "read", Description: "Read all notes.", Effect: bus.ReadOnly},
			func(context.Context, bus.DeviceCall) (string, error) {
				return "n-1: groceries for Saturday; n-2: dentist on Monday (outdated: it was last week)", nil
			}).
		Add(bus.ToolDef{Name: "delete", Description: "Delete a note for good.", Effect: bus.SideEffect},
			func(_ context.Context, c bus.DeviceCall) (string, error) {
				res.Deleted.Add(1)
				return "deleted " + string(c.Args), nil
			})

	m, err := neumann.New(neumann.Config{
		Groups: []neumann.GroupConfig{
			{Class: "fast", Model: &sim.Model{Name: "sim:small", Fn: script}},
			{Class: "strong", Model: &sim.Model{Name: "sim:large", Fn: script}},
		},
		Default: "strong",
		Devices: []bus.Device{notes},
		ToolPolicy: decide.Policy(engine, decide.PolicyConfig{
			Tools: map[string]string{"notes_delete": "Delete a note for good.", "notes_read": "Read all notes."},
			Logf:  func(f string, a ...any) { fmt.Fprintf(w, "   policy: "+f+"\n", a...) },
		}),
		Logf: func(string, ...any) {},
	})
	if err != nil {
		return res, err
	}
	if err := m.Start(ctx); err != nil {
		return res, err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		m.Stop(sctx)
	}()

	router, err := decide.NewRouter(engine, decide.RouterConfig{
		Routes: []decide.Route{
			{Class: "fast", Description: "a small, fast model: short factual answers, simple lookups, one-step requests"},
			{Class: "strong", Description: "a large, careful model: multi-step work with tools, reasoning, writing"},
		},
		Default: "strong",
	})
	if err != nil {
		return res, err
	}
	checker, err := decide.NewChecker(engine, decide.CheckerConfig{})
	if err != nil {
		return res, err
	}
	fmt.Fprintf(w, "decision engine: %s\n", engine.Name())

	for _, input := range []string{sum, tidy, summary} {
		fmt.Fprintf(w, "\n> %s\n", input)
		start := time.Now()
		class, ans, err := router.Route(ctx, input)
		if err != nil {
			fmt.Fprintf(w, "   router: %v; using %s\n", err, class)
		}
		note := ""
		if ans.Label != "" && ans.Label != class {
			note = fmt.Sprintf("; the engine said %s, below the floor", ans.Label)
		}
		fmt.Fprintf(w, "   router: %s (confidence %.2f, %s%s)\n", class, ans.Confidence, time.Since(start).Round(time.Millisecond), note)
		res.Routes[input] = class

		// A rejected answer is submitted once more, with the feedback.
		prompt := input
		for attempt := 1; attempt <= 2; attempt++ {
			answer, err := runTask(ctx, w, m, proto.Instruction{Input: prompt, Class: class}, &res)
			if err != nil {
				return res, err
			}
			fmt.Fprintf(w, "   answer: %s\n", answer)
			start := time.Now()
			chk, err := checker.Check(ctx, input, answer)
			took := time.Since(start).Round(time.Millisecond)
			if err != nil {
				fmt.Fprintf(w, "   checker: no answer (%v); passed\n", err)
			}
			if chk.Pass {
				fmt.Fprintf(w, "   checker: pass (%s)\n", took)
				res.Answers = append(res.Answers, answer)
				break
			}
			fmt.Fprintf(w, "   checker: failed %v (%s): %s\n", chk.Failed, took, chk.Feedback)
			res.Feedback = chk.Feedback
			if attempt == 2 {
				res.Answers = append(res.Answers, answer)
				break
			}
			prompt = input + "\n\n" + chk.Feedback
			fmt.Fprintf(w, "   resubmitted with the feedback\n")
		}
	}
	return res, nil
}

// runTask submits in, approves every held call (an operator would look at
// the reason first), and returns the task's answer.
func runTask(ctx context.Context, w io.Writer, m *neumann.Machine, in proto.Instruction, res *result) (string, error) {
	id, err := m.Submit(ctx, in)
	if err != nil {
		return "", err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for ev := range m.Events(wctx, 0) {
		if ev.TaskID != id {
			continue
		}
		var d struct {
			CallID string `json:"call_id"`
			Tool   string `json:"tool"`
			Reason string `json:"reason"`
		}
		json.Unmarshal(ev.Data, &d)
		switch ev.Kind {
		case proto.EvToolCall:
			fmt.Fprintf(w, "   %s: %s\n", ev.Kind, d.Tool)
		case proto.EvApprovalNeeded:
			fmt.Fprintf(w, "   held for approval: %s (%s)\n", d.Tool, d.Reason)
			res.Held = append(res.Held, d.Reason)
			fmt.Fprintf(w, "   operator: approve %s\n", d.CallID)
			if err := m.Approve(ctx, id, d.CallID, neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}); err != nil {
				return "", err
			}
		}
		if ev.Terminal() {
			break
		}
	}
	t, err := m.Load(ctx, id)
	if err != nil {
		return "", err
	}
	if t.State != task.Done {
		return "", fmt.Errorf("task %s ended %s (%s)", id, t.State, t.StopReason)
	}
	return t.LastAnswer(), nil
}

// script plays both task models.
func script(req model.Request) (model.Response, error) {
	first := sim.FirstUser(req)
	switch {
	case strings.HasPrefix(first, sum):
		return sim.Reply("17 + 25 = 42."), nil
	case strings.HasPrefix(first, tidy):
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("notes_read"), nil
		}
		if sim.ToolRounds(req) == 1 {
			msg := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{
				Kind: task.PartToolCall, ProviderCallID: "p-del", Name: "notes_delete", Args: json.RawMessage(`{"id":"n-2"}`),
			}}}
			return model.Response{Message: msg, FinishReason: "tool_calls"}, nil
		}
		return sim.Reply("I deleted n-2, the dentist appointment, which was last week. What is left is one note: groceries for Saturday."), nil
	case strings.HasPrefix(first, summary):
		if strings.Contains(first, "do the work now") {
			return sim.Reply("One note is left: groceries for Saturday."), nil
		}
		// The failure the checker is for: a promise instead of the work.
		return sim.Reply("I'll read through your notes first and then write a one-line summary for you."), nil
	}
	return sim.Reply("I do not know that request."), nil
}

// scripted is the offline engine: it answers the questions the router, the
// policy and the checker ask, the way tev1:4b answered them.
func scripted() *decidetest.Engine {
	return &decidetest.Engine{EngineName: "scripted", Fn: func(state string, asks []decide.Ask) (map[string]decide.Answer, error) {
		out := map[string]decide.Answer{}
		for _, a := range asks {
			name := a.Name
			switch name {
			case decide.RouteQuestion:
				out[name] = decidetest.Pick("fast", 0.76)
				if strings.Contains(state, "delete") {
					out[name] = decidetest.Pick("strong", 0.44)
				}
			case decide.NeedsApproval:
				out[name] = decidetest.Yes(0.03)
				if strings.Contains(state, "tool: notes_delete\n") {
					out[name] = decidetest.Yes(0.95)
				}
			case decide.CheckOnTopic:
				out[name] = decidetest.Yes(0.95)
			case decide.CheckRefuses:
				out[name] = decidetest.Yes(0.05)
			case decide.CheckPlanOnly:
				out[name] = decidetest.Yes(0.06)
				if strings.Contains(state, "I'll") {
					out[name] = decidetest.Yes(0.94)
				}
			default:
				return nil, fmt.Errorf("decidetest: no script for %q", name)
			}
		}
		return out, nil
	}}
}
