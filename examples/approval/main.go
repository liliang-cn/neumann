// Command approval shows a tool policy holding a call with side effects for a
// human (design §9.7). The scripted model looks up a balance and asks for two
// transfers in one round; the policy lets the read-only lookup through, holds
// every transfer for approval, and refuses transfers to an unknown account
// outright. An operator, here a goroutine reading events, approves the first
// transfer as it is and edits the amount of the second; the round then runs
// as a unit and the model answers.
//
//	go run ./examples/approval
//
// Offline: the model is sim.Model and the bank is a Go function.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
	bank := builtin.New("bank").
		Add(bus.ToolDef{Name: "balance", Description: "Read the balance.", Effect: bus.ReadOnly},
			func(context.Context, bus.DeviceCall) (string, error) { return "balance: 900 EUR", nil }).
		// SideEffect: a transfer moves money and is never repeated on its own.
		Add(bus.ToolDef{Name: "transfer", Description: "Transfer money.", Effect: bus.SideEffect},
			func(_ context.Context, c bus.DeviceCall) (string, error) {
				fmt.Printf("   bank: transfer %s (key %s)\n", c.Args, c.Key)
				return "transferred " + string(c.Args), nil
			})

	m, err := neumann.New(neumann.Config{
		Model:      &sim.Model{Fn: script},
		Devices:    []bus.Device{bank},
		ToolPolicy: policy,
		Logf:       func(string, ...any) {},
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := m.Stop(sctx); err != nil {
			log.Print(err)
		}
	}()

	id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: "Pay the rent and the electricity bill."})
	if err != nil {
		log.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for ev := range m.Events(wctx, 0) {
		if ev.TaskID != id {
			continue
		}
		switch ev.Kind {
		case proto.EvToolCall, proto.EvToolDenied, proto.EvApprovalNeeded, proto.EvApproved, proto.EvToolResult, proto.EvAnswer, proto.EvDone:
			fmt.Printf("event %-15s %s\n", ev.Kind, ev.Data)
		}
		if ev.Kind == proto.EvApprovalNeeded {
			operator(ctx, m, id, ev)
		}
		if ev.Terminal() {
			break
		}
	}
	recs, _ := m.Calls(ctx, id)
	fmt.Println("\n== call log ==")
	for _, r := range recs {
		fmt.Printf("%s %-14s %-6s policy=%-8s approval=%-7s args=%s orig=%s\n", r.CallID, r.Tool, r.Status, r.Policy, r.Approval, r.Args, r.OrigArgs)
	}
}

// policy runs on the vCore, before the call's dispatch is committed: fast,
// and with no side effects of its own.
func policy(_ context.Context, info neumann.ToolCallInfo) neumann.Decision {
	if info.Effect != bus.SideEffect {
		return neumann.Allow()
	}
	var args struct {
		To     string `json:"to"`
		Amount int    `json:"amount"`
	}
	if err := json.Unmarshal(info.Args, &args); err != nil {
		return neumann.Deny("the arguments are not valid JSON")
	}
	if !strings.HasPrefix(args.To, "DE") {
		return neumann.Deny("transfers go to known German accounts only")
	}
	return neumann.RequireApproval(fmt.Sprintf("transfer %d EUR to %s", args.Amount, args.To))
}

// operator answers a held call: the rent as it is, the electricity bill with
// the amount corrected. Approve returns once the decision is committed; the
// last answer of the round dispatches it.
func operator(ctx context.Context, m *neumann.Machine, id string, ev proto.Event) {
	var d struct {
		CallID string `json:"call_id"`
		Args   string `json:"args"`
		Reason string `json:"reason"`
	}
	json.Unmarshal(ev.Data, &d)
	decision := neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}
	if strings.Contains(d.Args, `"amount":95`) {
		decision = neumann.ApprovalDecision{Verdict: neumann.ApproveEdit, Args: json.RawMessage(`{"to":"DE02","amount":59}`)}
	}
	fmt.Printf("   operator: %s %s -> %s\n", d.CallID, d.Reason, decision.Verdict)
	if err := m.Approve(ctx, id, d.CallID, decision); err != nil {
		log.Fatal(err)
	}
}

// script plays the model: one round of tools, then the answer.
func script(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) > 0 {
		var lines []string
		for _, p := range req.Messages[len(req.Messages)-1].Parts {
			lines = append(lines, fmt.Sprintf("%s: %s", p.CallID, p.Text))
		}
		return sim.Reply("Done.\n" + strings.Join(lines, "\n")), nil
	}
	msg := task.Message{Role: task.RoleAssistant}
	for i, c := range [][2]string{
		{"bank_balance", `{}`},
		{"bank_transfer", `{"to":"DE01","amount":800}`},
		{"bank_transfer", `{"to":"DE02","amount":95}`},
		{"bank_transfer", `{"to":"XX99","amount":1}`},
	} {
		msg.Parts = append(msg.Parts, task.Part{Kind: task.PartToolCall, ProviderCallID: fmt.Sprint("p-", i), Name: c[0], Args: json.RawMessage(c[1])})
	}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: task.Usage{PromptTokens: 10, CompletionTokens: 5}}, nil
}
