package neumann_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// A machine with one tool and a scripted model: submit a task, follow its
// events until it ends, read the answer. Swap the sim.Model for an adapter
// from model/openai or model/anthropic to talk to a real model.
func Example() {
	clock := builtin.New("clock").Add(bus.ToolDef{
		Name:        "today",
		Description: "Today's date.",
		Schema:      json.RawMessage(`{"type":"object"}`),
		Effect:      bus.ReadOnly,
	}, func(context.Context, bus.DeviceCall) (string, error) { return "2026-10-10", nil })

	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("clock_today"), nil // the model asks for the tool
			}
			return sim.Reply("It is 10 October 2026."), nil // then answers
		}},
		Devices: []bus.Device{clock},
	})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil { // recovers unfinished tasks, then runs
		panic(err)
	}
	defer m.Stop(ctx)

	id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: "What is the date?"})
	if err != nil {
		panic(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for ev := range m.Events(wctx, 0) {
		if ev.TaskID == id && ev.Kind == proto.EvAnswer {
			var a struct{ Text string }
			_ = json.Unmarshal(ev.Data, &a)
			fmt.Println(a.Text)
		}
		if ev.TaskID == id && ev.Terminal() {
			fmt.Println(ev.Kind)
			break
		}
	}
	// Output:
	// It is 10 October 2026.
	// done
}
