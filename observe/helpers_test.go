package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

const (
	modelTime = 2 * time.Second
	toolTime  = 500 * time.Millisecond
)

// rig is a machine whose store reads a fake clock that only the model and
// the tool move: each model call takes modelTime, each tool call toolTime.
// Run one task at a time and every event time is known in advance:
//
//	accepted                         t
//	step usage tool_call             t+2s
//	tool_result                      t+2.5s
//	step usage answer done           t+4.5s
type rig struct {
	m     *neumann.Machine
	clock *sim.Clock
	store *mem.Memory
}

func newRig(t *testing.T) *rig {
	t.Helper()
	clock := sim.NewClock(t0)
	store := mem.NewMemory()
	store.Now = clock.Now
	echo := builtin.New("util").Add(bus.ToolDef{Name: "echo", Description: "echo", Effect: bus.ReadOnly},
		func(context.Context, bus.DeviceCall) (string, error) {
			clock.Advance(toolTime)
			return "ok", nil
		})
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
			clock.Advance(modelTime)
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("util_echo"), nil
			}
			return sim.Reply("done: " + sim.FirstUser(req)), nil
		}},
		Devices: []bus.Device{echo},
		Store:   store,
		Cores:   2,
		Logf:    t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) })
	return &rig{m: m, clock: clock, store: store}
}

// run submits tasks one at a time, each after the one before has finished,
// so the fake clock moves for one task at a time.
func (r *rig) run(t *testing.T, ids ...string) {
	t.Helper()
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		evs := r.m.Events(ctx, r.head(t))
		if _, err := r.m.Submit(ctx, proto.Instruction{TaskID: id, Input: id}); err != nil {
			cancel()
			t.Fatal(err)
		}
		finished := false
		for ev := range evs {
			if ev.TaskID == id && ev.Terminal() {
				finished = true
				break
			}
		}
		cancel()
		if !finished {
			t.Fatalf("task %s did not finish", id)
		}
	}
}

func (r *rig) head(t *testing.T) int64 {
	t.Helper()
	n, err := Head(context.Background(), r.m)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// log is every committed event.
func (r *rig) log(t *testing.T) []proto.Event {
	t.Helper()
	evs, err := r.m.EventLog(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

// script builds committed-looking events by hand, for the paths a scripted
// machine cannot reach cheaply (failures, cancels, migrations).
type script struct {
	seq     int64
	taskSeq map[string]int64
	evs     []proto.Event
}

func (s *script) add(at time.Duration, kind, taskID string, data any) proto.Event {
	if s.taskSeq == nil {
		s.taskSeq = map[string]int64{}
	}
	s.seq++
	s.taskSeq[taskID]++
	ev := proto.NewEvent(kind, taskID, data)
	ev.Seq, ev.TaskSeq, ev.Time = s.seq, s.taskSeq[taskID], t0.Add(at)
	s.evs = append(s.evs, ev)
	return ev
}

func feed(o Observer, evs []proto.Event) {
	for _, ev := range evs {
		o.Observe(ev)
	}
}

func usage(prompt, completion, reasoning, cached int) map[string]int {
	return map[string]int{"prompt_tokens": prompt, "completion_tokens": completion, "reasoning_tokens": reasoning, "cached_tokens": cached}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func sec(d time.Duration) float64 { return d.Seconds() }

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("t%d", i+1)
	}
	return out
}
