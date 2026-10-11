package neumann_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// kindsData is each event of a task as "kind data", for the kinds asked for.
func kindsData(t *testing.T, m *neumann.Machine, id string, kinds ...string) []string {
	t.Helper()
	var out []string
	for _, ev := range eventsOf(t, m, id) {
		for _, k := range kinds {
			if ev.Kind == k {
				out = append(out, ev.Kind+" "+string(ev.Data))
			}
		}
	}
	return out
}

// A steer with a sender reaches the model after its sender's name, inside
// SteerFormat, and its delivery is an event that names the sender; a steer
// without one, delivered in the same step, is exactly what it was.
func TestSteerWithASenderIsPrefixedAndReported(t *testing.T) {
	for _, framed := range []bool{false, true} {
		t.Run(map[bool]string{false: "identity", true: "default format"}[framed], func(t *testing.T) {
			entered := make(chan string, 4)
			gate, release := newGate()
			defer release()
			var mu sync.Mutex
			var saw []string
			cfg := neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}
			if framed {
				cfg.SteerFormat = neumann.DefaultSteerFormat
			}
			m, _ := start(t, cfg, func(req model.Request) (model.Response, error) {
				if sim.ToolRounds(req) == 0 {
					return sim.CallTools("dev_slow"), nil
				}
				mu.Lock()
				saw = sim.UserTexts(req)
				mu.Unlock()
				return sim.Reply("ok"), nil
			})
			id := submit(t, m, proto.Instruction{Input: "A"})
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("slow tool never started")
			}
			if err := m.SteerWith(bg, id, "also X", neumann.SteerOptions{From: "operator"}); err != nil {
				t.Fatal(err)
			}
			if err := m.Steer(bg, id, "and Y"); err != nil {
				t.Fatal(err)
			}
			release()
			waitDone(t, m, id)
			frame := func(s string) string { return s }
			if framed {
				frame = neumann.DefaultSteerFormat
			}
			want := []string{"A", frame("[message from operator] also X"), frame("and Y")}
			mu.Lock()
			if strings.Join(saw, "|") != strings.Join(want, "|") {
				t.Fatalf("model saw %q, want %q", saw, want)
			}
			mu.Unlock()
			got := kindsData(t, m, id, proto.EvSteerDelivered, proto.EvSteerDropped)
			if len(got) != 1 || got[0] != proto.EvSteerDelivered+` {"from":"operator","seq":1}` {
				t.Fatalf("steer events %q", got)
			}
			// Committed with the step that sent it, ahead of its events.
			evs := eventsOf(t, m, id)
			for i, ev := range evs {
				if ev.Kind == proto.EvSteerDelivered && (i+1 >= len(evs) || evs[i+1].Kind != proto.EvStep || evs[i+1].Seq != ev.Seq+1) {
					t.Fatalf("steer_delivered is not right before its step: %+v", evs)
				}
			}
		})
	}
}

// A steer the task never answers is reported dropped with its sender; one
// without a sender is reported as it always was, byte for byte.
func TestSteerWithASenderIsReportedDropped(t *testing.T) {
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(model.Request) (model.Response, error) {
		return sim.CallTools("dev_slow"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A", Budget: &task.Budget{MaxSteps: 1}})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow tool never started")
	}
	if err := m.SteerWith(bg, id, "too late", neumann.SteerOptions{From: "task:t-42"}); err != nil {
		t.Fatal(err)
	}
	if err := m.Steer(bg, id, "also late"); err != nil {
		t.Fatal(err)
	}
	release()
	got := waitDone(t, m, id)
	if got.StopReason != "budget" {
		t.Fatalf("ended %s: %s", got.State, got.StopReason)
	}
	want := []string{
		proto.EvSteerDropped + ` {"from":"task:t-42","seq":1,"text":"too late"}`,
		proto.EvSteerDropped + ` {"seq":2,"text":"also late"}`,
	}
	if evs := kindsData(t, m, id, proto.EvSteerDelivered, proto.EvSteerDropped); strings.Join(evs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("steer events:\n%s\nwant:\n%s", strings.Join(evs, "\n"), strings.Join(want, "\n"))
	}
}

// A steer without a sender produces no steer_delivered event: a machine that
// never names senders writes the event log it always did.
func TestSteerWithoutASenderIsDeliveredSilently(t *testing.T) {
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A"})
	<-entered
	if err := m.SteerWith(bg, id, "also X", neumann.SteerOptions{}); err != nil {
		t.Fatal(err)
	}
	release()
	got := waitDone(t, m, id)
	if !hasUserText(got, "also X") {
		t.Fatalf("regs %+v", got.Regs)
	}
	var kinds []string
	for _, ev := range eventsOf(t, m, id) {
		kinds = append(kinds, ev.Kind)
	}
	want := "accepted model_call step usage tool_call tool_result model_call step usage answer done"
	if strings.Join(kinds, " ") != want {
		t.Fatalf("events %s\nwant   %s", strings.Join(kinds, " "), want)
	}
}

// A sender must be a short name on one line; a keyed steer is committed once
// per key, and a repeat is acknowledged even after the task has ended.
func TestSteerWithChecksItsSenderAndKeepsItsKey(t *testing.T) {
	store := mem.NewMemory()
	entered := make(chan string, 4)
	gate, release := newGate()
	defer release()
	m, _ := start(t, neumann.Config{Store: store, Devices: []bus.Device{gateDev(entered, gate)}}, func(req model.Request) (model.Response, error) {
		if sim.ToolRounds(req) == 0 {
			return sim.CallTools("dev_slow"), nil
		}
		return sim.Reply("ok"), nil
	})
	id := submit(t, m, proto.Instruction{Input: "A"})
	<-entered
	for _, from := range []string{"a\nb", "tab\there", strings.Repeat("x", neumann.MaxSteerFrom+1)} {
		if err := m.SteerWith(bg, id, "x", neumann.SteerOptions{From: from}); err == nil {
			t.Fatalf("sender %q accepted", from)
		}
	}
	if err := m.SteerWith(bg, id, "  ", neumann.SteerOptions{From: "operator"}); err == nil {
		t.Fatal("an empty steer with a sender accepted")
	}
	o := neumann.SteerOptions{From: "task:b", Key: "b/c-1/1"}
	for range 3 {
		if err := m.SteerWith(bg, id, "once", o); err != nil {
			t.Fatal(err)
		}
	}
	release()
	got := waitDone(t, m, id)
	n := 0
	for _, msg := range got.Regs {
		if msg.Role == task.RoleUser && msg.TextContent() == "[message from task:b] once" {
			n++
		}
	}
	all, _ := store.Interrupts(bg, id, false)
	if n != 1 || len(all) != 1 || all[0].Key != o.Key || all[0].From != o.From {
		t.Fatalf("delivered %d times; interrupts %+v", n, all)
	}
	if err := m.SteerWith(bg, id, "once", o); err != nil {
		t.Fatalf("a repeat after the end: %v", err)
	}
	if err := m.SteerWith(bg, id, "once", neumann.SteerOptions{From: "task:b", Key: "b/c-1/2"}); !errors.Is(err, neumann.ErrFinished) {
		t.Fatalf("a new key after the end: %v", err)
	}
	if err := m.SteerWith(bg, "nope", "x", o); !errors.Is(err, neumann.ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
}
