package jsonl

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func TestOperatorControlFields(t *testing.T) {
	for _, c := range []struct{ line, mention string }{
		{`{"kind":"class","class":"big"}`, "task_id"},
		{`{"kind":"class","task_id":"t1"}`, "class"},
		{`{"kind":"class","task_id":"t1","class":"big","input":"x"}`, "input"},
		{`{"kind":"cores","class":"big"}`, "cores"},
		{`{"kind":"cores","class":"big","cores":0}`, "cores"},
		{`{"kind":"cores","cores":2}`, "class"},
		{`{"kind":"cores","class":"big","cores":"2"}`, "cores"},
		{`{"kind":"autoscale","class":"big"}`, "on"},
		{`{"kind":"autoscale","on":true}`, "class"},
		{`{"kind":"group_reset"}`, "class"},
		{`{"kind":"queue","class":"big"}`, "class"},
	} {
		e := decodeErr(t, c.line, CodeBadRequest)
		if !strings.Contains(e.Message, c.mention) {
			t.Errorf("%s: message %q does not mention %q", c.line, e.Message, c.mention)
		}
	}
	req := decodeOK(t, `{"kind":"cores","class":"big","cores":3}`)
	if req.Class != "big" || *req.Cores != 3 {
		t.Fatalf("cores %+v", req)
	}
	if req := decodeOK(t, `{"kind":"autoscale","class":"big","on":false}`); *req.On {
		t.Fatalf("autoscale %+v", req)
	}
	if req := decodeOK(t, `{"kind":"submit","input":"x","pin_class":true}`); !req.PinClass {
		t.Fatalf("submit %+v", req)
	}
}

func TestOperatorErrorCodes(t *testing.T) {
	if e := errorFor(&neumann.MigrateError{From: "a", To: "b", Check: "context", Detail: "too big"}); e.Code != CodeIncompatible || e.Check != "context" {
		t.Fatalf("migrate error %+v", e)
	}
	if e := errorFor(errors.Join(errors.New("task t1"), neumann.ErrTaskBusy)); e.Code != CodeTaskBusy {
		t.Fatalf("task busy %+v", e)
	}
}

// The operator controls over the wire: a task waiting for its group, which
// has run out of quota, is moved to another group with class (a move the
// group cannot take is refused, incompatible, naming the check); cores and
// autoscale pin and free a group's window; group_reset brings a group back;
// queue reports the queues by class.
func TestOperatorControlsOverTheWire(t *testing.T) {
	big := &sim.Model{Name: "big", Fn: func(model.Request) (model.Response, error) { return model.Response{}, sim.QuotaExhausted() }}
	notools := &sim.Model{Name: "notools", Info: &model.Caps{}, Fn: func(model.Request) (model.Response, error) { return sim.Reply("n"), nil }}
	little := &sim.Model{Name: "little", Fn: func(model.Request) (model.Response, error) { return sim.Reply("little done"), nil }}
	m, err := neumann.New(neumann.Config{Logf: t.Logf, Devices: []bus.Device{echoDevice()}, Groups: []neumann.GroupConfig{
		{Class: "big", Model: big}, {Class: "notools", Model: notools}, {Class: "little", Model: little},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	h := serve(t, m, nil)
	r := h.do(`{"id":1,"kind":"submit","input":"go","task_id":"t1"}`, nil)
	mustOK(t, r, proto.KindSubmit)
	deadline := time.Now().Add(wait)
	for {
		tc, err := m.Load(context.Background(), "t1")
		if err == nil && tc.State == task.Waiting && tc.Wait == task.WaitGroup {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("t1 never waited for its group")
		}
		time.Sleep(time.Millisecond)
	}

	r = h.do(`{"kind":"queue"}`, nil)
	mustOK(t, r, KindQueue)
	var q neumann.QueueStatus
	if err := json.Unmarshal(r.Queue, &q); err != nil || len(q.Classes) != 3 || q.Classes[0].Class != "big" || q.Classes[0].ForGroup != 1 {
		t.Fatalf("queue %s: %v", r.Queue, err)
	}
	r = h.do(`{"kind":"class","task_id":"t1","class":"notools"}`, nil)
	mustFail(t, r, CodeIncompatible)
	if r.Error.Check != "tools" || r.TaskID != "" {
		t.Fatalf("refused move %+v", r.Error)
	}
	mustFail(t, h.do(`{"kind":"class","task_id":"nope","class":"little"}`, nil), CodeNotFound)
	mustFail(t, h.do(`{"kind":"class","task_id":"t1","class":"nowhere"}`, nil), CodeFailed)
	r = h.do(`{"kind":"class","task_id":"t1","class":"little"}`, nil)
	mustOK(t, r, KindClass)
	if r.TaskID != "t1" {
		t.Fatalf("class reply %+v", r)
	}
	for {
		tc, err := m.Load(context.Background(), "t1")
		if err == nil && tc.State == task.Done {
			if tc.Class != "little" || tc.LastAnswer() != "little done" {
				t.Fatalf("t1 ended in %q: %q", tc.Class, tc.LastAnswer())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("t1 never finished")
		}
		time.Sleep(time.Millisecond)
	}

	mustOK(t, h.do(`{"kind":"cores","class":"little","cores":3}`, nil), KindCores)
	mustOK(t, h.do(`{"kind":"group_reset","class":"big"}`, nil), KindGroupReset)
	r = h.do(`{"kind":"status"}`, nil)
	var st neumann.Status
	if err := json.Unmarshal(r.Status, &st); err != nil {
		t.Fatal(err)
	}
	if g := st.Groups[2]; g.Class != "little" || g.Target != 3 || g.Auto {
		t.Fatalf("little after cores: %+v", g)
	}
	if g := st.Groups[0]; g.Breaker != "closed" {
		t.Fatalf("big after group_reset: %+v", g)
	}
	mustOK(t, h.do(`{"kind":"autoscale","class":"little","on":true}`, nil), KindAutoScale)
	mustFail(t, h.do(`{"kind":"cores","class":"nowhere","cores":2}`, nil), CodeFailed)
	mustFail(t, h.do(`{"kind":"autoscale","class":"nowhere","on":true}`, nil), CodeFailed)
	mustFail(t, h.do(`{"kind":"group_reset","class":"nowhere"}`, nil), CodeFailed)
	if st := m.Status(); !st.Groups[2].Auto {
		t.Fatalf("little after autoscale: %+v", st.Groups[2])
	}
}

// priority over the wire (design §12): it needs a task_id and a priority of
// 0 or more (0 itself given, not left out); a task on a vCore is task_busy,
// an unknown one not_found, and a ready task's change is committed with a
// priority_set event before the reply.
func TestPriorityOverTheWire(t *testing.T) {
	for _, line := range []string{
		`{"kind":"priority","task_id":"t1"}`,
		`{"kind":"priority","priority":0}`,
		`{"kind":"priority","task_id":"t1","priority":-1}`,
		`{"kind":"priority","task_id":"t1","priority":"high"}`,
		`{"kind":"priority","task_id":"t1","priority":1,"class":"big"}`,
	} {
		decodeErr(t, line, CodeBadRequest)
	}
	if req := decodeOK(t, `{"kind":"priority","task_id":"t1","priority":0}`); req.TaskID != "t1" || req.Priority != 0 {
		t.Fatalf("priority %+v", req)
	}

	gate := make(chan struct{})
	h := start(t, neumann.Config{Cores: 1}, func(req model.Request) (model.Response, error) {
		if sim.FirstUser(req) == "hold" {
			<-gate
		}
		return sim.Reply("done"), nil
	})
	mustOK(t, h.do(`{"kind":"submit","input":"hold","task_id":"hold"}`, nil), proto.KindSubmit)
	deadline := time.Now().Add(wait)
	for {
		if tc, err := h.m.Load(bg, "hold"); err == nil && tc.State == task.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hold never ran")
		}
		time.Sleep(time.Millisecond)
	}
	mustOK(t, h.do(`{"kind":"submit","input":"q","task_id":"q","priority":1}`, nil), proto.KindSubmit)
	mustFail(t, h.do(`{"kind":"priority","task_id":"hold","priority":2}`, nil), CodeTaskBusy)
	mustFail(t, h.do(`{"kind":"priority","task_id":"nope","priority":2}`, nil), CodeNotFound)
	r := h.do(`{"id":7,"kind":"priority","task_id":"q","priority":0}`, nil)
	mustOK(t, r, KindPriority)
	if r.TaskID != "q" || string(r.ID) != "7" {
		t.Fatalf("priority reply %+v", r)
	}
	if tc, err := h.m.Load(bg, "q"); err != nil || tc.Priority != 0 {
		t.Fatalf("q after priority: %+v, %v", tc, err)
	}
	close(gate)
}
