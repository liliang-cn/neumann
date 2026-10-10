package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

// approvalScript is two tasks under a tool policy (design §9.7):
//
//	a: one round of four calls: dev_pay held for approval (allowed at 10s),
//	   dev_look allowed, dev_rm denied by the policy, dev_x rewritten; then a
//	   second round whose dev_pay an operator denies at 20s; done at 22s. A
//	   restart in between announces the first held call again.
//	b: dev_pay held at 30s; the task is cancelled at 40s with it still held.
func approvalScript() script {
	var s script
	need := func(at time.Duration, task, call string, again bool) {
		d := map[string]any{"call_id": call, "tool": "dev_pay", "device": "dev", "effect": "side_effect",
			"args": `{"secret":"do-not-export"}`, "reason": "a payment of 800 EUR"}
		if again {
			d["recovered"] = true
		}
		s.add(at, proto.EvApprovalNeeded, task, d)
	}
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	s.add(2*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(2*time.Second, proto.EvUsage, "a", usage(10, 5, 0, 0))
	for _, c := range [][2]string{{"c-1", "dev_pay"}, {"c-2", "dev_look"}, {"c-3", "dev_rm"}, {"c-4", "dev_x"}} {
		s.add(2*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": c[0], "tool": c[1], "args": `{"secret":"do-not-export"}`})
	}
	s.add(2*time.Second, proto.EvToolDenied, "a", map[string]any{"call_id": "c-3", "tool": "dev_rm", "reason": "never rm", "by": "policy"})
	s.add(2*time.Second, proto.EvToolRewritten, "a", map[string]any{"call_id": "c-4", "tool": "dev_x", "original": `{"secret":"do-not-export"}`, "args": `{"n":1}`})
	need(2*time.Second, "a", "c-1", false)
	s.add(5*time.Second, proto.EvRecovered, "a", map[string]any{"from": "waiting_approval", "pending": 1})
	need(5*time.Second, "a", "c-1", true)
	s.add(10*time.Second, proto.EvApproved, "a", map[string]any{"call_id": "c-1"})
	s.add(11*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-2", "is_error": false})
	s.add(11*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-4", "is_error": false})
	s.add(12*time.Second, proto.EvToolResult, "a", map[string]any{"call_id": "c-1", "is_error": false})
	s.add(13*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(13*time.Second, proto.EvUsage, "a", usage(10, 5, 0, 0))
	s.add(13*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-5", "tool": "dev_pay"})
	need(13*time.Second, "a", "c-5", false)
	s.add(20*time.Second, proto.EvToolDenied, "a", map[string]any{"call_id": "c-5", "tool": "dev_pay", "reason": "twice is enough", "by": "operator"})
	s.add(22*time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 1})
	s.add(22*time.Second, proto.EvUsage, "a", usage(10, 5, 0, 0))
	s.add(22*time.Second, proto.EvAnswer, "a", map[string]any{"text": "done"})
	s.add(22*time.Second, proto.EvDone, "a", map[string]any{"stop_reason": "end_turn"})

	s.add(28*time.Second, proto.EvAccepted, "b", map[string]any{"class": "big"})
	s.add(30*time.Second, proto.EvStep, "b", map[string]any{"pc": "model", "core": 1})
	s.add(30*time.Second, proto.EvUsage, "b", usage(10, 5, 0, 0))
	s.add(30*time.Second, proto.EvToolCall, "b", map[string]any{"call_id": "c-1", "tool": "dev_pay"})
	need(30*time.Second, "b", "c-1", false)
	s.add(40*time.Second, proto.EvCancelled, "b", map[string]any{"stop_reason": "cancelled"})
	return s
}

// Denials count as tool calls with outcome denied, whoever denied them;
// each held call is counted once, however often a restart announces it;
// the wait for a decision is measured from the first announcement to the
// decision, by verdict, and a held call its task's end takes is cancelled.
func TestMetricsOfApprovals(t *testing.T) {
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s := approvalScript()
	feed(m, s.evs)
	c := collect(t, reader)
	c.must("neumann.tool.calls", map[string]string{"tool": "dev_rm", "outcome": "denied"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "dev_pay", "outcome": "denied"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "dev_pay", "outcome": "ok"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "dev_pay", "outcome": "cancelled"}, 1)
	c.must("neumann.tool.calls", map[string]string{"tool": "dev_x", "outcome": "ok"}, 1)
	c.mustHist("neumann.tool.duration", map[string]string{"tool": "dev_rm", "outcome": "denied"}, 1, 0)
	c.mustHist("neumann.tool.duration", map[string]string{"tool": "dev_pay", "outcome": "ok"}, 1, 10)
	c.must("neumann.tool.denied", map[string]string{"tool": "dev_rm", "by": "policy"}, 1)
	c.must("neumann.tool.denied", map[string]string{"tool": "dev_pay", "by": "operator"}, 1)
	c.must("neumann.tool.rewritten", map[string]string{"tool": "dev_x"}, 1)
	c.must("neumann.approvals.needed", map[string]string{"class": "big", "tool": "dev_pay"}, 3)
	c.must("neumann.approvals.decided", map[string]string{"class": "big", "verdict": "allow"}, 1)
	c.must("neumann.approvals.decided", map[string]string{"class": "big", "verdict": "deny"}, 1)
	c.must("neumann.approvals.decided", map[string]string{"class": "big", "verdict": "cancelled"}, 1)
	c.mustHist("neumann.approval.wait", map[string]string{"class": "big", "verdict": "allow"}, 1, 8)
	c.mustHist("neumann.approval.wait", map[string]string{"class": "big", "verdict": "deny"}, 1, 7)
	c.mustHist("neumann.approval.wait", map[string]string{"class": "big", "verdict": "cancelled"}, 1, 10)
	c.must("neumann.tasks.recovered", map[string]string{"class": "big", "from": "waiting_approval"}, 1)
}

// An edit is its own verdict.
func TestMetricsOfAnEditedApproval(t *testing.T) {
	reader, mp := newReader(t)
	m, err := NewMetrics(nil, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big"})
	s.add(time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-1", "tool": "dev_pay"})
	s.add(time.Second, proto.EvApprovalNeeded, "a", map[string]any{"call_id": "c-1", "tool": "dev_pay"})
	s.add(4*time.Second, proto.EvApproved, "a", map[string]any{"call_id": "c-1", "edited": true, "original": "{}", "args": "{}"})
	feed(m, s.evs)
	c := collect(t, reader)
	c.mustHist("neumann.approval.wait", map[string]string{"class": "big", "verdict": "edit"}, 1, 3)
}

// The task span notes every policy and approval event; each held call has an
// approval span from its announcement to its decision; a denied call's span
// ends at its denial, as an error.
func TestTraceOfApprovals(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tr := NewTracer(exp, TracerOptions{})
	s := approvalScript()
	feed(tr, s.evs)
	tr.Flush(context.Background())
	got := spans(exp)

	root := one(t, got, "a", "task")
	want := []string{"tool_denied@2s", "tool_rewritten@2s", "approval_needed@2s", "recovered@5s", "approval_needed@5s",
		"approved@10s", "approval_needed@13s", "tool_denied@20s"}
	if !reflect.DeepEqual(root.Events, want) {
		t.Fatalf("task span events %v, want %v", root.Events, want)
	}
	waits := byName(got, "a", "approval dev_pay")
	if len(waits) != 2 {
		t.Fatalf("approval spans %+v", waits)
	}
	if w := waits[0]; !w.Start.Equal(t0.Add(2*time.Second)) || !w.End.Equal(t0.Add(10*time.Second)) ||
		w.Attrs["neumann.verdict"] != "allow" || w.Attrs["neumann.call.id"] != "c-1" || w.Parent != root.ID {
		t.Fatalf("allowed approval span %+v", w)
	}
	if w := waits[1]; !w.End.Equal(t0.Add(20*time.Second)) || w.Attrs["neumann.verdict"] != "deny" {
		t.Fatalf("denied approval span %+v", w)
	}
	if rm := one(t, got, "a", "tool dev_rm"); rm.Status != codes.Error || rm.Attrs["neumann.outcome"] != "denied" ||
		!rm.Start.Equal(t0.Add(2*time.Second)) || !rm.End.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("denied call span %+v", rm)
	}
	if b := one(t, got, "b", "approval dev_pay"); b.Attrs["neumann.verdict"] != "cancelled" || !b.End.Equal(t0.Add(40*time.Second)) {
		t.Fatalf("cancelled approval span %+v", b)
	}
	for _, sp := range got {
		for k, v := range sp.Attrs {
			if strings.Contains(v, "do-not-export") || strings.Contains(v, "800 EUR") {
				t.Errorf("span %s carries task content in %s", sp.Name, k)
			}
		}
	}
}

// approval_needed is a warning an operator must act on; denials are
// warnings too; approvals and rewrites are info. No arguments, no reasons.
func TestLogOfApprovals(t *testing.T) {
	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	s := approvalScript()
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=WARN", "msg=approval_needed", "task=a", "class=big", "call=c-1", "tool=dev_pay", "device=dev", "effect=side_effect")
	find(t, ls, "level=WARN", "msg=approval_needed", "task=a", "call=c-1", "recovered=true")
	find(t, ls, "level=WARN", "msg=tool_denied", "task=a", "call=c-3", "tool=dev_rm", "by=policy")
	find(t, ls, "level=WARN", "msg=tool_denied", "task=a", "call=c-5", "by=operator")
	find(t, ls, "level=INFO", "msg=approved", "task=a", "call=c-1", "verdict=allow", "waited=8s")
	find(t, ls, "level=INFO", "msg=tool_rewritten", "task=a", "call=c-4", "tool=dev_x")
	if out := buf.String(); strings.Contains(out, "do-not-export") || strings.Contains(out, "800 EUR") || strings.Contains(out, "never rm") {
		t.Errorf("task content in the log:\n%s", out)
	}
}

// The gauges read a live machine: a task waiting for approval is counted by
// class and in total until it is approved.
func TestApprovalGauges(t *testing.T) {
	dev := builtin.New("dev").Add(bus.ToolDef{Name: "pay"}, func(context.Context, bus.DeviceCall) (string, error) { return "paid", nil })
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("dev_pay"), nil
			}
			return sim.Reply("done"), nil
		}},
		Devices: []bus.Device{dev},
		ToolPolicy: func(context.Context, neumann.ToolCallInfo) neumann.Decision {
			return neumann.RequireApproval("")
		},
		Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Stop(context.Background()) })
	reader, mp := newReader(t)
	met, err := NewMetrics(m, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer met.Close()
	id, err := m.Submit(context.Background(), proto.Instruction{Input: "pay"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.Status().Approval == 1 })
	c := collect(t, reader)
	c.must("neumann.tasks.awaiting_approval", nil, 1)
	c.must("neumann.queue.waiting_approval", map[string]string{"class": "default"}, 1)

	evs, _ := m.EventLog(context.Background(), 0, 0)
	var call string
	for _, ev := range evs {
		if ev.Kind == proto.EvApprovalNeeded {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(ev.Data, &d)
			call = d.CallID
		}
	}
	if err := m.Approve(context.Background(), id, call, neumann.ApprovalDecision{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { tc, _ := m.Load(context.Background(), id); return tc.State.Terminal() })
	c = collect(t, reader)
	c.must("neumann.tasks.awaiting_approval", nil, 0)
	c.must("neumann.queue.waiting_approval", map[string]string{"class": "default"}, 0)
}
