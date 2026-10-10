package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/task"
)

func TestRunReportsACallThatNeverRan(t *testing.T) {
	ran := make(chan struct{}, 1)
	d := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, func(context.Context, bus.DeviceCall) (string, error) {
		ran <- struct{}{}
		return "ok", nil
	})
	b := mustBus(t, 4, d)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	res, how := b.Run(cctx, "dev_x", nil, "", nil, nil, nil)
	if how != bus.NotRun || !res.IsError {
		t.Fatalf("got %+v, %v", res, how)
	}
	select {
	case <-ran:
		t.Fatal("the device ran")
	default:
	}
}

func TestRunReportsAQueuedCallAsNotRun(t *testing.T) {
	hold, entered := make(chan struct{}), make(chan struct{})
	d := builtin.New("dev").Add(bus.ToolDef{Name: "hold"}, func(context.Context, bus.DeviceCall) (string, error) {
		entered <- struct{}{}
		<-hold
		return "ok", nil
	})
	b := mustBus(t, 1, d)
	done := make(chan struct{})
	go func() { b.Call(ctx, "dev_hold", nil, "", nil, nil); close(done) }()
	<-entered
	cctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, how := b.Run(cctx, "dev_hold", nil, "", nil, nil, nil); how != bus.NotRun {
		t.Fatalf("queued call: %v", how)
	}
	close(hold)
	<-done
}

// A device that ignores its context keeps running after the caller gave up;
// what it returns then goes to late.
func TestRunHandsALateResultToLate(t *testing.T) {
	gate, entered := make(chan struct{}), make(chan struct{})
	d := builtin.New("dev").Add(bus.ToolDef{Name: "deaf"}, func(context.Context, bus.DeviceCall) (string, error) {
		close(entered)
		<-gate
		return "paid", nil
	})
	b := mustBus(t, 1, d)
	cctx, cancel := context.WithCancel(ctx)
	go func() { <-entered; cancel() }()
	late := make(chan task.ToolResult, 1)
	res, how := b.Run(cctx, "dev_deaf", nil, "", nil, nil, func(r task.ToolResult) { late <- r })
	if how != bus.Abandoned || !res.IsError {
		t.Fatalf("got %+v, %v", res, how)
	}
	close(gate)
	select {
	case r := <-late:
		if r.Content != "paid" || r.IsError {
			t.Fatalf("late %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the late result never arrived")
	}
	// Its slot is free again.
	if _, how := b.Run(ctx, "dev_deaf", nil, "", nil, nil, nil); how != bus.Returned {
		t.Fatalf("after: %v", how)
	}
}

// A device that stops when told and echoes the cancellation has no result:
// whether it took effect is unknown, and nothing arrives late.
func TestRunEchoOfCancelIsAbandonedWithoutALateResult(t *testing.T) {
	entered := make(chan struct{})
	d := builtin.New("dev").Add(bus.ToolDef{Name: "polite"}, func(c context.Context, _ bus.DeviceCall) (string, error) {
		close(entered)
		<-c.Done()
		time.Sleep(10 * time.Millisecond) // returns after the bus gave up
		return "", c.Err()
	})
	b := mustBus(t, 1, d)
	cctx, cancel := context.WithCancel(ctx)
	go func() { <-entered; cancel() }()
	late := make(chan task.ToolResult, 1)
	if _, how := b.Run(cctx, "dev_polite", nil, "", nil, nil, func(r task.ToolResult) { late <- r }); how != bus.Abandoned {
		t.Fatalf("got %v", how)
	}
	select {
	case r := <-late:
		t.Fatalf("an echo of the cancellation arrived as a late result: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRunTimeoutIsAResult(t *testing.T) {
	d := builtin.New("dev").Add(bus.ToolDef{Name: "deaf", Timeout: 20 * time.Millisecond}, func(context.Context, bus.DeviceCall) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "late", nil
	})
	b := mustBus(t, 1, d)
	late := make(chan task.ToolResult, 1)
	// The caller releases its context once Run returns, as a tool batch does.
	cctx, cancel := context.WithCancel(ctx)
	res, how := b.Run(cctx, "dev_deaf", nil, "", nil, nil, func(r task.ToolResult) { late <- r })
	cancel()
	if how != bus.Returned || !res.IsError {
		t.Fatalf("got %+v, %v", res, how)
	}
	select {
	case r := <-late:
		t.Fatalf("a timed-out call reported late: %+v", r)
	case <-time.After(200 * time.Millisecond):
	}
}
