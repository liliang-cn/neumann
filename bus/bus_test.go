package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func echo(_ context.Context, c bus.DeviceCall) (string, error) { return string(c.Args), nil }

func mustBus(t *testing.T, perDevice int, devs ...bus.Device) *bus.Bus {
	t.Helper()
	b, err := bus.New(ctx, perDevice, devs...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func names(specs []task.ToolSpec) []string {
	var out []string
	for _, s := range specs {
		out = append(out, s.Name)
	}
	return out
}

func TestFullName(t *testing.T) {
	cases := map[[2]string]string{
		{"builtin", "echo"}:           "builtin_echo",
		{"mcp.github", "list issues"}: "mcp_github_list_issues",
		{"a-b", "c"}:                  "a-b_c",
	}
	for in, want := range cases {
		if got := bus.FullName(in[0], in[1]); got != want {
			t.Errorf("FullName(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestDuplicateNamesAreRejected(t *testing.T) {
	d1 := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, echo)
	d2 := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, echo)
	if _, err := bus.New(ctx, 4, d1, d2); err == nil {
		t.Fatal("duplicate tool name was accepted")
	}
}

func TestSpecsAreSortedAndFiltered(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "zeta"}, echo).
		Add(bus.ToolDef{Name: "alpha"}, echo).
		Add(bus.ToolDef{Name: "beta"}, echo)
	b := mustBus(t, 4, d)
	check := func(allow, deny []string, want ...string) {
		t.Helper()
		if got := names(b.Specs(allow, deny)); !slices.Equal(got, want) {
			t.Errorf("Specs(%v,%v) = %v, want %v", allow, deny, got, want)
		}
	}
	check(nil, nil, "dev_alpha", "dev_beta", "dev_zeta")
	check(nil, []string{"dev_b"}, "dev_alpha", "dev_zeta")
	check([]string{"dev_a", "dev_z"}, nil, "dev_alpha", "dev_zeta")
}

func TestPermitted(t *testing.T) {
	cases := []struct {
		allow, deny []string
		want        bool
	}{
		{nil, nil, true},
		{[]string{"dev_"}, nil, true},
		{[]string{"other_"}, nil, false},
		{nil, []string{"dev_echo"}, false},
		{[]string{"dev_"}, []string{"dev_echo"}, false},
	}
	for _, c := range cases {
		if got := bus.Permitted("dev_echo", c.allow, c.deny); got != c.want {
			t.Errorf("Permitted(dev_echo, %v, %v) = %v", c.allow, c.deny, got)
		}
	}
}

func TestCallFailuresBecomeErrorResults(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "boom"}, func(context.Context, bus.DeviceCall) (string, error) {
			return "", errors.New("it broke")
		}).
		Add(bus.ToolDef{Name: "hang", Timeout: 20 * time.Millisecond}, func(c context.Context, _ bus.DeviceCall) (string, error) {
			<-c.Done()
			return "", c.Err()
		})
	b := mustBus(t, 4, d)
	cases := []struct {
		name string
		deny []string
		want string
	}{
		{"dev_nope", nil, "unknown tool"},
		{"dev_boom", []string{"dev_boom"}, "not permitted"},
		{"dev_boom", nil, "it broke"},
		{"dev_hang", nil, "timed out after"},
	}
	for _, c := range cases {
		res := b.Call(ctx, c.name, nil, "", nil, c.deny)
		if !res.IsError || !strings.Contains(res.Content, c.want) {
			t.Errorf("%s: got %+v, want an error containing %q", c.name, res, c.want)
		}
	}
}

func TestCallPassesArgsAndKey(t *testing.T) {
	var got bus.DeviceCall
	d := builtin.New("dev").Add(bus.ToolDef{Name: "rec"}, func(_ context.Context, c bus.DeviceCall) (string, error) {
		got = c
		return "ok", nil
	})
	b := mustBus(t, 4, d)
	res := b.Call(ctx, "dev_rec", json.RawMessage(`{"a":1}`), "k-1", nil, nil)
	if res.IsError || res.Content != "ok" {
		t.Fatalf("result %+v", res)
	}
	if got.Tool != "rec" || string(got.Args) != `{"a":1}` || got.Key != "k-1" {
		t.Fatalf("device saw %+v", got)
	}
}

func TestDeviceConcurrencyIsBounded(t *testing.T) {
	var inflight, peak atomic.Int32
	entered := make(chan struct{}, 6)
	release := make(chan struct{})
	d := builtin.New("dev").Add(bus.ToolDef{Name: "slow"}, func(context.Context, bus.DeviceCall) (string, error) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		inflight.Add(-1)
		return "ok", nil
	})
	b := mustBus(t, 2, d)
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Call(ctx, "dev_slow", nil, "", nil, nil)
		}()
	}
	<-entered
	<-entered
	time.Sleep(30 * time.Millisecond) // let a third call try to get in
	close(release)
	wg.Wait()
	if p := peak.Load(); p < 1 || p > 2 {
		t.Fatalf("peak concurrency %d, want 1..2", p)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency %d, want 2 (barrier guarantees overlap)", peak.Load())
	}
}

func TestTimeoutIsEnforcedByBus(t *testing.T) {
	d := builtin.New("dev").Add(bus.ToolDef{Name: "deaf", Timeout: 20 * time.Millisecond}, func(context.Context, bus.DeviceCall) (string, error) {
		time.Sleep(300 * time.Millisecond)
		return "late", nil
	})
	b := mustBus(t, 4, d)
	start := time.Now()
	res := b.Call(ctx, "dev_deaf", nil, "", nil, nil)
	if !res.IsError || !strings.Contains(res.Content, "timed out after") {
		t.Fatalf("got %+v", res)
	}
	if el := time.Since(start); el > 200*time.Millisecond {
		t.Fatalf("took %v", el)
	}
}

func TestCancelledCtxNeverReachesDevice(t *testing.T) {
	var n atomic.Int32
	d := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, func(context.Context, bus.DeviceCall) (string, error) {
		n.Add(1)
		return "ok", nil
	})
	b := mustBus(t, 4, d)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	for range 200 {
		res := b.Call(cctx, "dev_x", nil, "", nil, nil)
		if !res.IsError || !strings.Contains(res.Content, "cancelled before the tool ran") {
			t.Fatalf("got %+v", res)
		}
	}
	if n.Load() != 0 {
		t.Fatalf("device ran %d times", n.Load())
	}
}

func TestQueuedCallHonoursCancel(t *testing.T) {
	hold := make(chan struct{})
	entered := make(chan struct{})
	d := builtin.New("dev").Add(bus.ToolDef{Name: "hold"}, func(context.Context, bus.DeviceCall) (string, error) {
		entered <- struct{}{}
		<-hold
		return "ok", nil
	})
	b := mustBus(t, 1, d)
	done := make(chan struct{})
	go func() {
		b.Call(ctx, "dev_hold", nil, "", nil, nil)
		close(done)
	}()
	<-entered
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	res := b.Call(cctx, "dev_hold", nil, "", nil, nil)
	if !res.IsError || !strings.Contains(res.Content, "cancelled before the tool ran") || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("got %+v after %v", res, time.Since(start))
	}
	close(hold)
	<-done
}

func TestPanicIsIsolated(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "bad"}, func(context.Context, bus.DeviceCall) (string, error) { panic("kaboom") }).
		Add(bus.ToolDef{Name: "good"}, echo)
	b := mustBus(t, 1, d)
	res := b.Call(ctx, "dev_bad", nil, "", nil, nil)
	if !res.IsError || !strings.Contains(res.Content, "panicked") || !strings.Contains(res.Content, "kaboom") {
		t.Fatalf("got %+v", res)
	}
	done := make(chan task.ToolResult, 1)
	go func() { done <- b.Call(ctx, "dev_good", json.RawMessage(`1`), "", nil, nil) }()
	select {
	case r := <-done:
		if r.IsError {
			t.Fatalf("got %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("slot was not released after panic")
	}
}

func TestSlotIsPerDeviceNotPerTool(t *testing.T) {
	var inflight, peak atomic.Int32
	fn := func(context.Context, bus.DeviceCall) (string, error) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		inflight.Add(-1)
		return "ok", nil
	}
	b := mustBus(t, 1, builtin.New("dev").Add(bus.ToolDef{Name: "a"}, fn).Add(bus.ToolDef{Name: "b"}, fn))
	var wg sync.WaitGroup
	for _, n := range []string{"dev_a", "dev_b", "dev_a", "dev_b"} {
		wg.Add(1)
		go func() { defer wg.Done(); b.Call(ctx, n, nil, "", nil, nil) }()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatalf("peak %d, want 1", peak.Load())
	}

	// two separate devices under perDevice=1 run concurrently
	var inflight2 atomic.Int32
	bar := make(chan struct{})
	mk := func(name string) *builtin.Device {
		return builtin.New(name).Add(bus.ToolDef{Name: "t"}, func(context.Context, bus.DeviceCall) (string, error) {
			if inflight2.Add(1) == 2 {
				close(bar)
			}
			select {
			case <-bar:
			case <-time.After(time.Second):
				return "", errors.New("never overlapped")
			}
			return "ok", nil
		})
	}
	b2 := mustBus(t, 1, mk("d1"), mk("d2"))
	for _, n := range []string{"d1_t", "d2_t"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := b2.Call(ctx, n, nil, "", nil, nil); r.IsError {
				t.Errorf("%s: %+v", n, r)
			}
		}()
	}
	wg.Wait()
}

type closeRec struct {
	name   string
	closed *atomic.Int32
}

func (c closeRec) Name() string { return c.name }
func (c closeRec) Tools(context.Context) ([]bus.ToolDef, error) {
	return []bus.ToolDef{{Name: "x"}}, nil
}
func (c closeRec) Call(context.Context, bus.DeviceCall) (task.ToolResult, error) {
	return task.ToolResult{}, nil
}
func (c closeRec) Close() error { c.closed.Add(1); return nil }

func TestNewValidatesAndClosesOnError(t *testing.T) {
	if _, err := bus.New(ctx, 4, builtin.New("").Add(bus.ToolDef{Name: "x"}, echo)); err == nil {
		t.Fatal("empty device name accepted")
	}
	if _, err := bus.New(ctx, 4, builtin.New("dev").Add(bus.ToolDef{Name: ""}, echo)); err == nil {
		t.Fatal("empty tool name accepted")
	}
	var closed atomic.Int32
	dup := builtin.New("dev").Add(bus.ToolDef{Name: "x"}, echo)
	_, err := bus.New(ctx, 4, closeRec{"dev", &closed}, dup, closeRec{"zzz", &closed})
	if err == nil || !strings.Contains(err.Error(), `device "dev"`) || !strings.Contains(err.Error(), `tool "x"`) {
		t.Fatalf("err = %v", err)
	}
	if closed.Load() != 2 {
		t.Fatalf("closed %d devices, want 2", closed.Load())
	}
}

func TestSpecsSchemaIsCopied(t *testing.T) {
	d := builtin.New("dev").Add(bus.ToolDef{Name: "x", Schema: json.RawMessage(`{"type":"object"}`)}, echo)
	b := mustBus(t, 4, d)
	s := b.Specs(nil, nil)
	s[0].Schema[2] = 'X'
	if got := string(b.Specs(nil, nil)[0].Schema); got != `{"type":"object"}` {
		t.Fatalf("schema mutated: %s", got)
	}
}

func TestEffectDefaultsToSideEffect(t *testing.T) {
	d := builtin.New("dev").
		Add(bus.ToolDef{Name: "w"}, echo).
		Add(bus.ToolDef{Name: "r", Effect: bus.ReadOnly}, echo)
	b := mustBus(t, 4, d)
	if b.Effect("dev_w") != bus.SideEffect || b.Effect("dev_r") != bus.ReadOnly || b.Effect("dev_nope") != bus.SideEffect {
		t.Fatal("wrong effects")
	}
}
