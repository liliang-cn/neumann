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
		{"dev_hang", nil, "deadline exceeded"},
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
	d := builtin.New("dev").Add(bus.ToolDef{Name: "slow"}, func(context.Context, bus.DeviceCall) (string, error) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
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
	wg.Wait()
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency %d, want 2", peak.Load())
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
