// Command live runs neumann against a real OpenAI-compatible endpoint: six
// tasks on NEUMANN_CORES vCores (default 6), parallel tool calls, a slow tool with a steer arriving
// while it runs, and the event stream printed as it is committed. At the end
// it checks the phase-1 invariants against what really happened.
//
//	OPENAI_BASE_URL=https://host/v1 OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/live
//
// LLM_BASE_URL / LLM_API_KEY / LLM_MODEL are used when the OPENAI_ ones are unset.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

func env(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

// counting wraps an adapter to count the calls that really reached it.
type counting struct {
	model.Adapter
	calls atomic.Int64
}

func (c *counting) Call(ctx context.Context, req model.Request) (model.Response, error) {
	c.calls.Add(1)
	return c.Adapter.Call(ctx, req)
}

// Stream keeps the wrapper a model.Streamer: an adapter wrapper that forwards
// only Call silently turns streaming off.
func (c *counting) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	c.calls.Add(1)
	return c.Adapter.(model.Streamer).Stream(ctx, req, emit)
}

var weather = map[string]string{"paris": "18°C, light rain", "tokyo": "24°C, clear", "vienna": "15°C, windy", "chengdu": "22°C, overcast"}

func devices() []bus.Device {
	num := json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"},"b":{"type":"number"}},"required":["a","b"]}`)
	city := json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`)
	type ab struct{ A, B float64 }
	type c struct{ City string }
	calc := builtin.New("calc").
		Add(bus.ToolDef{Name: "add", Description: "Add two numbers.", Schema: num, Effect: bus.ReadOnly}, func(_ context.Context, call bus.DeviceCall) (string, error) {
			var v ab
			if err := json.Unmarshal(call.Args, &v); err != nil {
				return "", err
			}
			return fmt.Sprint(v.A + v.B), nil
		}).
		Add(bus.ToolDef{Name: "mul", Description: "Multiply two numbers.", Schema: num, Effect: bus.ReadOnly}, func(_ context.Context, call bus.DeviceCall) (string, error) {
			var v ab
			if err := json.Unmarshal(call.Args, &v); err != nil {
				return "", err
			}
			return fmt.Sprint(v.A * v.B), nil
		})
	info := builtin.New("info").
		Add(bus.ToolDef{Name: "weather", Description: "Current weather for a city.", Schema: city, Effect: bus.ReadOnly}, func(_ context.Context, call bus.DeviceCall) (string, error) {
			var v c
			if err := json.Unmarshal(call.Args, &v); err != nil {
				return "", err
			}
			if w, ok := weather[strings.ToLower(v.City)]; ok {
				return w, nil
			}
			return "", fmt.Errorf("no weather data for %q", v.City)
		}).
		Add(bus.ToolDef{Name: "population", Description: "Look up a city's population. Slow: takes a few seconds.", Schema: city, Effect: bus.ReadOnly}, func(ctx context.Context, call bus.DeviceCall) (string, error) {
			select {
			case <-time.After(4 * time.Second):
			case <-ctx.Done():
				return "", ctx.Err()
			}
			var v c
			_ = json.Unmarshal(call.Args, &v)
			return fmt.Sprintf("population of %s: about 2.0 million", v.City), nil
		})
	return []bus.Device{calc, info}
}

func main() {
	base, key, modelName := env("OPENAI_BASE_URL", "LLM_BASE_URL"), env("OPENAI_API_KEY", "LLM_API_KEY"), env("OPENAI_MODEL", "LLM_MODEL")
	if base == "" || key == "" || modelName == "" {
		log.Fatal("set OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL (or the LLM_ equivalents)")
	}
	adapter := &counting{Adapter: openai.New(base, key, modelName)}
	m, err := neumann.New(neumann.Config{
		Model:   adapter,
		Devices: devices(),
		Cores:   cores(),
		System:  "You are a precise assistant. Use the provided tools for arithmetic, weather and population; never compute or guess those yourself. Call independent tools in parallel. Answer in one or two sentences.",
		Logf:    log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("neumann live run: model %s, %d vCores\n\n", modelName, cores())

	inputs := []string{
		"What is (17*23) + (41*3)?",
		"What's the weather in Paris and in Tokyo?",
		"What is the population of Vienna?",
		"Say hello in five languages.",
		"Compute 123*456, then add 789 to the result.",
		"What's the weather in Chengdu?",
	}
	const steerText = "Also tell me the weather there."
	start := time.Now()
	names := map[string]string{}
	var ids []string
	for i, in := range inputs {
		id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: in})
		if err != nil {
			log.Fatal(err)
		}
		names[id] = fmt.Sprintf("T%d", i+1)
		ids = append(ids, id)
	}
	steerTarget := ids[2]

	// Streamed output: note when each step's first text arrives, to compare
	// with when the step's answer is committed.
	type stepKey struct {
		task string
		step int
	}
	var firstMu sync.Mutex
	firstText := map[stepKey]time.Duration{}
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	go func() {
		for d := range m.Deltas(dctx) {
			if d.Text == "" {
				continue
			}
			k := stepKey{d.TaskID, d.Step}
			firstMu.Lock()
			if _, seen := firstText[k]; !seen {
				firstText[k] = time.Since(start)
				fmt.Printf("%6.2fs  %-3s %-13s step %d: %q…\n", time.Since(start).Seconds(), names[d.TaskID], "first text", d.Step, clip(d.Text, 40))
			}
			firstMu.Unlock()
		}
	}()

	var steered sync.Once
	pending := len(ids)
	evCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for ev := range m.Events(evCtx, 0) {
		name := names[ev.TaskID]
		fmt.Printf("%6.2fs  %-3s %-13s %s\n", time.Since(start).Seconds(), name, ev.Kind, compact(ev.Data))
		if ev.TaskID == steerTarget && ev.Kind == proto.EvToolCall && strings.Contains(string(ev.Data), "population") {
			steered.Do(func() {
				go func() {
					time.Sleep(time.Second) // the slow tool is running now
					if err := m.Steer(ctx, steerTarget, steerText); err != nil {
						log.Printf("steer: %v", err)
						return
					}
					fmt.Printf("%6.2fs  %-3s %-13s %q\n", time.Since(start).Seconds(), name, "STEER SENT", steerText)
				}()
			})
		}
		if ev.Kind == proto.EvAnswer {
			t, _ := m.Load(ctx, ev.TaskID)
			firstMu.Lock()
			if ft, ok := firstText[stepKey{ev.TaskID, t.Used.Steps}]; ok {
				lead := time.Since(start) - ft
				fmt.Printf("%6.2fs  %-3s %-13s streaming showed the first text %dms before the answer was committed\n",
					time.Since(start).Seconds(), name, "stream lead", lead.Milliseconds())
			}
			firstMu.Unlock()
		}
		if ev.Terminal() {
			if pending--; pending == 0 {
				break
			}
		}
	}
	elapsed := time.Since(start)

	fmt.Println("\n== results ==")
	var used task.Usage
	steps, failures := 0, 0
	for _, id := range ids {
		t, err := m.Load(ctx, id)
		if err != nil {
			log.Fatal(err)
		}
		used.Add(t.Used)
		steps += t.Used.Steps
		calls, _ := m.Calls(ctx, id)
		fmt.Printf("%s %-5s steps=%d tools=%d tokens=%d/%d  %s\n", names[id], t.State, t.Used.Steps, len(calls),
			t.Used.PromptTokens, t.Used.CompletionTokens+t.Used.ReasoningTokens, oneLine(t.LastAnswer()))
		for _, c := range calls {
			if c.Status != mem.CallDone || c.Result == nil {
				fmt.Printf("   ! call %s %s is %s\n", c.CallID, c.Tool, c.Status)
				failures++
			}
		}
		if t.State != task.Done {
			failures++
		}
	}

	events, _ := m.EventLog(ctx, 0, 0)
	modelSteps, dropped := 0, 0
	steerSeen := false
	for _, ev := range events {
		if ev.Kind == proto.EvStep && strings.Contains(string(ev.Data), `"pc":"model"`) {
			modelSteps++
		}
		if ev.Kind == proto.EvSteerDropped {
			dropped++
		}
	}
	if t, _ := m.Load(ctx, steerTarget); t != nil {
		for _, msg := range t.Regs {
			if msg.Role == task.RoleUser && msg.TextContent() == neumann.DefaultSteerFormat(steerText) {
				steerSeen = true
			}
		}
	}

	fmt.Println("\n== checks ==")
	check := func(ok bool, what string) {
		mark := "ok  "
		if !ok {
			mark = "FAIL"
			failures++
		}
		fmt.Printf("[%s] %s\n", mark, what)
	}
	check(int64(modelSteps) == adapter.calls.Load(), fmt.Sprintf("model calls (%d) == model-step events (%d)", adapter.calls.Load(), modelSteps))
	check(steps == modelSteps, fmt.Sprintf("sum of task steps (%d) == model-step events", steps))
	check(steerSeen != (dropped > 0), fmt.Sprintf("steer to T3 delivered=%v, dropped events=%d (exactly one)", steerSeen, dropped))
	fmt.Printf("\nwall time %.1fs; confirmed tokens: prompt %d (cached %d), completion %d, reasoning %d\n",
		elapsed.Seconds(), used.PromptTokens, used.CachedTokens, used.CompletionTokens, used.ReasoningTokens)

	stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
	defer stopCancel()
	if err := m.Stop(stopCtx); err != nil {
		log.Printf("stop: %v", err)
	}
	if failures > 0 {
		os.Exit(1)
	}
}

func cores() int {
	if n, err := strconv.Atoi(os.Getenv("NEUMANN_CORES")); err == nil && n > 0 {
		return n
	}
	return 6
}

func compact(b json.RawMessage) string {
	s := oneLine(string(b))
	if len(s) > 110 {
		s = s[:110] + "…"
	}
	return s
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func clip(s string, n int) string {
	r := []rune(oneLine(s))
	if len(r) > n {
		return string(r[:n])
	}
	return string(r)
}
