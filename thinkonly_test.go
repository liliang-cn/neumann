package neumann_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// The task behind these tests failed live (2026-10-17, Ollama, qwen3.5:4b):
// asked for the columns of a CSV, the model read the file and then put the
// whole answer in its thinking and stopped, with empty content and no tool
// call. The adapter reported that as a transient error and the machine sent
// the same request eight times, the model doing the same each time, until
// the task failed model_error, the group's breaker open on the way.
const (
	csvSystem = "You are nm, a careful assistant that works on the files of a local workspace through tools.\n" +
		"- Every path is relative to the workspace root. Look at files with the tools instead of guessing; quote what you found.\n" +
		"- Finish with a short, direct answer that gives the exact values, names and file:line locations asked for."
	csvQuestion = "What are the column names of data/sales.csv?"
	csvHead     = "data/sales.csv: lines 1-3 of 321\n  1| order_id,date,region,product,units,unit_price,revenue\n" +
		"  2| O-10001,2026-01-02,South,Sprocket,23,3.40,78.20\n  3| O-10002,2026-01-02,West,Gadget,10,39.99,399.90\n"
	csvThought = "The user wants the column names of data/sales.csv. The first line of the file is the header: " +
		"order_id,date,region,product,units,unit_price,revenue. They are: 1. order_id 2. date 3. region 4. product 5. units 6. unit_price 7. revenue"
	csvAnswer = "The columns of data/sales.csv (line 1) are order_id, date, region, product, units, unit_price and revenue."
)

func fsDev() bus.Device {
	return builtin.New("fs").Add(bus.ToolDef{
		Name:        "fs_read",
		Description: "Read a text file of the workspace with line numbers.",
		Schema:      json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
	}, func(context.Context, bus.DeviceCall) (string, error) { return csvHead, nil })
}

// turn is one answer of the fake Ollama: reasoning in the reasoning field
// (or, with tags, in content between <think> and </think>), content, a call
// of fs_read, and the finish reason.
type turn struct {
	reasoning, content string
	tags, call         bool
	finish             string
}

// ollama is an OpenAI-compatible endpoint that answers each request with
// what reply makes of its body (the n-th request, from 0), streamed when the
// request asks for it, as Ollama does.
type ollama struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []map[string]any
}

func newOllama(t *testing.T, reply func(body map[string]any, n int) turn) *ollama {
	t.Helper()
	o := &ollama{}
	o.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			return // the adapter warming up its connection
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(b, &body); err != nil {
			t.Errorf("request is not JSON: %v", err)
		}
		o.mu.Lock()
		n := len(o.bodies)
		o.bodies = append(o.bodies, body)
		o.mu.Unlock()
		tn := reply(body, n)
		content, reasoning := tn.content, tn.reasoning
		if tn.tags && reasoning != "" {
			content, reasoning = "<think>\n"+reasoning+"\n</think>\n\n"+content, ""
		}
		var calls []any
		if tn.call {
			calls = []any{map[string]any{"index": 0, "id": "call_fz6fmlg7", "type": "function",
				"function": map[string]any{"name": "fs_read", "arguments": `{"path":"data/sales.csv"}`}}}
		}
		msg := map[string]any{"role": "assistant", "content": content}
		if reasoning != "" {
			msg["reasoning"] = reasoning
		}
		if calls != nil {
			msg["tool_calls"] = calls
		}
		usage := map[string]any{"prompt_tokens": 900, "completion_tokens": 120}
		if body["stream"] != true {
			json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": msg, "finish_reason": tn.finish}}, "usage": usage})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		send := func(v any) {
			b, _ := json.Marshal(v)
			io.WriteString(w, "data: "+string(b)+"\n\n")
		}
		delta := func(d map[string]any) { send(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": d}}}) }
		if reasoning != "" { // in two pieces, as a stream would
			delta(map[string]any{"role": "assistant", "content": "", "reasoning": reasoning[:len(reasoning)/2]})
			delta(map[string]any{"content": "", "reasoning": reasoning[len(reasoning)/2:]})
		}
		for i := 0; i < len(content); i += 7 {
			delta(map[string]any{"content": content[i:min(i+7, len(content))]})
		}
		if calls != nil {
			delta(map[string]any{"tool_calls": calls})
		}
		send(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": ""}, "finish_reason": tn.finish}}})
		send(map[string]any{"choices": []any{}, "usage": usage})
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(o.Close)
	return o
}

func (o *ollama) requests() []map[string]any {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]map[string]any(nil), o.bodies...)
}

// hasToolResult says the request carries the result of the file read.
func hasToolResult(body map[string]any) bool {
	msgs, _ := body["messages"].([]any)
	for _, m := range msgs {
		if mm, _ := m.(map[string]any); mm["role"] == "tool" {
			return true
		}
	}
	return false
}

// lastRole is the role of the request's last message.
func lastRole(body map[string]any) any {
	msgs, _ := body["messages"].([]any)
	if len(msgs) == 0 {
		return nil
	}
	return msgs[len(msgs)-1].(map[string]any)["role"]
}

// plainCalls hides the adapter's Stream: the machine makes plain calls.
type plainCalls struct{ a *openai.Adapter }

func (c plainCalls) Provider() string { return c.a.Provider() }
func (c plainCalls) Caps() model.Caps { return c.a.Caps() }
func (c plainCalls) MaxOut() int      { return c.a.MaxOut() }
func (c plainCalls) Call(ctx context.Context, req model.Request) (model.Response, error) {
	return c.a.Call(ctx, req)
}

// localAdapter is the adapter as -local sets it up, at o.
func localAdapter(o *ollama) *openai.Adapter {
	a := openai.New(o.URL+"/v1", "ollama", "qwen3.5:4b")
	a.Parallel, a.Prefill, a.ThinkOff = 1, true, "none"
	a.ThinkTags = [2]string{"<think>", "</think>"}
	a.MaxTokens = 8192
	return a
}

// A fresh call that thinks and stops without an answer (design §6.4) is not
// a transient error: the turn is asked again with thinking off at once
// (reasoning_effort "none", no thought carried over), and the task ends with
// the answer. One model_retry ("thought", no_think) and no failure the group
// would count. The same whether the backend parses the thinking out
// (reasoning field) or leaves it in content between the think tags, and
// whether the machine streams or not. A turn cut off at its cap while
// thinking still follows §13 (class "truncated").
func TestAFreshThoughtThatStopsIsRedoneWithoutThinking(t *testing.T) {
	for _, c := range []struct {
		name   string
		tags   bool
		finish string
		class  string
	}{
		{"reasoning field", false, "stop", "thought"},
		{"think tags", true, "stop", "thought"},
		{"cut off while thinking", false, "length", "truncated"},
	} {
		for _, streaming := range []bool{true, false} {
			name := c.name + "/call"
			if streaming {
				name = c.name + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				o := newOllama(t, func(body map[string]any, n int) turn {
					switch {
					case body["reasoning_effort"] == "none":
						return turn{content: csvAnswer, finish: "stop"}
					case hasToolResult(body):
						return turn{reasoning: csvThought, tags: c.tags, finish: c.finish}
					}
					return turn{reasoning: "I should read the file.", call: true, finish: "tool_calls"}
				})
				var ad model.Adapter = localAdapter(o)
				if !streaming {
					ad = plainCalls{localAdapter(o)}
				}
				m := startWith(t, neumann.Config{Model: ad, System: csvSystem, Devices: []bus.Device{fsDev()}})
				id := submit(t, m, proto.Instruction{Input: csvQuestion})
				got := waitDone(t, m, id)
				if got.State != task.Done || got.LastAnswer() != csvAnswer || got.NoThink || got.Retries != 0 || got.Truncations != 0 {
					t.Fatalf("ended %s %q: %+v", got.State, got.LastAnswer(), got)
				}
				reqs := o.requests()
				if len(reqs) != 3 {
					t.Fatalf("%d requests", len(reqs))
				}
				for i, r := range reqs {
					if _, ok := r["reasoning_effort"]; ok != (i == 2) {
						t.Fatalf("request %d: reasoning_effort %v", i, r["reasoning_effort"])
					}
					if (r["stream"] == true) != streaming {
						t.Fatalf("request %d: stream %v", i, r["stream"])
					}
				}
				if lastRole(reqs[2]) != "tool" {
					t.Fatalf("the redo ends with a %v message, not the tool result", lastRole(reqs[2]))
				}
				d := one(t, eventData(t, m, id, proto.EvModelRetry), proto.EvModelRetry)
				if d["class"] != c.class || d["no_think"] != true || d["wait"] != "none" {
					t.Fatalf("model_retry %v", d)
				}
				if got.Used.Steps != 3 {
					t.Fatalf("%d steps for 3 answered calls", got.Used.Steps)
				}
				// The thought is never an answer or a turn of the task.
				for _, msg := range got.Regs {
					if strings.Contains(msg.TextContent(), "They are: 1.") || strings.Contains(msg.TextContent(), "<think>") {
						t.Fatalf("the thought got into the registers: %+v", msg)
					}
				}
			})
		}
	}
}

// thoughtOnly is a turn that thought and stopped: reasoning, no text, no
// tool call, finish "stop".
func thoughtOnly(reasoning string) model.Response {
	return model.Response{
		Message:      task.Message{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartReasoning, Text: reasoning, Provider: "sim"}}},
		FinishReason: "stop",
		Usage:        task.Usage{PromptTokens: 10, CompletionTokens: 40},
	}
}

// The redo with thinking off must not loop. Coming back empty (the adapter's
// transient error) it is retried as any transient failure, thinking still
// off, up to the retry limit; coming back as a thought again (a backend that
// ignores the switch) it is asked again as it was, counted in Retries, and
// the task ends model_error at the limit, saying why.
func TestARedoWithoutThinkingThatStaysEmptyIsBounded(t *testing.T) {
	for _, c := range []struct {
		name    string
		redo    func() (model.Response, error)
		classes []string
		reason  string
	}{
		{"empty", func() (model.Response, error) { return model.Response{}, emptyContinuation() }, []string{"thought", "transient", "transient"}, "neither text nor tool calls"},
		{"a thought again", func() (model.Response, error) { return thoughtOnly(csvThought), nil }, []string{"thought", "thought", "thought"}, "stopped after thinking"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sm := &sim.Model{Info: &model.Caps{Tools: true, ThinkOff: true}, Fn: func(req model.Request) (model.Response, error) {
				if req.NoThink {
					return c.redo()
				}
				return thoughtOnly(csvThought), nil
			}}
			m := startWith(t, neumann.Config{Model: sm, MaxModelRetries: 3, BackoffJitter: func(time.Duration) time.Duration { return 0 }})
			id := submit(t, m, proto.Instruction{Input: csvQuestion})
			got := waitDone(t, m, id)
			if got.State != task.Failed || got.StopReason != "model_error" || !got.NoThink {
				t.Fatalf("ended %s %q: %+v", got.State, got.StopReason, got)
			}
			if f := failedEvent(t, m, id); !strings.Contains(f["error"], c.reason) || !strings.Contains(f["error"], "3 consecutive failures") {
				t.Fatalf("failed %v", f)
			}
			reqs := sm.Requests()
			if len(reqs) != 4 || reqs[0].NoThink {
				t.Fatalf("%d requests", len(reqs))
			}
			for _, r := range reqs[1:] {
				if !r.NoThink || sim.PrefillThink(r) != "" {
					t.Fatalf("a redo went out with thinking, or carrying the thought")
				}
			}
			var classes []string
			for _, r := range retries(t, m, id) {
				classes = append(classes, r.Class)
			}
			if strings.Join(classes, ",") != strings.Join(c.classes, ",") {
				t.Fatalf("model_retry classes %v, want %v", classes, c.classes)
			}
		})
	}
}

// Without a way to turn thinking off, a thought that stops is asked again as
// it was, at once (the provider answered; a backoff would only wait) and
// without a larger cap (it was not cut off). Each one counts in Retries, so
// a model that keeps doing it ends the task at the retry limit; one that
// answers in time finishes it, Retries back to 0. The group saw answers:
// its breaker (five failures) stays closed, so the other tasks on the
// backend are not stalled, as they were when this was a transient error.
func TestAThoughtThatStopsWithoutThinkOff(t *testing.T) {
	for _, c := range []struct {
		name    string
		answers int // the call that answers, from 1; 0: none does
	}{{"answered in time", 8}, {"never answered", 0}} {
		t.Run(c.name, func(t *testing.T) {
			var calls atomic.Int32
			sm := &sim.Model{Info: &model.Caps{Tools: true}, Fn: func(req model.Request) (model.Response, error) {
				if n := int(calls.Add(1)); n == c.answers {
					return sim.Reply(csvAnswer), nil
				}
				return thoughtOnly(csvThought), nil
			}}
			m := startWith(t, neumann.Config{Model: sm})
			id := submit(t, m, proto.Instruction{Input: csvQuestion})
			got := waitDone(t, m, id)
			if c.answers > 0 {
				if got.State != task.Done || got.LastAnswer() != csvAnswer || got.Retries != 0 {
					t.Fatalf("ended %s %q: %+v", got.State, got.LastAnswer(), got)
				}
			} else if got.State != task.Failed || got.StopReason != "model_error" {
				t.Fatalf("ended %s %q: %+v", got.State, got.StopReason, got)
			}
			reqs := sm.Requests()
			if len(reqs) != 8 {
				t.Fatalf("%d requests", len(reqs))
			}
			for _, r := range reqs {
				if r.NoThink || r.MaxOut != 0 {
					t.Fatalf("a redo went out with NoThink %v, cap %d", r.NoThink, r.MaxOut)
				}
			}
			rs := retries(t, m, id)
			if len(rs) != 7 {
				t.Fatalf("%d model_retry", len(rs))
			}
			for i, r := range rs {
				if r.Class != "thought" || r.Attempt != i+1 || r.Wait != "none" || r.DelayMS != 0 {
					t.Fatalf("model_retry %d: %+v", i, r)
				}
			}
			if d := machineEvents(t, m, proto.EvGroupDown); len(d) != 0 {
				t.Fatalf("group_down %v", d)
			}
			if got.Used.Steps != 8 {
				t.Fatalf("%d steps for 8 answered calls", got.Used.Steps)
			}
		})
	}
}

// A turn with nothing at all, no thinking either, stays a transient error
// (design §11): backed off, counted, and the task ends model_error at the
// retry limit; thinking is not turned off for it.
func TestATrulyEmptyTurnStaysTransient(t *testing.T) {
	o := newOllama(t, func(map[string]any, int) turn { return turn{finish: "stop"} })
	m := startWith(t, neumann.Config{Model: localAdapter(o), MaxModelRetries: 3, BackoffJitter: func(time.Duration) time.Duration { return 0 }})
	id := submit(t, m, proto.Instruction{Input: csvQuestion})
	got := waitDone(t, m, id)
	if got.State != task.Failed || got.StopReason != "model_error" || got.NoThink {
		t.Fatalf("ended %s %q: %+v", got.State, got.StopReason, got)
	}
	if n := len(o.requests()); n != 3 {
		t.Fatalf("%d requests", n)
	}
	for _, r := range o.requests() {
		if _, ok := r["reasoning_effort"]; ok {
			t.Fatalf("thinking was turned off for an empty turn")
		}
	}
	rs := retries(t, m, id)
	if len(rs) != 2 || rs[0].Class != "transient" || rs[0].Wait != "backoff" || rs[1].Attempt != 2 {
		t.Fatalf("model_retry %+v", rs)
	}
}

// A compaction answered with thinking alone has no summary to put in place
// of the span: a failed call, counted on from the failures before it, so
// that a model that keeps doing it ends the task at the retry limit (it was
// counted from 0 again after each answered call, and never got there).
func TestAThoughtForASummaryIsBounded(t *testing.T) {
	r := &rounder{rounds: 20, size: 400}
	sm := &sim.Model{Info: &smallWindow, Fn: func(req model.Request) (model.Response, error) {
		if sim.IsCompaction(req) {
			return thoughtOnly("Let me summarise what happened so far."), nil
		}
		return r.fn(req)
	}}
	m := startWith(t, neumann.Config{Model: sm, Devices: []bus.Device{echoDev()}, MaxModelRetries: 3, BackoffJitter: func(time.Duration) time.Duration { return 0 }})
	id := submit(t, m, proto.Instruction{Input: "go"})
	got := waitDone(t, m, id)
	if got.State != task.Failed || got.StopReason != "model_error" {
		t.Fatalf("ended %s %q", got.State, got.StopReason)
	}
	rs := retries(t, m, id)
	if len(rs) != 2 || rs[0].Attempt != 1 || rs[1].Attempt != 2 {
		t.Fatalf("model_retry %+v", rs)
	}
}
