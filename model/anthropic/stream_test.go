package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// sse is one server-sent event.
type sse struct{ event, data string }

// sseServer streams the given events and records the request body.
func sseServer(t *testing.T, events ...sse) (*httptest.Server, *map[string]any) {
	t.Helper()
	var body map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, e := range events {
			io.WriteString(w, "event: "+e.event+"\ndata: "+e.data+"\n\n")
			fl.Flush()
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &body
}

func stream(t *testing.T, a *Adapter) (model.Response, []model.Delta, error) {
	t.Helper()
	var deltas []model.Delta
	resp, err := a.Stream(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}},
		func(d model.Delta) { deltas = append(deltas, d) })
	return resp, deltas, err
}

func start(usage string) sse {
	return sse{"message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"usage":` + usage + `}}`}
}

func blockStart(i, block string) sse {
	return sse{"content_block_start", `{"type":"content_block_start","index":` + i + `,"content_block":` + block + `}`}
}

func delta(i, d string) sse {
	return sse{"content_block_delta", `{"type":"content_block_delta","index":` + i + `,"delta":` + d + `}`}
}

func blockStop(i string) sse {
	return sse{"content_block_stop", `{"type":"content_block_stop","index":` + i + `}`}
}

func msgDelta(stop, usage string) sse {
	return sse{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"` + stop + `","stop_sequence":null},"usage":` + usage + `}`}
}

var (
	ping = sse{"ping", `{"type":"ping"}`}
	stop = sse{"message_stop", `{"type":"message_stop"}`}
)

func TestStreamText(t *testing.T) {
	ts, body := sseServer(t,
		start(`{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":5,"output_tokens":1}`),
		blockStart("0", `{"type":"text","text":""}`),
		ping,
		delta("0", `{"type":"text_delta","text":"Hello"}`),
		delta("0", `{"type":"text_delta","text":", world"}`),
		blockStop("0"),
		msgDelta("end_turn", `{"output_tokens":7}`),
		stop,
	)
	resp, deltas, err := stream(t, New(ts.URL+"/v1", "k", "claude-x"))
	if err != nil {
		t.Fatal(err)
	}
	if (*body)["stream"] != true || (*body)["max_tokens"] != float64(8192) {
		t.Fatalf("request %v", *body)
	}
	if resp.Message.TextContent() != "Hello, world" || resp.FinishReason != "stop" {
		t.Fatalf("response %+v", resp)
	}
	if u := resp.Usage; u.PromptTokens != 15 || u.CachedTokens != 5 || u.CompletionTokens != 7 {
		t.Fatalf("usage %+v", u)
	}
	want := []model.Delta{{Text: "Hello"}, {Text: ", world"}}
	if len(deltas) != len(want) || deltas[0] != want[0] || deltas[1] != want[1] {
		t.Fatalf("deltas %+v", deltas)
	}
}

func TestStreamThinkingWithSignature(t *testing.T) {
	ts, _ := sseServer(t,
		start(`{"input_tokens":3,"output_tokens":1}`),
		blockStart("0", `{"type":"thinking","thinking":"","signature":""}`),
		delta("0", `{"type":"thinking_delta","thinking":"Let me "}`),
		delta("0", `{"type":"thinking_delta","thinking":"think <here>."}`),
		delta("0", `{"type":"signature_delta","signature":"EqQBCkgI"}`),
		delta("0", `{"type":"signature_delta","signature":"ARABGAIiQ=="}`),
		blockStop("0"),
		blockStart("1", `{"type":"redacted_thinking","data":"EmwKAhgBEgy3va3pzix"}`),
		blockStop("1"),
		blockStart("2", `{"type":"text","text":""}`),
		delta("2", `{"type":"text_delta","text":"42"}`),
		blockStop("2"),
		msgDelta("end_turn", `{"output_tokens":30}`),
		stop,
	)
	a := New(ts.URL+"/v1", "k", "claude-x")
	resp, deltas, err := stream(t, a)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 3 || deltas[0].Reasoning != "Let me " || deltas[1].Reasoning != "think <here>." || deltas[2].Text != "42" {
		t.Fatalf("deltas %+v", deltas)
	}
	p := resp.Message.Parts
	if len(p) != 3 {
		t.Fatalf("parts %+v", p)
	}
	// The assembled block is exactly what the non-streaming API returns.
	if p[0].Kind != task.PartReasoning || p[0].Text != "Let me think <here>." || p[0].Provider != a.Provider() ||
		string(p[0].Opaque) != `{"type":"thinking","thinking":"Let me think <here>.","signature":"EqQBCkgIARABGAIiQ=="}` {
		t.Fatalf("thinking %+v (%s)", p[0], p[0].Opaque)
	}
	if p[1].Kind != task.PartReasoning || p[1].Text != "" || string(p[1].Opaque) != `{"type":"redacted_thinking","data":"EmwKAhgBEgy3va3pzix"}` {
		t.Fatalf("redacted %+v (%s)", p[1], p[1].Opaque)
	}
	if p[2].Kind != task.PartText || p[2].Text != "42" {
		t.Fatalf("text %+v", p[2])
	}

	// Replayed to the same provider, both blocks go back byte for byte.
	s := newServer(t, 200, textReply)
	b := New(s.URL+"/v1", "k", "claude-x")
	call(t, b, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi"), resp.Message, task.Text(task.RoleUser, "and?"), task.Text(task.RoleAssistant, "."), task.Text(task.RoleUser, "ok")}})
	asst := s.messages(t, 0)[1].Content
	if len(asst) != 3 || !bytes.Equal(asst[0], p[0].Opaque) || !bytes.Equal(asst[1], p[1].Opaque) {
		t.Fatalf("replayed %s", asst)
	}
}

func TestStreamToolUseInputAcrossDeltas(t *testing.T) {
	ts, _ := sseServer(t,
		start(`{"input_tokens":50,"cache_read_input_tokens":40,"output_tokens":1}`),
		blockStart("0", `{"type":"text","text":""}`),
		delta("0", `{"type":"text_delta","text":"Adding."}`),
		blockStop("0"),
		blockStart("1", `{"type":"tool_use","id":"toolu_01","name":"calc_add","input":{}}`),
		delta("1", `{"type":"input_json_delta","partial_json":""}`),
		delta("1", `{"type":"input_json_delta","partial_json":"{\"a\": 1"}`),
		ping,
		delta("1", `{"type":"input_json_delta","partial_json":", \"b\": 2}"}`),
		blockStop("1"),
		blockStart("2", `{"type":"tool_use","id":"toolu_02","name":"clock_now","input":{}}`),
		blockStop("2"),
		msgDelta("tool_use", `{"output_tokens":25}`),
		stop,
	)
	a := New(ts.URL+"/v1", "k", "claude-x")
	resp, deltas, err := stream(t, a)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 1 || deltas[0].Text != "Adding." {
		t.Fatalf("tool input is not a text delta: %+v", deltas)
	}
	calls := resp.Message.ToolCalls()
	if len(calls) != 2 {
		t.Fatalf("calls %+v", calls)
	}
	if c := calls[0]; c.ProviderCallID != "toolu_01" || c.Name != "calc_add" || c.Provider != a.Provider() || c.CallID != "" {
		t.Fatalf("call 0 %+v", c)
	}
	var args map[string]any
	if err := json.Unmarshal(calls[0].Args, &args); err != nil || args["a"] != float64(1) || args["b"] != float64(2) {
		t.Fatalf("args %s (%v)", calls[0].Args, err)
	}
	if string(calls[0].Opaque) != `{"type":"tool_use","id":"toolu_01","name":"calc_add","input":{"a":1,"b":2}}` {
		t.Fatalf("opaque %s", calls[0].Opaque)
	}
	if string(calls[1].Args) != `{}` || string(calls[1].Opaque) != `{"type":"tool_use","id":"toolu_02","name":"clock_now","input":{}}` {
		t.Fatalf("call 1 %+v (%s)", calls[1], calls[1].Opaque)
	}
	if resp.FinishReason != "tool_calls" || resp.Usage.PromptTokens != 90 || resp.Usage.CachedTokens != 40 || resp.Usage.CompletionTokens != 25 {
		t.Fatalf("response %+v", resp)
	}
}

func TestStreamErrors(t *testing.T) {
	ts, _ := sseServer(t,
		start(`{"input_tokens":3,"output_tokens":1}`),
		sse{"error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`},
	)
	if _, _, err := stream(t, New(ts.URL+"/v1", "k", "m")); err == nil || !strings.Contains(err.Error(), "overloaded_error") || !strings.Contains(err.Error(), "Overloaded") {
		t.Fatalf("in-stream error: %v", err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(529)
		io.WriteString(w, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
	}))
	defer bad.Close()
	if _, _, err := stream(t, New(bad.URL+"/v1", "sk-ant-secret", "m")); err == nil || !strings.Contains(err.Error(), "529") || strings.Contains(err.Error(), "sk-ant-secret") {
		t.Fatalf("status error: %v", err)
	}

	cut, _ := sseServer(t,
		start(`{"input_tokens":3,"output_tokens":1}`),
		blockStart("0", `{"type":"text","text":""}`),
		delta("0", `{"type":"text_delta","text":"half"}`),
	)
	if _, _, err := stream(t, New(cut.URL+"/v1", "k", "m")); err == nil {
		t.Fatal("a stream cut mid-block was accepted")
	}

	noStop, _ := sseServer(t,
		start(`{"input_tokens":3,"output_tokens":1}`),
		blockStart("0", `{"type":"text","text":""}`),
		delta("0", `{"type":"text_delta","text":"all"}`),
		blockStop("0"),
		msgDelta("end_turn", `{"output_tokens":2}`),
	)
	if _, _, err := stream(t, New(noStop.URL+"/v1", "k", "m")); err == nil {
		t.Fatal("a stream that ends before message_stop was accepted")
	}
}

// A turn that ends with thinking alone (end_turn, no text, no tool use) is a
// thought that stopped without an answer (design §6.4): returned with its
// thinking blocks, whole or streamed, for the machine to judge (it asks
// again without thinking, Caps.ThinkOff), not as a transient error. A turn
// with no blocks at all is still one (TestEmptyAnswerIsAnError), and so is
// one whose only block is of a type that is not thinking.
func TestAThoughtThatStops(t *testing.T) {
	s := newServer(t, 200,
		`{"content":[{"type":"thinking","thinking":"The columns are order_id and date.","signature":"sig"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":20}}`,
		`{"content":[{"type":"server_tool_use","id":"x"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":20}}`)
	a := New(s.URL+"/v1", "k", "claude-x")
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}
	resp := call(t, a, req)
	if p := resp.Message.Parts; resp.FinishReason != "stop" || len(p) != 1 || p[0].Kind != task.PartReasoning || p[0].Text != "The columns are order_id and date." {
		t.Fatalf("whole: %+v", resp)
	}
	if _, err := a.Call(context.Background(), req); err == nil {
		t.Fatal("a turn with no text, no tool use and no thinking was accepted")
	}

	ts, _ := sseServer(t,
		start(`{"input_tokens":3,"output_tokens":1}`),
		blockStart("0", `{"type":"thinking","thinking":"","signature":""}`),
		delta("0", `{"type":"thinking_delta","thinking":"The columns are order_id and date."}`),
		delta("0", `{"type":"signature_delta","signature":"sig"}`),
		blockStop("0"),
		msgDelta("end_turn", `{"output_tokens":20}`),
		stop,
	)
	resp, deltas, err := stream(t, New(ts.URL+"/v1", "k", "claude-x"))
	if p := resp.Message.Parts; err != nil || resp.FinishReason != "stop" || len(p) != 1 || p[0].Kind != task.PartReasoning || len(deltas) != 1 || deltas[0].Reasoning == "" {
		t.Fatalf("streamed: %+v, %+v, %v", resp, deltas, err)
	}
}
