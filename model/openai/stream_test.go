package openai

import (
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

// sseServer streams the given data lines as server-sent events and records
// the request body it received.
func sseServer(t *testing.T, lines ...string) (*httptest.Server, *map[string]any) {
	t.Helper()
	var body map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, l := range lines {
			io.WriteString(w, l+"\n\n")
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

func TestStreamText(t *testing.T) {
	ts, body := sseServer(t,
		`: keep-alive`,
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"thinking…"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"Hello"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":", world"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":3,"completion_tokens_details":{"reasoning_tokens":40}}}`,
		`data: [DONE]`,
	)
	resp, deltas, err := stream(t, New(ts.URL+"/v1", "k", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if (*body)["stream"] != true || (*body)["stream_options"].(map[string]any)["include_usage"] != true {
		t.Fatalf("request %v", *body)
	}
	if resp.Message.TextContent() != "Hello, world" || resp.FinishReason != "stop" {
		t.Fatalf("response %+v", resp)
	}
	if resp.Usage.PromptTokens != 12 || resp.Usage.CompletionTokens != 3 || resp.Usage.ReasoningTokens != 40 {
		t.Fatalf("usage %+v", resp.Usage)
	}
	want := []model.Delta{{Reasoning: "thinking…"}, {Text: "Hello"}, {Text: ", world"}}
	if len(deltas) != len(want) {
		t.Fatalf("deltas %+v", deltas)
	}
	for i := range want {
		if deltas[i] != want[i] {
			t.Fatalf("delta %d = %+v, want %+v", i, deltas[i], want[i])
		}
	}
}

func TestStreamAssemblesToolCalls(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"calc_add","arguments":"{\"a\":"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"info_weather","arguments":"{\"city\":\"Paris\"}"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1,\"b\":2}"},"extra_content":{"google":{"thought_signature":"sig"}}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":5,"completion_tokens":9}}`,
		`data: [DONE]`,
	)
	a := New(ts.URL+"/v1", "k", "m")
	resp, deltas, err := stream(t, a)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 0 {
		t.Fatalf("tool calls are not text deltas: %+v", deltas)
	}
	calls := resp.Message.ToolCalls()
	if len(calls) != 2 {
		t.Fatalf("calls %+v", calls)
	}
	if calls[0].ProviderCallID != "call_a" || calls[0].Name != "calc_add" || string(calls[0].Args) != `{"a":1,"b":2}` {
		t.Fatalf("call 0 %+v", calls[0])
	}
	if calls[1].ProviderCallID != "call_b" || string(calls[1].Args) != `{"city":"Paris"}` {
		t.Fatalf("call 1 %+v", calls[1])
	}
	var op map[string]any
	if err := json.Unmarshal(calls[0].Opaque, &op); err != nil || op["extra_content"] == nil || op["id"] != "call_a" ||
		op["function"].(map[string]any)["arguments"] != `{"a":1,"b":2}` {
		t.Fatalf("opaque %s (%v)", calls[0].Opaque, err)
	}
	if calls[0].Provider != a.Provider() || resp.FinishReason != "tool_calls" || resp.Usage.CompletionTokens != 9 {
		t.Fatalf("response %+v", resp)
	}
}

func TestStreamErrors(t *testing.T) {
	ts, _ := sseServer(t, `data: {"error":{"message":"quota exceeded","code":429}}`)
	if _, _, err := stream(t, New(ts.URL+"/v1", "k", "m")); err == nil || !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("in-stream error: %v", err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"message":"overloaded"}}`)
	}))
	defer bad.Close()
	if _, _, err := stream(t, New(bad.URL+"/v1", "sk-secret", "m")); err == nil || !strings.Contains(err.Error(), "503") || strings.Contains(err.Error(), "sk-secret") {
		t.Fatalf("status error: %v", err)
	}

	cut, _ := sseServer(t, `data: {"choices":[{"index":0,"delta":{"content":"half"}}]}`)
	if _, _, err := stream(t, New(cut.URL+"/v1", "k", "m")); err == nil {
		t.Fatal("a stream that ends without finish_reason or [DONE] was accepted")
	}
}
