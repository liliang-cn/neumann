package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// server answers every request with the next canned body and records the
// request bodies it received.
type server struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []map[string]any
	auth   []string
}

func newServer(t *testing.T, status int, replies ...string) *server {
	t.Helper()
	s := &server{}
	n := 0
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(b, &body); err != nil {
			t.Errorf("request is not JSON: %v", err)
		}
		s.mu.Lock()
		s.bodies = append(s.bodies, body)
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		reply := replies[min(n, len(replies)-1)]
		n++
		s.mu.Unlock()
		w.WriteHeader(status)
		io.WriteString(w, reply)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) body(i int) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bodies[i]
}

const toolReply = `{"choices":[{"message":{"content":null,"tool_calls":[
 {"id":"call_9","type":"function","function":{"name":"calc_add","arguments":"{\"a\":1,\"b\":2}"},"extra_content":{"google":{"thought_signature":"sig-1"}}}
]},"finish_reason":"tool_calls"}],
"usage":{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":64},"completion_tokens_details":{"reasoning_tokens":7}}}`

const textReply = `{"choices":[{"message":{"content":"3"},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"completion_tokens":2}}`

func TestRequestShape(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "sk-test", "m1")
	_, err := a.Call(context.Background(), model.Request{
		System: "be brief",
		Tools:  []task.ToolSpec{{Name: "calc_add", Description: "add", Schema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"}}}`)}, {Name: "noargs"}},
		Messages: []task.Message{
			task.Text(task.RoleUser, "1+2?"),
			{Role: task.RoleAssistant, Parts: []task.Part{
				{Kind: task.PartToolCall, CallID: "c-1", ProviderCallID: "other-7", Provider: "someone-else", Name: "calc_add", Args: json.RawMessage(`{"a":1}`)},
				{Kind: task.PartToolCall, CallID: "c-2", Name: "noargs"},
			}},
			{Role: task.RoleTool, Parts: []task.Part{
				{Kind: task.PartToolResult, CallID: "c-1", Text: "3"},
				{Kind: task.PartToolResult, CallID: "c-2", Text: "ok", IsError: true},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.auth[0] != "Bearer sk-test" {
		t.Fatalf("auth header %q", s.auth[0])
	}
	b := s.body(0)
	if b["model"] != "m1" {
		t.Fatalf("model %v", b["model"])
	}
	msgs := b["messages"].([]any)
	roles := []string{}
	for _, m := range msgs {
		roles = append(roles, m.(map[string]any)["role"].(string))
	}
	if strings.Join(roles, ",") != "system,user,assistant,tool,tool" {
		t.Fatalf("roles %v", roles)
	}
	asst := msgs[2].(map[string]any)
	if asst["content"] != nil {
		t.Fatalf("assistant with only tool calls must send content null, got %v", asst["content"])
	}
	calls := asst["tool_calls"].([]any)
	first := calls[0].(map[string]any)
	// A call from another provider is replayed under the machine's own id.
	if first["id"] != "c-1" || first["function"].(map[string]any)["arguments"] != `{"a":1}` {
		t.Fatalf("first call %v", first)
	}
	if calls[1].(map[string]any)["function"].(map[string]any)["arguments"] != "{}" {
		t.Fatalf("empty args must be sent as {}: %v", calls[1])
	}
	if msgs[3].(map[string]any)["tool_call_id"] != "c-1" || msgs[4].(map[string]any)["tool_call_id"] != "c-2" {
		t.Fatalf("tool results %v %v", msgs[3], msgs[4])
	}
	tools := b["tools"].([]any)
	if fn := tools[1].(map[string]any)["function"].(map[string]any); fn["parameters"].(map[string]any)["type"] != "object" {
		t.Fatalf("a tool without schema must get an empty object schema: %v", fn)
	}
}

func TestParsesToolCallsUsageAndReplaysOpaque(t *testing.T) {
	s := newServer(t, 200, toolReply, textReply)
	a := New(s.URL+"/v1", "k", "m1")
	ctx := context.Background()
	resp, err := a.Call(ctx, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "1+2?")}})
	if err != nil {
		t.Fatal(err)
	}
	calls := resp.Message.ToolCalls()
	if len(calls) != 1 || calls[0].ProviderCallID != "call_9" || calls[0].Name != "calc_add" ||
		string(calls[0].Args) != `{"a":1,"b":2}` || calls[0].Provider != a.Provider() || calls[0].CallID != "" {
		t.Fatalf("calls %+v", calls)
	}
	if resp.FinishReason != "tool_calls" {
		t.Fatalf("finish %q", resp.FinishReason)
	}
	u := resp.Usage
	if u.PromptTokens != 100 || u.CompletionTokens != 20 || u.CachedTokens != 64 || u.ReasoningTokens != 7 {
		t.Fatalf("usage %+v", u)
	}

	// The machine assigns its own id and sends the turn back with a result.
	call := calls[0]
	call.CallID = "c-1"
	_, err = a.Call(ctx, model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "1+2?"),
		{Role: task.RoleAssistant, Parts: []task.Part{call}},
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "3"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := s.body(1)["messages"].([]any)
	replayed := msgs[1].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)
	if replayed["id"] != "call_9" {
		t.Fatalf("same provider must get its own id back: %v", replayed)
	}
	if replayed["extra_content"] == nil {
		t.Fatalf("provider-specific fields must be replayed verbatim: %v", replayed)
	}
	if msgs[2].(map[string]any)["tool_call_id"] != "call_9" {
		t.Fatalf("tool result must reference the provider id: %v", msgs[2])
	}
}

func TestHTTPErrorDoesNotLeakTheKey(t *testing.T) {
	s := newServer(t, 401, `{"error":{"message":"bad key"}}`)
	a := New(s.URL+"/v1", "sk-secret-123", "m1")
	_, err := a.Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "bad key") {
		t.Fatalf("err %v", err)
	}
	if strings.Contains(err.Error(), "sk-secret-123") {
		t.Fatal("error leaks the API key")
	}
}

func TestNoChoicesIsAnError(t *testing.T) {
	s := newServer(t, 200, `{"choices":[]}`)
	a := New(s.URL+"/v1", "k", "m1")
	if _, err := a.Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}); err == nil {
		t.Fatal("empty choices accepted")
	}
}

func TestDeepSeekCacheHitField(t *testing.T) {
	s := newServer(t, 200, `{"choices":[{"message":{"content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":900,"completion_tokens":1,"prompt_cache_hit_tokens":768,"prompt_cache_miss_tokens":132}}`)
	resp, err := New(s.URL+"/v1", "k", "deepseek-chat").Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.CachedTokens != 768 {
		t.Fatalf("cached %d", resp.Usage.CachedTokens)
	}
}

// A request's own MaxOut is sent as max_tokens and wins over MaxTokens; with
// neither, no max_tokens is sent at all.
func TestMaxOutIsSentAsMaxTokens(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "sk-test", "m1")
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}
	for i, c := range []struct {
		adapter, req int
		want         any
	}{{0, 0, nil}, {0, 16384, float64(16384)}, {1000, 0, float64(1000)}, {1000, 4000, float64(4000)}} {
		a.MaxTokens, req.MaxOut = c.adapter, c.req
		if _, err := a.Call(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if got := s.body(i)["max_tokens"]; got != c.want {
			t.Fatalf("adapter %d request %d: max_tokens %v, want %v", c.adapter, c.req, got, c.want)
		}
		if a.MaxOut() != c.adapter {
			t.Fatalf("MaxOut %d", a.MaxOut())
		}
	}
	var _ model.OutputCap = a
}
