package anthropic

import (
	"bytes"
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

// server answers every request with the next canned body and records the raw
// request bodies and headers it received.
type server struct {
	*httptest.Server
	mu      sync.Mutex
	raw     [][]byte
	headers []http.Header
}

func newServer(t *testing.T, status int, replies ...string) *server {
	t.Helper()
	s := &server{}
	n := 0
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if !json.Valid(b) {
			t.Errorf("request is not JSON: %s", b)
		}
		s.mu.Lock()
		s.raw = append(s.raw, b)
		s.headers = append(s.headers, r.Header.Clone())
		reply := replies[min(n, len(replies)-1)]
		n++
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, reply)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) body(i int) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m map[string]any
	json.Unmarshal(s.raw[i], &m)
	return m
}

// wireMessages decodes request i's messages keeping each content block's
// bytes exactly as they were sent.
type rawMsg struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
}

func (s *server) messages(t *testing.T, i int) []rawMsg {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var b struct {
		Messages []rawMsg `json:"messages"`
	}
	if err := json.Unmarshal(s.raw[i], &b); err != nil {
		t.Fatal(err)
	}
	return b.Messages
}

func block(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("block %s: %v", raw, err)
	}
	return m
}

func roles(msgs []rawMsg) string {
	var r []string
	for _, m := range msgs {
		r = append(r, m.Role)
	}
	return strings.Join(r, ",")
}

const textReply = `{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"3"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":2}}`

const toolReply = `{"id":"msg_2","type":"message","role":"assistant","model":"claude-x","content":[` +
	`{"type":"thinking","thinking":"Add them <carefully>.","signature":"EqQBCkgIARABGAIiQ=="},` +
	`{"type":"text","text":"Let me add."},` +
	`{"type":"tool_use","id":"toolu_01A","name":"calc_add","input":{"a":1,"b":2},"caller":{"type":"direct"}}` +
	`],"stop_reason":"tool_use","usage":{"input_tokens":100,"cache_creation_input_tokens":20,"cache_read_input_tokens":300,"output_tokens":50}}`

func call(t *testing.T, a *Adapter, req model.Request) model.Response {
	t.Helper()
	resp, err := a.Call(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRequestShape(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "sk-ant-test", "claude-x")
	call(t, a, model.Request{
		System: "be brief",
		Tools: []task.ToolSpec{
			{Name: "calc_add", Description: "add", Schema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"}}}`)},
			{Name: "noargs", Description: "nothing"},
		},
		Messages: []task.Message{
			task.Text(task.RoleUser, "1+2?"),
			{Role: task.RoleAssistant, Parts: []task.Part{
				{Kind: task.PartText, Text: "let me"},
				{Kind: task.PartToolCall, CallID: "c-1", ProviderCallID: "call_9", Provider: "openai:gpt", Name: "calc_add", Args: json.RawMessage(`{"a":1}`)},
				{Kind: task.PartToolCall, CallID: "c.2/x", Name: "noargs"},
			}},
			{Role: task.RoleTool, Parts: []task.Part{
				{Kind: task.PartToolResult, CallID: "c-1", Text: "3"},
				{Kind: task.PartToolResult, CallID: "c.2/x", Text: "boom", IsError: true},
			}},
			task.Text(task.RoleUser, "also 4+4"),
		},
	})

	h := s.headers[0]
	if h.Get("x-api-key") != "sk-ant-test" || h.Get("anthropic-version") != "2023-06-01" || h.Get("Authorization") != "" {
		t.Fatalf("headers %v", h)
	}
	if !strings.HasPrefix(h.Get("Content-Type"), "application/json") {
		t.Fatalf("content type %q", h.Get("Content-Type"))
	}
	b := s.body(0)
	if b["model"] != "claude-x" || b["max_tokens"] != float64(8192) || b["system"] != "be brief" {
		t.Fatalf("body %v", b)
	}
	if _, ok := b["stream"]; ok {
		t.Fatalf("a plain call must not ask for a stream: %v", b)
	}
	tools := b["tools"].([]any)
	t0, t1 := tools[0].(map[string]any), tools[1].(map[string]any)
	if t0["name"] != "calc_add" || t0["description"] != "add" || t0["input_schema"].(map[string]any)["properties"] == nil {
		t.Fatalf("tool 0 %v", t0)
	}
	if sch := t1["input_schema"].(map[string]any); sch["type"] != "object" || sch["properties"] == nil {
		t.Fatalf("a tool without schema must get an empty object schema: %v", t1)
	}

	msgs := s.messages(t, 0)
	// The tool results and the steer that followed them are one user turn.
	if roles(msgs) != "user,assistant,user" {
		t.Fatalf("roles %s", roles(msgs))
	}
	if u := block(t, msgs[0].Content[0]); u["type"] != "text" || u["text"] != "1+2?" {
		t.Fatalf("user %v", u)
	}
	asst := msgs[1].Content
	if len(asst) != 3 {
		t.Fatalf("assistant blocks %s", asst)
	}
	if b := block(t, asst[0]); b["type"] != "text" || b["text"] != "let me" {
		t.Fatalf("assistant text %v", b)
	}
	// Another provider's call goes out under the machine's id, not its own.
	tu := block(t, asst[1])
	if tu["type"] != "tool_use" || tu["id"] != "c-1" || tu["name"] != "calc_add" || tu["input"].(map[string]any)["a"] != float64(1) {
		t.Fatalf("tool_use 1 %v", tu)
	}
	tu2 := block(t, asst[2])
	if tu2["id"] != "c_2_x" {
		t.Fatalf("ids must be sanitized to [a-zA-Z0-9_-]: %v", tu2)
	}
	if in, ok := tu2["input"].(map[string]any); !ok || len(in) != 0 {
		t.Fatalf("empty args must be sent as {}: %v", tu2)
	}
	res := msgs[2].Content
	if len(res) != 3 {
		t.Fatalf("user turn %s", res)
	}
	r1, r2, steer := block(t, res[0]), block(t, res[1]), block(t, res[2])
	if r1["type"] != "tool_result" || r1["tool_use_id"] != "c-1" || r1["content"] != "3" || r1["is_error"] == true {
		t.Fatalf("result 1 %v", r1)
	}
	if r2["tool_use_id"] != "c_2_x" || r2["content"] != "boom" || r2["is_error"] != true {
		t.Fatalf("result 2 %v", r2)
	}
	if steer["type"] != "text" || steer["text"] != "also 4+4" {
		t.Fatalf("steer %v", steer)
	}
}

func TestToolResultsLeadTheMergedTurn(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	call(t, a, model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "go"),
		{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-1", Name: "slow", Args: json.RawMessage(`{}`)}}},
		task.Text(task.RoleUser, "hurry up"), // a steer committed before the result
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "done"}}},
		task.Text(task.RoleSystem, "note: budget low"),
	}})
	msgs := s.messages(t, 0)
	if roles(msgs) != "user,assistant,user" {
		t.Fatalf("roles %s", roles(msgs))
	}
	last := msgs[2].Content
	if len(last) != 3 {
		t.Fatalf("merged turn %s", last)
	}
	if b := block(t, last[0]); b["type"] != "tool_result" || b["tool_use_id"] != "c-1" {
		t.Fatalf("tool_result must come first: %s", last)
	}
	if block(t, last[1])["text"] != "hurry up" || block(t, last[2])["text"] != "note: budget low" {
		t.Fatalf("other blocks keep their order: %s", last)
	}
}

func TestMalformedArgsAreKeptUnderRaw(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	call(t, a, model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "go"),
		{Role: task.RoleAssistant, Parts: []task.Part{
			{Kind: task.PartToolCall, CallID: "c-1", Name: "f", Args: json.RawMessage(`{"a":`)},
			{Kind: task.PartToolCall, CallID: "c-2", Name: "f", Args: json.RawMessage(`[1,2]`)},
			{Kind: task.PartToolCall, CallID: "c-3", Name: "f", Args: json.RawMessage(" {\"ok\": true} ")},
		}},
		{Role: task.RoleTool, Parts: []task.Part{
			{Kind: task.PartToolResult, CallID: "c-1"},
			{Kind: task.PartToolResult, CallID: "c-2"},
			{Kind: task.PartToolResult, CallID: "c-3"},
		}},
	}})
	asst := s.messages(t, 0)[1].Content
	if in := block(t, asst[0])["input"].(map[string]any); in["_raw"] != `{"a":` || len(in) != 1 {
		t.Fatalf("invalid JSON must be kept under _raw: %v", in)
	}
	if in := block(t, asst[1])["input"].(map[string]any); in["_raw"] != `[1,2]` {
		t.Fatalf("a non-object must be kept under _raw: %v", in)
	}
	if in := block(t, asst[2])["input"].(map[string]any); in["ok"] != true || in["_raw"] != nil {
		t.Fatalf("a valid object is sent as is: %v", in)
	}
}

func TestParsesResponse(t *testing.T) {
	s := newServer(t, 200, toolReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	resp := call(t, a, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "1+2?")}})

	if resp.FinishReason != "tool_calls" {
		t.Fatalf("finish %q", resp.FinishReason)
	}
	u := resp.Usage
	if u.PromptTokens != 420 || u.CachedTokens != 300 || u.CompletionTokens != 50 {
		t.Fatalf("usage %+v", u)
	}
	m := resp.Message
	if m.Role != task.RoleAssistant || len(m.Parts) != 3 {
		t.Fatalf("message %+v", m)
	}
	r := m.Parts[0]
	if r.Kind != task.PartReasoning || r.Text != "Add them <carefully>." || r.Provider != "anthropic:claude-x" ||
		string(r.Opaque) != `{"type":"thinking","thinking":"Add them <carefully>.","signature":"EqQBCkgIARABGAIiQ=="}` {
		t.Fatalf("reasoning %+v (%s)", r, r.Opaque)
	}
	if m.Parts[1].Kind != task.PartText || m.Parts[1].Text != "Let me add." {
		t.Fatalf("text %+v", m.Parts[1])
	}
	c := m.Parts[2]
	if c.Kind != task.PartToolCall || c.ProviderCallID != "toolu_01A" || c.CallID != "" || c.Name != "calc_add" ||
		string(c.Args) != `{"a":1,"b":2}` || c.Provider != a.Provider() ||
		string(c.Opaque) != `{"type":"tool_use","id":"toolu_01A","name":"calc_add","input":{"a":1,"b":2},"caller":{"type":"direct"}}` {
		t.Fatalf("tool call %+v (%s)", c, c.Opaque)
	}
}

func TestStopReasons(t *testing.T) {
	for in, want := range map[string]string{"end_turn": "stop", "stop_sequence": "stop", "tool_use": "tool_calls", "max_tokens": "length"} {
		s := newServer(t, 200, `{"content":[{"type":"text","text":"x"}],"stop_reason":"`+in+`","usage":{"input_tokens":1,"output_tokens":1}}`)
		resp := call(t, New(s.URL+"/v1", "k", "m"), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
		if resp.FinishReason != want {
			t.Fatalf("%s mapped to %q, want %q", in, resp.FinishReason, want)
		}
	}
}

func TestEmptyAnswerIsAnError(t *testing.T) {
	s := newServer(t, 200, `{"content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":0}}`)
	if _, err := New(s.URL+"/v1", "k", "m").Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}); err == nil {
		t.Fatal("an answer with neither text nor tool calls was accepted")
	}
}

// history replays the parsed tool turn under machine id c-1, its result,
// another assistant turn and a user turn, so the replayed turn is not the
// one carrying the cache marker.
func history(resp model.Response) []task.Message {
	m := resp.Message
	for i := range m.Parts {
		if m.Parts[i].Kind == task.PartToolCall {
			m.Parts[i].CallID = "c-1"
		}
	}
	return []task.Message{
		task.Text(task.RoleUser, "1+2?"),
		m,
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "3"}}},
		task.Text(task.RoleAssistant, "It is 3."),
		task.Text(task.RoleUser, "thanks"),
	}
}

func TestSameProviderReplaysBlocksAndIDs(t *testing.T) {
	s := newServer(t, 200, toolReply, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	first := call(t, a, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "1+2?")}})
	call(t, a, model.Request{Messages: history(first)})

	msgs := s.messages(t, 1)
	if roles(msgs) != "user,assistant,user,assistant,user" {
		t.Fatalf("roles %s", roles(msgs))
	}
	asst := msgs[1].Content
	if len(asst) != 3 {
		t.Fatalf("assistant blocks %s", asst)
	}
	// Thinking and tool_use go back byte for byte, fields this adapter does
	// not know included; the signature covers them.
	if !bytes.Equal(asst[0], first.Message.Parts[0].Opaque) {
		t.Fatalf("thinking block changed:\n got %s\nwant %s", asst[0], first.Message.Parts[0].Opaque)
	}
	if !bytes.Equal(asst[2], first.Message.Parts[2].Opaque) {
		t.Fatalf("tool_use block changed:\n got %s\nwant %s", asst[2], first.Message.Parts[2].Opaque)
	}
	if r := block(t, msgs[2].Content[0]); r["tool_use_id"] != "toolu_01A" {
		t.Fatalf("the result must reference the provider's own id: %v", r)
	}
}

func TestOtherProvidersPrivateBlocksAreDropped(t *testing.T) {
	s := newServer(t, 200, toolReply, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	first := call(t, a, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "1+2?")}})

	// The same turn, answered by another model of this vendor.
	b := New(s.URL+"/v1", "k", "claude-y")
	h := history(first)
	h = append(h, task.Message{Role: task.RoleAssistant, Parts: []task.Part{
		{Kind: task.PartReasoning, Text: "private", Provider: "openai:gpt", Opaque: json.RawMessage(`{"x":1}`)},
		{Kind: task.PartReasoning, Text: "no opaque", Provider: b.Provider()},
	}}, task.Text(task.RoleUser, "again"))
	call(t, b, model.Request{Messages: h})

	msgs := s.messages(t, 1)
	// The reasoning-only turn renders to nothing, so the users around it merge.
	if roles(msgs) != "user,assistant,user,assistant,user" {
		t.Fatalf("roles %s", roles(msgs))
	}
	asst := msgs[1].Content
	if len(asst) != 2 {
		t.Fatalf("a thinking block from another provider must be dropped: %s", asst)
	}
	tu := block(t, asst[1])
	if tu["id"] != "c-1" || tu["input"].(map[string]any)["b"] != float64(2) {
		t.Fatalf("another provider's call is rendered under the machine id: %v", tu)
	}
	if r := block(t, msgs[2].Content[0]); r["tool_use_id"] != "c-1" {
		t.Fatalf("result %v", r)
	}
	last := msgs[4].Content
	if len(last) != 2 || block(t, last[0])["text"] != "thanks" || block(t, last[1])["text"] != "again" {
		t.Fatalf("last user turn %s", last)
	}
	for _, m := range msgs {
		for _, c := range m.Content {
			if bytes.Contains(c, []byte("private")) || bytes.Contains(c, []byte("no opaque")) {
				t.Fatalf("reasoning text leaked into the request: %s", c)
			}
		}
	}
}

func hasCache(t *testing.T, raw json.RawMessage) bool {
	t.Helper()
	cc, ok := block(t, raw)["cache_control"]
	if !ok {
		return false
	}
	if cc.(map[string]any)["type"] != "ephemeral" {
		t.Fatalf("cache_control %v", cc)
	}
	return true
}

func TestCacheMarkers(t *testing.T) {
	s := newServer(t, 200, toolReply, textReply, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	tools := []task.ToolSpec{{Name: "a", Description: "first"}, {Name: "b", Description: "second"}, {Name: "c", Description: "third"}}
	first := call(t, a, model.Request{Tools: tools, Messages: []task.Message{task.Text(task.RoleUser, "1+2?")}})
	h := history(first)
	call(t, a, model.Request{Tools: tools, Messages: h[:3]})
	call(t, a, model.Request{Tools: tools, Messages: h[:4]})

	// One message: nothing is second to last.
	for _, c := range s.messages(t, 0)[0].Content {
		if hasCache(t, c) {
			t.Fatalf("a lone message carries a marker: %s", c)
		}
	}
	got := s.body(1)["tools"].([]any)
	for i, tl := range got {
		_, marked := tl.(map[string]any)["cache_control"]
		if marked != (i == len(got)-1) {
			t.Fatalf("tool %d marked=%v; only the last tool carries the marker", i, marked)
		}
	}

	short, long := s.messages(t, 1), s.messages(t, 2)
	// Request 1: user, assistant, user. The assistant turn is second to last
	// and only its last block is marked.
	for i, m := range short {
		for j, c := range m.Content {
			want := i == 1 && j == len(m.Content)-1
			if hasCache(t, c) != want {
				t.Fatalf("request 1 message %d block %d marked=%v, want %v", i, j, !want, want)
			}
		}
	}
	// Request 2 moves the marker to the next turn. Every earlier block is
	// byte-identical, and the one that was marked differs only by the marker.
	if len(long) != 4 {
		t.Fatalf("roles %s", roles(long))
	}
	if !hasCache(t, long[2].Content[len(long[2].Content)-1]) {
		t.Fatalf("request 2 second-to-last turn is not marked: %s", long[2].Content)
	}
	for i := range short {
		for j := range short[i].Content {
			sb, lb := short[i].Content[j], long[i].Content[j]
			if i == 1 && j == len(short[i].Content)-1 {
				if want := append(append([]byte{}, lb[:len(lb)-1]...), []byte(`,"cache_control":{"type":"ephemeral"}}`)...); !bytes.Equal(sb, want) {
					t.Fatalf("the marker changed the block:\n marked %s\n plain  %s", sb, lb)
				}
				continue
			}
			if i == 2 {
				continue // request 1's last turn is request 2's marked turn
			}
			if !bytes.Equal(sb, lb) {
				t.Fatalf("message %d block %d changed between requests:\n %s\n %s", i, j, sb, lb)
			}
		}
	}
	// The tool definitions are byte-identical across requests.
	if !bytes.Equal(toolsRaw(t, s, 1), toolsRaw(t, s, 2)) {
		t.Fatal("tool definitions changed between requests")
	}
}

func toolsRaw(t *testing.T, s *server, i int) []byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var b struct {
		Tools json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(s.raw[i], &b); err != nil {
		t.Fatal(err)
	}
	return b.Tools
}

func TestHTTPErrorsDoNotLeakTheKey(t *testing.T) {
	const key = "sk-ant-secret-123"
	s := newServer(t, 401, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`)
	_, err := New(s.URL+"/v1", key, "m").Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "invalid x-api-key") {
		t.Fatalf("err %v", err)
	}
	if strings.Contains(err.Error(), key) {
		t.Fatal("error leaks the API key")
	}

	// A server that echoes the key back still does not get it into the error.
	echo := newServer(t, 400, `{"type":"error","error":{"type":"invalid_request_error","message":"bad key `+key+`"}}`)
	_, err = New(echo.URL+"/v1", key, "m").Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
	if err == nil || strings.Contains(err.Error(), key) {
		t.Fatalf("echoed key reached the error: %v", err)
	}
}

func TestRateLimitAndOverloadAreDistinguishable(t *testing.T) {
	classOf := func(status int, body string) model.ErrClass {
		s := newServer(t, status, body)
		_, err := New(s.URL+"/v1", "k", "m").Call(context.Background(), model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}})
		if err == nil {
			t.Fatalf("%d accepted", status)
		}
		return model.Classify(err)
	}
	if c := classOf(429, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`); c != model.RateLimited {
		t.Fatalf("429: %s", c)
	}
	if c := classOf(529, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`); c != model.Overload {
		t.Fatalf("529: %s", c)
	}
}

func TestWarmIgnoresStatus(t *testing.T) {
	var got http.Header
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, path = r.Header.Clone(), r.Method+" "+r.URL.Path
		w.WriteHeader(404)
	}))
	defer ts.Close()
	if err := New(ts.URL+"/v1/", "k", "m").Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if path != "GET /v1/models" || got.Get("x-api-key") != "k" || got.Get("anthropic-version") != "2023-06-01" {
		t.Fatalf("%s %v", path, got)
	}
	if err := New("http://127.0.0.1:1/v1", "k", "m").Warm(context.Background()); err == nil {
		t.Fatal("warm against a closed port succeeded")
	}
}

func TestProviderAndDefaults(t *testing.T) {
	a := New("https://api.anthropic.com/v1/", "k", "claude-x")
	if a.Provider() != "anthropic:claude-x" || a.BaseURL != "https://api.anthropic.com/v1" || a.MaxTokens != 8192 || a.HTTP == nil {
		t.Fatalf("%+v", a)
	}
	var _ model.Adapter = a
	var _ model.Streamer = a
	var _ model.Warmer = a
}

// A request's own MaxOut wins over the adapter's max_tokens; without one the
// adapter's (or the default) is sent.
func TestMaxOutWinsOverTheDefault(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "sk-ant-test", "claude-x")
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}
	for i, c := range []struct {
		adapter, req int
		want         float64
	}{{0, 0, 8192}, {0, 32768, 32768}, {1000, 0, 1000}, {1000, 4000, 4000}} {
		a.MaxTokens, req.MaxOut = c.adapter, c.req
		call(t, a, req)
		if got := s.body(i)["max_tokens"]; got != c.want {
			t.Fatalf("adapter %d request %d: max_tokens %v, want %v", c.adapter, c.req, got, c.want)
		}
		if a.MaxOut() != int(c.want) && c.req == 0 {
			t.Fatalf("MaxOut %d", a.MaxOut())
		}
	}
	var _ model.OutputCap = a
}

// Registers after a compaction (design §5.3): the task's request, then the
// summary as a second user message, then the rounds kept. They render with
// the roles alternating: the summary joins the request's user turn, and the
// kept round's results still lead their turn. A compaction request is the
// fixed prompt as system, one user message, no tools, its own max_tokens.
func TestCompactedRegistersRender(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "claude-x")
	call(t, a, model.Request{
		System: "be brief",
		Tools:  []task.ToolSpec{{Name: "look"}},
		Messages: []task.Message{
			task.Text(task.RoleUser, "find the bill"),
			task.SummaryMessage("rounds=3: the bill is #42"),
			{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-7", Name: "look", Args: json.RawMessage(`{}`)}}},
			{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-7", Text: "due friday"}}},
			task.Text(task.RoleUser, "and pay it"),
		},
	})
	msgs := s.messages(t, 0)
	if roles(msgs) != "user,assistant,user" {
		t.Fatalf("roles %s", roles(msgs))
	}
	first := msgs[0].Content
	if len(first) != 2 || block(t, first[0])["text"] != "find the bill" || !strings.HasPrefix(block(t, first[1])["text"].(string), task.SummaryHeader) {
		t.Fatalf("the request and the summary share the first turn: %s", first)
	}
	if b := block(t, msgs[2].Content[0]); b["type"] != "tool_result" || b["tool_use_id"] != "c-7" {
		t.Fatalf("the kept round's result leads its turn: %s", msgs[2].Content)
	}

	chunk := []task.Message{
		{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-1", Name: "look", Args: json.RawMessage(`{}`)}}},
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1", Text: "bill #42"}}},
	}
	call(t, a, model.Request{System: task.CompactSystem, Messages: task.CompactMessages(chunk), MaxOut: 512})
	b := s.body(1)
	if b["system"] != task.CompactSystem || b["max_tokens"] != float64(512) || b["tools"] != nil {
		t.Fatalf("compaction request: system %v, max_tokens %v, tools %v", b["system"], b["max_tokens"], b["tools"])
	}
	if m := s.messages(t, 1); roles(m) != "user" || len(m[0].Content) != 1 || !strings.Contains(block(t, m[0].Content[0])["text"].(string), "[result of c-1]\nbill #42") {
		t.Fatalf("compaction request messages %+v", m)
	}
}
