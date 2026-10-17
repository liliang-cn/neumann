package openai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestLocalCaps(t *testing.T) {
	a := New("http://x", "k", "m")
	a.Parallel, a.Prefill, a.ThinkOff = 1, true, "none"
	got := a.Caps()
	if got.Parallel != 1 || !got.Prefill || !got.ThinkOff {
		t.Fatalf("local caps %+v", got)
	}
	a.Parallel, a.ThinkOff = -3, ""
	if got := a.Caps(); got.Parallel != 0 || got.ThinkOff {
		t.Fatalf("negative Parallel is unknown, empty ThinkOff cannot turn thinking off: %+v", got)
	}
}

func TestNoThinkSendsThinkOff(t *testing.T) {
	s := newServer(t, 200, textReply)
	a := New(s.URL+"/v1", "k", "m")
	a.ThinkOff = "none"
	off := hi
	off.NoThink = true
	for _, req := range []model.Request{off, hi} {
		if _, err := a.Call(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.body(0)["reasoning_effort"]; got != "none" {
		t.Fatalf("NoThink sent reasoning_effort %v", got)
	}
	if got, ok := s.body(1)["reasoning_effort"]; ok {
		t.Fatalf("a request without NoThink sent reasoning_effort %v", got)
	}

	a.ThinkOff = "" // the model cannot turn thinking off: NoThink is ignored
	if _, err := a.Call(context.Background(), off); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.body(2)["reasoning_effort"]; ok {
		t.Fatalf("NoThink without ThinkOff sent reasoning_effort %v", got)
	}
}

// A request that ends with the text the model had written so far goes out
// ending with that assistant message, as it is, and the answer is only the
// continuation: the adapter does not merge the two.
func TestPrefillRendersTheTrailingAssistantMessage(t *testing.T) {
	s := newServer(t, 200, `{"choices":[{"message":{"content":" lived a fox."},"finish_reason":"stop"}]}`)
	a := New(s.URL+"/v1", "k", "m")
	a.Prefill = true
	resp, err := a.Call(context.Background(), model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "Tell a story."),
		task.Text(task.RoleAssistant, "Once upon a time there "),
	}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := s.body(0)["messages"].([]any)
	last := msgs[len(msgs)-1].(map[string]any)
	if len(msgs) != 2 || last["role"] != "assistant" || last["content"] != "Once upon a time there " || len(last) != 2 {
		t.Fatalf("messages %v", msgs)
	}
	if got := resp.Message.TextContent(); got != " lived a fox." {
		t.Fatalf("answer %q", got)
	}
}

func TestNewTimeouts(t *testing.T) {
	a := New("http://x", "k", "m")
	if a.Timeout != DefaultTimeout || a.FirstByteTimeout != 0 || a.IdleTimeout != 0 || a.HTTP.Timeout != 0 {
		t.Fatalf("timeouts %v %v %v, client %v", a.Timeout, a.FirstByteTimeout, a.IdleTimeout, a.HTTP.Timeout)
	}
}

// pacedServer writes header (when headers is true), then each chunk after
// its delay, then holds the response open until the client goes away.
func pacedServer(t *testing.T, headers bool, gap time.Duration, chunks ...string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		fl := w.(http.Flusher)
		if headers {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			fl.Flush()
		}
		for _, c := range chunks {
			select {
			case <-time.After(gap):
			case <-r.Context().Done():
				return
			}
			io.WriteString(w, c+"\n\n")
			fl.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)
	return ts
}

const (
	chunkA = `data: {"choices":[{"index":0,"delta":{"content":"a"}}]}`
	chunkZ = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
)

// timedOut checks err is a Transient timeout of the given kind, wrapping
// model.ErrResponseLost exactly when lost.
func timedOut(t *testing.T, how string, err error, which string, lost bool) {
	t.Helper()
	me := asModelError(t, how, err)
	var te *TimeoutError
	if me.Class != model.Transient || model.Classify(err) != model.Transient || !errors.As(err, &te) || te.Which != which {
		t.Fatalf("%s: want a Transient %s timeout, got %v (%s)", how, which, err, model.Classify(err))
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("%s: a timeout must not carry context.Canceled: %v", how, err)
	}
	if errors.Is(err, model.ErrResponseLost) != lost {
		t.Fatalf("%s: ErrResponseLost %v, want %v: %v", how, !lost, lost, err)
	}
}

func TestFirstByteTimeout(t *testing.T) {
	// Nothing at all comes back: the backend is still queueing the request.
	silent := pacedServer(t, false, 0)
	a := New(silent.URL+"/v1", "k", "m")
	a.FirstByteTimeout = 50 * time.Millisecond
	for how, err := range both(t, a, context.Background()) {
		timedOut(t, how, err, "first byte", false)
	}

	// Headers arrive at once but no body: the first byte is the body's.
	headersOnly := pacedServer(t, true, 0)
	a = New(headersOnly.URL+"/v1", "k", "m")
	a.FirstByteTimeout = 50 * time.Millisecond
	_, err := a.Stream(context.Background(), hi, func(model.Delta) {})
	timedOut(t, "headers only", err, "first byte", true)
}

func TestFirstByteTimeoutEndsAtTheFirstByte(t *testing.T) {
	// Chunks keep coming for well past FirstByteTimeout: it no longer applies.
	ts := pacedServer(t, true, 30*time.Millisecond, chunkA, chunkA, chunkA, chunkA, chunkA, chunkZ, "data: [DONE]")
	a := New(ts.URL+"/v1", "k", "m")
	a.FirstByteTimeout = 100 * time.Millisecond
	resp, err := a.Stream(context.Background(), hi, func(model.Delta) {})
	if err != nil || resp.Message.TextContent() != "aaaaa" {
		t.Fatalf("%q %v", resp.Message.TextContent(), err)
	}
}

func TestIdleTimeout(t *testing.T) {
	ts := pacedServer(t, true, 0, chunkA) // one chunk, then nothing
	a := New(ts.URL+"/v1", "k", "m")
	a.IdleTimeout = 50 * time.Millisecond
	var got string
	_, err := a.Stream(context.Background(), hi, func(d model.Delta) { got += d.Text })
	timedOut(t, "stalled stream", err, "idle", true)
	if got != "a" {
		t.Fatalf("the chunk before the stall was not reported: %q", got)
	}

	// Steady chunks reset it: the stream outlasts IdleTimeout several times.
	steady := pacedServer(t, true, 30*time.Millisecond, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA, chunkZ, "data: [DONE]")
	a = New(steady.URL+"/v1", "k", "m")
	a.IdleTimeout = 100 * time.Millisecond
	if resp, err := a.Stream(context.Background(), hi, func(model.Delta) {}); err != nil || resp.Message.TextContent() != "aaaaaa" {
		t.Fatalf("steady stream: %q %v", resp.Message.TextContent(), err)
	}
}

func TestTimeoutBoundsTheWholeCall(t *testing.T) {
	ts := pacedServer(t, true, 20*time.Millisecond, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA, chunkA)
	a := New(ts.URL+"/v1", "k", "m")
	a.Timeout, a.IdleTimeout = 80*time.Millisecond, time.Second
	_, err := a.Stream(context.Background(), hi, func(model.Delta) {})
	timedOut(t, "endless stream", err, "call", true)

	silent := pacedServer(t, false, 0)
	a = New(silent.URL+"/v1", "k", "m")
	a.Timeout = 50 * time.Millisecond
	for how, err := range both(t, a, context.Background()) {
		timedOut(t, how, err, "call", false)
	}
}

func TestNoTimeout(t *testing.T) {
	ts := pacedServer(t, true, 60*time.Millisecond, chunkA, chunkZ, "data: [DONE]")
	a := New(ts.URL+"/v1", "k", "m")
	a.Timeout = 0
	if _, err := a.Stream(context.Background(), hi, func(model.Delta) {}); err != nil {
		t.Fatal(err)
	}
}

// The caller ending the call is still Canceled when the adapter has timeouts
// of its own, longer or shorter.
func TestCallerWinsOverTimeouts(t *testing.T) {
	silent := pacedServer(t, false, 0)
	a := New(silent.URL+"/v1", "k", "m")
	a.FirstByteTimeout, a.IdleTimeout = time.Second, time.Second
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	for how, err := range both(t, a, ctx) {
		if model.Classify(err) != model.Canceled || !errors.Is(err, context.Canceled) {
			t.Errorf("caller cancel via %s: %v (%s)", how, err, model.Classify(err))
		}
	}

	stalled := pacedServer(t, true, 0, chunkA)
	dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dcancel()
	a = New(stalled.URL+"/v1", "k", "m")
	a.IdleTimeout = time.Second
	_, err := a.Stream(dctx, hi, func(model.Delta) {})
	if model.Classify(err) != model.Canceled || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline mid-stream: %v (%s)", err, model.Classify(err))
	}
}

// The timeouts work the same over the shared HTTP/2 transport.
func TestIdleTimeoutOverHTTP2(t *testing.T) {
	var n atomic.Int32
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.ProtoMajor != 2 {
			t.Errorf("served over %s", r.Proto)
		}
		if n.Add(1) > 1 {
			io.WriteString(w, textReply)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunkA+"\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	a := New(ts.URL+"/v1", "k", "m")
	trust(ts, a.HTTP.Transport)
	a.IdleTimeout = 50 * time.Millisecond
	_, err := a.Stream(context.Background(), hi, func(model.Delta) {})
	timedOut(t, "h2 stream", err, "idle", true)
	call(t, a) // the shared connection still serves the next request
}

func TestStreamReportsOllamaReasoning(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning":"Thinking"}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`,
		`data: [DONE]`,
	)
	_, deltas, err := stream(t, New(ts.URL+"/v1", "k", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 1 || deltas[0].Reasoning != "Thinking" {
		t.Fatalf("deltas %+v", deltas)
	}
}

// On a text prefill the adapter does not split a thought, so a backend that
// streams native reasoning and then nothing reports the reasoning as deltas
// and the turn as empty (ErrEmptyResponse): the machine must not take the
// reasoning for text continuing the prefix (design §6.4).
func TestTextPrefillThatOnlyThinksIsEmpty(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning":"hmm, where was I"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`,
		`data: [DONE]`,
	)
	a := New(ts.URL+"/v1", "k", "m")
	a.Prefill = true
	a.ThinkTags = [2]string{"<think>", "</think>"}
	var deltas []model.Delta
	_, err := a.Stream(context.Background(), model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "Tell a story."),
		task.Text(task.RoleAssistant, "Once upon"),
	}}, func(d model.Delta) { deltas = append(deltas, d) })
	if !errors.Is(err, model.ErrEmptyResponse) {
		t.Fatalf("err %v", err)
	}
	if len(deltas) != 1 || deltas[0].Reasoning != "hmm, where was I" || deltas[0].Text != "" {
		t.Fatalf("deltas %+v", deltas)
	}
}

// thinking is a request that asks the model to go on thinking: it ends with
// an assistant message holding only reasoning.
var thinking = model.Request{Messages: []task.Message{
	task.Text(task.RoleUser, "Count the hens."),
	{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartReasoning, Text: "There are three hens in the"}}},
}}

// With ThinkTags, a call cut off while thinking is continued (design §6.4):
// the trailing reasoning goes out as content inside the open tag, and the
// answer, all in content, is split at the close tag into the rest of the
// thinking and the text. Without ThinkTags the reasoning-only message goes
// out empty, and the content is all text.
func TestThinkTagsContinueAThought(t *testing.T) {
	s := newServer(t, 200,
		`{"choices":[{"message":{"content":" yard.\n</think>\n\nThree."},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"content":" yard, and"},"finish_reason":"length"}]}`,
		`{"choices":[{"message":{"content":"Three."},"finish_reason":"stop"}]}`)
	a := New(s.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	if !a.Caps().ThinkPrefill {
		t.Fatal("ThinkTags do not report ThinkPrefill")
	}
	resp, err := a.Call(context.Background(), thinking)
	if err != nil {
		t.Fatal(err)
	}
	msgs := s.body(0)["messages"].([]any)
	if last := msgs[len(msgs)-1].(map[string]any); last["role"] != "assistant" || last["content"] != "<think>\nThere are three hens in the" {
		t.Fatalf("last message %v", last)
	}
	if p := resp.Message.Parts; len(p) != 2 || p[0].Kind != task.PartReasoning || p[0].Text != " yard.\n" || p[0].Provider != a.Provider() || p[1].Text != "Three." {
		t.Fatalf("answer %+v", p)
	}
	// Cut off while still thinking: all reasoning, no text.
	resp, err = a.Call(context.Background(), thinking)
	if err != nil || resp.FinishReason != "length" || resp.Message.TextContent() != "" || resp.Message.Parts[0].Text != " yard, and" {
		t.Fatalf("cut off: %+v, %v", resp, err)
	}
	a.ThinkTags = [2]string{}
	if a.Caps().ThinkPrefill {
		t.Fatal("ThinkPrefill without ThinkTags")
	}
	resp, err = a.Call(context.Background(), thinking)
	if err != nil || resp.Message.TextContent() != "Three." {
		t.Fatalf("without ThinkTags: %+v, %v", resp, err)
	}
	msgs = s.body(2)["messages"].([]any)
	if last := msgs[len(msgs)-1].(map[string]any); last["content"] != "" {
		t.Fatalf("without ThinkTags the last message is %v", last)
	}
}

// A continued thought streams as reasoning up to the close tag and as text
// after it, a tag split across chunks included; nothing of the tag leaks.
func TestThinkTagsSplitTheStream(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"index":0,"delta":{"content":" yard. Three"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":" in all.</th"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"ink"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":">\n"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"\nThree."},"finish_reason":"stop"}]}`,
		`data: [DONE]`,
	)
	a := New(ts.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	var think, text strings.Builder
	resp, err := a.Stream(context.Background(), thinking, func(d model.Delta) {
		think.WriteString(d.Reasoning)
		text.WriteString(d.Text)
	})
	if err != nil {
		t.Fatal(err)
	}
	if think.String() != " yard. Three in all." || text.String() != "Three." {
		t.Fatalf("streamed reasoning %q, text %q", think.String(), text.String())
	}
	if p := resp.Message.Parts; len(p) != 2 || p[0].Text != " yard. Three in all." || p[1].Text != "Three." {
		t.Fatalf("answer %+v", p)
	}

	// A stream that never closes the thought is all reasoning, the held-back
	// end included.
	ts2, _ := sseServer(t,
		`data: {"choices":[{"index":0,"delta":{"content":" still thinking </t"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"length"}]}`,
		`data: [DONE]`,
	)
	a.BaseURL = ts2.URL + "/v1"
	think.Reset()
	text.Reset()
	resp, err = a.Stream(context.Background(), thinking, func(d model.Delta) {
		think.WriteString(d.Reasoning)
		text.WriteString(d.Text)
	})
	if err != nil || think.String() != " still thinking </t" || text.String() != "" || resp.Message.Parts[0].Text != " still thinking </t" {
		t.Fatalf("unclosed thought: %q / %q, %+v, %v", think.String(), text.String(), resp, err)
	}
}

// The splitter gives the same reasoning and text however the content is cut
// into pieces: at every pair of offsets, tags and multi-byte characters
// split across pieces included.
func TestThinkSplitAtEveryOffset(t *testing.T) {
	a := New("http://x", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	cases := []struct{ in, r, x string }{
		{"rest of thought</think>\n\nThe answer <think> stays", "rest of thought", "The answer <think> stays"},
		{"a </b> c </thin k</think>x</think>y", "a </b> c </thin k", "x</think>y"},
		{"</think>\nonly answer", "", "only answer"},
		{"never closed <", "never closed <", ""},
		{"ünïcødé 思考</think>\r\n\n答案", "ünïcødé 思考", "答案"},
	}
	for _, c := range cases {
		for i := 0; i <= len(c.in); i++ {
			for j := i; j <= len(c.in); j++ {
				sp := a.splitter()
				var r, x string
				for _, p := range []string{c.in[:i], c.in[i:j], c.in[j:]} {
					rr, xx := sp.feed(p)
					r, x = r+rr, x+xx
				}
				rr, xx := sp.flush()
				r, x = r+rr, x+xx
				if r != c.r || x != c.x {
					t.Fatalf("%q split at %d,%d: reasoning %q text %q", c.in, i, j, r, x)
				}
			}
		}
	}
}

// Only a request that continues a thought is split: with ThinkTags set, an
// answer to any other request that writes the tags is text as it is.
func TestThinkTagsLeaveOtherAnswersWhole(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"delta":{"content":"x</think>y"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: [DONE]`)
	a := New(ts.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	resp, err := a.Stream(context.Background(), hi, func(model.Delta) {})
	if err != nil || resp.Message.TextContent() != "x</think>y" {
		t.Fatalf("%q %v", resp.Message.TextContent(), err)
	}
}

// A backend that parses the thinking out itself (llama.cpp with reasoning
// parsing) continues a thought in the reasoning field and answers in
// untagged content: once reasoning arrives in its own field, content is
// text, in a stream and in a whole response alike.
func TestThinkTagsWithABackendThatParsesThinking(t *testing.T) {
	ts, _ := sseServer(t,
		`data: {"choices":[{"delta":{"content":" and"}}]}`,
		`data: {"choices":[{"delta":{"reasoning":" so it is 4."}}]}`,
		`data: {"choices":[{"delta":{"content":"The answer is 4."}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: [DONE]`)
	a := New(ts.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	var think, text strings.Builder
	resp, err := a.Stream(context.Background(), thinking, func(d model.Delta) {
		think.WriteString(d.Reasoning)
		text.WriteString(d.Text)
	})
	if err != nil || resp.Message.TextContent() != "The answer is 4." || resp.Message.Parts[0].Text != " and so it is 4." {
		t.Fatalf("stream: %+v, %v", resp.Message.Parts, err)
	}
	if think.String() != " and so it is 4." || text.String() != "The answer is 4." {
		t.Fatalf("streamed reasoning %q, text %q", think.String(), text.String())
	}

	s := newServer(t, 200, `{"choices":[{"message":{"reasoning_content":" so it is 4.","content":"The answer is 4."},"finish_reason":"stop"}]}`)
	a.BaseURL = s.URL + "/v1"
	resp, err = a.Call(context.Background(), thinking)
	if err != nil || resp.Message.TextContent() != "The answer is 4." || resp.Message.Parts[0].Text != " so it is 4." {
		t.Fatalf("call: %+v, %v", resp.Message.Parts, err)
	}
}

// A continued thought that stops without an answer comes back as the
// reasoning alone, not as an error: the machine judges it (design §6.4). A
// turn with nothing at all is still an error.
func TestAThoughtThatStopsWithoutAnAnswer(t *testing.T) {
	s := newServer(t, 200,
		`{"choices":[{"message":{"content":" and that is all."},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"content":""},"finish_reason":"stop"}]}`)
	a := New(s.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	resp, err := a.Call(context.Background(), thinking)
	if err != nil || resp.Message.TextContent() != "" || resp.Message.Parts[0].Text != " and that is all." || resp.FinishReason != "stop" {
		t.Fatalf("%+v, %v", resp, err)
	}
	if _, err := a.Call(context.Background(), thinking); !errors.Is(err, model.ErrEmptyResponse) || model.Classify(err) != model.Transient {
		t.Fatalf("an empty turn: %v", err)
	}
}
