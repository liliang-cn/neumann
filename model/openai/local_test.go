package openai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
