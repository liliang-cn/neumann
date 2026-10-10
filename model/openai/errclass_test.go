package openai

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

var now0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

const openaiBilling = `{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","code":"insufficient_quota"}}`

// cpaCoolingDown was observed live on a CLIProxyAPI gateway: the outer type
// says rate_limit_error, but the upstream quota resets in five days.
const cpaCoolingDown = `{"type":"error","error":{"type":"rate_limit_error","message":"All credentials for model claude-sonnet-4-6 are cooling down via provider antigravity (last error: {\n  \"error\": {\n    \"code\": 429,\n    \"message\": \"Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 120h41m12s.\",\n    \"status\": \"RESOURCE_EXHAUSTED\"\n  }\n})"}}`

// statusServer answers every request with status, headers and body.
func statusServer(t *testing.T, status int, h http.Header, body string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		for k, vs := range h {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

var hi = model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}

// both runs the request through Call and through Stream.
func both(t *testing.T, a *Adapter, ctx context.Context) map[string]error {
	t.Helper()
	_, cerr := a.Call(ctx, hi)
	_, serr := a.Stream(ctx, hi, func(model.Delta) {})
	return map[string]error{"Call": cerr, "Stream": serr}
}

func asModelError(t *testing.T, how string, err error) *model.Error {
	t.Helper()
	var me *model.Error
	if !errors.As(err, &me) {
		t.Fatalf("%s: %T %v is not a *model.Error", how, err, err)
	}
	return me
}

func TestAdapterClassifiesStatusErrors(t *testing.T) {
	const key = "sk-secret-123456"
	cases := []struct {
		status int
		h      http.Header
		body   string
		want   model.ErrClass
		after  time.Duration
	}{
		{429, hdr("Retry-After", "2", "x-ratelimit-remaining-requests", "0", "x-ratelimit-reset-requests", "2s"), `{"error":{"message":"slow down"}}`, model.RateLimited, 2 * time.Second},
		{429, nil, openaiBilling, model.QuotaExhausted, 0},
		{429, nil, cpaCoolingDown, model.QuotaExhausted, 120*time.Hour + 41*time.Minute + 12*time.Second},
		{529, nil, `{"error":{"type":"overloaded_error"}}`, model.Overload, 0},
		{429, nil, `{"error":{"message":"busy"}}`, model.Overload, 0},
		{502, nil, `bad gateway`, model.Transient, 0},
		{400, nil, `{"error":{"message":"timeout too large"}}`, model.Permanent, 0},
	}
	for _, c := range cases {
		ts := statusServer(t, c.status, c.h, c.body)
		a := New(ts.URL+"/v1", key, "m1")
		a.Now = func() time.Time { return now0 }
		for how, err := range both(t, a, context.Background()) {
			me := asModelError(t, how, err)
			if me.Class != c.want || me.Status != c.status || me.RetryAfter != c.after || me.Provider != a.Provider() {
				t.Errorf("%s %d: %+v", how, c.status, me)
			}
			if model.Classify(err) != c.want {
				t.Errorf("%s %d: Classify = %s", how, c.status, model.Classify(err))
			}
			s := err.Error()
			if !strings.Contains(s, strconv.Itoa(c.status)) || !strings.Contains(s, snippet([]byte(c.body))[:5]) {
				t.Errorf("%s %d: text lacks status or body: %s", how, c.status, s)
			}
			if strings.Contains(s, key) {
				t.Errorf("%s %d: error leaks the key: %s", how, c.status, s)
			}
		}
		if c.want == model.RateLimited {
			_, err := a.Call(context.Background(), hi)
			r := a.RateInfo(nil, err)
			if d, ok := r.Dim(model.DimRequests); !ok || d.Remaining != 0 || !d.ResetAt.Equal(now0.Add(2*time.Second)) {
				t.Errorf("rate info from a 429: %+v", r)
			}
			if !r.RetryAt.Equal(now0.Add(2 * time.Second)) {
				t.Errorf("RetryAt %v", r.RetryAt)
			}
		}
	}
}

func TestErrorRedactsAnEchoedKey(t *testing.T) {
	const key = "sk-live-abcdef123456"
	ts := statusServer(t, 401, nil, `{"error":{"message":"Incorrect API key provided: `+key+`"}}`)
	for how, err := range both(t, New(ts.URL+"/v1", key, "m1"), context.Background()) {
		if err == nil || strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "Incorrect API key") {
			t.Errorf("%s: %v", how, err)
		}
	}
}

func TestRateInfoOnSuccess(t *testing.T) {
	h := hdr("x-ratelimit-remaining-requests", "41", "x-ratelimit-reset-requests", "1s", "x-ratelimit-remaining-tokens", "9000", "x-ratelimit-reset-tokens", "6m0s")
	plain := statusServer(t, 200, h, textReply)
	a := New(plain.URL+"/v1", "k", "m1")
	a.Now = func() time.Time { return now0 }
	resp, err := a.Call(context.Background(), hi)
	if err != nil {
		t.Fatal(err)
	}
	r := a.RateInfo(&resp, nil)
	if d, ok := r.Dim(model.DimRequests); !ok || d.Remaining != 41 || !d.ResetAt.Equal(now0.Add(time.Second)) {
		t.Fatalf("Call rate info %+v", r)
	}

	sh := h.Clone()
	sh.Set("Content-Type", "text/event-stream")
	sse := statusServer(t, 200, sh, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	b := New(sse.URL+"/v1", "k", "m1")
	b.Now = func() time.Time { return now0 }
	resp, err = b.Stream(context.Background(), hi, func(model.Delta) {})
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := b.RateInfo(&resp, nil).Dim(model.DimTokens); !ok || d.Remaining != 9000 {
		t.Fatalf("Stream rate info %+v", resp.Rate)
	}

	none := statusServer(t, 200, nil, textReply)
	resp, err = New(none.URL+"/v1", "k", "m1").Call(context.Background(), hi)
	if err != nil || resp.Rate != nil {
		t.Fatalf("no headers: %+v %v", resp.Rate, err)
	}
}

func TestNetworkErrorsAreTransient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: connection refused
	for how, err := range both(t, New("http://"+addr+"/v1", "k", "m1"), context.Background()) {
		me := asModelError(t, how, err)
		if me.Class != model.Transient || model.Classify(err) != model.Transient {
			t.Errorf("%s: %+v", how, me)
		}
		if errors.Is(err, model.ErrResponseLost) {
			t.Errorf("%s: a refused connection never reached the provider, so nothing was lost", how)
		}
	}
}

// cutServer promises a longer body than it sends, then drops the connection.
func cutServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+100))
		w.WriteHeader(200)
		io.WriteString(w, body)
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestLostResponsesAreTransient(t *testing.T) {
	cut := cutServer(t, `{"choices":[{"message":{"content":"hal`)
	_, err := New(cut.URL+"/v1", "k", "m1").Call(context.Background(), hi)
	if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
		t.Fatalf("cut body: %v (%s)", err, model.Classify(err))
	}

	garbled := statusServer(t, 200, nil, `{"choices":[`)
	_, err = New(garbled.URL+"/v1", "k", "m1").Call(context.Background(), hi)
	if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
		t.Fatalf("garbled body: %v (%s)", err, model.Classify(err))
	}

	ended, _ := sseServer(t, `data: {"choices":[{"index":0,"delta":{"content":"half"}}]}`)
	_, _, err = stream(t, New(ended.URL+"/v1", "k", "m1"))
	if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
		t.Fatalf("stream ended early: %v (%s)", err, model.Classify(err))
	}

	sseCut := cutServer(t, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"half\"}}]}\n\n")
	_, _, err = stream(t, New(sseCut.URL+"/v1", "k", "m1"))
	if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
		t.Fatalf("stream connection dropped: %v (%s)", err, model.Classify(err))
	}

	empty := statusServer(t, 200, nil, `{"choices":[]}`)
	_, err = New(empty.URL+"/v1", "k", "m1").Call(context.Background(), hi)
	if model.Classify(err) != model.Transient {
		t.Fatalf("no choices: %v (%s)", err, model.Classify(err))
	}
}

func TestInStreamErrorClasses(t *testing.T) {
	cases := []struct {
		line string
		want model.ErrClass
	}{
		{`data: {"error":{"message":"quota exceeded","code":429}}`, model.QuotaExhausted},
		{`data: {"error":{"message":"rate limit","code":429}}`, model.Overload},
		{`data: {"error":{"type":"overloaded_error","message":"Overloaded"}}`, model.Overload},
		{`data: {"error":{"message":"upstream failed"}}`, model.Transient},
		{`data: {"error":{"message":"bad tool schema","code":400}}`, model.Permanent},
		{`data: {"error":{"message":"rate_limit_exceeded","code":"rate_limit_exceeded"}}`, model.Overload},
	}
	for _, c := range cases {
		ts, _ := sseServer(t, c.line)
		_, _, err := stream(t, New(ts.URL+"/v1", "k", "m1"))
		if got := model.Classify(err); got != c.want {
			t.Errorf("%s: %s (%v)", c.line, got, err)
		}
	}
}

func TestCancellationAndTimeouts(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); slow.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	for how, err := range both(t, New(slow.URL+"/v1", "k", "m1"), ctx) {
		if model.Classify(err) != model.Canceled || !errors.Is(err, context.Canceled) {
			t.Errorf("caller cancel via %s: %v (%s)", how, err, model.Classify(err))
		}
	}

	// The caller's own deadline ends the task: it is not retryable.
	dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dcancel()
	for how, err := range both(t, New(slow.URL+"/v1", "k", "m1"), dctx) {
		if model.Classify(err) != model.Canceled || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("caller deadline via %s: %v (%s)", how, err, model.Classify(err))
		}
	}

	// A per-request timeout while the caller still has time is the provider
	// being slow: transient.
	a := New(slow.URL+"/v1", "k", "m1")
	a.HTTP.Timeout = 50 * time.Millisecond
	for how, err := range both(t, a, context.Background()) {
		if model.Classify(err) != model.Transient {
			t.Errorf("request timeout via %s: %v (%s)", how, err, model.Classify(err))
		}
	}
}

func TestCancelMidStream(t *testing.T) {
	hold := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-hold:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(hold); ts.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	_, err := New(ts.URL+"/v1", "k", "m1").Stream(ctx, hi, func(model.Delta) { cancel() })
	if model.Classify(err) != model.Canceled {
		t.Fatalf("%v (%s)", err, model.Classify(err))
	}
}
