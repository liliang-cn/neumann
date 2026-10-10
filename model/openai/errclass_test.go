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

const geminiPerMinute = `{"error":{"code":429,"message":"Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 15","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"34s"}]}}`

const openaiBilling = `{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","code":"insufficient_quota"}}`

// cpaCoolingDown was observed live on a CLIProxyAPI gateway: the outer type
// says rate_limit_error, but the upstream quota resets in five days.
const cpaCoolingDown = `{"type":"error","error":{"type":"rate_limit_error","message":"All credentials for model claude-sonnet-4-6 are cooling down via provider antigravity (last error: {\n  \"error\": {\n    \"code\": 429,\n    \"message\": \"Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 120h41m12s.\",\n    \"status\": \"RESOURCE_EXHAUSTED\"\n  }\n})"}}`

const openaiRateLimit = `{"error":{"message":"Rate limit reached for gpt-4o on tokens per min (TPM): Limit 30000, Used 29000, Requested 2000.","type":"tokens","code":"rate_limit_exceeded"}}`

func TestClassifyStatus(t *testing.T) {
	rl := hdr("x-ratelimit-limit-tokens", "30000", "x-ratelimit-remaining-tokens", "0", "x-ratelimit-reset-tokens", "6m0s",
		"x-ratelimit-remaining-requests", "499", "x-ratelimit-reset-requests", "120ms")
	cases := []struct {
		name   string
		status int
		h      http.Header
		body   string
		want   model.ErrClass
		after  time.Duration
	}{
		{"429 retry-after seconds", 429, hdr("Retry-After", "2"), `{}`, model.RateLimited, 2 * time.Second},
		{"429 retry-after-ms wins", 429, hdr("Retry-After", "2", "retry-after-ms", "1500"), `{}`, model.RateLimited, 1500 * time.Millisecond},
		{"429 retry-after http date", 429, hdr("Retry-After", "Sat, 10 Oct 2026 12:00:07 GMT"), `{}`, model.RateLimited, 7 * time.Second},
		{"429 x-ratelimit headers", 429, rl, openaiRateLimit, model.RateLimited, 6 * time.Minute},
		{"429 anthropic headers", 429, hdr("anthropic-ratelimit-requests-remaining", "0", "anthropic-ratelimit-requests-reset", "2026-10-10T12:00:30Z"), `{}`, model.RateLimited, 30 * time.Second},
		{"429 undistinguished", 429, nil, `{"error":{"message":"too many requests"}}`, model.Overload, 0},
		{"429 overloaded", 429, nil, `{"error":{"message":"the engine is overloaded"}}`, model.Overload, 0},
		{"429 billing beats rate headers", 429, rl, openaiBilling, model.QuotaExhausted, 0},
		{"429 quota without a window", 429, nil, `{"error":{"message":"quota exceeded"}}`, model.QuotaExhausted, 0},
		{"429 per-minute quota is a rate limit", 429, nil, geminiPerMinute, model.RateLimited, 34 * time.Second},
		{"429 quota with retry-after is a rate limit", 429, hdr("Retry-After", "5"), `{"error":{"message":"quota exceeded"}}`, model.RateLimited, 5 * time.Second},
		{"429 cpa cooling down for days is quota", 429, nil, cpaCoolingDown, model.QuotaExhausted, 120*time.Hour + 41*time.Minute + 12*time.Second},
		{"429 cpa cooling down despite rate headers", 429, hdr("Retry-After", "30"), cpaCoolingDown, model.QuotaExhausted, 120*time.Hour + 41*time.Minute + 12*time.Second},
		{"429 short reset in body is a rate limit", 429, nil, `{"error":{"type":"rate_limit_error","message":"cooling down. Resets in 45s."}}`, model.RateLimited, 45 * time.Second},
		{"429 retry-after beyond an hour is quota", 429, hdr("Retry-After", "86400"), `{}`, model.QuotaExhausted, 24 * time.Hour},
		{"402", 402, nil, `{}`, model.QuotaExhausted, 0},
		{"403 billing", 403, nil, `{"error":{"message":"billing is not active for this account"}}`, model.QuotaExhausted, 0},
		{"403 quota", 403, nil, `{"error":{"message":"monthly quota used up"}}`, model.QuotaExhausted, 0},
		{"403 forbidden", 403, nil, `{"error":{"message":"forbidden"}}`, model.Permanent, 0},
		{"529", 529, nil, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, model.Overload, 0},
		{"503 overloaded", 503, nil, `{"error":{"message":"model is overloaded"}}`, model.Overload, 0},
		{"500 overloaded_error", 500, nil, `{"error":{"type":"overloaded_error"}}`, model.Overload, 0},
		{"503 plain", 503, nil, `service unavailable`, model.Transient, 0},
		{"503 with retry-after keeps the hint", 503, hdr("Retry-After", "5"), `unavailable`, model.Transient, 5 * time.Second},
		{"500", 500, nil, `{"error":{"message":"internal"}}`, model.Transient, 0},
		{"502", 502, nil, `bad gateway`, model.Transient, 0},
		{"504", 504, nil, `gateway timeout`, model.Transient, 0},
		{"408", 408, nil, `request timeout`, model.Transient, 0},
		{"400 timeout text stays permanent", 400, nil, `{"error":{"message":"timeout must be at most 600"}}`, model.Permanent, 0},
		{"400 context length", 400, nil, `{"error":{"code":"context_length_exceeded"}}`, model.Permanent, 0},
		{"401", 401, nil, `{"error":{"message":"bad key"}}`, model.Permanent, 0},
		{"404 timed out text", 404, nil, `upstream timed out looking for model`, model.Permanent, 0},
		{"422", 422, nil, `{}`, model.Permanent, 0},
	}
	for _, c := range cases {
		h := c.h
		if h == nil {
			h = http.Header{}
		}
		got, after := classify(c.status, h, []byte(c.body), now0)
		if got != c.want || after != c.after {
			t.Errorf("%s: classify = %s, %v; want %s, %v", c.name, got, after, c.want, c.after)
		}
	}
}

func TestParseReset(t *testing.T) {
	cases := []struct {
		in   string
		want time.Time
		ok   bool
	}{
		{"1s", now0.Add(time.Second), true},
		{"6m0s", now0.Add(6 * time.Minute), true},
		{"120ms", now0.Add(120 * time.Millisecond), true},
		{"1h2m3.5s", now0.Add(time.Hour + 2*time.Minute + 3500*time.Millisecond), true},
		{"1d2h", now0.Add(26 * time.Hour), true},
		{"7", now0.Add(7 * time.Second), true},
		{"0.5", now0.Add(500 * time.Millisecond), true},
		{"2026-10-10T12:00:30Z", now0.Add(30 * time.Second), true},
		{"Sat, 10 Oct 2026 12:01:00 GMT", now0.Add(time.Minute), true},
		{"", time.Time{}, false},
		{"later", time.Time{}, false},
	}
	for _, c := range cases {
		got, ok := parseReset(c.in, now0)
		if ok != c.ok || !got.Equal(c.want) {
			t.Errorf("parseReset(%q) = %v,%v want %v,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestRateInfoFromHeaders(t *testing.T) {
	if r := rateInfo(http.Header{}, now0); r != nil {
		t.Fatalf("no headers must give nil, got %+v", r)
	}
	h := hdr(
		"x-ratelimit-limit-requests", "500",
		"x-ratelimit-remaining-requests", "499",
		"x-ratelimit-reset-requests", "120ms",
		"x-ratelimit-limit-tokens", "30000",
		"x-ratelimit-remaining-tokens", "0",
		"x-ratelimit-reset-tokens", "6m0s",
		"anthropic-ratelimit-input-tokens-remaining", "1000",
		"anthropic-ratelimit-input-tokens-reset", "2026-10-10T12:00:30Z",
		"anthropic-ratelimit-output-tokens-limit", "8000",
		"anthropic-ratelimit-output-tokens-remaining", "7000",
		"retry-after", "3",
	)
	r := rateInfo(h, now0)
	if r == nil {
		t.Fatal("nil")
	}
	check := func(dim string, limit, remaining int64, reset time.Time) {
		t.Helper()
		d, ok := r.Dim(dim)
		if !ok || d.Limit != limit || d.Remaining != remaining || !d.ResetAt.Equal(reset) {
			t.Errorf("%s = %+v,%v want limit %d remaining %d reset %v", dim, d, ok, limit, remaining, reset)
		}
	}
	check(model.DimRequests, 500, 499, now0.Add(120*time.Millisecond))
	check(model.DimTokens, 30000, 0, now0.Add(6*time.Minute))
	check(model.DimInputTokens, 0, 1000, now0.Add(30*time.Second))
	check(model.DimOutputTokens, 8000, 7000, time.Time{})
	if !r.RetryAt.Equal(now0.Add(3*time.Second)) || !r.At.Equal(now0) {
		t.Fatalf("RetryAt %v At %v", r.RetryAt, r.At)
	}

	// A reset without a remaining count is not a dimension the limiter can use.
	if r := rateInfo(hdr("x-ratelimit-reset-requests", "1s"), now0); r != nil {
		if _, ok := r.Dim(model.DimRequests); ok {
			t.Fatalf("dimension without remaining: %+v", r)
		}
	}
}

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
