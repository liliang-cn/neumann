package anthropic

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

func errBody(typ, msg string) string {
	return `{"type":"error","error":{"type":"` + typ + `","message":"` + msg + `"}}`
}

// cpaCoolingDown was observed live on a CLIProxyAPI gateway: the outer type
// says rate_limit_error, but the upstream quota resets in five days.
const cpaCoolingDown = `{"type":"error","error":{"type":"rate_limit_error","message":"All credentials for model claude-sonnet-4-6 are cooling down via provider antigravity (last error: {\n  \"error\": {\n    \"code\": 429,\n    \"message\": \"Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 120h41m12s.\",\n    \"status\": \"RESOURCE_EXHAUSTED\"\n  }\n})"}}`

const cpaReset = 120*time.Hour + 41*time.Minute + 12*time.Second

func TestClassify(t *testing.T) {
	tokensOut := hdr("anthropic-ratelimit-tokens-limit", "80000", "anthropic-ratelimit-tokens-remaining", "0",
		"anthropic-ratelimit-tokens-reset", "2026-10-10T12:00:30Z",
		"anthropic-ratelimit-requests-remaining", "49", "anthropic-ratelimit-requests-reset", "2026-10-10T12:00:01Z")
	cases := []struct {
		name   string
		status int
		h      http.Header
		body   string
		want   model.ErrClass
		after  time.Duration
	}{
		{"429 rate_limit_error retry-after", 429, hdr("retry-after", "2"), errBody("rate_limit_error", "Number of request tokens has exceeded your per-minute rate limit"), model.RateLimited, 2 * time.Second},
		{"429 rate_limit_error rate headers", 429, tokensOut, errBody("rate_limit_error", "slow down"), model.RateLimited, 30 * time.Second},
		{"429 rate_limit_error nothing else", 429, nil, errBody("rate_limit_error", "slow down"), model.RateLimited, 0},
		{"429 cpa quota", 429, nil, cpaCoolingDown, model.QuotaExhausted, cpaReset},
		{"429 cpa quota despite retry-after", 429, hdr("retry-after", "30"), cpaCoolingDown, model.QuotaExhausted, cpaReset},
		{"429 retry-after beyond an hour", 429, hdr("retry-after", "7200"), errBody("rate_limit_error", "slow down"), model.QuotaExhausted, 2 * time.Hour},
		{"429 no anthropic error", 429, nil, `too many requests`, model.Overload, 0},
		{"529 overloaded_error", 529, nil, errBody("overloaded_error", "Overloaded"), model.Overload, 0},
		{"529 no body", 529, nil, ``, model.Overload, 0},
		{"529 api_error is still overload", 529, nil, errBody("api_error", "x"), model.Overload, 0},
		{"500 overloaded_error", 500, nil, errBody("overloaded_error", "Overloaded"), model.Overload, 0},
		{"503 overloaded_error", 503, hdr("retry-after", "5"), errBody("overloaded_error", "Overloaded"), model.Overload, 5 * time.Second},
		{"500 api_error", 500, nil, errBody("api_error", "Internal server error"), model.Transient, 0},
		{"502 html", 502, nil, `<html>bad gateway</html>`, model.Transient, 0},
		{"504 timeout_error", 504, nil, errBody("timeout_error", "Request timed out"), model.Transient, 0},
		{"400 invalid_request_error", 400, nil, errBody("invalid_request_error", "max_tokens: 99999999 > 64000"), model.Permanent, 0},
		{"400 timeout text stays permanent", 400, nil, errBody("invalid_request_error", "timeout must be at most 600"), model.Permanent, 0},
		{"400 prompt is too long", 400, nil, errBody("invalid_request_error", "prompt is too long: 210541 tokens > 200000 maximum"), model.ContextLength, 0},
		{"400 input and max_tokens exceed the limit", 400, nil, errBody("invalid_request_error", "input length and `max_tokens` exceed context limit: 197000 + 8192 > 200000, decrease input length or `max_tokens` and try again"), model.ContextLength, 0},
		{"in-stream prompt is too long", 0, nil, errBody("invalid_request_error", "prompt is too long: 210541 tokens > 200000 maximum"), model.ContextLength, 0},
		{"400 credit balance", 400, nil, errBody("invalid_request_error", "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."), model.QuotaExhausted, 0},
		{"401 authentication_error", 401, nil, errBody("authentication_error", "invalid x-api-key"), model.Permanent, 0},
		{"402 billing_error", 402, nil, errBody("billing_error", "billing"), model.QuotaExhausted, 0},
		{"403 permission_error", 403, nil, errBody("permission_error", "Your API key does not have permission to use the specified resource."), model.Permanent, 0},
		{"404 not_found_error", 404, nil, errBody("not_found_error", "model: claude-nope"), model.Permanent, 0},
		{"413 request_too_large", 413, nil, errBody("request_too_large", "Request exceeds the maximum allowed number of bytes."), model.Permanent, 0},
		{"408", 408, nil, `request timeout`, model.Transient, 0},
		{"in-stream overloaded_error", 0, nil, errBody("overloaded_error", "Overloaded"), model.Overload, 0},
		{"in-stream rate_limit_error", 0, nil, errBody("rate_limit_error", "slow down"), model.RateLimited, 0},
		{"in-stream api_error", 0, nil, errBody("api_error", "Internal server error"), model.Transient, 0},
		{"in-stream invalid_request_error", 0, nil, errBody("invalid_request_error", "bad"), model.Permanent, 0},
		{"in-stream cpa quota", 0, nil, cpaCoolingDown, model.QuotaExhausted, cpaReset},
		{"in-stream unknown type", 0, nil, errBody("brand_new_error", "?"), model.Transient, 0},
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
	const key = "sk-ant-secret-123456"
	rl := hdr("retry-after", "2", "anthropic-ratelimit-requests-limit", "50", "anthropic-ratelimit-requests-remaining", "0",
		"anthropic-ratelimit-requests-reset", "2026-10-10T12:00:02Z")
	cases := []struct {
		status int
		h      http.Header
		body   string
		want   model.ErrClass
		after  time.Duration
	}{
		{429, rl, errBody("rate_limit_error", "slow down"), model.RateLimited, 2 * time.Second},
		{429, nil, cpaCoolingDown, model.QuotaExhausted, cpaReset},
		{529, nil, errBody("overloaded_error", "Overloaded"), model.Overload, 0},
		{500, nil, errBody("api_error", "Internal server error"), model.Transient, 0},
		{400, nil, errBody("invalid_request_error", "timeout too large"), model.Permanent, 0},
		{401, nil, errBody("authentication_error", "invalid x-api-key"), model.Permanent, 0},
	}
	for _, c := range cases {
		ts := statusServer(t, c.status, c.h, c.body)
		a := New(ts.URL+"/v1", key, "m")
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
			if !strings.Contains(s, strconv.Itoa(c.status)) || !strings.Contains(s, c.body[:20]) {
				t.Errorf("%s %d: text lacks status or body: %s", how, c.status, s)
			}
			if strings.Contains(s, key) {
				t.Errorf("%s %d: error leaks the key: %s", how, c.status, s)
			}
		}
		if c.want == model.RateLimited {
			_, err := a.Call(context.Background(), hi)
			r := a.RateInfo(nil, err)
			if d, ok := r.Dim(model.DimRequests); !ok || d.Limit != 50 || d.Remaining != 0 || !d.ResetAt.Equal(now0.Add(2*time.Second)) {
				t.Errorf("rate info from a 429: %+v", r)
			}
			if !r.RetryAt.Equal(now0.Add(2 * time.Second)) {
				t.Errorf("RetryAt %v", r.RetryAt)
			}
		}
	}
}

func TestErrorsNeverCarryTheKey(t *testing.T) {
	const key = "sk-ant-live-abcdef123456"
	echo := statusServer(t, 401, nil, errBody("authentication_error", "invalid x-api-key: "+key))
	for how, err := range both(t, New(echo.URL+"/v1", key, "m"), context.Background()) {
		if err == nil || strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "invalid x-api-key") {
			t.Errorf("%s: %v", how, err)
		}
	}

	inStream, _ := sseServer(t, start(`{"input_tokens":3,"output_tokens":1}`),
		sse{"error", errBody("invalid_request_error", "bad key "+key)})
	_, _, err := stream(t, New(inStream.URL+"/v1", key, "m"))
	if err == nil || strings.Contains(err.Error(), key) {
		t.Errorf("in-stream: %v", err)
	}

	garbled := statusServer(t, 200, nil, `{"content":[`+key)
	_, err = New(garbled.URL+"/v1", key, "m").Call(context.Background(), hi)
	if err == nil || strings.Contains(err.Error(), key) {
		t.Errorf("garbled 2xx: %v", err)
	}
}

const sseOK = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

func TestRateInfoOnSuccess(t *testing.T) {
	h := hdr(
		"anthropic-ratelimit-requests-limit", "50",
		"anthropic-ratelimit-requests-remaining", "41",
		"anthropic-ratelimit-requests-reset", "2026-10-10T12:00:01Z",
		"anthropic-ratelimit-tokens-remaining", "9000",
		"anthropic-ratelimit-tokens-reset", "2026-10-10T12:01:00Z",
		"anthropic-ratelimit-input-tokens-limit", "40000",
		"anthropic-ratelimit-input-tokens-remaining", "39000",
		"anthropic-ratelimit-input-tokens-reset", "2026-10-10T12:00:06Z",
		"anthropic-ratelimit-output-tokens-limit", "8000",
		"anthropic-ratelimit-output-tokens-remaining", "7000",
		"anthropic-ratelimit-output-tokens-reset", "2026-10-10T12:00:07Z",
	)
	check := func(how string, r *model.RateInfo) {
		t.Helper()
		want := map[string]model.RateDim{
			model.DimRequests:     {Limit: 50, Remaining: 41, ResetAt: now0.Add(time.Second)},
			model.DimTokens:       {Remaining: 9000, ResetAt: now0.Add(time.Minute)},
			model.DimInputTokens:  {Limit: 40000, Remaining: 39000, ResetAt: now0.Add(6 * time.Second)},
			model.DimOutputTokens: {Limit: 8000, Remaining: 7000, ResetAt: now0.Add(7 * time.Second)},
		}
		for dim, w := range want {
			d, ok := r.Dim(dim)
			if !ok || d.Limit != w.Limit || d.Remaining != w.Remaining || !d.ResetAt.Equal(w.ResetAt) {
				t.Errorf("%s %s = %+v,%v want %+v", how, dim, d, ok, w)
			}
		}
		if r != nil && !r.At.Equal(now0) {
			t.Errorf("%s At %v", how, r.At)
		}
	}

	plain := statusServer(t, 200, h, textReply)
	a := New(plain.URL+"/v1", "k", "m")
	a.Now = func() time.Time { return now0 }
	resp, err := a.Call(context.Background(), hi)
	if err != nil {
		t.Fatal(err)
	}
	check("Call", resp.Rate)
	if a.RateInfo(&resp, nil) != resp.Rate {
		t.Error("RateInfo does not report Response.Rate")
	}

	sh := h.Clone()
	sh.Set("Content-Type", "text/event-stream")
	sseTS := statusServer(t, 200, sh, sseOK)
	b := New(sseTS.URL+"/v1", "k", "m")
	b.Now = func() time.Time { return now0 }
	resp, err = b.Stream(context.Background(), hi, func(model.Delta) {})
	if err != nil {
		t.Fatal(err)
	}
	check("Stream", b.RateInfo(&resp, nil))

	none := statusServer(t, 200, nil, textReply)
	resp, err = New(none.URL+"/v1", "k", "m").Call(context.Background(), hi)
	if err != nil || resp.Rate != nil {
		t.Fatalf("no headers: %+v %v", resp.Rate, err)
	}
	var _ model.RateReporter = a
}

func TestInStreamErrorEvent(t *testing.T) {
	cases := []struct {
		body string
		want model.ErrClass
	}{
		{errBody("overloaded_error", "Overloaded"), model.Overload},
		{errBody("rate_limit_error", "slow down"), model.RateLimited},
		{errBody("api_error", "Internal server error"), model.Transient},
		{errBody("invalid_request_error", "bad"), model.Permanent},
		{cpaCoolingDown, model.QuotaExhausted},
	}
	for _, c := range cases {
		ts, _ := sseServer(t, start(`{"input_tokens":3,"output_tokens":1}`),
			blockStart("0", `{"type":"text","text":""}`),
			delta("0", `{"type":"text_delta","text":"half"}`),
			sse{"error", c.body})
		a := New(ts.URL+"/v1", "k", "m")
		_, _, err := stream(t, a)
		me := asModelError(t, c.body, err)
		if me.Class != c.want || model.Classify(err) != c.want || me.Status != 0 || me.Provider != a.Provider() {
			t.Errorf("%s: %+v", c.body, me)
		}
		if !strings.Contains(err.Error(), "in-stream") {
			t.Errorf("%s: text does not say where: %v", c.body, err)
		}
		if errors.Is(err, model.ErrResponseLost) {
			t.Errorf("%s: an error the provider sent is an answer, not a lost response", c.body)
		}
	}
}

// cutServer promises a longer body than it sends, then drops the connection.
func cutServer(t *testing.T, ctype, body string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", ctype)
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
	lost := func(what string, err error) {
		t.Helper()
		if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
			t.Errorf("%s: %v (%s)", what, err, model.Classify(err))
		}
	}

	cut := cutServer(t, "application/json", `{"content":[{"type":"text","text":"hal`)
	_, err := New(cut.URL+"/v1", "k", "m").Call(context.Background(), hi)
	lost("cut body", err)

	garbled := statusServer(t, 200, nil, `{"content":[`)
	_, err = New(garbled.URL+"/v1", "k", "m").Call(context.Background(), hi)
	lost("garbled body", err)

	ended, _ := sseServer(t, start(`{"input_tokens":3,"output_tokens":1}`),
		blockStart("0", `{"type":"text","text":""}`),
		delta("0", `{"type":"text_delta","text":"half"}`))
	_, _, err = stream(t, New(ended.URL+"/v1", "k", "m"))
	lost("stream ended before message_stop", err)

	sseCut := cutServer(t, "text/event-stream", "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n")
	_, _, err = stream(t, New(sseCut.URL+"/v1", "k", "m"))
	lost("stream connection dropped", err)

	bad, _ := sseServer(t, start(`{"input_tokens":3,"output_tokens":1}`), sse{"content_block_delta", `{"type":"content_block_delta",`})
	_, _, err = stream(t, New(bad.URL+"/v1", "k", "m"))
	lost("undecodable stream event", err)
}

func TestNetworkErrorsAreTransient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: connection refused
	for how, err := range both(t, New("http://"+addr+"/v1", "k", "m"), context.Background()) {
		me := asModelError(t, how, err)
		if me.Class != model.Transient || model.Classify(err) != model.Transient {
			t.Errorf("%s: %+v", how, me)
		}
		if errors.Is(err, model.ErrResponseLost) {
			t.Errorf("%s: a refused connection never reached the provider, so nothing was lost", how)
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
	for how, err := range both(t, New(slow.URL+"/v1", "k", "m"), ctx) {
		me := asModelError(t, how, err)
		if me.Class != model.Canceled || model.Classify(err) != model.Canceled || !errors.Is(err, context.Canceled) {
			t.Errorf("caller cancel via %s: %v (%s)", how, err, model.Classify(err))
		}
	}

	// The caller's own deadline ends the task: it is not retryable.
	dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer dcancel()
	for how, err := range both(t, New(slow.URL+"/v1", "k", "m"), dctx) {
		me := asModelError(t, how, err)
		if me.Class != model.Canceled || me.Err != context.DeadlineExceeded || model.Classify(err) != model.Canceled {
			t.Errorf("caller deadline via %s: %v (%s)", how, err, model.Classify(err))
		}
	}

	// A per-request timeout while the caller still has time is the provider
	// being slow: transient.
	a := New(slow.URL+"/v1", "k", "m")
	a.HTTP.Timeout = 50 * time.Millisecond
	for how, err := range both(t, a, context.Background()) {
		me := asModelError(t, how, err)
		if me.Class != model.Transient || model.Classify(err) != model.Transient {
			t.Errorf("request timeout via %s: %v (%s)", how, err, model.Classify(err))
		}
	}
}

func TestCancelMidStream(t *testing.T) {
	hold := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n"+
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"+
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"a\"}}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-hold:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(hold); ts.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	_, err := New(ts.URL+"/v1", "k", "m").Stream(ctx, hi, func(model.Delta) { cancel() })
	if model.Classify(err) != model.Canceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v (%s)", err, model.Classify(err))
	}
}
