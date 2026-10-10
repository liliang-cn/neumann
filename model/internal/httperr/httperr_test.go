package httperr

import (
	"net/http"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/model"
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
		{"400 context length", 400, nil, `{"error":{"code":"context_length_exceeded"}}`, model.ContextLength, 0},
		{"400 maximum context length", 400, nil, `{"error":{"message":"This model's maximum context length is 8192 tokens. However, you requested 9000 tokens."}}`, model.ContextLength, 0},
		{"400 prompt is too long", 400, nil, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210541 tokens > 200000 maximum"}}`, model.ContextLength, 0},
		{"400 input and max_tokens exceed the limit", 400, nil, `{"error":{"message":"input length and ` + "`max_tokens`" + ` exceed context limit: 197000 + 8192 > 200000"}}`, model.ContextLength, 0},
		{"413 context length", 413, nil, `{"error":{"code":"context_length_exceeded"}}`, model.ContextLength, 0},
		{"in-stream context length", 0, nil, `{"error":{"code":"context_length_exceeded","message":"too long"}}`, model.ContextLength, 0},
		{"429 context words stay a rate limit", 429, hdr("Retry-After", "2"), `{"error":{"message":"maximum context length tokens per minute"}}`, model.RateLimited, 2 * time.Second},
		{"500 context words stay transient", 500, nil, `{"error":{"message":"context_length_exceeded upstream"}}`, model.Transient, 0},
		{"401", 401, nil, `{"error":{"message":"bad key"}}`, model.Permanent, 0},
		{"404 timed out text", 404, nil, `upstream timed out looking for model`, model.Permanent, 0},
		{"422", 422, nil, `{}`, model.Permanent, 0},
	}
	for _, c := range cases {
		h := c.h
		if h == nil {
			h = http.Header{}
		}
		got, after := Classify(c.status, h, []byte(c.body), now0)
		if got != c.want || after != c.after {
			t.Errorf("%s: Classify = %s, %v; want %s, %v", c.name, got, after, c.want, c.after)
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
		got, ok := ParseReset(c.in, now0)
		if ok != c.ok || !got.Equal(c.want) {
			t.Errorf("ParseReset(%q) = %v,%v want %v,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestRateInfoFromHeaders(t *testing.T) {
	if r := RateInfo(http.Header{}, now0); r != nil {
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
	r := RateInfo(h, now0)
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
	if r := RateInfo(hdr("x-ratelimit-reset-requests", "1s"), now0); r != nil {
		if _, ok := r.Dim(model.DimRequests); ok {
			t.Fatalf("dimension without remaining: %+v", r)
		}
	}
}
