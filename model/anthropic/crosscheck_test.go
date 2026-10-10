package anthropic

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
)

// The two adapters share one classifier. On a failure that says nothing
// Anthropic-specific — a status, retry and rate headers, a body with no
// Anthropic error.type — they must reach the same verdict, so that moving a
// task between them never changes what an error means.
func TestAdaptersAgreeOnNeutralFailures(t *testing.T) {
	rl := hdr("x-ratelimit-limit-tokens", "30000", "x-ratelimit-remaining-tokens", "0", "x-ratelimit-reset-tokens", "6m0s")
	arl := hdr("anthropic-ratelimit-requests-remaining", "0", "anthropic-ratelimit-requests-reset", "2026-10-10T12:00:30Z")
	cases := []struct {
		status int
		h      http.Header
		body   string
	}{
		{429, hdr("Retry-After", "2"), `{}`},
		{429, hdr("retry-after-ms", "1500"), `{}`},
		{429, hdr("Retry-After", "86400"), `{}`},
		{429, rl, `{"error":{"message":"Rate limit reached on tokens per min"}}`},
		{429, arl, `{}`},
		{429, nil, `{"error":{"message":"too many requests"}}`},
		{429, nil, `{"error":{"message":"quota exceeded"}}`},
		{429, nil, `{"error":{"message":"You exceeded your current quota","type":"insufficient_quota"}}`},
		{429, nil, `{"error":{"message":"cooling down. Resets in 45s."}}`},
		{429, nil, `{"error":{"code":429,"message":"Individual quota reached. Resets in 120h41m12s.","status":"RESOURCE_EXHAUSTED"}}`},
		{402, nil, `{}`},
		{403, nil, `{"error":{"message":"billing is not active for this account"}}`},
		{403, nil, `{"error":{"message":"forbidden"}}`},
		{529, nil, `{}`},
		{503, nil, `{"error":{"message":"model is overloaded"}}`},
		{503, hdr("Retry-After", "5"), `unavailable`},
		{500, nil, `{"error":{"message":"internal"}}`},
		{502, nil, `bad gateway`},
		{408, nil, `request timeout`},
		{400, nil, `{"error":{"message":"timeout must be at most 600"}}`},
		{401, nil, `{"error":{"message":"bad key"}}`},
		{404, nil, `upstream timed out looking for model`},
	}
	clock := func() time.Time { return now0 }
	for _, c := range cases {
		ts := statusServer(t, c.status, c.h, c.body)
		o := openai.New(ts.URL+"/v1", "k", "m")
		o.Now = clock
		a := New(ts.URL+"/v1", "k", "m")
		a.Now = clock
		_, oerr := o.Call(context.Background(), hi)
		_, aerr := a.Call(context.Background(), hi)
		var om, am *model.Error
		if !errors.As(oerr, &om) || !errors.As(aerr, &am) {
			t.Fatalf("%d %s: %v / %v", c.status, c.body, oerr, aerr)
		}
		if om.Class != am.Class || om.RetryAfter != am.RetryAfter || om.Status != am.Status {
			t.Errorf("%d %v %s: openai %s %v, anthropic %s %v", c.status, c.h, c.body, om.Class, om.RetryAfter, am.Class, am.RetryAfter)
		}
		if !reflect.DeepEqual(om.Rate, am.Rate) {
			t.Errorf("%d %v: rate openai %+v, anthropic %+v", c.status, c.h, om.Rate, am.Rate)
		}
	}
}
