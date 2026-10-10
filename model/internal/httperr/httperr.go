// Package httperr is the provider-neutral half of classifying a failed model
// call over HTTP (design §11): what a status, its headers and its body mean,
// the retry hints providers put in either, and the rate-limit headers.
// Adapters add their own provider's specifics on top.
package httperr

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
)

// QuotaWindow separates a rate limit from an exhausted quota by how long the
// provider says to wait: a per-minute or per-hour window refills while a
// task waits; a reset further away than this is a quota the account has used
// up, and retrying until then only burns attempts.
const QuotaWindow = time.Hour

// Classify decides a failure's class from its status, headers and body, and
// the provider's retry hint. status 0 is an error sent inside a stream with
// no numeric code. The status decides first: the body is read only to tell
// quota from rate limit among 429/403/402, and overload from other 5xx — so
// a 4xx whose text says "timeout" stays Permanent.
func Classify(status int, h http.Header, body []byte, now time.Time) (model.ErrClass, time.Duration) {
	flat := strings.NewReplacer(" ", "", "_", "", "-", "", "\\n", "", "\n", "").Replace(strings.ToLower(string(body)))
	headerAfter, hasHeader := RetryAfterHeader(h, now)
	bodyAfter, hasBody := BodyRetryAfter(body)
	hint := max(headerAfter, bodyAfter)
	overloaded := strings.Contains(flat, "overload")

	switch {
	case status == 402:
		return model.QuotaExhausted, hint
	case status == 429 || status == 403 || status == 0:
		strong := ContainsAny(flat, "insufficientquota", "billing", "creditbalance", "quotareached")
		weak := ContainsAny(flat, "quota", "resourceexhausted")
		short := hasHeader || hasBody || ContainsAny(flat, "perminute", "persecond")
		if hint > QuotaWindow || strong || (weak && !short) {
			return model.QuotaExhausted, hint
		}
	}

	switch {
	case status == 429:
		if hasHeader || hasBody {
			return model.RateLimited, hint
		}
		if r := RateInfo(h, now); r != nil && len(r.Dims) > 0 && !overloaded {
			return model.RateLimited, ExhaustedReset(r, now)
		}
		return model.Overload, 0
	case status == 529:
		return model.Overload, hint
	case status == 0:
		if overloaded || strings.Contains(flat, "ratelimit") {
			return model.Overload, hint
		}
		return model.Transient, hint
	case status >= 500:
		if overloaded {
			return model.Overload, hint
		}
		return model.Transient, hint
	case status == 408:
		return model.Transient, hint
	default:
		return model.Permanent, hint
	}
}

func ContainsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// RetryAfterHeader reads retry-after-ms (OpenAI, Azure), then Retry-After.
func RetryAfterHeader(h http.Header, now time.Time) (time.Duration, bool) {
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f < 1e12 {
			return time.Duration(f * float64(time.Millisecond)), true
		}
	}
	return model.ParseRetryAfter(h.Get("Retry-After"), now)
}

// Retry hints providers put in the body: Google's RetryInfo.retryDelay (also
// when a gateway quotes it as escaped JSON inside a message), "Resets in
// 120h41m12s" (CLIProxyAPI relaying an upstream quota), and OpenAI's "Please
// try again in 6m0s".
var bodyHints = []*regexp.Regexp{
	regexp.MustCompile(`retryDelay\\?"\s*:\s*\\?"([0-9][0-9.]*s)`),
	regexp.MustCompile(`(?i)\bresets?\s+in\s+([0-9][0-9.dhmsµu]*)`),
	regexp.MustCompile(`(?i)\btry\s+again\s+in\s+([0-9][0-9.dhmsµu]*)`),
}

// BodyRetryAfter returns the longest retry hint in the body.
func BodyRetryAfter(body []byte) (time.Duration, bool) {
	var (
		best  time.Duration
		found bool
	)
	for _, re := range bodyHints {
		for _, m := range re.FindAllSubmatch(body, -1) {
			if d, ok := ParseDuration(strings.TrimRight(string(m[1]), ".")); ok {
				best, found = max(best, d), true
			}
		}
	}
	return best, found
}

// ParseDuration is time.ParseDuration plus a leading day count ("1d2h").
func ParseDuration(v string) (time.Duration, bool) {
	var days time.Duration
	if i := strings.IndexByte(v, 'd'); i > 0 {
		n, err := strconv.Atoi(v[:i])
		if err != nil || n < 0 {
			return 0, false
		}
		days, v = time.Duration(n)*24*time.Hour, v[i+1:]
		if v == "" {
			return days, true
		}
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, false
	}
	return days + d, true
}

// ParseReset reads a rate-limit reset header: a duration ("6m0s", "1s",
// "120ms" — OpenAI), seconds (fractional allowed), a Unix time, or an
// absolute time (RFC 3339 — Anthropic — or an HTTP date).
func ParseReset(v string, now time.Time) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	if d, ok := ParseDuration(v); ok {
		return now.Add(d), true
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f < 1e12 {
		if f > 1e9 { // a Unix timestamp, not a wait
			return time.Unix(0, int64(f*1e9)).In(now.Location()), true
		}
		return now.Add(time.Duration(f * float64(time.Second))), true
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// rateDims maps the limiter's dimension names to how providers spell them in
// headers.
var rateDims = []struct{ dim, header string }{
	{model.DimRequests, "requests"},
	{model.DimTokens, "tokens"},
	{model.DimInputTokens, "input-tokens"},
	{model.DimOutputTokens, "output-tokens"},
}

// RateInfo reads OpenAI's x-ratelimit-{limit,remaining,reset}-<dim> and
// Anthropic's anthropic-ratelimit-<dim>-{limit,remaining,reset} (gateways
// pass either through), plus retry-after. nil when none is present.
func RateInfo(h http.Header, now time.Time) *model.RateInfo {
	get := func(dim, field string) string {
		if v := h.Get("x-ratelimit-" + field + "-" + dim); v != "" {
			return v
		}
		return h.Get("anthropic-ratelimit-" + dim + "-" + field)
	}
	r := &model.RateInfo{At: now}
	for _, d := range rateDims {
		remaining, err := strconv.ParseInt(strings.TrimSpace(get(d.header, "remaining")), 10, 64)
		if err != nil {
			continue
		}
		rd := model.RateDim{Remaining: remaining}
		if n, err := strconv.ParseInt(strings.TrimSpace(get(d.header, "limit")), 10, 64); err == nil {
			rd.Limit = n
		}
		if t, ok := ParseReset(get(d.header, "reset"), now); ok {
			rd.ResetAt = t
		}
		if r.Dims == nil {
			r.Dims = map[string]model.RateDim{}
		}
		r.Dims[d.dim] = rd
	}
	if d, ok := RetryAfterHeader(h, now); ok {
		r.RetryAt = now.Add(d)
	}
	if r.Dims == nil && r.RetryAt.IsZero() {
		return nil
	}
	return r
}

// ExhaustedReset is how long until the latest exhausted dimension refills.
func ExhaustedReset(r *model.RateInfo, now time.Time) time.Duration {
	var d time.Duration
	for _, dim := range r.Dims {
		if dim.Remaining <= 0 && !dim.ResetAt.IsZero() {
			d = max(d, dim.ResetAt.Sub(now))
		}
	}
	return d
}
