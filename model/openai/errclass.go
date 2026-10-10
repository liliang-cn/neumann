package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
)

var _ model.RateReporter = (*Adapter)(nil)

// quotaWindow separates a rate limit from an exhausted quota by how long the
// provider says to wait: a per-minute or per-hour window refills while a
// task waits; a reset further away than this is a quota the account has used
// up, and retrying until then only burns attempts.
const quotaWindow = time.Hour

// RateInfo returns the limits the provider reported with resp (on success)
// or with err (on failure); nil when it reported none.
func (a *Adapter) RateInfo(resp *model.Response, err error) *model.RateInfo {
	if resp != nil && resp.Rate != nil {
		return resp.Rate
	}
	var me *model.Error
	if errors.As(err, &me) {
		return me.Rate
	}
	return nil
}

func (a *Adapter) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// redact removes the API key from text that came from the provider: some
// echo the key they rejected, and an error message is logged and stored.
func (a *Adapter) redact(s string) string {
	if len(a.APIKey) >= 8 {
		s = strings.ReplaceAll(s, a.APIKey, "[redacted]")
	}
	return s
}

// statusError is the error for a non-2xx response.
func (a *Adapter) statusError(status int, h http.Header, body []byte) error {
	now := a.now()
	class, after := classify(status, h, body, now)
	return &model.Error{
		Class: class, Status: status, RetryAfter: after, Provider: a.Provider(),
		Msg: a.redact(snippet(body)), Rate: rateInfo(h, now),
	}
}

// streamError is an error object the provider sent inside a 2xx stream. Its
// code, when numeric, stands in for the status the response could not carry.
func (a *Adapter) streamError(raw json.RawMessage, h http.Header) error {
	var e struct {
		Code json.RawMessage `json:"code"`
	}
	json.Unmarshal(raw, &e)
	status := 0
	if n, err := strconv.Atoi(strings.TrimSpace(string(e.Code))); err == nil && n >= 100 && n <= 599 {
		status = n
	}
	now := a.now()
	class, after := classify(status, h, raw, now)
	return &model.Error{
		Class: class, Status: status, RetryAfter: after, Provider: a.Provider(),
		Msg: "in-stream error: " + a.redact(snippet(raw)), Rate: rateInfo(h, now),
	}
}

// ioError is a failure talking to the provider. If the caller's context is
// done, that is the reason, and the call is Canceled — including the
// caller's deadline. Otherwise it is Transient: a network error, or a
// per-request timeout (http.Client.Timeout) while the caller still has time.
// lost says the request had reached the provider: its response was under way.
func (a *Adapter) ioError(ctx context.Context, what string, err error, lost bool, h http.Header) error {
	if cerr := ctx.Err(); cerr != nil {
		return &model.Error{Class: model.Canceled, Provider: a.Provider(), Msg: a.redact(what + ": " + err.Error()), Err: cerr}
	}
	if lost {
		err = fmt.Errorf("%w: %w", model.ErrResponseLost, err)
	}
	var rate *model.RateInfo
	if h != nil {
		rate = rateInfo(h, a.now())
	}
	return &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: what, Err: err, Rate: rate}
}

// lostError is a 2xx response that could not be used: cut short, garbled.
func (a *Adapter) lostError(msg string, cause error) error {
	err := model.ErrResponseLost
	if cause != nil {
		err = fmt.Errorf("%w: %w", model.ErrResponseLost, cause)
	}
	return &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: a.redact(msg), Err: err}
}

// classify decides a failure's class from its status, headers and body, and
// the provider's retry hint. status 0 is an error sent inside a stream with
// no numeric code. The status decides first: the body is read only to tell
// quota from rate limit among 429/403/402, and overload from other 5xx — so
// a 4xx whose text says "timeout" stays Permanent.
func classify(status int, h http.Header, body []byte, now time.Time) (model.ErrClass, time.Duration) {
	flat := strings.NewReplacer(" ", "", "_", "", "-", "", "\\n", "", "\n", "").Replace(strings.ToLower(string(body)))
	headerAfter, hasHeader := retryAfterHeader(h, now)
	bodyAfter, hasBody := bodyRetryAfter(body)
	hint := max(headerAfter, bodyAfter)
	overloaded := strings.Contains(flat, "overload")

	switch {
	case status == 402:
		return model.QuotaExhausted, hint
	case status == 429 || status == 403 || status == 0:
		strong := containsAny(flat, "insufficientquota", "billing", "creditbalance", "quotareached")
		weak := containsAny(flat, "quota", "resourceexhausted")
		short := hasHeader || hasBody || containsAny(flat, "perminute", "persecond")
		if hint > quotaWindow || strong || (weak && !short) {
			return model.QuotaExhausted, hint
		}
	}

	switch {
	case status == 429:
		if hasHeader || hasBody {
			return model.RateLimited, hint
		}
		if r := rateInfo(h, now); r != nil && len(r.Dims) > 0 && !overloaded {
			return model.RateLimited, exhaustedReset(r, now)
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

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// retryAfterHeader reads retry-after-ms (OpenAI, Azure), then Retry-After.
func retryAfterHeader(h http.Header, now time.Time) (time.Duration, bool) {
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

// bodyRetryAfter returns the longest retry hint in the body.
func bodyRetryAfter(body []byte) (time.Duration, bool) {
	var (
		best  time.Duration
		found bool
	)
	for _, re := range bodyHints {
		for _, m := range re.FindAllSubmatch(body, -1) {
			if d, ok := parseDuration(strings.TrimRight(string(m[1]), ".")); ok {
				best, found = max(best, d), true
			}
		}
	}
	return best, found
}

// parseDuration is time.ParseDuration plus a leading day count ("1d2h").
func parseDuration(v string) (time.Duration, bool) {
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

// parseReset reads a rate-limit reset header: a duration ("6m0s", "1s",
// "120ms" — OpenAI), seconds (fractional allowed), a Unix time, or an
// absolute time (RFC 3339 — Anthropic — or an HTTP date).
func parseReset(v string, now time.Time) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	if d, ok := parseDuration(v); ok {
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

// rateInfo reads OpenAI's x-ratelimit-{limit,remaining,reset}-<dim> and
// Anthropic's anthropic-ratelimit-<dim>-{limit,remaining,reset} (gateways
// pass either through), plus retry-after. nil when none is present.
func rateInfo(h http.Header, now time.Time) *model.RateInfo {
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
		if t, ok := parseReset(get(d.header, "reset"), now); ok {
			rd.ResetAt = t
		}
		if r.Dims == nil {
			r.Dims = map[string]model.RateDim{}
		}
		r.Dims[d.dim] = rd
	}
	if d, ok := retryAfterHeader(h, now); ok {
		r.RetryAt = now.Add(d)
	}
	if r.Dims == nil && r.RetryAt.IsZero() {
		return nil
	}
	return r
}

// exhaustedReset is how long until the latest exhausted dimension refills.
func exhaustedReset(r *model.RateInfo, now time.Time) time.Duration {
	var d time.Duration
	for _, dim := range r.Dims {
		if dim.Remaining <= 0 && !dim.ResetAt.IsZero() {
			d = max(d, dim.ResetAt.Sub(now))
		}
	}
	return d
}
