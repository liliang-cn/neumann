package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/internal/httperr"
)

var _ model.RateReporter = (*Adapter)(nil)

// RateInfo returns the limits the provider reported with resp (on success)
// or with err (on failure); nil when it reported none. Anthropic reports
// anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-
// {limit,remaining,reset}, resets in RFC 3339.
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
	if a.APIKey != "" {
		s = strings.ReplaceAll(s, a.APIKey, "[redacted]")
	}
	return s
}

// classify is the shared classifier (httperr.Classify) plus what Anthropic
// says in its error body, {"type":"error","error":{"type":...}}:
//
//   - The shared quota rules come first, so a gateway that answers
//     rate_limit_error for a quota that resets in five days (CLIProxyAPI)
//     is QuotaExhausted, and so is a wait longer than httperr.QuotaWindow.
//   - 529 is Overload whatever the body says.
//   - Then error.type: rate_limit_error → RateLimited (with no retry hint,
//     the latest exhausted rate dimension's reset); overloaded_error →
//     Overload; api_error, timeout_error → Transient; billing_error →
//     QuotaExhausted; invalid_request_error → Permanent, except "credit
//     balance is too low", which Anthropic sends as a 400 and is the account
//     out of money; authentication_error, permission_error, not_found_error,
//     request_too_large → Permanent.
//   - A body with no error.type, or one this adapter does not know, keeps
//     the shared verdict.
//
// status 0 is an error event inside a stream.
func classify(status int, h http.Header, body []byte, now time.Time) (model.ErrClass, time.Duration) {
	class, hint := httperr.Classify(status, h, body, now)
	if class == model.QuotaExhausted || status == 529 {
		return class, hint
	}
	typ, msg := errorType(body)
	switch typ {
	case "rate_limit_error":
		if hint == 0 {
			if r := httperr.RateInfo(h, now); r != nil {
				hint = httperr.ExhaustedReset(r, now)
			}
		}
		return model.RateLimited, hint
	case "overloaded_error":
		return model.Overload, hint
	case "api_error", "timeout_error":
		return model.Transient, hint
	case "billing_error":
		return model.QuotaExhausted, hint
	case "invalid_request_error":
		if strings.Contains(strings.ToLower(msg), "credit balance") {
			return model.QuotaExhausted, hint
		}
		return model.Permanent, hint
	case "authentication_error", "permission_error", "not_found_error", "request_too_large":
		return model.Permanent, hint
	}
	return class, hint
}

// errorType reads error.type and error.message from an Anthropic error
// body; empty when the body is not one.
func errorType(body []byte) (typ, msg string) {
	var e struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil || len(e.Error) == 0 {
		return "", ""
	}
	var inner struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Error, &inner) != nil {
		return "", ""
	}
	return inner.Type, inner.Message
}

// statusError is the error for a non-2xx response.
func (a *Adapter) statusError(status int, h http.Header, body []byte) error {
	now := a.now()
	class, after := classify(status, h, body, now)
	return &model.Error{
		Class: class, Status: status, RetryAfter: after, Provider: a.Provider(),
		Msg: a.snippet(body), Rate: httperr.RateInfo(h, now),
	}
}

// streamError is an error event the provider sent inside a 2xx stream; raw
// is the whole event, so its error.type is read as in a status error.
func (a *Adapter) streamError(raw []byte, h http.Header) error {
	now := a.now()
	class, after := classify(0, h, raw, now)
	return &model.Error{
		Class: class, RetryAfter: after, Provider: a.Provider(),
		Msg: "in-stream error: " + a.snippet(raw), Rate: httperr.RateInfo(h, now),
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
		rate = httperr.RateInfo(h, a.now())
	}
	return &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: what, Err: err, Rate: rate}
}

// lostError is a 2xx response that could not be used: cut short, garbled,
// a stream that ended before message_stop.
func (a *Adapter) lostError(msg string, cause error) error {
	err := model.ErrResponseLost
	if cause != nil {
		err = fmt.Errorf("%w: %w", model.ErrResponseLost, cause)
	}
	return &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: a.redact(msg), Err: err}
}
