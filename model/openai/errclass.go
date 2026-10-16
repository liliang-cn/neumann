package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/internal/httperr"
)

var _ model.RateReporter = (*Adapter)(nil)

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
	class, after := httperr.Classify(status, h, body, now)
	return &model.Error{
		Class: class, Status: status, RetryAfter: after, Provider: a.Provider(),
		Msg: a.redact(snippet(body)), Rate: httperr.RateInfo(h, now),
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
	class, after := httperr.Classify(status, h, raw, now)
	return &model.Error{
		Class: class, Status: status, RetryAfter: after, Provider: a.Provider(),
		Msg: "in-stream error: " + a.redact(snippet(raw)), Rate: httperr.RateInfo(h, now),
	}
}

// ioError is a failure talking to the provider. If the caller's context is
// done, that is the reason, and the call is Canceled — including the
// caller's deadline. Otherwise it is Transient: a network error, or a
// per-request timeout (Timeout, FirstByteTimeout, IdleTimeout, or the
// client's own) while the caller still has time.
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

// lostError is a 2xx response that could not be used: cut short, garbled.
func (a *Adapter) lostError(msg string, cause error) error {
	err := model.ErrResponseLost
	if cause != nil {
		err = fmt.Errorf("%w: %w", model.ErrResponseLost, cause)
	}
	return &model.Error{Class: model.Transient, Provider: a.Provider(), Msg: a.redact(msg), Err: err}
}
