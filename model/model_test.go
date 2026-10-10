package model

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestErrClassString(t *testing.T) {
	want := map[ErrClass]string{
		Unknown:        "unknown",
		RateLimited:    "rate_limited",
		QuotaExhausted: "quota_exhausted",
		Overload:       "overload",
		Transient:      "transient",
		Permanent:      "permanent",
		Canceled:       "canceled",
		ErrClass(99):   "ErrClass(99)",
	}
	for c, s := range want {
		if c.String() != s {
			t.Errorf("%d.String() = %q, want %q", int(c), c.String(), s)
		}
	}
	for _, c := range []ErrClass{RateLimited, Overload, Transient} {
		if !c.Retryable() {
			t.Errorf("%s must be retryable", c)
		}
	}
	for _, c := range []ErrClass{Unknown, QuotaExhausted, Permanent, Canceled} {
		if c.Retryable() {
			t.Errorf("%s must not be retryable", c)
		}
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	perm := &Error{Class: Permanent, Status: 400, Msg: "request timeout exceeded the model's limit"}
	cases := []struct {
		name string
		err  error
		want ErrClass
	}{
		{"nil", nil, Unknown},
		{"plain", errors.New("boom"), Unknown},
		{"canceled", context.Canceled, Canceled},
		{"wrapped canceled", fmt.Errorf("openai: %w", context.Canceled), Canceled},
		{"canceled inside a url error", &url.Error{Op: "Post", URL: "http://x", Err: context.Canceled}, Canceled},
		{"caller deadline", context.DeadlineExceeded, Canceled},
		{"caller deadline inside a url error", &url.Error{Op: "Post", URL: "http://x", Err: context.DeadlineExceeded}, Canceled},
		{"model error class", &Error{Class: RateLimited}, RateLimited},
		{"wrapped model error", fmt.Errorf("call: %w", &Error{Class: QuotaExhausted}), QuotaExhausted},
		{"per-request timeout the adapter marked transient", &Error{Class: Transient, Err: context.DeadlineExceeded}, Transient},
		{"canceled outranks a model error", &Error{Class: Transient, Err: context.Canceled}, Canceled},
		{"permanent beats timeout text", perm, Permanent},
		{"permanent beats a wrapped net timeout", &Error{Class: Permanent, Err: timeoutErr{}}, Permanent},
		{"unknown model error falls through to its cause", &Error{Err: io.ErrUnexpectedEOF}, Transient},
		{"net error", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, Transient},
		{"net timeout", timeoutErr{}, Transient},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), Transient},
		{"connection reset", fmt.Errorf("read: %w", syscall.ECONNRESET), Transient},
		{"broken pipe", syscall.EPIPE, Transient},
		{"lost response", ErrResponseLost, Transient},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("%s: Classify = %s, want %s", c.name, got, c.want)
		}
	}
}

type overrider struct{ class ErrClass }

func (overrider) Provider() string { return "o" }
func (overrider) Call(context.Context, Request) (Response, error) {
	return Response{}, nil
}
func (o overrider) Classify(error) ErrClass { return o.class }

func TestClassifyWith(t *testing.T) {
	plain := errors.New("x")
	if got := ClassifyWith(overrider{Overload}, plain); got != Overload {
		t.Fatalf("adapter classifier ignored: %s", got)
	}
	if got := ClassifyWith(overrider{Unknown}, io.ErrUnexpectedEOF); got != Transient {
		t.Fatalf("an Unknown from the adapter must fall back to Classify: %s", got)
	}
	if got := ClassifyWith(overrider{Transient}, fmt.Errorf("x: %w", context.Canceled)); got != Canceled {
		t.Fatalf("no adapter may make a cancellation retryable: %s", got)
	}
	if got := ClassifyWith(nil, io.ErrUnexpectedEOF); got != Transient {
		t.Fatalf("nil adapter: %s", got)
	}
}

func TestErrorText(t *testing.T) {
	cause := errors.New("cause")
	e := &Error{Class: RateLimited, Status: 429, RetryAfter: 2 * time.Second, Provider: "openai:m", Msg: `{"error":"slow down"}`, Err: cause}
	s := e.Error()
	for _, want := range []string{"openai:m", "429", "rate_limited", "2s", "slow down", "cause"} {
		if !strings.Contains(s, want) {
			t.Errorf("%q lacks %q", s, want)
		}
	}
	if !errors.Is(e, cause) {
		t.Fatal("Unwrap does not reach the cause")
	}
	var got *Error
	if !errors.As(fmt.Errorf("w: %w", e), &got) || got != e {
		t.Fatal("errors.As")
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"2", 2 * time.Second, true},
		{" 30 ", 30 * time.Second, true},
		{"1.5", 1500 * time.Millisecond, true},
		{"0", 0, true},
		{"Sat, 10 Oct 2026 12:00:07 GMT", 7 * time.Second, true},
		{"Sat, 10 Oct 2026 11:59:00 GMT", 0, true}, // in the past: retry now
		{"", 0, false},
		{"-3", 0, false},
		{"soon", 0, false},
		{"NaN", 0, false},
		{"Inf", 0, false},
	}
	for _, c := range cases {
		got, ok := ParseRetryAfter(c.in, now)
		if got != c.want || ok != c.ok {
			t.Errorf("ParseRetryAfter(%q) = %v,%v want %v,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestRateInfoDim(t *testing.T) {
	var nilInfo *RateInfo
	if _, ok := nilInfo.Dim(DimRequests); ok {
		t.Fatal("nil RateInfo reports a dimension")
	}
	r := &RateInfo{Dims: map[string]RateDim{DimTokens: {Remaining: 0, ResetAt: time.Unix(10, 0)}}}
	d, ok := r.Dim(DimTokens)
	if !ok || d.Remaining != 0 || !d.ResetAt.Equal(time.Unix(10, 0)) {
		t.Fatalf("Dim = %+v,%v", d, ok)
	}
}
