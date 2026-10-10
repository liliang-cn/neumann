// Package model is the seam between the machine and a model provider. Every
// call through an Adapter is one instruction on a vCore.
package model

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/liliang-cn/neumann/task"
)

type Request struct {
	System   string
	Tools    []task.ToolSpec
	Messages []task.Message
	// MaxOut caps the tokens the answer may use; 0 leaves it to the adapter
	// (its configured default, or the provider's). The machine raises it when
	// a turn is cut off before producing anything (design §13).
	MaxOut int
}

// Response carries the assistant turn in neutral form. Tool calls come back
// with ProviderCallID set and CallID empty; the machine assigns CallID.
type Response struct {
	Message      task.Message
	FinishReason string
	Usage        task.Usage
	// Rate is what the provider said about its limits on this response, or
	// nil when it said nothing. It is never committed: the limiter consumes
	// it once and the next response supersedes it.
	Rate *RateInfo
}

type Adapter interface {
	Provider() string
	Call(ctx context.Context, req Request) (Response, error)
}

// OutputCap is an adapter that knows the output cap it sends when
// Request.MaxOut is 0. The machine raises a truncated turn's cap from it, and
// reserves output tokens by it; without it, 4096 is assumed.
type OutputCap interface {
	MaxOut() int
}

// Caps is what an adapter's model can take (design §6.3, §11): the machine
// reads it before it moves a task onto the adapter's group.
type Caps struct {
	Window   int  // context window in tokens, input and output together; 0 = unknown, never refused
	MaxOut   int  // the output cap a request is sent with when it carries none; 0 = unknown
	Tools    bool // the model takes tool definitions and calls tools
	MaxTools int  // most tool definitions one request may carry; 0 = no limit
}

// CapsReporter is an adapter that says what its model can take.
type CapsReporter interface {
	Caps() Caps
}

// CapsOf is what a can take: its own Caps when it reports them, otherwise
// tools with no limit and an unknown window, so that an adapter that says
// nothing is never refused a task. MaxOut falls back to OutputCap.
func CapsOf(a Adapter) Caps {
	var c Caps
	if r, ok := a.(CapsReporter); ok {
		c = r.Caps()
	} else {
		c.Tools = true
	}
	if c.MaxOut <= 0 {
		if oc, ok := a.(OutputCap); ok {
			c.MaxOut = oc.MaxOut()
		}
	}
	return c
}

// Metered is an adapter that knows how its provider counts a request against
// its limits (design §7.2): Reserve is what to take from each limiter
// dimension before the request is sent, Settle what the request actually
// used once its response is in. Dimensions are named as the limiter names
// them (DimRequests, DimTokens, ...); one left out of Settle keeps its
// reserved amount. Without it the machine uses a default (see core).
type Metered interface {
	Reserve(req Request) map[string]int64
	Settle(req Request, resp Response) map[string]int64
}

// Warmer is an adapter that can open its connection ahead of the first call.
// The machine warms it in the background at Start.
type Warmer interface {
	Warm(ctx context.Context) error
}

// Delta is a piece of an answer while it is being generated. It is a preview
// for people watching: it is never committed, and the committed answer is the
// record of what the model said.
type Delta struct {
	Text      string `json:"text,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}

// Streamer is an adapter that can report a call's output as it arrives. The
// machine uses it whenever an adapter implements it; emit is called from the
// calling goroutine, in order, and must not block.
type Streamer interface {
	Stream(ctx context.Context, req Request, emit func(Delta)) (Response, error)
}

// ErrClass is what a failed model call means for the machine: whether to
// retry, whether the execution group shrinks its window or opens its breaker,
// whether the limiter pauses (design §11).
type ErrClass int

const (
	// Unknown is an error nobody classified. The loop decides what that
	// means; an adapter should not leave its own errors here.
	Unknown ErrClass = iota
	// RateLimited: the provider said "not now" and when to come back. The
	// limiter pauses to the reset; the window is unchanged; retrying does not
	// count against the task.
	RateLimited
	// QuotaExhausted: the account's quota or billing is used up. The group's
	// breaker opens and stays open until an operator resets it.
	QuotaExhausted
	// Overload: the provider is overloaded (529, 503 "overloaded", a 429 with
	// no rate-limit headers). The window halves, once per congestion epoch.
	Overload
	// Transient: any other 5xx, a network error, a per-request timeout, a
	// lost response. Retried after backoff; counts toward the breaker.
	Transient
	// Permanent: any other 4xx. Retrying sends the same rejected request.
	Permanent
	// Canceled: the caller's context was cancelled or reached its deadline.
	// Never retried.
	Canceled
)

var errClassNames = [...]string{"unknown", "rate_limited", "quota_exhausted", "overload", "transient", "permanent", "canceled"}

func (c ErrClass) String() string {
	if c >= 0 && int(c) < len(errClassNames) {
		return errClassNames[c]
	}
	return "ErrClass(" + strconv.Itoa(int(c)) + ")"
}

// Retryable reports whether the same request may succeed if sent again
// later: RateLimited, Overload and Transient.
func (c ErrClass) Retryable() bool {
	return c == RateLimited || c == Overload || c == Transient
}

// ErrResponseLost marks a request that reached the provider whose response
// never arrived whole: the body was cut, a stream ended before its finish
// reason, the bytes could not be decoded. The provider may have done the work
// and billed it, which is what distinguishes it from a request that never got
// out (a refused connection). Its class is Transient.
var ErrResponseLost = errors.New("model: response lost")

// Error is a classified failure of a model call. Adapters return it for
// every failure they can explain; Classify reads Class from it.
type Error struct {
	Class      ErrClass
	Status     int           // HTTP status; 0 when there was none (network, in-stream)
	RetryAfter time.Duration // the provider's hint, 0 when it gave none
	Provider   string
	Msg        string // what the provider said: a body snippet, never a credential
	Err        error  // the cause, if any
	// Rate is what the provider's headers said about its limits when it
	// failed, or nil.
	Rate *RateInfo
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.Provider != "" {
		b.WriteString(e.Provider)
		b.WriteString(": ")
	}
	if e.Status != 0 {
		fmt.Fprintf(&b, "status %d ", e.Status)
	}
	b.WriteString("(")
	b.WriteString(e.Class.String())
	if e.RetryAfter > 0 {
		fmt.Fprintf(&b, ", retry after %s", e.RetryAfter)
	}
	b.WriteString(")")
	if e.Msg != "" {
		b.WriteString(": ")
		b.WriteString(e.Msg)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Err }

// Classify says what err means for the machine, in this order:
//
//  1. context.Canceled anywhere in the chain is Canceled, whatever else the
//     error claims: a cancelled call is never retried.
//  2. A wrapped *Error with a class other than Unknown has that class. This
//     is how an adapter's judgement outranks everything below, and why a
//     Permanent rejection whose text says "timeout" stays Permanent: the
//     text is never read here.
//  3. context.DeadlineExceeded is Canceled. A bare deadline reaching this
//     point is the caller's: the task's Deadline (§9.4) ended the call, and
//     retrying would only run past it. A per-request timeout while the
//     caller still has time is the provider being slow — Transient — and
//     only the adapter can tell the two apart (it checks its caller's
//     ctx.Err()), so it must say so with an *Error{Class: Transient}.
//  4. A network error (net.Error), io.ErrUnexpectedEOF, a reset or refused
//     connection, a broken pipe, or ErrResponseLost is Transient.
//  5. Anything else is Unknown.
func Classify(err error) ErrClass {
	if err == nil {
		return Unknown
	}
	if errors.Is(err, context.Canceled) {
		return Canceled
	}
	var me *Error
	if errors.As(err, &me) && me.Class != Unknown {
		return me.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Canceled
	}
	var ne net.Error
	if errors.As(err, &ne) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, ErrResponseLost) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) {
		return Transient
	}
	return Unknown
}

// Classifier is an adapter that knows its provider's errors better than
// Classify does. ClassifyWith consults it first.
type Classifier interface {
	Classify(err error) ErrClass
}

// ClassifyWith classifies err from adapter a: a cancellation is Canceled
// whatever the adapter says, then the adapter's own Classifier if it has one
// and it answers something other than Unknown, then Classify.
func ClassifyWith(a Adapter, err error) ErrClass {
	if err == nil {
		return Unknown
	}
	if errors.Is(err, context.Canceled) {
		return Canceled
	}
	if c, ok := a.(Classifier); ok {
		if class := c.Classify(err); class != Unknown {
			return class
		}
	}
	return Classify(err)
}

// Rate-limit dimensions, named as the limiter names them (§7.1).
const (
	DimRequests     = "requests"
	DimTokens       = "tokens"
	DimInputTokens  = "input_tokens"
	DimOutputTokens = "output_tokens"
)

// RateDim is one dimension of a provider's limit as it reported it.
type RateDim struct {
	Limit     int64     // 0 when not reported
	Remaining int64     // what is left in the current window
	ResetAt   time.Time // when the window refills; zero when not reported
}

// RateInfo is what a provider said about its limits on one response or
// failure (§7.3). A dimension is present only when the provider reported
// how much of it remains; the limiter trusts it over its own count.
type RateInfo struct {
	Dims    map[string]RateDim
	RetryAt time.Time // retry-after as an instant; zero when not given
	At      time.Time // when it was read
}

// Dim returns one dimension; it is safe on a nil RateInfo.
func (r *RateInfo) Dim(name string) (RateDim, bool) {
	if r == nil {
		return RateDim{}, false
	}
	d, ok := r.Dims[name]
	return d, ok
}

// RateReporter is an adapter that reads rate-limit feedback from its
// provider's responses. resp is the response of a successful call (nil on
// failure); err is the call's error (nil on success).
type RateReporter interface {
	RateInfo(resp *Response, err error) *RateInfo
}

// ParseRetryAfter reads an HTTP Retry-After value: delta-seconds (integer
// or, as some providers send, fractional) or an HTTP date, relative to now.
// A date in the past means retry now.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		if !(f >= 0 && f < 1e9) { // also rejects NaN and Inf
			return 0, false
		}
		return time.Duration(f * float64(time.Second)), true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(0, t.Sub(now)), true
	}
	return 0, false
}
