package core

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Defaults of a model call's retry policy (design §11, §13).
const (
	DefaultMaxRetries  = 8
	DefaultBackoffBase = time.Second
	DefaultBackoffMax  = 60 * time.Second

	// maxTruncationRedos is how often a turn cut off with nothing in it is
	// asked again with a fourfold output cap.
	maxTruncationRedos = 2
)

// Stop reasons of the model step's own failures.
const (
	StopModelError = "model_error" // a permanent rejection, or retries used up
	StopTruncated  = "truncated"   // still cut off with nothing after the redos
	StopTooLarge   = "too_large"   // the request can never fit its limiters
)

func (e *Env) maxRetries() int {
	if e.MaxRetries > 0 {
		return e.MaxRetries
	}
	return DefaultMaxRetries
}

func (e *Env) backoffBase() time.Duration {
	if e.BackoffBase > 0 {
		return e.BackoffBase
	}
	return DefaultBackoffBase
}

func (e *Env) backoffMax() time.Duration {
	if e.BackoffMax > 0 {
		return e.BackoffMax
	}
	return DefaultBackoffMax
}

// backoffDelay is the wait before retry number attempt (1-based): an
// exponential step, base·2^(attempt-1) capped at the maximum, with full
// jitter (uniform in [0, step]), but never shorter than what the provider
// asked for.
func (e *Env) backoffDelay(attempt int, floor time.Duration) time.Duration {
	step, ceil := e.backoffBase(), e.backoffMax()
	for i := 1; i < attempt && step < ceil; i++ {
		step *= 2
	}
	step = min(step, ceil)
	jitter := e.Jitter
	if jitter == nil {
		jitter = fullJitter
	}
	return max(jitter(step), floor)
}

func fullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// outcomeOf maps a call's error class onto its group's outcome, one to one.
// Unknown is taken as Transient, as everywhere else in the model step.
func outcomeOf(class model.ErrClass) group.Outcome {
	switch class {
	case model.RateLimited:
		return group.RateLimited
	case model.QuotaExhausted:
		return group.QuotaExhausted
	case model.Overload:
		return group.Overload
	case model.Permanent:
		return group.Permanent
	}
	return group.Transient
}

// modelFailed decides what a failed model call means for its task (design
// §11). The caller has settled the call with its limiters and its group and
// taken back the steers the call carried.
//
//   - Permanent, and a panic in the adapter: the task fails, "model_error".
//   - RateLimited: Waiting(Backoff) until the provider's retry time. Not
//     counted against the task's retries: a rate limit is the provider
//     pacing the account, not the request failing.
//   - QuotaExhausted: the group's breaker is open until an operator resets
//     it; the task waits for the group, Waiting(Group).
//   - Overload, Transient and Unknown: Waiting(Backoff) with exponential
//     backoff, counted in Retries; at the limit the task fails with
//     "model_error". Unknown is an error nobody could explain, and is taken as
//     Transient: more often a network hiccup than a bad request, and the
//     retry limit bounds the cost of being wrong.
//
// Every retry decision is a commit with a model_retry event; the scheduler
// arms the wake-up timer only once the task is handed back.
func (e *Env) modelFailed(t *task.TCB, c mem.CommitSet, class model.ErrClass, err error, info *model.RateInfo, panicked bool) mem.CommitSet {
	text := err.Error()
	if panicked || class == model.Permanent {
		return failWith(t, c, StopModelError, text)
	}
	switch class {
	case model.RateLimited:
		return e.backoff(t, c, class, e.retryAt(err, info), text)
	case model.QuotaExhausted:
		if e.groupOf(t.Class) == nil {
			return failWith(t, c, StopModelError, text)
		}
		return park(t, c, retryEvent(t, class.String(), 0, "group", text))
	}
	t.Retries++
	if t.Retries >= e.maxRetries() {
		return failWith(t, c, StopModelError, fmt.Sprintf("%s (%d consecutive failures)", text, t.Retries))
	}
	var floor time.Duration
	var me *model.Error
	if errors.As(err, &me) {
		floor = me.RetryAfter
	}
	if class != model.Overload {
		class = model.Transient
	}
	return e.backoff(t, c, class, e.Clock.Now().Add(e.backoffDelay(t.Retries, floor)), text)
}

// backoff describes t waiting until at before it calls the model again.
func (e *Env) backoff(t *task.TCB, c mem.CommitSet, class model.ErrClass, at time.Time, text string) mem.CommitSet {
	delay := max(at.Sub(e.Clock.Now()), 0)
	t.State, t.Wait, t.PC = task.Waiting, task.WaitBackoff, task.StepModel
	t.WakeAt = at
	t.Version++
	c.Task = t
	c.Events = append(c.Events, retryEvent(t, class.String(), delay, "backoff", text))
	return c
}

func retryEvent(t *task.TCB, class string, delay time.Duration, wait, text string) proto.Event {
	return proto.NewEvent(proto.EvModelRetry, t.ID, map[string]any{
		"class": class, "attempt": t.Retries, "delay_ms": delay.Milliseconds(), "wait": wait, "error": text,
	})
}

// truncated reports a turn cut off by its output cap before it produced
// anything: no text, no tool call (design §13).
func truncated(resp model.Response) bool {
	return resp.FinishReason == "length" && resp.Message.TextContent() == "" && len(resp.Message.ToolCalls()) == 0
}

// redoTruncated asks again with a fourfold output cap, at most
// maxTruncationRedos times in a task's life; after that the task fails with
// "truncated". The redo is a step like any other (it was counted), and its
// model_retry event says how large the new cap is.
func (e *Env) redoTruncated(t *task.TCB, c mem.CommitSet) mem.CommitSet {
	sent := e.maxOut(t)
	if t.Truncations >= maxTruncationRedos {
		return failWith(t, c, StopTruncated, fmt.Sprintf("the answer was cut off at %d output tokens with nothing in it, %d times", sent, t.Truncations+1))
	}
	t.Truncations++
	t.MaxOut = sent * 4
	c.Events = append(c.Events, proto.NewEvent(proto.EvModelRetry, t.ID, map[string]any{
		"class": "truncated", "attempt": t.Truncations, "delay_ms": 0, "wait": "none", "max_out": t.MaxOut,
		"error": fmt.Sprintf("cut off at %d output tokens with nothing produced", sent),
	}))
	e.ready(t, task.StepModel)
	c.Task = t
	return c
}

// failWith ends t Failed with a stop reason, the error text in its event.
func failWith(t *task.TCB, c mem.CommitSet, reason, text string) mem.CommitSet {
	t.State = task.Failed
	t.StopReason = reason
	t.Version++
	c.Task = t
	c.Events = append(c.Events, proto.NewEvent(proto.EvFailed, t.ID, map[string]string{"error": text, "stop_reason": reason}))
	return c
}
