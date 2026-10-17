package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
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
		if step > ceil/2 {
			step = ceil // doubling would pass the cap, or overflow
			break
		}
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
	if d == math.MaxInt64 {
		return time.Duration(rand.Int64N(int64(d)))
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// outcomeOf maps a call's error class onto its group's outcome, one to one.
// Unknown is taken as Transient, as everywhere else in the model step; a
// context-length rejection is an answer about the request, like Permanent.
func outcomeOf(class model.ErrClass) group.Outcome {
	switch class {
	case model.RateLimited:
		return group.RateLimited
	case model.QuotaExhausted:
		return group.QuotaExhausted
	case model.Overload:
		return group.Overload
	case model.Permanent, model.ContextLength:
		return group.Permanent
	}
	return group.Transient
}

// modelFailed decides what a failed model call means for its task (design
// §11). The caller has settled the call with its limiters and its group and
// taken back the steers the call carried.
//
//   - Permanent, and a panic in the adapter: the task fails, "model_error".
//   - RateLimited with a hint (retry-after, a retry instant, an exhausted
//     dimension's reset): Waiting(Backoff) until then. Not counted against
//     the task's retries: a rate limit is the provider pacing the account,
//     not the request failing. One with no hint at all cannot be told from a
//     misclassified quota error, so it is backed off and counted like a
//     transient failure: a provider that keeps saying it ends the task
//     rather than looping forever.
//   - QuotaExhausted: the group's breaker is open until an operator resets
//     it; the task moves to the group's fallback (groupGone) or waits for
//     the group, Waiting(Group).
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
		if at, ok := e.retryAt(err, info); ok {
			return e.backoff(t, c, class, at, text)
		}
	case model.QuotaExhausted:
		g := e.groupOf(t.Class)
		if g == nil {
			return failWith(t, c, StopModelError, text)
		}
		return e.groupGone(t, c, g, retryEvent(t, class.String(), 0, "group", text))
	}
	if class != model.Overload && class != model.RateLimited {
		class = model.Transient
	}
	return e.retry(t, c, class, err)
}

// retry backs t off before it calls the model again, counted in Retries
// with exponential backoff, never shorter than the provider's retry-after;
// at the limit the task fails with "model_error". The model_retry event
// carries class as it is.
func (e *Env) retry(t *task.TCB, c mem.CommitSet, class model.ErrClass, err error) mem.CommitSet {
	text := err.Error()
	t.Retries++
	if t.Retries >= e.maxRetries() {
		return failWith(t, c, StopModelError, fmt.Sprintf("%s (%d consecutive failures)", text, t.Retries))
	}
	var floor time.Duration
	var me *model.Error
	if errors.As(err, &me) {
		floor = me.RetryAfter
	}
	return e.backoff(t, c, class, e.Clock.Now().Add(e.backoffDelay(t.Retries, floor)), text)
}

// backoff describes t waiting until at before it calls the model again, at
// the same model instruction (a compaction is tried again as one).
func (e *Env) backoff(t *task.TCB, c mem.CommitSet, class model.ErrClass, at time.Time, text string) mem.CommitSet {
	delay := max(at.Sub(e.Clock.Now()), 0)
	t.State, t.Wait, t.PC = task.Waiting, task.WaitBackoff, modelPC(t)
	t.WakeAt = at
	t.Version++
	c.Task = t
	c.Events = append(c.Events, retryEvent(t, class.String(), delay, "backoff", text))
	return c
}

// retryData is a failed call's model_retry event (design §11).
type retryData struct {
	Class   string `json:"class"`
	Attempt int    `json:"attempt"`
	DelayMS int64  `json:"delay_ms"`
	Wait    string `json:"wait"`
	Error   string `json:"error"`
	// Dropped and DroppedThink are how many characters of the task's
	// Partial and PartialThink the failed call's commit drops: the call
	// continued them and its answer was of no use (design §6.4).
	Dropped      int `json:"dropped,omitempty"`
	DroppedThink int `json:"dropped_think,omitempty"`
}

func retryEvent(t *task.TCB, class string, delay time.Duration, wait, text string) proto.Event {
	return proto.NewEvent(proto.EvModelRetry, t.ID, retryData{Class: class, Attempt: t.Retries, DelayMS: delay.Milliseconds(), Wait: wait, Error: text})
}

// amendRetry notes on the model_retry event in c how many characters of a
// kept prefix the failed call's commit drops (design §6.4). A commit
// without a model_retry event (the task failed) is left as it is.
func amendRetry(c *mem.CommitSet, dropped, droppedThink int) {
	for i, ev := range c.Events {
		if ev.Kind != proto.EvModelRetry {
			continue
		}
		var d retryData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return
		}
		d.Dropped, d.DroppedThink = dropped, droppedThink
		c.Events[i] = proto.NewEvent(proto.EvModelRetry, ev.TaskID, d)
		return
	}
}

// prefixRefused reports a failure of a continued call that is the
// provider's answer to the request it was sent (design §6.4): a whole
// response with nothing usable in it (ErrEmptyResponse), a response lost or
// undecodable after the request got there (ErrResponseLost), or a
// rejection (Permanent). A rate limit, a full quota, an overloaded
// provider, a refused connection or a server error say nothing about the
// prefix.
func prefixRefused(class model.ErrClass, err error) bool {
	switch class {
	case model.Permanent:
		return true
	case model.Transient:
		return errors.Is(err, model.ErrEmptyResponse) || errors.Is(err, model.ErrResponseLost)
	}
	return false
}

// truncated reports a turn cut off by its output cap before it produced
// anything: no text, no tool call (design §13).
func truncated(resp model.Response) bool {
	return resp.FinishReason == "length" && strings.TrimSpace(resp.Message.TextContent()) == "" && len(resp.Message.ToolCalls()) == 0
}

// redoTruncated asks again with a fourfold output cap, at most
// maxTruncationRedos times in a row, the cap never above what the group's
// limiters could grant. After that, or when the cap cannot grow any more,
// the task fails with "truncated": a redo that would carry the same cap is
// not sent. The redo was an answered call (counted), and its model_retry
// event says how large the new cap is. The raise lasts until a call is
// answered without being cut off (Truncations goes back to 0).
func (e *Env) redoTruncated(g *Group, t *task.TCB, c mem.CommitSet) mem.CommitSet {
	sent := e.sendOut(g, t)
	if sent == 0 {
		sent = e.maxOut(t)
	}
	if t.Truncations >= maxTruncationRedos {
		return failWith(t, c, StopTruncated, fmt.Sprintf("the answer was cut off at %d output tokens with nothing in it, %d times", sent, t.Truncations+1))
	}
	next := e.raisedOut(g, t, t.Truncations+1)
	if next <= sent {
		return failWith(t, c, StopTruncated, fmt.Sprintf("the answer was cut off at %d output tokens with nothing in it, and the limiters allow no more", sent))
	}
	t.Truncations++
	c.Events = append(c.Events, proto.NewEvent(proto.EvModelRetry, t.ID, map[string]any{
		"class": "truncated", "attempt": t.Truncations, "delay_ms": 0, "wait": "none", "max_out": next,
		"error": fmt.Sprintf("cut off at %d output tokens with nothing produced", sent),
	}))
	e.ready(t, task.StepModel)
	c.Task = t
	return c
}

// redoWithoutThinking asks again, at the same output cap, with the model's
// thinking off (TCB.NoThink, model.Request.NoThink): the turn was cut off
// with nothing in it but thinking (design §13), or, stopped, the model
// thought and stopped without answering (design §6.4), and the adapter can
// turn thinking off. It does not count as one of the fourfold raises: those
// come only if the turn is still cut off without thinking. Like those, the
// redo was an answered call; its model_retry event says no_think, with class
// "thought" for a thought that stopped.
func (e *Env) redoWithoutThinking(g *Group, t *task.TCB, c mem.CommitSet, stopped bool) mem.CommitSet {
	sent := e.sendOut(g, t)
	if sent == 0 {
		sent = e.maxOut(t)
	}
	t.NoThink = true
	// attempt counts what the class counts: the raises for a truncation,
	// Retries (0: this answered call reset it) for a thought.
	class, attempt, text := "truncated", t.Truncations, fmt.Sprintf("cut off at %d output tokens with nothing but thinking", sent)
	if stopped {
		class, attempt, text = classThought, t.Retries, errThought
	}
	c.Events = append(c.Events, proto.NewEvent(proto.EvModelRetry, t.ID, map[string]any{
		"class": class, "attempt": attempt, "delay_ms": 0, "wait": "none", "max_out": sent, "no_think": true,
		"error": text,
	}))
	e.ready(t, task.StepModel)
	c.Task = t
	return c
}

// The model_retry class and error text of a thought that stopped without an
// answer (design §6.4).
const (
	classThought = "thought"
	errThought   = "the model stopped after thinking, with no answer"
)

// redoThought asks again, as it was and at once, after a thought that
// stopped without an answer when thinking cannot be turned off, or was off
// already (design §6.4). The call was answered, but the turn is counted in
// Retries like a failed call, so a model that keeps doing it ends the task
// with "model_error" at the retry limit instead of looping; t.Retries is the
// count before this call. No backoff: the provider answered, it is the
// model that did not. The group saw an answer, so its breaker is not
// touched.
func (e *Env) redoThought(t *task.TCB, c mem.CommitSet) mem.CommitSet {
	t.Retries++
	if t.Retries >= e.maxRetries() {
		return failWith(t, c, StopModelError, fmt.Sprintf("%s (%d consecutive failures)", errThought, t.Retries))
	}
	c.Events = append(c.Events, retryEvent(t, classThought, 0, "none", errThought))
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
