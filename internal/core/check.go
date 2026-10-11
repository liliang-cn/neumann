package core

import (
	"context"
	"fmt"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// The answer check's defaults (design §9.8).
const (
	// DefaultCheckTimeout bounds one call of the answer check: past it, the
	// answer passes.
	DefaultCheckTimeout = 3 * time.Second
	// DefaultCheckRetries is how many rejected answers of a task go back to
	// the model with the check's feedback.
	DefaultCheckRetries = 1
)

// AnswerInfo is what the answer check is shown of a final answer.
type AnswerInfo struct {
	TaskID  string
	Class   string // the task's execution group
	Request string // the task's request: its first user message
	Answer  string // the final text the model just produced
	Attempt int    // 0 for the task's first checked answer, n after n rejections
}

// AnswerVerdict is the answer check's answer. The zero value passes the
// answer. Reason is for the events (keep it short); Feedback, on a
// rejection, is what the model is given as a user message before it
// answers again.
type AnswerVerdict struct {
	Reject   bool
	Reason   string
	Feedback string
}

// AnswerCheck is asked about each final answer before its task is finished
// (design §9.8), inside the model instruction whose commit finishes it: it
// runs on a goroutine of its own while the vCore that owns the task waits
// for it, at most the check timeout; a check that times out or panics
// passes the answer.
type AnswerCheck func(ctx context.Context, info AnswerInfo) AnswerVerdict

// verdict is what came of asking the check about one answer: its verdict,
// or why it gave none (err), in which case the answer passes.
type verdict struct {
	AnswerVerdict
	err string
}

func (e *Env) checkTimeout() time.Duration {
	if e.CheckTimeout > 0 {
		return e.CheckTimeout
	}
	return DefaultCheckTimeout
}

func (e *Env) checkRetries() int {
	switch {
	case e.CheckRetries < 0:
		return 0
	case e.CheckRetries == 0:
		return DefaultCheckRetries
	}
	return e.CheckRetries
}

// checkAnswer asks the answer check about answer, t's final answer. The
// check runs under the task's context and the check timeout, on a goroutine
// of its own: the vCore waits for it at most that long, whether the check
// honours its context or not, and a check given up on is not waited for
// (what it says then is never used). A check that panics, times out, or
// whose task is cancelled meanwhile passes the answer (err says why): a
// check must never hold a task or lose its answer. ("check cancelled" never
// reaches a commit: a cancel or deadline is committed before the task's
// context ends, and the step then finalises the cancel; a halt commits
// nothing. It is set only so that a verdict is never taken from a check cut
// off that way.)
func (e *Env) checkAnswer(t *task.TCB, answer string) verdict {
	tctx, release := e.taskContext(t.ID)
	defer release()
	ctx, cancel := context.WithTimeout(tctx, e.checkTimeout())
	defer cancel()
	info := AnswerInfo{TaskID: t.ID, Class: t.Class, Request: request(t), Answer: answer, Attempt: t.Rejections}
	out := make(chan verdict, 1) // buffered: a check given up on still returns
	go func() {
		defer func() {
			if v := recover(); v != nil {
				out <- verdict{err: fmt.Sprintf("check panicked: %v", v)}
			}
		}()
		out <- verdict{AnswerVerdict: e.Check(ctx, info)}
	}()
	var v verdict
	select {
	case v = <-out:
	case <-ctx.Done():
	}
	switch {
	case v.err != "":
	case tctx.Err() != nil:
		v = verdict{err: "check cancelled"}
	case ctx.Err() != nil:
		v = verdict{err: "check timed out"}
	}
	return v
}

// request is the task's request: the text of its first user message, which
// compaction always keeps.
func request(t *task.TCB) string {
	for _, m := range t.Regs {
		if m.Role == task.RoleUser {
			return m.TextContent()
		}
	}
	return ""
}

// feedbackText is what the model is told of a rejection: the check's
// feedback, or a plain request to answer again that gives its reason.
func feedbackText(v AnswerVerdict) string {
	switch {
	case v.Feedback != "":
		return v.Feedback
	case v.Reason != "":
		return "Your answer was not accepted. Reason: " + v.Reason + ". Revise it and answer again."
	}
	return "Your answer was not accepted. Revise it and answer again."
}

// answerChecked completes c, a model step whose final answer is the last
// message of t's registers, by the check's verdict on it:
//
//   - passed (or the check gave no verdict): the task is done, as without a
//     check; its answer and done events say it was checked;
//   - rejected with a retry left: the feedback follows the answer as a user
//     message, Rejections counts it, an answer_rejected event records it,
//     and the task is ready for another model step;
//   - rejected with no retry left, or with no model step left in the
//     task's budget for another answer: the task is done with that answer
//     all the same (an answer is never dropped), end_turn, an
//     answer_rejected event marked final records it (budget: true when it
//     was the budget), and its answer and done events say check_failed.
//     Sending it back with no step left would only have the next step end
//     the task on its budget with the rejected answer, unmarked.
func (e *Env) answerChecked(t *task.TCB, c mem.CommitSet, v verdict) mem.CommitSet {
	check := map[string]any{"checked": true}
	if !v.Reject {
		if v.err != "" {
			check["check_error"] = v.err
		} else if v.Reason != "" {
			check["check_reason"] = v.Reason
		}
		return e.finishWith(t, c, "end_turn", check)
	}
	attempt := t.Rejections
	t.Rejections++
	// The same bound execModel applies before a call (design §5.1).
	spent := t.Budget.MaxSteps > 0 && t.Used.Steps+t.Unconfirmed >= t.Budget.MaxSteps
	if attempt < e.checkRetries() && !spent {
		feedback := feedbackText(v.AnswerVerdict)
		t.Regs = append(t.Regs, task.FeedbackMessage(feedback))
		e.ready(t, task.StepModel)
		c.Task = t
		c.Events = append(c.Events, proto.NewEvent(proto.EvAnswerRejected, t.ID, map[string]any{
			"reason": v.Reason, "feedback": feedback, "attempt": attempt,
		}))
		return c
	}
	final := map[string]any{"reason": v.Reason, "attempt": attempt, "final": true}
	if attempt < e.checkRetries() {
		final["budget"] = true
	}
	c.Events = append(c.Events, proto.NewEvent(proto.EvAnswerRejected, t.ID, final))
	check["check_failed"] = true
	if v.Reason != "" {
		check["check_reason"] = v.Reason
	}
	return e.finishWith(t, c, "end_turn", check)
}
