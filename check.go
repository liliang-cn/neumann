package neumann

import "github.com/liliang-cn/neumann/internal/core"

// AnswerCheck is asked about a final answer before its task is finished
// (design §9.8): Config.AnswerCheck. It is called once per final answer the
// model gives (a reply with no tool calls, not cut off, with no steer
// arriving during the call), on the goroutine that owns the task, after the
// model call and before the commit that would finish the task. It is not
// asked about an answer a task's budget ended with, nor about compactions,
// failures or cancellations.
//
// Its verdict exists only through that commit: a crash or a cancel before
// it lands discards it, and a restart redoes the model step and asks again
// about the new answer. Keep it fast and free of side effects. It runs
// under the task's context with Config.AnswerCheckTimeout (default 3s) on
// top, on a goroutine of its own, so the vCore never waits for it longer
// than that, whether it honours its context or not. A check that times out,
// panics or whose task is cancelled meanwhile passes the answer: the done
// event records why (check_error).
//
// A rejection with a retry left (Config.MaxAnswerRetries, default 1) puts
// the answer and its feedback, as a user message, into the registers, emits
// answer_rejected, and the model answers again; each answer is a model call
// like any other, so Budget.MaxSteps bounds the loop too. A rejection past
// the retries finishes the task with that answer anyway: an answer is never
// dropped, and answer_rejected (final: true) and the done event's
// check_failed say so.
type AnswerCheck = core.AnswerCheck

// AnswerInfo is what an AnswerCheck is shown: the task's id and class, its
// request (the first user message: Instruction.Input), the answer, and the
// attempt (0 for the first answer checked, n after n rejections).
type AnswerInfo = core.AnswerInfo

// AnswerVerdict is an AnswerCheck's answer; the zero value passes the
// answer. Reject it with a short Reason (for the events) and the Feedback
// the model is given (empty: a plain request to revise, with the reason).
type AnswerVerdict = core.AnswerVerdict
