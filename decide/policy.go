package decide

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
)

// NeedsApproval is the name Policy asks its own question under.
const NeedsApproval = "needs_approval"

// Policy defaults.
const (
	DefaultPolicyTimeout   = 2 * time.Second
	DefaultPolicyThreshold = 0   // every yes holds the call
	DefaultMaxArgs         = 512 // bytes of a call's arguments shown to the engine; see MaxArgs
)

// DefaultApprovalQuestion is the question Policy asks unless
// PolicyConfig.Question replaces it: is this call irreversible or harmful
// enough that a person should approve it first.
func DefaultApprovalQuestion() Question {
	return Noul("An AI agent is about to make this tool call on its own. Is it destructive, irreversible "+
		"or harmful enough (it deletes data, moves money, sends messages to people, changes permissions "+
		"or production systems) that a human should approve it first?").
		Describe("yes", "destroys or removes data, spends money, or reaches people or systems outside in a way that cannot be undone").
		Describe("no", "reads, creates or appends something harmless and easily undone")
}

// PolicyConfig configures Policy. The zero value asks
// DefaultApprovalQuestion, holds every call the engine answers yes for, and
// waits DefaultPolicyTimeout for an answer. As in RouterConfig and
// CheckerConfig, a zero number takes the package default and a negative one
// turns the bound off.
type PolicyConfig struct {
	// Question replaces DefaultApprovalQuestion; it must be a noul whose
	// yes means "hold the call". It is asked first, under the name
	// NeedsApproval.
	Question Question
	// Threshold is the confidence a yes needs to hold the call (see
	// Answer.Confidence; 0.5, say, needs a probability of yes of at least
	// 0.75). Zero takes DefaultPolicyThreshold; negative: every yes holds.
	// It must be below 1.
	Threshold float64
	// Timeout bounds each engine request (zero: DefaultPolicyTimeout; it
	// cannot be turned off). The policy runs on a vCore and the machine
	// gives it no timeout of its own; this is the one that keeps a slow
	// engine from stalling the group.
	Timeout time.Duration
	// MaxArgs is about how many bytes of a call's arguments the engine is
	// shown: every key of an object, its long string values cut in the
	// middle, each cut marked (zero: DefaultMaxArgs;
	// negative: all of them, which a long argument can push past the
	// model's context). What the engine reads costs time: tev1:4b, its
	// cache missed, reads some 700 tokens a second on an Apple-silicon Mac,
	// and dense text such as base64 is close to a token a byte, so 512
	// bytes of it take about a second of the 2 the policy waits.
	MaxArgs int
	// Tools adds a line about a tool to what the engine is shown, keyed by
	// the tool's full name (<device>_<tool>): its description, say. A
	// name says little; a sentence about what the tool does helps the
	// engine more than anything else here.
	Tools map[string]string
	// Extra questions are asked in the same request, in order, after
	// NeedsApproval; their answers reach Decide. None may be named
	// NeedsApproval.
	Extra []Ask
	// Decide, if set, makes the final decision once the engine has
	// answered: answers holds every answer (NeedsApproval's and Extra's), d
	// is what the policy would decide on its own. It runs on the vCore like
	// the policy: keep it fast and free of side effects.
	Decide func(info neumann.ToolCallInfo, answers map[string]Answer, d neumann.Decision) neumann.Decision
	// Fallback decides a call the engine did not answer (an error, no
	// answer within Timeout, an answer of the wrong type). The default
	// holds the call for approval, with a reason that says the engine gave
	// no answer: every call that gets this far is not ReadOnly, and an
	// Idempotent call (a delete by id) can do as much harm as any.
	Fallback func(info neumann.ToolCallInfo, err error) neumann.Decision
	// Logf reports engine failures (default log.Printf). An allowed call's
	// record keeps no reason, so this is where a failure shows when a
	// Fallback lets the call through.
	Logf func(format string, args ...any)
}

// Policy is a tool policy that asks engine whether each call needs a person
// (design §9.7). A ReadOnly call is allowed without asking. For any other,
// it asks the engine in one request, bounded by cfg.Timeout whether or not
// the engine honours its context:
//
//   - yes with Confidence at least cfg.Threshold: RequireApproval, with a
//     reason naming the engine, the question and the probability of yes,
//     e.g. "systemone:tev1:4b: needs_approval yes, p=0.97";
//   - otherwise Allow;
//   - no answer: cfg.Fallback.
//
// cfg.Decide, when set, has the last word on an answered call. The reason of
// a held or denied call is committed with its call record and shown in the
// approval_needed event: that is the record of what the engine said. The
// engine's requests are not instructions: they are not counted as steps or
// metered by any group or limiter.
//
// The policy is called once per call, in call order, so a round of n calls
// that are not ReadOnly waits for up to n engine requests while it holds its
// vCore. Start does the same while it recovers, for every planned call
// that was never decided: with the engine down, Start takes up to n ×
// Timeout for n such calls. A server that answers one request at a time
// queues concurrent calls from several vCores behind each other, each
// within its own Timeout.
//
// Policy panics on a nil engine or an invalid cfg: both are programming
// errors, found when the machine is configured rather than on its first
// tool call.
func Policy(engine Engine, cfg PolicyConfig) neumann.ToolPolicy {
	if engine == nil {
		panic("decide: Policy needs an engine")
	}
	q := cfg.Question
	if q.Type == "" && q.Instructions == "" {
		q = DefaultApprovalQuestion()
	}
	if q.Type != TypeNoul {
		panic(fmt.Sprintf("decide: Policy asks a noul, not a %q", q.Type))
	}
	asks := append([]Ask{{Name: NeedsApproval, Question: q}}, cfg.Extra...)
	if err := Validate(asks); err != nil {
		panic(fmt.Sprintf("decide: Policy: %v", err))
	}
	threshold, err := bound("Policy threshold", cfg.Threshold, DefaultPolicyThreshold)
	if err != nil {
		panic(err.Error())
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultPolicyTimeout
	}
	if cfg.MaxArgs == 0 {
		cfg.MaxArgs = DefaultMaxArgs
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	name := engine.Name()
	fallback := cfg.Fallback
	if fallback == nil {
		fallback = func(_ neumann.ToolCallInfo, err error) neumann.Decision {
			return neumann.RequireApproval(failReason(name, err) + "; held")
		}
	}
	return func(ctx context.Context, info neumann.ToolCallInfo) neumann.Decision {
		if info.Effect == bus.ReadOnly {
			return neumann.Allow()
		}
		answers, err := ask(ctx, engine, cfg.Timeout, CallState(info, cfg.Tools[info.Tool], cfg.MaxArgs), asks)
		if err != nil {
			if ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
				cfg.Logf("decide: policy for %s call %s/%s: %s: %s", info.Tool, info.TaskID, info.CallID, name, short(err, name, 500))
			} // else the task itself ended (cancelled, past its deadline): not the engine's failure
			return fallback(info, err)
		}
		a := answers[NeedsApproval]
		d := neumann.Allow()
		if a.Yes && a.Confidence >= threshold {
			d = neumann.RequireApproval(fmt.Sprintf("%s: %s yes, p=%.2f", name, NeedsApproval, a.P("yes")))
		}
		if cfg.Decide != nil {
			d = cfg.Decide(info, answers, d)
		}
		return d
	}
}

// failReason says, in a committed reason's few words, that the engine gave
// no answer and why: "systemone:tev1:4b: no answer within 2s", or
// "systemone:tev1:4b: no answer (400 Bad Request: ...)".
func failReason(engine string, err error) string {
	var te *timeoutError
	if errors.As(err, &te) {
		return fmt.Sprintf("%s: no answer within %s", engine, te.after)
	}
	return fmt.Sprintf("%s: no answer (%s)", engine, short(err, engine, 160))
}

// bound applies the package's convention for a confidence bound: zero is
// def, negative is 0 (anything passes), and 1 or more is an error.
func bound(what string, v, def float64) (float64, error) {
	switch {
	case v >= 1:
		return 0, fmt.Errorf("decide: %s %v is not below 1", what, v)
	case v == 0:
		return def, nil
	case v < 0:
		return 0, nil
	}
	return v, nil
}

// CallState is what Policy shows the engine about a call: one line each for
// the tool, its device, its declared effect, the task's class, note (if
// any) and the arguments, compacted and cut to about maxArgs bytes (zero:
// DefaultMaxArgs; negative: no cut). An object keeps every key, its long
// string values cut in the middle, so no field the model wrote is hidden
// behind a long one.
func CallState(info neumann.ToolCallInfo, note string, maxArgs int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tool: %s\n", info.Tool)
	if info.Device != "" {
		fmt.Fprintf(&b, "device: %s\n", info.Device)
	}
	fmt.Fprintf(&b, "declared effect: %s\n", info.Effect)
	if info.Class != "" {
		fmt.Fprintf(&b, "task class: %s\n", info.Class)
	}
	if note = strings.TrimSpace(note); note != "" {
		fmt.Fprintf(&b, "about the tool: %s\n", note)
	}
	if maxArgs == 0 {
		maxArgs = DefaultMaxArgs
	}
	args := compact(info.Args)
	if maxArgs > 0 {
		args = clipArgs(info.Args, maxArgs)
	}
	fmt.Fprintf(&b, "arguments: %s", args)
	return b.String()
}

// compact is args without insignificant space when they are valid JSON, as
// the model wrote them otherwise.
func compact(args []byte) string {
	if len(args) == 0 {
		return "(none)"
	}
	var b bytes.Buffer
	if json.Compact(&b, args) == nil {
		return b.String()
	}
	return string(args)
}
