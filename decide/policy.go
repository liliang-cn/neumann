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
	"unicode/utf8"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
)

// NeedsApproval is the name Policy asks its own question under.
const NeedsApproval = "needs_approval"

// DefaultPolicyTimeout bounds each engine request a Policy makes.
const DefaultPolicyTimeout = 2 * time.Second

// maxArgs is how much of a call's arguments Policy shows the engine.
const maxArgs = 4096

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
// waits DefaultPolicyTimeout for an answer.
type PolicyConfig struct {
	// Question replaces DefaultApprovalQuestion; it must be a noul whose
	// yes means "hold the call". It is asked under the name NeedsApproval.
	Question Question
	// Threshold is the confidence a yes needs to hold the call, in [0, 1).
	// Zero holds every yes (a probability of yes of at least 0.5); 0.5, for
	// one, needs a probability of at least 0.75 (see Answer.Confidence).
	Threshold float64
	// Timeout bounds each engine request (default DefaultPolicyTimeout). The
	// policy runs on a vCore and the machine gives it no timeout of its own;
	// this is the one that keeps a slow engine from stalling the group.
	Timeout time.Duration
	// Tools adds a line about a tool to what the engine is shown, keyed by
	// the tool's full name (<device>_<tool>): its description, say. A
	// name says little; a sentence about what the tool does helps the
	// engine more than anything else here.
	Tools map[string]string
	// Extra questions are asked in the same request; their answers reach
	// Decide. Names must not be NeedsApproval.
	Extra map[string]Question
	// Decide, if set, makes the final decision once the engine has
	// answered: answers holds every answer (NeedsApproval's and Extra's), d
	// is what the policy would decide on its own. It runs on the vCore like
	// the policy: keep it fast and free of side effects.
	Decide func(info neumann.ToolCallInfo, answers map[string]Answer, d neumann.Decision) neumann.Decision
	// Fallback decides a call the engine did not answer (an error, or no
	// answer within Timeout). The default holds SideEffect calls for
	// approval, with a reason that says the engine failed, and allows the
	// others.
	Fallback func(info neumann.ToolCallInfo, err error) neumann.Decision
	// Logf reports engine failures (default log.Printf). An allowed call's
	// record keeps no reason, so this is where a failure that let a call
	// through shows.
	Logf func(format string, args ...any)
}

// Policy is a tool policy that asks engine whether each call needs a person
// (design §9.7). A ReadOnly call is allowed without asking. For any other,
// it asks the engine in one request, bounded by cfg.Timeout:
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
// vCore. Policy panics on a nil engine or an invalid cfg: both are
// programming errors, found when the machine is configured rather than on
// its first tool call.
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
	qs := map[string]Question{NeedsApproval: q}
	for name, x := range cfg.Extra {
		if name == NeedsApproval {
			panic("decide: Policy: an extra question is named " + NeedsApproval)
		}
		qs[name] = x
	}
	if err := Validate(qs); err != nil {
		panic(fmt.Sprintf("decide: Policy: %v", err))
	}
	if cfg.Threshold < 0 || cfg.Threshold >= 1 {
		panic(fmt.Sprintf("decide: Policy: threshold %v is not in [0, 1)", cfg.Threshold))
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultPolicyTimeout
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	fallback := cfg.Fallback
	if fallback == nil {
		fallback = func(info neumann.ToolCallInfo, err error) neumann.Decision {
			if info.Effect == bus.SideEffect {
				return neumann.RequireApproval(fmt.Sprintf("%s: no answer (%v); held because the call has side effects", engine.Name(), err))
			}
			return neumann.Allow()
		}
	}
	name := engine.Name()
	return func(ctx context.Context, info neumann.ToolCallInfo) neumann.Decision {
		if info.Effect == bus.ReadOnly {
			return neumann.Allow()
		}
		actx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		answers, err := engine.Decide(actx, CallState(info, cfg.Tools[info.Tool]), qs)
		if err == nil {
			if _, ok := answers[NeedsApproval]; !ok {
				err = fmt.Errorf("decide: %s did not answer %q", name, NeedsApproval)
			}
		}
		if err != nil && ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("no answer within %s", cfg.Timeout)
		}
		cancel()
		if err != nil {
			cfg.Logf("decide: policy for %s call %s/%s: %s: %v", info.Tool, info.TaskID, info.CallID, name, err)
			return fallback(info, err)
		}
		a := answers[NeedsApproval]
		d := neumann.Allow()
		if a.Yes && a.Confidence >= cfg.Threshold {
			d = neumann.RequireApproval(fmt.Sprintf("%s: %s yes, p=%.2f", name, NeedsApproval, a.P("yes")))
		}
		if cfg.Decide != nil {
			d = cfg.Decide(info, answers, d)
		}
		return d
	}
}

// CallState is what Policy shows the engine about a call: one line each for
// the tool, its device, its declared effect, the task's class, note (if
// any) and the arguments, compacted and cut to 4 KiB.
func CallState(info neumann.ToolCallInfo, note string) string {
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
	fmt.Fprintf(&b, "arguments: %s", clip(compact(info.Args), maxArgs))
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

// clip cuts s to at most n bytes on a rune boundary, saying how much went.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return fmt.Sprintf("%s… (%d more bytes)", s[:cut], len(s)-cut)
}
