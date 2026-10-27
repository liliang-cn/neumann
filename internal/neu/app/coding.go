package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/internal/neu/tools"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Approval modes (-approve).
const (
	ApproveAsk   = "ask"   // side effects outside -writable wait for the user's answer
	ApproveAuto  = "auto"  // everything runs
	ApproveNever = "never" // side effects outside -writable are refused (read-only)
)

// Coding is neu's coding-assistant setup: the tools that change the
// workspace and run commands, and the approval policy in front of them.
// Its zero value (the bench's) keeps the tools neu always had and no
// policy.
type Coding struct {
	On      bool
	Approve string // ask, auto or never
	// NoPrompt, in ask mode, denies a call that would be held, with this
	// reason: there is nobody to ask (a one-shot run without a terminal).
	NoPrompt        string
	ShTimeout       time.Duration // the longest an sh_run call may run; 0: 10m
	ShScript        bool          // sh_run may run bash -lc scripts
	ShDenyEnv       []string      // variables sh_run's commands never see, besides the backends' keys
	WebAllowPrivate bool          // web_fetch may fetch local and private addresses
}

// CodingFlags registers the coding flags (neu's, not the bench's) and
// turns Coding on.
func (c *Config) CodingFlags(fs *flag.FlagSet) {
	c.Coding.On = true
	fs.StringVar(&c.Coding.Approve, "approve", ApproveAsk, "what runs without asking: ask (side effects outside -writable wait for your y/n), auto (everything), never (side effects outside -writable are refused)")
	fs.DurationVar(&c.Coding.ShTimeout, "sh-timeout", tools.ShMaxTimeout, "the longest an sh_run command may run (a call asks for up to this; default per call 2m)")
	fs.BoolVar(&c.Coding.ShScript, "sh-shell", true, "sh_run may run bash -lc scripts (pipes, &&); they always need approval in ask mode")
	fs.Func("sh-deny-env", "comma-separated environment variables sh_run's commands never see (NAME or PREFIX*), besides the backends' API keys", func(s string) error {
		c.Coding.ShDenyEnv = splitList(s)
		return nil
	})
	fs.BoolVar(&c.Coding.WebAllowPrivate, "web-allow-private", false, "web_fetch may fetch localhost and private addresses")
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (k *Coding) check() error {
	if !k.On {
		return nil
	}
	switch k.Approve {
	case "":
		k.Approve = ApproveAsk
	case ApproveAsk, ApproveAuto, ApproveNever:
	case "read-only", "readonly":
		k.Approve = ApproveNever
	default:
		return fmt.Errorf("unknown -approve %q: want ask, auto or never", k.Approve)
	}
	if k.ShTimeout < 0 {
		return errors.New("-sh-timeout must not be negative")
	}
	return nil
}

// KeyEnvs are the environment variables holding the keys of neu's
// backends: the configured ones and every one neu knows, which sh_run's
// commands never see.
func (c *Config) KeyEnvs() []string {
	keys := []string{"ANTHROPIC_API_KEY", OpenAIKeyEnv, LocalKeyEnv, "CPA_API_KEY"}
	for _, s := range c.Sides() {
		if s.KeyEnv != "" && !slices.Contains(keys, s.KeyEnv) {
			keys = append(keys, s.KeyEnv)
		}
	}
	return keys
}

// CodingSystem is neu's system prompt with the coding tools.
const CodingSystem = `You are neu, a careful coding assistant that works on the files of a local workspace through tools.
- Every path is relative to the workspace root. Look at files with the tools instead of guessing; quote what you found.
- Prefer fs_grep to find things and fs_read to read the parts you need; read big files in pages.
- Change files with fs_edit (exact text replacement; read the file first and copy old_string from it), create them with fs_write, remove them with fs_delete. Keep changes small and focused.
- Run builds, tests and other commands with sh_run (or go_test for Go tests) and read their output before you claim something works.
- Some calls wait for the user's approval. A call that comes back denied was refused by the user or the approval mode: do not repeat it unchanged; adjust, or explain what you need.
- Use web_fetch only for public documentation you need.
- Use calc_eval for any arithmetic over more than two numbers; never add up long columns in your head.
- Never edit tests to make them pass unless asked to.
- Finish with a short, direct answer: what you found or changed, with file:line locations.`

// setupCoding configures the tools and returns the policy, when Coding is
// on.
func (a *App) setupCoding() neumann.ToolPolicy {
	k := a.Cfg.Coding
	if !k.On {
		return nil
	}
	t := a.Tools
	t.Coding = true
	t.WriteAnywhere = k.Approve != ApproveNever
	t.Sh = tools.Shell{MaxTimeout: k.ShTimeout, Script: k.ShScript, DenyEnv: append(a.Cfg.KeyEnvs(), k.ShDenyEnv...)}
	t.Web = tools.Web{AllowPrivate: k.WebAllowPrivate}
	return a.policy
}

// policy is neu's ToolPolicy (design §9.7): a call that only reads, or
// changes a file under -writable, runs; any other side effect runs in auto
// mode, is refused in never mode, and in ask mode is held for the user
// (Approve) unless they allowed its tool for the session.
func (a *App) policy(_ context.Context, info neumann.ToolCallInfo) neumann.Decision {
	as := a.Tools.Assess(info.Tool, info.Args, info.Effect)
	if as.Safe {
		return neumann.Allow()
	}
	switch a.Cfg.Coding.Approve {
	case ApproveAuto:
		return neumann.Allow()
	case ApproveNever:
		return neumann.Deny(fmt.Sprintf("%s was refused: neu runs with -approve never (read-only), which allows side effects only under the writable directories; the user can rerun with -approve ask or auto", as.What))
	}
	if a.AlwaysAllowed(info.Tool) {
		return neumann.Allow()
	}
	if r := a.Cfg.Coding.NoPrompt; r != "" {
		return neumann.Deny(as.What + " needs the user's approval, but " + r)
	}
	return neumann.RequireApproval(as.What)
}

// always holds the tools the user allowed for the session.
type always struct {
	mu    sync.Mutex
	tools map[string]bool
}

// AllowAlways lets every later call of tool run without asking, until neu
// exits.
func (a *App) AllowAlways(tool string) {
	a.always.mu.Lock()
	defer a.always.mu.Unlock()
	if a.always.tools == nil {
		a.always.tools = map[string]bool{}
	}
	a.always.tools[tool] = true
}

// AlwaysAllowed reports whether the user allowed tool for the session.
func (a *App) AlwaysAllowed(tool string) bool {
	a.always.mu.Lock()
	defer a.always.mu.Unlock()
	return a.always.tools[tool]
}

// Held is a tool call waiting for the user's approval.
type Held struct {
	Task, Call, Tool string
	Args             []byte
	Reason           string // what the policy said: the call in one line
}

// HeldFrom is the held call an approval_needed event names.
func HeldFrom(ev proto.Event) (Held, bool) {
	if ev.Kind != proto.EvApprovalNeeded {
		return Held{}, false
	}
	call, _ := Field[string](ev, "call_id")
	tool, _ := Field[string](ev, "tool")
	args, _ := Field[string](ev, "args")
	reason, _ := Field[string](ev, "reason")
	return Held{Task: ev.TaskID, Call: call, Tool: tool, Args: []byte(args), Reason: reason}, call != ""
}

// HeldNow are the calls held for approval in the store: those a restart
// announced before anyone listened.
func (a *App) HeldNow(ctx context.Context) ([]Held, error) {
	var out []Held
	for _, ti := range a.M.Tasks() {
		if ti.State != task.Waiting || ti.Wait != task.WaitApproval {
			continue
		}
		recs, err := a.M.Calls(ctx, ti.ID)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			if r.Status == mem.CallPlanned && r.Approval == mem.ApprovalPending {
				out = append(out, Held{Task: ti.ID, Call: r.CallID, Tool: r.Tool, Args: r.Args, Reason: r.Reason})
			}
		}
	}
	return out, nil
}

// ledgered are the tools whose changes the ledger applies once per call,
// so a call a restart left unknown can always be sent again.
var ledgered = []string{"fs_write", "fs_edit", "fs_delete"}

// Reconcile answers the reconcile_needed calls of the ledgered tools, now
// and as tr reads them: each is sent again (Retry), and the ledger either
// recognises it as applied and returns what it did, or applies it. Calls
// of other tools stay held for an operator. report is told what was done.
func (a *App) Reconcile(ctx context.Context, tr *Tracker, report func(string)) {
	resolve := func(taskID, callID, tool string) {
		err := a.M.Resolve(ctx, taskID, callID, neumann.Outcome{Retry: true})
		switch {
		case errors.Is(err, neumann.ErrNotReconciling):
		case err != nil:
			report(fmt.Sprintf("[%s] could not resend %s after the restart: %v", taskID, tool, err))
		default:
			report(fmt.Sprintf("[%s] %s's outcome was unknown after a restart; sent again, the ledger applies it once", taskID, tool))
		}
	}
	tr.Subscribe(func(ev proto.Event) {
		if ev.Kind != proto.EvReconcileNeeded {
			return
		}
		if c, _ := Field[bool](ev, "cancelled"); c {
			return
		}
		tool, _ := Field[string](ev, "tool")
		callID, _ := Field[string](ev, "call_id")
		if slices.Contains(ledgered, tool) {
			go resolve(ev.TaskID, callID, tool)
		}
	})
	for _, ti := range a.M.Tasks() {
		if ti.State != task.Waiting || ti.Wait != task.WaitReconcile {
			continue
		}
		recs, err := a.M.Calls(ctx, ti.ID)
		if err != nil {
			continue
		}
		for _, r := range recs {
			if r.Status == mem.CallUnknown && slices.Contains(ledgered, r.Tool) {
				go resolve(ti.ID, r.CallID, r.Tool)
			}
		}
	}
}
