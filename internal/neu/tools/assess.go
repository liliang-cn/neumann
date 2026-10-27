package tools

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liliang-cn/neumann/bus"
)

// Assessment is what neu's approval policy needs to know of a tool call.
type Assessment struct {
	// Safe: the call may run without asking. It only reads (a read-only
	// tool, an allowlisted sh_run command), it changes a file under a
	// writable directory, or the tool will refuse it anyway (bad
	// arguments, a path outside the workspace), so the model hears why at
	// once.
	Safe bool
	// What is the call in one line, as a person reads it.
	What string
}

// Assess classifies a call of tool (its full name) with args; effect is
// the tool's declared effect, which decides for tools neu does not know
// (another device's).
func (t *Tools) Assess(tool string, args []byte, effect bus.Effect) Assessment {
	switch tool {
	case "fs_write", "fs_edit", "fs_delete":
		var in struct{ Path string }
		if decode(args, &in) != nil {
			return Assessment{Safe: true, What: tool}
		}
		what := tool + " " + in.Path
		// Refused by the sandbox whatever the mode: the tool says why. A
		// path outside -writable is not refused here: the policy decides.
		rp, err := t.Box.Mutable(in.Path)
		if err == nil && tool == "fs_delete" {
			_, err = (&Tools{Box: t.Box, WriteAnywhere: true}).deletePath(in.Path)
		}
		if err != nil || in.Path == "" {
			return Assessment{Safe: true, What: what} // refused by the tool
		}
		return Assessment{Safe: t.Box.InWritable(rp), What: tool + " " + t.Box.Rel(rp)}
	case "sh_run":
		in, _, show, err := t.shCommand(args)
		if err != nil {
			return Assessment{Safe: true, What: tool}
		}
		what := "$ " + show
		if d := orDot(in.Dir); d != "." {
			what += " (in " + d + ")"
		}
		return Assessment{Safe: in.Script == "" && ReadOnlyCommand(in.Args), What: what}
	}
	what := tool
	if len(args) > 0 && string(args) != "{}" {
		what += " " + clip(strings.Join(strings.Fields(string(args)), " "), 160)
	}
	return Assessment{Safe: effect == bus.ReadOnly || effect == bus.Idempotent, What: what}
}

// Preview is what a held call would do, for an approval prompt: for a
// file change the path and a diff of it (nothing is changed), for sh_run
// the command and where it runs, for anything else its arguments.
func (t *Tools) Preview(tool string, args []byte) string {
	switch tool {
	case "fs_write", "fs_edit", "fs_delete":
		if p := t.preview(tool, args); p != "" {
			return strings.TrimRight(p, "\n")
		}
	case "sh_run":
		in, _, _, err := t.shCommand(args)
		if err != nil {
			return err.Error()
		}
		return fmt.Sprintf("in %s/ of the workspace, without input; env without the API keys", orDot(in.Dir))
	}
	var v any
	if json.Unmarshal(args, &v) == nil {
		if b, err := json.MarshalIndent(v, "", "  "); err == nil {
			return clip(string(b), 1500)
		}
	}
	return clip(string(args), 1500)
}
