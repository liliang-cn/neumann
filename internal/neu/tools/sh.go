package tools

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/bus"
)

// sh_run's limits.
const (
	ShDefaultTimeout = 2 * time.Minute
	ShMaxTimeout     = 10 * time.Minute
	shHeadBytes      = 8 << 10
	shTailBytes      = 16 << 10
)

// Shell is how sh_run runs commands.
type Shell struct {
	// MaxTimeout is the longest a call may ask for; 0: ShMaxTimeout. A
	// call that names none runs at most ShDefaultTimeout (or MaxTimeout,
	// when that is shorter).
	MaxTimeout time.Duration
	// Script allows the script argument: a bash -lc command line.
	Script bool
	// DenyEnv are environment variables the command never sees: exact
	// names, or a prefix ending in * (AWS_*). neu adds its backends' key
	// variables.
	DenyEnv []string
}

func (s Shell) maxTimeout() time.Duration {
	if s.MaxTimeout > 0 {
		return s.MaxTimeout
	}
	return ShMaxTimeout
}

type shArgs struct {
	Args       []string
	Script     string
	Dir        string
	TimeoutSec int `json:"timeout_sec"`
}

// shCommand decodes an sh_run call: the argv it runs and how it shows.
func (t *Tools) shCommand(raw []byte) (in shArgs, argv []string, show string, err error) {
	if err = decode(raw, &in); err != nil {
		return
	}
	switch {
	case len(in.Args) > 0 && in.Script != "":
		err = errors.New("give args or script, not both")
	case len(in.Args) > 0:
		if in.Args[0] == "" {
			err = errors.New("args[0] is empty")
			return
		}
		argv, show = in.Args, shellQuote(in.Args)
		if in.Args[0] == "git" && ReadOnlyCommand(in.Args) {
			argv = append(append([]string{"git"}, gitSafe...), in.Args[1:]...)
		}
	case in.Script != "":
		if !t.Sh.Script {
			err = errors.New("script is turned off (sh_shell = false); pass the program and its arguments in args")
			return
		}
		argv, show = []string{"bash", "-lc", in.Script}, "bash -lc "+shellQuote([]string{in.Script})
	default:
		err = errors.New("args (the program and its arguments) or script is required")
	}
	return
}

// shellQuote shows argv as a shell would read it.
func shellQuote(args []string) string {
	var b strings.Builder
	for i, a := range args {
		if i > 0 {
			b.WriteByte(' ')
		}
		if a != "" && !strings.ContainsAny(a, " \t\n'\"\\$`|&;<>()*?[]{}!#~") {
			b.WriteString(a)
			continue
		}
		b.WriteString("'" + strings.ReplaceAll(a, "'", `'\''`) + "'")
	}
	return b.String()
}

// ShEnv is the environment sh_run's commands get: neu's own, without the
// variables deny names.
func ShEnv(environ, deny []string) []string {
	var out []string
	for _, kv := range environ {
		k, _, _ := strings.Cut(kv, "=")
		if !denied(k, deny) {
			out = append(out, kv)
		}
	}
	return out
}

func denied(k string, deny []string) bool {
	for _, d := range deny {
		if p, ok := strings.CutSuffix(d, "*"); ok {
			if strings.HasPrefix(k, p) {
				return true
			}
		} else if k == d {
			return true
		}
	}
	return false
}

func (t *Tools) shRun(ctx context.Context, c bus.DeviceCall) (string, error) {
	in, argv, show, err := t.shCommand(c.Args)
	if err != nil {
		return "", err
	}
	dir, err := t.Box.Resolve(orDot(in.Dir))
	if err != nil {
		return "", err
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return "", fmt.Errorf("%s is not a directory", orDot(in.Dir))
	}
	limit := min(ShDefaultTimeout, t.Sh.maxTimeout())
	if in.TimeoutSec > 0 {
		limit = min(time.Duration(in.TimeoutSec)*time.Second, t.Sh.maxTimeout())
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = ShEnv(os.Environ(), t.Sh.DenyEnv)
	killGroup(cmd)
	cmd.WaitDelay = 3 * time.Second
	out := &capped{head: shHeadBytes, tail: shTailBytes}
	cmd.Stdout, cmd.Stderr = out, out
	start := time.Now()
	err = cmd.Run()
	took := time.Since(start).Round(time.Millisecond)
	where := ""
	if rel := t.Box.Rel(dir); rel != "." {
		where = " (in " + rel + "/)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "$ %s%s\n", show, where)
	b.WriteString(out.String())
	if out.n > 0 && !strings.HasSuffix(out.String(), "\n") {
		b.WriteByte('\n')
	}
	var ee *exec.ExitError
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		fmt.Fprintf(&b, "killed: timed out after %s (timeout_sec raises it, up to %s)", limit, t.Sh.maxTimeout())
	case errors.As(err, &ee):
		fmt.Fprintf(&b, "exit status %d (%s)", ee.ExitCode(), took)
	case err != nil:
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("%s: %v", show, err)
	default:
		fmt.Fprintf(&b, "exit status 0 (%s)", took)
	}
	return b.String(), nil
}

// capped keeps the first head and the last tail bytes written to it.
type capped struct {
	mu         sync.Mutex
	head, tail int
	h          []byte
	t          []byte // a window of the last bytes, at most 2*tail before trimming
	n          int64  // bytes written in all
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n += int64(len(p))
	q := p
	if room := c.head - len(c.h); room > 0 {
		k := min(room, len(q))
		c.h = append(c.h, q[:k]...)
		q = q[k:]
	}
	if len(q) > 0 {
		c.t = append(c.t, q...)
		if len(c.t) > 2*c.tail {
			c.t = append(c.t[:0], c.t[len(c.t)-c.tail:]...)
		}
	}
	return len(p), nil
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.t
	if len(t) > c.tail {
		t = t[len(t)-c.tail:]
	}
	skipped := c.n - int64(len(c.h)) - int64(len(t))
	if skipped <= 0 {
		return string(c.h) + string(t)
	}
	return string(c.h) + fmt.Sprintf("\n[… %d bytes of output omitted …]\n", skipped) + string(t)
}
