package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
)

// Limits on what a tool returns, so one call cannot flood the context.
const (
	MaxReadLines   = 400
	DefReadLines   = 200
	MaxLineChars   = 400
	MaxOutputBytes = 48 << 10
	MaxGrepShown   = 100
	MaxListEntries = 400
	MaxFileBytes   = 8 << 20
	GoTestTimeout  = 3 * time.Minute
	GitTimeout     = 30 * time.Second
)

// CallStat is one tool call as the device saw it: how long the device
// itself spent, without the bus or the machine.
type CallStat struct {
	Task    string
	Key     string
	Tool    string // as the model sees it, e.g. fs_read
	Start   time.Time
	Dur     time.Duration
	IsError bool
}

// Tools builds nm's devices on a sandbox.
type Tools struct {
	Box    *Sandbox
	Ledger *Ledger
	// OnCall, when set, is told about every finished call.
	OnCall func(CallStat)
	// GoTestTimeout bounds go_test; 0 is GoTestTimeout.
	GoTestTimeout time.Duration
}

// New returns the tools on box; the write ledger lives in the first
// writable directory, out/ by convention.
func New(box *Sandbox) (*Tools, error) {
	t := &Tools{Box: box}
	if len(box.writable) > 0 {
		l, err := OpenLedger(filepath.Join(box.writable[0], ledgerName))
		if err != nil {
			return nil, err
		}
		t.Ledger = l
	}
	return t, nil
}

type fn func(ctx context.Context, call bus.DeviceCall) (string, error)

func (t *Tools) wrap(full string, f fn) builtin.Func {
	return func(ctx context.Context, call bus.DeviceCall) (string, error) {
		start := time.Now()
		out, err := f(ctx, call)
		if t.OnCall != nil {
			t.OnCall(CallStat{Task: call.Task, Key: call.Key, Tool: full, Start: start, Dur: time.Since(start), IsError: err != nil})
		}
		return out, err
	}
}

func schema(s string) json.RawMessage { return json.RawMessage(s) }

// Devices returns the devices: fs (list, read, grep, write), git (run), go
// (test) and calc (eval). Without a writable directory there is no
// fs_write.
func (t *Tools) Devices() []bus.Device {
	fsd := builtin.New("fs").
		Add(bus.ToolDef{
			Name:        "list",
			Description: "List files and directories under a path of the workspace, as a tree. Directories end with /, files show their size in bytes.",
			Schema:      schema(`{"type":"object","properties":{"path":{"type":"string","description":"directory, relative to the workspace root; default ."},"depth":{"type":"integer","description":"how many levels to descend, 1-6; default 2"}}}`),
			Effect:      bus.ReadOnly,
		}, t.wrap("fs_list", t.list)).
		Add(bus.ToolDef{
			Name:        "read",
			Description: fmt.Sprintf("Read a text file of the workspace with line numbers. Returns at most %d lines per call (default %d); the header says how many lines the file has, so read on with offset_line.", MaxReadLines, DefReadLines),
			Schema:      schema(`{"type":"object","properties":{"path":{"type":"string"},"offset_line":{"type":"integer","description":"first line to return, 1-based; default 1"},"limit_lines":{"type":"integer","description":"how many lines to return"}},"required":["path"]}`),
			Effect:      bus.ReadOnly,
		}, t.wrap("fs_read", t.read)).
		Add(bus.ToolDef{
			Name:        "grep",
			Description: fmt.Sprintf("Search files of the workspace for a regular expression (Go RE2 syntax), recursively. Returns file:line: text for at most %d matches, and always the total number of matching lines. count_only returns only the counts per file.", MaxGrepShown),
			Schema:      schema(`{"type":"object","properties":{"pattern":{"type":"string","description":"RE2 regular expression"},"path":{"type":"string","description":"file or directory to search; default ."},"glob":{"type":"string","description":"only files whose name (or path relative to path) matches this glob, e.g. *.go"},"ignore_case":{"type":"boolean"},"count_only":{"type":"boolean"}},"required":["pattern"]}`),
			Effect:      bus.ReadOnly,
		}, t.wrap("fs_grep", t.grep))
	if t.Ledger != nil {
		fsd.Add(bus.ToolDef{
			Name:        "write",
			Description: "Create or replace a text file with the given content. Allowed only under " + t.writableNames() + ". Returns the number of bytes written.",
			Schema:      schema(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`),
			Effect:      bus.SideEffect,
			Timeout:     30 * time.Second,
		}, t.wrap("fs_write", t.write))
	}
	git := builtin.New("git").Add(bus.ToolDef{
		Name:        "run",
		Description: "Run a read-only git command in a repository of the workspace: args[0] is one of " + strings.Join(gitAllowed, ", ") + "; the rest are its arguments. No shell.",
		Schema:      schema(`{"type":"object","properties":{"dir":{"type":"string","description":"repository directory, relative to the workspace root; default ."},"args":{"type":"array","items":{"type":"string"}}},"required":["args"]}`),
		Effect:      bus.ReadOnly,
		Timeout:     GitTimeout + 5*time.Second,
	}, t.wrap("git_run", t.git))
	gotest := builtin.New("go").Add(bus.ToolDef{
		Name:        "test",
		Description: "Run `go test ./...` in a Go module directory of the workspace and return its output (the end of it, when long). Optionally only tests matching run.",
		Schema:      schema(`{"type":"object","properties":{"dir":{"type":"string","description":"module directory, relative to the workspace root"},"run":{"type":"string","description":"optional -run regexp"}},"required":["dir"]}`),
		Effect:      bus.Idempotent,
		Timeout:     t.goTestTimeout() + 10*time.Second,
	}, t.wrap("go_test", t.goTest))
	calc := builtin.New("calc").Add(bus.ToolDef{
		Name:        "eval",
		Description: "Evaluate an arithmetic expression exactly: + - * / ^, parentheses, and sum(...), avg(...), min(...), max(...), abs(x), round(x, places). Write numbers without thousands separators or currency signs.",
		Schema:      schema(`{"type":"object","properties":{"expr":{"type":"string"}},"required":["expr"]}`),
		Effect:      bus.ReadOnly,
	}, t.wrap("calc_eval", func(_ context.Context, c bus.DeviceCall) (string, error) {
		var in struct{ Expr string }
		if err := json.Unmarshal(c.Args, &in); err != nil {
			return "", fmt.Errorf("bad arguments: %v", err)
		}
		return Calc(in.Expr)
	}))
	return []bus.Device{fsd, git, gotest, calc}
}

func (t *Tools) goTestTimeout() time.Duration {
	if t.GoTestTimeout > 0 {
		return t.GoTestTimeout
	}
	return GoTestTimeout
}

func (t *Tools) writableNames() string {
	var names []string
	for _, w := range t.Box.writable {
		names = append(names, t.Box.Rel(w)+"/")
	}
	return strings.Join(names, ", ")
}

// decode unmarshals args into v; a bad argument is the model's error.
func decode(args json.RawMessage, v any) error {
	if len(bytes.TrimSpace(args)) == 0 {
		args = json.RawMessage("{}")
	}
	if err := json.Unmarshal(args, v); err != nil {
		return fmt.Errorf("bad arguments: %v", err)
	}
	return nil
}

func orDot(p string) string {
	if strings.TrimSpace(p) == "" {
		return "."
	}
	return p
}

func (t *Tools) list(_ context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Path  string
		Depth int
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	depth := in.Depth
	if depth <= 0 {
		depth = 2
	}
	depth = min(depth, 6)
	dir, err := t.Box.Resolve(orDot(in.Path))
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return "", notFound(err, in.Path)
	}
	if !fi.IsDir() {
		return fmt.Sprintf("%s (%d bytes) is a file", t.Box.Rel(dir), fi.Size()), nil
	}
	var b strings.Builder
	n, truncated := 0, false
	var walk func(d string, level int)
	walk = func(d string, level int) {
		ents, err := os.ReadDir(d)
		if err != nil {
			fmt.Fprintf(&b, "%s(unreadable: %v)\n", strings.Repeat("  ", level), err)
			return
		}
		for _, e := range ents {
			if n >= MaxListEntries {
				truncated = true
				return
			}
			n++
			ind := strings.Repeat("  ", level)
			p := filepath.Join(d, e.Name())
			switch {
			case e.Type()&fs.ModeSymlink != 0:
				if _, err := t.Box.Resolve(p); err != nil {
					fmt.Fprintf(&b, "%s%s -> (outside the workspace)\n", ind, e.Name())
					continue
				}
				fmt.Fprintf(&b, "%s%s@\n", ind, e.Name())
			case e.IsDir():
				fmt.Fprintf(&b, "%s%s/\n", ind, e.Name())
				if level < depth && e.Name() != ".git" {
					walk(p, level+1)
				}
			default:
				size := int64(0)
				if info, err := e.Info(); err == nil {
					size = info.Size()
				}
				fmt.Fprintf(&b, "%s%s (%d)\n", ind, e.Name(), size)
			}
		}
	}
	fmt.Fprintf(&b, "%s/\n", t.Box.Rel(dir))
	walk(dir, 1)
	if truncated {
		fmt.Fprintf(&b, "... truncated at %d entries; list a subdirectory or use a smaller depth\n", MaxListEntries)
	}
	return b.String(), nil
}

func notFound(err error, p string) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%s: no such file or directory", p)
	}
	return err
}

// openFile resolves p and opens it for reading, refusing directories,
// huge files and binary content.
func (t *Tools) openFile(p string) (string, []byte, error) {
	rp, err := t.Box.Resolve(p)
	if err != nil {
		return "", nil, err
	}
	fi, err := os.Stat(rp)
	if err != nil {
		return "", nil, notFound(err, p)
	}
	if fi.IsDir() {
		return "", nil, fmt.Errorf("%s is a directory; use fs_list", p)
	}
	if fi.Size() > MaxFileBytes {
		return "", nil, fmt.Errorf("%s is %d bytes, over the %d byte limit; use fs_grep", p, fi.Size(), MaxFileBytes)
	}
	data, err := os.ReadFile(rp)
	if err != nil {
		return "", nil, err
	}
	return rp, data, nil
}

func binary(data []byte) bool {
	return bytes.IndexByte(data[:min(len(data), 8000)], 0) >= 0
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("… [%d more chars]", len(s)-n)
}

func splitLines(data []byte) []string {
	s := string(data)
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
}

func (t *Tools) read(_ context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Path       string
		OffsetLine int `json:"offset_line"`
		LimitLines int `json:"limit_lines"`
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.Path == "" {
		return "", errors.New("path is required")
	}
	rp, data, err := t.openFile(in.Path)
	if err != nil {
		return "", err
	}
	if binary(data) {
		return "", fmt.Errorf("%s looks binary", in.Path)
	}
	lines := splitLines(data)
	from := max(in.OffsetLine, 1)
	limit := in.LimitLines
	if limit <= 0 {
		limit = DefReadLines
	}
	limit = min(limit, MaxReadLines)
	if len(lines) == 0 {
		return fmt.Sprintf("%s: empty file (0 lines)", t.Box.Rel(rp)), nil
	}
	if from > len(lines) {
		return "", fmt.Errorf("%s has %d lines; offset_line %d is past the end", in.Path, len(lines), from)
	}
	to := min(from+limit-1, len(lines))
	var b strings.Builder
	width := len(fmt.Sprint(to))
	for i := from; i <= to; i++ {
		line := fmt.Sprintf("%*d| %s\n", width, i, clip(lines[i-1], MaxLineChars))
		if b.Len()+len(line) > MaxOutputBytes {
			to = i - 1
			break
		}
		b.WriteString(line)
	}
	head := fmt.Sprintf("%s: lines %d-%d of %d\n", t.Box.Rel(rp), from, to, len(lines))
	tail := ""
	if to < len(lines) {
		tail = fmt.Sprintf("[%d more lines; continue with offset_line=%d]\n", len(lines)-to, to+1)
	}
	return head + b.String() + tail, nil
}

func (t *Tools) grep(ctx context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Pattern    string
		Path       string
		Glob       string
		IgnoreCase bool `json:"ignore_case"`
		CountOnly  bool `json:"count_only"`
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.Pattern == "" {
		return "", errors.New("pattern is required")
	}
	pat := in.Pattern
	if in.IgnoreCase {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return "", fmt.Errorf("bad pattern: %v", err)
	}
	if in.Glob != "" {
		if _, err := path.Match(in.Glob, "x"); err != nil {
			return "", fmt.Errorf("bad glob: %v", err)
		}
	}
	start, err := t.Box.Resolve(orDot(in.Path))
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(start); err != nil {
		return "", notFound(err, in.Path)
	}
	var shown strings.Builder
	total, nshown, files := 0, 0, 0
	perFile := map[string]int{}
	var order []string
	err = filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			rp, err := t.Box.Resolve(p)
			if err != nil {
				return nil
			}
			if fi, err := os.Stat(rp); err != nil || fi.IsDir() {
				return nil
			}
		}
		if in.Glob != "" {
			rel, _ := filepath.Rel(start, p)
			okBase, _ := path.Match(in.Glob, d.Name())
			okRel, _ := path.Match(in.Glob, filepath.ToSlash(rel))
			if !okBase && !okRel {
				return nil
			}
		}
		fi, err := os.Stat(p)
		if err != nil || fi.Size() > MaxFileBytes {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		br := bufio.NewReader(f)
		head, _ := br.Peek(8000)
		if bytes.IndexByte(head, 0) >= 0 {
			return nil
		}
		rel := t.Box.Rel(p)
		sc := bufio.NewScanner(br)
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		n, matched := 0, 0
		for sc.Scan() {
			n++
			line := sc.Text()
			if !re.MatchString(line) {
				continue
			}
			matched++
			total++
			if !in.CountOnly && nshown < MaxGrepShown && shown.Len() < MaxOutputBytes {
				fmt.Fprintf(&shown, "%s:%d: %s\n", rel, n, clip(line, 300))
				nshown++
			}
		}
		if matched > 0 {
			files++
			perFile[rel] = matched
			order = append(order, rel)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if in.CountOnly {
		var b strings.Builder
		sort.Strings(order)
		for _, f := range order {
			fmt.Fprintf(&b, "%s: %d\n", f, perFile[f])
		}
		fmt.Fprintf(&b, "total: %d matching lines in %d files\n", total, files)
		return b.String(), nil
	}
	if total == 0 {
		return "no matches", nil
	}
	summary := fmt.Sprintf("%d matching lines in %d files", total, files)
	if nshown < total {
		summary += fmt.Sprintf(" (showing the first %d; narrow the pattern or path, or use count_only)", nshown)
	}
	return shown.String() + summary + "\n", nil
}

func (t *Tools) write(_ context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Path    string
		Content *string
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.Path == "" || in.Content == nil {
		return "", errors.New("path and content are required")
	}
	if len(*in.Content) > 1<<20 {
		return "", errors.New("content over 1 MiB")
	}
	rp, err := t.Box.Writable(in.Path)
	if err != nil {
		return "", err
	}
	return t.Ledger.Write(c.Key, c.Task, t.Box.Rel(rp), rp, []byte(*in.Content))
}

var gitAllowed = []string{"log", "show", "diff", "status", "blame", "rev-parse", "ls-files"}

// gitRefused are arguments that would let a read-only subcommand write a
// file, run a program or read outside the repository.
var gitRefused = []string{"--output", "--ext-diff", "--textconv", "--no-index", "--exec"}

// GitArgs checks a git command line against the allowlist.
func GitArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("args is empty; args[0] is the subcommand")
	}
	if !slices.Contains(gitAllowed, args[0]) {
		return fmt.Errorf("git %s is not allowed; allowed: %s", args[0], strings.Join(gitAllowed, ", "))
	}
	for _, a := range args[1:] {
		for _, r := range gitRefused {
			if a == r || strings.HasPrefix(a, r+"=") || (r == "--output" && strings.HasPrefix(a, r)) {
				return fmt.Errorf("git argument %q is not allowed", a)
			}
		}
		if strings.ContainsRune(a, 0) {
			return errors.New("argument contains a NUL byte")
		}
	}
	return nil
}

// cleanEnv is the environment for git and go: no user or system git
// config, no pager, and git cannot look above the workspace for a
// repository.
func (t *Tools) cleanEnv(extra ...string) []string {
	keep := []string{"PATH", "HOME", "TMPDIR", "GOPATH", "GOCACHE", "GOMODCACHE", "GOROOT", "LANG", "USER"}
	var env []string
	for _, k := range keep {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	env = append(env,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_PAGER=cat", "PAGER=cat",
		"GIT_CEILING_DIRECTORIES="+filepath.Dir(t.Box.root), "GIT_TERMINAL_PROMPT=0")
	return append(env, extra...)
}

func (t *Tools) git(ctx context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Dir  string
		Args []string
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if err := GitArgs(in.Args); err != nil {
		return "", err
	}
	dir, err := t.Box.Resolve(orDot(in.Dir))
	if err != nil {
		return "", err
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return "", fmt.Errorf("%s is not a directory", orDot(in.Dir))
	}
	// The repository git finds must itself be inside the workspace.
	ctx, cancel := context.WithTimeout(ctx, GitTimeout)
	defer cancel()
	top := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	top.Dir, top.Env = dir, t.cleanEnv()
	if out, err := top.Output(); err == nil {
		if _, err := t.Box.Resolve(strings.TrimSpace(string(out))); err != nil {
			return "", errors.New("that repository is outside the workspace")
		}
	}
	args := append([]string{"--no-pager", "-c", "core.fsmonitor=false", "-c", "diff.external=", "-c", "core.pager=cat"}, in.Args...)
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir, cmd.Env = dir, t.cleanEnv()
	out, err := cmd.CombinedOutput()
	res := tail(string(out), MaxOutputBytes, true)
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("git timed out after %s", GitTimeout)
		}
		return "", fmt.Errorf("git: %v\n%s", err, res)
	}
	if res == "" {
		res = "(no output)"
	}
	return res, nil
}

// tail keeps at most n bytes of s: its start when head, else its end.
func tail(s string, n int, head bool) string {
	if len(s) <= n {
		return s
	}
	if head {
		return s[:n] + fmt.Sprintf("\n… [%d more bytes]\n", len(s)-n)
	}
	return fmt.Sprintf("[… %d earlier bytes]\n", len(s)-n) + s[len(s)-n:]
}

func (t *Tools) goTest(ctx context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		Dir string
		Run string
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	dir, err := t.Box.Resolve(orDot(in.Dir))
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		return "", fmt.Errorf("%s has no go.mod", orDot(in.Dir))
	}
	ctx, cancel := context.WithTimeout(ctx, t.goTestTimeout())
	defer cancel()
	args := []string{"test", "-count=1"}
	if in.Run != "" {
		args = append(args, "-run", in.Run)
	}
	args = append(args, "./...")
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	cmd.Env = t.cleanEnv("GOWORK=off", "GOFLAGS=-mod=mod", "GOTOOLCHAIN=local", "CGO_ENABLED=0")
	cmd.WaitDelay = 5 * time.Second
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err = cmd.Run()
	out := tail(buf.String(), 12<<10, false)
	switch {
	case ctx.Err() != nil:
		return out + fmt.Sprintf("\ngo test timed out after %s", t.goTestTimeout()), nil
	case err != nil:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return out + fmt.Sprintf("\nFAIL (exit status %d)", ee.ExitCode()), nil
		}
		return "", fmt.Errorf("go test: %v", err)
	}
	return out + "\nPASS (exit status 0)", nil
}
