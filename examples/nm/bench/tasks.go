package bench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/liliang-cn/neumann/examples/nm/app"
	"github.com/liliang-cn/neumann/examples/nm/tools"
)

// Task is one kind of request a user makes, with a checker that judges
// the outcome objectively.
type Task struct {
	ID    string // e.g. repo-qa.preempt; also the stem of instance ids
	Kind  string // repo-qa, log-triage, csv-stats, code-review, fix-bug, changelog, quick
	Quick bool   // a short interactive request (0-1 tool calls)
	// Prompt is the request; {{fix}} and {{out}} are the instance's
	// fix-bug directory and output directory.
	Prompt string
	Check  func(c *CheckCtx) Verdict
	// NeedsFix: the instance gets its own copy of the fix-bug module.
	NeedsFix bool
}

// Instance is one submitted copy of a task.
type Instance struct {
	ID       string `json:"id"`
	Task     *Task  `json:"-"`
	TaskID   string `json:"task"`
	Priority int    `json:"priority"`
	Input    string `json:"-"`
	FixDir   string `json:"fix_dir,omitempty"` // relative to the root
	// Route is where the router sent it (hybrid), RouteMS how long the
	// decision took (decide; it is not part of WallMS, which starts at
	// the accepted commit).
	Route   app.Pick `json:"route,omitzero"`
	RouteMS float64  `json:"route_ms,omitempty"`
	OutDir  string   `json:"out_dir,omitempty"`
	TestSum string   `json:"test_sum,omitempty"`
}

// CheckCtx is what a checker sees.
type CheckCtx struct {
	Answer string
	Inst   *Instance
	Facts  *Facts
	Root   string
	Ledger []tools.LedgerEntry // every ledger entry of this instance's task
	Extra  string              // ops: text the steer asked for, etc.
}

// Verdict is a checker's judgement.
type Verdict struct {
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

func pass(format string, args ...any) Verdict { return Verdict{true, fmt.Sprintf(format, args...)} }
func fail(format string, args ...any) Verdict { return Verdict{false, fmt.Sprintf(format, args...)} }

// StaticClass is -route static's rule for the suite's kinds, from the
// measurements (qwen3.5:4b alone vs the cloud model alone, same suite):
// the 4B model passed every quick probe and repo question, and fix-bug
// (go test passes; 2 of 2 runs) and code-review (1 of 1) at about the
// cloud model's speed, so those stay local. It failed log-triage (wrong
// root cause after 7 steps), csv-stats (never summed the column) and
// changelog (skipped a commit): reading a lot and counting or covering
// every item, which the cloud model passed. Those go to strong.
func StaticClass(kind string) string {
	switch kind {
	case "log-triage", "csv-stats", "changelog":
		return app.ClassStrong
	}
	return app.ClassFast
}

// Tasks is the suite, in the order solo runs it.
func Tasks() []*Task {
	return []*Task{
		{
			ID: "repo-qa.preempt", Kind: "repo-qa",
			Prompt: "repo/ holds the source of neumann, a Go agent runtime. In which file and function does the machine decide whether a ready task may preempt (cut off) a running model call of a less urgent task? Answer with the file path, the function name, and file:line of that function.",
			Check:  checkPreempt,
		},
		{
			ID: "repo-qa.cores", Kind: "repo-qa",
			Prompt: "In the neumann source under repo/, how many vCores does a machine run when its Config sets no Cores, and where is that default defined? Give the number and file:line.",
			Check:  checkCores,
		},
		{
			ID: "repo-qa.warn", Kind: "repo-qa",
			Prompt: "In the neumann source under repo/, observe/log.go writes an activity log line per event. Which event kinds does it log at warn level? List every one of them.",
			Check:  checkWarn,
		},
		{
			ID: "log-triage", Kind: "log-triage",
			Prompt: "logs/service.log is our order service's log from 2026-10-14 (UTC), covering an incident. What was the root cause? Give the time (HH:MM:SS, UTC) of the first ERROR line it caused, list the endpoints it affected, and say exactly how many requests failed because of it.",
			Check:  checkLog,
		},
		{
			ID: "csv-stats", Kind: "csv-stats",
			Prompt: "data/sales.csv has our 2026 orders. Which region had the highest total revenue in Q3 2026 (July to September), and what exactly was that total?",
			Check:  checkSales,
		},
		{
			ID: "code-review", Kind: "code-review",
			Prompt: "Please review the change in review/change.diff before I merge it. Is there a bug? If so, name the file, the function and the line, and explain what goes wrong.",
			Check:  checkReview,
		},
		{
			ID: "fix-bug", Kind: "fix-bug", NeedsFix: true,
			Prompt: "The Go module in {{fix}} has failing tests. Find the bug in the code (do not change the tests), fix it with fs_write, run go_test on {{fix}} to confirm everything passes, and tell me what you changed.",
			Check:  checkFix,
		},
		{
			ID: "changelog", Kind: "changelog",
			Prompt: "changelog/ is a git repository. Write a changelog for everything committed since the tag v1.3.0, grouped by type of change (features, fixes, and so on), as Markdown, to {{out}}/CHANGELOG.md, and show it to me too.",
			Check:  checkChangelog,
		},
		{
			ID: "quick.lines", Kind: "quick", Quick: true,
			Prompt: "How many lines does data/sales.csv have, header included?",
			Check: func(c *CheckCtx) Verdict {
				return wantNumber(c.Answer, float64(c.Facts.Sales.Lines), 0)
			},
		},
		{
			ID: "quick.columns", Kind: "quick", Quick: true,
			Prompt: "What are the column names of data/sales.csv?",
			Check: func(c *CheckCtx) Verdict {
				return wantAll(c.Answer, "order_id", "date", "region", "product", "units", "unit_price", "revenue")
			},
		},
		{
			ID: "quick.where", Kind: "quick", Quick: true,
			Prompt: "Which file in repo/ declares DefaultMaxQueued? Just the path.",
			Check: func(c *CheckCtx) Verdict {
				return wantAll(c.Answer, "neumann.go")
			},
		},
		{
			ID: "quick.percent", Kind: "quick", Quick: true,
			Prompt: "What is 18% of 2450.75?",
			Check: func(c *CheckCtx) Verdict {
				if v := wantNumber(c.Answer, 441.135, 0.0001); v.Pass {
					return v
				}
				return wantNumber(c.Answer, 441.14, 0.0001)
			},
		},
		{
			ID: "quick.top", Kind: "quick", Quick: true,
			Prompt: "What are the top-level entries of the workspace root? Just list them.",
			Check: func(c *CheckCtx) Verdict {
				return wantAll(c.Answer, "repo", "logs", "data", "review", "changelog", "out")
			},
		},
	}
}

// TaskByID finds a task.
func TaskByID(id string) *Task {
	for _, t := range Tasks() {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// Select returns the tasks whose id or kind is in list (comma-separated;
// a trailing * matches a prefix); empty selects all.
func Select(list string) ([]*Task, error) {
	all := Tasks()
	if strings.TrimSpace(list) == "" {
		return all, nil
	}
	var out []*Task
	seen := map[string]bool{}
	for _, want := range strings.Split(list, ",") {
		want = strings.TrimSpace(want)
		n := 0
		for _, t := range all {
			ok := t.ID == want || t.Kind == want || strings.HasSuffix(want, "*") && strings.HasPrefix(t.ID, strings.TrimSuffix(want, "*"))
			if ok && !seen[t.ID] {
				seen[t.ID] = true
				out = append(out, t)
				n++
			} else if ok {
				n++
			}
		}
		if n == 0 {
			return nil, fmt.Errorf("no task matches %q", want)
		}
	}
	return out, nil
}

// --- checker helpers ----------------------------------------------------

var numRe = regexp.MustCompile(`-?\$?\d[\d,]*(?:\.\d+)?`)

// numbers are the numbers written in s; thousands separators and $ are
// ignored.
func numbers(s string) []float64 {
	var out []float64
	for _, m := range numRe.FindAllString(s, -1) {
		m = strings.NewReplacer(",", "", "$", "").Replace(m)
		if v, err := strconv.ParseFloat(m, 64); err == nil {
			out = append(out, v)
		}
	}
	return out
}

func wantNumber(answer string, want, tol float64) Verdict {
	for _, v := range numbers(answer) {
		if math.Abs(v-want) <= tol+1e-9 {
			return pass("found %v", want)
		}
	}
	return fail("the answer does not contain %v", want)
}

func wantAll(answer string, words ...string) Verdict {
	low := strings.ToLower(answer)
	var missing []string
	for _, w := range words {
		if !strings.Contains(low, strings.ToLower(w)) {
			missing = append(missing, w)
		}
	}
	if len(missing) > 0 {
		return fail("missing %s", strings.Join(missing, ", "))
	}
	return pass("mentions %s", strings.Join(words, ", "))
}

func anyOf(answer string, words ...string) bool {
	low := strings.ToLower(answer)
	for _, w := range words {
		if strings.Contains(low, strings.ToLower(w)) {
			return true
		}
	}
	return false
}

// fileLine finds file:line references to a file whose name ends in base;
// it returns the line numbers.
func fileLine(answer, base string) []int {
	re := regexp.MustCompile(regexp.QuoteMeta(base) + "`?\\s*(?:[:#]L?|,? line )(\\d+)")
	var out []int
	for _, m := range re.FindAllStringSubmatch(answer, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil {
			out = append(out, n)
		}
	}
	return out
}

var intRe = regexp.MustCompile(`\b\d+\b`)

// ints are the whole numbers written in s.
func ints(s string) []int {
	var out []int
	for _, m := range intRe.FindAllString(s, -1) {
		if n, err := strconv.Atoi(m); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func inRange(ns []int, lo, hi int) bool {
	for _, n := range ns {
		if n >= lo && n <= hi {
			return true
		}
	}
	return false
}

// --- the checkers -------------------------------------------------------

// The machine decides in internal/core/preempt.go, func (e *Env)
// preemptCheck, lines 153-207 at v0.3.0 (its doc comment starts at 140).
func checkPreempt(c *CheckCtx) Verdict {
	a := c.Answer
	if !strings.Contains(a, "preempt.go") {
		return fail("does not name internal/core/preempt.go")
	}
	if !strings.Contains(a, "preemptCheck") {
		return fail("does not name preemptCheck")
	}
	// file:line, or the line given on its own ("Line: 153").
	if lines := fileLine(a, "preempt.go"); !inRange(lines, 138, 207) && !inRange(ints(a), 138, 207) {
		return fail("no line within 138-207 (file:line %v)", lines)
	}
	return pass("internal/core/preempt.go preemptCheck with a line in range")
}

// DefaultCores = 4, neumann.go line 40 at v0.3.0.
func checkCores(c *CheckCtx) Verdict {
	if v := wantNumber(c.Answer, 4, 0); !v.Pass {
		return v
	}
	if !strings.Contains(c.Answer, "neumann.go") {
		return fail("does not name neumann.go")
	}
	if lines := fileLine(c.Answer, "neumann.go"); !inRange(lines, 36, 44) && !inRange(ints(c.Answer), 36, 44) {
		return fail("no line within 36-44 (file:line %v)", lines)
	}
	return pass("4, neumann.go:40")
}

// levelOf in observe/log.go at v0.3.0.
var warnKinds = []string{"model_retry", "throttled", "group_down", "migrate_refused", "steer_dropped", "tool_late", "tool_denied", "approval_needed"}

func checkWarn(c *CheckCtx) Verdict {
	// Either the event kinds or their constant names count.
	consts := map[string]string{"model_retry": "EvModelRetry", "throttled": "EvThrottled", "group_down": "EvGroupDown", "migrate_refused": "EvMigrateRefused",
		"steer_dropped": "EvSteerDropped", "tool_late": "EvToolLate", "tool_denied": "EvToolDenied", "approval_needed": "EvApprovalNeeded"}
	var missing []string
	for _, k := range warnKinds {
		if !strings.Contains(c.Answer, k) && !strings.Contains(c.Answer, consts[k]) {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fail("missing %s", strings.Join(missing, ", "))
	}
	if strings.Contains(c.Answer, "reconcile_needed") && !anyOf(c.Answer, "error level", "level error", "errorlevel", "at error") {
		return fail("lists reconcile_needed (an error-level event) as warn")
	}
	return pass("all 8 warn kinds")
}

func checkLog(c *CheckCtx) Verdict {
	f := c.Facts.Log
	a := c.Answer
	if !(anyOf(a, "pool") && anyOf(a, "exhaust", "all 20 connections", "saturat", "ran out of connections", "out of connections")) {
		return fail("root cause is not database connection pool exhaustion")
	}
	if !strings.Contains(a, f.FirstError) {
		return fail("first error time %s not given", f.FirstError)
	}
	if v := wantAll(a, f.Endpoints...); !v.Pass {
		return fail("endpoints: %s", v.Detail)
	}
	if v := wantNumber(a, float64(f.Failed), 0); !v.Pass {
		return fail("failed request count %d not given (numbers seen: %v)", f.Failed, numbers(a))
	}
	return pass("pool exhaustion at %s, %d failed requests on %d endpoints", f.FirstError, f.Failed, len(f.Endpoints))
}

func checkSales(c *CheckCtx) Verdict {
	f := c.Facts.Sales
	if !strings.Contains(c.Answer, f.Q3Top) {
		return fail("does not name %s", f.Q3Top)
	}
	if v := wantNumber(c.Answer, float64(f.Q3TopCents)/100, 0.005); !v.Pass {
		return fail("total %s not given (numbers seen: %v)", cents(f.Q3TopCents), numbers(c.Answer))
	}
	return pass("%s %s", f.Q3Top, cents(f.Q3TopCents))
}

// The bug: session/store.go, Get returns on !ok while holding s.mu (line
// 42-44 of the new file): the next Lock deadlocks.
func checkReview(c *CheckCtx) Verdict {
	a := c.Answer
	if !strings.Contains(a, "store.go") {
		return fail("does not name session/store.go")
	}
	if !strings.Contains(a, "Get") {
		return fail("does not name Get")
	}
	if !anyOf(a, "unlock", "deadlock", "never released", "not released", "still held", "held forever", "remains locked", "stays locked", "leaks the lock", "without releasing") {
		return fail("does not say the lock is left held")
	}
	if !(anyOf(a, "!ok", "not found", "missing", "does not exist", "doesn't exist", "absent", "not present", "unknown id", "not in the map", "early return", "no session") || inRange(fileLine(a, "store.go"), 39, 46) || anyOf(a, "line 42", "line 43", "line 44", "lines 42", "lines 41", "line 41")) {
		return fail("does not point at the not-found branch")
	}
	return pass("names the lock left held on Get's not-found return")
}

func checkFix(c *CheckCtx) Verdict {
	dir := filepath.Join(c.Root, c.Inst.FixDir)
	b, err := os.ReadFile(filepath.Join(dir, "window_test.go"))
	if err != nil {
		return fail("test file: %v", err)
	}
	s := sha256.Sum256(b)
	if hex.EncodeToString(s[:]) != c.Inst.TestSum {
		return fail("the test file was changed")
	}
	writes := 0
	for _, e := range c.Ledger {
		if e.Status == "applied" && strings.HasPrefix(e.Path, c.Inst.FixDir+"/") {
			writes++
		}
	}
	if writes == 0 {
		return fail("no fs_write under %s in the ledger", c.Inst.FixDir)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-count=1", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod", "GOTOOLCHAIN=local")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fail("go test still fails: %s", tail(string(out), 300))
	}
	return pass("go test passes after %d write(s)", writes)
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

var headingRe = regexp.MustCompile(`(?m)^\s{0,3}(#{1,6}\s.*|\*\*[^*\n]+\*\*:?\s*|[A-Z][A-Za-z /&]+:\s*)$`)

func checkChangelog(c *CheckCtx) Verdict {
	p := filepath.Join(c.Root, c.Inst.OutDir, "CHANGELOG.md")
	b, err := os.ReadFile(p)
	if err != nil {
		return fail("%s/CHANGELOG.md was not written", c.Inst.OutDir)
	}
	doc := string(b)
	var missing []string
	for _, cm := range c.Facts.Changelog {
		if !anyOf(doc, cm.Topic...) {
			missing = append(missing, cm.Subject)
		}
	}
	if len(missing) > 0 {
		return fail("CHANGELOG.md misses: %s", strings.Join(missing, "; "))
	}
	// Grouping: each feature under a heading that says feature/added,
	// each fix under one that says fix/bug.
	sections := splitSections(doc)
	for _, cm := range c.Facts.Changelog {
		var want []string
		switch cm.Type {
		case "feat":
			want = []string{"feat", "feature", "added", "new"}
		case "fix":
			want = []string{"fix", "bug"}
		default:
			continue
		}
		ok := false
		for _, s := range sections {
			if anyOf(s.body, cm.Topic...) && anyOf(s.heading, want...) {
				ok = true
				break
			}
		}
		if !ok {
			return fail("%q is not under a %s heading", cm.Subject, want[0])
		}
	}
	return pass("all %d commits, features and fixes grouped", len(c.Facts.Changelog))
}

type section struct{ heading, body string }

func splitSections(doc string) []section {
	idx := headingRe.FindAllStringIndex(doc, -1)
	var out []section
	for i, m := range idx {
		end := len(doc)
		if i+1 < len(idx) {
			end = idx[i+1][0]
		}
		out = append(out, section{heading: doc[m[0]:m[1]], body: doc[m[1]:end]})
	}
	return out
}
