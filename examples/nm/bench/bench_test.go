package bench

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var testdata = os.DirFS("..")

var (
	setupOnce sync.Once
	setupDir  string
	setupF    *Facts
	setupErr  error
)

// workspace builds the fixtures once for the package's tests.
func workspace(t *testing.T) (string, *Facts) {
	t.Helper()
	setupOnce.Do(func() {
		dir, err := os.MkdirTemp("", "nm-bench-test-")
		if err != nil {
			setupErr = err
			return
		}
		setupDir = filepath.Join(dir, "ws")
		setupF, setupErr = Setup(setupDir, "../../..", testdata)
	})
	if setupErr != nil {
		t.Skipf("fixtures need git and the v0.3.0 tag: %v", setupErr)
	}
	return setupDir, setupF
}

func TestMain(m *testing.M) {
	code := m.Run()
	if setupDir != "" {
		os.RemoveAll(filepath.Dir(setupDir))
	}
	os.Exit(code)
}

// The log's facts agree with an independent reading of the file.
func TestLogFactsMatchTheFile(t *testing.T) {
	var buf bytes.Buffer
	f := GenLog(&buf)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if f.Lines != len(lines) || f.Lines < 3000 || f.Lines > 5500 {
		t.Fatalf("lines: facts %d, file %d", f.Lines, len(lines))
	}
	failed, first := 0, ""
	firstErr := ""
	for _, l := range lines {
		if strings.Contains(l, " ERROR ") && firstErr == "" && (strings.Contains(l, "pool exhausted") || strings.Contains(l, "status=503")) {
			firstErr = l[11:19]
		}
		if strings.Contains(l, "status=503") {
			failed++
			if first == "" {
				first = l[11:19]
			}
			if !strings.Contains(l, "pool exhausted") {
				t.Fatalf("a 503 that is not the pool: %s", l)
			}
		}
	}
	if failed != f.Failed || first != f.FirstError || firstErr != f.FirstError {
		t.Fatalf("facts %+v; file: %d failed, first 503 %s, first pool error %s", f, failed, first, firstErr)
	}
	if f.FirstError != "14:07:31" || failed < 100 {
		t.Fatalf("unexpected incident: %+v", f)
	}
	// Deterministic.
	var again bytes.Buffer
	GenLog(&again)
	if again.String() != buf.String() {
		t.Fatal("GenLog is not deterministic")
	}
}

func TestSalesFactsMatchTheFile(t *testing.T) {
	var buf bytes.Buffer
	f := GenSales(&buf)
	rows, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != f.Lines {
		t.Fatalf("lines %d, facts %d", len(rows), f.Lines)
	}
	q3 := map[string]int64{}
	n := map[string]int{}
	for _, r := range rows[1:] {
		month, _ := strconv.Atoi(r[1][5:7])
		units, _ := strconv.ParseInt(r[4], 10, 64)
		price, _ := strconv.ParseFloat(r[5], 64)
		rev, _ := strconv.ParseFloat(r[6], 64)
		if c := int64(price*100+0.5) * units; c != int64(rev*100+0.5) {
			t.Fatalf("row %v: revenue is not units × price", r)
		}
		if month >= 7 && month <= 9 {
			q3[r[2]] += int64(rev*100 + 0.5)
			n[r[2]]++
		}
	}
	if q3[f.Q3Top] != f.Q3TopCents || n[f.Q3Top] != f.Q3TopOrders {
		t.Fatalf("facts %+v; file: %v %v", f, q3, n)
	}
	for reg, c := range q3 {
		if reg != f.Q3Top && c >= f.Q3TopCents {
			t.Fatalf("%s %d beats %s", reg, c, f.Q3Top)
		}
	}
	if f.Q3Top == f.YearTop {
		t.Fatal("Q3's top region is the year's: the task would not need the Q3 filter")
	}
}

func TestSetupBuildsTheWorkspace(t *testing.T) {
	dir, f := workspace(t)
	for _, p := range []string{"repo/neumann.go", "repo/internal/core/preempt.go", "logs/service.log", "data/sales.csv", "review/change.diff", "changelog/.git", "out", "fixbug"} {
		if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
	// The expected answers of repo-qa hold for the snapshot.
	src, _ := os.ReadFile(filepath.Join(dir, "repo/internal/core/preempt.go"))
	if line := lineOf(src, "func (e *Env) preemptCheck("); line < 138 || line > 207 {
		t.Errorf("preemptCheck at line %d", line)
	}
	src, _ = os.ReadFile(filepath.Join(dir, "repo/neumann.go"))
	if line := lineOf(src, "DefaultCores     = 4"); line < 36 || line > 44 {
		t.Errorf("DefaultCores at line %d", line)
	}
	src, _ = os.ReadFile(filepath.Join(dir, "repo/observe/log.go"))
	m := regexp.MustCompile(`(?s)return slog.LevelDebug, true\s*case (.*?):\s*return slog.LevelWarn`).FindSubmatch(src)
	if m == nil {
		t.Fatal("levelOf not found")
	}
	for _, k := range []string{"EvModelRetry", "EvThrottled", "EvGroupDown", "EvMigrateRefused", "EvSteerDropped", "EvToolLate", "EvToolDenied", "EvApprovalNeeded"} {
		if !bytes.Contains(m[1], []byte(k)) {
			t.Errorf("%s is not logged at warn in the snapshot", k)
		}
	}
	if strings.Count(string(m[1]), "proto.Ev") != len(warnKinds) {
		t.Errorf("warn kinds in the snapshot: %s", m[1])
	}
	if len(f.Changelog) != 10 {
		t.Fatalf("changelog commits: %d", len(f.Changelog))
	}
	// Setup again is a no-op that returns the same facts.
	again, err := Setup(dir, "../../..", testdata)
	if err != nil || again.Log.Failed != f.Log.Failed || again.Sales != f.Sales {
		t.Fatalf("second Setup: %v %+v", err, again)
	}
}

func lineOf(src []byte, needle string) int {
	sc := bufio.NewScanner(bytes.NewReader(src))
	for n := 1; sc.Scan(); n++ {
		if strings.Contains(sc.Text(), needle) {
			return n
		}
	}
	return -1
}

func TestCheckers(t *testing.T) {
	dir, f := workspace(t)
	ctx := func(answer string) *CheckCtx { return &CheckCtx{Answer: answer, Facts: f, Root: dir} }
	cases := []struct {
		task   string
		answer string
		want   bool
	}{
		{"repo-qa.preempt", "The decision is in `internal/core/preempt.go:153`, function `preemptCheck` on *Env.", true},
		{"repo-qa.preempt", "internal/core/preempt.go, function preemptCheck.", false},
		{"repo-qa.preempt", "**File:** `repo/internal/core/preempt.go`  \n**Function:** `preemptCheck`  \n**Line:** 153", true},
		{"repo-qa.preempt", "internal/sched/sched.go:597 in Urgent", false},
		{"repo-qa.cores", "It runs 4 vCores: DefaultCores = 4 at neumann.go:40.", true},
		{"repo-qa.cores", "8 vCores, neumann.go:40", false},
		{"repo-qa.warn", "model_retry, throttled, group_down, migrate_refused, steer_dropped, tool_late, tool_denied, approval_needed", true},
		{"repo-qa.warn", "EvModelRetry, EvThrottled, EvGroupDown, EvMigrateRefused, EvSteerDropped, EvToolLate, EvToolDenied and EvApprovalNeeded", true},
		{"repo-qa.warn", "model_retry, throttled, group_down", false},
		{"log-triage", "Root cause: the DB connection pool was exhausted (all 20 connections in use) starting 14:07:31 UTC; /api/orders, /api/checkout and /api/inventory returned 503, " + strconv.Itoa(f.Log.Failed) + " requests failed.", true},
		{"log-triage", "The search backend was down at 13:22:10.", false},
		{"log-triage", "DB pool exhausted at 14:07:31 on /api/orders, /api/checkout, /api/inventory; 12 requests failed.", false},
		{"csv-stats", "The top region in Q3 was " + f.Sales.Q3Top + " with $" + commas(f.Sales.Q3TopCents) + ".", true},
		{"csv-stats", "The top region in Q3 was " + f.Sales.Q3Top + " with " + cents(f.Sales.Q3TopCents+100) + ".", false},
		{"csv-stats", "The top region was " + f.Sales.YearTop + " with " + cents(f.Sales.YearTopCents) + ".", false},
		{"code-review", "Bug in session/store.go, Store.Get: when the id is not found (`!ok`) it returns while still holding s.mu — the mutex is never unlocked and the next call deadlocks.", true},
		{"code-review", "session/store.go Get switched from RWMutex to Mutex, which serializes readers.", false},
		{"quick.lines", "It has " + strconv.Itoa(f.Sales.Lines) + " lines.", true},
		{"quick.percent", "18% of 2450.75 is 441.135.", true},
		{"quick.percent", "about 441", false},
		{"quick.where", "repo/neumann.go", true},
	}
	for _, c := range cases {
		v := TaskByID(c.task).Check(ctx(c.answer))
		if v.Pass != c.want {
			t.Errorf("%s(%q) = %+v, want pass=%v", c.task, c.answer, v, c.want)
		}
	}
}

func commas(c int64) string {
	s := strconv.FormatInt(c/100, 10)
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String() + "." + cents(c)[len(cents(c))-2:]
}

func TestChangelogChecker(t *testing.T) {
	dir, f := workspace(t)
	inst := &Instance{ID: "x", OutDir: "out/test-changelog"}
	write := func(doc string) Verdict {
		os.MkdirAll(filepath.Join(dir, inst.OutDir), 0o755)
		os.WriteFile(filepath.Join(dir, inst.OutDir, "CHANGELOG.md"), []byte(doc), 0o644)
		return checkChangelog(&CheckCtx{Inst: inst, Facts: f, Root: dir})
	}
	good := `# Changelog

## Features
- Add CSV export for monthly reports
- Add rate limit headers to API responses
- Support pagination on /v1/orders

## Bug Fixes
- Handle empty input in the parser
- Correct the timezone offset in digest emails
- Fix crash when the config file is missing

## Other
- Document retry settings
- Bump sqlite driver
- Avoid re-hashing cache keys
- Split server setup
`
	if v := write(good); !v.Pass {
		t.Fatalf("good changelog: %+v", v)
	}
	bad := strings.Replace(good, "- Fix crash when the config file is missing\n", "", 1) + "- Fix crash when the config file is missing\n"
	if v := write(bad); v.Pass {
		t.Fatalf("a fix under Other passed: %+v", v)
	}
	if v := write(strings.Replace(good, "- Support pagination on /v1/orders\n", "", 1)); v.Pass {
		t.Fatalf("a missing commit passed: %+v", v)
	}
	os.RemoveAll(filepath.Join(dir, inst.OutDir))
	if v := checkChangelog(&CheckCtx{Inst: inst, Facts: f, Root: dir}); v.Pass {
		t.Fatal("no file passed")
	}
}
