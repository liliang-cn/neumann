package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Facts are the expected answers, computed by the generators that built
// the workspace. They are saved next to it so a restarted bench (the crash
// scenario) checks against the same values.
type Facts struct {
	Log       LogFacts   `json:"log"`
	Sales     SalesFacts `json:"sales"`
	Changelog []Commit   `json:"changelog"`
	RepoTag   string     `json:"repo_tag"`
}

// LogFacts describe the planted incident in logs/service.log.
type LogFacts struct {
	Lines      int      `json:"lines"`
	FirstError string   `json:"first_error"` // HH:MM:SS UTC
	Failed     int      `json:"failed"`      // requests that failed because of it
	Endpoints  []string `json:"endpoints"`
}

// SalesFacts describe data/sales.csv.
type SalesFacts struct {
	Lines        int    `json:"lines"` // header included
	Q3Top        string `json:"q3_top"`
	Q3TopCents   int64  `json:"q3_top_cents"`
	Q3TopOrders  int    `json:"q3_top_orders"`
	Q3Second     string `json:"q3_second"`
	YearTop      string `json:"year_top"`
	YearTopCents int64  `json:"year_top_cents"`
}

// Commit is one commit of the changelog fixture repository.
type Commit struct {
	Subject string   `json:"subject"`
	Type    string   `json:"type"`  // feat, fix, docs, chore, perf, refactor
	Topic   []string `json:"topic"` // any of these, case-insensitive, shows it was covered
	File    string   `json:"file"`
	Content string   `json:"-"`
}

const (
	factsFile   = ".bench-facts.json"
	fixturesVer = "nm-fixtures-3"
	repoTag     = "v0.3.0"
)

// Setup builds the workspace in dir unless it is already built: the repo
// snapshot (git archive of repoTag from repoSrc), the generated log and CSV,
// the review diff, the changelog repository and out/. testdata holds the
// committed fixtures. It returns the facts.
func Setup(dir, repoSrc string, testdata fs.FS) (*Facts, error) {
	if b, err := os.ReadFile(filepath.Join(dir, factsFile)); err == nil {
		var f struct {
			Version string `json:"version"`
			Facts
		}
		if json.Unmarshal(b, &f) == nil && f.Version == fixturesVer {
			return &f.Facts, nil
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	facts := &Facts{RepoTag: repoTag}
	if err := snapshotRepo(repoSrc, repoTag, filepath.Join(dir, "repo")); err != nil {
		return nil, fmt.Errorf("repo snapshot: %w", err)
	}
	var buf bytes.Buffer
	facts.Log = GenLog(&buf)
	if err := writeFile(filepath.Join(dir, "logs", "service.log"), buf.Bytes()); err != nil {
		return nil, err
	}
	buf.Reset()
	facts.Sales = GenSales(&buf)
	if err := writeFile(filepath.Join(dir, "data", "sales.csv"), buf.Bytes()); err != nil {
		return nil, err
	}
	diff, err := fs.ReadFile(testdata, "testdata/review/change.diff")
	if err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(dir, "review", "change.diff"), diff); err != nil {
		return nil, err
	}
	facts.Changelog = changelogCommits()
	if err := buildChangelogRepo(filepath.Join(dir, "changelog"), facts.Changelog); err != nil {
		return nil, fmt.Errorf("changelog repo: %w", err)
	}
	for _, d := range []string{"out", "fixbug"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return nil, err
		}
	}
	b, _ := json.MarshalIndent(struct {
		Version string `json:"version"`
		*Facts
	}{fixturesVer, facts}, "", "  ")
	if err := writeFile(filepath.Join(dir, factsFile), b); err != nil {
		return nil, err
	}
	return facts, nil
}

func writeFile(p string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

// snapshotRepo extracts `git archive tag` of src into dst.
func snapshotRepo(src, tag, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	cmd := exec.Command("git", "-C", src, "archive", "--format=tar", tag)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git archive %s in %s: %v: %s", tag, src, err, errb.String())
	}
	tr := tar.NewReader(&out)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(h.Name)
		if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			return fmt.Errorf("bad path in archive: %s", h.Name)
		}
		p := filepath.Join(dst, name)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(p, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
}

// CopyFixbug copies the fix-bug module into dst (go.mod.txt becomes
// go.mod: a go.mod inside testdata would make it a module of its own,
// which go:embed refuses). It returns the SHA-256 of the test file, which
// a fix must leave alone.
func CopyFixbug(testdata fs.FS, dst string) (string, error) {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return "", err
	}
	ents, err := fs.ReadDir(testdata, "testdata/fixbug")
	if err != nil {
		return "", err
	}
	var testSum string
	for _, e := range ents {
		b, err := fs.ReadFile(testdata, "testdata/fixbug/"+e.Name())
		if err != nil {
			return "", err
		}
		name := strings.TrimSuffix(e.Name(), ".txt")
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			return "", err
		}
		if strings.HasSuffix(name, "_test.go") {
			s := sha256.Sum256(b)
			testSum = hex.EncodeToString(s[:])
		}
	}
	return testSum, nil
}

// --- the service log ---------------------------------------------------

type logLine struct {
	at   time.Time
	text string
}

// GenLog writes a service log of about four thousand lines with one
// planted incident: from 14:07:31 the database connection pool is
// exhausted and every request to the three endpoints that use the database
// fails with a 503 after its 5s acquire timeout, until the workers are
// restarted at 14:31. Around it: ordinary traffic, slow requests, 404s,
// validation errors, an unrelated search outage at 13:22, a deploy at
// 14:05, pool statistics, cache warnings. It is deterministic.
func GenLog(w io.Writer) LogFacts {
	r := rand.New(rand.NewPCG(7, 11))
	day := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	start, end := day.Add(13*time.Hour), day.Add(15*time.Hour)
	poolDown := day.Add(14*time.Hour + 7*time.Minute + 31*time.Second + 200*time.Millisecond)
	waitStart := poolDown.Add(-4*time.Second - 800*time.Millisecond) // requests that will time out at once
	poolUp := day.Add(14*time.Hour + 31*time.Minute + 2*time.Second)
	searchDown, searchUp := day.Add(13*time.Hour+22*time.Minute+10*time.Second), day.Add(13*time.Hour+25*time.Minute+40*time.Second)

	dbEndpoints := []string{"/api/orders", "/api/checkout", "/api/inventory"}
	type ep struct {
		path, method string
		db           bool
		weight       int
		base         int
	}
	eps := []ep{
		{"/api/orders", "GET", true, 30, 45},
		{"/api/checkout", "POST", true, 14, 120},
		{"/api/inventory", "GET", true, 16, 35},
		{"/api/search", "GET", false, 22, 80},
		{"/api/users", "GET", false, 12, 25},
		{"/healthz", "GET", false, 6, 1},
	}
	total := 0
	for _, e := range eps {
		total += e.weight
	}
	pick := func() ep {
		n := r.IntN(total)
		for _, e := range eps {
			if n < e.weight {
				return e
			}
			n -= e.weight
		}
		return eps[0]
	}
	var lines []logLine
	add := func(at time.Time, level, comp, msg string) {
		lines = append(lines, logLine{at, fmt.Sprintf("%s %-5s %-5s %s", at.Format("2006-01-02T15:04:05.000Z"), level, comp, msg)})
	}
	reqID := func() string { return fmt.Sprintf("r%07x", r.Uint32()&0xfffffff) }
	failed, first := 0, time.Time{}

	request := func(at time.Time, e ep) {
		id := reqID()
		dur := e.base + r.IntN(e.base+20)
		status := 200
		errMsg := ""
		switch {
		case e.db && !at.Before(waitStart) && at.Before(poolUp):
			status, dur, errMsg = 503, 5000+r.IntN(3), "db: acquire connection: timeout after 5s (pool exhausted)"
		case e.path == "/api/search" && !at.Before(searchDown) && at.Before(searchUp) && r.IntN(3) > 0:
			status, errMsg = 502, "search backend unavailable: connection refused"
		case e.path == "/api/users" && r.IntN(25) == 0:
			status, errMsg = 404, "user not found"
		case e.path == "/api/checkout" && r.IntN(30) == 0:
			status, errMsg = 400, "validation: missing field shipping_address"
		case e.db && at.After(poolDown.Add(-4*time.Minute)) && at.Before(waitStart):
			dur = dur*4 + r.IntN(900) // the pool fills up: requests slow down first
		}
		if r.IntN(60) == 0 && status == 200 {
			dur += 900 + r.IntN(1500)
		}
		done := at.Add(time.Duration(dur) * time.Millisecond)
		msg := fmt.Sprintf("req=%s method=%s path=%s status=%d dur_ms=%d", id, e.method, e.path, status, dur)
		switch {
		case status == 503:
			failed++
			if first.IsZero() || done.Before(first) {
				first = done
			}
			add(done, "ERROR", "http", msg+fmt.Sprintf(" err=%q", errMsg))
		case status >= 500:
			add(done, "ERROR", "http", msg+fmt.Sprintf(" err=%q", errMsg))
		case status >= 400:
			add(done, "WARN", "http", msg+fmt.Sprintf(" err=%q", errMsg))
		default:
			add(done, "INFO", "http", msg)
			if dur > 1000 {
				add(done, "WARN", "http", fmt.Sprintf("slow request req=%s path=%s dur_ms=%d", id, e.path, dur))
			}
		}
	}

	// One checkout is already waiting when the pool runs dry, so the first
	// failure lands in the same second as the pool's first error.
	request(waitStart.Add(150*time.Millisecond), eps[1])
	for at := start; at.Before(end); at = at.Add(time.Duration(300+r.IntN(3300)) * time.Millisecond) {
		request(at, pick())
	}
	// Periodic pool statistics, and the pool's own errors while exhausted.
	for at := start.Add(17 * time.Second); at.Before(end); at = at.Add(30 * time.Second) {
		inUse := 3 + r.IntN(6)
		waiting := 0
		switch {
		case !at.Before(poolDown) && at.Before(poolUp):
			inUse, waiting = 20, 6+r.IntN(30)
		case at.After(poolDown.Add(-4*time.Minute)) && at.Before(poolDown):
			inUse = 16 + int(4*at.Sub(poolDown.Add(-4*time.Minute))/(4*time.Minute))
			if inUse > 19 {
				inUse = 19
			}
		}
		add(at, "INFO", "db", fmt.Sprintf("pool stats in_use=%d idle=%d waiting=%d max=20", inUse, 20-inUse, waiting))
		if inUse >= 16 && inUse < 20 {
			add(at.Add(5*time.Millisecond), "WARN", "db", fmt.Sprintf("pool usage high in_use=%d max=20", inUse))
		}
	}
	add(poolDown, "ERROR", "db", "pool exhausted: all 20 connections in use, 7 waiters (acquire timeout 5s)")
	for at := poolDown.Add(time.Duration(9+r.IntN(9)) * time.Second); at.Before(poolUp); at = at.Add(time.Duration(9+r.IntN(12)) * time.Second) {
		add(at, "ERROR", "db", fmt.Sprintf("pool exhausted: all 20 connections in use, %d waiters (acquire timeout 5s)", 5+r.IntN(40)))
	}
	add(poolUp.Add(-7*time.Second), "WARN", "ops", "restarting order-service workers (manual, by on-call)")
	add(poolUp, "INFO", "db", "pool reset: 20 connections closed, 4 opened")
	add(day.Add(14*time.Hour+5*time.Minute+12*time.Second), "INFO", "ops", "deploy finished version=v2.14.0 (orders: batch inventory reservations)")
	add(searchDown.Add(-2*time.Second), "WARN", "search", "search-2: health check failed, removing from rotation")
	add(searchUp, "INFO", "search", "search-2: back in rotation")
	for at := start.Add(4 * time.Minute); at.Before(end); at = at.Add(time.Duration(5+r.IntN(11)) * time.Minute) {
		add(at, "WARN", "cache", fmt.Sprintf("cache miss rate %d%% over 5m (threshold 20%%)", 21+r.IntN(15)))
	}
	for at := start.Add(11 * time.Minute); at.Before(end); at = at.Add(time.Duration(13+r.IntN(20)) * time.Minute) {
		add(at, "INFO", "rt", fmt.Sprintf("gc pause %dms heap=%dMB", 2+r.IntN(30), 300+r.IntN(200)))
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].at.Before(lines[j].at) })
	for _, l := range lines {
		fmt.Fprintln(w, l.text)
	}
	return LogFacts{Lines: len(lines), FirstError: first.Format("15:04:05"), Failed: failed, Endpoints: dbEndpoints}
}

// --- the sales CSV ------------------------------------------------------

var (
	regions  = []string{"North", "South", "East", "West", "Central"}
	products = []struct {
		name  string
		cents int64
	}{{"Widget", 1250}, {"Gadget", 3999}, {"Gizmo", 725}, {"Doohickey", 12900}, {"Sprocket", 340}}
)

// GenSales writes 320 orders of 2026 as CSV: order_id, date, region,
// product, units, unit_price, revenue. The seed is chosen (deterministically)
// so that Q3's top region is not the year's, and leads Q3 by at least 3%.
func GenSales(w io.Writer) SalesFacts {
	for seed := uint64(1); ; seed++ {
		var buf bytes.Buffer
		f, ok := genSales(&buf, seed)
		if ok {
			w.Write(buf.Bytes())
			return f
		}
	}
}

func genSales(w io.Writer, seed uint64) (SalesFacts, bool) {
	r := rand.New(rand.NewPCG(seed, 2026))
	type row struct {
		date    time.Time
		region  string
		product int
		units   int64
	}
	var rows []row
	jan1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for range 320 {
		d := jan1.AddDate(0, 0, r.IntN(365))
		reg := regions[r.IntN(len(regions))]
		if r.IntN(5) == 0 {
			reg = "North" // North sells the most over the year
		}
		if d.Month() >= 7 && d.Month() <= 9 && r.IntN(6) == 0 {
			reg = "West" // West has a strong Q3
		}
		rows = append(rows, row{d, reg, r.IntN(len(products)), int64(1 + r.IntN(40))})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].date.Before(rows[j].date) })
	year, q3, q3n := map[string]int64{}, map[string]int64{}, map[string]int{}
	fmt.Fprintln(w, "order_id,date,region,product,units,unit_price,revenue")
	for i, rw := range rows {
		p := products[rw.product]
		rev := p.cents * rw.units
		year[rw.region] += rev
		if m := rw.date.Month(); m >= 7 && m <= 9 {
			q3[rw.region] += rev
			q3n[rw.region]++
		}
		fmt.Fprintf(w, "O-%05d,%s,%s,%s,%d,%s,%s\n", 10001+i, rw.date.Format("2006-01-02"), rw.region, p.name, rw.units, cents(p.cents), cents(rev))
	}
	top := func(m map[string]int64) (string, string) {
		names := append([]string(nil), regions...)
		sort.Slice(names, func(i, j int) bool { return m[names[i]] > m[names[j]] })
		return names[0], names[1]
	}
	q3Top, q3Second := top(q3)
	yTop, _ := top(year)
	f := SalesFacts{Lines: len(rows) + 1, Q3Top: q3Top, Q3TopCents: q3[q3Top], Q3TopOrders: q3n[q3Top], Q3Second: q3Second, YearTop: yTop, YearTopCents: year[yTop]}
	ok := q3Top != yTop && float64(q3[q3Top]) >= 1.03*float64(q3[q3Second])
	return f, ok
}

func cents(c int64) string { return fmt.Sprintf("%d.%02d", c/100, c%100) }

// --- the changelog repository ------------------------------------------

func changelogCommits() []Commit {
	return []Commit{
		{Subject: "feat(export): add CSV export for monthly reports", Type: "feat", Topic: []string{"csv"}, File: "export/csv.go", Content: "package export\n\n// CSV writes a report as CSV.\nfunc CSV() {}\n"},
		{Subject: "fix(parser): handle empty input without panicking", Type: "fix", Topic: []string{"empty input", "empty"}, File: "parser/parse.go", Content: "package parser\n\n// Parse parses s; empty input yields nil.\nfunc Parse(s string) []string {\n\tif s == \"\" {\n\t\treturn nil\n\t}\n\treturn []string{s}\n}\n"},
		{Subject: "docs: document the retry settings in the README", Type: "docs", Topic: []string{"retry", "retries"}, File: "README.md", Content: "# reports\n\n## Retries\n\nSet RETRY_MAX and RETRY_BACKOFF.\n"},
		{Subject: "Add rate limit headers to API responses", Type: "feat", Topic: []string{"rate limit", "rate-limit", "ratelimit"}, File: "api/headers.go", Content: "package api\n\n// RateLimitHeaders are sent on every response.\nvar RateLimitHeaders = []string{\"X-RateLimit-Limit\", \"X-RateLimit-Remaining\"}\n"},
		{Subject: "feat(api): support pagination on /v1/orders", Type: "feat", Topic: []string{"pagination", "paginat"}, File: "api/orders.go", Content: "package api\n\n// Orders lists orders, a page at a time.\nfunc Orders(page, size int) {}\n"},
		{Subject: "fix: correct the timezone offset in daily digest emails", Type: "fix", Topic: []string{"timezone", "time zone"}, File: "digest/digest.go", Content: "package digest\n\nimport \"time\"\n\n// Local is the zone digests are sent in.\nvar Local = time.UTC\n"},
		{Subject: "chore(deps): bump the sqlite driver to 1.60", Type: "chore", Topic: []string{"sqlite"}, File: "deps.txt", Content: "modernc.org/sqlite v1.60.1\n"},
		{Subject: "Fix crash when the config file is missing", Type: "fix", Topic: []string{"config"}, File: "config/load.go", Content: "package config\n\nimport \"os\"\n\n// Load reads path; a missing file gives the defaults.\nfunc Load(path string) map[string]string {\n\tif _, err := os.Stat(path); err != nil {\n\t\treturn map[string]string{}\n\t}\n\treturn nil\n}\n"},
		{Subject: "perf(cache): avoid re-hashing keys on every lookup", Type: "perf", Topic: []string{"hash", "cache"}, File: "cache/cache.go", Content: "package cache\n\n// Key is a precomputed hash.\ntype Key uint64\n"},
		{Subject: "refactor: split server setup into smaller functions", Type: "refactor", Topic: []string{"server setup", "server"}, File: "server/setup.go", Content: "package server\n\nfunc listen()  {}\nfunc routes()  {}\nfunc Setup() { listen(); routes() }\n"},
	}
}

func buildChangelogRepo(dir string, commits []Commit) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	env := append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_AUTHOR_NAME=Dana Reyes", "GIT_AUTHOR_EMAIL=dana@example.com",
		"GIT_COMMITTER_NAME=Dana Reyes", "GIT_COMMITTER_EMAIL=dana@example.com")
	git := func(extra []string, args ...string) error {
		cmd := exec.Command("git", args...)
		cmd.Dir, cmd.Env = dir, append(env, extra...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git %v: %v: %s", args, err, out)
		}
		return nil
	}
	if err := git(nil, "init", "-q", "-b", "main"); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(dir, "README.md"), []byte("# reports\n")); err != nil {
		return err
	}
	when := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	dates := func(t time.Time) []string {
		s := t.Format(time.RFC3339)
		return []string{"GIT_AUTHOR_DATE=" + s, "GIT_COMMITTER_DATE=" + s}
	}
	if err := git(nil, "add", "-A"); err != nil {
		return err
	}
	if err := git(dates(when), "commit", "-q", "-m", "initial import"); err != nil {
		return err
	}
	if err := git(dates(when), "tag", "v1.3.0"); err != nil {
		return err
	}
	for i, c := range commits {
		if err := writeFile(filepath.Join(dir, c.File), []byte(c.Content)); err != nil {
			return err
		}
		if err := git(nil, "add", "-A"); err != nil {
			return err
		}
		if err := git(dates(when.Add(time.Duration(i+1)*26*time.Hour)), "commit", "-q", "-m", c.Subject); err != nil {
			return err
		}
	}
	return nil
}
