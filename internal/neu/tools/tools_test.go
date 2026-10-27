package tools

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/bus"
)

// workspace builds root/ with a few files, an out/ dir, and next to it a
// secret file that symlinks inside root point at.
func workspace(t *testing.T) (root, secret string) {
	t.Helper()
	base := t.TempDir()
	root = filepath.Join(base, "ws")
	must(t, os.MkdirAll(filepath.Join(root, "src", "deep"), 0o755))
	must(t, os.WriteFile(filepath.Join(root, "src", "a.go"), []byte("package a\n\nfunc Hello() string { return \"hi\" }\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(root, "src", "deep", "b.txt"), []byte("one\ntwo\nthree\nHello again\n"), 0o644))
	secret = filepath.Join(base, "secret.txt")
	must(t, os.WriteFile(secret, []byte("TOP SECRET Hello\n"), 0o644))
	must(t, os.Symlink(secret, filepath.Join(root, "src", "leak.txt")))
	must(t, os.Symlink(base, filepath.Join(root, "updir")))
	return root, secret
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newTools(t *testing.T, root string, writable ...string) *Tools {
	t.Helper()
	if writable == nil {
		writable = []string{"out"}
	}
	box, err := NewSandbox(root, writable)
	must(t, err)
	tl, err := New(box)
	must(t, err)
	return tl
}

func call(t *testing.T, tl *Tools, tool string, args any, key string) (string, bool) {
	t.Helper()
	b, _ := json.Marshal(args)
	dev, name, _ := strings.Cut(tool, "_")
	for _, d := range tl.Devices() {
		if d.Name() != dev {
			continue
		}
		res, err := d.Call(context.Background(), bus.DeviceCall{Tool: name, Args: b, Key: key, Task: "t1"})
		if err != nil {
			return err.Error(), true
		}
		return res.Content, res.IsError
	}
	t.Fatalf("no device for %s", tool)
	return "", true
}

func TestSandboxRefusesEscapes(t *testing.T) {
	root, secret := workspace(t)
	tl := newTools(t, root)
	for _, p := range []string{"../secret.txt", "/etc/passwd", secret, "src/leak.txt", "updir/secret.txt", "src/../../secret.txt"} {
		if out, isErr := call(t, tl, "fs_read", map[string]any{"path": p}, ""); !isErr || strings.Contains(out, "SECRET") {
			t.Errorf("fs_read %q = %q, %v; want refused", p, out, isErr)
		}
	}
	if out, isErr := call(t, tl, "fs_list", map[string]any{"path": "updir"}, ""); !isErr {
		t.Errorf("fs_list updir = %q; want refused", out)
	}
	out, isErr := call(t, tl, "fs_grep", map[string]any{"pattern": "Hello"}, "")
	if isErr || strings.Contains(out, "SECRET") || !strings.Contains(out, "src/a.go:3:") || !strings.Contains(out, "src/deep/b.txt:4:") {
		t.Errorf("fs_grep = %q; want both workspace matches and no secret", out)
	}
	out, _ = call(t, tl, "fs_list", map[string]any{"depth": 3}, "")
	if !strings.Contains(out, "leak.txt -> (outside the workspace)") || !strings.Contains(out, "b.txt (") {
		t.Errorf("fs_list = %q", out)
	}
}

func TestReadNumbersAndPages(t *testing.T) {
	root, _ := workspace(t)
	var lines []string
	for i := 1; i <= 450; i++ {
		lines = append(lines, "line")
	}
	must(t, os.WriteFile(filepath.Join(root, "big.txt"), []byte(strings.Join(lines, "\n")+"\n"), 0o644))
	tl := newTools(t, root)
	out, isErr := call(t, tl, "fs_read", map[string]any{"path": "big.txt"}, "")
	if isErr || !strings.HasPrefix(out, "big.txt: lines 1-200 of 450\n") || !strings.Contains(out, "continue with offset_line=201") {
		t.Fatalf("fs_read = %.200q", out)
	}
	out, _ = call(t, tl, "fs_read", map[string]any{"path": "big.txt", "offset_line": 440, "limit_lines": 1000}, "")
	if !strings.HasPrefix(out, "big.txt: lines 440-450 of 450\n") || !strings.Contains(out, "450| line") {
		t.Fatalf("fs_read tail = %q", out)
	}
}

func TestGrepCountsBeyondWhatItShows(t *testing.T) {
	root, _ := workspace(t)
	var b strings.Builder
	for i := range 250 {
		b.WriteString("ERROR x\n")
		if i%2 == 0 {
			b.WriteString("INFO y\n")
		}
	}
	must(t, os.WriteFile(filepath.Join(root, "app.log"), []byte(b.String()), 0o644))
	tl := newTools(t, root)
	out, _ := call(t, tl, "fs_grep", map[string]any{"pattern": "^ERROR", "path": "app.log"}, "")
	if !strings.Contains(out, "250 matching lines in 1 files (showing the first 100") {
		t.Fatalf("fs_grep = %q", out[len(out)-200:])
	}
	out, _ = call(t, tl, "fs_grep", map[string]any{"pattern": "error", "ignore_case": true, "count_only": true}, "")
	if !strings.Contains(out, "app.log: 250\n") {
		t.Fatalf("count_only = %q", out)
	}
	out, _ = call(t, tl, "fs_grep", map[string]any{"pattern": "Hello", "glob": "*.go"}, "")
	if !strings.Contains(out, "1 matching lines in 1 files") {
		t.Fatalf("glob = %q", out)
	}
}

func TestWriteIsConfinedAndIdempotent(t *testing.T) {
	root, secret := workspace(t)
	tl := newTools(t, root, "out", "src/deep")
	for _, p := range []string{"src/a.go", "../x.txt", "out/.ledger", "updir/x.txt", "out"} {
		if out, isErr := call(t, tl, "fs_write", map[string]any{"path": p, "content": "x"}, "k0"); !isErr {
			t.Errorf("fs_write %q = %q; want refused", p, out)
		}
	}
	must(t, os.Symlink(filepath.Dir(secret), filepath.Join(root, "out", "esc")))
	if out, isErr := call(t, tl, "fs_write", map[string]any{"path": "out/esc/new.txt", "content": "x"}, "k0"); !isErr {
		t.Errorf("write through a symlink out of the workspace = %q; want refused", out)
	}

	out, isErr := call(t, tl, "fs_write", map[string]any{"path": "out/notes/a.md", "content": "hello"}, "t1/c-1/1")
	if isErr || !strings.Contains(out, "wrote 5 bytes") {
		t.Fatalf("first write = %q", out)
	}
	must(t, os.WriteFile(filepath.Join(root, "out", "notes", "a.md"), []byte("changed by hand"), 0o644))
	out, isErr = call(t, tl, "fs_write", map[string]any{"path": "out/notes/a.md", "content": "hello"}, "t1/c-1/1")
	if isErr || !strings.Contains(out, "already written") {
		t.Fatalf("repeat = %q", out)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "out", "notes", "a.md")); string(b) != "changed by hand" {
		t.Fatalf("a repeat of an applied key wrote again: %q", b)
	}
	if out, _ := call(t, tl, "fs_write", map[string]any{"path": "src/deep/b.txt", "content": "fixed"}, "t1/c-2/1"); !strings.Contains(out, "wrote") {
		t.Fatalf("write to second writable dir = %q", out)
	}

	// A new process (a restart) reads the ledger and still refuses the key.
	tl2 := newTools(t, root, "out", "src/deep")
	if _, ok := tl2.Ledger.Applied("t1/c-1/1"); !ok {
		t.Fatal("reopened ledger lost an applied key")
	}
	out, _ = call(t, tl2, "fs_write", map[string]any{"path": "out/notes/a.md", "content": "hello"}, "t1/c-1/1")
	if !strings.Contains(out, "already written") {
		t.Fatalf("repeat after reopen = %q", out)
	}
	ents, err := ReadLedger(filepath.Join(root, "out", ".ledger"))
	must(t, err)
	var applied, dup int
	for _, e := range ents {
		switch e.Status {
		case "applied":
			applied++
		case "duplicate":
			dup++
		}
	}
	if applied != 2 || dup != 2 {
		t.Fatalf("ledger: %d applied, %d duplicate; want 2, 2: %+v", applied, dup, ents)
	}
}

func TestGitAllowlist(t *testing.T) {
	for _, ok := range [][]string{{"log", "--oneline"}, {"show", "HEAD"}, {"diff", "HEAD~1"}, {"ls-files", "-o"}, {"blame", "a.go"}} {
		if err := GitArgs(ok); err != nil {
			t.Errorf("GitArgs(%q) = %v", ok, err)
		}
	}
	for _, bad := range [][]string{{}, {"commit", "-m", "x"}, {"push"}, {"-c", "x=y", "log"}, {"diff", "--output=/tmp/x"}, {"diff", "--no-index", "/etc/passwd", "a"}, {"log", "--ext-diff"}, {"config", "--list"}, {"checkout", "."}} {
		if err := GitArgs(bad); err == nil {
			t.Errorf("GitArgs(%q) allowed", bad)
		}
	}
}

func TestGitRunsInWorkspaceRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	root, _ := workspace(t)
	repo := filepath.Join(root, "repo")
	must(t, os.MkdirAll(repo, 0o755))
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "first commit"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	tl := newTools(t, root)
	out, isErr := call(t, tl, "git_run", map[string]any{"dir": "repo", "args": []string{"log", "--oneline"}}, "")
	if isErr || !strings.Contains(out, "first commit") {
		t.Fatalf("git log = %q, %v", out, isErr)
	}
	if out, isErr := call(t, tl, "git_run", map[string]any{"dir": "repo", "args": []string{"commit", "--allow-empty", "-m", "x"}}, ""); !isErr {
		t.Fatalf("git commit allowed: %q", out)
	}
	// src/ is not a repository, and git must not find one above the root.
	if out, isErr := call(t, tl, "git_run", map[string]any{"dir": "src", "args": []string{"status"}}, ""); !isErr {
		t.Fatalf("git status outside any repo = %q; want an error", out)
	}
}

func TestCalc(t *testing.T) {
	for expr, want := range map[string]string{
		"1+2*3":                     "7",
		"(1+2)*3":                   "9",
		"0.1+0.2":                   "0.3",
		"sum(1250.50, 99.95, 0.05)": "1350.5",
		"avg(1,2)":                  "1.5",
		"max(3, -1, 7.5)":           "7.5",
		"round(2/3, 2)":             "0.67",
		"2^10":                      "1024",
		"-3 + abs(-5)":              "2",
		"17.5/100*2480":             "434",
		"10/4":                      "2.5",
	} {
		got, err := Calc(expr)
		if err != nil || got != want {
			t.Errorf("Calc(%q) = %q, %v; want %q", expr, got, err, want)
		}
	}
	for _, bad := range []string{"1/0", "1,234+1", "foo(1)", "(1+2", "", "2^0.5", "x"} {
		if got, err := Calc(bad); err == nil {
			t.Errorf("Calc(%q) = %q; want an error", bad, got)
		}
	}
}

// With the whole root writable, the store's and git's directories stay
// closed.
func TestWholeRootWritableKeepsReservedDirs(t *testing.T) {
	root := t.TempDir()
	box, err := NewSandbox(root, []string{"."})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{".neu/state.db", ".git/config", ".neu"} {
		if _, err := box.Writable(p); err == nil {
			t.Errorf("%s writable", p)
		}
	}
	for _, p := range []string{"notes.md", "docs/a.md", ".neutral/x"} {
		if _, err := box.Writable(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}
