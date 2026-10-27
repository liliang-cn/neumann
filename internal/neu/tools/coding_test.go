package tools

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
)

func codingTools(t *testing.T, root string, writable ...string) *Tools {
	t.Helper()
	tl := newTools(t, root, writable...)
	tl.Coding, tl.WriteAnywhere = true, true
	tl.Sh = Shell{Script: true}
	return tl
}

func read(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	must(t, err)
	return string(b)
}

func TestEditReplacesExactUniqueText(t *testing.T) {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	a := filepath.Join(root, "src", "a.go")

	out, isErr := call(t, tl, "fs_edit", map[string]any{"path": "src/a.go", "old_string": `return "hi"`, "new_string": `return "hello"`}, "t1/c1/1")
	if isErr || !strings.Contains(out, "edited src/a.go: 1 replacement") || !strings.Contains(out, `-func Hello() string { return "hi" }`) || !strings.Contains(out, `+func Hello() string { return "hello" }`) {
		t.Fatalf("edit = %q", out)
	}
	if got := read(t, a); got != "package a\n\nfunc Hello() string { return \"hello\" }\n" {
		t.Fatalf("file %q", got)
	}

	out, isErr = call(t, tl, "fs_edit", map[string]any{"path": "src/a.go", "old_string": "nothere", "new_string": "x"}, "t1/c2/1")
	if !isErr || !strings.Contains(out, "old_string not found in src/a.go") || !strings.Contains(out, "fs_read") {
		t.Fatalf("not found = %q, %v", out, isErr)
	}
	out, isErr = call(t, tl, "fs_edit", map[string]any{"path": "src/a.go", "old_string": "  return \"hello\"  ", "new_string": "x"}, "t1/c2b/1")
	if !isErr || !strings.Contains(out, "without its leading or trailing whitespace") {
		t.Fatalf("whitespace hint = %q", out)
	}

	must(t, os.WriteFile(filepath.Join(root, "dup.txt"), []byte("x = 1\ny = 2\nx = 1\n"), 0o640))
	out, isErr = call(t, tl, "fs_edit", map[string]any{"path": "dup.txt", "old_string": "x = 1", "new_string": "x = 3"}, "t1/c3/1")
	if !isErr || !strings.Contains(out, "matches 2 times in dup.txt") || !strings.Contains(out, "replace_all") {
		t.Fatalf("ambiguous = %q, %v", out, isErr)
	}
	out, isErr = call(t, tl, "fs_edit", map[string]any{"path": "dup.txt", "old_string": "x = 1", "new_string": "x = 3", "replace_all": true}, "t1/c4/1")
	if isErr || !strings.Contains(out, "2 replacements") || read(t, filepath.Join(root, "dup.txt")) != "x = 3\ny = 2\nx = 3\n" {
		t.Fatalf("replace_all = %q", out)
	}
	if fi, _ := os.Stat(filepath.Join(root, "dup.txt")); fi.Mode().Perm() != 0o640 {
		t.Errorf("an edit changed the file's mode to %v", fi.Mode().Perm())
	}

	must(t, os.WriteFile(filepath.Join(root, "crlf.txt"), []byte("a\r\nb\r\n"), 0o644))
	if out, _ := call(t, tl, "fs_edit", map[string]any{"path": "crlf.txt", "old_string": "a\nb", "new_string": "c"}, "t1/c5/1"); !strings.Contains(out, "CRLF") {
		t.Errorf("CRLF hint = %q", out)
	}
	for _, args := range []map[string]any{
		{"path": "src/a.go", "old_string": "", "new_string": "x"},
		{"path": "src/a.go", "old_string": "a", "new_string": "a"},
		{"path": "missing.go", "old_string": "a", "new_string": "b"},
		{"path": "src", "old_string": "a", "new_string": "b"},
	} {
		if out, isErr := call(t, tl, "fs_edit", args, "t1/c6/1"); !isErr {
			t.Errorf("fs_edit %v = %q; want an error", args, out)
		}
	}
}

// The ledger applies an edit once per call: a resend under the same key,
// an operator's Retry under a new attempt, and a crash between the change
// and its applied line are all recognised, never applied again (an edit
// whose new text contains its old text would double).
func TestEditAppliedOnceAcrossReplays(t *testing.T) {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	f := filepath.Join(root, "notes.md")
	must(t, os.WriteFile(f, []byte("todo: foo\n"), 0o644))
	edit := map[string]any{"path": "notes.md", "old_string": "foo", "new_string": "foo bar"}
	if out, isErr := call(t, tl, "fs_edit", edit, "t1/c1/1"); isErr {
		t.Fatal(out)
	}
	for _, key := range []string{"t1/c1/1", "t1/c1/2"} {
		out, isErr := call(t, tl, "fs_edit", edit, key)
		if isErr || !strings.Contains(out, "already applied") || !strings.Contains(out, "+todo: foo bar") {
			t.Fatalf("replay %s = %q, %v", key, out, isErr)
		}
	}
	if got := read(t, f); got != "todo: foo bar\n" {
		t.Fatalf("applied more than once: %q", got)
	}

	// A crash after the rename, before the applied line: drop that line.
	if out, isErr := call(t, tl, "fs_edit", map[string]any{"path": "notes.md", "old_string": "bar", "new_string": "bar baz"}, "t1/c2/1"); isErr {
		t.Fatal(out)
	}
	dropLastLedgerLine(t, tl.Ledger.Path())
	tl2 := codingTools(t, root)
	out, isErr := call(t, tl2, "fs_edit", map[string]any{"path": "notes.md", "old_string": "bar", "new_string": "bar baz"}, "t1/c2/2")
	if isErr || !strings.Contains(out, "already applied") {
		t.Fatalf("replay after a crash past the change = %q, %v", out, isErr)
	}
	if got := read(t, f); got != "todo: foo bar baz\n" {
		t.Fatalf("file %q", got)
	}

	// A crash after the intent, before the change: it is made on replay.
	if out, isErr := call(t, tl2, "fs_edit", map[string]any{"path": "notes.md", "old_string": "todo", "new_string": "done"}, "t1/c3/1"); isErr {
		t.Fatal(out)
	}
	dropLastLedgerLine(t, tl2.Ledger.Path())
	must(t, os.WriteFile(f, []byte("todo: foo bar baz\n"), 0o644)) // the change never landed
	tl3 := codingTools(t, root)
	out, isErr = call(t, tl3, "fs_edit", map[string]any{"path": "notes.md", "old_string": "todo", "new_string": "done"}, "t1/c3/2")
	if isErr || strings.Contains(out, "already applied") || read(t, f) != "done: foo bar baz\n" {
		t.Fatalf("replay of an intent that did not land = %q, %v; file %q", out, isErr, read(t, f))
	}
	ents, err := ReadLedger(tl3.Ledger.Path())
	must(t, err)
	applied := map[string]int{}
	for _, e := range ents {
		if e.Status == "applied" {
			applied[callOf(e.Key)]++
		}
	}
	for id, n := range applied {
		if n != 1 {
			t.Errorf("%s applied %d times", id, n)
		}
	}
}

func dropLastLedgerLine(t *testing.T, p string) {
	t.Helper()
	lines := strings.SplitAfter(strings.TrimSuffix(read(t, p), "\n"), "\n")
	if !strings.Contains(lines[len(lines)-1], `"status":"applied"`) {
		t.Fatalf("last ledger line %q", lines[len(lines)-1])
	}
	must(t, os.WriteFile(p, []byte(strings.Join(lines[:len(lines)-1], "")), 0o644))
}

func TestDeleteOnceAndConfined(t *testing.T) {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	b := filepath.Join(root, "src", "deep", "b.txt")
	out, isErr := call(t, tl, "fs_delete", map[string]any{"path": "src/deep/b.txt"}, "t1/d1/1")
	if isErr || !strings.Contains(out, "deleted src/deep/b.txt") {
		t.Fatalf("delete = %q", out)
	}
	if _, err := os.Stat(b); !os.IsNotExist(err) {
		t.Fatal("file still there")
	}
	must(t, os.WriteFile(b, []byte("recreated\n"), 0o644))
	if out, isErr := call(t, tl, "fs_delete", map[string]any{"path": "src/deep/b.txt"}, "t1/d1/2"); isErr || !strings.Contains(out, "already applied") {
		t.Fatalf("replay = %q", out)
	}
	if read(t, b) != "recreated\n" {
		t.Fatal("a replayed delete deleted again")
	}
	must(t, os.MkdirAll(filepath.Join(root, ".git"), 0o755))
	must(t, os.WriteFile(filepath.Join(root, ".git", "HEAD"), []byte("x"), 0o644))
	for _, p := range []string{"src/leak.txt", "src", "missing.txt", ".git/HEAD", "../secret.txt", ".neu/state.db", "out/.ledger"} {
		if out, isErr := call(t, tl, "fs_delete", map[string]any{"path": p}, "t1/d2/1"); !isErr {
			t.Errorf("fs_delete %q = %q; want refused", p, out)
		}
	}
}

// With WriteAnywhere the whole root is writable, but never .neu/, .git/
// (at any depth), reserved files, or anything outside.
func TestWriteAnywhereStaysInTheRoot(t *testing.T) {
	root, secret := workspace(t)
	tl := codingTools(t, root)
	tl.Box.Reserve(filepath.Join(root, "store.db"))
	must(t, os.MkdirAll(filepath.Join(root, "vendor", "x", ".git"), 0o755))
	for _, p := range []string{"../x.txt", "/etc/x", secret, "src/leak.txt", "updir/secret.txt", ".git/config", "vendor/x/.git/config", ".neu/x", "out/.ledger", "store.db", "."} {
		if out, isErr := call(t, tl, "fs_write", map[string]any{"path": p, "content": "x"}, "t1/w0/1"); !isErr {
			t.Errorf("fs_write %q = %q; want refused", p, out)
		}
	}
	if read(t, secret) != "TOP SECRET Hello\n" {
		t.Fatal("secret changed")
	}
	out, isErr := call(t, tl, "fs_write", map[string]any{"path": "src/a.go", "content": "package a\n"}, "t1/w1/1")
	if isErr || !strings.Contains(out, "wrote 10 bytes to src/a.go") || !strings.Contains(out, "-func Hello()") {
		t.Fatalf("write = %q", out)
	}
	out, _ = call(t, tl, "fs_write", map[string]any{"path": "new/dir/c.txt", "content": "c\n"}, "t1/w2/1")
	if !strings.Contains(out, "created new/dir/c.txt") {
		t.Fatalf("create = %q", out)
	}

	// Without WriteAnywhere (-approve never) writes stay in -writable.
	tl.WriteAnywhere = false
	if out, isErr := call(t, tl, "fs_edit", map[string]any{"path": "src/a.go", "old_string": "a", "new_string": "b"}, "t1/w3/1"); !isErr || !strings.Contains(out, "only under out/") {
		t.Fatalf("edit outside -writable = %q", out)
	}
}

func TestDiffIsCompact(t *testing.T) {
	var a, b []string
	for i := 1; i <= 100; i++ {
		a = append(a, fmt.Sprintf("line %d", i))
		b = append(b, fmt.Sprintf("line %d", i))
	}
	b[9], b[79] = "changed 10", "changed 80"
	d := Diff("f", strings.Join(a, "\n")+"\n", strings.Join(b, "\n")+"\n", 0)
	if strings.Count(d, "@@") != 4 || !strings.Contains(d, "@@ -8,5 +8,5 @@\n line 8\n line 9\n-line 10\n+changed 10\n line 11\n") || !strings.Contains(d, "+changed 80") || strings.Count(d, "\n") > 16 {
		t.Fatalf("diff:\n%s", d)
	}
	if Diff("f", "x", "x", 0) != "" {
		t.Fatal("no change, a diff")
	}
	d = Diff("f", "", strings.Repeat("new line\n", 500), 10)
	if !strings.Contains(d, "@@ -0,0 +1,500 @@") || !strings.Contains(d, "more diff lines") {
		t.Fatalf("capped diff:\n%s", d)
	}
}

func TestReadOnlyCommand(t *testing.T) {
	yes := [][]string{
		{"ls", "-la"}, {"cat", "go.mod"}, {"head", "-n", "5", "a"}, {"tail", "x"}, {"wc", "-l", "x"},
		{"rg", "-n", "foo"}, {"grep", "-rn", "x", "."}, {"find", ".", "-name", "*.go"},
		{"go", "build", "./..."}, {"go", "vet", "./..."}, {"go", "test", "-run", "TestX", "-v", "./..."}, {"go", "list", "-m", "all"}, {"go", "env", "GOPATH"},
		{"git", "status"}, {"git", "log", "--oneline", "-5"}, {"git", "diff", "HEAD~1"}, {"git", "show", "HEAD:go.mod"},
		{"make", "-n"}, {"make", "--dry-run", "test"},
	}
	no := [][]string{
		{}, {"rm", "-rf", "x"}, {"find", ".", "-delete"}, {"find", ".", "-name", "x", "-exec", "rm", "{}", ";"}, {"find", ".", "-fprint", "out"},
		{"git", "push"}, {"git", "commit", "-m", "x"}, {"git", "checkout", "."}, {"git", "-c", "core.pager=sh", "log"}, {"git", "diff", "--output=/tmp/x"},
		{"git", "log", "--ext-diff"}, {"git", "grep", "-Ovim", "x"}, {"git", "reflog", "expire", "--all"},
		{"go", "build", "-o", "/tmp/x"}, {"go", "test", "-exec", "sh"}, {"go", "test", "-coverprofile=c.out"}, {"go", "test", "-c"}, {"go", "env", "-w", "GOFLAGS=x"},
		{"go", "run", "."}, {"go", "generate"}, {"go", "mod", "tidy"}, {"go", "get", "x"},
		{"make"}, {"make", "test"}, {"rg", "--pre", "sh", "x"}, {"tree", "-o", "f"},
		{"./script.sh"}, {"/bin/ls"}, {"sh", "-c", "ls"}, {"bash", "-c", "ls | wc"}, {"sed", "-i", "s/a/b/", "f"}, {"sort", "-o", "f"},
		{"curl", "http://x"}, {"ls\x00"},
	}
	for _, a := range yes {
		if !ReadOnlyCommand(a) {
			t.Errorf("%q: not read-only; want read-only", a)
		}
	}
	for _, a := range no {
		if ReadOnlyCommand(a) {
			t.Errorf("%q: read-only; want approval", a)
		}
	}
}

func TestAssess(t *testing.T) {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	cases := []struct {
		tool, args string
		effect     bus.Effect
		safe       bool
	}{
		{"fs_write", `{"path":"out/a.md","content":"x"}`, bus.SideEffect, true},
		{"fs_write", `{"path":"src/a.go","content":"x"}`, bus.SideEffect, false},
		{"fs_edit", `{"path":"src/a.go","old_string":"a","new_string":"b"}`, bus.SideEffect, false},
		{"fs_delete", `{"path":"src/a.go"}`, bus.SideEffect, false},
		{"fs_write", `{"path":"../x","content":"x"}`, bus.SideEffect, true}, // refused by the tool anyway
		{"fs_write", `not json`, bus.SideEffect, true},
		{"sh_run", `{"args":["go","test","./..."]}`, bus.SideEffect, true},
		{"sh_run", `{"args":["rm","-rf","src"]}`, bus.SideEffect, false},
		{"sh_run", `{"args":["git","push"]}`, bus.SideEffect, false},
		{"sh_run", `{"script":"ls | wc -l"}`, bus.SideEffect, false},
		{"sh_run", `{"script":"ls"}`, bus.SideEffect, false},
		{"fs_read", `{"path":"a"}`, bus.ReadOnly, true},
		{"go_test", `{"dir":"."}`, bus.Idempotent, true},
		{"mcp_deploy", `{"env":"prod"}`, bus.SideEffect, false},
		{"mcp_lookup", `{}`, bus.ReadOnly, true},
	}
	for _, c := range cases {
		as := tl.Assess(c.tool, []byte(c.args), c.effect)
		if as.Safe != c.safe {
			t.Errorf("%s %s: safe %v, want %v (%q)", c.tool, c.args, as.Safe, c.safe, as.What)
		}
	}
	if w := tl.Assess("sh_run", []byte(`{"args":["git","push","origin"],"dir":"src"}`), bus.SideEffect).What; w != "$ git push origin (in src)" {
		t.Errorf("what %q", w)
	}
	if p := tl.Preview("fs_edit", []byte(`{"path":"src/a.go","old_string":"\"hi\"","new_string":"\"yo\""}`)); !strings.Contains(p, `+func Hello() string { return "yo" }`) {
		t.Errorf("preview %q", p)
	}
	if read(t, filepath.Join(root, "src", "a.go")) != "package a\n\nfunc Hello() string { return \"hi\" }\n" {
		t.Error("a preview changed the file")
	}
}

func skipWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a unix shell")
	}
}

func TestShRunArgvAndScript(t *testing.T) {
	skipWindows(t)
	root, _ := workspace(t)
	tl := codingTools(t, root)
	out, isErr := call(t, tl, "sh_run", map[string]any{"args": []string{"ls", "deep"}, "dir": "src"}, "")
	if isErr || !strings.Contains(out, "$ ls deep (in src/)") || !strings.Contains(out, "b.txt") || !strings.Contains(out, "exit status 0") {
		t.Fatalf("argv = %q", out)
	}
	// argv mode has no shell: the pipe is an argument.
	out, _ = call(t, tl, "sh_run", map[string]any{"args": []string{"echo", "a|b", "$HOME"}}, "")
	if !strings.Contains(out, "a|b $HOME\n") {
		t.Fatalf("argv echo = %q", out)
	}
	out, isErr = call(t, tl, "sh_run", map[string]any{"script": "printf 'x\\ny\\n' | wc -l; exit 3"}, "")
	if isErr || !strings.Contains(out, "2") || !strings.Contains(out, "exit status 3") {
		t.Fatalf("script = %q", out)
	}
	tl.Sh.Script = false
	if out, isErr := call(t, tl, "sh_run", map[string]any{"script": "ls"}, ""); !isErr || !strings.Contains(out, "sh_shell") {
		t.Fatalf("script turned off = %q", out)
	}
	for _, args := range []map[string]any{{}, {"args": []string{"ls"}, "script": "ls"}, {"args": []string{"ls"}, "dir": "../"}, {"args": []string{"no-such-program-neu"}}} {
		if out, isErr := call(t, tl, "sh_run", args, ""); !isErr {
			t.Errorf("sh_run %v = %q; want an error", args, out)
		}
	}
}

func TestShRunTimeoutKillsTheGroup(t *testing.T) {
	skipWindows(t)
	root, _ := workspace(t)
	tl := codingTools(t, root)
	tl.Sh.MaxTimeout = time.Second
	start := time.Now()
	out, isErr := call(t, tl, "sh_run", map[string]any{"script": "sleep 30 & sleep 30; echo never", "timeout_sec": 60}, "")
	if isErr || !strings.Contains(out, "killed: timed out after 1s") || strings.Contains(out, "\nnever") {
		t.Fatalf("timeout = %q", out)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("took %s: the background child kept the call open", d)
	}
}

func TestShRunOutputIsCapped(t *testing.T) {
	skipWindows(t)
	root, _ := workspace(t)
	tl := codingTools(t, root)
	out, _ := call(t, tl, "sh_run", map[string]any{"args": []string{"seq", "1", "200000"}}, "")
	if len(out) > shHeadBytes+shTailBytes+500 || !strings.Contains(out, "bytes of output omitted") || !strings.HasPrefix(out, "$ seq 1 200000\n1\n2\n") || !strings.Contains(out, "\n200000\nexit status 0") {
		t.Fatalf("capped output: %d bytes, %q … %q", len(out), out[:80], out[len(out)-80:])
	}
}

func TestShRunNeverSeesTheKeys(t *testing.T) {
	skipWindows(t)
	root, _ := workspace(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-secret-1")
	t.Setenv("MY_GATEWAY_KEY", "sk-secret-2")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sk-secret-3")
	t.Setenv("NEU_TEST_VISIBLE", "visible")
	tl := codingTools(t, root)
	tl.Sh.DenyEnv = []string{"ANTHROPIC_API_KEY", "MY_GATEWAY_KEY", "AWS_*"}
	out, _ := call(t, tl, "sh_run", map[string]any{"args": []string{"env"}}, "")
	if strings.Contains(out, "sk-secret") || !strings.Contains(out, "NEU_TEST_VISIBLE=visible") {
		t.Fatalf("env: %s", out)
	}
	if env := ShEnv([]string{"A=1", "AWS_X=2", "B_API_KEY=3"}, []string{"AWS_*", "B_API_KEY"}); len(env) != 1 || env[0] != "A=1" {
		t.Fatalf("ShEnv %q", env)
	}
}

func TestReadOnlyGitGetsSafeOptions(t *testing.T) {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	_, argv, show, err := tl.shCommand([]byte(`{"args":["git","log","-1"]}`))
	if err != nil || argv[1] != "--no-pager" || show != "git log -1" {
		t.Fatalf("argv %q show %q err %v", argv, show, err)
	}
	_, argv, _, _ = tl.shCommand([]byte(`{"args":["git","push"]}`))
	if argv[1] != "push" {
		t.Fatalf("a non-read-only git command was rewritten: %q", argv)
	}
}
