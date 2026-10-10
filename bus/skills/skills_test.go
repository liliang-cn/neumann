package skills_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/skills"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func skillMD(name, desc, body string) string {
	return fmt.Sprintf("---\nname: %s\ndescription: %s\n---\n%s\n", name, desc, body)
}

func mustNew(t *testing.T, roots ...string) *skills.Device {
	t.Helper()
	d, err := skills.New("skills", roots...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func call(t *testing.T, d *skills.Device, tool string, args any) task.ToolResult {
	t.Helper()
	b, _ := json.Marshal(args)
	res, err := d.Call(ctx, bus.DeviceCall{Tool: tool, Args: b})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func names(d *skills.Device) []string {
	var out []string
	for _, s := range d.Skills() {
		out = append(out, s.Name)
	}
	return out
}

func problemFor(d *skills.Device, sub string) string {
	for _, p := range d.Problems() {
		if strings.Contains(p.Path, sub) {
			return p.Reason
		}
	}
	return ""
}

func TestScanValidatesAndReportsProblems(t *testing.T) {
	user, project := t.TempDir(), t.TempDir()
	write(t, filepath.Join(project, "pdf", "SKILL.md"), skillMD("PDF_Forms", "Project's PDF skill", "project body"))
	write(t, filepath.Join(user, "pdf", "SKILL.md"), skillMD("pdf-forms", "User's PDF skill", "user body"))
	write(t, filepath.Join(user, "aa-dup", "SKILL.md"), skillMD("data", "first", ""))
	write(t, filepath.Join(user, "zz-dup", "SKILL.md"), skillMD("data", "second", ""))
	write(t, filepath.Join(user, "noname", "SKILL.md"), "---\ndescription: d\n---\n")
	write(t, filepath.Join(user, "nodesc", "SKILL.md"), "---\nname: nodesc\n---\n")
	write(t, filepath.Join(user, "badname", "SKILL.md"), skillMD("bad.name!", "d", ""))
	write(t, filepath.Join(user, "nofront", "SKILL.md"), "# just markdown\n")
	write(t, filepath.Join(user, "listname", "SKILL.md"), "---\nname: [a]\ndescription: d\n---\n")
	write(t, filepath.Join(user, "quoted", "SKILL.md"), "---\nname: \"quoted\"\ndescription: \"Use when: a\n  form needs \\\"filling\\\"\"\nwhen_to_use: >\n  The user hands\n  over a PDF.\nallowed-tools: Read, Bash(git add:*)\nmetadata:\n  version: 2\n---\nbody\n")
	write(t, filepath.Join(user, "notes", "README.md"), "not a skill")               // no SKILL.md: ignored, not a problem
	write(t, filepath.Join(user, "loose.md"), "a file at the root")                  // not a directory: ignored
	write(t, filepath.Join(user, ".hidden", "SKILL.md"), skillMD("hidden", "d", "")) // hidden: ignored
	// One level deep: a SKILL.md inside a skill's subdirectory is a resource.
	write(t, filepath.Join(user, "data2", "SKILL.md"), skillMD("data2", "d", ""))
	write(t, filepath.Join(user, "data2", "templates", "SKILL.md"), skillMD("template", "d", ""))
	missing := filepath.Join(user, "does-not-exist")

	d := mustNew(t, project, user, missing)
	if got, want := names(d), []string{"data", "data2", "pdf-forms", "quoted"}; !slices.Equal(got, want) {
		t.Fatalf("skills %q, want %q", got, want)
	}
	for _, s := range d.Skills() {
		switch s.Name {
		case "pdf-forms":
			if s.Description != "Project's PDF skill" || s.Dir != filepath.Join(project, "pdf") {
				t.Errorf("the earlier root must win: %+v", s)
			}
		case "data":
			if s.Description != "first" {
				t.Errorf("the first in directory order must win: %+v", s)
			}
		case "quoted":
			if s.Description != `Use when: a form needs "filling"` || s.WhenToUse != "The user hands over a PDF." {
				t.Errorf("quoted %+v", s)
			}
			if !slices.Equal(s.AllowedTools, []string{"Read", "Bash(git add:*)"}) || s.Metadata["version"] != "2" {
				t.Errorf("quoted %+v", s)
			}
		}
	}
	for sub, want := range map[string]string{
		filepath.Join(user, "pdf"): "duplicate skill name",
		"zz-dup":                   "duplicate skill name",
		"noname":                   "no name",
		"nodesc":                   "no description",
		"badname":                  "outside [a-z0-9-]",
		"nofront":                  "does not open",
		"listname":                 "must be a string",
		"does-not-exist":           "cannot scan root",
	} {
		if got := problemFor(d, sub); !strings.Contains(got, want) {
			t.Errorf("problem for %s: %q, want it to mention %q", sub, got, want)
		}
	}
	if n := len(d.Problems()); n != 8 {
		t.Errorf("%d problems: %v", n, d.Problems())
	}
}

func TestCatalogueIsSortedAndByteStable(t *testing.T) {
	root := t.TempDir()
	// Created out of order; descriptions with line breaks fold onto one line.
	write(t, filepath.Join(root, "z", "SKILL.md"), skillMD("zeta", "last", ""))
	write(t, filepath.Join(root, "a", "SKILL.md"), "---\nname: alpha\ndescription: |\n  first\n  of all\nwhen_to_use: always\n---\n")
	write(t, filepath.Join(root, "m", "SKILL.md"), skillMD("mu", "middle", ""))

	d := mustNew(t, root)
	want := "Available skills. Before doing a task one of them covers, call skills_load with its name and follow the instructions it returns.\n" +
		"\n- alpha: first of all\n  When to use: always" +
		"\n- mu: middle" +
		"\n- zeta: last"
	if got := d.Catalogue(); got != want {
		t.Fatalf("catalogue:\n%s\nwant:\n%s", got, want)
	}
	if res := call(t, d, "list", nil); res.IsError || res.Content != want {
		t.Fatalf("list %+v", res)
	}
	// Byte-stable across scans and across devices.
	d.Rescan()
	if d.Catalogue() != want || mustNew(t, root).Catalogue() != want {
		t.Fatal("catalogue changed with nothing on disk changing")
	}
	// An empty device lists nothing, and says so.
	empty := mustNew(t, t.TempDir())
	if empty.Catalogue() != "" {
		t.Fatalf("empty catalogue %q", empty.Catalogue())
	}
	if res := call(t, empty, "list", map[string]any{}); res.IsError || res.Content != "No skills are installed." {
		t.Fatalf("empty list %+v", res)
	}
}

func TestLoadReturnsBodyAndResources(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pdf")
	write(t, filepath.Join(dir, "SKILL.md"), "---\nname: pdf\ndescription: d\nallowed-tools: [Read]\n---\n\n# PDF\n\nRun scripts/fill.py.\n")
	write(t, filepath.Join(dir, "scripts", "fill.py"), "print('x')\n")
	write(t, filepath.Join(dir, "references", "fields.md"), "12345")
	write(t, filepath.Join(dir, ".git", "config"), "hidden")
	write(t, filepath.Join(dir, ".DS_Store"), "hidden")
	d := mustNew(t, root)

	res := call(t, d, "load", map[string]string{"name": "PDF"}) // looked up normalised
	if res.IsError {
		t.Fatal(res.Content)
	}
	for _, want := range []string{
		"Skill: pdf\nDirectory: " + dir + "\n",
		"Allowed tools (as the skill declares them): Read\n",
		"\n# PDF\n\nRun scripts/fill.py.\n",
		"- references/fields.md (5 bytes)\n- scripts/fill.py (11 bytes)",
	} {
		if !strings.Contains(res.Content, want) {
			t.Errorf("load lacks %q:\n%s", want, res.Content)
		}
	}
	for _, bad := range []string{"description: d", "name: pdf\n", ".git", ".DS_Store", "SKILL.md ("} {
		if strings.Contains(res.Content, bad) {
			t.Errorf("load contains %q:\n%s", bad, res.Content)
		}
	}

	// load reads the disk at call time: an edit shows without a Rescan.
	write(t, filepath.Join(dir, "SKILL.md"), "---\nname: pdf\ndescription: d\n---\nedited\n")
	if res := call(t, d, "load", map[string]string{"name": "pdf"}); !strings.Contains(res.Content, "\nedited\n") {
		t.Errorf("edit not picked up:\n%s", res.Content)
	}

	for _, a := range []map[string]string{{"name": "nope"}, {}} {
		if res := call(t, d, "load", a); !res.IsError {
			t.Errorf("load %v: %+v", a, res)
		}
	}
	if res, _ := d.Call(ctx, bus.DeviceCall{Tool: "load", Args: json.RawMessage(`{"name":`)}); !res.IsError {
		t.Errorf("bad JSON: %+v", res)
	}
	if _, err := d.Call(ctx, bus.DeviceCall{Tool: "exec"}); err == nil {
		t.Error("unknown tool accepted")
	}
}

// jail builds a skill next to a secret outside it, with symlinks both ways.
func jail(t *testing.T) (d *skills.Device, root, secret string) {
	t.Helper()
	root = t.TempDir() // on macOS under /var/folders, itself a symlink to /private/var
	outside := t.TempDir()
	secret = filepath.Join(outside, "secret.txt")
	write(t, secret, "the secret")
	dir := filepath.Join(root, "s")
	write(t, filepath.Join(dir, "SKILL.md"), skillMD("s", "d", "body"))
	write(t, filepath.Join(dir, "refs", "a.md"), "inside text")
	write(t, filepath.Join(root, "other", "SKILL.md"), skillMD("other", "d", "other body"))
	must(t, os.Symlink(secret, filepath.Join(dir, "escape.txt")))
	must(t, os.Symlink(outside, filepath.Join(dir, "escape-dir")))
	must(t, os.Symlink(filepath.Join(dir, "refs", "a.md"), filepath.Join(dir, "alias.md"))) // absolute, inside
	must(t, os.Symlink(filepath.Join("refs", "a.md"), filepath.Join(dir, "rel-alias.md")))  // relative, inside
	return mustNew(t, root), root, secret
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadIsJailedToTheSkill(t *testing.T) {
	d, _, secret := jail(t)
	for _, p := range []string{
		"../other/SKILL.md",
		"refs/../../other/SKILL.md",
		"..",
		secret,
		"/etc/passwd",
		"escape.txt",
		"escape-dir/secret.txt",
		"refs",
		"missing.md",
		"",
	} {
		res := call(t, d, "read", map[string]string{"name": "s", "path": p})
		if !res.IsError || strings.Contains(res.Content, "the secret") || strings.Contains(res.Content, "other body") {
			t.Errorf("read %q: %+v", p, res)
		}
	}
	for _, p := range []string{"refs/a.md", "alias.md", "rel-alias.md", "./refs/a.md", "refs//a.md"} {
		if res := call(t, d, "read", map[string]string{"name": "s", "path": p}); res.IsError || res.Content != "inside text" {
			t.Errorf("read %q: %+v", p, res)
		}
	}
	// load lists the in-skill symlinks and not the escaping ones.
	res := call(t, d, "load", map[string]string{"name": "s"})
	for _, want := range []string{"- alias.md (11 bytes)", "- refs/a.md (11 bytes)", "- rel-alias.md (11 bytes)"} {
		if !strings.Contains(res.Content, want) {
			t.Errorf("load lacks %q:\n%s", want, res.Content)
		}
	}
	if strings.Contains(res.Content, "escape") {
		t.Errorf("load lists an escaping link:\n%s", res.Content)
	}
	if res := call(t, d, "read", map[string]string{"name": "nope", "path": "refs/a.md"}); !res.IsError {
		t.Errorf("unknown skill: %+v", res)
	}
}

// A root reached through a symlink (as /tmp is /private/tmp on macOS) jails
// the same way.
func TestReadThroughASymlinkedRoot(t *testing.T) {
	_, root, _ := jail(t)
	link := filepath.Join(t.TempDir(), "link")
	must(t, os.Symlink(root, link))
	d := mustNew(t, link)
	if res := call(t, d, "read", map[string]string{"name": "s", "path": "refs/a.md"}); res.IsError || res.Content != "inside text" {
		t.Fatalf("%+v", res)
	}
	if res := call(t, d, "read", map[string]string{"name": "s", "path": "escape.txt"}); !res.IsError {
		t.Fatalf("%+v", res)
	}
}

func TestReadCapsSize(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "s")
	write(t, filepath.Join(dir, "SKILL.md"), skillMD("s", "d", ""))
	write(t, filepath.Join(dir, "big.txt"), strings.Repeat("x", 300))
	// 99 ASCII bytes then a 2-byte character straddling the cap at 100.
	write(t, filepath.Join(dir, "utf8.txt"), strings.Repeat("y", 99)+"é"+strings.Repeat("z", 50))
	write(t, filepath.Join(dir, "default.txt"), strings.Repeat("w", skills.DefaultReadLimit+10))
	d := mustNew(t, root)

	if res := call(t, d, "read", map[string]string{"name": "s", "path": "default.txt"}); res.IsError ||
		!strings.HasPrefix(res.Content, strings.Repeat("w", skills.DefaultReadLimit)+"\n\n[truncated") {
		t.Fatalf("default cap: %d bytes, error %v", len(res.Content), res.IsError)
	}
	d.SetReadLimit(100)
	res := call(t, d, "read", map[string]string{"name": "s", "path": "big.txt"})
	if res.IsError || res.Content != strings.Repeat("x", 100)+"\n\n[truncated: showing the first 100 of 300 bytes]" {
		t.Fatalf("%+v", res)
	}
	res = call(t, d, "read", map[string]string{"name": "s", "path": "utf8.txt"})
	if res.IsError || res.Content != strings.Repeat("y", 99)+"\n\n[truncated: showing the first 99 of 151 bytes]" {
		t.Fatalf("%+v", res)
	}
}

func TestReadRefusesBinary(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "s")
	write(t, filepath.Join(dir, "SKILL.md"), skillMD("s", "d", ""))
	write(t, filepath.Join(dir, "logo.png"), "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
	write(t, filepath.Join(dir, "latin1.txt"), "caf\xe9 au lait")
	write(t, filepath.Join(dir, "short-bad.txt"), "\xff\xfe")
	d := mustNew(t, root)
	for _, p := range []string{"logo.png", "latin1.txt", "short-bad.txt"} {
		res := call(t, d, "read", map[string]string{"name": "s", "path": p})
		if !res.IsError || !strings.Contains(res.Content, "binary file") {
			t.Errorf("read %s: %+v", p, res)
		}
	}
}

func TestToolsAreReadOnly(t *testing.T) {
	d := mustNew(t, t.TempDir())
	defs, err := d.Tools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, def := range defs {
		got = append(got, def.Name)
		if def.Effect != bus.ReadOnly {
			t.Errorf("%s: effect %v", def.Name, def.Effect)
		}
		if def.Cacheable != (def.Name == "list") {
			t.Errorf("%s: cacheable %v, want it only on list (load and read go to disk every time)", def.Name, def.Cacheable)
		}
		if !json.Valid(def.Schema) {
			t.Errorf("%s: schema %s", def.Name, def.Schema)
		}
		if def.Name != "list" && !strings.Contains(def.Description, "executes nothing") {
			t.Errorf("%s does not say it executes nothing: %s", def.Name, def.Description)
		}
	}
	if !slices.Equal(got, []string{"list", "load", "read"}) {
		t.Fatalf("tools %q", got)
	}
	if _, err := skills.New(""); err == nil {
		t.Error("empty device name accepted")
	}
}

func TestThroughTheBus(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "pdf", "SKILL.md"), skillMD("pdf", "Fill PDF forms", "Use the form."))
	write(t, filepath.Join(root, "pdf", "ref.md"), "field list")
	d := mustNew(t, root)
	b, err := bus.New(ctx, 2, d)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var specs []string
	for _, s := range b.Specs([]string{"skills_"}, nil) {
		specs = append(specs, s.Name)
	}
	if !slices.Equal(specs, []string{"skills_list", "skills_load", "skills_read"}) {
		t.Fatalf("specs %q", specs)
	}
	for _, n := range specs {
		if e := b.Effect(n); e != bus.ReadOnly {
			t.Errorf("%s effect %v", n, e)
		}
	}

	res, how := b.Run(ctx, "skills_list", nil, "k1", nil, nil, nil)
	if how != bus.Returned || res.IsError || !strings.Contains(res.Content, "- pdf: Fill PDF forms") {
		t.Fatalf("list %+v %v", res, how)
	}
	res, how = b.Run(ctx, "skills_load", json.RawMessage(`{"name":"pdf"}`), "k2", nil, nil, nil)
	if how != bus.Returned || res.IsError || !strings.Contains(res.Content, "Use the form.") {
		t.Fatalf("load %+v %v", res, how)
	}
	res, _ = b.Run(ctx, "skills_read", json.RawMessage(`{"name":"pdf","path":"ref.md"}`), "k3", nil, nil, nil)
	if res.IsError || res.Content != "field list" {
		t.Fatalf("read %+v", res)
	}

	// A page table that denies read hides it and refuses it.
	deny := []string{"skills_read"}
	if specs := b.Specs(nil, deny); slices.ContainsFunc(specs, func(s task.ToolSpec) bool { return s.Name == "skills_read" }) {
		t.Fatal("denied tool listed")
	}
	res, how = b.Run(ctx, "skills_read", json.RawMessage(`{"name":"pdf","path":"ref.md"}`), "k4", nil, deny, nil)
	if how != bus.Returned || !res.IsError || !strings.Contains(res.Content, "not permitted") {
		t.Fatalf("denied read %+v %v", res, how)
	}
	// An allow list that names only list refuses load.
	res, _ = b.Run(ctx, "skills_load", json.RawMessage(`{"name":"pdf"}`), "k5", []string{"skills_list"}, nil, nil)
	if !res.IsError || !strings.Contains(res.Content, "not permitted") {
		t.Fatalf("load outside allow %+v", res)
	}
	// The tool list the bus snapshots is byte-identical on every request.
	if a, b2 := mustJSON(t, b.Specs(nil, nil)), mustJSON(t, b.Specs(nil, nil)); !bytes.Equal(a, b2) {
		t.Fatal("specs not byte-stable")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Rescan picks up added and removed skills; the bus need not be rebuilt,
// because the catalogue is a tool result, not a tool definition.
func TestRescanThroughAnAttachedBus(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "a", "SKILL.md"), skillMD("a", "first", ""))
	d := mustNew(t, root)
	b, err := bus.New(ctx, 2, d)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "b", "SKILL.md"), skillMD("b", "second", ""))
	if res := b.Call(ctx, "skills_list", nil, "", nil, nil); strings.Contains(res.Content, "- b:") {
		t.Fatal("new skill listed before Rescan")
	}
	d.Rescan()
	if res := b.Call(ctx, "skills_list", nil, "", nil, nil); !strings.Contains(res.Content, "- b: second") {
		t.Fatalf("after Rescan: %s", res.Content)
	}
	must(t, os.RemoveAll(filepath.Join(root, "a")))
	d.Rescan()
	if got := names(d); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("after removal %q", got)
	}
}

// Calls and Rescan run concurrently (go test -race).
func TestConcurrentCallsAndRescan(t *testing.T) {
	d, _, _ := jail(t)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if i%4 == 0 {
					d.Rescan()
					continue
				}
				_ = d.Catalogue()
				for _, tool := range []string{"list", "load", "read"} {
					res, err := d.Call(ctx, bus.DeviceCall{Tool: tool, Args: json.RawMessage(`{"name":"s","path":"refs/a.md"}`)})
					if err != nil || res.IsError {
						t.Errorf("%s: %+v %v", tool, res, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}
