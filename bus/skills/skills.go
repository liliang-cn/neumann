// Package skills is a device that serves skills by progressive disclosure.
//
// A skill is a directory holding a SKILL.md — YAML frontmatter (name,
// description, and optionally when_to_use, allowed-tools, metadata) followed
// by markdown instructions — plus any resource files it refers to (scripts,
// references, templates). The model first sees only a catalogue of names and
// descriptions; it loads a skill's instructions when a task calls for it, and
// reads a resource file only when the instructions point to one. That keeps
// every skill's full text out of the context until it is needed.
//
// To start, point a device at one or more roots and attach it:
//
//	dev, err := skills.New("skills", "./skills", os.ExpandEnv("$HOME/.skills"))
//	m, err := neumann.New(neumann.Config{Model: adapter, Devices: []bus.Device{dev}})
//
// [Device.Problems] lists skills that were skipped and why; examples/skills
// is a complete program.
//
// Three tools, all ReadOnly:
//
//   - list: the catalogue. Byte-stable for a given set of skills, so it is
//     Cacheable: within one task a repeated list is served from L2 (a Rescan
//     shows up in the next task, or after a write in this one). Catalogue
//     returns the same text for a host that would rather put it in the system
//     prompt.
//   - load {name}: the SKILL.md body without its frontmatter, the skill's
//     directory, and its resource files with their sizes.
//   - read {name, path}: one resource file as text, confined to that skill's
//     directory, capped in size, and refused if it is binary.
//
// Nothing here executes anything. A skill's scripts are run, if at all, by a
// device that executes things; load reports the skill's directory so the
// model can name the script to that device.
//
// Discovery. Each root is scanned one level deep: <root>/<dir>/SKILL.md is a
// skill. Deeper directories are not scanned, because they belong to the skill
// above them: a SKILL.md under a skill's templates/ or examples/ directory is
// one of its resources, not a second skill. A host with skills in several
// places passes several roots. Earlier roots win a name: a duplicate in a later
// root (or later in the same root, in directory order) is skipped and
// reported, so a host can let project skills shadow user skills by passing the
// project root first. Hidden entries (".git", ".DS_Store") are ignored, and a
// directory without a SKILL.md is not a skill and is not reported.
//
// Freshness. The catalogue is what the last scan saw; Rescan re-reads the
// roots. The bus snapshots tool definitions when the device is attached, but
// those definitions never contain the catalogue, so a Rescan is visible to the
// next list call without re-attaching. load and read always read the disk at
// call time, so an edited SKILL.md or resource is picked up without a Rescan;
// only a skill added, removed or renamed, or a changed description, needs one.
// A host that put Catalogue in its system prompt must refresh that itself.
package skills

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

const (
	// DefaultReadLimit is how much of one resource file read returns; the
	// rest is cut off with a note saying so.
	DefaultReadLimit = 256 << 10

	// MaxNameLen bounds a skill's name.
	MaxNameLen = 64

	maxSkillFile = 1 << 20 // a SKILL.md larger than this is refused
	maxResources = 500     // resource files listed by load
	sniffLen     = 8 << 10 // bytes inspected to decide that a file is binary
)

// Skill is one valid skill as the last scan found it.
type Skill struct {
	Name         string // normalised: lower case, [a-z0-9-]
	Description  string
	WhenToUse    string
	AllowedTools []string          // declared by the skill; this device does not enforce it
	Metadata     map[string]string // nested keys are joined with '.'
	Dir          string            // the skill's directory, as found under its root
	File         string            // Dir/SKILL.md
}

// Problem is a skill that was skipped, or a root that could not be scanned.
type Problem struct {
	Path   string
	Reason string
}

func (p Problem) String() string { return p.Path + ": " + p.Reason }

type Device struct {
	name  string
	roots []string

	mu        sync.RWMutex
	skills    map[string]Skill
	names     []string // sorted
	problems  []Problem
	catalogue string
	readLimit int64
}

// New scans roots for skills. Invalid skills and unreadable roots do not fail
// it; they are skipped and reported by Problems. The only error is an empty
// device name.
func New(name string, roots ...string) (*Device, error) {
	if name == "" {
		return nil, errors.New("skills: empty device name")
	}
	d := &Device{name: name, roots: append([]string(nil), roots...), readLimit: DefaultReadLimit}
	d.Rescan()
	return d, nil
}

// SetReadLimit sets how many bytes of a resource file read returns (n <= 0
// restores DefaultReadLimit).
func (d *Device) SetReadLimit(n int64) {
	if n <= 0 {
		n = DefaultReadLimit
	}
	d.mu.Lock()
	d.readLimit = n
	d.mu.Unlock()
}

// Rescan re-reads the roots and replaces the catalogue and the problems.
func (d *Device) Rescan() {
	skills := map[string]Skill{}
	var problems []Problem
	for _, root := range d.roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			problems = append(problems, Problem{root, "cannot scan root: " + err.Error()})
			continue
		}
		for _, e := range entries { // ReadDir sorts by name
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			dir := filepath.Join(root, e.Name())
			if st, err := os.Stat(dir); err != nil || !st.IsDir() {
				continue // a file, or a symlink to nothing
			}
			file := filepath.Join(dir, "SKILL.md")
			if _, err := os.Lstat(file); errors.Is(err, fs.ErrNotExist) {
				continue
			}
			s, _, err := parseSkill(file)
			if err != nil {
				problems = append(problems, Problem{file, err.Error()})
				continue
			}
			if prev, dup := skills[s.Name]; dup {
				problems = append(problems, Problem{file, fmt.Sprintf("duplicate skill name %q (already provided by %s)", s.Name, prev.File)})
				continue
			}
			s.Dir, s.File = dir, file
			skills[s.Name] = s
		}
	}
	names := make([]string, 0, len(skills))
	for n := range skills {
		names = append(names, n)
	}
	sort.Strings(names)
	cat := d.buildCatalogue(skills, names)

	d.mu.Lock()
	d.skills, d.names, d.problems, d.catalogue = skills, names, problems, cat
	d.mu.Unlock()
}

// Skills is the valid skills, sorted by name.
func (d *Device) Skills() []Skill {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]Skill, 0, len(d.names))
	for _, n := range d.names {
		s := d.skills[n]
		s.AllowedTools = append([]string(nil), s.AllowedTools...)
		if s.Metadata != nil {
			m := make(map[string]string, len(s.Metadata))
			for k, v := range s.Metadata {
				m[k] = v
			}
			s.Metadata = m
		}
		out = append(out, s)
	}
	return out
}

// Problems is what the last scan skipped, and why, in scan order.
func (d *Device) Problems() []Problem {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]Problem(nil), d.problems...)
}

// Catalogue is the list of skills as the list tool returns it: one entry per
// skill, sorted by name, with its description (and when_to_use) each folded
// onto one line. It is byte-identical for the same set of skills, so it can
// sit in a cached system prompt. It is "" when there are no skills.
func (d *Device) Catalogue() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.catalogue
}

func (d *Device) buildCatalogue(skills map[string]Skill, names []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Available skills. Before doing a task one of them covers, call %s with its name and follow the instructions it returns.\n", bus.FullName(d.name, "load"))
	for _, n := range names {
		s := skills[n]
		fmt.Fprintf(&b, "\n- %s: %s", s.Name, oneLine(s.Description))
		if s.WhenToUse != "" {
			fmt.Fprintf(&b, "\n  When to use: %s", oneLine(s.WhenToUse))
		}
	}
	return b.String()
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// normalizeName lower-cases a name and turns spaces and underscores into
// hyphens; validName then checks what is left.
func normalizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '_' {
			return '-'
		}
		return r
	}, s)
}

// validName: 1..MaxNameLen of [a-z0-9-], no leading, trailing or doubled
// hyphen.
func validName(s string) error {
	if s == "" {
		return errors.New("name is empty")
	}
	if len(s) > MaxNameLen {
		return fmt.Errorf("name %q is longer than %d characters", s, MaxNameLen)
	}
	for _, r := range s {
		if !(r == '-' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9') {
			return fmt.Errorf("name %q has a character outside [a-z0-9-]", s)
		}
	}
	if s[0] == '-' || s[len(s)-1] == '-' || strings.Contains(s, "--") {
		return fmt.Errorf("name %q has a leading, trailing or doubled hyphen", s)
	}
	return nil
}

// parseSkill reads and validates one SKILL.md; Dir and File are left to the
// caller.
func parseSkill(file string) (Skill, string, error) {
	f, err := os.Open(file)
	if err != nil {
		return Skill{}, "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxSkillFile+1))
	if err != nil {
		return Skill{}, "", err
	}
	if len(raw) > maxSkillFile {
		return Skill{}, "", fmt.Errorf("SKILL.md is larger than %d bytes", maxSkillFile)
	}
	if !utf8.Valid(raw) {
		return Skill{}, "", errors.New("SKILL.md is not valid UTF-8")
	}
	front, body, err := splitFrontmatter(string(raw))
	if err != nil {
		return Skill{}, "", err
	}
	fm, err := parseFrontmatter(front)
	if err != nil {
		return Skill{}, "", err
	}
	str := func(key string) (string, error) {
		v, ok := fm.m[key]
		if !ok {
			return "", nil
		}
		if v.kind != scalarKind {
			return "", fmt.Errorf("frontmatter %q must be a string", key)
		}
		return strings.TrimSpace(v.s), nil
	}
	var s Skill
	name, err := str("name")
	if err != nil {
		return Skill{}, "", err
	}
	if name == "" {
		return Skill{}, "", errors.New("frontmatter has no name")
	}
	s.Name = normalizeName(name)
	if err := validName(s.Name); err != nil {
		return Skill{}, "", err
	}
	if s.Description, err = str("description"); err != nil {
		return Skill{}, "", err
	}
	if s.Description == "" {
		return Skill{}, "", errors.New("frontmatter has no description")
	}
	if s.WhenToUse, err = str("when_to_use"); err != nil {
		return Skill{}, "", err
	}
	if v, ok := fm.m["allowed-tools"]; ok {
		switch v.kind {
		case listKind:
			for _, t := range v.list {
				if t = strings.TrimSpace(t); t != "" {
					s.AllowedTools = append(s.AllowedTools, t)
				}
			}
		case scalarKind:
			s.AllowedTools = splitTools(v.s)
		default:
			return Skill{}, "", errors.New("frontmatter \"allowed-tools\" must be a list or a string")
		}
	}
	if v, ok := fm.m["metadata"]; ok {
		switch {
		case v.kind == mapKind:
			s.Metadata = map[string]string{}
			flatten("", v, s.Metadata)
		case v.kind == scalarKind && v.s == "":
		default:
			return Skill{}, "", errors.New("frontmatter \"metadata\" must be a mapping")
		}
	}
	return s, body, nil
}

// splitTools splits an allowed-tools string on commas when it has any, and on
// white space otherwise ("Read, Grep" and "Read Grep" are both two tools;
// "Bash(git add:*), Read" keeps its space).
func splitTools(s string) []string {
	var parts []string
	if strings.Contains(s, ",") {
		parts = strings.Split(s, ",")
	} else {
		parts = strings.Fields(s)
	}
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func flatten(prefix string, v yvalue, out map[string]string) {
	switch v.kind {
	case scalarKind:
		out[prefix] = v.s
	case listKind:
		out[prefix] = strings.Join(v.list, ", ")
	case mapKind:
		for _, k := range v.keys {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			flatten(p, v.m[k], out)
		}
	}
}

// --- the device ---

func (d *Device) Name() string { return d.name }

func (d *Device) Tools(context.Context) ([]bus.ToolDef, error) {
	load, read := bus.FullName(d.name, "load"), bus.FullName(d.name, "read")
	return []bus.ToolDef{
		{
			Name: "list",
			Description: "List the installed skills: each one's name and what it is for. A skill is a set of instructions, " +
				"sometimes with reference files, for one kind of task; call " + load + " before doing such a task.",
			Schema:    json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Effect:    bus.ReadOnly,
			Cacheable: true,
		},
		{
			Name: "load",
			Description: "Load a skill: its instructions (SKILL.md without the frontmatter), its directory, and its resource files " +
				"with their sizes. Follow the instructions; read a resource with " + read + " when they point to one. " +
				"This tool executes nothing: a script a skill mentions must be run with a tool that executes things, if one is available.",
			Schema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","description":"The skill's name, as listed."}},"required":["name"],"additionalProperties":false}`),
			Effect: bus.ReadOnly,
		},
		{
			Name: "read",
			Description: "Read one resource file of a skill as text. The path is relative to the skill's directory, as " + load +
				" lists it; paths that leave the skill's directory are refused, binary files are refused, and large files are truncated. " +
				"This tool executes nothing, scripts included: it only returns their text.",
			Schema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","description":"The skill's name."},"path":{"type":"string","description":"A file path relative to the skill's directory, e.g. references/api.md."}},"required":["name","path"],"additionalProperties":false}`),
			Effect: bus.ReadOnly,
		},
	}, nil
}

func (d *Device) Close() error { return nil }

type args struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

func (d *Device) Call(ctx context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	var a args
	if len(c.Args) > 0 && string(c.Args) != "null" {
		if err := json.Unmarshal(c.Args, &a); err != nil {
			return errResult("invalid arguments: " + err.Error()), nil
		}
	}
	switch c.Tool {
	case "list":
		if cat := d.Catalogue(); cat != "" {
			return task.ToolResult{Content: cat}, nil
		}
		return task.ToolResult{Content: "No skills are installed."}, nil
	case "load":
		return d.load(a.Name), nil
	case "read":
		return d.read(a.Name, a.Path), nil
	}
	return task.ToolResult{}, fmt.Errorf("skills %s: no tool %q", d.name, c.Tool)
}

func errResult(msg string) task.ToolResult { return task.ToolResult{Content: msg, IsError: true} }

func (d *Device) lookup(name string) (Skill, int64, *task.ToolResult) {
	d.mu.RLock()
	s, ok := d.skills[normalizeName(name)]
	limit := d.readLimit
	d.mu.RUnlock()
	if strings.TrimSpace(name) == "" {
		r := errResult("name is required")
		return s, 0, &r
	}
	if !ok {
		r := errResult(fmt.Sprintf("no skill named %q; %s lists the installed skills", name, bus.FullName(d.name, "list")))
		return s, 0, &r
	}
	return s, limit, nil
}

func (d *Device) load(name string) task.ToolResult {
	s, _, bad := d.lookup(name)
	if bad != nil {
		return *bad
	}
	// The disk, not the scan: an edited SKILL.md is served as it is now.
	_, body, err := parseSkill(s.File)
	if err != nil {
		return errResult(fmt.Sprintf("skill %q cannot be loaded: %v", s.Name, err))
	}
	res, more, err := resources(s.Dir)
	if err != nil {
		return errResult(fmt.Sprintf("skill %q: cannot list its files: %v", s.Name, err))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Skill: %s\nDirectory: %s\n", s.Name, s.Dir)
	if len(s.AllowedTools) > 0 {
		fmt.Fprintf(&b, "Allowed tools (as the skill declares them): %s\n", strings.Join(s.AllowedTools, ", "))
	}
	b.WriteString("\n")
	b.WriteString(body)
	b.WriteString("\n\n")
	if len(res) == 0 {
		b.WriteString("Resources: none.")
	} else {
		fmt.Fprintf(&b, "Resources (read one with %s {\"name\": %q, \"path\": \"<path>\"}; nothing is executed):", bus.FullName(d.name, "read"), s.Name)
		for _, r := range res {
			fmt.Fprintf(&b, "\n- %s (%d bytes)", r.path, r.size)
		}
		if more > 0 {
			fmt.Fprintf(&b, "\n- ... and %d more not listed", more)
		}
	}
	return task.ToolResult{Content: b.String()}
}

type resource struct {
	path string // slash-separated, relative to the skill's directory
	size int64
}

// resources lists a skill's files: everything under its directory except
// SKILL.md itself and hidden entries. A symlink is listed when it resolves to
// a regular file inside the directory; symlinked directories are not walked.
func resources(dir string) ([]resource, int, error) {
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, 0, err
	}
	var out []resource
	more := 0
	err = filepath.WalkDir(real, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if p == real {
				return err
			}
			return nil // an unreadable subdirectory: list what can be read
		}
		rel, _ := filepath.Rel(real, p)
		if rel == "." {
			return nil
		}
		if strings.HasPrefix(e.Name(), ".") {
			if e.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if e.IsDir() || rel == "SKILL.md" {
			return nil
		}
		var size int64
		switch {
		case e.Type()&fs.ModeSymlink != 0:
			target, err := filepath.EvalSymlinks(p)
			if err != nil || !inside(real, target) {
				return nil
			}
			st, err := os.Stat(target)
			if err != nil || !st.Mode().IsRegular() {
				return nil
			}
			size = st.Size()
		case e.Type().IsRegular():
			st, err := e.Info()
			if err != nil {
				return nil
			}
			size = st.Size()
		default:
			return nil
		}
		if len(out) == maxResources {
			more++
			return nil
		}
		out = append(out, resource{filepath.ToSlash(rel), size})
		return nil
	})
	return out, more, err
}

// inside reports whether path is dir or under it; both must already be
// resolved through EvalSymlinks.
func inside(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

func (d *Device) read(name, path string) task.ToolResult {
	s, limit, bad := d.lookup(name)
	if bad != nil {
		return *bad
	}
	if path == "" {
		return errResult("path is required")
	}
	// Lexically first: no absolute paths, no "..", nothing that is not a
	// plain path below the skill's directory.
	rel := filepath.FromSlash(path)
	if filepath.IsAbs(rel) || strings.HasPrefix(path, "/") || strings.HasPrefix(path, `\`) || !filepath.IsLocal(rel) {
		return errResult(fmt.Sprintf("path %q is refused: give a path relative to the skill's directory, without '..'", path))
	}
	// Then through symlinks, on both sides: the skill's directory itself may
	// be reached through one (on macOS /tmp is /private/tmp), and a symlink
	// inside it may point anywhere.
	realDir, err := filepath.EvalSymlinks(s.Dir)
	if err != nil {
		return errResult(fmt.Sprintf("skill %q is no longer on disk: %v", s.Name, err))
	}
	target, err := filepath.EvalSymlinks(filepath.Join(realDir, rel))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errResult(fmt.Sprintf("skill %q has no file %q; %s lists its files", s.Name, path, bus.FullName(d.name, "load")))
		}
		return errResult(fmt.Sprintf("path %q: %v", path, err))
	}
	if !inside(realDir, target) {
		return errResult(fmt.Sprintf("path %q is refused: it resolves outside the skill's directory", path))
	}
	targetRel, _ := filepath.Rel(realDir, target)
	// Open through an os.Root, which refuses to leave the directory even if a
	// path component is swapped for a symlink after the check above.
	root, err := os.OpenRoot(realDir)
	if err != nil {
		return errResult(fmt.Sprintf("skill %q: %v", s.Name, err))
	}
	defer root.Close()
	f, err := root.Open(targetRel)
	if err != nil {
		return errResult(fmt.Sprintf("path %q cannot be opened: %v", path, err))
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return errResult(fmt.Sprintf("path %q: %v", path, err))
	}
	if st.IsDir() {
		return errResult(fmt.Sprintf("path %q is a directory; %s lists the skill's files", path, bus.FullName(d.name, "load")))
	}
	if !st.Mode().IsRegular() {
		return errResult(fmt.Sprintf("path %q is not a regular file", path))
	}
	data, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return errResult(fmt.Sprintf("path %q: %v", path, err))
	}
	truncated := int64(len(data)) < st.Size()
	if binary(data, truncated) {
		return errResult(fmt.Sprintf("path %q is a binary file (%d bytes); this tool returns text only", path, st.Size()))
	}
	if truncated {
		// Cut on a character boundary, and say what is missing.
		cut := completeRunes(data)
		return task.ToolResult{Content: fmt.Sprintf("%s\n\n[truncated: showing the first %d of %d bytes]", data[:cut], cut, st.Size())}
	}
	return task.ToolResult{Content: string(data)}
}

// binary: a NUL byte, or bytes that are not UTF-8, in the first sniffLen
// bytes. When the file goes on past what was read, a character cut off at the
// end of it is not held against the file.
func binary(data []byte, truncated bool) bool {
	head := data
	if len(head) > sniffLen {
		head, truncated = head[:sniffLen], true
	}
	if slices.Contains(head, 0) {
		return true
	}
	if truncated {
		head = head[:completeRunes(head)]
	}
	return !utf8.Valid(head)
}

// completeRunes is the length of b without an incomplete character at its
// end.
func completeRunes(b []byte) int {
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:]) {
				return i
			}
			break
		}
	}
	return len(b)
}
