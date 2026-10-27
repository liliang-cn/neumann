package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/liliang-cn/neumann/internal/neu/app"
)

// BusFile is the config file's part for the tool sources besides the
// workspace tools: MCP servers and skills. File embeds it.
type BusFile struct {
	MCP []app.MCPServer `toml:"mcp"`
	// SkillsDir is one directory or a list; it replaces the default ones.
	SkillsDir dirList `toml:"skills_dir"`
}

// dirList is a TOML string or array of strings.
type dirList []string

func (d *dirList) UnmarshalTOML(v any) error {
	switch v := v.(type) {
	case string:
		*d = dirList{v}
	case []any:
		for _, e := range v {
			s, ok := e.(string)
			if !ok {
				return fmt.Errorf("skills_dir: want a string or a list of strings, got %T", e)
			}
			*d = append(*d, s)
		}
	default:
		return fmt.Errorf("skills_dir: want a string or a list of strings, got %T", v)
	}
	return nil
}

// check reports a bad [[mcp]] entry or two entries with one name.
func (b *BusFile) check(path string) error {
	seen := map[string]bool{}
	for _, s := range b.MCP {
		if err := s.Check(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if seen[s.Name] {
			return fmt.Errorf("%s: two [[mcp]] entries are named %q", path, s.Name)
		}
		seen[s.Name] = true
	}
	return nil
}

// busFlags are the command line's part of it.
type busFlags struct {
	noMCP  bool
	skills string
}

func (b *busFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&b.noMCP, "no-mcp", false, "start none of the config file's [[mcp]] servers")
	fs.StringVar(&b.skills, "skills", "", "comma-separated skill directories (each holds <skill>/SKILL.md), relative to -root; default <root>/.neu/skills and "+userSkillsDir()+", those that exist")
}

// userSkillsDir is $XDG_CONFIG_HOME/neu/skills or ~/.config/neu/skills.
func userSkillsDir() string { return filepath.Join(filepath.Dir(DefaultConfigPath()), "skills") }

// extras are the MCP servers, skill directories and messages for
// app.Build. The flags win over the file; f may be nil.
func (b *busFlags) extras(f *File, root string) app.Extras {
	ex := app.Extras{Messages: true}
	if f != nil && !b.noMCP {
		ex.MCP = f.MCP
	}
	var dirs []string
	switch {
	case b.skills != "":
		dirs = strings.Split(b.skills, ",")
	case f != nil && len(f.SkillsDir) > 0:
		dirs = f.SkillsDir
	default:
		for _, d := range []string{filepath.Join(root, ".neu", "skills"), userSkillsDir()} {
			if st, err := os.Stat(d); err == nil && st.IsDir() {
				ex.SkillDirs = append(ex.SkillDirs, d)
			}
		}
		return ex
	}
	for _, d := range dirs {
		if d = strings.TrimSpace(d); d != "" {
			ex.SkillDirs = append(ex.SkillDirs, skillPath(d, root))
		}
	}
	return ex
}

// skillPath expands a leading ~ and makes a relative path relative to the
// workspace root.
func skillPath(d, root string) string {
	if d == "~" || strings.HasPrefix(d, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			d = filepath.Join(home, d[1:])
		}
	}
	if !filepath.IsAbs(d) {
		d = filepath.Join(root, d)
	}
	return d
}

// toolsText is /tools: every tool with its device and effect.
func toolsText(a *app.App) string {
	var b strings.Builder
	tl := a.ToolList()
	w := 4
	for _, t := range tl {
		w = max(w, len(t.Name))
	}
	fmt.Fprintf(&b, "%d tools", len(tl))
	if s := a.MCPServers(); len(s) > 0 {
		fmt.Fprintf(&b, " (MCP: %s)", strings.Join(s, ", "))
	}
	for _, t := range tl {
		dev := t.Device
		if t.Source == "mcp" {
			dev += " (" + t.Source + ")"
		}
		fmt.Fprintf(&b, "\n  %-*s  %-11s  %s", w, t.Name, t.Effect, dev)
	}
	return b.String()
}

// skillsText is /skills: the skills found, and those skipped and why.
func skillsText(a *app.App) string {
	d := a.Skills()
	if d == nil {
		return "no skill directories (-skills, skills_dir, or " + filepath.Join("<root>", ".neu", "skills") + " / " + userSkillsDir() + ")"
	}
	var b strings.Builder
	sk := d.Skills()
	fmt.Fprintf(&b, "%d skill%s", len(sk), plural(len(sk)))
	for _, s := range sk {
		fmt.Fprintf(&b, "\n  %-20s %s\n  %-20s %s", s.Name, clip(s.Description, 100), "", s.Dir)
	}
	for _, p := range d.Problems() {
		fmt.Fprintf(&b, "\n  skipped %s", p)
	}
	return b.String()
}

// busLine is the startup line about the extra tool sources, or "".
func busLine(a *app.App) string {
	var parts []string
	if s := a.MCPServers(); len(s) > 0 {
		parts = append(parts, "MCP "+strings.Join(s, ", "))
	}
	if d := a.Skills(); d != nil {
		p := fmt.Sprintf("%d skill%s", len(d.Skills()), plural(len(d.Skills())))
		if n := len(d.Problems()); n > 0 {
			p += fmt.Sprintf(" (%d skipped; /skills says why)", n)
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}
