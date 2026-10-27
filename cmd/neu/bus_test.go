package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

func TestConfigFileMCPAndSkills(t *testing.T) {
	f, err := LoadFile(writeFile(t, `
backend = "anthropic"
skills_dir = ["~/team-skills", "skills"]

[[mcp]]
name = "time"
command = "uvx"
args = ["mcp-server-time", "--local-timezone", "Europe/Vienna"]
env = { TZ = "Europe/Vienna", GITHUB_TOKEN = "${GITHUB_TOKEN}" }
effect = "read_only"
side_effect = ["set_alarm"]
startup_timeout = "1m"

[[mcp]]
name = "remote"
url = "https://mcp.example/mcp"
headers = { Authorization = "Bearer ${REMOTE_TOKEN}" }
enabled = false
read_only = ["search"]
idempotent = ["put"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.MCP) != 2 {
		t.Fatalf("mcp %+v", f.MCP)
	}
	tm, rm := f.MCP[0], f.MCP[1]
	if tm.Name != "time" || tm.Command != "uvx" || strings.Join(tm.Args, " ") != "mcp-server-time --local-timezone Europe/Vienna" ||
		tm.Env["TZ"] != "Europe/Vienna" || tm.Env["GITHUB_TOKEN"] != "${GITHUB_TOKEN}" || tm.Effect != "read_only" ||
		strings.Join(tm.SideEffect, ",") != "set_alarm" || tm.StartupTimeout != "1m" || !tm.On() {
		t.Errorf("time %+v", tm)
	}
	if rm.URL != "https://mcp.example/mcp" || rm.Headers["Authorization"] != "Bearer ${REMOTE_TOKEN}" || rm.On() ||
		strings.Join(rm.ReadOnly, ",") != "search" || strings.Join(rm.Idempotent, ",") != "put" {
		t.Errorf("remote %+v", rm)
	}
	if strings.Join(f.SkillsDir, ",") != "~/team-skills,skills" {
		t.Errorf("skills_dir %v", f.SkillsDir)
	}
	if f, err = LoadFile(writeFile(t, "skills_dir = \"/abs/skills\"\n")); err != nil || strings.Join(f.SkillsDir, ",") != "/abs/skills" {
		t.Errorf("one skills_dir: %v %v", f, err)
	}

	for body, want := range map[string]string{
		"[[mcp]]\nname = \"a\"\n": "needs command",
		"[[mcp]]\nname = \"a\"\ncommand = \"x\"\n[[mcp]]\nname = \"a\"\ncommand = \"y\"\n": "two [[mcp]] entries",
		"[[mcp]]\nname = \"a\"\ncommand = \"x\"\neffect = \"safe\"\n":                      "effect",
		"[[mcp]]\nname = \"a\"\ncommand = \"x\"\nreadonly = [\"t\"]\n":                     "unknown key(s) mcp.readonly",
		"skills_dir = 3\n": "skills_dir",
	} {
		if _, err := LoadFile(writeFile(t, body)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", body, err, want)
		}
	}
}

// The skill directories: -skills, else skills_dir, else those of
// <root>/.neu/skills and the user's that exist, in that order; -no-mcp
// drops the file's servers.
func TestExtrasFromFlagsAndFile(t *testing.T) {
	root, cfgHome := t.TempDir(), t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfgHome)
	parse := func(args ...string) busFlags {
		var b busFlags
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		b.register(fs)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := parse()
	if ex := b.extras(nil, root); len(ex.SkillDirs) != 0 || !ex.Messages {
		t.Fatalf("no dirs exist: %+v", ex)
	}
	project, user := filepath.Join(root, ".neu", "skills"), filepath.Join(cfgHome, "neu", "skills")
	for _, d := range []string{project, user} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(b.extras(nil, root).SkillDirs, ","); got != project+","+user {
		t.Fatalf("defaults: %s", got)
	}
	file := &File{BusFile: BusFile{MCP: []app.MCPServer{{Name: "x", Command: "x"}}, SkillsDir: dirList{"team", "/abs"}}}
	ex := b.extras(file, root)
	if got := strings.Join(ex.SkillDirs, ","); got != filepath.Join(root, "team")+",/abs" || len(ex.MCP) != 1 {
		t.Fatalf("file: %+v", ex)
	}
	b = parse("-skills", "/a, b", "-no-mcp")
	ex = b.extras(file, root)
	if got := strings.Join(ex.SkillDirs, ","); got != "/a,"+filepath.Join(root, "b") || len(ex.MCP) != 0 {
		t.Fatalf("flags: %+v", ex)
	}
}

// A message from another task shows in the progress lines with its sender.
func TestProgressShowsTheSender(t *testing.T) {
	for _, tc := range []struct {
		ev   proto.Event
		want string
	}{
		{proto.NewEvent(proto.EvSteerDelivered, "t1", map[string]any{"seq": 3, "from": "task:t2"}), "[t1] message from task:t2 delivered"},
		{proto.NewEvent(proto.EvSteerDelivered, "t1", map[string]any{"seq": 3}), "[t1] steer delivered"},
		{proto.NewEvent(proto.EvSteerDropped, "t1", map[string]any{"seq": 3, "from": "task:t2", "text": "x"}), "[t1] message from task:t2 dropped: the task ended first"},
		{proto.NewEvent(proto.EvSteerDropped, "t1", map[string]any{"seq": 3, "text": "x"}), ""},
	} {
		if got := progress(tc.ev); got != tc.want {
			t.Errorf("%s %s: %q, want %q", tc.ev.Kind, tc.ev.Data, got, tc.want)
		}
	}
}

// /tools lists every tool with its effect and device; /skills the skills.
func TestToolsAndSkillsText(t *testing.T) {
	root := t.TempDir()
	sk := filepath.Join(root, ".neu", "skills", "hello")
	if err := os.MkdirAll(sk, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(sk, "SKILL.md"), []byte("---\nname: hello\ndescription: Say hello.\n---\nSay hello.\n"), 0o644)
	cfg := app.Config{Backend: "cloud", Model: "m", Cores: 2, Root: root, Writable: "out", MaxSteps: 5, Quiet: true, Store: filepath.Join(root, ".neu", "state.db")}
	var b busFlags
	ctx := context.Background()
	a, err := app.Build(ctx, cfg, app.Options{Adapter: &sim.Model{}, Extras: b.extras(nil, root)})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(5 * time.Second)
	tools := toolsText(a)
	for _, want := range []string{"fs_read      read_only    fs", "fs_write     side_effect  fs", "tasks_send   idempotent   tasks", "skills_load  read_only    skills"} {
		if !strings.Contains(tools, want) {
			t.Errorf("/tools lacks %q:\n%s", want, tools)
		}
	}
	if s := skillsText(a); !strings.HasPrefix(s, "1 skill\n  hello") || !strings.Contains(s, "Say hello.") {
		t.Errorf("/skills:\n%s", s)
	}
	if l := busLine(a); l != "1 skill" {
		t.Errorf("startup line %q", l)
	}
}
