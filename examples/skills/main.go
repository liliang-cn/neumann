// Command skills attaches a skills device to a machine and drives it with a
// scripted model that does what a real one would: list the skills, load the
// one the task needs, read the resource its instructions point to, answer.
//
//	go run ./examples/skills
//
// The skills tree is created in a temporary directory and removed at the end.
// One of its skills is deliberately broken, to show Problems.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/skills"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

var tree = map[string]string{
	"pdf-forms/SKILL.md": `---
name: pdf-forms
description: Fill in PDF forms and check the result.
when_to_use: The user hands over a PDF form and the values to put in it.
allowed-tools: [shell_run]
---
# Filling PDF forms

1. Read references/fields.md for the field names this form uses.
2. Run scripts/fill.py with the values (with a tool that executes things).
3. Open the output and check every field.
`,
	"pdf-forms/references/fields.md": "| field | meaning |\n|---|---|\n| name_1 | full name |\n| date_2 | date of birth |\n",
	"pdf-forms/scripts/fill.py":      "import sys\nprint('filling', sys.argv[1:])\n",
	"changelog/SKILL.md": `---
name: changelog
description: >
  Write a changelog entry from the commits
  since the last tag.
---
Group the commits by kind and keep one line each.
`,
	"broken/SKILL.md": "---\ndescription: a skill with no name\n---\n",
}

func main() {
	root, err := os.MkdirTemp("", "neumann-skills-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)
	for path, content := range tree {
		p := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			log.Fatal(err)
		}
	}

	dev, err := skills.New("skills", root)
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range dev.Problems() {
		fmt.Printf("skipped %s\n", p)
	}
	fmt.Printf("\n== catalogue (also what skills_list returns) ==\n%s\n", dev.Catalogue())

	m, err := neumann.New(neumann.Config{
		Model:   &sim.Model{Fn: script},
		Devices: []bus.Device{dev},
		// A host may put the catalogue in the system prompt instead of
		// relying on the list tool; it is byte-stable, so it caches.
		System: "You are a careful assistant.\n\n" + dev.Catalogue(),
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := m.Stop(sctx); err != nil {
			log.Print(err)
		}
	}()

	id, err := m.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, Input: "Fill in form.pdf for Ada Lovelace, born 1815-12-10."})
	if err != nil {
		log.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for ev := range m.Events(wctx, 0) {
		if ev.TaskID != id {
			continue
		}
		switch ev.Kind {
		case proto.EvToolCall, proto.EvAnswer, proto.EvDone, proto.EvFailed:
			fmt.Printf("event %-11s %s\n", ev.Kind, ev.Data)
		}
		if ev.Terminal() {
			break
		}
	}
}

// script plays the model: one step per tool round so far, printing each tool
// result it is shown.
func script(req model.Request) (model.Response, error) {
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Role == task.RoleTool {
		for _, p := range req.Messages[n-1].Parts {
			if p.Kind == task.PartToolResult {
				fmt.Printf("\n== the model sees ==\n%s\n\n", p.Text)
			}
		}
	}
	switch sim.ToolRounds(req) {
	case 0:
		return callTool("skills_list", `{}`), nil
	case 1:
		return callTool("skills_load", `{"name":"pdf-forms"}`), nil
	case 2:
		return callTool("skills_read", `{"name":"pdf-forms","path":"references/fields.md"}`), nil
	case 3:
		// The instructions say to run scripts/fill.py; this machine has no
		// device that executes things, and the skills device will not.
		fields := req.Messages[len(req.Messages)-1].Parts[0].Text
		if !strings.Contains(fields, "name_1") {
			return sim.Reply("the field list did not arrive"), nil
		}
		return sim.Reply("The form uses name_1 and date_2. Nothing here can run scripts/fill.py, " +
			"so enter name_1 = Ada Lovelace and date_2 = 1815-12-10 by hand."), nil
	}
	return sim.Reply("done"), nil
}

func callTool(name, args string) model.Response {
	msg := task.Message{Role: task.RoleAssistant, Parts: []task.Part{{
		Kind: task.PartToolCall, ProviderCallID: "p-" + name, Name: name, Args: json.RawMessage(args),
	}}}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: task.Usage{PromptTokens: 10, CompletionTokens: 5}}
}
