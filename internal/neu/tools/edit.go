package tools

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/liliang-cn/neumann/bus"
)

// MaxWriteBytes bounds what fs_write and fs_edit may write.
const MaxWriteBytes = 1 << 20

// changePath resolves p for a change: anywhere under the root with
// WriteAnywhere, else only under the writable directories.
func (t *Tools) changePath(p string) (string, error) {
	if p == "" {
		return "", errors.New("path is required")
	}
	if t.WriteAnywhere {
		return t.Box.Mutable(p)
	}
	return t.Box.Writable(p)
}

type writeArgs struct {
	Path    string
	Content *string
}

type editArgs struct {
	Path       string
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all"`
}

func (t *Tools) write(_ context.Context, c bus.DeviceCall) (string, error) {
	var in writeArgs
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.Path == "" || in.Content == nil {
		return "", errors.New("path and content are required")
	}
	if len(*in.Content) > MaxWriteBytes {
		return "", errors.New("content over 1 MiB")
	}
	rp, err := t.changePath(in.Path)
	if err != nil {
		return "", err
	}
	if !t.Coding {
		return t.Ledger.Write(c.Key, c.Task, t.Box.Rel(rp), rp, []byte(*in.Content))
	}
	rel := t.Box.Rel(rp)
	return t.Ledger.Do(c.Key, c.Task, "write", rel, rp, func(cur []byte, exists bool) (Change, error) {
		return Change{After: []byte(*in.Content), Result: writeResult(rel, cur, exists, *in.Content)}, nil
	})
}

func writeResult(rel string, cur []byte, exists bool, content string) string {
	n := len(splitLines([]byte(content)))
	if !exists {
		return fmt.Sprintf("created %s: %d bytes, %d lines", rel, len(content), n)
	}
	if string(cur) == content {
		return fmt.Sprintf("wrote %d bytes to %s (unchanged)", len(content), rel)
	}
	return fmt.Sprintf("wrote %d bytes to %s (%d lines)\n%s", len(content), rel, n, Diff(rel, string(cur), content, 0))
}

// planEdit applies an edit to cur, or says why it does not apply.
func planEdit(rel string, cur []byte, exists bool, in editArgs) (string, string, error) {
	if !exists {
		return "", "", fmt.Errorf("%s: no such file; create it with fs_write", rel)
	}
	if binary(cur) {
		return "", "", fmt.Errorf("%s looks binary", rel)
	}
	s := string(cur)
	n := strings.Count(s, in.OldString)
	switch {
	case n == 0:
		hint := "read the file again (fs_read) and copy the text exactly, including indentation and line breaks"
		if strings.Contains(s, "\r\n") && strings.Contains(in.OldString, "\n") && !strings.Contains(in.OldString, "\r\n") {
			hint = "the file has CRLF line endings; " + hint
		} else if t := strings.TrimSpace(in.OldString); t != "" && t != in.OldString && strings.Contains(s, t) {
			hint = "the text matches only without its leading or trailing whitespace; " + hint
		}
		return "", "", fmt.Errorf("old_string not found in %s: %s", rel, hint)
	case n > 1 && !in.ReplaceAll:
		return "", "", fmt.Errorf("old_string matches %d times in %s; include more of the surrounding lines to make it unique, or set replace_all to change every one", n, rel)
	}
	limit := 1
	if in.ReplaceAll {
		limit = -1
	}
	after := strings.Replace(s, in.OldString, in.NewString, limit)
	if len(after) > MaxWriteBytes+len(s) {
		return "", "", errors.New("the edit would add over 1 MiB")
	}
	if !in.ReplaceAll {
		n = 1
	}
	head := fmt.Sprintf("edited %s: %d replacement%s", rel, n, plural(n))
	return after, head + "\n" + Diff(rel, s, after, 0), nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func (t *Tools) edit(_ context.Context, c bus.DeviceCall) (string, error) {
	var in editArgs
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.OldString == "" {
		return "", errors.New("old_string is empty; to create a file or replace all of it use fs_write")
	}
	if in.OldString == in.NewString {
		return "", errors.New("old_string and new_string are the same; nothing to change")
	}
	rp, err := t.changePath(in.Path)
	if err != nil {
		return "", err
	}
	rel := t.Box.Rel(rp)
	return t.Ledger.Do(c.Key, c.Task, "edit", rel, rp, func(cur []byte, exists bool) (Change, error) {
		after, res, err := planEdit(rel, cur, exists, in)
		if err != nil {
			return Change{}, err
		}
		return Change{After: []byte(after), Result: res}, nil
	})
}

// deletePath resolves p for fs_delete, refusing a symlink (it would
// delete what it points at) and directories.
func (t *Tools) deletePath(p string) (string, error) {
	rp, err := t.changePath(p)
	if err != nil {
		return "", err
	}
	if lex, err := t.Box.lexical(p); err == nil {
		if fi, err := os.Lstat(lex); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%s is a symlink; not deleted", p)
		}
	}
	if fi, err := os.Stat(rp); err == nil && fi.IsDir() {
		return "", fmt.Errorf("%s is a directory; fs_delete deletes files", p)
	}
	return rp, nil
}

func (t *Tools) remove(_ context.Context, c bus.DeviceCall) (string, error) {
	var in struct{ Path string }
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	rp, err := t.deletePath(in.Path)
	if err != nil {
		return "", err
	}
	rel := t.Box.Rel(rp)
	return t.Ledger.Do(c.Key, c.Task, "delete", rel, rp, func(cur []byte, exists bool) (Change, error) {
		if !exists {
			return Change{}, fmt.Errorf("%s: no such file", rel)
		}
		return Change{Delete: true, Result: fmt.Sprintf("deleted %s (%d bytes, %d lines)", rel, len(cur), len(splitLines(cur)))}, nil
	})
}

// preview is what a change would do, for an approval prompt: the path and
// a diff, computed without changing anything.
func (t *Tools) preview(tool string, raw []byte) string {
	switch tool {
	case "fs_write":
		var in writeArgs
		if decode(raw, &in) != nil || in.Content == nil {
			return ""
		}
		rp, err := t.changePath(in.Path)
		if err != nil {
			return in.Path + ": " + err.Error()
		}
		rel := t.Box.Rel(rp)
		cur, exists, _, err := current(rp, rel)
		if err != nil {
			return err.Error()
		}
		if !exists {
			return fmt.Sprintf("create %s (%d bytes)\n%s", rel, len(*in.Content), Diff(rel, "", *in.Content, 12))
		}
		return fmt.Sprintf("replace %s (%d → %d bytes)\n%s", rel, len(cur), len(*in.Content), Diff(rel, string(cur), *in.Content, 24))
	case "fs_edit":
		var in editArgs
		if decode(raw, &in) != nil {
			return ""
		}
		rp, err := t.changePath(in.Path)
		if err != nil {
			return in.Path + ": " + err.Error()
		}
		rel := t.Box.Rel(rp)
		cur, exists, _, err := current(rp, rel)
		if err != nil {
			return err.Error()
		}
		after, _, err := planEdit(rel, cur, exists, in)
		if err != nil {
			return err.Error()
		}
		return "edit " + rel + "\n" + Diff(rel, string(cur), after, 24)
	case "fs_delete":
		var in struct{ Path string }
		if decode(raw, &in) != nil {
			return ""
		}
		rp, err := t.deletePath(in.Path)
		if err != nil {
			return in.Path + ": " + err.Error()
		}
		fi, err := os.Stat(rp)
		if err != nil {
			return "delete " + t.Box.Rel(rp) + " (does not exist)"
		}
		return fmt.Sprintf("delete %s (%d bytes)", t.Box.Rel(rp), fi.Size())
	}
	return ""
}

// ledgerPath is where the ledger lives: in the first writable directory
// (out/.ledger, where it always was), else in .neu/.
func ledgerPath(box *Sandbox) string {
	if len(box.writable) > 0 {
		return filepath.Join(box.writable[0], ledgerName)
	}
	return filepath.Join(box.root, ".neu", ledgerName)
}
