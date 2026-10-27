// Package tools is nm's workspace devices: reading, searching and listing
// files, read-only git, go test, a calculator and a write tool, all confined
// to one workspace root.
package tools

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Sandbox resolves paths the model gives against a workspace root. A path
// may be relative to the root or absolute inside it; anything that leaves
// the root, through ".." or a symlink, is refused.
type Sandbox struct {
	root     string   // absolute, symlinks resolved
	writable []string // absolute, symlinks resolved; each inside root
}

// NewSandbox returns a sandbox on root. writable are directories (relative
// to root, or absolute inside it) under which writes are allowed; each is
// created if missing.
func NewSandbox(root string, writable []string) (*Sandbox, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("workspace root: %w", err)
	}
	if fi, err := os.Stat(real); err != nil || !fi.IsDir() {
		return nil, fmt.Errorf("workspace root %s is not a directory", root)
	}
	s := &Sandbox{root: real}
	for _, w := range writable {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		p, err := s.lexical(w)
		if err != nil {
			return nil, fmt.Errorf("writable %q: %w", w, err)
		}
		if err := os.MkdirAll(p, 0o755); err != nil {
			return nil, err
		}
		rp, err := s.Resolve(w)
		if err != nil {
			return nil, fmt.Errorf("writable %q: %w", w, err)
		}
		s.writable = append(s.writable, rp)
	}
	return s, nil
}

// Root is the workspace root, absolute with symlinks resolved.
func (s *Sandbox) Root() string { return s.root }

var errEscape = errors.New("path is outside the workspace")

// lexical joins p onto the root and cleans it, refusing a result outside
// the root. It does not look at the file system.
func (s *Sandbox) lexical(p string) (string, error) {
	if strings.ContainsRune(p, 0) {
		return "", errors.New("path contains a NUL byte")
	}
	var full string
	if filepath.IsAbs(p) {
		full = filepath.Clean(p)
	} else {
		full = filepath.Join(s.root, p)
	}
	if !within(s.root, full) {
		return "", errEscape
	}
	return full, nil
}

// Resolve returns the real path of p: inside the root after every symlink
// is followed. The file need not exist: the deepest existing ancestor is
// resolved and the rest appended, so a write to a new file under a symlink
// that points outside is refused too.
func (s *Sandbox) Resolve(p string) (string, error) {
	full, err := s.lexical(p)
	if err != nil {
		return "", err
	}
	existing, rest := full, ""
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", errEscape
		}
		rest = filepath.Join(filepath.Base(existing), rest)
		existing = parent
	}
	real, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	out := filepath.Join(real, rest)
	if !within(s.root, out) {
		return "", errEscape
	}
	return out, nil
}

// Rel is p (a real path inside the root) relative to the root, with forward
// slashes; "." for the root itself.
func (s *Sandbox) Rel(p string) string {
	r, err := filepath.Rel(s.root, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(r)
}

// Writable resolves p and checks that a write there is allowed: under one
// of the writable directories, and not a hidden file of nm's own (the
// ledger).
func (s *Sandbox) Writable(p string) (string, error) {
	rp, err := s.Resolve(p)
	if err != nil {
		return "", err
	}
	if filepath.Base(rp) == ledgerName {
		return "", errors.New("that file is reserved")
	}
	for _, w := range s.writable {
		if rp != w && within(w, rp) {
			return rp, nil
		}
	}
	var names []string
	for _, w := range s.writable {
		names = append(names, s.Rel(w)+"/")
	}
	return "", fmt.Errorf("writes are allowed only under %s", strings.Join(names, ", "))
}

// within reports whether p is dir or inside it; both are clean absolute paths.
func within(dir, p string) bool {
	if p == dir {
		return true
	}
	return strings.HasPrefix(p, strings.TrimSuffix(dir, string(filepath.Separator))+string(filepath.Separator))
}
