// Package tools is nm's workspace devices: reading, searching and listing
// files, read-only git, go test, a calculator and a write tool, all confined
// to one workspace root.
package tools

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Sandbox resolves paths the model gives against a workspace root. A path
// may be relative to the root or absolute inside it; anything that leaves
// the root, through ".." or a symlink, is refused.
type Sandbox struct {
	root     string   // absolute, symlinks resolved
	writable []string // absolute, symlinks resolved; each inside root
	reserved []string // files never written: the store and its journals
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
	out, err := realPath(full)
	if err != nil {
		return "", err
	}
	if !within(s.root, out) {
		return "", errEscape
	}
	return out, nil
}

// realPath is the clean absolute path full with every symlink of its
// deepest existing ancestor resolved.
func realPath(full string) (string, error) {
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
	return filepath.Join(real, rest), nil
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

// reservedDirs are directories that are never writable, even with the
// whole root writable: neu's store and git's, at any depth (a nested
// repository's .git too).
var reservedDirs = []string{".neu", ".git"}

// Reserve marks files (absolute or relative to the root) that no tool may
// write or delete, such as a store outside .neu/.
func (s *Sandbox) Reserve(paths ...string) {
	for _, p := range paths {
		if abs, err := filepath.Abs(p); err == nil {
			if rp, err := realPath(abs); err == nil && within(s.root, rp) {
				s.reserved = append(s.reserved, rp)
			}
		}
	}
}

// changeable resolves p and refuses what no tool may change: the ledger,
// a reserved file, anything in a reserved directory, and the root itself.
func (s *Sandbox) changeable(p string) (string, error) {
	rp, err := s.Resolve(p)
	if err != nil {
		return "", err
	}
	if rp == s.root {
		return "", errors.New("that is the workspace root")
	}
	if filepath.Base(rp) == ledgerName || slices.Contains(s.reserved, rp) {
		return "", errors.New("that file is reserved")
	}
	for _, part := range strings.Split(s.Rel(rp), "/") {
		if slices.Contains(reservedDirs, part) {
			return "", fmt.Errorf("%s/ is reserved", part)
		}
	}
	return rp, nil
}

// Writable resolves p and checks that a write there is allowed without
// asking: under one of the writable directories, and changeable.
func (s *Sandbox) Writable(p string) (string, error) {
	rp, err := s.changeable(p)
	if err != nil {
		return "", err
	}
	if s.InWritable(rp) {
		return rp, nil
	}
	if len(s.writable) == 0 {
		return "", errors.New("no directory is writable")
	}
	var names []string
	for _, w := range s.writable {
		names = append(names, s.Rel(w)+"/")
	}
	return "", fmt.Errorf("writes are allowed only under %s", strings.Join(names, ", "))
}

// InWritable reports whether rp, a resolved path, is inside one of the
// writable directories (not the directory itself).
func (s *Sandbox) InWritable(rp string) bool {
	for _, w := range s.writable {
		if rp != w && within(w, rp) {
			return true
		}
	}
	return false
}

// Mutable resolves p for a change anywhere under the root: everything but
// the ledger, reserved files and reserved directories.
func (s *Sandbox) Mutable(p string) (string, error) { return s.changeable(p) }

// within reports whether p is dir or inside it; both are clean absolute paths.
func within(dir, p string) bool {
	if p == dir {
		return true
	}
	return strings.HasPrefix(p, strings.TrimSuffix(dir, string(filepath.Separator))+string(filepath.Separator))
}
