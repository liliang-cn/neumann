package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/task"
)

func TestSecondOpenIsLocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	s := open(t, path)
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open: want ErrLocked, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s2.Close()
}

func TestOpenRejectsUnsharedPaths(t *testing.T) {
	for _, p := range []string{"", ":memory:"} {
		if s, err := Open(p); err == nil {
			s.Close()
			t.Fatalf("Open(%q) succeeded", p)
		}
	}
}

func TestPragmas(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	defer s.Close()
	var v int
	if err := s.db.QueryRow("PRAGMA synchronous").Scan(&v); err != nil || v != 2 {
		t.Fatalf("synchronous = %d, %v; want 2 (FULL)", v, err)
	}
	if runtime.GOOS == "darwin" {
		if err := s.db.QueryRow("PRAGMA fullfsync").Scan(&v); err != nil || v != 1 {
			t.Fatalf("fullfsync = %d, %v; want 1", v, err)
		}
	}
}

func TestAwkwardPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a ?#%b")
	if err := mkdir(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "n e?u#m%41.db")
	s := open(t, path)
	tc := &task.TCB{ID: "a", State: task.Ready, Class: "default", Version: 1}
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, path)
	defer s.Close()
	if _, err := s.Load(ctx, "a"); err != nil {
		t.Fatalf("reopen at awkward path: %v", err)
	}
}

func mkdir(d string) error { return os.MkdirAll(d, 0o755) }
