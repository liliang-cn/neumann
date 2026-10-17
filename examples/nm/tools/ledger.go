package tools

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ledgerName = ".ledger"

// LedgerEntry is one line of the write ledger: a write fs_write applied, or
// a call that arrived again with a key already applied (Status
// "duplicate", nothing written).
type LedgerEntry struct {
	Time   time.Time `json:"t"`
	Key    string    `json:"key"`
	Task   string    `json:"task,omitempty"`
	Path   string    `json:"path"`
	Bytes  int       `json:"bytes"`
	SHA256 string    `json:"sha256"`
	Status string    `json:"status"` // applied or duplicate
}

// Ledger makes fs_write idempotent by its call's idempotency key: a key
// already applied is not written again. It is a JSON Lines file, fsynced
// after the file it records, so a crash between the two leaves a write
// without its line (the operator's Resolve then retries it under a new
// key, which writes the same content again) and never a line without its
// write.
type Ledger struct {
	path    string
	mu      sync.Mutex
	applied map[string]LedgerEntry
}

// OpenLedger reads the ledger at path (creating nothing until the first
// write).
func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, applied: map[string]LedgerEntry{}}
	ents, err := ReadLedger(path)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if e.Status == "applied" {
			l.applied[e.Key] = e
		}
	}
	return l, nil
}

// ReadLedger returns every entry of the ledger at path; none if it does
// not exist. A torn last line (a crash mid-append) is skipped.
func ReadLedger(path string) ([]LedgerEntry, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []LedgerEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var e LedgerEntry
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.Key != "" {
			out = append(out, e)
		}
	}
	return out, sc.Err()
}

// Path is the ledger file.
func (l *Ledger) Path() string { return l.path }

// Applied reports whether a write with key was applied, and its entry.
func (l *Ledger) Applied(key string) (LedgerEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.applied[key]
	return e, ok
}

// Write writes data to abs (shown as rel) unless key was applied already.
func (l *Ledger) Write(key, task, rel, abs string, data []byte) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sum := sha256.Sum256(data)
	e := LedgerEntry{Time: time.Now().UTC(), Key: key, Task: task, Path: rel, Bytes: len(data), SHA256: hex.EncodeToString(sum[:])}
	if key != "" {
		if prev, ok := l.applied[key]; ok {
			e.Status = "duplicate"
			if err := l.append(e); err != nil {
				return "", err
			}
			return fmt.Sprintf("already written: %s (%d bytes) under this call's key", prev.Path, prev.Bytes), nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), ".nm-write-*")
	if err != nil {
		return "", err
	}
	_, werr := tmp.Write(data)
	if werr == nil {
		werr = tmp.Sync()
	}
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write %s: %v", rel, firstErr(werr, cerr))
	}
	if err := os.Rename(tmp.Name(), abs); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	e.Status = "applied"
	if err := l.append(e); err != nil {
		return "", err
	}
	if key != "" {
		l.applied[key] = e
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(data), rel), nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func (l *Ledger) append(e LedgerEntry) error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(e)
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	return firstErr(err, f.Close())
}
