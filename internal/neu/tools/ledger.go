package tools

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const ledgerName = ".ledger"

// maxLedgerResult bounds the result text a ledger line keeps for a repeat.
const maxLedgerResult = 6 << 10

// LedgerEntry is one line of the change ledger: a change about to be made
// (Status "intent"), a change applied, or a call that arrived again for a
// change already applied (Status "duplicate", nothing changed).
type LedgerEntry struct {
	Time   time.Time `json:"t"`
	Key    string    `json:"key"`
	Task   string    `json:"task,omitempty"`
	Kind   string    `json:"kind,omitempty"` // write (also when empty: older ledgers), edit or delete
	Path   string    `json:"path"`
	Bytes  int       `json:"bytes"`
	SHA256 string    `json:"sha256"`           // the file after the change; "" when it is deleted
	Before string    `json:"before,omitempty"` // the file before; "" when it did not exist
	Result string    `json:"result,omitempty"` // what the tool returned, for a repeat
	Status string    `json:"status"`           // intent, applied or duplicate
}

// Ledger makes the file-changing tools (fs_write, fs_edit, fs_delete)
// apply each call once. It is a JSON Lines file. A change is recorded as
// an intent (the file's hash before and after) before it is made and as
// applied after, each line fsynced, and calls are matched by their call,
// the task/call part of the idempotency key, so a call sent again after a
// restart is recognised whether it arrives under the same key (a resend)
// or a new attempt (an operator's Retry):
//
//   - applied: nothing is changed again, and the first result is returned;
//   - an intent only (a crash between the change and its applied line): if
//     the file now has the intent's after-hash the change landed, and it
//     is recorded applied without being redone; otherwise it is made now.
//
// Changes to files run one at a time, so two calls of one round on the
// same file cannot lose each other's edit.
type Ledger struct {
	path    string
	mu      sync.Mutex
	applied map[string]LedgerEntry // by call
	intents map[string]LedgerEntry // by call: intents without an applied line
}

// OpenLedger reads the ledger at path (creating nothing until the first
// change).
func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, applied: map[string]LedgerEntry{}, intents: map[string]LedgerEntry{}}
	ents, err := ReadLedger(path)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		id := callOf(e.Key)
		switch e.Status {
		case "applied":
			l.applied[id] = e
			delete(l.intents, id)
		case "intent":
			if _, ok := l.applied[id]; !ok {
				l.intents[id] = e
			}
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

// callOf is the call an idempotency key names: task/call without the
// attempt. A key of another shape is its own call.
func callOf(key string) string {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		return key
	}
	if _, err := strconv.Atoi(key[i+1:]); err != nil {
		return key
	}
	return key[:i]
}

// Path is the ledger file.
func (l *Ledger) Path() string { return l.path }

// Applied reports whether the call of key (any attempt) was applied, and
// its entry.
func (l *Ledger) Applied(key string) (LedgerEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.applied[callOf(key)]
	return e, ok
}

// Change is what a tool decided to do to a file, given its content now.
type Change struct {
	After  []byte // the new content; ignored with Delete
	Delete bool
	Result string // what the tool returns
}

// Write writes data to abs (shown as rel) unless the call was applied
// already.
func (l *Ledger) Write(key, task, rel, abs string, data []byte) (string, error) {
	return l.Do(key, task, "write", rel, abs, func(cur []byte, exists bool) (Change, error) {
		return Change{After: data, Result: fmt.Sprintf("wrote %d bytes to %s", len(data), rel)}, nil
	})
}

// Do changes the file abs (shown as rel) as f decides from its current
// content, once per call (see Ledger). f is not called when the call was
// applied already; its error is returned and nothing is changed.
func (l *Ledger) Do(key, task, kind, rel, abs string, f func(cur []byte, exists bool) (Change, error)) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := callOf(key)
	e := LedgerEntry{Time: time.Now().UTC(), Key: key, Task: task, Kind: kind, Path: rel}
	if key != "" {
		if prev, ok := l.applied[id]; ok {
			e.Status, e.SHA256, e.Bytes = "duplicate", prev.SHA256, prev.Bytes
			if err := l.append(e); err != nil {
				return "", err
			}
			return repeated(prev), nil
		}
	}
	cur, exists, mode, err := current(abs, rel)
	if err != nil {
		return "", err
	}
	if in, ok := l.intents[id]; ok && key != "" && in.Path == rel && hashOf(cur, exists) == in.SHA256 && in.SHA256 != in.Before {
		// The change landed before a crash; only its applied line is missing.
		in.Time, in.Key, in.Status = e.Time, key, "applied"
		if err := l.append(in); err != nil {
			return "", err
		}
		l.applied[id] = in
		delete(l.intents, id)
		return repeated(in), nil
	}
	ch, err := f(cur, exists)
	if err != nil {
		return "", err
	}
	e.Before = hashOf(cur, exists)
	if ch.Delete {
		if !exists {
			return "", fmt.Errorf("%s: no such file", rel)
		}
	} else {
		e.SHA256, e.Bytes = hashOf(ch.After, true), len(ch.After)
	}
	e.Result = clipResult(ch.Result)
	if key != "" {
		e.Status = "intent"
		if err := l.append(e); err != nil {
			return "", err
		}
		l.intents[id] = e
	}
	if ch.Delete {
		err = os.Remove(abs)
	} else {
		err = writeFile(abs, rel, ch.After, mode)
	}
	if err != nil {
		return "", err
	}
	e.Status = "applied"
	if err := l.append(e); err != nil {
		return "", err
	}
	if key != "" {
		l.applied[id] = e
		delete(l.intents, id)
	}
	return ch.Result, nil
}

// repeated is a repeat's result: what the call did the first time.
func repeated(prev LedgerEntry) string {
	if prev.Kind == "" || prev.Kind == "write" {
		return fmt.Sprintf("already written: %s (%d bytes) under this call's key", prev.Path, prev.Bytes)
	}
	return "already applied under this call's key (a repeat after a restart; nothing changed now). It did:\n" + prev.Result
}

func clipResult(s string) string {
	if len(s) <= maxLedgerResult {
		return s
	}
	return s[:maxLedgerResult] + "\n…"
}

// current reads the file at abs: its content, whether it exists, and its
// mode (0644 for a new file).
func current(abs, rel string) ([]byte, bool, fs.FileMode, error) {
	fi, err := os.Lstat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, 0o644, nil
	}
	if err != nil {
		return nil, false, 0, err
	}
	if fi.IsDir() {
		return nil, false, 0, fmt.Errorf("%s is a directory", rel)
	}
	if !fi.Mode().IsRegular() {
		return nil, false, 0, fmt.Errorf("%s is not a regular file", rel)
	}
	if fi.Size() > MaxFileBytes {
		return nil, false, 0, fmt.Errorf("%s is %d bytes, over the %d byte limit", rel, fi.Size(), MaxFileBytes)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, false, 0, err
	}
	return data, true, fi.Mode().Perm(), nil
}

func hashOf(data []byte, exists bool) string {
	if !exists {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeFile replaces abs with data atomically (a temporary file renamed
// over it), keeping mode.
func writeFile(abs, rel string, data []byte, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), ".neu-write-*")
	if err != nil {
		return err
	}
	_, werr := tmp.Write(data)
	if werr == nil {
		werr = tmp.Chmod(mode)
	}
	if werr == nil {
		werr = tmp.Sync()
	}
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("write %s: %v", rel, firstErr(werr, cerr))
	}
	if err := os.Rename(tmp.Name(), abs); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
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
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
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
