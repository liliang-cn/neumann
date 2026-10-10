// Package sqlite is a durable mem.Store on SQLite (pure Go, no cgo).
//
// One writer goroutine applies commits. While one transaction is being
// written, newly arriving commits queue up and go into the next transaction
// together (group commit), so the cost of each fsync is shared under load
// without delaying a commit that arrives alone. Every commit set runs under
// its own savepoint: one that fails validation is rolled back alone and does
// not affect the others in its transaction.
//
// TCBs and call records are stored gob-encoded: a model can emit tool
// arguments that are not valid JSON, and they must round-trip byte for byte.
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

var ErrClosed = errors.New("sqlite: store closed")

// ErrLocked is returned by Open when another Store (in this or another
// process) already owns the database file.
var ErrLocked = errors.New("sqlite: database is locked by another owner")

// maxBatch bounds how many queued commit sets share one transaction.
const maxBatch = 128

const schema = `
CREATE TABLE IF NOT EXISTS tasks (
	id       TEXT PRIMARY KEY,
	version  INTEGER NOT NULL,
	terminal INTEGER NOT NULL,
	tcb      BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS calls (
	task_id TEXT NOT NULL,
	call_id TEXT NOT NULL,
	idx     INTEGER NOT NULL,
	rec     BLOB NOT NULL,
	PRIMARY KEY (task_id, call_id)
);
CREATE TABLE IF NOT EXISTS interrupts (
	seq     INTEGER PRIMARY KEY,
	task_id TEXT NOT NULL,
	kind    TEXT NOT NULL,
	text    TEXT NOT NULL,
	applied INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS interrupts_task ON interrupts (task_id, applied);
CREATE TABLE IF NOT EXISTS events (
	seq      INTEGER PRIMARY KEY,
	task_id  TEXT NOT NULL,
	task_seq INTEGER NOT NULL,
	kind     TEXT NOT NULL,
	data     BLOB
);
CREATE TABLE IF NOT EXISTS task_seqs (
	task_id TEXT PRIMARY KEY,
	n       INTEGER NOT NULL
);`

type Store struct {
	db   *sql.DB
	lock *os.File // sidecar flock held for the Store's lifetime; nil where unsupported

	mu     sync.RWMutex // guards closed against sends on reqs
	closed bool
	reqs   chan *request
	done   chan struct{} // closed when the writer has exited
}

var _ mem.Store = (*Store)(nil)

type request struct {
	c   mem.CommitSet
	evs []proto.Event
	err error
	ok  chan struct{}
}

// Open opens (creating if needed) the database at path. Commits are durable
// when Commit returns: the database runs in WAL mode with synchronous=FULL
// (and F_FULLFSYNC on macOS, where plain fsync does not flush the drive
// cache). Transactions take the write lock when they begin, so a batch never
// fails upgrading a read lock.
//
// A Store owns its database: Open takes an exclusive advisory lock on
// path+".lock" and fails with ErrLocked while another Store holds it. Where
// the platform has no flock this is not enforced. ":memory:" and "" are
// rejected, since every pooled connection would get a database of its own.
func Open(path string) (*Store, error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file::memory:") {
		return nil, fmt.Errorf("sqlite: open %q: need a file path (an in-memory database is per connection)", path)
	}
	lock, err := lockFile(path + ".lock")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	fail := func(err error) (*Store, error) {
		if lock != nil {
			unlockFile(lock)
		}
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return fail(fmt.Errorf("sqlite: open %s: %w", path, err))
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return fail(fmt.Errorf("sqlite: create schema: %w", err))
	}
	s := &Store{db: db, lock: lock, reqs: make(chan *request, maxBatch), done: make(chan struct{})}
	go s.writer()
	return s, nil
}

// dsn builds the connection string. The path goes into a file: URI, so every
// byte that is special there ('?', '#', '%', space...) is percent-escaped;
// '/' is kept literal so absolute paths stay absolute.
func dsn(path string) string {
	esc := strings.ReplaceAll(url.PathEscape(path), "%2F", "/")
	q := "_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)"
	if runtime.GOOS == "darwin" {
		q += "&_pragma=fullfsync(1)"
	}
	return "file:" + esc + "?" + q
}

// Close waits for queued commits to be written, then closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.reqs)
	s.mu.Unlock()
	<-s.done
	err := s.db.Close()
	if s.lock != nil {
		if lerr := unlockFile(s.lock); err == nil {
			err = lerr
		}
	}
	return err
}

// Commit applies c atomically and returns its events with sequence numbers.
// Once accepted for writing it is not abandoned when ctx ends: returning an
// error for a commit that then lands would be a lie, and commits are short.
func (s *Store) Commit(ctx context.Context, c mem.CommitSet) ([]proto.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := &request{c: c, ok: make(chan struct{})}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	s.reqs <- r
	s.mu.RUnlock()
	<-r.ok
	return r.evs, r.err
}

func (s *Store) writer() {
	defer close(s.done)
	for first := range s.reqs {
		batch := []*request{first}
	drain:
		for len(batch) < maxBatch {
			select {
			case r, ok := <-s.reqs:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		s.write(batch)
	}
}

func (s *Store) write(batch []*request) {
	defer func() {
		for _, r := range batch {
			close(r.ok)
		}
	}()
	fail := func(err error) {
		for _, r := range batch {
			if r.err == nil {
				r.evs, r.err = nil, err
			}
		}
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		fail(fmt.Errorf("sqlite: begin: %w", err))
		return
	}
	for _, r := range batch {
		if _, err := tx.ExecContext(ctx, "SAVEPOINT c"); err != nil {
			tx.Rollback()
			fail(fmt.Errorf("sqlite: savepoint: %w", err))
			return
		}
		evs, err := apply(ctx, tx, r.c)
		if err != nil {
			r.err = err
			if _, rerr := tx.ExecContext(ctx, "ROLLBACK TO c"); rerr != nil {
				tx.Rollback()
				fail(fmt.Errorf("sqlite: rollback to savepoint: %w", rerr))
				return
			}
		} else {
			r.evs = evs
		}
		if _, err := tx.ExecContext(ctx, "RELEASE c"); err != nil {
			tx.Rollback()
			fail(fmt.Errorf("sqlite: release savepoint: %w", err))
			return
		}
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback() // never leave an open transaction on the pooled connection
		fail(fmt.Errorf("sqlite: commit: %w", err))
	}
}

// apply validates c against the state before it, then writes it, exactly as
// mem.Memory does (the conformance suite holds both to the same rules).
func apply(ctx context.Context, tx *sql.Tx, c mem.CommitSet) ([]proto.Event, error) {
	if c.NewTask && c.Task == nil {
		return nil, errors.New("mem: NewTask requires Task")
	}
	if len(c.Applied) > 0 && c.Task == nil {
		return nil, errors.New("mem: Applied requires Task")
	}
	if t := c.Task; t != nil {
		version, _, exists, err := taskRow(ctx, tx, t.ID)
		if err != nil {
			return nil, err
		}
		switch {
		case c.NewTask && exists:
			return nil, fmt.Errorf("%w: %s", mem.ErrExists, t.ID)
		case c.NewTask && t.Version != 1:
			return nil, fmt.Errorf("%w: new task %s must have version 1, got %d", mem.ErrVersion, t.ID, t.Version)
		case !c.NewTask && !exists:
			return nil, fmt.Errorf("%w: %s", mem.ErrNotFound, t.ID)
		case !c.NewTask && t.Version != version+1:
			return nil, fmt.Errorf("%w: task %s is at %d, commit carries %d", mem.ErrVersion, t.ID, version, t.Version)
		}
	}
	for _, r := range c.Calls {
		if c.NewTask && c.Task.ID == r.TaskID {
			continue
		}
		if _, _, exists, err := taskRow(ctx, tx, r.TaskID); err != nil {
			return nil, err
		} else if !exists {
			return nil, fmt.Errorf("%w: %s", mem.ErrNotFound, r.TaskID)
		}
	}
	for _, seq := range c.Applied {
		var owner string
		var applied bool
		err := tx.QueryRowContext(ctx, "SELECT task_id, applied FROM interrupts WHERE seq = ?", seq).Scan(&owner, &applied)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (owner != c.Task.ID || applied)) {
			return nil, fmt.Errorf("mem: interrupt %d is not pending for task %s", seq, c.Task.ID)
		}
		if err != nil {
			return nil, err
		}
	}
	for _, in := range c.NewInterrupts {
		_, terminal, exists, err := taskRow(ctx, tx, in.TaskID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", mem.ErrNotFound, in.TaskID)
		}
		if terminal {
			return nil, fmt.Errorf("%w: %s", mem.ErrFinished, in.TaskID)
		}
	}

	events := append([]proto.Event(nil), c.Events...)
	if t := c.Task; t != nil {
		blob, err := encode(t)
		if err != nil {
			return nil, fmt.Errorf("sqlite: encode task %s: %w", t.ID, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tasks (id, version, terminal, tcb) VALUES (?, ?, ?, ?)
			 ON CONFLICT (id) DO UPDATE SET version = excluded.version, terminal = excluded.terminal, tcb = excluded.tcb`,
			t.ID, t.Version, t.State.Terminal(), blob); err != nil {
			return nil, err
		}
	}
	for _, r := range c.Calls {
		blob, err := encode(r)
		if err != nil {
			return nil, fmt.Errorf("sqlite: encode call %s/%s: %w", r.TaskID, r.CallID, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO calls (task_id, call_id, idx, rec) VALUES (?, ?, ?, ?)
			 ON CONFLICT (task_id, call_id) DO UPDATE SET idx = excluded.idx, rec = excluded.rec`,
			r.TaskID, r.CallID, r.Index, blob); err != nil {
			return nil, err
		}
	}
	for _, in := range c.NewInterrupts {
		if _, err := tx.ExecContext(ctx, "INSERT INTO interrupts (task_id, kind, text, applied) VALUES (?, ?, ?, 0)",
			in.TaskID, in.Kind, in.Text); err != nil {
			return nil, err
		}
	}
	if t := c.Task; t != nil {
		if len(c.Applied) > 0 {
			args := make([]any, len(c.Applied))
			for i, seq := range c.Applied {
				args[i] = seq
			}
			q := "UPDATE interrupts SET applied = 1 WHERE seq IN (?" + strings.Repeat(",?", len(args)-1) + ")"
			if _, err := tx.ExecContext(ctx, q, args...); err != nil {
				return nil, err
			}
		}
		if t.State.Terminal() {
			left, err := pending(ctx, tx, t.ID)
			if err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE interrupts SET applied = 1 WHERE task_id = ? AND applied = 0", t.ID); err != nil {
				return nil, err
			}
			for _, in := range left {
				if in.Kind == mem.InterruptSteer {
					events = append(events, proto.NewEvent(proto.EvSteerDropped, t.ID,
						map[string]any{"seq": in.Seq, "text": in.Text}))
				}
			}
		}
	}
	for i := range events {
		events[i].Data = bytes.Clone(events[i].Data)
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO task_seqs (task_id, n) VALUES (?, 1)
			 ON CONFLICT (task_id) DO UPDATE SET n = n + 1 RETURNING n`, events[i].TaskID).Scan(&events[i].TaskSeq); err != nil {
			return nil, err
		}
		res, err := tx.ExecContext(ctx, "INSERT INTO events (task_id, task_seq, kind, data) VALUES (?, ?, ?, ?)",
			events[i].TaskID, events[i].TaskSeq, events[i].Kind, []byte(events[i].Data))
		if err != nil {
			return nil, err
		}
		if events[i].Seq, err = res.LastInsertId(); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func taskRow(ctx context.Context, tx *sql.Tx, id string) (version int64, terminal, exists bool, err error) {
	err = tx.QueryRowContext(ctx, "SELECT version, terminal FROM tasks WHERE id = ?", id).Scan(&version, &terminal)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, false, nil
	}
	return version, terminal, err == nil, err
}

type querier interface {
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
}

func pending(ctx context.Context, q querier, taskID string) ([]mem.Interrupt, error) {
	return interrupts(ctx, q, "SELECT seq, task_id, kind, text, applied FROM interrupts WHERE task_id = ? AND applied = 0 ORDER BY seq", taskID)
}

func interrupts(ctx context.Context, q querier, query string, args ...any) ([]mem.Interrupt, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mem.Interrupt
	for rows.Next() {
		var in mem.Interrupt
		if err := rows.Scan(&in.Seq, &in.TaskID, &in.Kind, &in.Text, &in.Applied); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func encode(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decode(b []byte, v any) error { return gob.NewDecoder(bytes.NewReader(b)).Decode(v) }

func (s *Store) Load(ctx context.Context, id string) (*task.TCB, error) {
	var blob []byte
	err := s.db.QueryRowContext(ctx, "SELECT tcb FROM tasks WHERE id = ?", id).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", mem.ErrNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	var t task.TCB
	if err := decode(blob, &t); err != nil {
		return nil, fmt.Errorf("sqlite: decode task %s: %w", id, err)
	}
	return &t, nil
}

func (s *Store) Unfinished(ctx context.Context) ([]*task.TCB, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, tcb FROM tasks WHERE terminal = 0 ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*task.TCB
	for rows.Next() {
		var id string
		var blob []byte
		if err := rows.Scan(&id, &blob); err != nil {
			return nil, err
		}
		var t task.TCB
		if err := decode(blob, &t); err != nil {
			return nil, fmt.Errorf("sqlite: decode task %s: %w", id, err)
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *Store) Calls(ctx context.Context, taskID string) ([]mem.CallRecord, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT call_id, rec FROM calls WHERE task_id = ? ORDER BY idx", taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []mem.CallRecord{}
	for rows.Next() {
		var id string
		var blob []byte
		if err := rows.Scan(&id, &blob); err != nil {
			return nil, err
		}
		var r mem.CallRecord
		if err := decode(blob, &r); err != nil {
			return nil, fmt.Errorf("sqlite: decode call %s/%s: %w", taskID, id, err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Interrupts(ctx context.Context, taskID string, unappliedOnly bool) ([]mem.Interrupt, error) {
	if unappliedOnly {
		return pending(ctx, s.db, taskID)
	}
	return interrupts(ctx, s.db, "SELECT seq, task_id, kind, text, applied FROM interrupts WHERE task_id = ? ORDER BY seq", taskID)
}

func (s *Store) Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	if limit <= 0 {
		limit = -1 // SQLite: no limit
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, task_id, task_seq, kind, data FROM events WHERE seq > ? ORDER BY seq LIMIT ?", afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []proto.Event
	for rows.Next() {
		var e proto.Event
		var data []byte
		if err := rows.Scan(&e.Seq, &e.TaskID, &e.TaskSeq, &e.Kind, &data); err != nil {
			return nil, err
		}
		if len(data) > 0 {
			e.Data = data
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
