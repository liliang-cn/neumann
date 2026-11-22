// Package sqlite is a durable mem.Store on SQLite (pure Go, no cgo).
//
// One writer goroutine applies commits. While one transaction is being
// written, newly arriving commits queue up and go into the next transaction
// together (group commit), so the cost of each fsync is shared under load
// without delaying a commit that arrives alone. Every commit set runs under
// its own savepoint: one that fails validation is rolled back alone and does
// not affect the others in its transaction.
//
// TCBs, call records and archived messages are stored gob-encoded: a model can emit tool
// arguments that are not valid JSON, and they must round-trip byte for byte.
//
// Use: s, err := sqlite.Open(path); pass s as neumann.Config.Store; Close it
// after Machine.Stop. One process owns a database file at a time (a second
// Open fails with ErrLocked).
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
	"slices"
	"strings"
	"sync"
	"time"

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
);
CREATE TABLE IF NOT EXISTS archive (
	seq     INTEGER PRIMARY KEY,
	task_id TEXT NOT NULL,
	msg     BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS archive_task ON archive (task_id, seq);
CREATE INDEX IF NOT EXISTS events_task ON events (task_id, seq);`

type Store struct {
	db   *sql.DB
	lock *os.File // sidecar flock held for the Store's lifetime; nil where unsupported

	mu     sync.RWMutex // guards closed against sends on reqs
	closed bool
	reqs   chan *request
	done   chan struct{} // closed when the writer has exited

	now     func() time.Time // stamps event times
	stamped time.Time        // the last stamp; the writer's alone
}

// Option configures a Store at Open.
type Option func(*Store)

// WithClock sets the clock that stamps event times (proto.Event.Time); the
// default is time.Now.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

var (
	_ mem.Store       = (*Store)(nil)
	_ mem.EventPruner = (*Store)(nil)
)

// A request is a commit set, or a prune when prune is set: the writer runs
// a prune in a transaction of its own, between batches of commits.
type request struct {
	c     mem.CommitSet
	prune *pruneReq
	evs   []proto.Event
	err   error
	ok    chan struct{}
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
//
// Every event is stamped with its commit time (proto.Event.Time), stored as
// Unix nanoseconds. A database written before events had times gets the
// column at Open; its old events read back with a zero time. One written
// before event retention gets the events_task index (task_id, seq) that
// PruneEvents reads at Open, built once: on a large log the first Open takes
// that long.
func Open(path string, opts ...Option) (*Store, error) {
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
	s := &Store{db: db, lock: lock, reqs: make(chan *request, maxBatch), done: make(chan struct{}), now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if s.stamped, err = eventTimes(db); err != nil {
		db.Close()
		return fail(fmt.Errorf("sqlite: event times: %w", err))
	}
	if err := interruptSenders(db); err != nil {
		db.Close()
		return fail(fmt.Errorf("sqlite: interrupt senders: %w", err))
	}
	go s.writer()
	return s, nil
}

// eventTimes adds the events' time column to a database created before it
// existed, and returns the latest time stored, which new stamps never go
// below.
func eventTimes(db *sql.DB) (time.Time, error) {
	var has int
	if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('events') WHERE name = 'time'").Scan(&has); err != nil {
		return time.Time{}, err
	}
	if has == 0 {
		if _, err := db.Exec("ALTER TABLE events ADD COLUMN time INTEGER"); err != nil {
			return time.Time{}, err
		}
	}
	var last sql.NullInt64
	if err := db.QueryRow("SELECT MAX(time) FROM events").Scan(&last); err != nil {
		return time.Time{}, err
	}
	if !last.Valid {
		return time.Time{}, nil
	}
	return time.Unix(0, last.Int64).UTC(), nil
}

// interruptSenders adds the interrupts' sender and idem_key columns
// (mem.Interrupt's From and Key) to a database created before they existed;
// its old interrupts read back with both empty.
func interruptSenders(db *sql.DB) error {
	for _, col := range []string{"sender", "idem_key"} {
		var has int
		if err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('interrupts') WHERE name = ?", col).Scan(&has); err != nil {
			return err
		}
		if has == 0 {
			if _, err := db.Exec("ALTER TABLE interrupts ADD COLUMN " + col + " TEXT NOT NULL DEFAULT ''"); err != nil {
				return err
			}
		}
	}
	return nil
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
		if first.prune != nil {
			s.pruneEvents(first)
			continue
		}
		batch := []*request{first}
		var prune *request // queued behind the batch: it runs after it
	drain:
		for len(batch) < maxBatch {
			select {
			case r, ok := <-s.reqs:
				if !ok {
					break drain
				}
				if r.prune != nil {
					prune = r
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		s.write(batch)
		if prune != nil {
			s.pruneEvents(prune)
		}
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
		evs, err := s.apply(ctx, tx, r.c)
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
func (s *Store) apply(ctx context.Context, tx *sql.Tx, c mem.CommitSet) ([]proto.Event, error) {
	if c.NewTask && c.Task == nil {
		return nil, errors.New("mem: NewTask requires Task")
	}
	if len(c.Applied) > 0 && c.Task == nil {
		return nil, errors.New("mem: Applied requires Task")
	}
	if len(c.Archive) > 0 && c.Task == nil {
		return nil, errors.New("mem: Archive requires Task")
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
	if t := c.Task; t != nil && !c.NewTask {
		in, ok, err := pendingCancel(ctx, tx, t.ID)
		if err != nil {
			return nil, err
		}
		if ok && !(t.State.Terminal() && t.StopReason == mem.CancelReason(in)) {
			return nil, fmt.Errorf("%w: %s", mem.ErrCancelPending, t.ID)
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
		if r.Status == mem.CallDispatched {
			if _, ok, err := pendingCancel(ctx, tx, r.TaskID); err != nil {
				return nil, err
			} else if ok {
				return nil, fmt.Errorf("%w: %s: call %s cannot be dispatched", mem.ErrCancelPending, r.TaskID, r.CallID)
			}
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
	for i, msg := range c.Archive {
		blob, err := encode(msg)
		if err != nil {
			return nil, fmt.Errorf("sqlite: encode archived message %d of %s: %w", i, c.Task.ID, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO archive (task_id, msg) VALUES (?, ?)", c.Task.ID, blob); err != nil {
			return nil, err
		}
	}
	for _, in := range c.NewInterrupts {
		if _, err := tx.ExecContext(ctx, "INSERT INTO interrupts (task_id, kind, text, sender, idem_key, applied) VALUES (?, ?, ?, ?, ?, 0)",
			in.TaskID, in.Kind, in.Text, in.From, in.Key); err != nil {
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
					events = append(events, proto.NewEvent(proto.EvSteerDropped, t.ID, mem.SteerEventData(in)))
				}
			}
		}
	}
	if len(events) > 0 {
		s.stamped = mem.CommitTime(s.now(), s.stamped)
	}
	for i := range events {
		events[i].Time = s.stamped
		events[i].Data = bytes.Clone(events[i].Data)
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO task_seqs (task_id, n) VALUES (?, 1)
			 ON CONFLICT (task_id) DO UPDATE SET n = n + 1 RETURNING n`, events[i].TaskID).Scan(&events[i].TaskSeq); err != nil {
			return nil, err
		}
		res, err := tx.ExecContext(ctx, "INSERT INTO events (task_id, task_seq, kind, data, time) VALUES (?, ?, ?, ?, ?)",
			events[i].TaskID, events[i].TaskSeq, events[i].Kind, []byte(events[i].Data), events[i].Time.UnixNano())
		if err != nil {
			return nil, err
		}
		if events[i].Seq, err = res.LastInsertId(); err != nil {
			return nil, err
		}
	}
	return events, nil
}

// pendingCancel returns the first unapplied cancel interrupt of a task, as
// of this point in the transaction: a cancel committed earlier in the same
// group commit counts.
func pendingCancel(ctx context.Context, tx *sql.Tx, id string) (mem.Interrupt, bool, error) {
	in := mem.Interrupt{TaskID: id, Kind: mem.InterruptCancel}
	err := tx.QueryRowContext(ctx,
		"SELECT seq, text FROM interrupts WHERE task_id = ? AND applied = 0 AND kind = ? ORDER BY seq LIMIT 1",
		id, mem.InterruptCancel).Scan(&in.Seq, &in.Text)
	if errors.Is(err, sql.ErrNoRows) {
		return mem.Interrupt{}, false, nil
	}
	return in, err == nil, err
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
	return interrupts(ctx, q, "SELECT seq, task_id, kind, text, sender, idem_key, applied FROM interrupts WHERE task_id = ? AND applied = 0 ORDER BY seq", taskID)
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
		if err := rows.Scan(&in.Seq, &in.TaskID, &in.Kind, &in.Text, &in.From, &in.Key, &in.Applied); err != nil {
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
	return interrupts(ctx, s.db, "SELECT seq, task_id, kind, text, sender, idem_key, applied FROM interrupts WHERE task_id = ? ORDER BY seq", taskID)
}

func (s *Store) Events(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error) {
	if limit <= 0 {
		limit = -1 // SQLite: no limit
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, task_id, task_seq, kind, data, time FROM events WHERE seq > ? ORDER BY seq LIMIT ?", afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []proto.Event
	for rows.Next() {
		var e proto.Event
		var data []byte
		var at sql.NullInt64
		if err := rows.Scan(&e.Seq, &e.TaskID, &e.TaskSeq, &e.Kind, &data, &at); err != nil {
			return nil, err
		}
		if at.Valid {
			e.Time = time.Unix(0, at.Int64).UTC()
		}
		if len(data) > 0 {
			e.Data = data
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Archive(ctx context.Context, taskID string) ([]task.Message, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT seq, msg FROM archive WHERE task_id = ? ORDER BY seq", taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []task.Message
	for rows.Next() {
		var seq int64
		var blob []byte
		if err := rows.Scan(&seq, &blob); err != nil {
			return nil, err
		}
		var m task.Message
		if err := decode(blob, &m); err != nil {
			return nil, fmt.Errorf("sqlite: decode archived message %d of %s: %w", seq, taskID, err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

type pruneReq struct {
	before time.Time
	limit  int
	n      int
}

// PruneEvents deletes the events mem.EventPruner allows, oldest first. It
// runs on the writer, in a transaction of its own between two batches of
// commits, so it never contends with them for the write lock; commits that
// arrive meanwhile wait for it and go into the next batch, which is why the
// caller keeps limit small. Like Commit, once queued it is not abandoned
// when ctx ends.
//
// The space deleted events took stays in the file and is reused by the
// events committed after them: with retention the file stops growing at
// what MaxAge worth of events takes, but does not shrink. VACUUM, run
// while no machine has the file open, gives the space back.
func (s *Store) PruneEvents(ctx context.Context, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r := &request{prune: &pruneReq{before: before, limit: limit}, ok: make(chan struct{})}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrClosed
	}
	s.reqs <- r
	s.mu.RUnlock()
	<-r.ok
	return r.prune.n, r.err
}

func (s *Store) pruneEvents(r *request) {
	defer close(r.ok)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		r.err = fmt.Errorf("sqlite: begin prune: %w", err)
		return
	}
	n, err := prune(ctx, tx, r.prune.before, r.prune.limit)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		tx.Rollback()
		r.err = fmt.Errorf("sqlite: prune events: %w", err)
		return
	}
	r.prune.n = n
}

// pruneChunk bounds the seqs in one DELETE statement, below SQLite's limit
// on bound parameters.
const pruneChunk = 500

func prune(ctx context.Context, tx *sql.Tx, before time.Time, limit int) (int, error) {
	var top sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(seq) FROM events").Scan(&top); err != nil || !top.Valid {
		return 0, err
	}
	cut, err := oldThrough(ctx, tx, before.UnixNano(), top.Int64)
	if err != nil {
		return 0, err
	}
	// Event times never decrease with seq, so the events that are old and
	// not the newest are those with seq <= bound: the scan stops there on
	// the primary key, and a task's newest event is one lookup in
	// events_task.
	bound := min(cut, top.Int64-1)
	if bound <= 0 {
		return 0, nil
	}
	if limit <= 0 {
		limit = -1 // SQLite: no limit
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT e.seq FROM events e
		WHERE e.seq <= ?1 AND (e.task_id = '' OR (
			NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = e.task_id AND t.terminal = 0)
			AND (SELECT MAX(o.seq) FROM events o WHERE o.task_id = e.task_id) <= ?1))
		ORDER BY e.seq LIMIT ?2`, bound, limit)
	if err != nil {
		return 0, err
	}
	var gone []any
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return 0, err
		}
		gone = append(gone, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(gone) == 0 {
		return 0, err
	}
	for chunk := range slices.Chunk(gone, pruneChunk) {
		q := "DELETE FROM events WHERE seq IN (?" + strings.Repeat(",?", len(chunk)-1) + ")"
		if _, err := tx.ExecContext(ctx, q, chunk...); err != nil {
			return 0, err
		}
	}
	return len(gone), nil
}

// oldThrough is the highest seq whose event is stamped before before (Unix
// nanoseconds), 0 if there is none. Events read back before they had times
// count as older than any. Times never decrease with seq, so it is a binary
// search on the primary key, a few dozen point reads, where a scan for it
// would read every old event and an index on time would cost every commit.
func oldThrough(ctx context.Context, tx *sql.Tx, before, top int64) (int64, error) {
	// at(n): the first event with seq >= n, and whether it is old.
	at := func(n int64) (int64, bool, error) {
		var seq, t int64
		err := tx.QueryRowContext(ctx, "SELECT seq, COALESCE(time, 0) FROM events WHERE seq >= ? ORDER BY seq LIMIT 1", n).Scan(&seq, &t)
		return seq, t < before, err
	}
	first, old, err := at(0)
	if err != nil || !old {
		return 0, err
	}
	// Invariant: the first event at or after lo is old (its seq is found);
	// the first at or after hi is not, or hi is past top.
	found, lo, hi := first, first, top+1
	for lo+1 < hi {
		mid := lo + (hi-lo)/2
		seq, old, err := at(mid)
		if err != nil {
			return 0, err
		}
		if old {
			found, lo = seq, seq
		} else {
			hi = mid
		}
	}
	return found, nil
}
