package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
)

func coreUp() mem.CommitSet {
	return mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvCoreUp, "", nil), proto.NewEvent(proto.EvCoreUp, "", nil)}}
}

// Times are stamped by the store's clock, kept to the nanosecond, survive a
// reopen, and never go back, not even across a reopen with a clock behind.
func TestEventTimesWithAClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	now := time.Date(2026, 10, 10, 12, 0, 0, 987654321, time.FixedZone("CEST", 2*3600))
	s, err := Open(path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	evs, err := s.Commit(ctx, coreUp())
	if err != nil {
		t.Fatal(err)
	}
	if evs[0].Time != now.UTC() || evs[1].Time != now.UTC() {
		t.Fatalf("stamped %v, %v; want %v", evs[0].Time, evs[1].Time, now.UTC())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	behind := now.Add(-time.Minute)
	s, err = Open(path, WithClock(func() time.Time { return behind }))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stored, err := s.Events(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stored[0].Time != now.UTC() {
		t.Fatalf("after reopen: %v, want %v", stored[0].Time, now.UTC())
	}
	evs, err = s.Commit(ctx, coreUp())
	if err != nil {
		t.Fatal(err)
	}
	if evs[0].Time != now.UTC() {
		t.Fatalf("with the clock behind the log: stamped %v, want %v", evs[0].Time, now.UTC())
	}
}

// A database written before events had times gains the column on Open; its
// old events read back with a zero time, new ones are stamped.
func TestOpenAddsTheTimeColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE events (
		seq      INTEGER PRIMARY KEY,
		task_id  TEXT NOT NULL,
		task_seq INTEGER NOT NULL,
		kind     TEXT NOT NULL,
		data     BLOB
	);
	CREATE TABLE task_seqs (task_id TEXT PRIMARY KEY, n INTEGER NOT NULL);
	INSERT INTO events (task_id, task_seq, kind, data) VALUES ('', 1, 'core_up', NULL);
	INSERT INTO task_seqs (task_id, n) VALUES ('', 1);`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	s, err := Open(path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Commit(ctx, coreUp()); err != nil {
		t.Fatal(err)
	}
	evs, err := s.Events(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || !evs[0].Time.IsZero() || evs[1].Time != now || evs[2].Time != now {
		t.Fatalf("events %+v", evs)
	}
}
