package sqlite

import (
	"database/sql"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

func seqsOf(evs []proto.Event) []int64 {
	out := make([]int64, len(evs))
	for i, e := range evs {
		out[i] = e.Seq
	}
	return out
}

// What was pruned stays pruned across a reopen, and seqs go on from the
// newest event, never reusing a pruned one.
func TestPruneSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	var now atomic.Int64
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	now.Store(t0.UnixNano())
	clock := WithClock(func() time.Time { return time.Unix(0, now.Load()) })
	s, err := Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := s.Commit(ctx, coreUp()); err != nil {
			t.Fatal(err)
		}
	}
	now.Store(t0.Add(time.Hour).UnixNano())
	if _, err := s.Commit(ctx, coreUp()); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PruneEvents(ctx, t0.Add(time.Minute), 0); err != nil || n != 6 {
		t.Fatalf("pruned %d, %v; want 6", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	evs, err := s.Commit(ctx, coreUp())
	if err != nil {
		t.Fatal(err)
	}
	if evs[0].Seq != 9 || evs[0].TaskSeq != 9 {
		t.Fatalf("after reopen committed seq %d task seq %d, want 9, 9", evs[0].Seq, evs[0].TaskSeq)
	}
	all, _ := s.Events(ctx, 0, 0)
	if !slices.Equal(seqsOf(all), []int64{7, 8, 9, 10}) {
		t.Fatalf("seqs %v, want [7 8 9 10]", seqsOf(all))
	}
}

// Events written before events had times count as older than any.
func TestPruneUntimedEvents(t *testing.T) {
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
	INSERT INTO events (task_id, task_seq, kind, data) VALUES ('', 1, 'core_up', NULL), ('', 2, 'core_up', NULL);
	INSERT INTO task_seqs (task_id, n) VALUES ('', 2);`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	s, err := Open(path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Commit(ctx, coreUp()); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PruneEvents(ctx, now, 0); err != nil || n != 2 {
		t.Fatalf("pruned %d, %v; want the 2 untimed events", n, err)
	}
	all, _ := s.Events(ctx, 0, 0)
	if !slices.Equal(seqsOf(all), []int64{3, 4}) {
		t.Fatalf("seqs %v, want [3 4]", seqsOf(all))
	}
}

// Pruning in small steps while commits keep arriving fails no commit and
// takes exactly the eligible events: the old machine-level ones and the
// finished task's, never the unfinished task's.
func TestPruneAlongsideCommits(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	defer s.Close()
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var now atomic.Int64
	now.Store(t0.UnixNano())
	s.now = func() time.Time { return time.Unix(0, now.Load()) }

	run := &task.TCB{ID: "run", State: task.Ready, Class: "default", Version: 1}
	done := &task.TCB{ID: "done", State: task.Ready, Class: "default", Version: 1}
	for _, tc := range []*task.TCB{run, done} {
		if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, NewTask: true, Events: []proto.Event{proto.NewEvent(proto.EvAccepted, tc.ID, nil)}}); err != nil {
			t.Fatal(err)
		}
	}
	const old = 300
	for i := range old {
		id := []string{"", "run", "done"}[i%3]
		if _, err := s.Commit(ctx, mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvStep, id, nil)}}); err != nil {
			t.Fatal(err)
		}
	}
	end := done.Clone()
	end.Version, end.State = 2, task.Done
	if _, err := s.Commit(ctx, mem.CommitSet{Task: end, Events: []proto.Event{proto.NewEvent(proto.EvDone, "done", nil)}}); err != nil {
		t.Fatal(err)
	}
	now.Store(t0.Add(time.Hour).UnixNano())

	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	for w := range 4 {
		wg.Go(func() {
			for i := range 50 {
				id := []string{"", "run"}[(w+i)%2]
				if _, err := s.Commit(ctx, mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvStep, id, nil)}}); err != nil {
					errs <- err
				}
			}
		})
	}
	pruned := 0
	for {
		n, err := s.PruneEvents(ctx, t0.Add(time.Minute), 7)
		if err != nil {
			t.Fatal(err)
		}
		pruned += n
		if n < 7 {
			break
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("commit during prune: %v", err)
	}
	// Old: 100 machine-level steps, and done's accepted, 100 steps and done.
	if want := 100 + 102; pruned != want {
		t.Fatalf("pruned %d, want %d", pruned, want)
	}
	all, _ := s.Events(ctx, 0, 0)
	for _, e := range all {
		if e.Time.Equal(t0) && e.TaskID != "run" {
			t.Fatalf("old event %+v of %q kept", e, e.TaskID)
		}
	}
	if len(all) != 2+old+1+200-pruned {
		t.Fatalf("%d events left, want %d", len(all), 2+old+1+200-pruned)
	}
}
