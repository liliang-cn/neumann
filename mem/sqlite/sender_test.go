package sqlite

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// A database written before steers had senders gains the columns on Open:
// its old interrupts read back with no sender and no key, are applied and
// dropped as before, and new ones keep theirs.
func TestOpenAddsTheSenderColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatal(err)
	}
	blob, err := encode(&task.TCB{ID: "a", State: task.Waiting, Wait: task.WaitIO, Class: "default", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE tasks (
		id       TEXT PRIMARY KEY,
		version  INTEGER NOT NULL,
		terminal INTEGER NOT NULL,
		tcb      BLOB NOT NULL
	);
	CREATE TABLE interrupts (
		seq     INTEGER PRIMARY KEY,
		task_id TEXT NOT NULL,
		kind    TEXT NOT NULL,
		text    TEXT NOT NULL,
		applied INTEGER NOT NULL
	);
	INSERT INTO interrupts (task_id, kind, text, applied) VALUES ('a', 'steer', 'old one', 0), ('a', 'steer', 'old two', 0);`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO tasks (id, version, terminal, tcb) VALUES ('a', 1, 0, ?)", blob); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s := open(t, path)
	defer s.Close()
	got, err := s.Interrupts(ctx, "a", true)
	if err != nil {
		t.Fatal(err)
	}
	want := []mem.Interrupt{
		{TaskID: "a", Seq: 1, Kind: mem.InterruptSteer, Text: "old one"},
		{TaskID: "a", Seq: 2, Kind: mem.InterruptSteer, Text: "old two"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("old interrupts:\n got %+v\nwant %+v", got, want)
	}
	if _, err := s.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "a", Kind: mem.InterruptSteer, Text: "new", From: "task:b", Key: "b/c-1/1"}}}); err != nil {
		t.Fatal(err)
	}
	tc, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	tc.State, tc.Version = task.Done, 2
	evs, err := s.Commit(ctx, mem.CommitSet{Task: tc, Applied: []int64{1}, Events: []proto.Event{proto.NewEvent(proto.EvDone, "a", nil)}})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind+" "+string(ev.Data))
	}
	wantEvs := []string{
		proto.EvDone + " ",
		proto.EvSteerDropped + ` {"seq":2,"text":"old two"}`,
		proto.EvSteerDropped + ` {"from":"task:b","seq":3,"text":"new"}`,
	}
	if !reflect.DeepEqual(kinds, wantEvs) {
		t.Fatalf("events:\n got %q\nwant %q", kinds, wantEvs)
	}
	all, _ := s.Interrupts(ctx, "a", false)
	if len(all) != 3 || all[2].From != "task:b" || all[2].Key != "b/c-1/1" || !all[2].Applied {
		t.Fatalf("interrupts %+v", all)
	}
}
