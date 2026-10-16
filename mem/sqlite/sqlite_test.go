package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/storetest"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

var ctx = context.Background()

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) mem.Store {
		s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
		t.Cleanup(func() { s.Close() })
		return s
	})
}

func TestEverythingSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neumann.db")
	s := open(t, path)
	tc := &task.TCB{
		ID: "a", State: task.Waiting, Wait: task.WaitIO, PC: task.StepTools, Class: "default", Version: 1, CallSeq: 1,
		Regs: []task.Message{
			task.Text(task.RoleUser, "q"),
			// A model may emit arguments that are not valid JSON; they must
			// survive byte for byte.
			{Role: task.RoleAssistant, Parts: []task.Part{{Kind: task.PartToolCall, CallID: "c-1", Name: "x", Args: json.RawMessage(`{"a":`)}}},
		},
	}
	if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, NewTask: true,
		Calls:  []mem.CallRecord{{TaskID: "a", CallID: "c-1", Index: 1, Tool: "x", Args: json.RawMessage(`{"a":`), Status: mem.CallDispatched, Attempt: 1, Key: "a/c-1/1", Effect: task.ReadOnly}},
		Events: []proto.Event{proto.NewEvent(proto.EvAccepted, "a", map[string]int{"n": 1})},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "a", Kind: mem.InterruptSteer, Text: "s"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	defer s.Close()
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != task.Waiting || got.Wait != task.WaitIO || got.CallSeq != 1 || string(got.Regs[1].Parts[0].Args) != `{"a":` {
		t.Fatalf("task %+v", got)
	}
	calls, _ := s.Calls(ctx, "a")
	if len(calls) != 1 || calls[0].Status != mem.CallDispatched || calls[0].Key != "a/c-1/1" || calls[0].Effect != task.ReadOnly || string(calls[0].Args) != `{"a":` {
		t.Fatalf("calls %+v", calls)
	}
	ints, _ := s.Interrupts(ctx, "a", true)
	if len(ints) != 1 || ints[0].Text != "s" {
		t.Fatalf("interrupts %+v", ints)
	}
	un, _ := s.Unfinished(ctx)
	if len(un) != 1 || un[0].ID != "a" {
		t.Fatalf("unfinished %+v", un)
	}
	// Sequence numbers continue where they stopped.
	evs, err := s.Commit(ctx, mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvStep, "a", nil)}})
	if err != nil {
		t.Fatal(err)
	}
	if evs[0].Seq != 2 || evs[0].TaskSeq != 2 {
		t.Fatalf("after reopen: %+v", evs)
	}
	old, _ := s.Events(ctx, 0, 0)
	if len(old) != 2 || string(old[0].Data) != `{"n":1}` {
		t.Fatalf("events %+v", old)
	}
}

func TestConcurrentCommitsAreGroupedAndCorrect(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	defer s.Close()
	const tasks, steps = 20, 10
	var wg sync.WaitGroup
	errs := make(chan error, tasks)
	for i := range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("t%d", i)
			tc := &task.TCB{ID: id, State: task.Ready, Class: "default", Version: 1}
			if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, NewTask: true}); err != nil {
				errs <- err
				return
			}
			for range steps {
				tc.Version++
				if _, err := s.Commit(ctx, mem.CommitSet{Task: tc, Events: []proto.Event{proto.NewEvent(proto.EvStep, id, nil)}}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	evs, _ := s.Events(ctx, 0, 0)
	if len(evs) != tasks*steps {
		t.Fatalf("%d events", len(evs))
	}
	for i, ev := range evs {
		if ev.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d", i, ev.Seq)
		}
	}
	for i := range tasks {
		got, _ := s.Load(ctx, fmt.Sprintf("t%d", i))
		if got.Version != steps+1 {
			t.Fatalf("t%d at version %d", i, got.Version)
		}
	}
}

func TestOneBadCommitDoesNotSinkItsBatch(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	defer s.Close()
	var wg sync.WaitGroup
	var bad, good int
	var mu sync.Mutex
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := mem.CommitSet{Task: &task.TCB{ID: fmt.Sprintf("t%d", i), State: task.Ready, Version: 1}, NewTask: true}
			if i%5 == 0 {
				c.Task.Version = 7 // invalid: rejected on its own
			}
			_, err := s.Commit(ctx, c)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				good++
			case errors.Is(err, mem.ErrVersion):
				bad++
			default:
				t.Errorf("unexpected %v", err)
			}
		}()
	}
	wg.Wait()
	if good != 40 || bad != 10 {
		t.Fatalf("good %d bad %d", good, bad)
	}
}

func TestCommitAfterClose(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	s.Close()
	if _, err := s.Commit(ctx, mem.CommitSet{Task: &task.TCB{ID: "a", Version: 1}, NewTask: true}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// A TCB written before a field existed (here Rejections, and Feedback on
// its messages) decodes as before, the new fields zero, and is committed
// over as usual: gob matches fields by name.
func TestOldTaskBlobsDecode(t *testing.T) {
	type oldMessage struct {
		Role    task.Role
		Parts   []task.Part
		Summary bool
	}
	type oldTCB struct {
		ID      string
		State   task.State
		Class   string
		Regs    []oldMessage
		Used    task.Usage
		Version int64
	}
	old := oldTCB{ID: "a", State: task.Ready, Class: "default", Version: 3, Used: task.Usage{Steps: 2},
		Regs: []oldMessage{{Role: task.RoleUser, Parts: []task.Part{{Kind: task.PartText, Text: "q"}}}}}
	blob, err := encode(old)
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, filepath.Join(t.TempDir(), "neumann.db"))
	defer s.Close()
	if _, err := s.db.Exec(`INSERT INTO tasks (id, version, terminal, tcb) VALUES ('a', 3, 0, ?)`, blob); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "a" || got.Version != 3 || got.Used.Steps != 2 || got.Rejections != 0 || got.Partial != "" || got.PartialThink != "" || got.Preempts != 0 || got.NoThink || len(got.Regs) != 1 || got.Regs[0].TextContent() != "q" || got.Regs[0].Feedback {
		t.Fatalf("old blob decoded as %+v", got)
	}
	got.Rejections, got.Partial, got.Preempts, got.NoThink, got.Version = 1, "Once", 1, true, 4
	got.Regs = append(got.Regs, task.FeedbackMessage("again"))
	if _, err := s.Commit(ctx, mem.CommitSet{Task: got}); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Load(ctx, "a"); err != nil || again.Rejections != 1 || again.Partial != "Once" || again.Preempts != 1 || !again.NoThink || !again.Regs[1].Feedback {
		t.Fatalf("after a commit over it: %+v, %v", again, err)
	}
}
