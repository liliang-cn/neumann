package mem_test

import (
	"context"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
)

// The Memory store stamps events with its Now, in UTC, and never moves
// backwards when the clock does.
func TestMemoryStampsEventsWithItsClock(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 10, 12, 0, 0, 123456789, time.FixedZone("CEST", 2*3600))
	s := mem.NewMemory()
	s.Now = func() time.Time { return now }
	commit := func() proto.Event {
		t.Helper()
		evs, err := s.Commit(ctx, mem.CommitSet{Events: []proto.Event{proto.NewEvent(proto.EvCoreUp, "", nil)}})
		if err != nil {
			t.Fatal(err)
		}
		return evs[0]
	}
	if got := commit().Time; got != now.UTC() {
		t.Fatalf("stamped %v, want %v", got, now.UTC())
	}
	later := now.Add(1500 * time.Millisecond)
	now = later
	if got := commit().Time; got != later.UTC() {
		t.Fatalf("stamped %v, want %v", got, later.UTC())
	}
	now = later.Add(-time.Hour) // the wall clock steps back
	if got := commit().Time; got != later.UTC() {
		t.Fatalf("after the clock stepped back: stamped %v, want %v", got, later.UTC())
	}
	stored, _ := s.Events(ctx, 0, 0)
	if stored[2].Time != later.UTC() {
		t.Fatalf("stored %v", stored[2].Time)
	}
}
