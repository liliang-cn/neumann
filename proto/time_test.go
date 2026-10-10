package proto

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// An event's time goes over the wire to the nanosecond and back; an unknown
// (zero) time is left out rather than sent as year 1.
func TestEventTimeOnTheWire(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 123456789, time.UTC)
	b, err := json.Marshal(Event{Kind: EvStep, Seq: 1, TaskSeq: 1, Time: at})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"time":"2026-10-10T12:00:00.123456789Z"`) {
		t.Fatalf("marshalled %s", b)
	}
	var back Event
	if err := json.Unmarshal(b, &back); err != nil || !back.Time.Equal(at) {
		t.Fatalf("back %v, %v", back.Time, err)
	}
	if b, _ := json.Marshal(Event{Kind: EvStep}); strings.Contains(string(b), "time") {
		t.Fatalf("zero time marshalled: %s", b)
	}
}
