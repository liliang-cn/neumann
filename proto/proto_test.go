package proto

import "testing"

func TestNewEventMarshalsData(t *testing.T) {
	ev := NewEvent(EvAnswer, "t1", map[string]string{"text": "hi"})
	if ev.Kind != EvAnswer || ev.TaskID != "t1" || string(ev.Data) != `{"text":"hi"}` {
		t.Fatalf("event = %+v data=%s", ev, ev.Data)
	}
	if ev.Seq != 0 || ev.TaskSeq != 0 {
		t.Fatalf("sequence numbers are the store's to assign, got %d/%d", ev.Seq, ev.TaskSeq)
	}
	if NewEvent(EvStep, "t1", nil).Data != nil {
		t.Fatalf("nil data should stay nil")
	}
}

func TestTerminalEvents(t *testing.T) {
	for kind, want := range map[string]bool{
		EvDone: true, EvFailed: true, EvCancelled: true,
		EvAccepted: false, EvAnswer: false, EvSteerDropped: false,
	} {
		if (Event{Kind: kind}).Terminal() != want {
			t.Errorf("%s: Terminal() != %v", kind, want)
		}
	}
}
