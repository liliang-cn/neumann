package jsonl

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

func decodeOK(t *testing.T, line string) Request {
	t.Helper()
	req, err := Decode([]byte(line))
	if err != nil {
		t.Fatalf("Decode(%s): %v", line, err)
	}
	return req
}

func decodeErr(t *testing.T, line, code string) *Error {
	t.Helper()
	_, err := Decode([]byte(line))
	if err == nil {
		t.Fatalf("Decode(%s): no error, want %s", line, code)
	}
	if err.Code != code {
		t.Fatalf("Decode(%s): code %q (%s), want %q", line, err.Code, err.Message, code)
	}
	if err.Message == "" {
		t.Fatalf("Decode(%s): an error must say what is wrong", line)
	}
	return err
}

func TestRequestRoundTrip(t *testing.T) {
	deadline := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	from := int64(42)
	reqs := []Request{
		{ID: json.RawMessage(`"a-1"`), Instruction: proto.Instruction{
			Kind: proto.KindSubmit, TaskID: "t1", Input: "hello", Class: "default", Priority: 1,
			Budget: &task.Budget{MaxSteps: 3, Deadline: deadline}, Allow: []string{"util_*"}, Deny: []string{"util_rm"},
		}},
		{ID: json.RawMessage(`7`), Instruction: proto.Instruction{Kind: proto.KindSteer, TaskID: "t1", Input: "also this"}},
		{Instruction: proto.Instruction{Kind: proto.KindCancel, TaskID: "t1"}},
		{Instruction: proto.Instruction{Kind: KindResolve, TaskID: "t1"}, CallID: "c-1", Outcome: &Outcome{Result: "paid"}},
		{Instruction: proto.Instruction{Kind: KindResolve, TaskID: "t1"}, CallID: "c-1", Outcome: &Outcome{Result: "boom", IsError: true}},
		{Instruction: proto.Instruction{Kind: KindResolve, TaskID: "t1"}, CallID: "c-1", Outcome: &Outcome{Retry: true}},
		{Instruction: proto.Instruction{Kind: KindApprove, TaskID: "t1"}, CallID: "c-1", Decision: &Decision{Verdict: "allow"}},
		{Instruction: proto.Instruction{Kind: KindApprove, TaskID: "t1"}, CallID: "c-1", Decision: &Decision{Verdict: "deny", Reason: "no"}},
		{Instruction: proto.Instruction{Kind: KindApprove, TaskID: "t1"}, CallID: "c-1", Decision: &Decision{Verdict: "edit", Args: json.RawMessage(`{"amount":5}`)}},
		{Instruction: proto.Instruction{Kind: KindStatus}},
		{Instruction: proto.Instruction{Kind: KindSubscribe}, FromSeq: &from},
		{Instruction: proto.Instruction{Kind: KindUnsubscribe}},
	}
	for _, want := range reqs {
		line, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		got := decodeOK(t, string(line))
		again, _ := json.Marshal(got)
		if string(again) != string(line) {
			t.Errorf("round trip changed the request:\n sent %s\n back %s", line, again)
		}
	}
}

func TestDecodeKeepsTheID(t *testing.T) {
	for _, id := range []string{`"x"`, `12`, `-1.5`, `"with \"quotes\""`} {
		req := decodeOK(t, `{"id":`+id+`,"kind":"status"}`)
		if string(req.ID) != id {
			t.Errorf("id %s came back as %s", id, req.ID)
		}
	}
	if req := decodeOK(t, `{"id":null,"kind":"status"}`); req.ID != nil {
		t.Errorf("a null id is no id, got %s", req.ID)
	}
	e := decodeErr(t, `{"id":{"a":1},"kind":"status"}`, CodeBadRequest)
	if !strings.Contains(e.Message, "id") {
		t.Errorf("message %q does not name the id", e.Message)
	}
}

// An error found after the id was read still carries it, so the client can
// match the reply to its request.
func TestErrorsEchoTheID(t *testing.T) {
	req, err := Decode([]byte(`{"id":"r9","kind":"steer","task_id":"t1"}`))
	if err == nil {
		t.Fatal("steer without input accepted")
	}
	if string(req.ID) != `"r9"` || req.Kind != proto.KindSteer {
		t.Fatalf("partial request %+v lost the id or kind", req)
	}
	reply := ErrorReply(req, err)
	b, _ := Marshal(reply)
	if !strings.Contains(string(b), `"id":"r9"`) || !strings.Contains(string(b), `"ok":false`) {
		t.Fatalf("reply %s", b)
	}
}

func TestMalformedLine(t *testing.T) {
	for _, line := range []string{`{"kind":"status"`, `not json`, `{"kind":"status"} trailing`, `{"kind": 'x'}`} {
		e := decodeErr(t, line, CodeBadRequest)
		if !strings.Contains(e.Message, "JSON") {
			t.Errorf("%s: message %q should say the JSON is malformed", line, e.Message)
		}
	}
	for _, line := range []string{`[1,2]`, `"submit"`, `3`, `null`} {
		e := decodeErr(t, line, CodeBadRequest)
		if !strings.Contains(e.Message, "object") {
			t.Errorf("%s: message %q should ask for an object", line, e.Message)
		}
	}
}

func TestKinds(t *testing.T) {
	decodeErr(t, `{"input":"hi"}`, CodeBadRequest)
	decodeErr(t, `{"kind":7}`, CodeBadRequest)
	e := decodeErr(t, `{"kind":"launch"}`, CodeUnknownKind)
	if !strings.Contains(e.Message, "launch") || !strings.Contains(e.Message, "submit") {
		t.Errorf("unknown-kind message %q should name the kind and list the known ones", e.Message)
	}
	for _, k := range UnsupportedKinds {
		req, err := Decode([]byte(`{"id":1,"kind":"` + k + `","task_id":"t1"}`))
		if err == nil || err.Code != CodeUnsupported {
			t.Fatalf("%s: %v, want %s", k, err, CodeUnsupported)
		}
		if req.Kind != k || string(req.ID) != "1" {
			t.Fatalf("%s: partial request %+v", k, req)
		}
	}
}

func TestFieldValidationPerKind(t *testing.T) {
	cases := []struct {
		line, code, mention string
	}{
		// submit
		{`{"kind":"submit"}`, CodeBadRequest, "input"},
		{`{"kind":"submit","input":"   "}`, CodeBadRequest, "input"},
		{`{"kind":"submit","input":"hi","call_id":"c"}`, CodeBadRequest, "call_id"},
		{`{"kind":"submit","input":"hi","priority":"high"}`, CodeBadRequest, "priority"},
		{`{"kind":"submit","input":"hi","budget":{"deadline":"tomorrow"}}`, CodeBadRequest, "budget"},
		{`{"kind":"submit","input":"hi","inptu":"x"}`, CodeBadRequest, "inptu"},
		// steer
		{`{"kind":"steer","input":"x"}`, CodeBadRequest, "task_id"},
		{`{"kind":"steer","task_id":"t1"}`, CodeBadRequest, "input"},
		{`{"kind":"steer","task_id":"t1","input":"x","class":"big"}`, CodeBadRequest, "class"},
		// cancel
		{`{"kind":"cancel"}`, CodeBadRequest, "task_id"},
		{`{"kind":"cancel","task_id":"t1","input":"why"}`, CodeBadRequest, "input"},
		// resolve
		{`{"kind":"resolve","call_id":"c","outcome":{"result":"x"}}`, CodeBadRequest, "task_id"},
		{`{"kind":"resolve","task_id":"t1","outcome":{"result":"x"}}`, CodeBadRequest, "call_id"},
		{`{"kind":"resolve","task_id":"t1","call_id":"c"}`, CodeBadRequest, "outcome"},
		{`{"kind":"resolve","task_id":"t1","call_id":"c","outcome":{"retry":true,"result":"x"}}`, CodeBadRequest, "retry"},
		{`{"kind":"resolve","task_id":"t1","call_id":"c","outcome":{"result":"x","bogus":1}}`, CodeBadRequest, "bogus"},
		// approve
		{`{"kind":"approve","call_id":"c","decision":{"verdict":"allow"}}`, CodeBadRequest, "task_id"},
		{`{"kind":"approve","task_id":"t1","decision":{"verdict":"allow"}}`, CodeBadRequest, "call_id"},
		{`{"kind":"approve","task_id":"t1","call_id":"c"}`, CodeBadRequest, "decision"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{}}`, CodeBadRequest, "verdict"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"maybe"}}`, CodeBadRequest, "verdict"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"edit"}}`, CodeBadRequest, "args"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"edit","args":null}}`, CodeBadRequest, "args"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"allow","args":{}}}`, CodeBadRequest, "args"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"allow","reason":"x"}}`, CodeBadRequest, "reason"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","decision":{"verdict":"allow","bogus":1}}`, CodeBadRequest, "bogus"},
		{`{"kind":"approve","task_id":"t1","call_id":"c","outcome":{"result":"x"},"decision":{"verdict":"allow"}}`, CodeBadRequest, "outcome"},
		// status, subscribe, unsubscribe
		{`{"kind":"status","task_id":"t1"}`, CodeBadRequest, "task_id"},
		{`{"kind":"subscribe","from_seq":-1}`, CodeBadRequest, "from_seq"},
		{`{"kind":"subscribe","from_seq":"0"}`, CodeBadRequest, "from_seq"},
		{`{"kind":"unsubscribe","from_seq":3}`, CodeBadRequest, "from_seq"},
	}
	for _, c := range cases {
		e := decodeErr(t, c.line, c.code)
		if !strings.Contains(e.Message, c.mention) {
			t.Errorf("%s: message %q does not mention %q", c.line, e.Message, c.mention)
		}
	}
	// A resolve with an empty result is a real answer: the call returned "".
	req := decodeOK(t, `{"kind":"resolve","task_id":"t1","call_id":"c","outcome":{}}`)
	if req.Outcome == nil || req.Outcome.Retry || req.Outcome.Result != "" {
		t.Fatalf("outcome %+v", req.Outcome)
	}
	// subscribe without from_seq starts from the beginning.
	if req := decodeOK(t, `{"kind":"subscribe"}`); req.FromSeq != nil {
		t.Fatalf("from_seq %v", *req.FromSeq)
	}
}

func TestReplyAndEventLines(t *testing.T) {
	b, err := Marshal(Reply{ID: json.RawMessage(`"q"`), Kind: proto.KindSubmit, OK: true, TaskID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"reply","id":"q","kind":"submit","ok":true,"task_id":"t1"}` + "\n"; string(b) != want {
		t.Fatalf("reply line\n got %s want %s", b, want)
	}
	// Data is passed through as committed.
	ev := proto.Event{Kind: proto.EvAnswer, TaskID: "t1", Seq: 9, TaskSeq: 3, Data: json.RawMessage(`{"text":"<b>&</b>"}`)}
	b, err = Marshal(EventLine{Event: ev})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"event","kind":"answer","task_id":"t1","seq":9,"task_seq":3,"data":{"text":"<b>&</b>"}}` + "\n"; string(b) != want {
		t.Fatalf("event line\n got %s want %s", b, want)
	}
	var back Line
	if err := json.Unmarshal(b, &back); err != nil || back.Type != TypeEvent {
		t.Fatalf("a client reads the type first: %+v %v", back, err)
	}
	b, _ = Marshal(Reply{Kind: proto.KindSubmit, Error: &Error{Code: CodeBusy, Message: "full", Count: 4, Limit: 4}})
	if want := `{"type":"reply","kind":"submit","ok":false,"error":{"code":"busy","message":"full","count":4,"limit":4}}` + "\n"; string(b) != want {
		t.Fatalf("error line\n got %s want %s", b, want)
	}
}
