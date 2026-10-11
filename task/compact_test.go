package task

import (
	"encoding/json"
	"strings"
	"testing"
)

func call(id, name, args string) Part {
	return Part{Kind: PartToolCall, CallID: id, Name: name, Args: json.RawMessage(args)}
}

func result(id, text string, isErr bool) Part {
	return Part{Kind: PartToolResult, CallID: id, Text: text, IsError: isErr}
}

// convo is a task's registers: the goal, two tool rounds, a steer, a text
// turn, and a third round still waiting for its results.
func convo() []Message {
	return []Message{
		Text(RoleUser, "find the bill and pay it"),
		{Role: RoleAssistant, Parts: []Part{{Kind: PartText, Text: "looking"}, call("c-1", "dev_look", `{"n":0}`), call("c-2", "dev_look", `{"n":1}`)}},
		{Role: RoleTool, Parts: []Part{result("c-1", "balance 10", false), result("c-2", "no such account", true)}},
		Text(RoleUser, "also check the date"),
		Text(RoleAssistant, "noted"),
		{Role: RoleAssistant, Parts: []Part{call("c-3", "dev_pay", `{"n":0}`)}},
		{Role: RoleTool, Parts: []Part{result("c-3", "paid", false)}},
		{Role: RoleAssistant, Parts: []Part{call("c-4", "dev_look", `{"n":0}`)}},
	}
}

func TestUnitsKeepRoundsWhole(t *testing.T) {
	head, units := Units(convo())
	if head != 1 {
		t.Fatalf("head %d, want 1 (the goal)", head)
	}
	want := []Unit{
		{Start: 1, End: 3, Round: true, Complete: true},
		{Start: 3, End: 4, Complete: true},
		{Start: 4, End: 5, Round: true, Complete: true},
		{Start: 5, End: 7, Round: true, Complete: true},
		{Start: 7, End: 8, Round: true},
	}
	if len(units) != len(want) {
		t.Fatalf("units %+v", units)
	}
	for i := range want {
		if units[i] != want[i] {
			t.Errorf("unit %d = %+v, want %+v", i, units[i], want[i])
		}
	}

	regs := append([]Message{Text(RoleUser, "goal"), SummaryMessage("earlier")}, convo()[1:3]...)
	_, units = Units(regs)
	if !units[0].Summary || units[0].Round || units[1].Summary || !units[1].Round {
		t.Fatalf("a summary is a unit of its own: %+v", units)
	}

	if head, units := Units([]Message{Text(RoleAssistant, "x")}); head != 0 || len(units) != 1 {
		t.Fatalf("no user message: head %d, units %+v", head, units)
	}
}

func TestCheckPairing(t *testing.T) {
	ok := convo()[:7]
	if err := CheckPairing(ok); err != nil {
		t.Fatalf("paired registers: %v", err)
	}
	if err := CheckPairing(convo()); err == nil || !strings.Contains(err.Error(), "c-4") {
		t.Fatalf("a call with no result: %v", err)
	}
	orphan := append([]Message{Text(RoleUser, "q")}, convo()[2:3]...)
	if err := CheckPairing(orphan); err == nil || !strings.Contains(err.Error(), "c-1") {
		t.Fatalf("a result with no call: %v", err)
	}
	twice := append(convo()[:3], Message{Role: RoleTool, Parts: []Part{result("c-1", "again", false)}})
	if err := CheckPairing(twice); err == nil {
		t.Fatal("two results for one call")
	}
}

func TestTranscriptIsStable(t *testing.T) {
	msgs := append([]Message{SummaryMessage("rounds=2: paid twice")}, convo()[1:7]...)
	msgs[1].Parts = append([]Part{{Kind: PartReasoning, Text: "secret thoughts", Provider: "p", Opaque: json.RawMessage(`{}`)}}, msgs[1].Parts...)
	a, b := Transcript(msgs), Transcript(msgs)
	if a != b {
		t.Fatal("the transcript of the same messages differs")
	}
	for _, want := range []string{
		"### summary of earlier work\nrounds=2: paid twice",
		"### assistant\nlooking\n[tool call c-1: dev_look {\"n\":0}]",
		"### tool results\n[result of c-1]\nbalance 10\n[result of c-2, an error]\nno such account",
		"### user\nalso check the date",
		"### assistant\nnoted",
	} {
		if !strings.Contains(a, want) {
			t.Errorf("transcript lacks %q:\n%s", want, a)
		}
	}
	if strings.Contains(a, "secret thoughts") {
		t.Error("reasoning reached the transcript")
	}
	req := CompactMessages(msgs)
	if len(req) != 1 || req[0].Role != RoleUser || req[0].TextContent() != a {
		t.Fatalf("compaction request messages %+v", req)
	}
}

func TestSummaryMessage(t *testing.T) {
	m := SummaryMessage("the gist")
	if m.Role != RoleUser || !m.Summary || !strings.HasPrefix(m.TextContent(), SummaryHeader) || !strings.HasSuffix(m.TextContent(), "the gist") {
		t.Fatalf("summary %+v", m)
	}
	if SummaryText(m) != "the gist" || SummaryText(Text(RoleUser, "x")) != "" {
		t.Fatal("SummaryText")
	}
	b, _ := json.Marshal(Text(RoleUser, "x"))
	if strings.Contains(string(b), "summary") {
		t.Fatalf("an ordinary message marshals a summary flag: %s", b)
	}
}

// A feedback message is an ordinary user message to compaction: a unit of
// its own, never a summary, rendered under "### user"; an ordinary message
// marshals no feedback flag.
func TestFeedbackMessage(t *testing.T) {
	m := FeedbackMessage("give the figure")
	if m.Role != RoleUser || !m.Feedback || m.Summary || m.TextContent() != "give the figure" || SummaryText(m) != "" {
		t.Fatalf("feedback %+v", m)
	}
	msgs := []Message{Text(RoleUser, "goal"), Text(RoleAssistant, "a draft"), m, Text(RoleAssistant, "final")}
	head, units := Units(msgs)
	if head != 1 || len(units) != 3 || units[1].Round || units[1].Summary || !units[1].Complete || units[1].Start != 2 {
		t.Fatalf("head %d, units %+v", head, units)
	}
	if tr := Transcript(msgs[1:]); tr != "### assistant\na draft\n\n### user\ngive the figure\n\n### assistant\nfinal" {
		t.Fatalf("transcript %q", tr)
	}
	if b, _ := json.Marshal(Text(RoleUser, "x")); strings.Contains(string(b), "feedback") {
		t.Fatalf("an ordinary message marshals a feedback flag: %s", b)
	}
	if c := CloneMessages(msgs); !c[2].Feedback {
		t.Fatal("a clone lost the feedback flag")
	}
}

func TestCloneMessagesIsDeep(t *testing.T) {
	orig := convo()
	c := CloneMessages(orig)
	c[1].Parts[1].Args[2] = 'X'
	c[0].Parts[0].Text = "changed"
	if string(orig[1].Parts[1].Args) != `{"n":0}` || orig[0].TextContent() != "find the bill and pay it" {
		t.Fatal("clone shares memory with the original")
	}
	if CloneMessages(nil) != nil {
		t.Fatal("nil clones to nil")
	}
}
