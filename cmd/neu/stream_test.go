package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/proto"
)

func TestConsoleStreamsTheFocusedAnswer(t *testing.T) {
	var out, errw bytes.Buffer
	c := newConsole(&out, &errw, false)
	c.setFocus("a")
	c.delta(proto.Delta{TaskID: "a", Step: 1, Text: "Let me look."})
	c.event(proto.Event{TaskID: "a", Kind: proto.EvToolCall})
	c.line(&errw, "[a] → fs_read")
	c.delta(proto.Delta{TaskID: "b", Step: 1, Text: "not shown"})
	c.delta(proto.Delta{TaskID: "a", Step: 2, Reasoning: "hidden"})
	c.delta(proto.Delta{TaskID: "a", Step: 2, Text: "It has "})
	c.delta(proto.Delta{TaskID: "a", Step: 2, Text: "60 lines."})
	if !c.answer("a", "It has 60 lines.") {
		t.Fatal("a streamed answer is reported as not shown")
	}
	if got := out.String(); got != "Let me look.\nIt has 60 lines.\n" {
		t.Fatalf("stdout %q", got)
	}
	if strings.Contains(out.String()+errw.String(), "not shown") || strings.Contains(errw.String(), "hidden") {
		t.Fatalf("another task's text or the reasoning streamed: %q / %q", out.String(), errw.String())
	}
	if !strings.HasPrefix(errw.String(), "[a] → fs_read\n") {
		t.Fatalf("progress line %q", errw.String())
	}
}

func TestConsoleRetriedCallIsNotTheAnswer(t *testing.T) {
	var out, errw bytes.Buffer
	c := newConsole(&out, &errw, false)
	c.setFocus("a")
	c.delta(proto.Delta{TaskID: "a", Step: 1, Text: "It has 5"})
	c.event(proto.Event{TaskID: "a", Kind: proto.EvModelRetry})
	c.delta(proto.Delta{TaskID: "a", Step: 2, Text: "It has 60 lines."})
	if !c.answer("a", "It has 60 lines.") {
		t.Fatal("the answer streamed after the retry is reported as not shown")
	}
	if !strings.Contains(errw.String(), "retried") {
		t.Fatalf("no retry note: %q", errw.String())
	}
}

func TestConsoleDifferentFinalIsPrintedAgain(t *testing.T) {
	var out, errw bytes.Buffer
	c := newConsole(&out, &errw, false)
	c.setFocus("a")
	c.delta(proto.Delta{TaskID: "a", Step: 1, Text: "partial"})
	if c.answer("a", "a different answer") {
		t.Fatal("a streamed text that is not the answer is reported as the answer")
	}
	if c.answer("b", "never streamed") {
		t.Fatal("a task that never streamed is reported as shown")
	}
}

func TestConsoleThinking(t *testing.T) {
	var out, errw bytes.Buffer
	c := newConsole(&out, &errw, true)
	c.setFocus("a")
	c.delta(proto.Delta{TaskID: "a", Step: 1, Reasoning: "count the lines"})
	c.delta(proto.Delta{TaskID: "a", Step: 1, Text: "60."})
	c.answer("a", "60.")
	if errw.String() != "  … count the lines\n" || out.String() != "60.\n" {
		t.Fatalf("stderr %q stdout %q", errw.String(), out.String())
	}
}
