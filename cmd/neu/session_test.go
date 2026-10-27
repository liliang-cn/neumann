package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionCarriesTheConversation(t *testing.T) {
	store := filepath.Join(t.TempDir(), ".neu", "state.db")
	s := openSession(store)
	if got := s.input("hi"); got != "hi" {
		t.Fatalf("first request %q", got)
	}
	s.asked("t1", "fix the bug")
	if got := s.input("is it fixed?"); got != "is it fixed?" {
		t.Fatalf("a turn without an answer was sent: %q", got)
	}
	s.answered("t1", "I changed i := 1 to i := 0 in sum.go; go test passes.")
	got := s.input("is it fixed?")
	for _, want := range []string{"User: fix the bug", "Assistant: I changed i := 1", "The user's new request:\nis it fixed?"} {
		if !strings.Contains(got, want) {
			t.Fatalf("request lacks %q:\n%s", want, got)
		}
	}

	// It survives a restart, and /new forgets it.
	s2 := openSession(store)
	if s2.len() != 1 || !strings.Contains(s2.input("again"), "fix the bug") {
		t.Fatalf("reopened session %+v", s2.Turns)
	}
	s2.reset()
	if openSession(store).input("again") != "again" {
		t.Fatal("reset session still carries turns")
	}
}

func TestSessionKeepsTheNewestWithinBudget(t *testing.T) {
	s := openSession(filepath.Join(t.TempDir(), ".neu", "state.db"))
	big := strings.Repeat("x", sessionBudget/3)
	for i, id := range []string{"a", "b", "c", "d", "e"} {
		s.asked(id, "q"+id)
		s.answered(id, big+string(rune('0'+i)))
	}
	got := s.input("now")
	if strings.Contains(got, "User: qa") || !strings.Contains(got, "User: qe") {
		t.Fatalf("not the newest turns kept")
	}
	if len(got) > sessionBudget+2048 {
		t.Fatalf("request of %d bytes over the budget", len(got))
	}
}
