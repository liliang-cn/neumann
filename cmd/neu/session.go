package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// A session is the conversation of the REPL: the questions asked and the
// answers given, in order. Every task is a new task on the machine; what
// carries the conversation from one to the next is that each new request
// is sent with the turns before it. The session is kept next to the store
// (.neu/session.json), so a restarted neu picks it up; /new starts over.
type session struct {
	mu    sync.Mutex
	path  string
	Turns []turn         `json:"turns"`
	ids   map[string]int // task id → its turn
}

type turn struct {
	Task     string `json:"task"`
	Question string `json:"question"`
	Answer   string `json:"answer,omitempty"`
}

// sessionBudget bounds the conversation sent with a request, in bytes:
// the oldest turns are left out first.
const sessionBudget = 24 << 10

func openSession(store string) *session {
	s := &session{path: filepath.Join(filepath.Dir(store), "session.json"), ids: map[string]int{}}
	if b, err := os.ReadFile(s.path); err == nil {
		json.Unmarshal(b, s)
		for i, t := range s.Turns {
			s.ids[t.Task] = i
		}
	}
	return s
}

// input is the request sent for question: the question alone when the
// session is empty, else the earlier turns and then the question.
func (s *session) input(question string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kept []string
	size := 0
	for i := len(s.Turns) - 1; i >= 0; i-- {
		t := s.Turns[i]
		if t.Answer == "" {
			continue // still running, or ended without an answer
		}
		block := "User: " + t.Question + "\n\nAssistant: " + t.Answer
		if size+len(block) > sessionBudget && len(kept) > 0 {
			break
		}
		kept = append(kept, block)
		size += len(block)
	}
	if len(kept) == 0 {
		return question
	}
	var b strings.Builder
	b.WriteString("This request continues a conversation with the same user in this workspace. The earlier turns, oldest first (your own earlier answers are what you did and said; the workspace may have changed since):\n\n")
	for i := len(kept) - 1; i >= 0; i-- {
		b.WriteString("<turn>\n")
		b.WriteString(kept[i])
		b.WriteString("\n</turn>\n\n")
	}
	b.WriteString("The user's new request:\n")
	b.WriteString(question)
	return b.String()
}

// asked records a submitted question.
func (s *session) asked(id, question string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids[id] = len(s.Turns)
	s.Turns = append(s.Turns, turn{Task: id, Question: question})
	s.save()
}

// answered records a task's answer; a task that is not in the session is
// ignored.
func (s *session) answered(id, answer string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.ids[id]
	if !ok || answer == "" {
		return
	}
	s.Turns[i].Answer = answer
	s.save()
}

// reset forgets the conversation.
func (s *session) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Turns, s.ids = nil, map[string]int{}
	os.Remove(s.path)
}

func (s *session) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Turns)
}

// save writes the session. Callers hold mu. A failed write only loses the
// conversation across a restart.
func (s *session) save() {
	b, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		return
	}
	os.MkdirAll(filepath.Dir(s.path), 0o700)
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, s.path)
	}
}
