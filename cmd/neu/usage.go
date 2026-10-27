package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// usageLine is one task's usage: its answered model calls and tokens (the
// TCB's Used, which the machine adds each usage event into), how often it
// was compacted, and how long it ran. Cached is a share of the prompt
// tokens and is left out when the provider reported no cache hits;
// reasoning tokens are part of out.
func usageLine(id string, u task.Usage, compactions int, took time.Duration) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] %d call%s, %s in", id, u.Steps, plural(u.Steps), tokens(u.PromptTokens))
	if u.CachedTokens > 0 && u.PromptTokens > 0 {
		fmt.Fprintf(&b, " (%d%% cached)", u.CachedTokens*100/u.PromptTokens)
	}
	fmt.Fprintf(&b, ", %s out", tokens(u.CompletionTokens))
	if u.ReasoningTokens > 0 {
		fmt.Fprintf(&b, " (%s reasoning)", tokens(u.ReasoningTokens))
	}
	if took > 0 {
		fmt.Fprintf(&b, ", %s", took.Round(100*time.Millisecond))
	}
	if compactions > 0 {
		fmt.Fprintf(&b, ", compacted %d×", compactions)
	}
	return b.String()
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// tokens is a token count in a few characters: 950, 41.2k, 1.30M.
func tokens(n int) string {
	switch {
	case n < 1000:
		return fmt.Sprintf("%d", n)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	}
	return fmt.Sprintf("%.2fM", float64(n)/1e6)
}

// taskUsage is the usage line of a task that has ended with ev: Used from
// its TCB, compactions and the time from its first event read this session
// to its end from the tracker.
func taskUsage(tr *app.Tracker, t *task.TCB, ev proto.Event) string {
	evs := tr.Events(t.ID)
	n := 0
	for _, e := range evs {
		if e.Kind == proto.EvCompacted {
			n++
		}
	}
	var took time.Duration
	if len(evs) > 0 && !evs[0].Time.IsZero() && !ev.Time.IsZero() {
		took = ev.Time.Sub(evs[0].Time)
	}
	return usageLine(t.ID, t.Used, n, took)
}

// tally sums the session's usage events: every answered model call of
// every task, running ones included.
type tally struct {
	mu          sync.Mutex
	u           task.Usage
	compactions int
	ended       int
	tasks       map[string]bool
}

func newTally() *tally { return &tally{tasks: map[string]bool{}} }

func (s *tally) add(ev proto.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case ev.Kind == proto.EvUsage:
		var u task.Usage
		if json.Unmarshal(ev.Data, &u) == nil {
			s.u.Add(u)
			s.u.Steps++
			s.tasks[ev.TaskID] = true
		}
	case ev.Kind == proto.EvCompacted:
		s.compactions++
	case ev.TaskID != "" && ev.Terminal():
		s.ended++
	}
}

// String is the session's totals in one line.
func (s *tally) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	line := usageLine("session", s.u, s.compactions, 0)
	return fmt.Sprintf("%s; %d task%s made calls, %d ended", line, len(s.tasks), plural(len(s.tasks)), s.ended)
}
