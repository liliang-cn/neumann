package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/proto"
)

// console is neu's terminal: progress lines and answers on their writers,
// and the focused task's answer streamed as the model writes it
// (Machine.Deltas). Everything it prints goes through one lock, so a
// progress line never lands in the middle of a streamed word.
type console struct {
	mu       sync.Mutex
	out, err io.Writer
	think    bool // stream the focused task's reasoning too (on err)

	focus   string // the task whose output streams; "": none
	midline bool   // the last thing printed was streamed text without a newline
	thought bool   // reasoning is being streamed and its line is open
	streams map[string]*streamed
}

// streamed is what a task's model has streamed since its last tool round:
// the text that should become its answer.
type streamed struct {
	text strings.Builder
	any  bool // anything was streamed for the task at all
}

func newConsole(out, err io.Writer, think bool) *console {
	return &console{out: out, err: err, think: think, streams: map[string]*streamed{}}
}

// watch streams the machine's deltas until ctx ends.
func (c *console) watch(ctx context.Context, m *neumann.Machine) {
	go func() {
		for d := range m.Deltas(ctx) {
			c.delta(d)
		}
	}()
}

// setFocus makes id the task whose answer streams.
func (c *console) setFocus(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine()
	c.focus = id
}

func (c *console) delta(d proto.Delta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.TaskID != c.focus {
		return
	}
	s := c.streams[d.TaskID]
	if s == nil {
		s = &streamed{}
		c.streams[d.TaskID] = s
	}
	if d.Reasoning != "" && c.think {
		if !c.thought {
			c.breakLine()
			fmt.Fprint(c.err, "  … ")
			c.thought = true
		}
		fmt.Fprint(c.err, strings.ReplaceAll(d.Reasoning, "\n", "\n    "))
		c.midline = true
	}
	if d.Text == "" {
		return
	}
	if c.thought {
		fmt.Fprintln(c.err)
		c.thought, c.midline = false, false
	}
	s.any = true
	s.text.WriteString(d.Text)
	fmt.Fprint(c.out, d.Text)
	c.midline = !strings.HasSuffix(d.Text, "\n")
}

// event is told every committed event of the machine, before it is printed:
// a tool round or a restarted call means the text streamed so far was not
// the answer.
func (c *console) event(ev proto.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.streams[ev.TaskID]
	if s == nil {
		return
	}
	switch ev.Kind {
	case proto.EvToolCall:
		s.text.Reset() // the step's text was a preamble to its tools
	case proto.EvModelRetry:
		if s.text.Len() > 0 {
			c.breakLine()
			fmt.Fprintln(c.err, "  (the model call is retried; the answer starts again)")
		}
		s.text.Reset()
	}
}

// line prints one progress line, on a line of its own.
func (c *console) line(w io.Writer, s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine()
	fmt.Fprintln(w, s)
}

// answer reports whether the task's answer was already streamed in full,
// and forgets the task. A streamed answer ends its line here.
func (c *console) answer(id, final string) (shown bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.streams[id]
	delete(c.streams, id)
	if c.focus == id {
		c.focus = ""
	}
	if s == nil || !s.any {
		return false
	}
	shown = strings.TrimSpace(s.text.String()) == strings.TrimSpace(final)
	c.breakLine()
	return shown
}

// breakLine ends an open streamed line. Callers hold mu.
func (c *console) breakLine() {
	if c.midline {
		if c.thought {
			fmt.Fprintln(c.err)
			c.thought = false
		} else {
			fmt.Fprintln(c.out)
		}
		c.midline = false
	}
}
