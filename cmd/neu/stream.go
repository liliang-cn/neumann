package main

import (
	"context"
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

	// While an approval prompt waits for its answer (hold), everything
	// else is kept in pending and printed when it is answered.
	paused  bool
	pending []pend
}

type pend struct {
	w io.Writer
	s string
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
			c.write(c.err, "  … ")
			c.thought = true
		}
		c.write(c.err, strings.ReplaceAll(d.Reasoning, "\n", "\n    "))
		c.midline = true
	}
	if d.Text == "" {
		return
	}
	if c.thought {
		c.write(c.err, "\n")
		c.thought, c.midline = false, false
	}
	s.any = true
	s.text.WriteString(d.Text)
	c.write(c.out, d.Text)
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
			c.write(c.err, "  (the model call is retried; the answer starts again)\n")
		}
		s.text.Reset()
	}
}

// line prints one progress line, on a line of its own.
func (c *console) line(w io.Writer, s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine()
	c.write(w, s+"\n")
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
			c.write(c.err, "\n")
			c.thought = false
		} else {
			c.write(c.out, "\n")
		}
		c.midline = false
	}
}

// write prints s on w, or keeps it while a prompt is open. Callers hold mu.
func (c *console) write(w io.Writer, s string) {
	if c.paused {
		c.pending = append(c.pending, pend{w, s})
		return
	}
	io.WriteString(w, s)
}

// hold opens a prompt: the open streamed line is ended, and until release
// nothing else is printed (it is kept, in order, and printed then), so the
// prompt and the answer typed after it stay on their own lines. One prompt
// is open at a time.
func (c *console) hold() (release func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine()
	c.paused = true
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.paused = false
		for _, p := range c.pending {
			io.WriteString(p.w, p.s)
		}
		c.pending = nil
	}
}

// prompt prints s on err while a prompt is open (hold), past the pause.
func (c *console) prompt(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	io.WriteString(c.err, s)
}
