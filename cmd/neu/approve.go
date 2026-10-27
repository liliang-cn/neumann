package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/internal/neu/app"
	"github.com/liliang-cn/neumann/proto"
)

// stdinIsTerminal reports whether r is a terminal; tests replace it.
var stdinIsTerminal = func(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// noPrompt is why a one-shot run without a terminal cannot ask.
const noPrompt = "stdin is not a terminal, so neu cannot ask; run neu in a terminal, or rerun with -approve auto to allow side effects (or -approve never to refuse them)"

// readFunc returns the next line the user types: io.EOF when no more will
// come, ctx's error when ctx ends first.
type readFunc func(ctx context.Context) (string, error)

// lineSource reads stdin one line at a time for the REPL, and hands the
// next line to an approval prompt instead while one waits for its answer.
type lineSource struct {
	lines chan string // the REPL's; closed at EOF
	done  chan struct{}
	mu    sync.Mutex
	want  chan string // set while a prompt waits
}

func readLines(r io.Reader) *lineSource {
	s := &lineSource{lines: make(chan string), done: make(chan struct{})}
	go func() {
		defer close(s.lines)
		defer close(s.done)
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			s.mu.Lock()
			w := s.want
			s.want = nil
			s.mu.Unlock()
			if w != nil {
				w <- sc.Text() // buffered
				continue
			}
			s.lines <- sc.Text()
		}
	}()
	return s
}

// answer is a readFunc: the next line typed, which the REPL does not see.
func (s *lineSource) answer(ctx context.Context) (string, error) {
	ch := make(chan string, 1)
	s.mu.Lock()
	s.want = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.want == ch {
			s.want = nil
		}
		s.mu.Unlock()
	}()
	select {
	case l := <-ch:
		return l, nil
	case <-s.done:
		select {
		case l := <-ch:
			return l, nil
		default:
		}
		return "", io.EOF
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// readerLines is a readFunc over r, for a one-shot run: stdin is read only
// when a prompt asks.
func readerLines(r io.Reader) readFunc {
	br := bufio.NewReader(r)
	var mu sync.Mutex
	return func(ctx context.Context) (string, error) {
		type res struct {
			s   string
			err error
		}
		ch := make(chan res, 1)
		go func() {
			mu.Lock()
			defer mu.Unlock()
			s, err := br.ReadString('\n')
			if err == io.EOF && s != "" {
				err = nil
			}
			ch <- res{strings.TrimRight(s, "\r\n"), err}
		}()
		select {
		case r := <-ch:
			return r.s, r.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// approver answers held tool calls (approval_needed) one at a time: it
// shows each on the terminal and reads y / n / a. Calls held at once wait
// in a queue, and while a prompt is open the console prints nothing else.
type approver struct {
	a    *app.App
	con  *console
	read readFunc // nil: nobody to ask

	mu    sync.Mutex
	queue []app.Held
	seen  map[string]bool
	wake  chan struct{}
}

func newApprover(a *app.App, con *console, read readFunc) *approver {
	return &approver{a: a, con: con, read: read, seen: map[string]bool{}, wake: make(chan struct{}, 1)}
}

// start answers the calls held now and those tr reads from now on, for
// the tasks keep accepts (nil: all), until ctx ends.
func (p *approver) start(ctx context.Context, tr *app.Tracker, keep func(task string) bool) {
	tr.Subscribe(func(ev proto.Event) {
		if h, ok := app.HeldFrom(ev); ok && (keep == nil || keep(h.Task)) {
			p.enqueue(h)
		}
	})
	if held, err := p.a.HeldNow(ctx); err == nil {
		for _, h := range held {
			if keep == nil || keep(h.Task) {
				p.enqueue(h)
			}
		}
	}
	go p.loop(ctx)
}

func (p *approver) enqueue(h app.Held) {
	p.mu.Lock()
	k := h.Task + "/" + h.Call
	if p.seen[k] {
		p.mu.Unlock()
		return
	}
	p.seen[k] = true
	p.queue = append(p.queue, h)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *approver) next(ctx context.Context) (app.Held, bool) {
	for {
		p.mu.Lock()
		if len(p.queue) > 0 {
			h := p.queue[0]
			p.queue = p.queue[1:]
			p.mu.Unlock()
			return h, true
		}
		p.mu.Unlock()
		select {
		case <-p.wake:
		case <-ctx.Done():
			return app.Held{}, false
		}
	}
}

func (p *approver) loop(ctx context.Context) {
	for {
		h, ok := p.next(ctx)
		if !ok {
			return
		}
		p.decide(ctx, h)
	}
}

// what is the one-line description of a held call.
func what(h app.Held) string {
	if h.Reason != "" {
		return h.Reason
	}
	return h.Tool
}

func (p *approver) decide(ctx context.Context, h app.Held) {
	tag := "[" + h.Task + "]"
	allow := neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}
	switch {
	case p.a.Cfg.Coding.Approve == app.ApproveAuto:
		p.settle(ctx, h, allow, fmt.Sprintf("%s ✓ %s (held before; -approve auto)", tag, what(h)))
		return
	case p.a.Cfg.Coding.Approve == app.ApproveNever:
		p.settle(ctx, h, neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "refused: neu runs with -approve never (read-only)"},
			fmt.Sprintf("%s ✗ %s (held before; -approve never refuses it)", tag, what(h)))
		return
	case p.a.AlwaysAllowed(h.Tool):
		p.settle(ctx, h, allow, fmt.Sprintf("%s ✓ %s (always allowed this session)", tag, what(h)))
		return
	case p.read == nil:
		p.settle(ctx, h, neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: noPrompt}, fmt.Sprintf("%s ✗ %s: %s", tag, what(h), noPrompt))
		return
	}
	release := p.con.hold()
	defer release()
	var b strings.Builder
	fmt.Fprintf(&b, "%s needs approval: %s\n", tag, what(h))
	if pv := p.a.Tools.Preview(h.Tool, h.Args); pv != "" {
		for _, l := range strings.Split(pv, "\n") {
			b.WriteString("    " + l + "\n")
		}
	}
	fmt.Fprintf(&b, "  allow? [y]es, [n]o (n <reason> tells the model why), [a]lways %s this session: ", h.Tool)
	p.con.prompt(b.String())
	for {
		line, err := p.read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				p.con.prompt("\n" + p.approve(ctx, h, neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: "the user gave no answer (input closed)"}, tag+" ✗ denied: no answer") + "\n")
			}
			return // ctx ended: the call stays held for the next start
		}
		d, always, ok := parseAnswer(line)
		if !ok {
			p.con.prompt("  answer y, n (or n <reason>), or a: ")
			continue
		}
		if always {
			p.a.AllowAlways(h.Tool)
		}
		note := tag + " ✓ allowed"
		if always {
			note = fmt.Sprintf("%s ✓ allowed, and every %s from now on", tag, h.Tool)
		}
		if d.Verdict == neumann.ApproveDeny {
			note = tag + " ✗ denied"
		}
		p.con.prompt(p.approve(ctx, h, d, note) + "\n")
		return
	}
}

// parseAnswer reads y / yes, n / no [reason], a / always.
func parseAnswer(line string) (d neumann.ApprovalDecision, always, ok bool) {
	line = strings.TrimSpace(line)
	word, rest, _ := strings.Cut(line, " ")
	switch strings.ToLower(word) {
	case "y", "yes":
		return neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}, false, true
	case "a", "always":
		return neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}, true, true
	case "n", "no":
		reason := "the user denied this call"
		if r := strings.TrimSpace(rest); r != "" {
			reason += ": " + r
		}
		return neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: reason}, false, true
	}
	return neumann.ApprovalDecision{}, false, false
}

// approve commits d for h; it returns note, or why it could not.
func (p *approver) approve(ctx context.Context, h app.Held, d neumann.ApprovalDecision, note string) string {
	err := p.a.M.Approve(context.WithoutCancel(ctx), h.Task, h.Call, d)
	switch {
	case errors.Is(err, neumann.ErrNotAwaitingApproval):
		return fmt.Sprintf("[%s] %s is no longer waiting for approval", h.Task, h.Tool)
	case err != nil:
		return fmt.Sprintf("[%s] approve %s: %v", h.Task, h.Tool, err)
	}
	return note
}

// settle decides h without asking and prints the note.
func (p *approver) settle(ctx context.Context, h app.Held, d neumann.ApprovalDecision, note string) {
	p.con.line(p.con.err, p.approve(ctx, h, d, note))
}
