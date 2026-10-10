package jsonl

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/liliang-cn/neumann/proto"
)

// subscription changes the event stream: start it at from, or turn it off.
type subscription struct {
	from int64
	off  bool
}

// item is a reply for the writer, with the stream change it carries out.
type item struct {
	reply Reply
	sub   *subscription
}

// writer owns the output. It is the only goroutine that writes to out, so
// lines never interleave, and it alone switches the event stream, so a
// subscribe reply is written exactly between the old stream's last event and
// the new stream's first.
//
// Replies go first: whenever one is waiting it is written before the next
// event, so a backlog of events never delays the answer to an instruction.
// The machine is never blocked by a slow reader either way: events are read
// from the store at the writer's pace (Machine.Events), and a reader that
// falls behind only falls behind in the log.
type writer struct {
	m      Machine
	out    io.Writer
	failed func() // called once when a write fails: the reader is gone

	items chan item     // closed by finish: no more replies
	abort chan struct{} // closed by finish when draining takes too long
	done  chan struct{}
	err   error // the first write error; read after done
}

func newWriter(m Machine, out io.Writer, failed func()) *writer {
	return &writer{
		m: m, out: out, failed: failed,
		items: make(chan item, replyBuffer),
		abort: make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// send queues a reply. It blocks only when replyBuffer replies are already
// waiting for a reader that does not read, which pushes back on the input.
func (w *writer) send(it item) { w.items <- it }

func (w *writer) run() {
	defer close(w.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var events <-chan proto.Event
	stopEvents := func() {}
	defer func() { stopEvents() }()

	items := w.items
	reply := func(it item, ok bool) {
		if !ok {
			items = nil // finish: only the events are left to write
			return
		}
		if it.sub != nil {
			stopEvents()
			events = nil
			if !it.sub.off {
				sctx, scancel := context.WithCancel(ctx)
				events, stopEvents = w.m.Events(sctx, it.sub.from), scancel
			}
		}
		w.write(it.reply)
	}
	for items != nil || events != nil {
		select {
		case it, ok := <-items:
			reply(it, ok)
			continue
		default:
		}
		select {
		case it, ok := <-items:
			reply(it, ok)
		case ev, ok := <-events:
			if !ok {
				events = nil // the machine stopped and everything committed was read
				continue
			}
			w.write(EventLine{Event: ev})
			if w.err != nil {
				stopEvents() // nobody is reading: stop reading the log for them
				events = nil
			}
		case <-w.abort:
			return
		}
	}
}

// write writes one line, unless an earlier write failed.
func (w *writer) write(v any) {
	if w.err != nil {
		return
	}
	b, err := Marshal(v)
	if err == nil {
		_, err = w.out.Write(b)
	}
	if err != nil {
		w.err = fmt.Errorf("jsonl: write output: %w", err)
		w.failed()
	}
}

// finish says there are no more replies, waits up to drain for the events
// still due (the stream ends by itself once the machine has stopped and every
// committed event is written), and returns the first write error.
func (w *writer) finish(drain time.Duration) error {
	close(w.items)
	t := time.NewTimer(drain)
	defer t.Stop()
	select {
	case <-w.done:
	case <-t.C:
		close(w.abort)
		select {
		case <-w.done:
		case <-time.After(time.Second):
			// Stuck in a write nobody reads; the process is about to exit.
			return fmt.Errorf("jsonl: output not drained within %s", drain)
		}
	}
	return w.err
}
