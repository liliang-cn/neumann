package observe

import (
	"context"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// Source is the part of a machine observe reads. *neumann.Machine is one;
// nothing here needs anything a host could not call itself.
type Source interface {
	EventSource
	EventLog(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error)
	Status() neumann.Status
	Queue() neumann.QueueStatus
	Load(ctx context.Context, id string) (*task.TCB, error)
	Calls(ctx context.Context, id string) ([]mem.CallRecord, error)
}

// EventSource streams committed events with Seq > fromSeq, in order, as
// neumann.Machine.Events does.
type EventSource interface {
	Events(ctx context.Context, fromSeq int64) <-chan proto.Event
}

var _ Source = (*neumann.Machine)(nil)

// Observer is fed committed events in Seq order, from one goroutine.
// Metrics, Tracer and Log are Observers.
type Observer interface {
	Observe(ev proto.Event)
}

// Flusher is an Observer that buffers: Follow flushes it whenever it has
// caught up with the log, and once more when it returns.
type Flusher interface {
	Flush(ctx context.Context) error
}

// ObserverFunc adapts a function to an Observer.
type ObserverFunc func(ev proto.Event)

func (f ObserverFunc) Observe(ev proto.Event) { f(ev) }

// Follow feeds every committed event with Seq > fromSeq to each observer in
// turn, replaying the log first and then following it live, until ctx ends
// or the machine stops and everything it committed has been read. It
// returns the Seq of the last event fed (fromSeq if none): a host that
// saves it can resume from there after a restart.
//
// Follow reads the durable log at its own pace, so a slow observer never
// holds the machine up; it only falls behind. It returns ctx.Err() when ctx
// ended, nil when the stream did.
func Follow(ctx context.Context, src EventSource, fromSeq int64, obs ...Observer) (int64, error) {
	last := fromSeq
	flush := func() {
		for _, o := range obs {
			if f, ok := o.(Flusher); ok {
				f.Flush(context.WithoutCancel(ctx))
			}
		}
	}
	defer flush()
	evs := src.Events(ctx, fromSeq)
	for {
		select {
		case ev, ok := <-evs:
			if !ok {
				return last, ctx.Err()
			}
			for _, o := range obs {
				o.Observe(ev)
			}
			last = ev.Seq
			if len(evs) == 0 {
				flush() // caught up: nothing buffered waits for the next event
			}
		case <-ctx.Done():
			return last, ctx.Err()
		}
	}
}

// Head is the Seq of the last committed event, 0 for an empty log. It
// costs O(log n) reads of one event each, so a host can start observers at
// the end of a long log (counting only what happens from now on) without
// reading it.
func Head(ctx context.Context, src interface {
	EventLog(ctx context.Context, afterSeq int64, limit int) ([]proto.Event, error)
}) (int64, error) {
	// beyond(n): an event with Seq > n exists. True up to the head, false
	// from it on, whatever gaps the log has.
	beyond := func(n int64) (bool, error) {
		evs, err := src.EventLog(ctx, n, 1)
		return len(evs) > 0, err
	}
	lo, hi := int64(0), int64(1) // beyond(lo) is true or lo is 0; find hi with beyond(hi) false
	for {
		ok, err := beyond(hi - 1)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		lo, hi = hi, hi*2
	}
	// The head is in [lo, hi): beyond(head-1) holds, beyond(head) does not.
	for lo+1 < hi {
		mid := lo + (hi-lo)/2
		ok, err := beyond(mid - 1)
		if err != nil {
			return 0, err
		}
		if ok {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo, nil
}
