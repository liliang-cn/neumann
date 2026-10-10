package neumann

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/liliang-cn/neumann/proto"
)

// deltaBuffer is how many deltas a subscriber may fall behind before it
// starts losing them.
const deltaBuffer = 1024

// deltaHub fans streamed output out to live subscribers. Publishing never
// blocks a vCore: a subscriber whose buffer is full misses that delta, and
// the miss is counted.
type deltaHub struct {
	mu      sync.Mutex
	subs    map[chan proto.Delta]struct{}
	closed  bool
	dropped atomic.Int64
}

func newDeltaHub() *deltaHub { return &deltaHub{subs: map[chan proto.Delta]struct{}{}} }

func (h *deltaHub) publish(d proto.Delta) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- d:
		default:
			h.dropped.Add(1)
		}
	}
}

func (h *deltaHub) subscribe(ctx context.Context) <-chan proto.Delta {
	ch := make(chan proto.Delta, deltaBuffer)
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		close(ch)
		return ch
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	context.AfterFunc(ctx, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	})
	return ch
}

func (h *deltaHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
}

// Deltas streams model output live, as it is generated: text and reasoning
// pieces of every running model step, for as long as ctx lasts or until the
// machine stops. Model calls stream whenever the adapter can (model.Streamer).
//
// Deltas are a preview. They are not committed and have no sequence number; a
// subscriber that falls more than 1024 deltas behind misses some (counted in
// Status.DeltasDropped), and a step that fails or is interrupted may have
// streamed text that never becomes an answer. The committed answer event is
// the record.
func (m *Machine) Deltas(ctx context.Context) <-chan proto.Delta {
	return m.deltas.subscribe(ctx)
}
