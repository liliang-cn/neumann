package neumann

import "sync"

// hub wakes event subscribers after a commit. A subscriber takes the current
// channel before reading the log, so a commit that lands in between still
// wakes it: the producer never blocks and no wake-up is lost.
type hub struct {
	mu     sync.Mutex
	ch     chan struct{}
	closed bool
}

func newHub() *hub { return &hub{ch: make(chan struct{})} }

func (h *hub) notify() {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.ch)
	h.ch = make(chan struct{})
}

func (h *hub) wait() (<-chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ch, h.closed
}

func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	close(h.ch)
	h.ch = make(chan struct{})
}
