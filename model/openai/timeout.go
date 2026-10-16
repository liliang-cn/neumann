package openai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// DefaultTimeout is the Timeout New sets: the longest one call may take.
const DefaultTimeout = 5 * time.Minute

// TimeoutError is a per-request timeout of this adapter: Timeout,
// FirstByteTimeout or IdleTimeout ran out while the caller's context was
// still alive. It reaches the caller wrapped in a Transient *model.Error, and
// also in model.ErrResponseLost when part of the response had arrived.
type TimeoutError struct {
	Which string        // "call", "first byte" or "idle"
	After time.Duration // the limit that ran out
}

func (e *TimeoutError) Error() string { return fmt.Sprintf("%s timeout after %s", e.Which, e.After) }

// Timeout reports true, as net.Error does for a timeout.
func (e *TimeoutError) Timeout() bool { return true }

// watchdog enforces one request's timeouts by cancelling its context with a
// *TimeoutError as the cause. The gap timer runs FirstByteTimeout until the
// body yields its first byte, then IdleTimeout, reset by every read that
// yields data.
type watchdog struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	stopT  func() bool // stops the whole-call timer; nil without one
	idle   time.Duration

	mu   sync.Mutex
	gap  *time.Timer
	seen bool // the body has yielded a byte
	done bool
}

// watch derives the request context for one call from the caller's ctx and
// starts its timers. The caller must close the response body, or call stop
// when there is none.
func (a *Adapter) watch(ctx context.Context) *watchdog {
	w := &watchdog{idle: a.IdleTimeout}
	w.ctx, w.cancel = context.WithCancelCause(ctx)
	if d := a.Timeout; d > 0 {
		t := time.AfterFunc(d, func() { w.cancel(&TimeoutError{Which: "call", After: d}) })
		w.stopT = t.Stop
	}
	if d := a.FirstByteTimeout; d > 0 {
		w.gap = time.AfterFunc(d, func() { w.cancel(&TimeoutError{Which: "first byte", After: d}) })
	}
	return w
}

// fired is the timeout that cancelled the request, or nil if none did. A
// cancelled caller is not a timeout: its context's error is the reason.
func (w *watchdog) fired() *TimeoutError {
	var te *TimeoutError
	if errors.As(context.Cause(w.ctx), &te) {
		return te
	}
	return nil
}

// cause replaces err with the timeout that caused it, if one did, so the
// chain carries no context.Canceled from cancelling the request.
func (w *watchdog) cause(err error) error {
	if te := w.fired(); te != nil {
		return te
	}
	return err
}

// read notes that the body yielded data: the first byte stops the
// first-byte timer and starts the idle one; later data resets it.
func (w *watchdog) read() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	if !w.seen {
		w.seen = true
		if w.gap != nil {
			w.gap.Stop()
			w.gap = nil
		}
		if d := w.idle; d > 0 {
			w.gap = time.AfterFunc(d, func() { w.cancel(&TimeoutError{Which: "idle", After: d}) })
		}
		return
	}
	if w.gap != nil {
		w.gap.Reset(w.idle)
	}
}

// stop ends the watch and releases the request context.
func (w *watchdog) stop() {
	w.mu.Lock()
	w.done = true
	if w.gap != nil {
		w.gap.Stop()
	}
	w.mu.Unlock()
	if w.stopT != nil {
		w.stopT()
	}
	w.cancel(context.Canceled)
}

// watchedBody is a response body under a watchdog. A read that fails
// because a timeout cancelled the request returns the *TimeoutError.
type watchedBody struct {
	io.ReadCloser
	w *watchdog
}

func (b *watchedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.w.read()
	}
	if err != nil && err != io.EOF {
		err = b.w.cause(err)
	}
	return n, err
}

func (b *watchedBody) Close() error {
	err := b.ReadCloser.Close()
	b.w.stop()
	return err
}
