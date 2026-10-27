package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/anthropic"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/task"
)

// CallRecord is one model request as the adapter saw it: from the call to
// its return, without the machine's commits around it.
type CallRecord struct {
	Class       string // the group whose adapter made it
	Fingerprint string // of the request's first user message: which task sent it
	Start       time.Time
	First       time.Time // first streamed piece; zero if none
	End         time.Time
	Err         string
}

// Backend is what both the OpenAI-compatible and the Anthropic adapter
// implement: the adapter and every optional interface Timed forwards.
type Backend interface {
	model.Adapter
	model.Streamer
	model.CapsReporter
	model.OutputCap
	model.Warmer
	model.RateReporter
}

var (
	_ Backend = (*openai.Adapter)(nil)
	_ Backend = (*anthropic.Adapter)(nil)
)

// Timed wraps an adapter and records every call. It forwards each optional
// interface the adapters implement (OutputCap, CapsReporter, Warmer,
// Streamer, RateReporter); it implements no others, so the machine sees
// exactly what it would see without the wrapper.
type Timed struct {
	Adapter Backend
	Class   string // the execution group it serves, for records

	mu      sync.Mutex
	records []CallRecord
}

var (
	_ model.Adapter      = (*Timed)(nil)
	_ model.Streamer     = (*Timed)(nil)
	_ model.CapsReporter = (*Timed)(nil)
	_ model.OutputCap    = (*Timed)(nil)
	_ model.Warmer       = (*Timed)(nil)
	_ model.RateReporter = (*Timed)(nil)
)

// Fingerprint names a task by its request text.
func Fingerprint(input string) string {
	s := sha256.Sum256([]byte(input))
	return hex.EncodeToString(s[:8])
}

func requestPrint(req model.Request) string {
	for _, m := range req.Messages {
		if m.Role == task.RoleUser {
			return Fingerprint(m.TextContent())
		}
	}
	return ""
}

func (t *Timed) record(r CallRecord) {
	t.mu.Lock()
	t.records = append(t.records, r)
	t.mu.Unlock()
}

// Records returns a copy of every call recorded so far.
func (t *Timed) Records() []CallRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]CallRecord(nil), t.records...)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (t *Timed) Provider() string { return t.Adapter.Provider() }
func (t *Timed) Caps() model.Caps { return t.Adapter.Caps() }
func (t *Timed) MaxOut() int      { return t.Adapter.MaxOut() }
func (t *Timed) Warm(ctx context.Context) error {
	return t.Adapter.Warm(ctx)
}
func (t *Timed) RateInfo(resp *model.Response, err error) *model.RateInfo {
	return t.Adapter.RateInfo(resp, err)
}

func (t *Timed) Call(ctx context.Context, req model.Request) (model.Response, error) {
	r := CallRecord{Class: t.Class, Fingerprint: requestPrint(req), Start: time.Now()}
	resp, err := t.Adapter.Call(ctx, req)
	r.End, r.Err = time.Now(), errString(err)
	t.record(r)
	return resp, err
}

func (t *Timed) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	r := CallRecord{Class: t.Class, Fingerprint: requestPrint(req), Start: time.Now()}
	var once sync.Once
	resp, err := t.Adapter.Stream(ctx, req, func(d model.Delta) {
		once.Do(func() { r.First = time.Now() })
		emit(d)
	})
	r.End, r.Err = time.Now(), errString(err)
	t.record(r)
	return resp, err
}
