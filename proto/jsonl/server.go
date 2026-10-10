package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/proto"
)

// Machine is what the server drives; *neumann.Machine implements it.
type Machine interface {
	Submit(ctx context.Context, in proto.Instruction) (string, error)
	Steer(ctx context.Context, taskID, text string) error
	Cancel(ctx context.Context, taskID string) error
	Resolve(ctx context.Context, taskID, callID string, o neumann.Outcome) error
	Approve(ctx context.Context, taskID, callID string, d neumann.ApprovalDecision) error
	Events(ctx context.Context, fromSeq int64) <-chan proto.Event
	Status() neumann.Status
	Stop(ctx context.Context) error

	SetClass(ctx context.Context, taskID, class string) error
	SetCores(class string, n int) error
	AutoScale(class string, on bool) error
	GroupReset(ctx context.Context, class string) error
	Queue() neumann.QueueStatus
}

var _ Machine = (*neumann.Machine)(nil)

// Defaults of a Server's zero fields.
const (
	DefaultStopTimeout  = 10 * time.Second
	DefaultDrainTimeout = 5 * time.Second
	DefaultMaxLine      = 16 << 20
	replyBuffer         = 256
)

// Server speaks the JSON Lines protocol for a started Machine. It owns the
// machine's shutdown: Serve stops it when the input ends.
type Server struct {
	Machine Machine
	// StopTimeout bounds the graceful Stop at the end of Serve.
	StopTimeout time.Duration
	// DrainTimeout bounds how long, after Stop, Serve keeps writing the
	// events the subscription has not delivered yet.
	DrainTimeout time.Duration
	// MaxLine is the longest input line accepted, in bytes.
	MaxLine int
	Logf    func(format string, args ...any)

	queued atomic.Int64 // replies handed to the writer, for tests
}

func (s *Server) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// Serve reads instructions from in and writes replies and events to out until
// in ends or ctx is cancelled. Instructions run one at a time, in the order
// they arrive, so their replies come back in that order and an instruction
// never overtakes an earlier one (a steer sent right after its submit finds
// the task). Then it stops the machine within StopTimeout, writes what the
// subscription still has to deliver within DrainTimeout, and returns.
//
// It returns nil after a clean end of input or cancellation, and the error
// otherwise: reading in failed, or writing out failed (the reader is gone).
// A failed write also ends the input loop, since no reply can be delivered.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	maxLine := s.MaxLine
	if maxLine <= 0 {
		maxLine = DefaultMaxLine
	}
	w := newWriter(s.Machine, out, cancel)
	go w.run()

	lines := make(chan line)
	go readLines(in, maxLine, lines)

	// Instructions do not use ctx: one being executed when the signal comes
	// finishes, so its reply tells the truth about what was committed.
	opCtx := context.WithoutCancel(ctx)
	var readErr error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case l, ok := <-lines:
			if !ok {
				break loop
			}
			if l.err != nil {
				readErr = l.err
				break loop
			}
			r, sub := s.handle(opCtx, l)
			if r == nil {
				continue
			}
			s.queued.Add(1)
			w.send(item{reply: *r, sub: sub})
		}
	}

	stopTimeout := s.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = DefaultStopTimeout
	}
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	if err := s.Machine.Stop(sctx); err != nil {
		s.logf("neumann: stop: %v", err)
	}
	scancel()

	drain := s.DrainTimeout
	if drain <= 0 {
		drain = DefaultDrainTimeout
	}
	if err := w.finish(drain); err != nil {
		return err
	}
	if readErr != nil {
		return fmt.Errorf("jsonl: read input: %w", readErr)
	}
	return nil
}

// handle executes one input line. A nil reply means the line was blank.
func (s *Server) handle(ctx context.Context, l line) (*Reply, *subscription) {
	if l.tooLong {
		return &Reply{Error: errorf(CodeBadRequest, "line longer than %d bytes", l.limit)}, nil
	}
	b := bytes.TrimSpace(l.b)
	if len(b) == 0 {
		return nil, nil
	}
	req, derr := Decode(b)
	if derr != nil {
		r := ErrorReply(req, derr)
		return &r, nil
	}
	r := Reply{ID: req.ID, Kind: req.Kind, OK: true}
	var err error
	var sub *subscription
	switch req.Kind {
	case proto.KindSubmit:
		r.TaskID, err = s.Machine.Submit(ctx, req.Instruction)
	case proto.KindSteer:
		r.TaskID = req.TaskID
		err = s.Machine.Steer(ctx, req.TaskID, req.Input)
	case proto.KindCancel:
		r.TaskID = req.TaskID
		err = s.Machine.Cancel(ctx, req.TaskID)
	case KindResolve:
		r.TaskID = req.TaskID
		o := req.Outcome
		err = s.Machine.Resolve(ctx, req.TaskID, req.CallID, neumann.Outcome{Retry: o.Retry, Result: o.Result, IsError: o.IsError})
	case KindApprove:
		r.TaskID = req.TaskID
		err = s.Machine.Approve(ctx, req.TaskID, req.CallID, approval(req.Decision))
	case KindStatus:
		r.Status, err = json.Marshal(s.Machine.Status())
	case KindClass:
		r.TaskID = req.TaskID
		err = s.Machine.SetClass(ctx, req.TaskID, req.Class)
	case KindCores:
		err = s.Machine.SetCores(req.Class, *req.Cores)
	case KindAutoScale:
		err = s.Machine.AutoScale(req.Class, *req.On)
	case KindGroupReset:
		err = s.Machine.GroupReset(ctx, req.Class)
	case KindQueue:
		r.Queue, err = json.Marshal(s.Machine.Queue())
	case KindSubscribe:
		from := int64(0)
		if req.FromSeq != nil {
			from = *req.FromSeq
		}
		r.FromSeq = &from
		sub = &subscription{from: from}
	case KindUnsubscribe:
		sub = &subscription{off: true}
	}
	if err != nil {
		r = ErrorReply(req, errorFor(err))
		sub = nil
	}
	return &r, sub
}

// approval is the machine's form of a decision Decode has checked.
func approval(d *Decision) neumann.ApprovalDecision {
	switch d.Verdict {
	case "deny":
		return neumann.ApprovalDecision{Verdict: neumann.ApproveDeny, Reason: d.Reason}
	case "edit":
		return neumann.ApprovalDecision{Verdict: neumann.ApproveEdit, Args: d.Args}
	}
	return neumann.ApprovalDecision{Verdict: neumann.ApproveAllow}
}

// errorFor maps the machine's errors to reply codes.
func errorFor(err error) *Error {
	e := &Error{Code: CodeFailed, Message: err.Error()}
	var ce *neumann.CapacityError
	var me *neumann.MigrateError
	switch {
	case errors.As(err, &ce):
		e.Code, e.Count, e.Limit = CodeBusy, ce.Count, ce.Limit
	case errors.As(err, &me):
		e.Code, e.Check = CodeIncompatible, me.Check
	case errors.Is(err, neumann.ErrTaskBusy):
		e.Code = CodeTaskBusy
	case errors.Is(err, neumann.ErrBusy):
		e.Code = CodeBusy
	case errors.Is(err, neumann.ErrNotReconciling):
		e.Code = CodeNotReconciling
	case errors.Is(err, neumann.ErrNotAwaitingApproval):
		e.Code = CodeNotAwaiting
	case errors.Is(err, neumann.ErrNotFound):
		e.Code = CodeNotFound
	case errors.Is(err, neumann.ErrFinished):
		e.Code = CodeFinished
	case errors.Is(err, neumann.ErrStopped):
		e.Code = CodeStopped
	case errors.Is(err, neumann.ErrNotStarted):
		e.Code = CodeNotStarted
	}
	return e
}

// line is one input line, or why there is none.
type line struct {
	b       []byte
	tooLong bool
	limit   int
	err     error // reading failed; the input is over
}

// readLines splits in into lines of at most max bytes. A longer line is
// skipped up to its newline and reported, so one bad line costs one error
// reply and nothing else. The channel is closed at end of input.
func readLines(in io.Reader, max int, out chan<- line) {
	defer close(out)
	r := bufio.NewReaderSize(in, 64<<10)
	var buf []byte
	tooLong := false
	for {
		chunk, err := r.ReadSlice('\n')
		if !tooLong {
			if len(buf)+len(chunk) > max+1 { // +1: the newline does not count
				tooLong, buf = true, buf[:0]
			} else {
				buf = append(buf, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(chunk) > 0 && chunk[len(chunk)-1] == '\n' || (err == io.EOF && (len(buf) > 0 || tooLong)) {
			l := line{b: bytes.TrimRight(buf, "\r\n"), tooLong: tooLong, limit: max}
			if len(l.b) > max {
				l.tooLong = true
			}
			out <- l
			buf, tooLong = nil, false
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			out <- line{err: err}
			return
		}
	}
}
