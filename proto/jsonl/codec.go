package jsonl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/liliang-cn/neumann/proto"
)

// Line types: the first field of every output line says which it is.
const (
	TypeReply = "reply"
	TypeEvent = "event"
)

// Instruction kinds the frontend adds to proto's submit, steer and cancel.
const (
	KindResolve     = "resolve"
	KindStatus      = "status"
	KindSubscribe   = "subscribe"
	KindUnsubscribe = "unsubscribe"

	// Operator controls (design §9.6).
	KindClass      = "class"       // move a task to another group: Machine.SetClass
	KindCores      = "cores"       // pin a group's window: Machine.SetCores
	KindAutoScale  = "autoscale"   // let a group's window adapt again, or freeze it: Machine.AutoScale
	KindGroupReset = "group_reset" // close a group's breaker: Machine.GroupReset
	KindQueue      = "queue"       // the queues by class: Machine.Queue
)

// KnownKinds are the instructions this frontend executes.
var KnownKinds = []string{
	proto.KindSubmit, proto.KindSteer, proto.KindCancel, KindResolve,
	KindStatus, KindSubscribe, KindUnsubscribe,
	KindClass, KindCores, KindAutoScale, KindGroupReset, KindQueue,
}

// UnsupportedKinds are operator controls the design names (§9.6) that the
// machine does not offer yet. They are answered with CodeUnsupported, not
// CodeUnknownKind, so a client can tell "not yet" from "never heard of it".
var UnsupportedKinds = []string{"suspend", "resume", "priority"}

// Error codes of a failed reply.
const (
	CodeBadRequest     = "bad_request"     // malformed JSON, a missing, unknown or ill-typed field
	CodeUnknownKind    = "unknown_kind"    // kind is not an instruction at all
	CodeUnsupported    = "unsupported"     // a designed operator control the machine does not have yet
	CodeNotFound       = "not_found"       // no such task
	CodeFinished       = "finished"        // the task has already ended
	CodeNotReconciling = "not_reconciling" // resolve: the task or call is not held for an operator
	CodeTaskBusy       = "task_busy"       // class: the task is executing an instruction or waiting for its tools; try again
	CodeIncompatible   = "incompatible"    // class: the target group cannot take the task; check says which check failed
	CodeBusy           = "busy"            // submit: the machine is at capacity; count and limit say how full
	CodeStopped        = "stopped"         // the machine is stopping
	CodeNotStarted     = "not_started"     // the machine has not finished starting
	CodeFailed         = "failed"          // anything else: the message says what
)

// Outcome is an operator's answer to a call held for reconciliation: Retry
// sends the call again; otherwise Result is what the call returned and
// IsError marks a failure.
type Outcome struct {
	Result  string `json:"result,omitempty"`
	IsError bool   `json:"is_error,omitempty"`
	Retry   bool   `json:"retry,omitempty"`
}

// Request is one input line: an instruction, plus the optional id the reply
// echoes and the fields of the frontend's own kinds.
type Request struct {
	// ID is any JSON string or number; the reply carries it back verbatim.
	ID json.RawMessage `json:"id,omitempty"`
	proto.Instruction
	CallID  string   `json:"call_id,omitempty"`  // resolve
	Outcome *Outcome `json:"outcome,omitempty"`  // resolve
	FromSeq *int64   `json:"from_seq,omitempty"` // subscribe; absent means 0, every event
	Cores   *int     `json:"cores,omitempty"`    // cores
	On      *bool    `json:"on,omitempty"`       // autoscale
}

// Error is why an instruction failed. Count and Limit are set for CodeBusy,
// Check for CodeIncompatible.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Count   int    `json:"count,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Check   string `json:"check,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Reply answers one Request. OK means the instruction is done and, for
// submit, steer, cancel and resolve, already durable.
type Reply struct {
	Type    string          `json:"type"` // always TypeReply; Marshal sets it
	ID      json.RawMessage `json:"id,omitempty"`
	Kind    string          `json:"kind,omitempty"`
	OK      bool            `json:"ok"`
	TaskID  string          `json:"task_id,omitempty"`
	FromSeq *int64          `json:"from_seq,omitempty"` // subscribe: where the stream starts
	Status  json.RawMessage `json:"status,omitempty"`   // status: the machine's snapshot
	Queue   json.RawMessage `json:"queue,omitempty"`    // queue: the queues by class
	Error   *Error          `json:"error,omitempty"`
}

// EventLine is a committed event on the output stream.
type EventLine struct {
	Type string `json:"type"` // always TypeEvent; Marshal sets it
	proto.Event
}

// Line is what a client reads first to tell a reply from an event.
type Line struct {
	Type string `json:"type"`
}

// ErrorReply is the failed reply to req.
func ErrorReply(req Request, err *Error) Reply {
	return Reply{ID: req.ID, Kind: req.Kind, Error: err}
}

// Marshal renders a Reply or an EventLine as one line, newline included. HTML
// characters are left as they are: the reader is a program, not a browser.
func Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case Reply:
		x.Type = TypeReply
		v = x
	case EventLine:
		x.Type = TypeEvent
		v = x
	default:
		return nil, fmt.Errorf("jsonl: cannot marshal %T as a line", v)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// fields lists, per kind, the fields an instruction may carry besides id and
// kind. Anything else is refused by name: a misspelt "taskid" is an error,
// not a field that is silently ignored.
var fields = map[string][]string{
	proto.KindSubmit: {"task_id", "input", "class", "pin_class", "priority", "budget", "allow", "deny"},
	proto.KindSteer:  {"task_id", "input"},
	proto.KindCancel: {"task_id"},
	KindResolve:      {"task_id", "call_id", "outcome"},
	KindStatus:       {},
	KindSubscribe:    {"from_seq"},
	KindUnsubscribe:  {},
	KindClass:        {"task_id", "class"},
	KindCores:        {"class", "cores"},
	KindAutoScale:    {"class", "on"},
	KindGroupReset:   {"class"},
	KindQueue:        {},
}

// Decode parses and validates one input line. On error the returned Request
// still carries whatever id and kind could be read, so the error reply can be
// matched to its request.
func Decode(line []byte) (Request, *Error) {
	var req Request
	if !json.Valid(line) {
		var v any
		err := json.Unmarshal(line, &v)
		return req, errorf(CodeBadRequest, "malformed JSON: %v", err)
	}
	if t := bytes.TrimSpace(line); len(t) == 0 || t[0] != '{' {
		return req, errorf(CodeBadRequest, "a line must be a JSON object, like {\"kind\":\"status\"}")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(line, &obj); err != nil {
		return req, errorf(CodeBadRequest, "malformed JSON: %v", err)
	}

	if raw, ok := obj["id"]; ok {
		switch t := bytes.TrimSpace(raw); {
		case string(t) == "null":
		case t[0] == '"' || t[0] == '-' || (t[0] >= '0' && t[0] <= '9'):
			req.ID = slices.Clone(t)
		default:
			return req, errorf(CodeBadRequest, `"id" must be a string or a number`)
		}
	}
	raw, ok := obj["kind"]
	if !ok {
		return req, errorf(CodeBadRequest, `missing "kind"`)
	}
	if err := json.Unmarshal(raw, &req.Kind); err != nil {
		return req, errorf(CodeBadRequest, `"kind" must be a string`)
	}
	if slices.Contains(UnsupportedKinds, req.Kind) {
		return req, errorf(CodeUnsupported, "kind %q is not supported by this machine yet", req.Kind)
	}
	allowed, ok := fields[req.Kind]
	if !ok {
		return req, errorf(CodeUnknownKind, "unknown kind %q; known kinds: %s", req.Kind, strings.Join(KnownKinds, ", "))
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if k == "id" || k == "kind" {
			continue
		}
		if !slices.Contains(allowed, k) {
			return req, errorf(CodeBadRequest, "field %q is not used by kind %q", k, req.Kind)
		}
		if err := strict(obj[k], req.field(k)); err != nil {
			return req, errorf(CodeBadRequest, "field %q: %v", k, err)
		}
	}
	return req, req.validate()
}

// field is where the JSON field name is decoded to.
func (r *Request) field(name string) any {
	switch name {
	case "task_id":
		return &r.TaskID
	case "input":
		return &r.Input
	case "class":
		return &r.Class
	case "pin_class":
		return &r.PinClass
	case "cores":
		return &r.Cores
	case "on":
		return &r.On
	case "priority":
		return &r.Priority
	case "budget":
		return &r.Budget
	case "allow":
		return &r.Allow
	case "deny":
		return &r.Deny
	case "call_id":
		return &r.CallID
	case "outcome":
		return &r.Outcome
	case "from_seq":
		return &r.FromSeq
	}
	panic("jsonl: no field " + name)
}

// strict decodes raw into v, refusing fields v does not have.
func strict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// validate checks what each kind requires. The machine checks again what only
// it can know (a class it runs, a priority it supports).
func (r *Request) validate() *Error {
	need := func(field, value string) *Error {
		if strings.TrimSpace(value) == "" {
			return errorf(CodeBadRequest, "%s needs %q", r.Kind, field)
		}
		return nil
	}
	switch r.Kind {
	case proto.KindSubmit:
		return need("input", r.Input)
	case proto.KindSteer:
		if err := need("task_id", r.TaskID); err != nil {
			return err
		}
		return need("input", r.Input)
	case proto.KindCancel:
		return need("task_id", r.TaskID)
	case KindResolve:
		if err := need("task_id", r.TaskID); err != nil {
			return err
		}
		if err := need("call_id", r.CallID); err != nil {
			return err
		}
		if r.Outcome == nil {
			return errorf(CodeBadRequest, `resolve needs "outcome": {"result":"..."}, {"result":"...","is_error":true} or {"retry":true}`)
		}
		if r.Outcome.Retry && (r.Outcome.Result != "" || r.Outcome.IsError) {
			return errorf(CodeBadRequest, `outcome: "retry" sends the call again and cannot carry a "result" or "is_error"`)
		}
	case KindClass:
		if err := need("task_id", r.TaskID); err != nil {
			return err
		}
		return need("class", r.Class)
	case KindGroupReset:
		return need("class", r.Class)
	case KindCores:
		if err := need("class", r.Class); err != nil {
			return err
		}
		if r.Cores == nil || *r.Cores < 1 {
			return errorf(CodeBadRequest, `cores needs "cores", a count of at least 1`)
		}
	case KindAutoScale:
		if err := need("class", r.Class); err != nil {
			return err
		}
		if r.On == nil {
			return errorf(CodeBadRequest, `autoscale needs "on": true or false`)
		}
	case KindSubscribe:
		if r.FromSeq != nil && *r.FromSeq < 0 {
			return errorf(CodeBadRequest, `"from_seq" must be >= 0`)
		}
	}
	return nil
}
