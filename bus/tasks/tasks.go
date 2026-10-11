package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

// Defaults.
const (
	// DefaultMaxText bounds a sent message, in bytes: it lands in the other
	// task's context whole.
	DefaultMaxText = 8 << 10
	// TitleLen is how much of a task's request tasks_list shows, in runes.
	TitleLen = 80
)

// Machine is what the device needs of the machine it serves;
// *neumann.Machine satisfies it.
type Machine interface {
	SteerWith(ctx context.Context, taskID, text string, o neumann.SteerOptions) error
	Load(ctx context.Context, id string) (*task.TCB, error)
	Unfinished(ctx context.Context) ([]*task.TCB, error)
}

var _ Machine = (*neumann.Machine)(nil)

// Visible decides whether caller may see other in a list, and send to it.
// Both are committed states, read when the call runs; caller is the task
// making the call.
type Visible func(caller, other *task.TCB) bool

type config struct {
	visible Visible
	maxText int
}

// Option configures a device.
type Option func(*config)

// WithVisible restricts which tasks a task sees and can send to. A task it
// may not see is reported as unknown, exactly as a task that does not
// exist. nil, the default: every unfinished task of the machine.
func WithVisible(v Visible) Option {
	return func(c *config) { c.visible = v }
}

// WithMaxText bounds a sent message, in bytes; n <= 0 keeps DefaultMaxText.
func WithMaxText(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxText = n
		}
	}
}

// Device lets tasks of one machine send messages to each other. Build it
// with New, pass it in neumann.Config.Devices, and Bind it to the machine
// New returns before calling Start.
type Device struct {
	name string
	cfg  config
	m    atomic.Pointer[bound]
}

type bound struct{ m Machine }

// New returns an unbound device. name is the prefix the model sees on its
// tools: with "tasks", tasks_send and tasks_list.
func New(name string, opts ...Option) *Device {
	d := &Device{name: name, cfg: config{maxText: DefaultMaxText}}
	for _, o := range opts {
		o(&d.cfg)
	}
	return d
}

// Bind gives the device the machine it serves. Bind it before Start: a call
// that recovery sends again runs as Start returns, and a call before Bind
// gets an error result.
func (d *Device) Bind(m Machine) { d.m.Store(&bound{m}) }

func (d *Device) Name() string { return d.name }

func (d *Device) Tools(context.Context) ([]bus.ToolDef, error) {
	return []bus.ToolDef{
		{
			Name: "send",
			Description: "Send a message to another unfinished task of this system (find ids with " + bus.FullName(d.name, "list") + "). " +
				"It reaches that task as a message headed [message from task:<your task id>] at its next step, " +
				"and it can answer the same way. Errors if the task is unknown or has finished.",
			Schema: json.RawMessage(`{"type":"object","properties":{` +
				`"task_id":{"type":"string","description":"The id of the task to send to."},` +
				`"text":{"type":"string","description":"The message."}},` +
				`"required":["task_id","text"]}`),
			// Idempotent: the call's key goes with the steer (SteerOptions.Key),
			// so a call sent again after a restart does not send twice.
			Effect: bus.Idempotent,
		},
		{
			Name:        "list",
			Description: "List the other unfinished tasks you can send messages to: id, class, state and the start of each task's request.",
			Schema:      json.RawMessage(`{"type":"object","properties":{}}`),
			Effect:      bus.ReadOnly,
		},
	}, nil
}

// Call runs send or list. What the model did wrong, and what a task cannot
// do (send to itself, to a task that has ended), comes back as an error
// result; only an unknown tool, or a store that fails, is a Go error, which
// the bus also turns into an error result.
func (d *Device) Call(ctx context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	b := d.m.Load()
	if b == nil {
		return refuse("the %s device is not bound to a machine", d.name)
	}
	if c.Task == "" {
		return refuse("the call names no calling task")
	}
	switch c.Tool {
	case "send":
		return d.send(ctx, b.m, c)
	case "list":
		return d.list(ctx, b.m, c.Task)
	}
	return task.ToolResult{}, fmt.Errorf("tasks %s: no tool %q", d.name, c.Tool)
}

func (d *Device) Close() error { return nil }

func refuse(format string, args ...any) (task.ToolResult, error) {
	return task.ToolResult{Content: fmt.Sprintf(format, args...), IsError: true}, nil
}

func (d *Device) send(ctx context.Context, m Machine, c bus.DeviceCall) (task.ToolResult, error) {
	var in struct {
		TaskID string `json:"task_id"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(c.Args, &in); err != nil {
		return refuse("bad arguments: %v", err)
	}
	switch {
	case in.TaskID == "":
		return refuse("task_id is required")
	case strings.TrimSpace(in.Text) == "":
		return refuse("text is empty")
	case len(in.Text) > d.cfg.maxText:
		return refuse("text is %d bytes, more than the limit of %d", len(in.Text), d.cfg.maxText)
	case in.TaskID == c.Task:
		return refuse("task %s is you: a task cannot send to itself", in.TaskID)
	}
	if d.cfg.visible != nil {
		ok, err := d.sees(ctx, m, c.Task, in.TaskID)
		if err != nil {
			return task.ToolResult{}, err
		}
		if !ok {
			return refuse("no task %s", in.TaskID)
		}
	}
	err := m.SteerWith(ctx, in.TaskID, in.Text, neumann.SteerOptions{From: "task:" + c.Task, Key: c.Key})
	switch {
	case errors.Is(err, neumann.ErrNotFound):
		return refuse("no task %s", in.TaskID)
	case errors.Is(err, neumann.ErrFinished):
		return refuse("task %s has finished; it can no longer receive messages", in.TaskID)
	case errors.Is(err, neumann.ErrStopped):
		return refuse("the machine is stopping; nothing was sent")
	case err != nil:
		return task.ToolResult{}, err
	}
	return task.ToolResult{Content: "sent to task " + in.TaskID}, nil
}

// sees reports whether caller may see other, by the device's Visible; an
// unknown other is not seen.
func (d *Device) sees(ctx context.Context, m Machine, caller, other string) (bool, error) {
	me, err := m.Load(ctx, caller)
	if err != nil {
		return false, err
	}
	them, err := m.Load(ctx, other)
	if errors.Is(err, neumann.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return d.cfg.visible(me, them), nil
}

// entry is one task as tasks_list shows it.
type entry struct {
	ID    string `json:"id"`
	Class string `json:"class"`
	State string `json:"state"`
	Wait  string `json:"wait,omitempty"` // what a waiting task waits for
	Title string `json:"title"`
}

func (d *Device) list(ctx context.Context, m Machine, caller string) (task.ToolResult, error) {
	all, err := m.Unfinished(ctx)
	if err != nil {
		return task.ToolResult{}, err
	}
	var me *task.TCB
	if d.cfg.visible != nil {
		for _, t := range all {
			if t.ID == caller {
				me = t
			}
		}
		if me == nil { // ended since: it still sees what it would have
			if me, err = m.Load(ctx, caller); err != nil {
				return task.ToolResult{}, err
			}
		}
	}
	out := []entry{}
	for _, t := range all {
		if t.ID == caller || (me != nil && !d.cfg.visible(me, t)) {
			continue
		}
		e := entry{ID: t.ID, Class: t.Class, State: t.State.String(), Title: Title(t)}
		if t.State == task.Waiting {
			e.Wait = t.Wait.String()
		}
		out = append(out, e)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return task.ToolResult{}, err
	}
	return task.ToolResult{Content: string(b)}, nil
}

// Title is the start of t's request, the first user message in its
// registers, on one line: at most TitleLen runes, "…" marking a cut.
func Title(t *task.TCB) string {
	var s string
	for _, msg := range t.Regs {
		if msg.Role == task.RoleUser {
			s = msg.TextContent()
			break
		}
	}
	s = strings.Join(strings.FieldsFunc(s, unicode.IsSpace), " ")
	if utf8.RuneCountInString(s) <= TitleLen {
		return s
	}
	return string([]rune(s)[:TitleLen-1]) + "…"
}
