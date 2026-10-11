package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/task"
)

// Defaults.
const (
	// DefaultMaxText bounds a sent message, in bytes: it lands in the other
	// task's context whole.
	DefaultMaxText = 8 << 10
	// DefaultMaxSends bounds the messages one task sends: two tasks that
	// answer every message would otherwise talk until a budget stops them.
	DefaultMaxSends = 32
)

// Machine is what the device needs of the machine it serves;
// *neumann.Machine satisfies it.
type Machine interface {
	SteerWith(ctx context.Context, taskID, text string, o neumann.SteerOptions) error
	HasSteer(ctx context.Context, taskID, key string) (bool, error)
	Tasks() []neumann.TaskInfo
	Load(ctx context.Context, id string) (*task.TCB, error)
	Calls(ctx context.Context, id string) ([]mem.CallRecord, error)
}

var _ Machine = (*neumann.Machine)(nil)

// Visible decides whether caller may see other in a list, and send to it.
// Both are as last committed; caller is the task making the call.
type Visible func(caller, other neumann.TaskInfo) bool

// SameClass is the default Visible: tasks see the tasks of their own class
// (execution group) only.
func SameClass(caller, other neumann.TaskInfo) bool { return caller.Class == other.Class }

// All lets every task see every unfinished task of the machine.
func All(_, _ neumann.TaskInfo) bool { return true }

type config struct {
	visible  Visible
	maxText  int
	maxSends int
}

// Option configures a device.
type Option func(*config)

// WithVisible sets which tasks a task sees and can send to (default
// SameClass; All for every task). A task it may not see is reported as
// unknown, exactly as a task that does not exist. nil keeps the default.
func WithVisible(v Visible) Option {
	return func(c *config) {
		if v != nil {
			c.visible = v
		}
	}
}

// WithMaxText bounds a sent message, in bytes; n <= 0 keeps DefaultMaxText.
func WithMaxText(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxText = n
		}
	}
}

// WithMaxSends bounds the messages one task may send, over its whole life
// (counted from its call log, so a restart does not reset it); a send past
// it is an error result. n < 0: no bound; 0 keeps DefaultMaxSends.
func WithMaxSends(n int) Option {
	return func(c *config) {
		if n != 0 {
			c.maxSends = n
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
	d := &Device{name: name, cfg: config{visible: SameClass, maxText: DefaultMaxText, maxSends: DefaultMaxSends}}
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
				"It reaches that task at its next step as a message from task:<your task id>, marked as not from its user, " +
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
	// A call sent again after a restart is answered as it was the first
	// time, before anything that may have changed since (the receiver has
	// finished, the limits are lower) is looked at.
	if in.TaskID != "" && c.Key != "" {
		had, err := m.HasSteer(ctx, in.TaskID, c.Key)
		if err != nil {
			return task.ToolResult{}, err
		}
		if had {
			return sent(in.TaskID)
		}
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
	case !neumann.ValidFrom("task:" + c.Task):
		return refuse("your task id %q cannot name a sender (only [A-Za-z0-9:._@/-])", c.Task)
	}
	me, err := d.info(ctx, m, c.Task)
	if err != nil {
		return task.ToolResult{}, err
	}
	them, err := d.info(ctx, m, in.TaskID)
	switch {
	case errors.Is(err, neumann.ErrNotFound):
		return refuse("no task %s", in.TaskID)
	case err != nil:
		return task.ToolResult{}, err
	case !d.cfg.visible(me, them):
		return refuse("no task %s", in.TaskID)
	case them.State.Terminal():
		return refuse("task %s has finished; it can no longer receive messages", in.TaskID)
	}
	if d.cfg.maxSends > 0 {
		n, err := d.sends(ctx, m, c)
		if err != nil {
			return task.ToolResult{}, err
		}
		if n >= d.cfg.maxSends {
			return refuse("you have sent %d messages, the limit; send no more", n)
		}
	}
	err = m.SteerWith(ctx, in.TaskID, in.Text, neumann.SteerOptions{From: "task:" + c.Task, Key: c.Key})
	switch {
	case errors.Is(err, neumann.ErrNotFound):
		return refuse("no task %s", in.TaskID)
	case errors.Is(err, neumann.ErrFinished):
		return refuse("task %s has finished; it can no longer receive messages", in.TaskID)
	case err != nil:
		return task.ToolResult{}, err
	}
	return sent(in.TaskID)
}

func sent(id string) (task.ToolResult, error) {
	return task.ToolResult{Content: "sent to task " + id}, nil
}

// info is a task as Tasks shows it, or, for one that is not there (it has
// finished), as loaded from the store.
func (d *Device) info(ctx context.Context, m Machine, id string) (neumann.TaskInfo, error) {
	for _, t := range m.Tasks() {
		if t.ID == id {
			return t, nil
		}
	}
	t, err := m.Load(ctx, id)
	if err != nil {
		return neumann.TaskInfo{}, err
	}
	return neumann.TaskInfo{ID: t.ID, Class: t.Class, State: t.State, Wait: t.Wait, Title: neumann.Title(t)}, nil
}

// sends counts the messages the caller sent before this call: its earlier
// send calls (by call index) that went out or may have (done without an
// error, dispatched, unknown). A refused send does not count, and neither
// does this one, so a call sent again after a restart counts the same.
func (d *Device) sends(ctx context.Context, m Machine, c bus.DeviceCall) (int, error) {
	calls, err := m.Calls(ctx, c.Task)
	if err != nil {
		return 0, err
	}
	me := -1
	if parts := strings.Split(c.Key, "/"); len(parts) >= 3 {
		for _, r := range calls {
			if r.CallID == parts[len(parts)-2] {
				me = r.Index
			}
		}
	}
	tool, n := bus.FullName(d.name, "send"), 0
	for _, r := range calls {
		if r.Tool != tool || (me >= 0 && r.Index >= me) {
			continue
		}
		switch r.Status {
		case mem.CallDone:
			if r.Result != nil && !r.Result.IsError {
				n++
			}
		case mem.CallDispatched, mem.CallUnknown:
			n++
		}
	}
	return n, nil
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
	me, err := d.info(ctx, m, caller)
	if err != nil {
		return task.ToolResult{}, err
	}
	out := []entry{}
	for _, t := range m.Tasks() {
		if t.ID == caller || !d.cfg.visible(me, t) {
			continue
		}
		e := entry{ID: t.ID, Class: t.Class, State: t.State.String(), Title: t.Title}
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
