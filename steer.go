package neumann

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/liliang-cn/neumann/internal/core"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/task"
)

// MaxSteerFrom bounds SteerOptions.From, in bytes.
const MaxSteerFrom = 256

// SteerOptions are a steer's optional parts (SteerWith). The zero value is
// a plain Steer.
type SteerOptions struct {
	// From names the sender: "operator", "user", "task:<id>" (what the
	// bus/tasks device sends), or any other name of 1 to MaxSteerFrom bytes
	// of [A-Za-z0-9:._@/-]. It is committed with the steer. The model gets
	// the message through Config.MessageFormat, which by default says it is
	// this sender's and not the user's; the steer_delivered event (sent to
	// the model) or steer_dropped event (the task ended first) carries it as
	// from. Empty: no sender, and nothing differs from a steer before
	// senders existed — SteerFormat, no steer_delivered event, no from.
	From string
	// Key makes the steer idempotent: a steer whose Key the task already has
	// (pending, delivered or dropped) is not committed again, and SteerWith
	// returns nil as it did for the first, even once the task has ended. A
	// device passes its call's idempotency key, so a call sent again after a
	// restart does not steer twice. A steer with a Key is also admitted
	// while Stop drains the tool batches, so a device call that sends one
	// then is not refused. Empty: every call is a new steer. The store must
	// keep Key (mem.Interrupt.Key) for any of this to hold across a restart.
	Key string
}

// ValidFrom reports whether from may name a steer's sender: 1 to
// MaxSteerFrom bytes of [A-Za-z0-9:._@/-]. Nothing else can appear in the
// frame a message is delivered in: no bracket, space, line break, or
// character that renders as one or reorders text.
func ValidFrom(from string) bool {
	if from == "" || len(from) > MaxSteerFrom {
		return false
	}
	for i := 0; i < len(from); i++ {
		switch c := from[i]; {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == ':', c == '.', c == '_', c == '@', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
}

// DefaultMessageFormat frames a steer that has a sender. It says, before the
// message, who sent it, that the sender is not the user, and that the
// message is information to weigh rather than the user's instructions: a
// message from another task carries whatever that task read, and a task
// reading untrusted content must not be able to speak to another task as
// its user. The text follows quoted (QuoteMessage), so no line of it can
// pass for the frame or for another message's.
func DefaultMessageFormat(from, text string) string { return core.DefaultMessageFormat(from, text) }

// QuoteMessage puts "> " before every line of text. Every line break a
// reader could see is one: \n, \r\n, \r, vertical tab, form feed, the
// file, group and record separators, U+0085, U+2028 and U+2029 all become \n
// first. NUL becomes U+FFFD, as invalid UTF-8 does.
func QuoteMessage(text string) string { return core.QuoteMessage(text) }

// SteerWith is Steer with a sender and an idempotency key (SteerOptions).
func (m *Machine) SteerWith(ctx context.Context, taskID, text string, o SteerOptions) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("neumann: steer needs text")
	}
	if o.From != "" && !ValidFrom(o.From) {
		return fmt.Errorf("neumann: steer sender %q: at most %d bytes of [A-Za-z0-9:._@/-]", o.From, MaxSteerFrom)
	}
	if o.Key == "" {
		if err := m.enter(); err != nil {
			return err
		}
		defer m.inflight.Done()
		return m.commitSteer(ctx, taskID, text, o)
	}
	if err := m.enterKeyed(); err != nil {
		return err
	}
	defer m.keyedIn.Done()
	unlock, err := m.keyLocks.lock(ctx, taskID)
	if err != nil {
		return err
	}
	defer unlock()
	if had, err := m.hasSteer(ctx, taskID, o.Key); err != nil || had {
		return err
	}
	return m.commitSteer(ctx, taskID, text, o)
}

func (m *Machine) commitSteer(ctx context.Context, taskID, text string, o SteerOptions) error {
	_, err := m.store.Commit(ctx, mem.CommitSet{NewInterrupts: []mem.Interrupt{
		{TaskID: taskID, Kind: mem.InterruptSteer, Text: text, From: o.From, Key: o.Key},
	}})
	if err == nil {
		m.hub.notify()
	}
	return err
}

// HasSteer reports whether a task has a steer committed with key (pending,
// delivered or dropped); false for an unknown task. A device that sends with
// SteerWith asks it first, so that a call sent again after a restart is
// answered as it was the first time, whatever has changed since.
func (m *Machine) HasSteer(ctx context.Context, taskID, key string) (bool, error) {
	if key == "" {
		return false, nil
	}
	return m.hasSteer(ctx, taskID, key)
}

func (m *Machine) hasSteer(ctx context.Context, taskID, key string) (bool, error) {
	had, err := m.store.Interrupts(ctx, taskID, false)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(had, func(in mem.Interrupt) bool { return in.Kind == mem.InterruptSteer && in.Key == key }), nil
}

// enterKeyed admits a keyed steer: from Start until Stop has drained the
// tool batches. The caller must call m.keyedIn.Done when its commit is over.
func (m *Machine) enterKeyed() error {
	m.admit.Lock()
	defer m.admit.Unlock()
	switch {
	case m.drained:
		return ErrStopped
	case !m.accepting && m.stopped:
		return ErrStopped
	case !m.accepting:
		return ErrNotStarted
	}
	m.keyedIn.Add(1)
	return nil
}

// drainKeyed refuses keyed steers from now on and waits for those admitted.
func (m *Machine) drainKeyed() {
	m.admit.Lock()
	m.drained = true
	m.admit.Unlock()
	m.keyedIn.Wait()
}

// keyLocks is one lock per task, for keyed steers: the look for a key and
// the commit of the steer happen under it, so two calls with one key cannot
// both commit. Steers to different tasks do not wait for each other, and a
// wait ends with its context.
type keyLocks struct {
	mu sync.Mutex
	m  map[string]*keyLock
}

type keyLock struct {
	ch    chan struct{}
	users int
}

func (k *keyLocks) lock(ctx context.Context, id string) (unlock func(), err error) {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*keyLock{}
	}
	l := k.m[id]
	if l == nil {
		l = &keyLock{ch: make(chan struct{}, 1)}
		k.m[id] = l
	}
	l.users++
	k.mu.Unlock()
	leave := func() {
		k.mu.Lock()
		if l.users--; l.users == 0 {
			delete(k.m, id)
		}
		k.mu.Unlock()
	}
	select {
	case l.ch <- struct{}{}:
		return func() { <-l.ch; leave() }, nil
	case <-ctx.Done():
		leave()
		return nil, ctx.Err()
	}
}

// TitleLen is how much of a task's request TaskInfo.Title holds, in runes.
const TitleLen = 80

// TaskInfo is a light view of an unfinished task (Machine.Tasks): no
// registers, no store read.
type TaskInfo struct {
	ID    string          `json:"id"`
	Class string          `json:"class"`
	State task.State      `json:"state"`
	Wait  task.WaitReason `json:"wait"`  // what a Waiting task waits for
	Title string          `json:"title"` // the start of its request (Title)
}

// Title is the start of t's request, its first user message, on one line:
// at most TitleLen runes, "…" marking a cut.
func Title(t *task.TCB) string {
	var s string
	for _, msg := range t.Regs {
		if msg.Role == task.RoleUser {
			s = msg.TextContent()
			break
		}
	}
	s = strings.Join(strings.Fields(strings.ToValidUTF8(s, "�")), " ")
	if utf8.RuneCountInString(s) <= TitleLen {
		return s
	}
	return string([]rune(s)[:TitleLen-1]) + "…"
}

// Tasks is every unfinished task as last committed, ordered by id. Like
// Status it is a cheap copy kept in memory as commits land: no store read,
// no registers decoded.
// It is updated right after a commit's events are stored, so a reader of
// Events can see a commit a moment before Tasks does.
func (m *Machine) Tasks() []TaskInfo { return m.index.list() }

// taskIndex is Tasks' source: set from every commit that carries a task,
// and from recovery.
type taskIndex struct {
	mu sync.Mutex
	m  map[string]TaskInfo
}

func (x *taskIndex) put(t *task.TCB) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if t.State.Terminal() {
		delete(x.m, t.ID)
		return
	}
	if x.m == nil {
		x.m = map[string]TaskInfo{}
	}
	info, ok := x.m[t.ID]
	if !ok {
		info = TaskInfo{ID: t.ID, Title: Title(t)} // the request never changes
	}
	info.Class, info.State, info.Wait = t.Class, t.State, t.Wait
	x.m[t.ID] = info
}

func (x *taskIndex) drop(id string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.m, id)
}

func (x *taskIndex) list() []TaskInfo {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make([]TaskInfo, 0, len(x.m))
	for _, info := range x.m {
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b TaskInfo) int { return strings.Compare(a.ID, b.ID) })
	return out
}
