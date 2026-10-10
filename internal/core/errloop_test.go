package core

import (
	"testing"
	"time"

	"github.com/liliang-cn/neumann/internal/group"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

func cancelBehind(t *testing.T, store *mem.Memory) {
	t.Helper()
	if _, err := store.Commit(bg, mem.CommitSet{NewInterrupts: []mem.Interrupt{{TaskID: "t1", Kind: mem.InterruptCancel, Text: ReasonCancelled}}}); err != nil {
		t.Fatal(err)
	}
}

// design §4.1 rule 4 on the commits this step adds: a cancel committed while
// the model was answering makes the store refuse the backoff, the wait for
// the group and the truncation redo alike, and the step finalises the task
// instead, keeping what it learned for certain (the step, the usage).
func TestRetryCommitsYieldToACommittedCancel(t *testing.T) {
	for name, fail := range map[string]func() (model.Response, error){
		"backoff":   func() (model.Response, error) { return model.Response{}, sim.Transient() },
		"ratelimit": func() (model.Response, error) { return model.Response{}, sim.RateLimited(time.Minute) },
		"group":     func() (model.Response, error) { return model.Response{}, sim.QuotaExhausted() },
		"truncated": func() (model.Response, error) { return sim.Truncated(), nil },
	} {
		t.Run(name, func(t *testing.T) {
			var store *mem.Memory
			e, s, sm := newTestEnv(t, func(model.Request) (model.Response, error) {
				cancelBehind(t, store)
				return fail()
			})
			store = s
			e.Groups = map[string]*Group{"default": {Class: "default", Ctl: group.New(group.Config{})}}
			e.step(bg, newTask(t, e, s, "q"), 0)
			got := load(t, s)
			if got.State != task.Cancelled || got.StopReason != ReasonCancelled || got.Used.Steps != 1 || sm.Calls() != 1 {
				t.Fatalf("task %s %q, steps %d, calls %d", got.State, got.StopReason, got.Used.Steps, sm.Calls())
			}
			if hasKind(s, "model_retry") || !hasKind(s, "step") {
				t.Fatalf("events %s", kinds(s))
			}
			if r, w, h := e.Sched.Counts(); r+w+h != 0 {
				t.Fatalf("left behind %d/%d/%d", r, w, h)
			}
		})
	}
}

// A wake-up refused by a committed cancel finalises the task instead.
func TestWakeYieldsToACommittedCancel(t *testing.T) {
	for _, w := range []task.WaitReason{task.WaitBackoff, task.WaitGroup} {
		e, s, _ := newTestEnv(t, reply)
		tc := newTask(t, e, s, "q")
		tc.State, tc.Wait, tc.WakeAt, tc.Version = task.Waiting, w, time.Now().Add(time.Hour), 2
		if _, err := s.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
			t.Fatal(err)
		}
		e.Sched.Add(tc)
		cancelBehind(t, s)
		if e.Wake(bg, "t1", w) {
			t.Fatalf("%s: woke a cancelled task", w)
		}
		if got := load(t, s); got.State != task.Cancelled {
			t.Fatalf("%s: task %s", w, got.State)
		}
		if r, wt, h := e.Sched.Counts(); r+wt+h != 0 {
			t.Fatalf("%s: left behind %d/%d/%d", w, r, wt, h)
		}
	}
}

// Waking a task in another wait, or one already gone, does nothing.
func TestWakeOnlyTakesItsOwnWait(t *testing.T) {
	e, s, _ := newTestEnv(t, reply)
	tc := newTask(t, e, s, "q")
	tc.State, tc.Wait, tc.Version = task.Waiting, task.WaitReconcile, 2
	if _, err := s.Commit(bg, mem.CommitSet{Task: tc}); err != nil {
		t.Fatal(err)
	}
	e.Sched.Add(tc)
	if e.Wake(bg, "t1", task.WaitBackoff) || e.Wake(bg, "nobody", task.WaitBackoff) {
		t.Fatal("woke the wrong task")
	}
	if got := load(t, s); got.State != task.Waiting || got.Version != 2 {
		t.Fatalf("task %s v%d", got.State, got.Version)
	}
}
