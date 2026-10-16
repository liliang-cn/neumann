package core

import (
	"strconv"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/task"
)

// landSuspend (design §9.9): the last pending suspend or resume decides;
// all of them are applied where it lands; a suspend lands on a task that
// is ready or waiting for a backoff or its group, and waits on one waiting
// for its tools or an operator; a resume makes a suspended task ready, and
// one that cancels a suspend before it landed says so; a terminal commit is
// left to the store; a suspend of a suspended task is applied silently.
func TestLandSuspend(t *testing.T) {
	sus := mem.Interrupt{Seq: 1, Kind: mem.InterruptSuspend}
	res := mem.Interrupt{Seq: 2, Kind: mem.InterruptResume}
	sus2 := mem.Interrupt{Seq: 3, Kind: mem.InterruptSuspend}
	steer := mem.Interrupt{Seq: 4, Kind: mem.InterruptSteer, Text: "hi"}
	ready := task.TCB{State: task.Ready}
	backoff := task.TCB{State: task.Waiting, Wait: task.WaitBackoff}
	io := task.TCB{State: task.Waiting, Wait: task.WaitIO}
	suspended := task.TCB{State: task.Suspended}
	for _, c := range []struct {
		name    string
		t       task.TCB
		pending []mem.Interrupt
		state   task.State
		applied string
		events  string
	}{
		{"nothing pending", ready, []mem.Interrupt{steer}, task.Ready, "", ""},
		{"suspend a ready task", ready, []mem.Interrupt{sus, steer}, task.Suspended, "1", `suspended {"kept":0,"kept_think":0,"was":"x"}`},
		{"suspend a backoff", backoff, []mem.Interrupt{sus}, task.Suspended, "1", `suspended {"kept":0,"kept_think":0,"was":"x"}`},
		{"suspend, resume", ready, []mem.Interrupt{sus, res}, task.Ready, "1,2", `resumed {"pending":true}`},
		{"suspend, resume, suspend", ready, []mem.Interrupt{sus, res, sus2}, task.Suspended, "1,2,3", `suspended {"kept":0,"kept_think":0,"was":"x"}`},
		{"a resume alone", ready, []mem.Interrupt{res}, task.Ready, "2", ""},
		{"suspend while tools run", io, []mem.Interrupt{sus}, task.Waiting, "", ""},
		{"suspend, resume while tools run", io, []mem.Interrupt{sus, res}, task.Waiting, "1,2", `resumed {"pending":true}`},
		{"resume a suspended task", suspended, []mem.Interrupt{res}, task.Ready, "2", `resumed {"pending":false}`},
		{"suspend a suspended task", suspended, []mem.Interrupt{sus}, task.Suspended, "1", ""},
		{"finished", task.TCB{State: task.Done}, []mem.Interrupt{sus}, task.Done, "", ""},
	} {
		e := &Env{Clock: sched.RealClock{}}
		tc := c.t
		got := e.landSuspend(&tc, mem.CommitSet{Task: &tc, Applied: []int64{9}}, c.pending, "x", "", "")
		var applied, events []string
		for _, s := range got.Applied[1:] {
			applied = append(applied, strconv.FormatInt(s, 10))
		}
		for _, ev := range got.Events {
			events = append(events, ev.Kind+" "+string(ev.Data))
		}
		if tc.State != c.state || strings.Join(applied, ",") != c.applied || strings.Join(events, ";") != c.events {
			t.Errorf("%s: %s, applied %v, events %v", c.name, tc.State, applied, events)
		}
		if got.Applied[0] != 9 {
			t.Errorf("%s: the commit's own applied interrupts were lost", c.name)
		}
	}
}

// suspendedAfter: the last suspend or resume decides, the state when there
// is none.
func TestSuspendedAfter(t *testing.T) {
	sus := mem.Interrupt{Kind: mem.InterruptSuspend}
	res := mem.Interrupt{Kind: mem.InterruptResume}
	steer := mem.Interrupt{Kind: mem.InterruptSteer}
	for _, c := range []struct {
		state   task.State
		pending []mem.Interrupt
		want    bool
	}{
		{task.Ready, nil, false},
		{task.Suspended, []mem.Interrupt{steer}, true},
		{task.Running, []mem.Interrupt{sus, steer}, true},
		{task.Running, []mem.Interrupt{sus, res}, false},
		{task.Suspended, []mem.Interrupt{res}, false},
		{task.Suspended, []mem.Interrupt{res, sus}, true},
	} {
		if got := suspendedAfter(c.state, c.pending); got != c.want {
			t.Errorf("%s %v: %v", c.state, c.pending, got)
		}
	}
}
