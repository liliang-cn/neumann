package sched_test

import (
	"context"
	"testing"

	"github.com/liliang-cn/neumann/internal/sched"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// Nudge hands over a task nobody is at work on, where a suspend or resume
// lands at once (design §9.9): a ready one, one in backoff (its wake-up
// timer stopped), one waiting for its group, a suspended one.
func TestNudgeTakesALandableTask(t *testing.T) {
	for _, tc := range []*task.TCB{
		tcb("ready", 0, 0),
		{ID: "backoff", State: task.Waiting, Wait: task.WaitBackoff, Class: "default", WakeAt: t0.Add(1)},
		waiting("group", task.WaitGroup),
		{ID: "suspended", State: task.Suspended, Class: "default"},
	} {
		clk := sim.NewClock(t0)
		s := sched.New(clk, 0)
		woke := false
		s.OnWake(func(string) { woke = true })
		s.Add(tc)
		got, ok := s.Nudge(tc.ID, true, sched.Landable)
		if !ok || got != tc {
			t.Fatalf("%s: Nudge did not hand the task over", tc.ID)
		}
		if s.Nudged(tc.ID) || s.Cutting(tc.ID) {
			t.Fatalf("%s: handed over, and marked too", tc.ID)
		}
		if r, w, h := s.Counts(); r != 0 || w != 0 || h != 1 {
			t.Fatalf("%s: counts %d/%d/%d", tc.ID, r, w, h)
		}
		clk.Advance(1)
		if woke {
			t.Fatalf("%s: the backoff timer ran for a task taken by Nudge", tc.ID)
		}
		if s.Hand(got) != sched.Queued {
			t.Fatalf("%s: hand-back refused", tc.ID)
		}
	}
}

// A task waiting for its tools, an approval or reconciliation lands when
// that wait is over: Nudge only marks it, and the mark outlives a hand-back
// where nothing lands (Waiting(IO)) until its holder reads its interrupts.
func TestNudgeMarksAWaitThatLandsLater(t *testing.T) {
	for _, w := range []task.WaitReason{task.WaitIO, task.WaitApproval, task.WaitReconcile} {
		s := sched.New(&clock{now: t0}, 0)
		tc := waiting("w", w)
		s.Add(tc)
		if _, ok := s.Nudge("w", true, sched.Landable); ok {
			t.Fatalf("%s: Nudge took the task", w)
		}
		if !s.Nudged("w") || !s.Cutting("w") {
			t.Fatalf("%s: not marked", w)
		}
		got, ok := s.Checkout("w")
		if !ok {
			t.Fatal("checkout")
		}
		if s.Hand(got) != sched.Queued || !s.Nudged("w") {
			t.Fatalf("%s: a hand-back still waiting cleared the mark, or was refused", w)
		}
		s.Heard("w")
		if s.Nudged("w") || s.Cutting("w") {
			t.Fatalf("%s: Heard left the mark", w)
		}
	}
}

// A task held when it is nudged is not taken back where the suspend or
// resume lands at once: Hand reports Nudged, the holder keeps it, and the
// next hand-back (after it landed) is taken. A cancel comes first; Requeue
// ignores the mark; a resume's nudge does not cut.
func TestHandRefusesANudgedTask(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	s.Add(tcb("t", 0, 0))
	held, _ := s.Next(context.Background(), "default")
	if _, ok := s.Nudge("t", false, sched.Landable); ok {
		t.Fatal("Nudge took a held task")
	}
	if !s.Nudged("t") || s.Cutting("t") {
		t.Fatal("a resume's nudge: marked nudged only")
	}
	if h := s.Hand(held); h != sched.Nudged {
		t.Fatalf("hand-back of a nudged ready task: %v", h)
	}
	if _, _, h := s.Counts(); h != 1 {
		t.Fatal("the holder no longer holds it")
	}
	held.State = task.Suspended
	if h := s.Hand(held); h != sched.Queued {
		t.Fatalf("the hand-back after landing: %v", h)
	}

	got, _ := s.Checkout("t")
	s.Nudge("t", true, func(*task.TCB) bool { return false })
	if !s.Requeue(got) {
		t.Fatal("Requeue refused a nudged task")
	}

	got, _ = s.Checkout("t")
	s.Nudge("t", true, func(*task.TCB) bool { return false })
	s.Cancel("t")
	if h := s.Hand(got); h != sched.Refused {
		t.Fatalf("a cancelled, nudged task: %v", h)
	}
}

// Nudge leaves alone a task the scheduler does not know (not handed to it
// yet, or finished) and a cancelled one.
func TestNudgeLeavesUnknownAndCancelledTasks(t *testing.T) {
	s := sched.New(&clock{now: t0}, 0)
	if _, ok := s.Nudge("nope", true, sched.Landable); ok || s.Nudged("nope") {
		t.Fatal("an unknown task was nudged")
	}
	s.Add(tcb("c", 0, 0))
	got, _ := s.Cancel("c")
	if _, ok := s.Nudge("c", true, sched.Landable); ok || s.Nudged("c") {
		t.Fatal("a cancelled task was nudged")
	}
	got.State = task.Cancelled
	s.Requeue(got)
}
