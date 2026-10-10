package sched

import (
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/task"
)

func TestReviewWaitingForRacesOwner(t *testing.T) {
	s := New(RealClock{}, 0)
	for _, id := range []string{"a", "b"} {
		s.Add(&task.TCB{ID: id, Class: "c", State: task.Waiting, Wait: task.WaitGroup, ReadySince: time.Now(), Version: 1})
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.WaitingFor("c", task.WaitGroup)
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		tc, ok := s.CheckoutIf("a", func(*task.TCB) bool { return true })
		if !ok {
			continue
		}
		tc.ReadySince = time.Now()
		s.Requeue(tc)
	}
	close(stop)
	wg.Wait()
}
