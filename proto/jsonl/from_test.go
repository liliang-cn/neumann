package jsonl

import (
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/proto"
)

// A steer may name its sender; the steer_dropped event that reports it
// carries the sender, and a sender the machine refuses is an error reply.
func TestSteerWithASender(t *testing.T) {
	slow := newSlow()
	h := start(t, neumann.Config{Devices: []bus.Device{slow.device(bus.ReadOnly)}}, slowModel)
	mustOK(t, h.do(`{"kind":"subscribe"}`, nil), KindSubscribe)
	var evs []out
	mustOK(t, h.do(`{"kind":"submit","task_id":"t1","input":"go"}`, &evs), proto.KindSubmit)
	select {
	case <-slow.started:
	case <-time.After(wait):
		t.Fatal("the tool never started")
	}
	mustOK(t, h.do(`{"kind":"steer","task_id":"t1","input":"hurry","from":"operator"}`, &evs), proto.KindSteer)
	mustFail(t, h.do(`{"kind":"steer","task_id":"t1","input":"x","from":"a\nb"}`, &evs), CodeFailed)
	mustOK(t, h.do(`{"kind":"cancel","task_id":"t1"}`, &evs), proto.KindCancel)
	evs = h.eventsUntil(evs, 1)
	if o := h.next(); o.Kind != proto.EvSteerDropped || string(o.Data) != `{"from":"operator","seq":1,"text":"hurry"}` {
		t.Fatalf("steer_dropped %+v (%s)", o, o.Data)
	}
}
