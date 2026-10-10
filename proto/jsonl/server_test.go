package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

var bg = context.Background()

const wait = 10 * time.Second

// out is any output line, parsed: a reply or an event.
type out struct {
	Type    string          `json:"type"`
	ID      json.RawMessage `json:"id"`
	Kind    string          `json:"kind"`
	OK      bool            `json:"ok"`
	TaskID  string          `json:"task_id"`
	FromSeq *int64          `json:"from_seq"`
	Status  json.RawMessage `json:"status"`
	Queue   json.RawMessage `json:"queue"`
	Error   *Error          `json:"error"`
	Seq     int64           `json:"seq"`
	TaskSeq int64           `json:"task_seq"`
	Data    json.RawMessage `json:"data"`
}

func (o out) event() proto.Event {
	return proto.Event{Kind: o.Kind, TaskID: o.TaskID, Seq: o.Seq, TaskSeq: o.TaskSeq, Data: o.Data}
}

type harness struct {
	t     *testing.T
	m     *neumann.Machine
	srv   *Server
	inW   *io.PipeWriter
	outR  *io.PipeReader
	done  chan error
	once  sync.Once
	lines chan out
	gate  sync.Mutex // held: the reader stops taking lines
	stop  context.CancelFunc
}

// echoDevice has util_echo, which returns its arguments.
func echoDevice() bus.Device {
	return builtin.New("util").Add(bus.ToolDef{Name: "echo", Description: "Echo the arguments.", Effect: bus.ReadOnly},
		func(_ context.Context, c bus.DeviceCall) (string, error) { return string(c.Args), nil })
}

// echoModel calls util_echo once, then answers.
func echoModel(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) == 0 {
		return sim.CallTools("util_echo"), nil
	}
	return sim.Reply("done: " + sim.FirstUser(req)), nil
}

func newMachine(t *testing.T, cfg neumann.Config, fn func(model.Request) (model.Response, error)) *neumann.Machine {
	t.Helper()
	cfg.Model = &sim.Model{Fn: fn}
	cfg.Logf = t.Logf
	cfg.SteerFormat = func(s string) string { return s }
	m, err := neumann.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	return m
}

// serve runs a server over pipes for an already started machine.
func serve(t *testing.T, m *neumann.Machine, tune func(*Server)) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := &Server{Machine: m, StopTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second, Logf: t.Logf}
	if tune != nil {
		tune(srv)
	}
	ctx, cancel := context.WithCancel(bg)
	h := &harness{t: t, m: m, srv: srv, inW: inW, outR: outR, done: make(chan error, 1), lines: make(chan out, 4096), stop: cancel}
	go func() {
		err := srv.Serve(ctx, inR, outW)
		outW.Close()
		h.done <- err
	}()
	t.Cleanup(func() {
		inW.Close()
		go io.Copy(io.Discard, outR) // whatever is left, so the writer is never stuck
		select {
		case err := <-h.done:
			h.done <- err
		case <-time.After(wait):
			t.Errorf("Serve did not return")
		}
		cancel()
	})
	return h
}

func start(t *testing.T, cfg neumann.Config, fn func(model.Request) (model.Response, error)) *harness {
	t.Helper()
	if cfg.Devices == nil {
		cfg.Devices = []bus.Device{echoDevice()}
	}
	return serve(t, newMachine(t, cfg, fn), nil)
}

// read starts reading the output; until then nothing reads it.
func (h *harness) read() {
	h.once.Do(func() {
		go func() {
			defer close(h.lines)
			sc := bufio.NewScanner(h.outR)
			sc.Buffer(nil, 1<<24)
			for sc.Scan() {
				var o out
				if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
					h.t.Errorf("output line is not JSON: %s", sc.Bytes())
					continue
				}
				if o.Type != TypeReply && o.Type != TypeEvent {
					h.t.Errorf("output line has no type: %s", sc.Bytes())
				}
				h.gate.Lock()
				h.gate.Unlock()
				h.lines <- o
			}
		}()
	})
}

func (h *harness) send(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.inW, line+"\n"); err != nil {
		h.t.Fatalf("write %s: %v", line, err)
	}
}

// next is the next output line.
func (h *harness) next() out {
	h.t.Helper()
	h.read()
	select {
	case o, ok := <-h.lines:
		if !ok {
			h.t.Fatalf("output ended")
		}
		return o
	case <-time.After(wait):
		h.t.Fatalf("no output line within %s", wait)
	}
	panic("unreachable")
}

// reply returns the next reply, collecting the events before it into evs.
func (h *harness) reply(evs *[]out) out {
	h.t.Helper()
	for {
		o := h.next()
		if o.Type == TypeReply {
			return o
		}
		if evs != nil {
			*evs = append(*evs, o)
		}
	}
}

// do sends a request and returns its reply.
func (h *harness) do(line string, evs *[]out) out {
	h.t.Helper()
	h.send(line)
	return h.reply(evs)
}

// eventsUntil reads events until n terminal events have been seen; a reply
// in between fails the test.
func (h *harness) eventsUntil(evs []out, terminals int) []out {
	h.t.Helper()
	count := func() int {
		n := 0
		for _, e := range evs {
			if e.event().Terminal() {
				n++
			}
		}
		return n
	}
	for count() < terminals {
		o := h.next()
		if o.Type != TypeEvent {
			h.t.Fatalf("unexpected reply %+v", o)
		}
		evs = append(evs, o)
	}
	return evs
}

func mustOK(t *testing.T, o out, kind string) {
	t.Helper()
	if !o.OK || o.Error != nil || o.Kind != kind {
		t.Fatalf("want an ok %s reply, got %+v (error %+v)", kind, o, o.Error)
	}
}

func mustFail(t *testing.T, o out, code string) {
	t.Helper()
	if o.OK || o.Error == nil || o.Error.Code != code {
		t.Fatalf("want error %s, got %+v (error %+v)", code, o, o.Error)
	}
}

func TestSubmitStreamsEventsInOrderThroughDone(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	r := h.do(`{"id":"sub","kind":"subscribe","from_seq":0}`, nil)
	mustOK(t, r, KindSubscribe)
	if string(r.ID) != `"sub"` || r.FromSeq == nil || *r.FromSeq != 0 {
		t.Fatalf("subscribe reply %+v", r)
	}
	var evs []out
	r = h.do(`{"id":1,"kind":"submit","input":"hello"}`, &evs)
	mustOK(t, r, proto.KindSubmit)
	if string(r.ID) != "1" || r.TaskID == "" {
		t.Fatalf("submit reply %+v", r)
	}
	evs = h.eventsUntil(evs, 1)
	var kinds []string
	var prev *out
	for _, e := range evs {
		if e.TaskID == "" {
			continue // a machine event: a vCore starting
		}
		if e.TaskID != r.TaskID {
			t.Fatalf("event of another task: %+v", e)
		}
		if prev != nil && (e.Seq <= prev.Seq || e.TaskSeq != prev.TaskSeq+1) {
			t.Fatalf("events out of order: %+v after %+v", e, *prev)
		}
		prev = &e
		kinds = append(kinds, e.Kind)
	}
	got := strings.Join(kinds, " ")
	for _, k := range []string{proto.EvAccepted, proto.EvToolCall, proto.EvToolResult, proto.EvAnswer, proto.EvDone} {
		if !strings.Contains(got, k) {
			t.Fatalf("events %q lack %s", got, k)
		}
	}
	if kinds[0] != proto.EvAccepted || kinds[len(kinds)-1] != proto.EvDone {
		t.Fatalf("events %q do not run from accepted to done", got)
	}
	// The answer is what the model said.
	for _, e := range evs {
		if e.Kind == proto.EvAnswer && !strings.Contains(string(e.Data), "done: hello") {
			t.Fatalf("answer %s", e.Data)
		}
	}
}

// A client that reconnects with the last seq it saw gets exactly the events
// it missed, then live ones: the same sequence an always-on subscriber saw.
func TestSubscribeReplaysThenGoesLive(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	always := h.m.Events(ctx, 0)

	mustOK(t, h.do(`{"kind":"subscribe"}`, nil), KindSubscribe)
	var evs []out
	a := h.do(`{"kind":"submit","input":"first"}`, &evs)
	mustOK(t, a, proto.KindSubmit)
	evs = h.eventsUntil(evs, 1)
	cut := evs[2].Seq // the client "saw" up to here, then lost the connection

	var replay []out
	r := h.do(fmt.Sprintf(`{"id":"again","kind":"subscribe","from_seq":%d}`, cut), &replay)
	mustOK(t, r, KindSubscribe)
	if *r.FromSeq != cut {
		t.Fatalf("subscribe reply from_seq %d, want %d", *r.FromSeq, cut)
	}
	// Events written before the reply belong to the old stream; after it,
	// the new stream starts exactly after cut.
	replay = nil
	b := h.do(`{"kind":"submit","input":"second"}`, &replay)
	mustOK(t, b, proto.KindSubmit)
	replay = h.eventsUntil(replay, 2)

	var want []proto.Event
	for ev := range always {
		if ev.Seq > cut {
			want = append(want, ev)
		}
		if ev.TaskID == b.TaskID && ev.Terminal() {
			break
		}
	}
	if len(replay) != len(want) {
		t.Fatalf("replayed %d events, the always-on subscriber has %d after seq %d", len(replay), len(want), cut)
	}
	for i := range want {
		got := replay[i].event()
		if got.Seq != want[i].Seq || got.Kind != want[i].Kind || got.TaskID != want[i].TaskID ||
			got.TaskSeq != want[i].TaskSeq || string(got.Data) != string(want[i].Data) {
			t.Fatalf("event %d: got %+v, always-on has %+v", i, got, want[i])
		}
	}

	// unsubscribe stops the stream; status still answers.
	mustOK(t, h.do(`{"kind":"unsubscribe"}`, nil), KindUnsubscribe)
	mustOK(t, h.do(`{"kind":"submit","input":"third"}`, nil), proto.KindSubmit)
	s := h.do(`{"kind":"status"}`, nil)
	mustOK(t, s, KindStatus)
	var st neumann.Status
	if err := json.Unmarshal(s.Status, &st); err != nil || st.Cores == 0 {
		t.Fatalf("status %s: %v", s.Status, err)
	}
}

// slowDevice has slow_wait, which runs until released or cancelled and
// reports each start on started.
type slowDevice struct {
	started chan string
	release chan struct{}
}

func newSlow() *slowDevice {
	return &slowDevice{started: make(chan string, 16), release: make(chan struct{})}
}

func (s *slowDevice) device(effect bus.Effect) bus.Device {
	return builtin.New("slow").Add(bus.ToolDef{Name: "wait", Effect: effect},
		func(ctx context.Context, c bus.DeviceCall) (string, error) {
			s.started <- c.Key
			select {
			case <-s.release:
				return "released", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
}

func slowModel(req model.Request) (model.Response, error) {
	if sim.ToolRounds(req) == 0 {
		return sim.CallTools("slow_wait"), nil
	}
	return sim.Reply("finished"), nil
}

func TestSteerCancelAndTheirErrors(t *testing.T) {
	slow := newSlow()
	h := start(t, neumann.Config{Devices: []bus.Device{slow.device(bus.ReadOnly)}}, slowModel)

	mustFail(t, h.do(`{"kind":"steer","task_id":"nope","input":"x"}`, nil), CodeNotFound)
	mustFail(t, h.do(`{"kind":"cancel","task_id":"nope"}`, nil), CodeNotFound)
	mustFail(t, h.do(`{"kind":"resolve","task_id":"nope","call_id":"c","outcome":{"result":"x"}}`, nil), CodeNotFound)

	mustOK(t, h.do(`{"kind":"subscribe"}`, nil), KindSubscribe)
	var evs []out
	mustOK(t, h.do(`{"kind":"submit","task_id":"t1","input":"go"}`, &evs), proto.KindSubmit)
	select {
	case <-slow.started:
	case <-time.After(wait):
		t.Fatal("the tool never started")
	}
	r := h.do(`{"id":"st","kind":"steer","task_id":"t1","input":"hurry"}`, &evs)
	mustOK(t, r, proto.KindSteer)
	if r.TaskID != "t1" || string(r.ID) != `"st"` {
		t.Fatalf("steer reply %+v", r)
	}
	mustFail(t, h.do(`{"kind":"resolve","task_id":"t1","call_id":"c-1","outcome":{"result":"x"}}`, &evs), CodeNotReconciling)
	mustOK(t, h.do(`{"kind":"cancel","task_id":"t1"}`, &evs), proto.KindCancel)
	evs = h.eventsUntil(evs, 1)
	last := evs[len(evs)-1]
	if last.Kind != proto.EvCancelled || last.TaskID != "t1" {
		t.Fatalf("task ended with %+v", last)
	}
	// The terminal commit reports the undelivered steer right after the
	// terminal event: a task's last event is not always its terminal one.
	if o := h.next(); o.Kind != proto.EvSteerDropped || o.TaskID != "t1" {
		t.Fatalf("the undelivered steer was not reported dropped: %+v", o)
	}
	mustFail(t, h.do(`{"kind":"steer","task_id":"t1","input":"x"}`, nil), CodeFinished)
	mustFail(t, h.do(`{"kind":"cancel","task_id":"t1"}`, nil), CodeFinished)
	mustFail(t, h.do(`{"kind":"submit","task_id":"a/b","input":"x"}`, nil), CodeFailed)
}

// A call whose outcome a restart made unknown is held for an operator, and a
// resolve over the wire settles it.
func TestResolveAfterARestart(t *testing.T) {
	store := mem.NewMemory()
	slow := newSlow()
	m1 := newMachine(t, neumann.Config{Store: store, Devices: []bus.Device{slow.device(bus.SideEffect)}}, slowModel)
	if _, err := m1.Submit(bg, proto.Instruction{TaskID: "pay", Input: "pay the bill"}); err != nil {
		t.Fatal(err)
	}
	<-slow.started
	ctx, cancel := context.WithTimeout(bg, 200*time.Millisecond)
	defer cancel()
	if err := m1.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop with a call in flight: %v", err)
	}

	slow2 := newSlow()
	m2 := newMachine(t, neumann.Config{Store: store, Devices: []bus.Device{slow2.device(bus.SideEffect)}}, slowModel)
	h := serve(t, m2, nil)
	mustOK(t, h.do(`{"kind":"subscribe"}`, nil), KindSubscribe)
	var callID string
	for callID == "" {
		o := h.next()
		if o.Kind == proto.EvReconcileNeeded {
			var d struct {
				CallID string `json:"call_id"`
			}
			json.Unmarshal(o.Data, &d)
			callID = d.CallID
		}
	}
	mustFail(t, h.do(`{"kind":"resolve","task_id":"pay","call_id":"c-99","outcome":{"result":"x"}}`, nil), CodeNotReconciling)
	var evs []out
	r := h.do(fmt.Sprintf(`{"id":5,"kind":"resolve","task_id":"pay","call_id":%q,"outcome":{"result":"paid"}}`, callID), &evs)
	mustOK(t, r, KindResolve)
	evs = h.eventsUntil(evs, 1)
	if last := evs[len(evs)-1]; last.Kind != proto.EvDone {
		t.Fatalf("task ended with %+v", last)
	}
	mustFail(t, h.do(fmt.Sprintf(`{"kind":"resolve","task_id":"pay","call_id":%q,"outcome":{"retry":true}}`, callID), nil), CodeNotReconciling)
}

func TestBusyCarriesCountAndLimit(t *testing.T) {
	slow := newSlow()
	h := start(t, neumann.Config{MaxQueued: 1, Devices: []bus.Device{slow.device(bus.ReadOnly)}}, slowModel)
	mustOK(t, h.do(`{"kind":"submit","input":"one"}`, nil), proto.KindSubmit)
	r := h.do(`{"id":"two","kind":"submit","input":"two"}`, nil)
	mustFail(t, r, CodeBusy)
	if r.Error.Count != 1 || r.Error.Limit != 1 || string(r.ID) != `"two"` {
		t.Fatalf("busy reply %+v %+v", r, r.Error)
	}
	close(slow.release)
}

func TestErrorCodes(t *testing.T) {
	for _, c := range []struct {
		err  error
		code string
	}{
		{neumann.ErrNotFound, CodeNotFound},
		{fmt.Errorf("x: %w", neumann.ErrNotFound), CodeNotFound},
		{neumann.ErrFinished, CodeFinished},
		{fmt.Errorf("task t: %w", neumann.ErrNotReconciling), CodeNotReconciling},
		{&neumann.CapacityError{Count: 3, Limit: 3}, CodeBusy},
		{neumann.ErrBusy, CodeBusy},
		{neumann.ErrStopped, CodeStopped},
		{neumann.ErrNotStarted, CodeNotStarted},
		{errors.New("disk on fire"), CodeFailed},
	} {
		e := errorFor(c.err)
		if e.Code != c.code || e.Message != c.err.Error() {
			t.Errorf("%v: %+v, want code %s", c.err, e, c.code)
		}
	}
}

// Bad input gets an error reply and the server carries on.
func TestBadLinesDoNotStopTheServer(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	mustFail(t, h.do(`{"kind":"submit"`, nil), CodeBadRequest)
	mustFail(t, h.do(`{"id":3,"kind":"teleport"}`, nil), CodeUnknownKind)
	r := h.do(`{"id":4,"kind":"suspend","task_id":"t1"}`, nil)
	mustFail(t, r, CodeUnsupported)
	if string(r.ID) != "4" || r.Kind != "suspend" {
		t.Fatalf("unsupported reply %+v", r)
	}
	h.send("") // blank lines are skipped
	h.send("   \r")
	r = h.do(`{"id":"s","kind":"status"}`+"\r", nil) // CRLF input
	mustOK(t, r, KindStatus)
	if string(r.ID) != `"s"` {
		t.Fatalf("status reply %+v", r)
	}
}

func TestOverlongLineIsRefused(t *testing.T) {
	h := serve(t, newMachine(t, neumann.Config{Devices: []bus.Device{echoDevice()}}, echoModel), func(s *Server) { s.MaxLine = 64 })
	r := h.do(`{"id":1,"kind":"submit","input":"`+strings.Repeat("x", 200)+`"}`, nil)
	mustFail(t, r, CodeBadRequest)
	if !strings.Contains(r.Error.Message, "64") {
		t.Fatalf("message %q should name the limit", r.Error.Message)
	}
	mustOK(t, h.do(`{"kind":"status"}`, nil), KindStatus)
}

// EOF on input stops the machine gracefully and Serve returns nil once every
// reply is written.
func TestEOFStopsCleanly(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	h.read()
	h.send(`{"id":"last","kind":"submit","input":"bye"}`)
	h.inW.Close()
	var replies []out
	for o := range h.lines {
		if o.Type == TypeReply {
			replies = append(replies, o)
		}
	}
	if len(replies) != 1 || !replies[0].OK || string(replies[0].ID) != `"last"` {
		t.Fatalf("replies %+v", replies)
	}
	select {
	case err := <-h.done:
		h.done <- err
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(wait):
		t.Fatal("Serve did not return after EOF")
	}
	if _, err := h.m.Submit(bg, proto.Instruction{Input: "late"}); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("the machine is not stopped: %v", err)
	}
}

// Cancelling Serve's context (SIGINT, SIGTERM) is the same graceful stop.
func TestContextStopsCleanly(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	mustOK(t, h.do(`{"kind":"status"}`, nil), KindStatus)
	h.stop()
	select {
	case err := <-h.done:
		h.done <- err
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(wait):
		t.Fatal("Serve did not return after its context ended")
	}
	if _, err := h.m.Submit(bg, proto.Instruction{Input: "late"}); !errors.Is(err, neumann.ErrStopped) {
		t.Fatalf("the machine is not stopped: %v", err)
	}
}

// Nobody reads the output: submits are still executed and committed, the
// machine runs every task to the end, and when the reader comes back the
// waiting replies are written before the backlog of events.
func TestStalledReaderDoesNotBlockSubmits(t *testing.T) {
	h := start(t, neumann.Config{}, echoModel)
	mustOK(t, h.do(`{"kind":"subscribe"}`, nil), KindSubscribe)
	// From here nothing reads the output: the reader holds at most the line
	// it has scanned, and the server blocks writing the one after it.
	h.gate.Lock()
	const n = 20
	for i := range n {
		h.send(fmt.Sprintf(`{"id":%d,"kind":"submit","input":"task %d"}`, i, i))
	}
	deadline := time.Now().Add(wait)
	for {
		evs, err := h.m.EventLog(bg, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		done := 0
		for _, e := range evs {
			if e.Kind == proto.EvDone {
				done++
			}
		}
		if done == n && h.srv.queued.Load() == n+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("with the output stalled, %d of %d tasks finished and %d replies queued", done, n, h.srv.queued.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The reader comes back. At most one line was already scanned and one
	// being written when it stalled; then come all n replies, in order.
	h.gate.Unlock()
	events := 0
	for i := 0; i < n; {
		o := h.next()
		if o.Type == TypeEvent {
			if events++; events > 2 {
				t.Fatalf("event %+v written before reply %d of %d", o, i, n)
			}
			continue
		}
		mustOK(t, o, proto.KindSubmit)
		if string(o.ID) != fmt.Sprint(i) {
			t.Fatalf("reply %d has id %s", i, o.ID)
		}
		i++
	}
}
