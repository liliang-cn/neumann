package observe

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func get(t *testing.T, srv *httptest.Server, path string, header ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

func TestHandlerStatusQueueAndTasks(t *testing.T) {
	r := newRig(t)
	r.run(t, "a")
	srv := httptest.NewServer(NewHandler(r.m, HandlerOptions{}))
	t.Cleanup(srv.Close) // after the streams' bodies are closed

	resp, b := get(t, srv, "/status")
	var st neumann.Status
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" || json.Unmarshal(b, &st) != nil {
		t.Fatalf("/status: %d %s", resp.StatusCode, b)
	}
	if len(st.Groups) != 1 || st.Groups[0].Class != "default" || st.Groups[0].Target != 2 {
		t.Fatalf("/status: %s", b)
	}
	resp, b = get(t, srv, "/queue")
	var q neumann.QueueStatus
	if resp.StatusCode != 200 || json.Unmarshal(b, &q) != nil || len(q.Classes) != 1 || q.Classes[0].Class != "default" {
		t.Fatalf("/queue: %d %s", resp.StatusCode, b)
	}

	resp, b = get(t, srv, "/tasks/a")
	var tv struct {
		Task  *task.TCB        `json:"task"`
		Calls []mem.CallRecord `json:"calls"`
	}
	if resp.StatusCode != 200 || json.Unmarshal(b, &tv) != nil {
		t.Fatalf("/tasks/a: %d %s", resp.StatusCode, b)
	}
	if tv.Task.ID != "a" || tv.Task.State != task.Done || len(tv.Calls) != 1 || tv.Calls[0].Tool != "util_echo" || tv.Calls[0].Status != mem.CallDone {
		t.Fatalf("/tasks/a: %s", b)
	}
	if resp, _ := get(t, srv, "/tasks/nope"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/tasks/nope: %d", resp.StatusCode)
	}

	// Read-only: nothing but GET and HEAD.
	for _, path := range []string{"/status", "/tasks/a", "/events"} {
		resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s: %d", path, resp.StatusCode)
		}
	}
	// Off unless asked for.
	for _, path := range []string{"/metrics", "/debug/pprof/", "/debug/pprof/heap"} {
		if resp, _ := get(t, srv, path); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s served by default: %d", path, resp.StatusCode)
		}
	}
}

// sse reads a server-sent event stream.
type sse struct {
	t    *testing.T
	body io.Closer
	r    *bufio.Reader
}

type sseEvent struct {
	id, event string
	ev        proto.Event
}

func openSSE(t *testing.T, srv *httptest.Server, path string, header ...string) *sse {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("%s: %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	s := &sse{t: t, body: resp.Body, r: bufio.NewReader(resp.Body)}
	t.Cleanup(func() { resp.Body.Close() })
	return s
}

func (s *sse) next() sseEvent {
	s.t.Helper()
	var e sseEvent
	got := make(chan error, 1)
	go func() {
		for {
			line, err := s.r.ReadString('\n')
			if err != nil {
				got <- err
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "" && e.id != "":
				got <- nil
				return
			case strings.HasPrefix(line, "id: "):
				e.id = line[4:]
			case strings.HasPrefix(line, "event: "):
				e.event = line[7:]
			case strings.HasPrefix(line, "data: "):
				if err := json.Unmarshal([]byte(line[6:]), &e.ev); err != nil {
					got <- err
					return
				}
			}
		}
	}()
	select {
	case err := <-got:
		if err != nil {
			s.t.Fatalf("read event: %v", err)
		}
	case <-time.After(10 * time.Second):
		s.t.Fatal("no event")
	}
	return e
}

// /events replays the log from from_seq, then goes on live; Last-Event-ID
// (a reconnecting EventSource) resumes after the last event the client saw.
func TestHandlerEventStream(t *testing.T) {
	r := newRig(t)
	r.run(t, "a")
	log := r.log(t)
	srv := httptest.NewServer(NewHandler(r.m, HandlerOptions{}))
	t.Cleanup(srv.Close) // after the streams' bodies are closed

	s := openSSE(t, srv, "/events?from_seq=0")
	for _, want := range log {
		e := s.next()
		if e.id != strconv.FormatInt(want.Seq, 10) || e.event != want.Kind || e.ev.Seq != want.Seq || !e.ev.Time.Equal(want.Time) || string(e.ev.Data) != string(want.Data) {
			t.Fatalf("replayed %+v, want %+v", e, want)
		}
	}
	// Live: a task submitted now shows up on the open stream.
	if _, err := r.m.Submit(context.Background(), proto.Instruction{TaskID: "b", Input: "b"}); err != nil {
		t.Fatal(err)
	}
	for {
		e := s.next()
		if e.ev.TaskID == "b" && e.ev.Kind == proto.EvAccepted {
			break
		}
	}

	cut := log[3].Seq
	s = openSSE(t, srv, "/events?from_seq=0", "Last-Event-ID", strconv.FormatInt(cut, 10))
	if e := s.next(); e.ev.Seq != cut+1 {
		t.Fatalf("resumed at %d, want %d", e.ev.Seq, cut+1)
	}
	s = openSSE(t, srv, "/events?from_seq="+strconv.FormatInt(cut, 10)+"&task=a")
	for {
		e := s.next()
		if e.ev.TaskID != "a" || e.ev.Seq <= cut {
			t.Fatalf("filtered stream gave %+v", e.ev)
		}
		if e.ev.Kind == proto.EvDone {
			break
		}
	}
	if resp, _ := get(t, srv, "/events?from_seq=x"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad from_seq: %d", resp.StatusCode)
	}
}

// /metrics, when given a Prometheus reader's handler, is the text exposition
// of the OTel metrics; it parses, and carries the machine's numbers.
func TestHandlerMetricsAndPprof(t *testing.T) {
	r := newRig(t)
	reader, metrics, err := PrometheusReader()
	if err != nil {
		t.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	m, err := NewMetrics(r.m, mp, MetricsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r.run(t, "a")
	feed(m, r.log(t))
	srv := httptest.NewServer(NewHandler(r.m, HandlerOptions{Metrics: metrics, Pprof: true}))
	t.Cleanup(srv.Close) // after the streams' bodies are closed

	resp, b := get(t, srv, "/metrics")
	if resp.StatusCode != 200 {
		t.Fatalf("/metrics: %d %s", resp.StatusCode, b)
	}
	p := expfmt.NewTextParser(prommodel.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(string(b)))
	if err != nil {
		t.Fatalf("exposition does not parse: %v\n%s", err, b)
	}
	for _, name := range []string{"neumann_tasks_accepted_total", "neumann_model_duration_seconds", "neumann_tasks_unfinished", "neumann_group_window", "neumann_tool_calls_total"} {
		if fams[name] == nil {
			t.Errorf("no %s in the exposition", name)
		}
	}
	if f := fams["neumann_tasks_accepted_total"]; f != nil && f.GetMetric()[0].GetCounter().GetValue() != 1 {
		t.Errorf("tasks accepted %v", f.GetMetric()[0])
	}

	if resp, b := get(t, srv, "/debug/pprof/"); resp.StatusCode != 200 || !strings.Contains(string(b), "heap") {
		t.Fatalf("/debug/pprof/: %d %s", resp.StatusCode, b)
	}
	if resp, b := get(t, srv, "/debug/pprof/goroutine?debug=1"); resp.StatusCode != 200 || !strings.Contains(string(b), "goroutine") {
		t.Fatalf("goroutine profile: %d", resp.StatusCode)
	}
	if resp, b := get(t, srv, "/debug/pprof/heap"); resp.StatusCode != 200 || len(b) == 0 {
		t.Fatalf("heap profile: %d", resp.StatusCode)
	}
	if resp, _ := get(t, srv, "/debug/pprof/nosuch"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown profile: %d", resp.StatusCode)
	}
}

// With a token, every endpoint wants it as a bearer token.
func TestHandlerToken(t *testing.T) {
	r := newRig(t)
	srv := httptest.NewServer(NewHandler(r.m, HandlerOptions{Token: "s3cret", Pprof: true}))
	t.Cleanup(srv.Close) // after the streams' bodies are closed
	for _, path := range []string{"/status", "/queue", "/tasks/a", "/events", "/debug/pprof/"} {
		resp, _ := get(t, srv, path)
		if resp.StatusCode != http.StatusUnauthorized || !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer") {
			t.Fatalf("%s without a token: %d", path, resp.StatusCode)
		}
		if resp, _ := get(t, srv, path, "Authorization", "Bearer wrong"); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s with a wrong token: %d", path, resp.StatusCode)
		}
	}
	if resp, _ := get(t, srv, "/status", "Authorization", "Bearer s3cret"); resp.StatusCode != 200 {
		t.Fatalf("with the token: %d", resp.StatusCode)
	}
}

// Follow hands every event to its observers in order and returns the last
// seq once the machine stops; Head finds the end of the log.
func TestFollowAndHead(t *testing.T) {
	r := newRig(t)
	if n, err := Head(context.Background(), r.m); err != nil || n != 0 {
		t.Fatalf("Head of an empty log: %d %v", n, err)
	}
	r.run(t, ids(3)...)
	// Stopping commits the vCores' core_down events: the log ends after them.
	r.m.Stop(context.Background())
	log := r.log(t)
	last := log[len(log)-1].Seq
	if n, err := Head(context.Background(), r.m); err != nil || n != last {
		t.Fatalf("Head %d %v, want %d", n, err, last)
	}
	var seen []int64
	n, err := Follow(context.Background(), r.m, 5, ObserverFunc(func(ev proto.Event) { seen = append(seen, ev.Seq) }))
	if err != nil || n != last || len(seen) != int(last-5) || seen[0] != 6 {
		t.Fatalf("Follow returned %d %v after %v", n, err, seen)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Follow(ctx, r.m, 0); err == nil {
		t.Fatal("Follow with a cancelled context returned no error")
	}
}
