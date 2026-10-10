package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var serving = regexp.MustCompile(`observe: serving (http://\S+)/status`)

// runTask submits one task and reads the event stream until it is done.
func (p *proc) runTask(id string) {
	p.t.Helper()
	p.send(`{"kind":"subscribe"}`)
	p.reply(nil)
	var evs []map[string]any
	p.send(`{"kind":"submit","task_id":"` + id + `","input":"ping"}`)
	if r := p.reply(&evs); r["ok"] != true {
		p.t.Fatalf("submit: %v", r)
	}
	p.until(evs, id, "done")
}

// observeURL waits for the process to log where it serves the observe view.
func (p *proc) observeURL() string {
	p.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if m := serving.FindStringSubmatch(p.stderr.String()); m != nil {
			return m[1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatalf("no observe address logged; stderr:\n%s", p.stderr.String())
	return ""
}

func fetch(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// -observe-addr serves the read-only view; -metrics adds /metrics; pprof
// stays off unless asked for.
func TestObserveAddr(t *testing.T) {
	f := newFake(t)
	p := launch(t, env(f), "-echo", "-observe-addr", "127.0.0.1:0", "-metrics")
	base := p.observeURL()
	p.runTask("o-1")

	code, body := fetch(t, base+"/status")
	var st struct {
		Groups []struct {
			Class string `json:"class"`
		} `json:"groups"`
	}
	if code != 200 || json.Unmarshal([]byte(body), &st) != nil || len(st.Groups) != 1 || st.Groups[0].Class != "default" {
		t.Fatalf("/status: %d %s", code, body)
	}
	if code, body := fetch(t, base+"/tasks/o-1"); code != 200 || !strings.Contains(body, `"util_echo"`) {
		t.Fatalf("/tasks/o-1: %d %s", code, body)
	}
	// The metrics count from the moment the process started observing.
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, body := fetch(t, base+"/metrics")
		if code == 200 && strings.Contains(body, `neumann_tasks_accepted_total{class="default"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/metrics: %d %s", code, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, _ := fetch(t, base+"/debug/pprof/"); code != http.StatusNotFound {
		t.Fatalf("pprof served without -pprof: %d", code)
	}
	p.stdin.Close()
	if err := p.wait(); err != nil {
		t.Fatalf("exit: %v; stderr:\n%s", err, p.stderr.String())
	}
}

// With OTEL_EXPORTER_OTLP_ENDPOINT set, traces and metrics go to that
// collector over OTLP/HTTP, flushed when the process stops.
func TestOTLPExport(t *testing.T) {
	f := newFake(t)
	var mu sync.Mutex
	got := map[string]int{}
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		got[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer collector.Close()
	p := launch(t, env(f, "OTEL_EXPORTER_OTLP_ENDPOINT="+collector.URL, "OTEL_METRIC_EXPORT_INTERVAL=100000"), "-echo")
	p.runTask("x-1")
	p.stdin.Close()
	if err := p.wait(); err != nil {
		t.Fatalf("exit: %v; stderr:\n%s", err, p.stderr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if got["/v1/traces"] == 0 || got["/v1/metrics"] == 0 {
		t.Fatalf("collector got %v; stderr:\n%s", got, p.stderr.String())
	}
}
