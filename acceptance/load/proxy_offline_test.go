package load

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The proxy itself, with no network: a streamed response reaches the client
// chunk by chunk, not when the upstream finishes, and every forwarded request
// is counted by path and by the upstream's status.
func TestMeterStreamsAndCounts(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/chat/completions":
			if r.Header.Get("Authorization") != "Bearer k" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-release
			io.WriteString(w, "data: [DONE]\n\n")
		case "/v1/hang":
			io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done() // until the proxy gives up on it
		default:
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer up.Close()
	target, _ := url.Parse(up.URL + "/v1")
	prx := newMeter(target, http.DefaultTransport, io.Discard, time.Now())
	srv := httptest.NewServer(prx)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(resp.Body).ReadString('\n')
		got <- line
	}()
	select {
	case line := <-got:
		if line != "data: first\n" {
			t.Fatalf("first line %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first chunk did not come through before the upstream finished")
	}
	close(release)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if r, err := http.Post(srv.URL+"/v1/other", "application/json", strings.NewReader("{}")); err == nil {
		r.Body.Close()
	}
	// A client that hangs up mid-stream: the copy is aborted, and the
	// exchange still gets its latency and is marked as the client's doing.
	hr, err := http.Post(srv.URL+"/v1/hang", "text/event-stream", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	bufio.NewReader(hr.Body).ReadString('\n')
	hr.Body.Close()
	srv.Close() // waits for the handlers to return

	xs := prx.snapshot()
	if hang := summarize(xs, "/v1/hang"); hang.Received != 1 || hang.Hangups != 1 || hang.BodyErr != 0 || hang.Max <= 0 {
		t.Fatalf("hang counted %+v", hang)
	}
	chat, other := summarize(xs, "/v1/chat/completions"), summarize(xs, "/v1/other")
	if chat.Received != 1 || chat.ByStatus["200"] != 1 || chat.BodyErr != 0 {
		t.Fatalf("chat counted %+v", chat)
	}
	if other.Received != 1 || other.ByStatus["429"] != 1 || other.Non2xx != 1 {
		t.Fatalf("other counted %+v", other)
	}
}
