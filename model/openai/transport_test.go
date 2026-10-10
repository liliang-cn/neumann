package openai

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// protoServer answers chat completions with "ok" and records the protocol of
// every request it served.
type protoServer struct {
	mu     sync.Mutex
	protos []string
}

func (p *protoServer) handler(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	p.mu.Lock()
	p.protos = append(p.protos, r.Proto)
	p.mu.Unlock()
	io.WriteString(w, textReply)
}

func (p *protoServer) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.protos...)
}

// tlsH2Server is a TLS server speaking HTTP/2 and HTTP/1.1.
func tlsH2Server(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	ts := httptest.NewUnstartedServer(h)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts
}

func trust(ts *httptest.Server, rt http.RoundTripper) {
	roots := ts.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	switch t := rt.(type) {
	case *http.Transport:
		t.TLSClientConfig.RootCAs = roots
	case *h3Fallback:
		trust(ts, t.h2)
		t.h3.(*http3.Transport).TLSClientConfig.RootCAs = roots
	}
}

func call(t *testing.T, a *Adapter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.Call(ctx, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "hi")}}); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP2MultiplexesOneConnection(t *testing.T) {
	var p protoServer
	var conns atomic.Int32
	ts := httptest.NewUnstartedServer(http.HandlerFunc(p.handler))
	ts.EnableHTTP2 = true
	ts.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	ts.StartTLS()
	t.Cleanup(ts.Close)

	a := New(ts.URL+"/v1", "k", "m")
	trust(ts, a.HTTP.Transport)
	if err := a.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); call(t, a) }()
	}
	wg.Wait()
	for _, proto := range p.seen() {
		if proto != "HTTP/2.0" {
			t.Fatalf("served over %s", proto)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("16 concurrent calls opened %d connections, want 1", n)
	}
}

func TestHTTP1IsReallyHTTP1(t *testing.T) {
	var p protoServer
	ts := tlsH2Server(t, p.handler)
	a := New(ts.URL+"/v1", "k", "m").WithProtocol(HTTP1)
	trust(ts, a.HTTP.Transport)
	call(t, a)
	if got := p.seen(); len(got) != 1 || got[0] != "HTTP/1.1" {
		t.Fatalf("served over %v", got)
	}
}

func TestHTTP3AgainstALocalServer(t *testing.T) {
	var p protoServer
	ts := tlsH2Server(t, p.handler)
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h3 := &http3.Server{Handler: http.HandlerFunc(p.handler), TLSConfig: http3.ConfigureTLSConfig(ts.TLS.Clone())}
	go h3.Serve(udp)
	t.Cleanup(func() { h3.Close(); udp.Close() })

	a := New("https://"+udp.LocalAddr().String()+"/v1", "k", "m").WithProtocol(HTTP3)
	trust(ts, a.HTTP.Transport)
	call(t, a)
	call(t, a)
	if got := p.seen(); len(got) != 2 || got[0] != "HTTP/3.0" || got[1] != "HTTP/3.0" {
		t.Fatalf("served over %v", got)
	}
	if a.HTTP.Transport.(*h3Fallback).Fallback() {
		t.Fatal("fell back although QUIC works")
	}
}

func TestHTTP3FallsBackToTCPWhenQUICIsUnreachable(t *testing.T) {
	old := quicHandshakeTimeout
	quicHandshakeTimeout = 300 * time.Millisecond
	defer func() { quicHandshakeTimeout = old }()

	var p protoServer
	ts := tlsH2Server(t, p.handler) // TCP only: nothing answers QUIC on this port
	a := New(ts.URL+"/v1", "k", "m").WithProtocol(HTTP3)
	trust(ts, a.HTTP.Transport)
	start := time.Now()
	call(t, a)
	if !a.HTTP.Transport.(*h3Fallback).Fallback() {
		t.Fatal("did not record the fallback")
	}
	call(t, a) // straight to TCP, no second handshake wait
	if got := p.seen(); len(got) != 2 || got[0] != "HTTP/2.0" {
		t.Fatalf("served over %v", got)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("fallback took %v", d)
	}
}

type fakeRT struct {
	err   error
	calls atomic.Int32
	body  string
}

func (f *fakeRT) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls.Add(1)
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		f.body = string(b)
	}
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")), Request: r}, nil
}

func TestFallbackOnlyForHandshakeFailures(t *testing.T) {
	newReq := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "https://x/v1/chat/completions", strings.NewReader(`{"a":1}`))
		return r
	}

	h3, h2 := &fakeRT{err: &dialError{&quic.HandshakeTimeoutError{}}}, &fakeRT{}
	f := &h3Fallback{h3: h3, h2: h2}
	if _, err := f.RoundTrip(newReq()); err != nil {
		t.Fatal(err)
	}
	if h2.body != `{"a":1}` {
		t.Fatalf("retry lost the body: %q", h2.body)
	}
	f.RoundTrip(newReq())
	if h3.calls.Load() != 1 || h2.calls.Load() != 2 {
		t.Fatalf("h3 %d h2 %d", h3.calls.Load(), h2.calls.Load())
	}

	// A failure after the request may have been sent is not retried, even
	// one that looks like a timeout.
	h3, h2 = &fakeRT{err: &quic.IdleTimeoutError{}}, &fakeRT{}
	f = &h3Fallback{h3: h3, h2: h2}
	if _, err := f.RoundTrip(newReq()); err == nil || h2.calls.Load() != 0 || f.Fallback() {
		t.Fatalf("err %v, h2 calls %d", err, h2.calls.Load())
	}
}

func TestWarmIgnoresStatus(t *testing.T) {
	ts := tlsH2Server(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	a := New(ts.URL+"/v1", "k", "m")
	trust(ts, a.HTTP.Transport)
	if err := a.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := New("https://127.0.0.1:1/v1", "k", "m")
	if err := b.Warm(context.Background()); err == nil {
		t.Fatal("warm against a closed port succeeded")
	}
}
