package load

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// meter is the billing proxy: an in-process reverse proxy in front of the
// gateway that forwards every request unchanged, streaming responses chunk
// by chunk, and counts what it forwarded. It is the provider's view of the
// run: every request that reached it went on to the gateway.
type meter struct {
	start time.Time
	rp    *httputil.ReverseProxy

	inflight atomic.Int64
	peak     atomic.Int64

	mu   sync.Mutex
	reqs []*exchange
}

// exchange is one request through the proxy.
type exchange struct {
	Path     string  `json:"path"`
	Start    float64 `json:"start_ms"`  // since the run started
	Header   float64 `json:"header_ms"` // until the upstream's response header, 0 if none
	Total    float64 `json:"total_ms"`  // until the response was copied to the client
	Status   int     `json:"status"`    // the upstream's status; 0: no response
	Upstream string  `json:"upstream_err,omitempty"`
	Body     string  `json:"body_err,omitempty"` // the response body broke off
	// ClientClosed: the body broke off because the client had hung up.
	ClientClosed bool  `json:"client_closed,omitempty"`
	Bytes        int64 `json:"bytes"`
}

type exchangeKey struct{}

func newMeter(target *url.URL, rt http.RoundTripper, logs io.Writer, start time.Time) *meter {
	m := &meter{start: start}
	upstream := &url.URL{Scheme: target.Scheme, Host: target.Host}
	m.rp = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream) // the path, query and headers stay as the client sent them
		},
		Transport:     rt,
		FlushInterval: -1, // flush after every write: a stream reaches the client as it arrives
		ErrorLog:      log.New(logs, "proxy: ", 0),
		ModifyResponse: func(resp *http.Response) error {
			if x, ok := resp.Request.Context().Value(exchangeKey{}).(*exchange); ok {
				m.mu.Lock()
				x.Status = resp.StatusCode
				x.Header = ms(time.Since(m.start)) - x.Start
				m.mu.Unlock()
				resp.Body = &meteredBody{ReadCloser: resp.Body, m: m, x: x}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if x, ok := r.Context().Value(exchangeKey{}).(*exchange); ok {
				m.mu.Lock()
				x.Upstream = err.Error()
				m.mu.Unlock()
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return m
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (m *meter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x := &exchange{Path: r.URL.Path, Start: ms(time.Since(m.start))}
	m.mu.Lock()
	m.reqs = append(m.reqs, x)
	m.mu.Unlock()
	n := m.inflight.Add(1)
	for p := m.peak.Load(); n > p && !m.peak.CompareAndSwap(p, n); p = m.peak.Load() {
	}
	// Deferred: a body that breaks off mid-copy makes ReverseProxy panic with
	// http.ErrAbortHandler, unwinding through here.
	defer func() {
		m.mu.Lock()
		x.Total = ms(time.Since(m.start)) - x.Start
		if x.Body != "" && r.Context().Err() != nil {
			// The client hung up first: the adapter closes a stream once
			// it has read [DONE], sometimes before the gateway's last
			// chunk. The answer had come through whole.
			x.ClientClosed = true
		}
		m.mu.Unlock()
		m.inflight.Add(-1)
	}()
	m.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), exchangeKey{}, x)))
}

// meteredBody notes how much of a response came through and whether it
// broke off before its end.
type meteredBody struct {
	io.ReadCloser
	m *meter
	x *exchange
}

func (b *meteredBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.m.mu.Lock()
	b.x.Bytes += int64(n)
	if err != nil && !errors.Is(err, io.EOF) && b.x.Body == "" {
		b.x.Body = err.Error()
	}
	b.m.mu.Unlock()
	return n, err
}

// snapshot copies the exchanges so far.
func (m *meter) snapshot() []exchange {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]exchange, len(m.reqs))
	for i, x := range m.reqs {
		out[i] = *x
	}
	return out
}

// counts is a summary of exchanges on one path.
type counts struct {
	Received int            `json:"received"`       // requests the proxy forwarded
	ByStatus map[string]int `json:"by_status"`      // upstream status, "none" when there was no response
	BodyErr  int            `json:"body_errors"`    // the upstream's body broke off
	Hangups  int            `json:"client_hangups"` // the client closed before the upstream's last byte
	Non2xx   int            `json:"non_2xx"`
	P50      float64        `json:"latency_p50_ms"`
	P90      float64        `json:"latency_p90_ms"`
	P99      float64        `json:"latency_p99_ms"`
	Max      float64        `json:"latency_max_ms"`
}

func summarize(xs []exchange, path string) counts {
	c := counts{ByStatus: map[string]int{}}
	var lat []float64
	for _, x := range xs {
		if x.Path != path {
			continue
		}
		c.Received++
		key := "none"
		if x.Status != 0 {
			key = fmt.Sprint(x.Status)
		}
		c.ByStatus[key]++
		if x.Status < 200 || x.Status > 299 {
			c.Non2xx++
		}
		switch {
		case x.ClientClosed:
			c.Hangups++
		case x.Body != "":
			c.BodyErr++
		}
		lat = append(lat, x.Total)
	}
	sort.Float64s(lat)
	q := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))]
	}
	c.P50, c.P90, c.P99 = q(0.5), q(0.9), q(0.99)
	if len(lat) > 0 {
		c.Max = lat[len(lat)-1]
	}
	return c
}

// wire is the adapter's side of the wire: every HTTP request its client
// sent, and what came of it. It sees what the proxy cannot (a request that
// never left the client) and lets a mismatch be placed on one side or the
// other.
type wire struct {
	inner http.RoundTripper

	mu       sync.Mutex
	sent     int
	answered map[int]int // by status
	noResp   []string    // the transport's error, no response at all
	bodyErr  int         // the body broke off after a response header
}

func (w *wire) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := w.inner.RoundTrip(r)
	if r.Method != http.MethodPost { // the warm-up GET is not a model call
		return resp, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent++
	if err != nil {
		w.noResp = append(w.noResp, err.Error())
		return resp, err
	}
	w.answered[resp.StatusCode]++
	resp.Body = &wireBody{ReadCloser: resp.Body, w: w}
	return resp, nil
}

func (w *wire) CloseIdleConnections() {
	if c, ok := w.inner.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

type wireBody struct {
	io.ReadCloser
	w    *wire
	once sync.Once
}

func (b *wireBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.once.Do(func() {
			b.w.mu.Lock()
			b.w.bodyErr++
			b.w.mu.Unlock()
		})
	}
	return n, err
}
