package openai

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Protocol is the wire protocol to the endpoint.
type Protocol string

const (
	// HTTP2 is the default: every vCore's requests share one TLS connection,
	// multiplexed, so concurrency costs no handshakes.
	HTTP2 Protocol = "h2"
	// HTTP3 runs over QUIC: a 1-RTT handshake (0-RTT on resumption) and no
	// head-of-line blocking across streams. If the QUIC handshake fails
	// (UDP blocked, no h3 on the server) it falls back to HTTP2 for good.
	HTTP3 Protocol = "h3"
	// HTTP1 exists as a baseline for measurement.
	HTTP1 Protocol = "http/1.1"
)

// quicHandshakeTimeout bounds how long a first request waits on a QUIC
// handshake before falling back to TCP. A variable so tests can shorten it.
var quicHandshakeTimeout = 3 * time.Second

// NewTransport returns a RoundTripper tuned for many concurrent model calls
// to one host: a large idle pool, keep-alives, and TLS session resumption so
// a reconnect skips the full handshake.
func NewTransport(p Protocol) http.RoundTripper {
	switch p {
	case HTTP1:
		t := tcpTransport()
		t.ForceAttemptHTTP2 = false
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{} // non-nil and empty: no h2
		return t
	case HTTP3:
		return &h3Fallback{h3: quicTransport(), h2: tcpTransport()}
	default:
		return tcpTransport()
	}
}

func tcpTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(64)},
	}
}

func quicTransport() *http3.Transport {
	return &http3.Transport{
		TLSClientConfig: &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(64)},
		QUICConfig: &quic.Config{
			HandshakeIdleTimeout: quicHandshakeTimeout,
			KeepAlivePeriod:      15 * time.Second,
		},
		// DialAddr returns only after the handshake completes, so every
		// failure before a request could be sent surfaces here, and only
		// here, as a dialError.
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			conn, err := quic.DialAddr(ctx, addr, tlsCfg, cfg)
			if err != nil {
				return nil, &dialError{err}
			}
			return conn, nil
		},
	}
}

// dialError is a failure to establish a QUIC connection: no request was sent.
type dialError struct{ err error }

func (e *dialError) Error() string { return "quic dial: " + e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// h3Fallback sends over HTTP/3 until a QUIC handshake fails, then over TCP
// for the rest of the process. Retrying after a failed handshake is safe: the
// request never left the client.
type h3Fallback struct {
	h3     http.RoundTripper
	h2     http.RoundTripper
	broken atomic.Bool
}

func (f *h3Fallback) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.broken.Load() {
		return f.h2.RoundTrip(req)
	}
	resp, err := f.h3.RoundTrip(req)
	if err == nil || !handshakeFailed(err) || req.Context().Err() != nil {
		return resp, err
	}
	f.broken.Store(true)
	retry := req.Clone(req.Context())
	if req.Body != nil {
		if req.GetBody == nil {
			return nil, err
		}
		body, gerr := req.GetBody()
		if gerr != nil {
			return nil, err
		}
		retry.Body = body
	}
	return f.h2.RoundTrip(retry)
}

// Fallback reports whether QUIC was found unusable and requests now use TCP.
func (f *h3Fallback) Fallback() bool { return f.broken.Load() }

func (f *h3Fallback) CloseIdleConnections() {
	for _, t := range []http.RoundTripper{f.h3, f.h2} {
		if c, ok := t.(interface{ CloseIdleConnections() }); ok {
			c.CloseIdleConnections()
		}
	}
}

// handshakeFailed reports errors that can only happen before a request was
// sent: the QUIC connection could not be established. An idle timeout looks
// the same whether it fires during the handshake or after a request went out,
// so the error type alone cannot tell them apart; where it happened can.
func handshakeFailed(err error) bool {
	var de *dialError
	return errors.As(err, &de)
}

// WithProtocol switches the adapter's wire protocol and returns it.
func (a *Adapter) WithProtocol(p Protocol) *Adapter {
	a.HTTP.Transport = NewTransport(p)
	return a
}

// Warm opens the connection (TCP or QUIC, plus TLS) before the first model
// call needs it, so the first task does not pay for the handshake. Any HTTP
// status counts as warm; only a network failure is an error.
func (a *Adapter) Warm(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.BaseURL+"/models", nil)
	if err != nil {
		return err
	}
	if a.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.APIKey)
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}
