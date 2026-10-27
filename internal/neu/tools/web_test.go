package tools

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
)

const page = `<!doctype html><html><head><title>The  Page</title><style>body{color:red}</style>
<script>var secret = "do not show";</script></head>
<body><nav><a href="/docs">Docs</a> <a href="#top">top</a></nav>
<h1>Hello</h1><p>First   paragraph
with a <a href="https://example.org/x">link</a>.</p>
<ul><li>one</li><li>two</li></ul>
<pre>  keep
    indent</pre>
<noscript>enable js</noscript><img src=x alt="a cat"><p>Last<br>line</p></body></html>`

func TestHTMLText(t *testing.T) {
	base, _ := url.Parse("https://site.example/a/b")
	got := HTMLText(page, base)
	for _, want := range []string{
		"Title: The Page\n", "Docs (https://site.example/docs)", "# Hello\n", "First paragraph with a link (https://example.org/x).",
		"- one\n- two\n", "  keep\n    indent", "[image: a cat]", "Last\nline",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	for _, bad := range []string{"secret", "color:red", "enable js", "<", "(#top)", "\n\n\n"} {
		if strings.Contains(got, bad) {
			t.Errorf("%q in:\n%s", bad, got)
		}
	}
}

func webTools(t *testing.T, allowPrivate bool) *Tools {
	root, _ := workspace(t)
	tl := codingTools(t, root)
	tl.Web = Web{AllowPrivate: allowPrivate}
	return tl
}

func TestWebFetch(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, page)
	})
	mux.HandleFunc("/hop/", func(w http.ResponseWriter, r *http.Request) {
		var n int
		fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/hop/"), "%d", &n)
		if n == 0 {
			http.Redirect(w, r, "/page", http.StatusFound)
			return
		}
		http.Redirect(w, r, fmt.Sprintf("/hop/%d", n-1), http.StatusFound)
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, strings.Repeat("0123456789", 10000))
	})
	mux.HandleFunc("/bin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte{0x89, 'P', 'N', 'G', 0, 0})
	})
	mux.HandleFunc("/ftp", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ftp://example.com/x", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	tl := webTools(t, true)

	out, isErr := call(t, tl, "web_fetch", map[string]any{"url": srv.URL + "/hop/3"}, "")
	if isErr || !strings.Contains(out, "URL: "+srv.URL+"/page\n") || !strings.Contains(out, "# Hello") || !strings.Contains(out, "Docs ("+srv.URL+"/docs)") || strings.Contains(out, "secret") {
		t.Fatalf("redirected page = %q", out)
	}
	if out, isErr := call(t, tl, "web_fetch", map[string]any{"url": srv.URL + "/hop/4"}, ""); isErr {
		t.Fatalf("5 redirects = %q", out) // hop/4 … hop/0, then /page
	}
	if out, isErr := call(t, tl, "web_fetch", map[string]any{"url": srv.URL + "/hop/5"}, ""); !isErr || !strings.Contains(out, "redirects") {
		t.Fatalf("6 redirects = %q", out)
	}
	out, isErr = call(t, tl, "web_fetch", map[string]any{"url": srv.URL + "/big", "max_bytes": 1000}, "")
	if isErr || !strings.Contains(out, "more bytes of text") || len(out) > 1300 {
		t.Fatalf("capped = %d bytes %q", len(out), out[len(out)-120:])
	}
	if out, isErr := call(t, tl, "web_fetch", map[string]any{"url": srv.URL + "/bin"}, ""); !isErr || !strings.Contains(out, "image/png, not text") {
		t.Fatalf("binary = %q", out)
	}
	for _, u := range []string{"file:///etc/passwd", "ftp://example.com/x", srv.URL + "/ftp", "notaurl"} {
		if out, isErr := call(t, tl, "web_fetch", map[string]any{"url": u}, ""); !isErr {
			t.Errorf("%s = %q; want refused", u, out)
		}
	}

	// Without -web-allow-private the test server's loopback address and
	// every local or private name are refused.
	tl = webTools(t, false)
	for _, u := range []string{srv.URL + "/page", "http://localhost:43511/", "http://foo.localhost/", "http://10.1.2.3/", "http://192.168.123.61:43127/", "http://[::1]/", "http://169.254.169.254/latest/meta-data", "http://100.100.1.1/"} {
		out, isErr := call(t, tl, "web_fetch", map[string]any{"url": u}, "")
		if !isErr || !strings.Contains(out, "web_fetch fetches public addresses only") {
			t.Errorf("%s = %q; want a private address refused", u, out)
		}
	}
}

func TestPrivateAddr(t *testing.T) {
	for _, s := range []string{"127.0.0.1", "::1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.1.1", "fe80::1", "fc00::1", "0.0.0.0", "::ffff:127.0.0.1", "100.64.0.1", "224.0.0.1"} {
		if !privateAddr(netip.MustParseAddr(s)) {
			t.Errorf("%s is public", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "142.250.1.1", "2606:4700::1111"} {
		if privateAddr(netip.MustParseAddr(s)) {
			t.Errorf("%s is private", s)
		}
	}
}
