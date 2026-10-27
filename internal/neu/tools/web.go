package tools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"github.com/liliang-cn/neumann/bus"
)

// web_fetch's limits.
const (
	WebTimeout      = 30 * time.Second
	WebMaxRedirects = 5
	WebMaxBody      = 4 << 20 // bytes read from the server
	WebDefBytes     = 20 << 10
	WebMaxBytes     = MaxOutputBytes
)

// Web is how web_fetch fetches.
type Web struct {
	// AllowPrivate lets it fetch loopback, private and link-local
	// addresses (and localhost); off, they are refused.
	AllowPrivate bool
	// Client replaces the HTTP client (tests); nil: one that honours
	// HTTPS_PROXY / HTTP_PROXY / NO_PROXY.
	Client *http.Client
}

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// privateAddr reports whether ip is not a public unicast address.
func privateAddr(ip netip.Addr) bool {
	ip = ip.Unmap()
	return !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() || cgnat.Contains(ip)
}

var errPrivate = errors.New("is a local or private address; web_fetch fetches public addresses only (neu -web-allow-private allows them)")

// checkHost refuses a host that is, or resolves to, a private address.
func checkHost(ctx context.Context, host string) error {
	h := strings.TrimSuffix(strings.ToLower(host), ".")
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return fmt.Errorf("%s %w", host, errPrivate)
	}
	if ip, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		if privateAddr(ip) {
			return fmt.Errorf("%s %w", host, errPrivate)
		}
		return nil
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", h)
	if err != nil {
		return fmt.Errorf("resolve %s: %v", host, err)
	}
	for _, a := range addrs {
		if privateAddr(a) {
			return fmt.Errorf("%s (%s) %w", host, a, errPrivate)
		}
	}
	return nil
}

func checkURL(ctx context.Context, u *url.URL, allowPrivate bool) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s: only http and https URLs are fetched", u.Redacted())
	}
	if u.Hostname() == "" {
		return fmt.Errorf("%s: no host", u.Redacted())
	}
	if allowPrivate {
		return nil
	}
	return checkHost(ctx, u.Hostname())
}

func (w Web) client() *http.Client {
	if w.Client != nil {
		c := *w.Client
		c.CheckRedirect = w.checkRedirect
		return &c
	}
	d := &net.Dialer{Timeout: 10 * time.Second}
	if !w.AllowPrivate {
		// A direct connection is checked again where it lands, against a
		// name that resolved one way for checkHost and another for the dial.
		// A proxy is dialled as configured; it resolves the target itself.
		proxies := proxyHosts()
		d.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil || proxies[address] {
				return nil
			}
			if privateAddr(ap.Addr()) {
				return fmt.Errorf("%s %w", ap.Addr(), errPrivate)
			}
			return nil
		}
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           d.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		MaxIdleConns:          4,
		IdleConnTimeout:       30 * time.Second,
	}
	return &http.Client{Transport: tr, CheckRedirect: w.checkRedirect, Timeout: WebTimeout}
}

// proxyHosts are the addresses of the configured proxies, which the dial
// check lets through (a proxy on the loopback address is common).
func proxyHosts() map[string]bool {
	out := map[string]bool{}
	for _, u := range []string{"https://example.com", "http://example.com"} {
		req, _ := http.NewRequest(http.MethodGet, u, nil)
		p, err := http.ProxyFromEnvironment(req)
		if err != nil || p == nil {
			continue
		}
		port := p.Port()
		if port == "" {
			port = map[string]string{"https": "443", "http": "80", "socks5": "1080"}[p.Scheme]
		}
		host := p.Hostname()
		out[net.JoinHostPort(host, port)] = true
		if ips, err := net.LookupHost(host); err == nil {
			for _, ip := range ips {
				out[net.JoinHostPort(ip, port)] = true
			}
		}
	}
	return out
}

func (w Web) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > WebMaxRedirects {
		return fmt.Errorf("stopped after %d redirects", WebMaxRedirects)
	}
	return checkURL(req.Context(), req.URL, w.AllowPrivate)
}

func (t *Tools) webFetch(ctx context.Context, c bus.DeviceCall) (string, error) {
	var in struct {
		URL      string
		MaxBytes int `json:"max_bytes"`
	}
	if err := decode(c.Args, &in); err != nil {
		return "", err
	}
	if in.URL == "" {
		return "", errors.New("url is required")
	}
	u, err := url.Parse(in.URL)
	if err != nil {
		return "", fmt.Errorf("bad url: %v", err)
	}
	if err := checkURL(ctx, u, t.Web.AllowPrivate); err != nil {
		return "", err
	}
	limit := in.MaxBytes
	if limit <= 0 {
		limit = WebDefBytes
	}
	limit = min(limit, WebMaxBytes)
	ctx, cancel := context.WithTimeout(ctx, WebTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "neu-web-fetch/1 (+https://github.com/liliang-cn/neumann)")
	req.Header.Set("Accept", "text/html,text/plain,application/json,application/xml;q=0.9,*/*;q=0.5")
	resp, err := t.Web.client().Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return "", fmt.Errorf("fetch %s: %v", u.Redacted(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, WebMaxBody+1))
	if err != nil {
		return "", fmt.Errorf("read %s: %v", resp.Request.URL.Redacted(), err)
	}
	cut := len(body) > WebMaxBody
	if cut {
		body = body[:WebMaxBody]
	}
	final := resp.Request.URL
	ct := resp.Header.Get("Content-Type")
	mt, _, _ := mime.ParseMediaType(ct)
	if mt == "" {
		mt = http.DetectContentType(body)
		mt, _, _ = mime.ParseMediaType(mt)
	}
	var text string
	switch {
	case mt == "text/html" || mt == "application/xhtml+xml":
		text = HTMLText(string(body), final)
	case strings.HasPrefix(mt, "text/") || strings.HasSuffix(mt, "json") || strings.HasSuffix(mt, "xml") || mt == "application/javascript":
		text = string(body)
	default:
		return "", fmt.Errorf("%s is %s, not text; web_fetch returns text only", final.Redacted(), or(mt, "of unknown type"))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "URL: %s\nstatus: %s\n", final.Redacted(), resp.Status)
	if mt != "" {
		fmt.Fprintf(&b, "type: %s\n", mt)
	}
	b.WriteString("\n")
	if len(text) > limit {
		text = text[:limit] + fmt.Sprintf("\n… [%d more bytes of text; max_bytes raises the cap, up to %d]", len(text)-limit, WebMaxBytes)
	} else if cut {
		text += fmt.Sprintf("\n… [the page is over %d bytes; the rest was not read]", WebMaxBody)
	}
	b.WriteString(text)
	return b.String(), nil
}

func or(s, def string) string {
	if s != "" {
		return s
	}
	return def
}

// HTMLText is the readable text of an HTML page: no scripts, styles or
// tags; headings, list items and paragraphs on lines of their own; links
// as "text (url)" with url resolved against base.
func HTMLText(src string, base *url.URL) string {
	doc, err := html.Parse(strings.NewReader(src))
	if err != nil {
		return src
	}
	w := &textWriter{}
	var title string
	var walk func(n *html.Node, pre bool)
	walk = func(n *html.Node, pre bool) {
		switch n.Type {
		case html.TextNode:
			w.text(n.Data, pre)
			return
		case html.ElementNode:
		case html.DocumentNode:
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				walk(c, pre)
			}
			return
		default:
			return
		}
		switch n.DataAtom {
		case atom.Script, atom.Style, atom.Noscript, atom.Template, atom.Svg, atom.Iframe, atom.Object, atom.Canvas, atom.Head:
			if n.DataAtom == atom.Head {
				for c := n.FirstChild; c != nil; c = c.NextSibling {
					if c.DataAtom == atom.Title && c.FirstChild != nil {
						title = strings.Join(strings.Fields(c.FirstChild.Data), " ")
					}
				}
			}
			return
		case atom.Br:
			w.newline()
			return
		case atom.Hr:
			w.block()
			w.raw("---")
			w.block()
			return
		case atom.Img:
			if alt := strings.TrimSpace(attr(n, "alt")); alt != "" {
				w.text("[image: "+alt+"]", false)
			}
			return
		}
		block, line := isBlock(n.DataAtom), isLine(n.DataAtom)
		switch {
		case block:
			w.block()
		case line:
			w.line()
		}
		switch n.DataAtom {
		case atom.H1, atom.H2, atom.H3, atom.H4, atom.H5, atom.H6:
			w.raw(strings.Repeat("#", int(n.Data[1]-'0')) + " ")
		case atom.Li:
			w.raw("- ")
		case atom.Td, atom.Th:
			w.text(" | ", false)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, pre || n.DataAtom == atom.Pre)
		}
		if n.DataAtom == atom.A {
			if href := strings.TrimSpace(attr(n, "href")); href != "" && !strings.HasPrefix(href, "#") && !strings.HasPrefix(strings.ToLower(href), "javascript:") {
				if base != nil {
					if r, err := base.Parse(href); err == nil {
						href = r.String()
					}
				}
				w.text(" ("+href+")", false)
			}
		}
		switch {
		case block:
			w.block()
		case line:
			w.line()
		}
	}
	walk(doc, false)
	out := w.String()
	if title != "" {
		out = "Title: " + title + "\n\n" + out
	}
	return out
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func isBlock(a atom.Atom) bool {
	switch a {
	case atom.P, atom.Div, atom.Section, atom.Article, atom.Main, atom.Header, atom.Footer, atom.Nav, atom.Aside,
		atom.H1, atom.H2, atom.H3, atom.H4, atom.H5, atom.H6, atom.Ul, atom.Ol, atom.Pre, atom.Blockquote,
		atom.Table, atom.Dl, atom.Figure, atom.Figcaption, atom.Form, atom.Address, atom.Details, atom.Summary:
		return true
	}
	return false
}

// isLine: an element on a line of its own, without a blank line around it.
func isLine(a atom.Atom) bool {
	switch a {
	case atom.Li, atom.Tr, atom.Dt, atom.Dd, atom.Option:
		return true
	}
	return false
}

// textWriter collapses whitespace outside <pre> and keeps at most one
// blank line between blocks.
type textWriter struct {
	b     strings.Builder
	space bool // a space is pending before the next word
}

func (w *textWriter) tail() string {
	s := w.b.String()
	return s[max(0, len(s)-2):]
}

func (w *textWriter) raw(s string) {
	w.b.WriteString(s)
	w.space = false
}

func (w *textWriter) text(s string, pre bool) {
	if pre {
		w.b.WriteString(s)
		w.space = false
		return
	}
	for i, f := range strings.Fields(s) {
		t := w.tail()
		if (i > 0 || w.space || startsSpace(s)) && t != "" && !strings.HasSuffix(t, "\n") && !strings.HasSuffix(t, " ") {
			w.b.WriteByte(' ')
		}
		w.b.WriteString(f)
		w.space = false
	}
	if endsSpace(s) {
		w.space = true
	}
}

func startsSpace(s string) bool { return s != "" && strings.ContainsRune(" \t\n\r\f", rune(s[0])) }
func endsSpace(s string) bool {
	return s != "" && strings.ContainsRune(" \t\n\r\f", rune(s[len(s)-1]))
}

func (w *textWriter) newline() {
	w.b.WriteByte('\n')
	w.space = false
}

func (w *textWriter) line() {
	if w.b.Len() > 0 && !strings.HasSuffix(w.tail(), "\n") {
		w.b.WriteByte('\n')
	}
	w.space = false
}

func (w *textWriter) block() {
	if w.b.Len() == 0 {
		return
	}
	switch t := w.tail(); {
	case t == "\n\n":
	case strings.HasSuffix(t, "\n"):
		w.b.WriteByte('\n')
	default:
		w.b.WriteString("\n\n")
	}
	w.space = false
}

func (w *textWriter) String() string {
	lines := strings.Split(w.b.String(), "\n")
	var out []string
	blank := 0
	for _, l := range lines {
		l = strings.TrimRight(l, " \t")
		if strings.TrimSpace(l) == "" {
			blank++
			if blank > 1 || len(out) == 0 {
				continue
			}
			l = ""
		} else {
			blank = 0
		}
		out = append(out, l)
	}
	return strings.TrimSpace(strings.Join(out, "\n")) + "\n"
}
