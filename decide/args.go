package decide

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// clipArgs renders a call's arguments in about n bytes, and reports whether
// they are whole: every key shown, every short value (ShortValue bytes or
// less) as written, and every long value with its start and its end. A
// field the model wrote anywhere is as likely to be the dangerous one as
// the first ("comment": a page of prose, then "cmd": "rm -rf /"), so
// arguments that are valid JSON keep every key at every depth, and are cut
// in this order until they fit:
//
//  1. long values, to a common cap, in the middle (a start, a marker, an
//     end), the longest losing the most: still whole;
//  2. long values, to a bare "…": no longer whole, since a long command
//     reduced to "…" says nothing about what it would do;
//  3. short values, to a common cap, then to "…": not whole;
//  4. the rendering itself, in the middle: not whole.
//
// Invalid JSON is cut in the middle, and is not whole once cut.
func clipArgs(args []byte, n int) (string, bool) {
	full := compact(args)
	if len(full) <= n {
		return full, true
	}
	v, err := parseOrdered(args)
	if err != nil {
		return clipMiddle(full, n), false
	}
	const whole = ShortValue // a short cap that cuts no short value
	if r, ok := largestFit(n, minCut, longestString(v), func(c int) string { return render(v, c, whole) }); ok {
		return r, true
	}
	if r := render(v, 0, whole); len(r) <= n {
		return r, false
	}
	if r, ok := largestFit(n, 0, ShortValue-1, func(c int) string { return render(v, 0, c) }); ok {
		return r, false
	}
	return clipMiddle(render(v, 0, 0), n), false
}

// largestFit is the rendering at the largest cap in [lo, hi] that fits in n
// bytes, found by bisection (a rendering grows with its cap), and whether
// any fits.
func largestFit(n, lo, hi int, at func(cap int) string) (string, bool) {
	best, ok := "", false
	for lo <= hi {
		mid := (lo + hi) / 2
		if r := at(mid); len(r) <= n {
			best, ok, lo = r, true, mid+1
		} else {
			hi = mid - 1
		}
	}
	return best, ok
}

// ShortValue is the length, in bytes, up to which a string value in a
// call's arguments counts as short: Policy holds a call, without asking the
// engine, when its arguments cannot be shown with every short value whole.
// A dangerous command, a path or an account number is short; prose and
// payloads are long.
const ShortValue = 64

// object is a JSON object with its keys in the order they were written.
type object struct {
	keys []string
	vals []any
}

// literal is a number, true, false or null, as written.
type literal string

// parseOrdered parses JSON keeping object key order. Values are *object,
// []any, string or literal.
func parseOrdered(b []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	v, err := parseValue(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data")
	}
	return v, nil
}

func parseValue(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t := t.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := &object{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				v, err := parseValue(d)
				if err != nil {
					return nil, err
				}
				o.keys, o.vals = append(o.keys, k.(string)), append(o.vals, v)
			}
			_, err := d.Token() // '}'
			return o, err
		case '[':
			var a []any
			for d.More() {
				v, err := parseValue(d)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err := d.Token() // ']'
			return a, err
		}
		return nil, fmt.Errorf("unexpected %v", t)
	case string:
		return t, nil
	case json.Number:
		return literal(t.String()), nil
	case bool:
		return literal(fmt.Sprint(t)), nil
	case nil:
		return literal("null"), nil
	}
	return nil, fmt.Errorf("unexpected %T", t)
}

func longestString(v any) int {
	switch v := v.(type) {
	case *object:
		m := 0
		for _, x := range v.vals {
			m = max(m, longestString(x))
		}
		return m
	case []any:
		m := 0
		for _, x := range v {
			m = max(m, longestString(x))
		}
		return m
	case string:
		return len(v)
	}
	return 0
}

// render writes v compactly, with each string value cut to about long
// bytes if it is longer than ShortValue, or to about short bytes if not.
func render(v any, long, short int) string {
	var b strings.Builder
	write(&b, v, long, short)
	return b.String()
}

func write(b *strings.Builder, v any, long, short int) {
	switch v := v.(type) {
	case *object:
		b.WriteByte('{')
		for i, k := range v.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, k)
			b.WriteByte(':')
			write(b, v.vals[i], long, short)
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, x := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			write(b, x, long, short)
		}
		b.WriteByte(']')
	case string:
		limit := short
		if len(v) > ShortValue {
			limit = long
		}
		writeString(b, cutString(v, limit))
	case literal:
		b.WriteString(string(v))
	}
}

// cutString cuts s to about n bytes: its first two thirds and last third
// with a short marker between, or, when n leaves no room for that, a bare
// "…". s stays whole when the cut would not be shorter.
func cutString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < minCut {
		return "…"
	}
	head := runeStart(s, n*2/3)
	tail := len(s) - (n - head)
	for tail < len(s) && !utf8.RuneStart(s[tail]) {
		tail++
	}
	if c := fmt.Sprintf("%s…[%d bytes cut]…%s", s[:head], tail-head, s[tail:]); len(c) < len(s) {
		return c
	}
	return s
}

// minCut is the shortest cap that keeps a start and an end of a value.
const minCut = 24

// writeString writes s as a JSON string, without HTML escaping.
func writeString(b *strings.Builder, s string) {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetEscapeHTML(false)
	e.Encode(s)
	b.Write(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}
