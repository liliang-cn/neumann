package decide

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// clipArgs renders a call's arguments in about n bytes. Arguments that are
// a JSON object keep every key, at every depth: a field the model wrote
// last is as likely to be the dangerous one as the first ("comment": a page
// of prose, then "cmd": "rm -rf /"). Long string values are cut in the
// middle, the longest first, until the whole fits; only if the keys alone
// do not fit is the rendering itself cut in the middle. Anything that is not
// a JSON object is cut in the middle.
func clipArgs(args []byte, n int) string {
	full := compact(args)
	if len(full) <= n {
		return full
	}
	v, err := parseOrdered(args)
	if err != nil {
		return clipMiddle(full, n)
	}
	if _, ok := v.(*object); !ok {
		return clipMiddle(full, n)
	}
	// The largest cap on string values that fits, by bisection; a value
	// under the cap is shown whole.
	lo, hi := minString, longestString(v)
	best := ""
	for lo <= hi {
		mid := (lo + hi) / 2
		if r := render(v, mid); len(r) <= n {
			best, lo = r, mid+1
		} else {
			hi = mid - 1
		}
	}
	if best == "" {
		return clipMiddle(render(v, minString), n)
	}
	return best
}

// minString is the shortest a string value is cut to: enough for its start,
// its end and the marker.
const minString = 48

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

// render writes v compactly, with every string value longer than limit cut
// in the middle to about limit bytes.
func render(v any, limit int) string {
	var b strings.Builder
	write(&b, v, limit)
	return b.String()
}

func write(b *strings.Builder, v any, limit int) {
	switch v := v.(type) {
	case *object:
		b.WriteByte('{')
		for i, k := range v.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(b, k)
			b.WriteByte(':')
			write(b, v.vals[i], limit)
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, x := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			write(b, x, limit)
		}
		b.WriteByte(']')
	case string:
		writeString(b, cutString(v, limit))
	case literal:
		b.WriteString(string(v))
	}
}

// cutString cuts s to about n bytes, keeping its first two thirds and last
// third, with a short marker between.
func cutString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	head := runeStart(s, n*2/3)
	tail := len(s) - (n - head)
	for tail < len(s) && !utf8.RuneStart(s[tail]) {
		tail++
	}
	return fmt.Sprintf("%s…[%d bytes cut]…%s", s[:head], tail-head, s[tail:])
}

// writeString writes s as a JSON string, without HTML escaping.
func writeString(b *strings.Builder, s string) {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetEscapeHTML(false)
	e.Encode(s)
	b.Write(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}
