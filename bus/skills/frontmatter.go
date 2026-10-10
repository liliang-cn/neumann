package skills

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// The frontmatter of a SKILL.md is parsed by hand: it is a small, fixed subset
// of YAML, and a YAML library would be the only dependency of this package.
//
// Supported, which covers what skill files are written with:
//
//   - a mapping of keys at column 0, with '#' comments and blank lines;
//   - plain scalars, including plain multi-line scalars continued on more
//     indented lines (folded with spaces);
//   - double-quoted scalars (with \" \\ \/ \n \t \r \0 \uXXXX \UXXXXXXXX and an
//     escaped line break) and single-quoted scalars ('' is a quote), both of
//     which may span lines and are folded as YAML folds them;
//   - block scalars '|' and '>' with the chomping indicators '-' and '+';
//   - sequences, flow ([a, "b"]) or block ("- a", indented or not);
//   - nested mappings, block or flow ({k: v}), as values.
//
// Not supported, and reported as an error rather than misread: anchors,
// aliases, tags, multi-document streams, complex keys, sequences of mappings,
// and tabs in indentation.

type kind int

const (
	scalarKind kind = iota
	listKind
	mapKind
)

type yvalue struct {
	kind kind
	s    string
	list []string
	m    map[string]yvalue
	keys []string // m's keys in document order
}

// splitFrontmatter splits a SKILL.md into its frontmatter lines and body. The
// file must open with a line "---"; the frontmatter ends at the next "---" (or
// "...") line. A UTF-8 BOM and CRLF line endings are accepted.
func splitFrontmatter(src string) (front []string, body string, err error) {
	src = strings.TrimPrefix(src, "\ufeff")
	src = strings.ReplaceAll(src, "\r\n", "\n")
	lines := strings.Split(src, "\n")
	if len(lines) == 0 || strings.TrimRight(lines[0], " \t") != "---" {
		return nil, "", errors.New("SKILL.md does not open with a '---' frontmatter line")
	}
	for i := 1; i < len(lines); i++ {
		if l := strings.TrimRight(lines[i], " \t"); l == "---" || l == "..." {
			body = strings.Join(lines[i+1:], "\n")
			return lines[1:i], strings.TrimSpace(body), nil
		}
	}
	return nil, "", errors.New("frontmatter is not closed by a '---' line")
}

// parseFrontmatter parses frontmatter lines into a mapping.
func parseFrontmatter(lines []string) (yvalue, error) {
	for i, l := range lines {
		ind := l[:len(l)-len(strings.TrimLeft(l, " \t"))]
		if strings.Contains(ind, "\t") {
			return yvalue{}, fmt.Errorf("frontmatter line %d: tab in indentation", i+2)
		}
	}
	v, err := parseMapping(lines, 2)
	if err != nil {
		return yvalue{}, err
	}
	return v, nil
}

func indentOf(l string) int { return len(l) - len(strings.TrimLeft(l, " ")) }

func blank(l string) bool {
	t := strings.TrimSpace(l)
	return t == "" || strings.HasPrefix(t, "#")
}

// parseMapping parses lines whose keys sit at column 0. lineNo is the file's
// line number of lines[0], for messages.
func parseMapping(lines []string, lineNo int) (yvalue, error) {
	out := yvalue{kind: mapKind, m: map[string]yvalue{}}
	for i := 0; i < len(lines); {
		l := lines[i]
		if blank(l) {
			i++
			continue
		}
		at := lineNo + i
		if indentOf(l) != 0 {
			return out, fmt.Errorf("frontmatter line %d: unexpected indentation", at)
		}
		key, rest, ok := splitKey(l)
		if !ok {
			return out, fmt.Errorf("frontmatter line %d: expected 'key: value', got %q", at, l)
		}
		if _, dup := out.m[key]; dup {
			return out, fmt.Errorf("frontmatter line %d: duplicate key %q", at, key)
		}
		// The value's continuation: every following line that is indented or
		// blank, plus, for an empty value, a compact sequence at column 0.
		j := i + 1
		for j < len(lines) && (strings.TrimSpace(lines[j]) == "" || indentOf(lines[j]) > 0 ||
			(strings.TrimSpace(rest) == "" && strings.HasPrefix(lines[j], "- ")) ||
			(strings.TrimSpace(rest) == "" && strings.TrimSpace(lines[j]) == "-")) {
			j++
		}
		v, err := parseValue(rest, lines[i+1:j], at)
		if err != nil {
			return out, err
		}
		out.m[key] = v
		out.keys = append(out.keys, key)
		i = j
	}
	return out, nil
}

// splitKey splits "key: rest". The colon must be followed by a space or the
// end of the line.
func splitKey(l string) (key, rest string, ok bool) {
	for i := 0; i < len(l); i++ {
		if l[i] == ':' && (i+1 == len(l) || l[i+1] == ' ') {
			key = strings.TrimSpace(l[:i])
			if key == "" || strings.ContainsAny(key[:1], `"'[{&*!|>%@`+"`") || strings.HasPrefix(key, "- ") {
				return "", "", false
			}
			return key, l[i+1:], true
		}
	}
	return "", "", false
}

// parseValue parses the value after "key:" — rest is the remainder of the key
// line, cont the lines that continue it.
func parseValue(rest string, cont []string, at int) (yvalue, error) {
	r := strings.TrimSpace(rest)
	switch {
	case r == "" || strings.HasPrefix(r, "#"):
		return parseNested(cont, at+1)
	case r[0] == '|' || r[0] == '>':
		s, err := parseBlockScalar(r, cont, at)
		return yvalue{s: s}, err
	case r[0] == '"' || r[0] == '\'':
		s, used, tail, err := parseQuoted(r, cont, at)
		if err != nil {
			return yvalue{}, err
		}
		if !blank(tail) {
			return yvalue{}, fmt.Errorf("frontmatter line %d: text after the closing quote", at+used)
		}
		if err := onlyBlank(cont[used:], at+used+1); err != nil {
			return yvalue{}, err
		}
		return yvalue{s: s}, nil
	case r[0] == '[' || r[0] == '{':
		flow := r
		for _, c := range cont {
			flow += " " + strings.TrimSpace(c)
		}
		return parseFlow(strings.TrimSpace(flow), at)
	case strings.ContainsAny(r[:1], "&*!%@`"):
		return yvalue{}, fmt.Errorf("frontmatter line %d: anchors, aliases, tags and directives are not supported", at)
	}
	return yvalue{s: foldPlain(stripComment(r), cont)}, nil
}

// onlyBlank requires that nothing but blank lines and comments follow.
func onlyBlank(lines []string, at int) error {
	for i, l := range lines {
		if !blank(l) {
			return fmt.Errorf("frontmatter line %d: unexpected text %q", at+i, strings.TrimSpace(l))
		}
	}
	return nil
}

// parseNested parses the indented lines under a key with no inline value: a
// block sequence, a nested mapping, or nothing (an empty value).
func parseNested(cont []string, at int) (yvalue, error) {
	first := -1
	for i, l := range cont {
		if !blank(l) {
			first = i
			break
		}
	}
	if first < 0 {
		return yvalue{}, nil
	}
	ind := indentOf(cont[first])
	t := strings.TrimSpace(cont[first])
	if t == "-" || strings.HasPrefix(t, "- ") {
		return parseBlockList(cont, ind, at)
	}
	if _, _, ok := splitKey(strings.TrimSpace(cont[first])); !ok {
		// A plain scalar that starts on the next line.
		return yvalue{s: foldPlain("", cont)}, nil
	}
	ded := make([]string, len(cont))
	for i, l := range cont {
		switch {
		case blank(l):
			ded[i] = ""
		case indentOf(l) < ind:
			return yvalue{}, fmt.Errorf("frontmatter line %d: inconsistent indentation", at+i)
		default:
			ded[i] = l[ind:]
		}
	}
	return parseMapping(ded, at)
}

func parseBlockList(cont []string, ind, at int) (yvalue, error) {
	out := yvalue{kind: listKind}
	for i := 0; i < len(cont); i++ {
		l := cont[i]
		if blank(l) {
			continue
		}
		t := strings.TrimSpace(l)
		if indentOf(l) != ind || (t != "-" && !strings.HasPrefix(t, "- ")) {
			return out, fmt.Errorf("frontmatter line %d: expected a '- item' at the sequence's indentation", at+i)
		}
		item := strings.TrimSpace(strings.TrimPrefix(t, "-"))
		switch {
		case item == "":
			out.list = append(out.list, "")
		case item[0] == '"' || item[0] == '\'':
			s, used, tail, err := parseQuoted(item, cont[i+1:], at+i)
			if err != nil {
				return out, err
			}
			if !blank(tail) {
				return out, fmt.Errorf("frontmatter line %d: text after the closing quote", at+i+used)
			}
			out.list = append(out.list, s)
			i += used
		case item[0] == '[' || item[0] == '{' || item[0] == '|' || item[0] == '>':
			return out, fmt.Errorf("frontmatter line %d: nested collections in a sequence are not supported", at+i)
		default:
			if _, _, ok := splitKey(item); ok {
				return out, fmt.Errorf("frontmatter line %d: sequences of mappings are not supported", at+i)
			}
			out.list = append(out.list, stripComment(item))
		}
	}
	return out, nil
}

// stripComment removes a " #" comment from a plain scalar.
func stripComment(s string) string {
	if strings.HasPrefix(s, "#") {
		return ""
	}
	for i := 1; i < len(s); i++ {
		if s[i] == '#' && (s[i-1] == ' ' || s[i-1] == '\t') {
			return strings.TrimSpace(s[:i])
		}
	}
	return strings.TrimSpace(s)
}

// foldPlain folds a plain scalar's continuation lines: a single line break is
// a space, each blank line between text is a newline.
func foldPlain(first string, cont []string) string {
	var b strings.Builder
	b.WriteString(first)
	breaks := 0
	for _, l := range cont {
		t := strings.TrimSpace(l)
		if t == "" {
			breaks++
			continue
		}
		if strings.HasPrefix(t, "#") {
			continue
		}
		t = stripComment(t)
		if b.Len() > 0 {
			if breaks == 0 {
				b.WriteByte(' ')
			} else {
				b.WriteString(strings.Repeat("\n", breaks))
			}
		}
		breaks = 0
		b.WriteString(t)
	}
	s := b.String()
	if s == "~" || s == "null" {
		return ""
	}
	return s
}

// parseQuoted parses a quoted scalar that opens at the start of first and may
// continue on the lines of cont. It returns the value, how many lines of cont
// it consumed, and what followed the closing quote on its last line.
func parseQuoted(first string, cont []string, at int) (val string, used int, tail string, err error) {
	q := first[0]
	var b strings.Builder
	line := first[1:]
	breaks := -1 // -1: no line break pending
	for {
		if breaks >= 0 {
			// Folding: trailing white space before a break is dropped, one
			// break is a space, n+1 breaks are n newlines.
			s := strings.TrimRight(b.String(), " \t")
			b.Reset()
			b.WriteString(s)
			if breaks == 0 {
				b.WriteByte(' ')
			} else {
				b.WriteString(strings.Repeat("\n", breaks))
			}
			breaks = -1
		}
		for i := 0; i < len(line); i++ {
			c := line[i]
			switch {
			case c == q && q == '\'' && i+1 < len(line) && line[i+1] == '\'':
				b.WriteByte('\'')
				i++
			case c == q:
				return b.String(), used, line[i+1:], nil
			case c == '\\' && q == '"':
				if i+1 == len(line) {
					// An escaped line break: join without a space.
					line = ""
					if used == len(cont) {
						return "", 0, "", fmt.Errorf("frontmatter line %d: unterminated quoted string", at)
					}
					line = strings.TrimLeft(cont[used], " \t")
					used++
					i = -1
					continue
				}
				n, adv, e := unescape(line[i+1:])
				if e != nil {
					return "", 0, "", fmt.Errorf("frontmatter line %d: %v", at+used, e)
				}
				b.WriteString(n)
				i += adv
			default:
				b.WriteByte(c)
			}
		}
		// The line ended inside the quotes: continue on the next one.
		breaks = 0
		for {
			if used == len(cont) {
				return "", 0, "", fmt.Errorf("frontmatter line %d: unterminated quoted string", at)
			}
			next := strings.TrimLeft(cont[used], " \t")
			used++
			if strings.TrimSpace(next) == "" {
				breaks++
				continue
			}
			line = next
			break
		}
	}
}

// unescape decodes one escape after a backslash; adv is how many bytes of s it
// consumed.
func unescape(s string) (string, int, error) {
	if s == "" {
		return "", 0, errors.New("dangling backslash")
	}
	switch s[0] {
	case '"', '\\', '/', ' ':
		return s[:1], 1, nil
	case 'n':
		return "\n", 1, nil
	case 't', '\t':
		return "\t", 1, nil
	case 'r':
		return "\r", 1, nil
	case '0':
		return "\x00", 1, nil
	case 'x', 'u', 'U':
		n := map[byte]int{'x': 2, 'u': 4, 'U': 8}[s[0]]
		if len(s) < 1+n {
			return "", 0, fmt.Errorf("short \\%c escape", s[0])
		}
		r, err := strconv.ParseUint(s[1:1+n], 16, 32)
		if err != nil {
			return "", 0, fmt.Errorf("bad \\%c escape %q", s[0], s[1:1+n])
		}
		return string(rune(r)), 1 + n, nil
	}
	return "", 0, fmt.Errorf("unknown escape \\%c", s[0])
}

// parseBlockScalar parses a '|' or '>' block scalar whose header is h.
func parseBlockScalar(h string, cont []string, at int) (string, error) {
	folded := h[0] == '>'
	chomp := byte(0)
	for _, c := range []byte(stripComment(h[1:])) {
		switch {
		case c == '-' || c == '+':
			chomp = c
		case c >= '1' && c <= '9':
			// An explicit indentation indicator; the content's own
			// indentation is used instead, which agrees for any file that
			// does not start its block with extra spaces.
		default:
			return "", fmt.Errorf("frontmatter line %d: bad block scalar header %q", at, h)
		}
	}
	ind := 0
	for _, l := range cont {
		if strings.TrimSpace(l) != "" {
			ind = indentOf(l)
			break
		}
	}
	var lines []string
	for i, l := range cont {
		if strings.TrimSpace(l) == "" {
			lines = append(lines, "")
			continue
		}
		if indentOf(l) < ind {
			return "", fmt.Errorf("frontmatter line %d: block scalar line is less indented than its first line", at+1+i)
		}
		lines = append(lines, l[ind:])
	}
	trailing := 0
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
		trailing++
	}
	var b strings.Builder
	if folded {
		empties := 0
		prevMore := false
		for i, l := range lines {
			if l == "" {
				empties++
				continue
			}
			more := l[0] == ' ' || l[0] == '\t'
			if i > 0 && b.Len() > 0 {
				switch {
				case more || prevMore:
					b.WriteString(strings.Repeat("\n", empties+1))
				case empties == 0:
					b.WriteByte(' ')
				default:
					b.WriteString(strings.Repeat("\n", empties))
				}
			} else if empties > 0 {
				b.WriteString(strings.Repeat("\n", empties))
			}
			empties = 0
			prevMore = more
			b.WriteString(l)
		}
	} else {
		b.WriteString(strings.Join(lines, "\n"))
	}
	s := b.String()
	if s == "" {
		return "", nil
	}
	switch chomp {
	case '-':
	case '+':
		s += strings.Repeat("\n", trailing+1)
	default:
		s += "\n"
	}
	return s, nil
}

// parseFlow parses a one-level flow sequence or mapping; items are plain or
// quoted scalars.
func parseFlow(s string, at int) (yvalue, error) {
	open, closing := s[0], byte(']')
	if open == '{' {
		closing = '}'
	}
	end := -1
	var items []string
	var cur strings.Builder
	for i := 1; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\'':
			v, _, tail, err := parseQuoted(s[i:], nil, at)
			if err != nil {
				return yvalue{}, err
			}
			// Re-quote so a comma or colon inside stays part of the item.
			cur.WriteString("\x00" + v + "\x00")
			i = len(s) - len(tail) - 1
		case c == '[' || c == '{':
			return yvalue{}, fmt.Errorf("frontmatter line %d: nested flow collections are not supported", at)
		case c == ',':
			items = append(items, cur.String())
			cur.Reset()
		case c == closing:
			end = i
			items = append(items, cur.String())
			i = len(s)
		default:
			cur.WriteByte(c)
		}
	}
	if end < 0 {
		return yvalue{}, fmt.Errorf("frontmatter line %d: unterminated flow collection", at)
	}
	if !blank(s[end+1:]) {
		return yvalue{}, fmt.Errorf("frontmatter line %d: text after a flow collection", at)
	}
	scalar := func(raw string) string {
		raw = strings.TrimSpace(raw)
		if strings.HasPrefix(raw, "\x00") && strings.HasSuffix(raw, "\x00") && len(raw) >= 2 {
			return raw[1 : len(raw)-1]
		}
		return raw
	}
	// A trailing comma leaves an empty last item; drop it.
	if n := len(items); n > 0 && strings.TrimSpace(items[n-1]) == "" {
		items = items[:n-1]
	}
	if open == '[' {
		out := yvalue{kind: listKind}
		for _, it := range items {
			out.list = append(out.list, scalar(it))
		}
		return out, nil
	}
	out := yvalue{kind: mapKind, m: map[string]yvalue{}}
	for _, it := range items {
		k, v, ok := cutFlowPair(it)
		if !ok {
			return yvalue{}, fmt.Errorf("frontmatter line %d: flow mapping item %q is not 'key: value'", at, strings.TrimSpace(it))
		}
		k = scalar(k)
		if _, dup := out.m[k]; dup {
			return yvalue{}, fmt.Errorf("frontmatter line %d: duplicate key %q", at, k)
		}
		out.m[k] = yvalue{s: scalar(v)}
		out.keys = append(out.keys, k)
	}
	return out, nil
}

// cutFlowPair splits "k: v" outside quoted text (which parseFlow marked with
// NUL bytes).
func cutFlowPair(it string) (k, v string, ok bool) {
	inQ := false
	for i := 0; i < len(it); i++ {
		switch {
		case it[i] == 0:
			inQ = !inQ
		case !inQ && it[i] == ':' && (i+1 == len(it) || it[i+1] == ' '):
			return it[:i], it[i+1:], true
		}
	}
	return "", "", false
}
