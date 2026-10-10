package skills

import (
	"reflect"
	"strings"
	"testing"
)

func parse(t *testing.T, src string) (yvalue, string, error) {
	t.Helper()
	front, body, err := splitFrontmatter(src)
	if err != nil {
		return yvalue{}, "", err
	}
	v, err := parseFrontmatter(front)
	return v, body, err
}

func TestFrontmatterScalars(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{"plain", `description: Fill PDF forms`, "Fill PDF forms"},
		{"plain with comment", `description: Fill forms # not this`, "Fill forms"},
		{"plain with colon inside", `description: ratio 3:1 and a: b`, "ratio 3:1 and a: b"},
		{"plain hash without space", `description: issue#42`, "issue#42"},
		{"plain multi-line", "description: Fill PDF\n  forms and\n  extract text", "Fill PDF forms and extract text"},
		{"plain multi-line with blank", "description: one\n\n  two", "one\ntwo"},
		{"plain on next line", "description:\n  starts below\n  and goes on", "starts below and goes on"},
		{"double quoted", `description: "Use when: a \"form\" needs filling"`, `Use when: a "form" needs filling`},
		{"double quoted escapes", `description: "tab\there\nnew \u00e9 \\ \/"`, "tab\there\nnew é \\ /"},
		{"double quoted multi-line", "description: \"first line\n  second line\n\n  after blank\"", "first line second line\nafter blank"},
		{"double quoted escaped break", "description: \"glued\\\n  together\"", "gluedtogether"},
		{"double quoted comment after", `description: "x" # trailing`, "x"},
		{"single quoted", `description: 'it''s a "form": yes'`, `it's a "form": yes`},
		{"single quoted multi-line", "description: 'a\n   b'", "a b"},
		{"literal", "description: |\n  line one\n    indented\n  line three\n", "line one\n  indented\nline three\n"},
		{"literal strip", "description: |-\n  a\n  b\n\n", "a\nb"},
		{"literal keep", "description: |+\n  a\n\n", "a\n\n\n"}, // two blank lines follow, one from the closing line
		{"folded", "description: >\n  folded\n  text\n\n  para\n", "folded text\npara\n"},
		{"folded strip with comment", "description: >- # note\n  a\n  b", "a b"},
		{"null", `description: ~`, ""},
		{"empty", `description:`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, _, err := parse(t, "---\nname: x\n"+c.yaml+"\n---\nbody")
			if err != nil {
				t.Fatal(err)
			}
			got := v.m["description"]
			if got.kind != scalarKind || got.s != c.want {
				t.Fatalf("got %q (kind %d), want %q", got.s, got.kind, c.want)
			}
		})
	}
}

func TestFrontmatterCollections(t *testing.T) {
	src := `---
# a comment
name: x
allowed-tools: [Read, "Bash(git add:*)", 'Grep',]
block:
  - one
  - "two, three"   # comment
compact:
- a
- b
metadata:
  author: someone
  version: "1.2"
  nested:
    deep: yes
  tags: [a, b]
flowmap: {k: v, "q: x": 'y'}
---
`
	v, _, err := parse(t, src)
	if err != nil {
		t.Fatal(err)
	}
	if got := v.m["allowed-tools"].list; !reflect.DeepEqual(got, []string{"Read", "Bash(git add:*)", "Grep"}) {
		t.Errorf("flow list %q", got)
	}
	if got := v.m["block"].list; !reflect.DeepEqual(got, []string{"one", "two, three"}) {
		t.Errorf("block list %q", got)
	}
	if got := v.m["compact"].list; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("compact list %q", got)
	}
	md := map[string]string{}
	flatten("", v.m["metadata"], md)
	want := map[string]string{"author": "someone", "version": "1.2", "nested.deep": "yes", "tags": "a, b"}
	if !reflect.DeepEqual(md, want) {
		t.Errorf("metadata %q", md)
	}
	fm := v.m["flowmap"]
	if fm.kind != mapKind || fm.m["k"].s != "v" || fm.m["q: x"].s != "y" {
		t.Errorf("flow map %+v", fm)
	}
}

func TestFrontmatterSplit(t *testing.T) {
	_, body, err := parse(t, "\ufeff---\r\nname: x\r\ndescription: d\r\n---\r\n\r\n# Title\r\n\r\ntext\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if body != "# Title\n\ntext" {
		t.Fatalf("body %q", body)
	}
	// A '---' inside the body is the body's.
	_, body, err = parse(t, "---\nname: x\n---\na\n---\nb")
	if err != nil || body != "a\n---\nb" {
		t.Fatalf("body %q, %v", body, err)
	}
}

func TestFrontmatterErrors(t *testing.T) {
	cases := map[string]string{
		"no frontmatter":     "# Title\nname: x\n",
		"not closed":         "---\nname: x\ndescription: d\n",
		"unterminated quote": "---\ndescription: \"open\n  still open\n---\n",
		"tab indentation":    "---\nmetadata:\n\tk: v\n---\n",
		"duplicate key":      "---\nname: a\nname: b\n---\n",
		"not a key":          "---\nname: a\njust words\n---\n",
		"text after quote":   "---\nname: \"a\" b\n---\n",
		"anchor":             "---\nname: &a x\n---\n",
		"bad escape":         "---\nname: \"\\q\"\n---\n",
		"unclosed flow":      "---\nallowed-tools: [a, b\n---\n",
		"seq of maps":        "---\nlist:\n  - k: v\n---\n",
		"bad indentation":    "---\n  name: x\n---\n",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parse(t, src); err == nil {
				t.Fatal("parsed")
			}
		})
	}
}

func TestNormalizeAndValidateName(t *testing.T) {
	for in, want := range map[string]string{"PDF_Forms": "pdf-forms", " Data Analysis ": "data-analysis", "abc-1": "abc-1"} {
		n := normalizeName(in)
		if n != want || validName(n) != nil {
			t.Errorf("%q → %q (%v), want %q", in, n, validName(n), want)
		}
	}
	for _, bad := range []string{"", "a.b", "-a", "a-", "a--b", "ü", strings.Repeat("a", MaxNameLen+1)} {
		if validName(normalizeName(bad)) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
