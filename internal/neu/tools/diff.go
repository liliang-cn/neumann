package tools

import (
	"fmt"
	"strings"
)

// Limits on a diff shown to the model or the user.
const (
	diffContext  = 2
	diffMaxLines = 60
	diffMaxBytes = 4 << 10
	diffMaxCells = 1 << 20 // LCS table cells; above it the changed span is one hunk
)

// Diff is a compact unified diff of a file's change from before to after,
// at most maxLines lines (0: diffMaxLines) and diffMaxBytes bytes; "" when
// they are equal.
func Diff(name, before, after string, maxLines int) string {
	if before == after {
		return ""
	}
	if maxLines <= 0 {
		maxLines = diffMaxLines
	}
	a, b := diffLines(before), diffLines(after)
	ops := lineOps(a, b)
	var out []string
	out = append(out, "--- "+name, "+++ "+name)
	hunks := group(ops)
	for _, h := range hunks {
		out = append(out, h...)
	}
	total := len(out)
	var sb strings.Builder
	for i, l := range out {
		if i >= maxLines || sb.Len()+len(l)+1 > diffMaxBytes {
			fmt.Fprintf(&sb, "… [%d more diff lines]\n", total-i)
			break
		}
		sb.WriteString(clip(l, 200))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func diffLines(s string) []string {
	if s == "" {
		return nil
	}
	ls := strings.SplitAfter(s, "\n")
	if ls[len(ls)-1] == "" {
		ls = ls[:len(ls)-1]
	}
	for i, l := range ls {
		if strings.HasSuffix(l, "\n") {
			ls[i] = strings.TrimSuffix(strings.TrimSuffix(l, "\n"), "\r")
		} else {
			ls[i] = l + "⏎ (no newline at end)"
		}
	}
	return ls
}

// op is one line of the edit script: ' ' kept, '-' removed, '+' added,
// with its line numbers in a and b (1-based).
type op struct {
	kind   byte
	text   string
	ai, bi int
}

// lineOps is an edit script from a to b: the common prefix and suffix are
// kept, and the middle is diffed by LCS when it is small enough, else
// shown as removed and added.
func lineOps(a, b []string) []op {
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	var ops []op
	for i := 0; i < pre; i++ {
		ops = append(ops, op{' ', a[i], i + 1, i + 1})
	}
	ma, mb := a[pre:len(a)-suf], b[pre:len(b)-suf]
	if len(ma)*len(mb) <= diffMaxCells {
		ops = append(ops, lcsOps(ma, mb, pre)...)
	} else {
		for i, l := range ma {
			ops = append(ops, op{'-', l, pre + i + 1, 0})
		}
		for i, l := range mb {
			ops = append(ops, op{'+', l, 0, pre + i + 1})
		}
	}
	for i := 0; i < suf; i++ {
		ai, bi := len(a)-suf+i, len(b)-suf+i
		ops = append(ops, op{' ', a[ai], ai + 1, bi + 1})
	}
	return ops
}

func lcsOps(a, b []string, off int) []op {
	n, m := len(a), len(b)
	// t[i][j]: LCS length of a[i:] and b[j:].
	t := make([][]int32, n+1)
	for i := range t {
		t[i] = make([]int32, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				t[i][j] = t[i+1][j+1] + 1
			} else {
				t[i][j] = max(t[i+1][j], t[i][j+1])
			}
		}
	}
	var ops []op
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			ops = append(ops, op{' ', a[i], off + i + 1, off + j + 1})
			i++
			j++
		case i < n && (j == m || t[i+1][j] >= t[i][j+1]):
			ops = append(ops, op{'-', a[i], off + i + 1, 0})
			i++
		default:
			ops = append(ops, op{'+', b[j], 0, off + j + 1})
			j++
		}
	}
	return ops
}

// group cuts the script into hunks with diffContext lines of context.
func group(ops []op) [][]string {
	var hunks [][]string
	i := 0
	for i < len(ops) {
		for i < len(ops) && ops[i].kind == ' ' {
			i++
		}
		if i == len(ops) {
			break
		}
		start := max(0, i-diffContext)
		end := i
		for end < len(ops) {
			if ops[end].kind != ' ' {
				end++
				continue
			}
			// A run of kept lines longer than twice the context ends the hunk.
			run := end
			for run < len(ops) && ops[run].kind == ' ' {
				run++
			}
			if run == len(ops) || run-end > 2*diffContext {
				end = min(end+diffContext, len(ops))
				break
			}
			end = run
		}
		hunks = append(hunks, hunk(ops[start:end]))
		i = end
	}
	return hunks
}

func hunk(ops []op) []string {
	var as, bs, an, bn int
	for _, o := range ops {
		if o.kind != '+' {
			if as == 0 {
				as = o.ai
			}
			an++
		}
		if o.kind != '-' {
			if bs == 0 {
				bs = o.bi
			}
			bn++
		}
	}
	// An empty side starts after the line before it, as diff -u writes it.
	if an == 0 {
		as = prevLine(ops, func(o op) int { return o.ai })
	}
	if bn == 0 {
		bs = prevLine(ops, func(o op) int { return o.bi })
	}
	lines := []string{fmt.Sprintf("@@ -%d,%d +%d,%d @@", as, an, bs, bn)}
	for _, o := range ops {
		lines = append(lines, string(o.kind)+o.text)
	}
	return lines
}

func prevLine(ops []op, n func(op) int) int {
	for _, o := range ops {
		if v := n(o); v > 0 {
			return v - 1
		}
	}
	return 0
}
