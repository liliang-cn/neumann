package task

import "encoding/json"

// EstimateTokens is a rough count of the tokens a request with this system
// prompt, these tools and these messages takes: its JSON encoding's bytes
// divided by four, plus one. Tokenisers differ by provider and none is
// linked in; the estimate errs high for JSON-heavy content and low for
// non-Latin text, which is good enough to reserve room in a rate limiter
// (design §7.2) and to tell whether a task fits a model's window before it
// is moved there (§6.3), never zero for a large context.
//
// A part that carries Opaque bytes is replayed as those bytes, so it is
// counted by them alone: its parsed Args and reasoning Text describe the
// same content and are not sent again. Counting both estimated a tool call
// that wrote a file at nearly twice its cost, which made compaction fire
// early and fail a task near a small window.
func EstimateTokens(system string, tools []ToolSpec, msgs []Message) int64 {
	b, err := json.Marshal(struct {
		S string
		T []ToolSpec
		M []Message
	}{system, tools, sentForm(msgs)})
	if err != nil {
		return 1
	}
	return int64(len(b)/4) + 1
}

// sentForm is msgs as a request carries them: a part with Opaque keeps only
// its Opaque. The input is not changed; it is copied only when a part needs
// trimming.
func sentForm(msgs []Message) []Message {
	var out []Message
	for i, m := range msgs {
		for j, p := range m.Parts {
			if len(p.Opaque) == 0 || (len(p.Args) == 0 && p.Text == "") {
				continue
			}
			if out == nil {
				out = append([]Message(nil), msgs...)
			}
			if &out[i].Parts[0] == &msgs[i].Parts[0] {
				out[i].Parts = append([]Part(nil), m.Parts...)
			}
			out[i].Parts[j].Args, out[i].Parts[j].Text = nil, ""
		}
	}
	if out == nil {
		return msgs
	}
	return out
}
