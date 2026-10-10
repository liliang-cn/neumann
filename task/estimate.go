package task

import "encoding/json"

// EstimateTokens is a rough count of the tokens a request with this system
// prompt, these tools and these messages takes: its JSON encoding's bytes
// divided by four, plus one. Tokenisers differ by provider and none is
// linked in; the estimate errs high for JSON-heavy content and low for
// non-Latin text, which is good enough to reserve room in a rate limiter
// (design §7.2) and to tell whether a task fits a model's window before it
// is moved there (§6.3), never zero for a large context.
func EstimateTokens(system string, tools []ToolSpec, msgs []Message) int64 {
	b, err := json.Marshal(struct {
		S string
		T []ToolSpec
		M []Message
	}{system, tools, msgs})
	if err != nil {
		return 1
	}
	return int64(len(b)/4) + 1
}
