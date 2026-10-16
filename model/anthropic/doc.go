// Package anthropic adapts Anthropic's native Messages API (POST /messages)
// to the machine's provider-neutral messages.
//
// # Getting started
//
// Read the key from the environment; never put it in source:
//
//	a := anthropic.New("https://api.anthropic.com/v1", os.Getenv("ANTHROPIC_API_KEY"), "<model name>")
//	a.MaxTokens = 8192 // the API requires one; 0 sends DefaultMaxTokens
//	m, err := neumann.New(neumann.Config{Model: a})
//
// The adapter streams (model.Streamer), replays thinking blocks and tool-use
// ids byte for byte to the model that produced them, places cache_control
// breakpoints, classifies Anthropic's errors and reads its rate-limit headers.
// ThinkingBudget turns on extended thinking; a request with NoThink goes out
// without it. The adapter does not offer to continue a cut-off answer
// (Caps.Prefill is false).
package anthropic
