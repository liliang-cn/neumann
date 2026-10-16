// Package openai adapts OpenAI-compatible /chat/completions endpoints
// (OpenAI, DeepSeek, Ollama, and gateways that front other models) to the
// machine's provider-neutral messages.
//
// # Getting started
//
// Read the key from the environment; never put it in source:
//
//	a := openai.New(os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL"))
//	a.MaxTokens = 4096 // optional; 0 lets the provider decide
//	m, err := neumann.New(neumann.Config{Model: a})
//
// The base URL goes up to and including the version, for example
// https://api.openai.com/v1. The adapter streams (model.Streamer), reports
// its capabilities (set ContextWindow when you know it; a gateway's is
// unknown by default), classifies the provider's errors and reads its
// rate-limit headers. It speaks HTTP/2 by default; [Adapter.WithProtocol]
// selects HTTP/3 with a fallback to HTTP/2.
//
// # Local backends
//
// Ollama, llama.cpp and LM Studio serve few requests at once and can be slow
// to start answering a long prompt. Tell the machine what the backend can do
// and give the timeouts room:
//
//	a := openai.New("http://127.0.0.1:11434/v1", "ollama", "qwen3.5:9b-mlx")
//	a.Parallel = 1         // Ollama's MLX runner serves one request at a time
//	a.Prefill = true       // a trailing assistant message is continued
//	a.ThinkOff = "none"    // reasoning_effort that turns thinking off
//	a.ThinkTags = [2]string{"<think>", "</think>"} // a thought cut off is continued too
//	a.MaxTokens = 8192     // a model looping in its thinking stops here
//	a.Timeout = 0          // no limit on the whole call...
//	a.FirstByteTimeout = 10 * time.Minute // ...but on reading the prompt
//	a.IdleTimeout = 2 * time.Minute       // and on a stalled stream
//
// cmd/neumann's -local flag sets exactly these.
package openai
