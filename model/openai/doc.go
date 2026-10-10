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
package openai
