// Package model is the seam between the machine and a model provider. Every
// call through an Adapter is one instruction on a vCore.
package model

import (
	"context"

	"github.com/liliang-cn/neumann/task"
)

type Request struct {
	System   string
	Tools    []task.ToolSpec
	Messages []task.Message
}

// Response carries the assistant turn in neutral form. Tool calls come back
// with ProviderCallID set and CallID empty; the machine assigns CallID.
type Response struct {
	Message      task.Message
	FinishReason string
	Usage        task.Usage
}

type Adapter interface {
	Provider() string
	Call(ctx context.Context, req Request) (Response, error)
}

// Warmer is an adapter that can open its connection ahead of the first call.
// The machine warms it in the background at Start.
type Warmer interface {
	Warm(ctx context.Context) error
}

// Delta is a piece of an answer while it is being generated. It is a preview
// for people watching: it is never committed, and the committed answer is the
// record of what the model said.
type Delta struct {
	Text      string `json:"text,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}

// Streamer is an adapter that can report a call's output as it arrives. The
// machine uses it whenever an adapter implements it; emit is called from the
// calling goroutine, in order, and must not block.
type Streamer interface {
	Stream(ctx context.Context, req Request, emit func(Delta)) (Response, error)
}
