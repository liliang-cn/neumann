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
