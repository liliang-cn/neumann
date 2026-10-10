package openai

import (
	"testing"

	"github.com/liliang-cn/neumann/model"
)

func TestCaps(t *testing.T) {
	a := New("http://x", "k", "m")
	if got := a.Caps(); got != (model.Caps{Tools: true, MaxTools: DefaultMaxTools}) {
		t.Fatalf("default caps %+v", got)
	}
	a.ContextWindow, a.MaxTokens, a.NoTools, a.MaxTools = 32000, 1000, true, -1
	if got := model.CapsOf(a); got != (model.Caps{Window: 32000, MaxOut: 1000}) {
		t.Fatalf("configured caps %+v", got)
	}
}
