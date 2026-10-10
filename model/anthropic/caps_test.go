package anthropic

import (
	"testing"

	"github.com/liliang-cn/neumann/model"
)

func TestCaps(t *testing.T) {
	a := New("http://x", "k", "m")
	if got := a.Caps(); got != (model.Caps{Window: DefaultContextWindow, MaxOut: DefaultMaxTokens, Tools: true}) {
		t.Fatalf("default caps %+v", got)
	}
	a.ContextWindow, a.NoTools, a.MaxTools = -1, true, 64
	if got := model.CapsOf(a); got != (model.Caps{MaxOut: DefaultMaxTokens, MaxTools: 64}) {
		t.Fatalf("configured caps %+v", got)
	}
}
