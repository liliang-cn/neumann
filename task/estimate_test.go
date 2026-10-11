package task

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// A part replayed from its Opaque bytes is sent as those bytes alone: its
// parsed Args (or reasoning Text) must not be counted a second time. The
// soak's first run hit this: an 8 KB file write was estimated at over twice
// its cost, so compaction fired early and a small window failed the task.
func TestEstimateCountsAReplayedPartOnce(t *testing.T) {
	var src strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&src, "\treturn fmt.Errorf(\"line %d: %%q\", s) // \"quoted\"\n", i)
	}
	args, _ := json.Marshal(map[string]string{"path": "store/store.go", "content": src.String()})
	opaque, _ := json.Marshal(map[string]any{"id": "p-1", "type": "function",
		"function": map[string]any{"name": "ws_write", "arguments": string(args)}})
	withOpaque := []Message{{Role: RoleAssistant, Parts: []Part{{Kind: PartToolCall, CallID: "c-1", Name: "ws_write", Args: args, Provider: "openai:m", Opaque: opaque}}}}
	opaqueOnly := []Message{{Role: RoleAssistant, Parts: []Part{{Kind: PartToolCall, CallID: "c-1", Name: "ws_write", Provider: "openai:m", Opaque: opaque}}}}
	got, want := EstimateTokens("", nil, withOpaque), EstimateTokens("", nil, opaqueOnly)
	if got != want {
		t.Fatalf("a tool call with Opaque is estimated at %d tokens, want %d (its Opaque alone)", got, want)
	}
	if sent := int64(len(opaque) / 4); got > sent*13/10 {
		t.Fatalf("estimate %d is far above the %d bytes/4 actually sent", got, sent)
	}
	// Without Opaque the Args are what is sent, and they still count.
	plain := []Message{{Role: RoleAssistant, Parts: []Part{{Kind: PartToolCall, CallID: "c-1", Name: "ws_write", Args: args}}}}
	if EstimateTokens("", nil, plain) < int64(len(args)/4) {
		t.Fatal("a tool call without Opaque must count its Args")
	}
	// The caller's messages are not changed.
	if len(withOpaque[0].Parts[0].Args) == 0 {
		t.Fatal("EstimateTokens changed its input")
	}
}
