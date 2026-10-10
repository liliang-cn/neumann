package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestHello runs the example end to end, so it cannot rot unnoticed.
func TestHello(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	start := time.Now()
	res, err := run(ctx, &out)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	if !strings.Contains(res.Answer, "42") {
		t.Errorf("answer = %q, want it to mention 42", res.Answer)
	}
	// The SideEffect tool ran exactly once, with the results of both reads.
	if len(res.Notes) != 1 || res.Notes[0] != "19 + 23 = 42; 6 × 7 = 42" {
		t.Errorf("notes = %q, want exactly one note with both results", res.Notes)
	}
	for _, want := range []string{"accepted", "tool_call", "tool_result", "answer", "done", "call log:", "side_effect done"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("run took %s; the example is meant to finish in well under a second", d)
	}
}
