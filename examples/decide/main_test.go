package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestDecide runs the example offline, end to end.
func TestDecide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out bytes.Buffer
	res, err := run(ctx, &out, scripted())
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	if res.Routes[sum] != "fast" || res.Routes[tidy] != "strong" {
		t.Errorf("routes %v", res.Routes)
	}
	if len(res.Held) != 1 || !strings.Contains(res.Held[0], "needs_approval yes, p=0.95") {
		t.Errorf("held %q", res.Held)
	}
	if n := res.Deleted.Load(); n != 1 {
		t.Errorf("notes_delete ran %d times", n)
	}
	if !strings.Contains(res.Feedback, "do the work now") {
		t.Errorf("feedback %q", res.Feedback)
	}
	if len(res.Answers) != 3 || res.Answers[2] != "One note is left: groceries for Saturday." {
		t.Errorf("answers %q", res.Answers)
	}
	if t.Failed() {
		t.Log(out.String())
	}
}
