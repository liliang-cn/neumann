package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
)

// TestMessaging runs the example end to end, so it cannot rot unnoticed.
func TestMessaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	res, err := run(ctx, &out)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	if !strings.Contains(res.Answers["planner"], "completed in 1889") {
		t.Errorf("planner answered %q", res.Answers["planner"])
	}
	if got := strings.Join(res.Delivered, ", "); got != "expert task:planner, planner task:expert" {
		t.Errorf("steer_delivered events: %s", got)
	}
	if got := res.Saw["expert"]; got != neumann.DefaultMessageFormat("task:planner", "When was the Eiffel Tower completed?") {
		t.Errorf("expert's model saw %q", got)
	}
	if got := res.Saw["planner"]; got != neumann.DefaultMessageFormat("task:expert", "The Eiffel Tower was completed in 1889.") {
		t.Errorf("planner's model saw %q", got)
	}
	for _, want := range []string{"tasks_list", "tasks_send", `steer_delivered  {"from":"task:planner"`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}
