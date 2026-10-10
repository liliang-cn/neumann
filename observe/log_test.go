package observe

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/proto"
)

func lines(b *bytes.Buffer) []string {
	return strings.Split(strings.TrimSpace(b.String()), "\n")
}

func find(t *testing.T, ls []string, parts ...string) string {
	t.Helper()
	for _, l := range ls {
		ok := true
		for _, p := range parts {
			if !strings.Contains(l, p) {
				ok = false
				break
			}
		}
		if ok {
			return l
		}
	}
	t.Fatalf("no line with %q in:\n%s", parts, strings.Join(ls, "\n"))
	return ""
}

// One greppable key=value line per notable event, at the level its kind
// deserves; step-level detail only at debug; no task content.
func TestLogLines(t *testing.T) {
	var s script
	s.add(0, proto.EvAccepted, "a", map[string]any{"class": "big", "priority": 1})
	s.add(time.Second, proto.EvStep, "a", map[string]any{"pc": "model", "core": 3})
	s.add(time.Second, proto.EvModelRetry, "a", map[string]any{"class": "overload", "attempt": 1, "delay_ms": 1000, "wait": "backoff", "error": "529"})
	s.add(2*time.Second, proto.EvThrottled, "", map[string]any{"task": "a", "class": "big", "waited_ms": 250})
	s.add(2*time.Second, proto.EvGroupDown, "", map[string]any{"class": "big", "cause": "transient"})
	s.add(3*time.Second, proto.EvToolCall, "a", map[string]any{"call_id": "c-1", "tool": "pay_send", "args": `{"secret":"do-not-log"}`})
	s.add(4*time.Second, proto.EvReconcileNeeded, "a", map[string]any{"call_id": "c-1", "tool": "pay_send", "args": `{"secret":"do-not-log"}`})
	s.add(5*time.Second, proto.EvAnswer, "a", map[string]any{"text": "the answer is private"})
	s.add(5*time.Second, proto.EvFailed, "a", map[string]any{"error": "boom", "stop_reason": "model_error"})

	var buf bytes.Buffer
	l := NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	feed(l, s.evs)
	ls := lines(&buf)
	find(t, ls, "level=INFO", "msg=accepted", "seq=1", "task=a", "class=big", "priority=1", "at=2026-10-10T12:00:00Z")
	find(t, ls, "level=WARN", "msg=model_retry", "task=a", "class=big", "error_class=overload", "delay_ms=1000")
	find(t, ls, "level=WARN", "msg=throttled", "task=a", "class=big", "waited_ms=250")
	find(t, ls, "level=WARN", "msg=group_down", "class=big", "cause=transient")
	find(t, ls, "level=ERROR", "msg=reconcile_needed", "task=a", "call=c-1", "tool=pay_send")
	find(t, ls, "level=ERROR", "msg=failed", "task=a", "stop_reason=model_error", `error=boom`)
	for _, l := range ls {
		if strings.Contains(l, "msg=step") || strings.Contains(l, "msg=tool_call") {
			t.Errorf("step-level line at info: %s", l)
		}
	}
	if strings.Contains(buf.String(), "do-not-log") || strings.Contains(buf.String(), "private") {
		t.Errorf("task content in the log:\n%s", buf.String())
	}

	buf.Reset()
	l = NewLog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	feed(l, s.evs)
	find(t, lines(&buf), "level=DEBUG", "msg=step", "task=a", "pc=model", "core=3")
	find(t, lines(&buf), "level=DEBUG", "msg=tool_call", "task=a", "call=c-1", "tool=pay_send")
}

// The periodic resource line: the machine's load and the process's.
func TestLogResources(t *testing.T) {
	r := newRig(t)
	r.run(t, "a")
	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		LogResources(ctx, slog.New(slog.NewTextHandler(&buf, nil)), r.m, 10*time.Millisecond)
	}()
	waitFor(t, func() bool { return strings.Count(buf.String(), "msg=resources") >= 2 })
	cancel()
	<-done
	find(t, strings.Split(buf.String(), "\n"), "level=INFO", "msg=resources", "unfinished=0", "cores=", "ready=0", "goroutines=", "heap_bytes=", "groups_down=0")
}
