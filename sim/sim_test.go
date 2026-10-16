package sim

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

func TestHelpers(t *testing.T) {
	req := model.Request{Messages: []task.Message{
		task.Text(task.RoleUser, "first"),
		CallTools("a", "b").Message,
		{Role: task.RoleTool, Parts: []task.Part{{Kind: task.PartToolResult, CallID: "c-1"}}},
		task.Text(task.RoleUser, "second"),
	}}
	if FirstUser(req) != "first" {
		t.Fatalf("FirstUser = %q", FirstUser(req))
	}
	if got := UserTexts(req); len(got) != 2 || got[1] != "second" {
		t.Fatalf("UserTexts = %v", got)
	}
	if ToolRounds(req) != 1 {
		t.Fatalf("ToolRounds = %d", ToolRounds(req))
	}
	calls := CallTools("a", "b").Message.ToolCalls()
	if len(calls) != 2 || calls[1].Name != "b" || string(calls[1].Args) != `{"n":1}` || calls[1].ProviderCallID != "p-1" {
		t.Fatalf("CallTools = %+v", calls)
	}
}

func TestModelCountsCalls(t *testing.T) {
	m := &Model{Fn: func(model.Request) (model.Response, error) { return Reply("ok"), nil }}
	for range 3 {
		if _, err := m.Call(context.Background(), model.Request{}); err != nil {
			t.Fatal(err)
		}
	}
	if m.Calls() != 3 {
		t.Fatalf("Calls = %d", m.Calls())
	}
}

func TestFailInjectsEachClass(t *testing.T) {
	cases := []struct {
		err    error
		class  model.ErrClass
		status int
	}{
		{RateLimited(3 * time.Second), model.RateLimited, 429},
		{QuotaExhausted(), model.QuotaExhausted, 429},
		{Overload(), model.Overload, 529},
		{Transient(), model.Transient, 502},
		{Permanent(), model.Permanent, 400},
		{Fail(model.Overload, time.Second), model.Overload, 529},
	}
	for _, c := range cases {
		m := &Model{Fn: func(model.Request) (model.Response, error) { return model.Response{}, c.err }}
		for how, call := range map[string]func() error{
			"Call": func() error { _, err := m.Call(context.Background(), model.Request{}); return err },
			"Stream": func() error {
				_, err := m.Stream(context.Background(), model.Request{}, func(model.Delta) {})
				return err
			},
		} {
			err := call()
			if got := model.Classify(err); got != c.class {
				t.Errorf("%s %s: Classify = %s", how, c.class, got)
			}
			var me *model.Error
			if !errors.As(err, &me) || me.Status != c.status || me.Provider != "sim" {
				t.Errorf("%s %s: %+v", how, c.class, me)
			}
		}
	}
	var me *model.Error
	errors.As(RateLimited(3*time.Second), &me)
	if me.RetryAfter != 3*time.Second {
		t.Fatalf("RetryAfter %v", me.RetryAfter)
	}
	m := &Model{}
	if r := m.RateInfo(nil, RateLimited(3*time.Second)); r == nil || r.RetryAt.IsZero() {
		t.Fatalf("a scripted rate limit must report when to retry: %+v", r)
	}
}

func TestLostAndTruncated(t *testing.T) {
	m := &Model{Fn: func(model.Request) (model.Response, error) { return model.Response{}, Lost() }}
	_, err := m.Call(context.Background(), model.Request{})
	if model.Classify(err) != model.Transient || !errors.Is(err, model.ErrResponseLost) {
		t.Fatalf("Lost: %v", err)
	}
	if m.Calls() != 1 {
		t.Fatal("a lost response is still a call that happened")
	}

	tr := Truncated()
	if tr.FinishReason != "length" || tr.Message.TextContent() != "" || len(tr.Message.ToolCalls()) != 0 || tr.Usage.CompletionTokens == 0 {
		t.Fatalf("Truncated = %+v", tr)
	}
}

func TestCanceledCallIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := &Model{Fn: func(model.Request) (model.Response, error) { return Reply("x"), nil }}
	if _, err := m.Call(ctx, model.Request{}); model.Classify(err) != model.Canceled {
		t.Fatalf("%v", err)
	}
}

func TestClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewClock(start)
	if !c.Now().Equal(start) {
		t.Fatal(c.Now())
	}
	c.Advance(90 * time.Second)
	if !c.Now().Equal(start.Add(90 * time.Second)) {
		t.Fatal(c.Now())
	}
	c.Set(start)
	if !c.Now().Equal(start) {
		t.Fatal(c.Now())
	}
}

// A paced stream sends its first piece at once and each next one Every
// later on the clock; a ctx that ends between two pieces ends the call with
// only the pieces sent so far. A prefilled request is answered with the
// rest.
func TestPacedStreamAndPrefill(t *testing.T) {
	clk := NewClock(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	m := &Model{Clock: clk, Every: time.Second, Pieces: 4, Fn: func(req model.Request) (model.Response, error) {
		return Continue(req, "abcdefgh"), nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan string, 8)
	done := make(chan error, 1)
	go func() {
		_, err := m.Stream(ctx, model.Request{}, func(d model.Delta) { got <- d.Text })
		done <- err
	}()
	if p := <-got; p != "ab" {
		t.Fatalf("first piece %q", p)
	}
	for m.Clock.Timers() == 0 {
		time.Sleep(time.Millisecond)
	}
	clk.Advance(time.Second)
	if p := <-got; p != "cd" {
		t.Fatalf("second piece %q", p)
	}
	for m.Clock.Timers() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != context.Canceled || m.Emitted() != 2 {
		t.Fatalf("cut off: %v after %d pieces", err, m.Emitted())
	}
	req := model.Request{Messages: []task.Message{task.Text(task.RoleUser, "go"), task.Text(task.RoleAssistant, "abcd")}}
	if Prefill(req) != "abcd" {
		t.Fatalf("prefill %q", Prefill(req))
	}
	resp, err := m.Call(context.Background(), req)
	if err != nil || resp.Message.TextContent() != "efgh" || len(m.Requests()) != 2 {
		t.Fatalf("continued %q, %v, %d requests", resp.Message.TextContent(), err, len(m.Requests()))
	}
}
