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
