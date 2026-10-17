package openai

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// wholeAndStreamed runs req once as a plain call against a server answering whole and
// once as a stream against one sending lines, with ThinkTags set as -local
// sets them, and returns the two results with what the stream reported.
func wholeAndStreamed(t *testing.T, whole string, lines []string, req model.Request) (call, stream model.Response, callErr, streamErr error, think, text string) {
	t.Helper()
	s := newServer(t, 200, whole)
	a := New(s.URL+"/v1", "k", "m")
	a.ThinkTags = [2]string{"<think>", "</think>"}
	call, callErr = a.Call(context.Background(), req)
	ts, _ := sseServer(t, append(lines, `data: [DONE]`)...)
	a.BaseURL = ts.URL + "/v1"
	var th, tx strings.Builder
	stream, streamErr = a.Stream(context.Background(), req, func(d model.Delta) {
		th.WriteString(d.Reasoning)
		tx.WriteString(d.Text)
	})
	return call, stream, callErr, streamErr, th.String(), tx.String()
}

// A call that continued no thought and came back with thinking alone, no
// text and no tool call, finish "stop", is a thought that stopped without an
// answer (design §6.4): returned with the thinking as its one reasoning
// part, for the machine to judge, not as a transient error. The thinking
// comes in the reasoning field (Ollama), in reasoning_content, or in content
// between the think tags with nothing after the close tag; whole or
// streamed. Cut off at the cap, the same turn carries its thinking too, so
// the machine can tell it was thinking (design §13).
func TestAFreshThoughtThatStops(t *testing.T) {
	for _, c := range []struct {
		name, whole string
		lines       []string
		want        string
		finish      string
	}{
		{"reasoning field",
			`{"choices":[{"message":{"content":"","reasoning":"They are: 1. order_id 2. date"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":"","reasoning":"They are: 1. order_id"}}]}`,
				`data: {"choices":[{"delta":{"content":"","reasoning":" 2. date"}}]}`,
				`data: {"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}`},
			"They are: 1. order_id 2. date", "stop"},
		{"reasoning_content",
			`{"choices":[{"message":{"content":null,"reasoning_content":"They are: 1. order_id 2. date"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"reasoning_content":"They are: 1. order_id 2. date"}}]}`,
				`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`},
			"They are: 1. order_id 2. date", "stop"},
		{"think tags",
			`{"choices":[{"message":{"content":"\n<think>\nThey are: 1. order_id 2. date</think>\n\n"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":"\n<thi"}}]}`,
				`data: {"choices":[{"delta":{"content":"nk>\nThey are: 1. order_id"}}]}`,
				`data: {"choices":[{"delta":{"content":" 2. date</thi"}}]}`,
				`data: {"choices":[{"delta":{"content":"nk>\n\n"},"finish_reason":"stop"}]}`},
			"\nThey are: 1. order_id 2. date", "stop"},
		{"white space after the reasoning field",
			`{"choices":[{"message":{"content":"\n\n","reasoning":"They are: 1. order_id 2. date"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"reasoning":"They are: 1. order_id 2. date"}}]}`,
				`data: {"choices":[{"delta":{"content":"\n\n"},"finish_reason":"stop"}]}`},
			"They are: 1. order_id 2. date", "stop"},
		{"white space after the think tags",
			`{"choices":[{"message":{"content":"<think>They are: 1. order_id 2. date</think> \n"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":"<think>They are: 1. order_id 2. date</think> \n"},"finish_reason":"stop"}]}`},
			"They are: 1. order_id 2. date", "stop"},
		{"cut off while thinking",
			`{"choices":[{"message":{"content":"","reasoning":"They are: 1. order_id 2. date"},"finish_reason":"length"}]}`,
			[]string{`data: {"choices":[{"delta":{"reasoning":"They are: 1. order_id 2. date"}}]}`,
				`data: {"choices":[{"delta":{},"finish_reason":"length"}]}`},
			"They are: 1. order_id 2. date", "length"},
	} {
		t.Run(c.name, func(t *testing.T) {
			call, stream, callErr, streamErr, think, text := wholeAndStreamed(t, c.whole, c.lines, hi)
			for _, r := range []struct {
				how  string
				resp model.Response
				err  error
			}{{"call", call, callErr}, {"stream", stream, streamErr}} {
				p := r.resp.Message.Parts
				if r.err != nil || r.resp.FinishReason != c.finish || len(p) != 1 || p[0].Kind != task.PartReasoning || p[0].Text != c.want || p[0].Provider != "openai:m" {
					t.Fatalf("%s: %+v, %v", r.how, r.resp, r.err)
				}
			}
			if think != c.want || strings.TrimSpace(text) != "" {
				t.Fatalf("streamed reasoning %q, text %q", think, text)
			}
		})
	}
}

// The thinking of a call that continued no thought is not part of an answer
// that has text or tool calls: it is streamed, and left out of the turn, as
// before. Content that opens with the think tag is split at the close tag
// (the thinking streamed as reasoning, the text after it the answer);
// content that only looks like it at first is text, as it came.
func TestAFreshAnswerLeavesItsThinkingOut(t *testing.T) {
	for _, c := range []struct {
		name, whole string
		lines       []string
		think, text string
	}{
		{"reasoning field",
			`{"choices":[{"message":{"content":"Seven columns.","reasoning":"Count them."},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"reasoning":"Count them."}}]}`,
				`data: {"choices":[{"delta":{"content":"Seven columns."},"finish_reason":"stop"}]}`},
			"Count them.", "Seven columns."},
		{"think tags",
			`{"choices":[{"message":{"content":"<think>Count them.</think>\n\nSeven columns."},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":"<think>Count"}}]}`,
				`data: {"choices":[{"delta":{"content":" them.</think>\n\nSeven columns."},"finish_reason":"stop"}]}`},
			"Count them.", "Seven columns."},
		{"not a tag after all",
			`{"choices":[{"message":{"content":"  <th> is a table header."},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":"  <th"}}]}`,
				`data: {"choices":[{"delta":{"content":"> is a table header."},"finish_reason":"stop"}]}`},
			"", "  <th> is a table header."},
		{"ends inside what could be the tag",
			`{"choices":[{"message":{"content":" <thi"},"finish_reason":"stop"}]}`,
			[]string{`data: {"choices":[{"delta":{"content":" <thi"},"finish_reason":"stop"}]}`},
			"", " <thi"},
	} {
		t.Run(c.name, func(t *testing.T) {
			call, stream, callErr, streamErr, think, text := wholeAndStreamed(t, c.whole, c.lines, hi)
			for _, r := range []struct {
				how  string
				resp model.Response
				err  error
			}{{"call", call, callErr}, {"stream", stream, streamErr}} {
				p := r.resp.Message.Parts
				if r.err != nil || len(p) != 1 || p[0].Kind != task.PartText || p[0].Text != c.text {
					t.Fatalf("%s: %+v, %v", r.how, p, r.err)
				}
			}
			if think != c.think || text != c.text {
				t.Fatalf("streamed reasoning %q, text %q", think, text)
			}
		})
	}
}

// A turn with nothing at all, no thinking either, is still a Transient
// error, whole or streamed (design §11); white space alone is nothing. One that reports reasoning tokens
// and no reasoning text (OpenAI's reasoning models) did think: it is
// returned, empty, with its usage, for the machine to judge.
func TestAnEmptyTurn(t *testing.T) {
	_, _, callErr, streamErr, _, _ := wholeAndStreamed(t,
		`{"choices":[{"message":{"content":""},"finish_reason":"stop"}]}`,
		[]string{`data: {"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}`}, hi)
	_, _, wsCallErr, wsStreamErr, _, _ := wholeAndStreamed(t,
		`{"choices":[{"message":{"content":" \n\n"},"finish_reason":"stop"}]}`,
		[]string{`data: {"choices":[{"delta":{"content":" \n"}}]}`, `data: {"choices":[{"delta":{"content":"\n"},"finish_reason":"stop"}]}`}, hi)
	for _, err := range []error{callErr, streamErr, wsCallErr, wsStreamErr} {
		var me *model.Error
		if !errors.As(err, &me) || me.Class != model.Transient || !strings.Contains(me.Msg, "neither text nor tool calls") || !errors.Is(err, model.ErrEmptyResponse) {
			t.Fatalf("an empty turn: %v", err)
		}
	}
	call, stream, callErr, streamErr, _, _ := wholeAndStreamed(t,
		`{"choices":[{"message":{"content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":50}}}`,
		[]string{`data: {"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":50}}}`}, hi)
	for _, r := range []struct {
		resp model.Response
		err  error
	}{{call, callErr}, {stream, streamErr}} {
		if r.err != nil || len(r.resp.Message.Parts) != 0 || r.resp.Usage.ReasoningTokens != 50 || r.resp.FinishReason != "stop" {
			t.Fatalf("reasoning tokens alone: %+v, %v", r.resp, r.err)
		}
	}
}
