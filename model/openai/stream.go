package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/internal/httperr"
)

var _ model.Streamer = (*Adapter)(nil)

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content          *string           `json:"content"`
			ReasoningContent *string           `json:"reasoning_content"`
			Reasoning        *string           `json:"reasoning"` // Ollama's spelling
			ToolCalls        []json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *wireUsage      `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// callAcc assembles one tool call from its stream deltas: the arguments
// arrive in pieces; every other field (id, type, provider extras such as
// Gemini's thought signature) is kept as last sent.
type callAcc struct {
	fields map[string]json.RawMessage
	name   string
	args   strings.Builder
}

func (c *callAcc) add(raw json.RawMessage) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	for k, v := range m {
		switch k {
		case "index":
		case "function":
			var fn struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			}
			if err := json.Unmarshal(v, &fn); err != nil {
				return err
			}
			if fn.Name != "" {
				c.name = fn.Name
			}
			c.args.WriteString(fn.Arguments)
		default:
			c.fields[k] = v
		}
	}
	return nil
}

func (c *callAcc) object() (json.RawMessage, error) {
	obj := make(map[string]any, len(c.fields)+1)
	for k, v := range c.fields {
		obj[k] = v
	}
	if _, ok := obj["type"]; !ok {
		obj["type"] = "function"
	}
	obj["function"] = map[string]string{"name": c.name, "arguments": c.args.String()}
	return json.Marshal(obj)
}

// Stream is Call over server-sent events: text and reasoning are reported to
// emit as they arrive, and the assembled turn is returned at the end. A stream
// that ends before a finish reason or [DONE] is an error, not a short answer:
// a Transient one wrapping model.ErrResponseLost.
func (a *Adapter) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	resp, err := a.post(ctx, req, true)
	if err != nil {
		return model.Response{}, err
	}
	defer resp.Body.Close()

	var (
		text      strings.Builder
		reasoning strings.Builder // split off a continued thought (ThinkTags)
		split     *thinkSplit
		calls     = map[int]*callAcc{}
		finish    string
		usage     wireUsage
		done      bool
	)
	if a.thinkPrefill(req) {
		split = a.splitter()
	}
	content := func(piece string) {
		r, t := "", piece
		if split != nil {
			r, t = split.feed(piece)
		}
		if r != "" {
			reasoning.WriteString(r)
			emit(model.Delta{Reasoning: r})
		}
		if t != "" {
			text.WriteString(t)
			emit(model.Delta{Text: t})
		}
	}
	r := bufio.NewReaderSize(resp.Body, 64<<10)
	for !done {
		line, err := r.ReadString('\n')
		if err != nil && !(errors.Is(err, io.EOF) && line != "") {
			if errors.Is(err, io.EOF) {
				break
			}
			return model.Response{}, a.ioError(ctx, "read stream", err, true, resp.Header)
		}
		line = strings.TrimRight(line, "\r\n")
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue // comments, event names, blank separators
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			done = true
			break
		}
		var ch streamChunk
		if err := json.Unmarshal([]byte(data), &ch); err != nil {
			return model.Response{}, a.lostError("decode stream chunk: "+snippet([]byte(data)), err)
		}
		if len(ch.Error) > 0 && string(ch.Error) != "null" {
			return model.Response{}, a.streamError(ch.Error, resp.Header)
		}
		if ch.Usage != nil {
			usage = *ch.Usage
		}
		for _, c := range ch.Choices {
			d := c.Delta
			for _, r := range []*string{d.ReasoningContent, d.Reasoning} {
				if r != nil && *r != "" {
					emit(model.Delta{Reasoning: *r})
				}
			}
			if d.Content != nil && *d.Content != "" {
				content(*d.Content)
			}
			for _, raw := range d.ToolCalls {
				var idx struct {
					Index int `json:"index"`
				}
				if err := json.Unmarshal(raw, &idx); err != nil {
					return model.Response{}, a.lostError("decode tool call delta", err)
				}
				acc := calls[idx.Index]
				if acc == nil {
					acc = &callAcc{fields: map[string]json.RawMessage{}}
					calls[idx.Index] = acc
				}
				if err := acc.add(raw); err != nil {
					return model.Response{}, a.lostError("decode tool call delta", err)
				}
			}
			if c.FinishReason != nil && *c.FinishReason != "" {
				finish = *c.FinishReason
			}
		}
	}
	if !done && finish == "" {
		if err := ctx.Err(); err != nil {
			return model.Response{}, a.ioError(ctx, "read stream", err, true, resp.Header)
		}
		return model.Response{}, a.lostError("stream ended before the answer was complete", nil)
	}

	if split != nil {
		if r, _ := split.flush(); r != "" {
			reasoning.WriteString(r)
			emit(model.Delta{Reasoning: r})
		}
	}
	idxs := make([]int, 0, len(calls))
	for i := range calls {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	objs := make([]json.RawMessage, 0, len(idxs))
	for _, i := range idxs {
		obj, err := calls[i].object()
		if err != nil {
			return model.Response{}, fmt.Errorf("openai: assemble tool call: %w", err)
		}
		objs = append(objs, obj)
	}
	out, err := a.response(text.String(), reasoning.String(), objs, finish, usage.usage())
	if err != nil {
		return model.Response{}, err
	}
	out.Rate = httperr.RateInfo(resp.Header, a.now())
	return out, nil
}
