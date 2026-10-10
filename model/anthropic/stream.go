package anthropic

import (
	"bufio"
	"bytes"
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

type streamEvent struct {
	Type    string `json:"type"`
	Message *struct {
		Usage wireUsage `json:"usage"`
	} `json:"message"`
	Index        int             `json:"index"`
	ContentBlock json.RawMessage `json:"content_block"`
	Delta        json.RawMessage `json:"delta"`
	Usage        *struct {
		InputTokens              *int `json:"input_tokens"`
		CacheCreationInputTokens *int `json:"cache_creation_input_tokens"`
		CacheReadInputTokens     *int `json:"cache_read_input_tokens"`
		OutputTokens             *int `json:"output_tokens"`
	} `json:"usage"`
}

type blockDelta struct {
	Type        string          `json:"type"`
	Text        string          `json:"text"`
	Thinking    string          `json:"thinking"`
	Signature   string          `json:"signature"`
	PartialJSON string          `json:"partial_json"`
	Citation    json.RawMessage `json:"citation"`
	StopReason  string          `json:"stop_reason"`
}

// blockAcc assembles one content block from content_block_start and its
// deltas. Keys keep the order content_block_start sent them in; the
// streamed fields are filled in at the end, so the result is the block the
// non-streaming API returns.
type blockAcc struct {
	keys      []string
	vals      map[string]json.RawMessage
	text      strings.Builder
	thinking  strings.Builder
	signature strings.Builder
	input     strings.Builder
	citations []json.RawMessage
	touched   map[string]bool
}

func newBlockAcc(raw json.RawMessage) (*blockAcc, error) {
	b := &blockAcc{vals: map[string]json.RawMessage{}, touched: map[string]bool{}}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("content block is not an object: %s", raw)
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := tok.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		if _, dup := b.vals[key]; !dup {
			b.keys = append(b.keys, key)
		}
		b.vals[key] = v
	}
	// Seed the streamed fields with whatever the start already carried.
	for _, f := range b.strings() {
		var s string
		if v, ok := b.vals[f.key]; ok && json.Unmarshal(v, &s) == nil {
			f.sb.WriteString(s)
		}
	}
	if v, ok := b.vals["citations"]; ok {
		json.Unmarshal(v, &b.citations)
	}
	return b, nil
}

// strings lists the streamed string fields in a fixed order, so a key the
// start did not carry is always appended in the same place.
func (b *blockAcc) strings() []struct {
	key string
	sb  *strings.Builder
} {
	return []struct {
		key string
		sb  *strings.Builder
	}{{"text", &b.text}, {"thinking", &b.thinking}, {"signature", &b.signature}}
}

func (b *blockAcc) add(d blockDelta) {
	switch d.Type {
	case "text_delta":
		b.text.WriteString(d.Text)
		b.touched["text"] = true
	case "thinking_delta":
		b.thinking.WriteString(d.Thinking)
		b.touched["thinking"] = true
	case "signature_delta":
		b.signature.WriteString(d.Signature)
		b.touched["signature"] = true
	case "input_json_delta":
		b.input.WriteString(d.PartialJSON)
		b.touched["input"] = true
	case "citations_delta":
		if len(d.Citation) > 0 {
			b.citations = append(b.citations, d.Citation)
			b.touched["citations"] = true
		}
	}
}

func (b *blockAcc) set(key string, v json.RawMessage) {
	if _, ok := b.vals[key]; !ok {
		b.keys = append(b.keys, key)
	}
	b.vals[key] = v
}

func (b *blockAcc) object() (json.RawMessage, error) {
	for _, f := range b.strings() {
		if !b.touched[f.key] {
			continue
		}
		v, err := marshal(f.sb.String())
		if err != nil {
			return nil, err
		}
		b.set(f.key, v)
	}
	if b.touched["citations"] {
		v, err := marshal(b.citations)
		if err != nil {
			return nil, err
		}
		b.set("citations", v)
	}
	if b.touched["input"] && strings.TrimSpace(b.input.String()) != "" {
		// A stream cut at max_tokens can leave the input unfinished; it is
		// still an object on the way back, with the text under _raw.
		v, err := input(json.RawMessage(b.input.String()))
		if err != nil {
			return nil, err
		}
		b.set("input", v)
	}
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range b.keys {
		if i > 0 {
			out.WriteByte(',')
		}
		k, err := marshal(key)
		if err != nil {
			return nil, err
		}
		out.Write(k)
		out.WriteByte(':')
		v, err := compact(b.vals[key])
		if err != nil {
			return nil, err
		}
		out.Write(v)
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

// Stream is Call over server-sent events: text and thinking are reported to
// emit as they arrive, and the assembled turn is returned at the end. A
// stream that ends before message_stop is an error, not a short answer: a
// Transient one wrapping model.ErrResponseLost. An error event is classified
// from its error.type like a status error.
func (a *Adapter) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	resp, err := a.post(ctx, req, true)
	if err != nil {
		return model.Response{}, err
	}
	defer resp.Body.Close()

	var (
		blocks = map[int]*blockAcc{}
		stop   string
		usage  wireUsage
		done   bool
		data   strings.Builder
	)
	// handle processes one event's data; it reports true at message_stop.
	handle := func(raw string) (bool, error) {
		var ev streamEvent
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			return false, a.lostError("decode stream event: "+a.snippet([]byte(raw)), err)
		}
		switch ev.Type {
		case "message_start":
			if ev.Message != nil {
				usage = ev.Message.Usage
			}
		case "content_block_start":
			acc, err := newBlockAcc(ev.ContentBlock)
			if err != nil {
				return false, a.lostError("decode content block", err)
			}
			blocks[ev.Index] = acc
		case "content_block_delta":
			acc := blocks[ev.Index]
			if acc == nil {
				return false, a.lostError(fmt.Sprintf("delta for content block %d before its start", ev.Index), nil)
			}
			var d blockDelta
			if err := json.Unmarshal(ev.Delta, &d); err != nil {
				return false, a.lostError("decode content block delta", err)
			}
			acc.add(d)
			switch {
			case d.Type == "text_delta" && d.Text != "":
				emit(model.Delta{Text: d.Text})
			case d.Type == "thinking_delta" && d.Thinking != "":
				emit(model.Delta{Reasoning: d.Thinking})
			}
		case "message_delta":
			var d blockDelta
			if len(ev.Delta) > 0 {
				if err := json.Unmarshal(ev.Delta, &d); err != nil {
					return false, a.lostError("decode message delta", err)
				}
			}
			if d.StopReason != "" {
				stop = d.StopReason
			}
			if u := ev.Usage; u != nil {
				setIf(&usage.InputTokens, u.InputTokens)
				setIf(&usage.CacheCreationInputTokens, u.CacheCreationInputTokens)
				setIf(&usage.CacheReadInputTokens, u.CacheReadInputTokens)
				setIf(&usage.OutputTokens, u.OutputTokens)
			}
		case "message_stop":
			return true, nil
		case "error":
			return false, a.streamError([]byte(raw), resp.Header)
		}
		return false, nil // ping, content_block_stop, and event types added later
	}

	r := bufio.NewReaderSize(resp.Body, 64<<10)
	for !done {
		line, err := r.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return model.Response{}, a.ioError(ctx, "read stream", err, true, resp.Header)
		}
		eof := err != nil
		line = strings.TrimRight(line, "\r\n")
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(v, " "))
		}
		// A blank line ends an event; so does the end of the body.
		if (line == "" || eof) && data.Len() > 0 {
			d, herr := handle(data.String())
			data.Reset()
			if herr != nil {
				return model.Response{}, herr
			}
			done = d
		}
		if eof {
			break
		}
	}
	if !done {
		if err := ctx.Err(); err != nil {
			return model.Response{}, a.ioError(ctx, "read stream", err, true, resp.Header)
		}
		return model.Response{}, a.lostError("stream ended before message_stop", nil)
	}

	idxs := make([]int, 0, len(blocks))
	for i := range blocks {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	objs := make([]json.RawMessage, 0, len(idxs))
	for _, i := range idxs {
		obj, err := blocks[i].object()
		if err != nil {
			return model.Response{}, a.lostError("assemble content block", err)
		}
		objs = append(objs, obj)
	}
	out, err := a.response(objs, stop, usage.usage())
	if err != nil {
		return model.Response{}, err
	}
	out.Rate = httperr.RateInfo(resp.Header, a.now())
	return out, nil
}

func setIf(dst *int, v *int) {
	if v != nil {
		*dst = *v
	}
}
