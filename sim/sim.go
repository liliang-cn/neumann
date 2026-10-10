package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/task"
)

// Model is a scripted adapter. Fn decides each reply from the request, so
// concurrent tasks can share one script and stay deterministic per task.
type Model struct {
	Fn func(req model.Request) (model.Response, error)
	// Name is the Provider; empty is "sim".
	Name string
	// Info is what Caps reports; nil takes tools, any number, with an
	// unknown window.
	Info *model.Caps

	mu    sync.Mutex
	calls int
}

// Provider is Name, or "sim".
func (m *Model) Provider() string {
	if m.Name != "" {
		return m.Name
	}
	return "sim"
}

// Caps is *Info, or a model that takes tools with an unknown window.
func (m *Model) Caps() model.Caps {
	if m.Info != nil {
		return *m.Info
	}
	return model.Caps{Tools: true}
}

// Call counts the call, returns ctx's error if it has ended, and otherwise
// whatever Fn decides.
func (m *Model) Call(ctx context.Context, req model.Request) (model.Response, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.Response{}, err
	}
	return m.Fn(req)
}

// Stream is Call that also reports the reply's text in three pieces, so tests
// exercise the streaming path the machine takes by default.
func (m *Model) Stream(ctx context.Context, req model.Request, emit func(model.Delta)) (model.Response, error) {
	resp, err := m.Call(ctx, req)
	if err != nil {
		return resp, err
	}
	r := []rune(resp.Message.TextContent())
	for i, n := 0, len(r); i < 3 && n > 0; i++ {
		if piece := string(r[i*n/3 : (i+1)*n/3]); piece != "" {
			emit(model.Delta{Text: piece})
		}
	}
	return resp, nil
}

// RateInfo reports the limits a scripted failure carries (see RateLimited),
// or a scripted response's Rate.
func (m *Model) RateInfo(resp *model.Response, err error) *model.RateInfo {
	if resp != nil && resp.Rate != nil {
		return resp.Rate
	}
	var me *model.Error
	if errors.As(err, &me) {
		return me.Rate
	}
	return nil
}

// Calls is how many times Call or Stream was invoked.
func (m *Model) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

var simUsage = task.Usage{PromptTokens: 10, CompletionTokens: 5}

// Reply is a final text answer.
func Reply(text string) model.Response {
	return model.Response{Message: task.Text(task.RoleAssistant, text), FinishReason: "stop", Usage: simUsage}
}

// CallTools is a turn calling each named tool once; call i gets args {"n":i}
// and provider id "p-i".
func CallTools(names ...string) model.Response {
	msg := task.Message{Role: task.RoleAssistant}
	for i, n := range names {
		msg.Parts = append(msg.Parts, task.Part{
			Kind:           task.PartToolCall,
			ProviderCallID: fmt.Sprintf("p-%d", i),
			Name:           n,
			Args:           json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		})
	}
	return model.Response{Message: msg, FinishReason: "tool_calls", Usage: simUsage}
}

// FirstUser is the first user message's text; tests use it as a task's name.
func FirstUser(req model.Request) string {
	for _, m := range req.Messages {
		if m.Role == task.RoleUser {
			return m.TextContent()
		}
	}
	return ""
}

// UserTexts is every user message's text, in order.
func UserTexts(req model.Request) []string {
	var out []string
	for _, m := range req.Messages {
		if m.Role == task.RoleUser {
			out = append(out, m.TextContent())
		}
	}
	return out
}

// ToolRounds counts tool-result messages: the tool rounds completed so far.
func ToolRounds(req model.Request) int {
	n := 0
	for _, m := range req.Messages {
		if m.Role == task.RoleTool {
			n++
		}
	}
	return n
}

// failStatus is the HTTP status a real provider would send for each class.
var failStatus = map[model.ErrClass]int{
	model.RateLimited:    429,
	model.QuotaExhausted: 429,
	model.Overload:       529,
	model.Transient:      502,
	model.Permanent:      400,
}

// Fail is a scripted model failure of the given class, as an adapter would
// report it. A RateLimited failure with retryAfter > 0 also carries a
// RateInfo whose RetryAt is that far from the wall clock.
func Fail(class model.ErrClass, retryAfter time.Duration) error {
	e := &model.Error{
		Class: class, Status: failStatus[class], RetryAfter: retryAfter,
		Provider: "sim", Msg: "scripted " + class.String(),
	}
	if class == model.RateLimited && retryAfter > 0 {
		now := time.Now()
		e.Rate = &model.RateInfo{RetryAt: now.Add(retryAfter), At: now}
	}
	return e
}

// RateLimited is a 429 with a retry-after hint.
func RateLimited(retryAfter time.Duration) error { return Fail(model.RateLimited, retryAfter) }

// QuotaExhausted is the account out of quota or credit.
func QuotaExhausted() error { return Fail(model.QuotaExhausted, 0) }

// Overload is a 529: the provider is overloaded.
func Overload() error { return Fail(model.Overload, 0) }

// Transient is a 502: a failure worth retrying after backoff.
func Transient() error { return Fail(model.Transient, 0) }

// Permanent is a 400: the provider rejected the request.
func Permanent() error { return Fail(model.Permanent, 0) }

// Lost is a call that reached the provider whose response never came back:
// the call counts as made (Calls includes it), the error is Transient and
// wraps model.ErrResponseLost.
func Lost() error {
	return &model.Error{Class: model.Transient, Provider: "sim", Msg: "scripted lost response", Err: model.ErrResponseLost}
}

// Truncated is a turn cut off by the output limit before it produced
// anything: finish reason "length", no text, no tool calls, but tokens spent.
func Truncated() model.Response {
	return model.Response{
		Message:      task.Message{Role: task.RoleAssistant},
		FinishReason: "length",
		Usage:        task.Usage{PromptTokens: 10, CompletionTokens: 50, ReasoningTokens: 50},
	}
}

// ContextTooLong is the provider rejecting a request as longer than the
// model's context window (a 400, context_length_exceeded).
func ContextTooLong() error {
	return &model.Error{Class: model.ContextLength, Status: 400, Provider: "sim", Msg: "scripted context_length_exceeded"}
}

// IsCompaction reports whether req is a compaction request (design §5.3):
// the fixed compaction prompt, no tools.
func IsCompaction(req model.Request) bool {
	return req.System == task.CompactSystem && len(req.Tools) == 0
}

// summaryRounds reads the "rounds=N" a Summarize summary starts with.
func summaryRounds(text string) int {
	var n int
	fmt.Sscanf(text, "rounds=%d", &n)
	return n
}

// Summarize answers a compaction request the way a faithful model would, in
// a form tests can read back: "rounds=N", where N counts the tool rounds the
// summarised span stood for (the rounds of a summary inside it, plus each
// tool message), then how many blocks the transcript had. It stays short
// however much it stands for.
func Summarize(req model.Request) model.Response {
	text := ""
	if len(req.Messages) > 0 {
		text = req.Messages[len(req.Messages)-1].TextContent()
	}
	n, blocks := 0, 0
	for _, block := range strings.Split(text, "\n\n### ") {
		block = strings.TrimPrefix(block, "### ")
		blocks++
		switch {
		case strings.HasPrefix(block, "summary of earlier work\n"):
			n += summaryRounds(strings.TrimPrefix(block, "summary of earlier work\n"))
		case strings.HasPrefix(block, "tool results"):
			n++
		}
	}
	return Reply(fmt.Sprintf("rounds=%d (from %d blocks of transcript)", n, blocks))
}

// Rounds is the tool rounds a task has completed by the time of req: those
// still in its messages (ToolRounds) plus those its compaction summaries
// (Summarize's) stand for. Scripted models that act by round use it, so
// they keep their place across compactions.
func Rounds(req model.Request) int {
	n := ToolRounds(req)
	for _, m := range req.Messages {
		if m.Summary {
			n += summaryRounds(task.SummaryText(m))
		}
	}
	return n
}
