package decide

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// ask runs engine.Decide under its own timeout, and checks that every
// question got an answer of its own type. It stops waiting when ctx or the
// timeout ends, whether or not Decide does: an engine that ignores its
// context would otherwise hold the caller (for Policy, a vCore) for as long
// as it likes. The price is that such an engine's goroutine outlives the
// call until Decide returns; its answer is then dropped. Leaking a goroutine
// per misbehaving request is the lesser harm: a held vCore stalls every task
// of its group.
func ask(ctx context.Context, engine Engine, timeout time.Duration, state string, asks []Ask) (map[string]Answer, error) {
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type reply struct {
		answers map[string]Answer
		err     error
	}
	done := make(chan reply, 1) // buffered: a late Decide never blocks
	go func() {
		a, err := engine.Decide(actx, state, asks)
		done <- reply{a, err}
	}()
	var r reply
	select {
	case r = <-done:
	case <-actx.Done():
	}
	if r.err == nil && r.answers == nil && actx.Err() != nil {
		r.err = actx.Err()
	}
	if r.err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err() // the caller's own end, not the engine's failure
		}
		if actx.Err() != nil {
			return nil, &timeoutError{engine: engine.Name(), after: timeout}
		}
		return nil, r.err
	}
	for _, a := range asks {
		got, ok := r.answers[a.Name]
		if !ok {
			return nil, fmt.Errorf("decide: %s: %q unanswered", engine.Name(), a.Name)
		}
		if got.Type != a.Question.Type {
			return nil, fmt.Errorf("decide: %s: %q answered as a %s, asked as a %s", engine.Name(), a.Name, got.Type, a.Question.Type)
		}
	}
	return r.answers, nil
}

// timeoutError is an engine that gave no answer in time. It is a
// context.DeadlineExceeded to errors.Is.
type timeoutError struct {
	engine string
	after  time.Duration
}

func (e *timeoutError) Error() string {
	return fmt.Sprintf("decide: %s: no answer within %s", e.engine, e.after)
}

func (e *timeoutError) Unwrap() error { return context.DeadlineExceeded }

// short is err's message without the "decide: " and "<engine>: " prefixes,
// cut to n bytes: for a reason that names the engine itself.
func short(err error, engine string, n int) string {
	msg := strings.TrimPrefix(err.Error(), "decide: ")
	msg = strings.TrimPrefix(msg, engine+": ")
	return clipHead(msg, n)
}

// clipHead cuts s to about n bytes on a rune boundary, keeping its start and
// saying how much went.
func clipHead(s string, n int) string {
	n = max(n, 0)
	if len(s) <= n {
		return s
	}
	cut := runeStart(s, n)
	return fmt.Sprintf("%s… [%d more bytes]", s[:cut], len(s)-cut)
}

// clipMiddle cuts s to about n bytes on rune boundaries, keeping its first
// two thirds and its last third, with a marker where the middle went: the
// start of a text says what it is, the end what it came to.
func clipMiddle(s string, n int) string {
	n = max(n, 0)
	if len(s) <= n {
		return s
	}
	head := runeStart(s, n*2/3)
	tail := len(s) - (n - head)
	for tail < len(s) && !utf8.RuneStart(s[tail]) {
		tail++
	}
	return fmt.Sprintf("%s\n[… %d bytes cut …]\n%s", s[:head], tail-head, s[tail:])
}

// runeStart is the largest rune boundary of s at or before i.
func runeStart(s string, i int) int {
	if i >= len(s) {
		return len(s)
	}
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return i
}
