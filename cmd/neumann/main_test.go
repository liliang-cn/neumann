package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const secret = "sk-test-do-not-print-me"

// fakeOpenAI is an OpenAI-compatible endpoint scripted like sim.Model: the
// first turn of a task calls util_echo with the task's input, the next one
// answers with the tool's result. It streams when asked to, as the adapter
// does by default.
type fakeOpenAI struct {
	*httptest.Server
	mu        sync.Mutex
	auth      []string
	maxTokens []int // max_tokens of every request, 0 when absent
	calls     int
}

func newFake(t *testing.T) *fakeOpenAI {
	f := &fakeOpenAI{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "no such path", http.StatusNotFound)
			return
		}
		var body struct {
			Stream    bool `json:"stream"`
			MaxTokens int  `json:"max_tokens"`
			Messages  []struct {
				Role    string  `json:"role"`
				Content *string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.maxTokens = append(f.maxTokens, body.MaxTokens)
		f.calls++
		f.mu.Unlock()
		var input, result string
		for _, m := range body.Messages {
			if m.Content == nil {
				continue
			}
			switch {
			case m.Role == "user" && input == "":
				input = *m.Content
			case m.Role == "tool":
				result = *m.Content
			}
		}
		args, _ := json.Marshal(map[string]string{"text": input})
		callDelta := map[string]any{"tool_calls": []any{map[string]any{
			"index": 0, "id": "call_1", "type": "function",
			"function": map[string]string{"name": "util_echo", "arguments": string(args)},
		}}}
		finish := "tool_calls"
		delta := callDelta
		if result != "" {
			finish, delta = "stop", map[string]any{"content": "echoed: " + result}
		}
		usage := map[string]int{"prompt_tokens": 11, "completion_tokens": 3}
		if !body.Stream {
			msg := map[string]any{"role": "assistant", "content": delta["content"], "tool_calls": delta["tool_calls"]}
			json.NewEncoder(w).Encode(map[string]any{
				"choices": []any{map[string]any{"message": msg, "finish_reason": finish}}, "usage": usage,
			})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range []any{
			map[string]any{"choices": []any{map[string]any{"index": 0, "delta": delta}}},
			map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}}},
			map[string]any{"choices": []any{}, "usage": usage},
		} {
			b, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(f.Close)
	return f
}

var (
	buildOnce sync.Once
	binary    string
	buildErr  error
)

// build compiles the command once per test binary.
func build(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "neumann-e2e")
		if err != nil {
			buildErr = err
			return
		}
		binary = filepath.Join(dir, "neumann")
		out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binary
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binary != "" {
		os.RemoveAll(filepath.Dir(binary))
	}
	os.Exit(code)
}

// proc is the command running as a subprocess.
type proc struct {
	t       *testing.T
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	lines   chan map[string]any
	stderr  *syncBuffer
	exited  chan struct{} // closed when Wait returns; exitErr is then set
	exitErr error
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func launch(t *testing.T, env []string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(build(t), args...)
	cmd.Env = append(cleanEnv(), env...)
	p := &proc{t: t, cmd: cmd, lines: make(chan map[string]any, 1024), stderr: &syncBuffer{}, exited: make(chan struct{})}
	cmd.Stderr = p.stderr
	var err error
	if p.stdin, err = cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stdoutDone := make(chan struct{})
	go func() {
		// Wait only once stdout is read to the end: Wait closes the pipe.
		<-stdoutDone
		p.exitErr = cmd.Wait()
		close(p.exited)
	}()
	go func() {
		defer close(stdoutDone)
		defer close(p.lines)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 1<<24)
		for sc.Scan() {
			var v map[string]any
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				t.Errorf("stdout line is not JSON: %s", sc.Bytes())
				continue
			}
			p.lines <- v
		}
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		p.wait()
		if strings.Contains(p.stderr.String(), secret) {
			t.Errorf("the API key appears on stderr:\n%s", p.stderr.String())
		}
	})
	return p
}

// cleanEnv is the test's environment without any NEUMANN_ or provider
// variables, so a developer's shell cannot change what the test runs.
func cleanEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "NEUMANN_") || strings.HasPrefix(kv, "OPENAI_") || strings.HasPrefix(kv, "ANTHROPIC_") || strings.HasPrefix(kv, "OTEL_") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func (p *proc) wait() error {
	p.t.Helper()
	select {
	case <-p.exited:
		return p.exitErr
	case <-time.After(20 * time.Second):
		p.t.Fatalf("the process did not exit; stderr:\n%s", p.stderr.String())
	}
	return nil
}

func (p *proc) send(line string) {
	p.t.Helper()
	if _, err := io.WriteString(p.stdin, line+"\n"); err != nil {
		p.t.Fatalf("write %s: %v", line, err)
	}
}

func (p *proc) next() map[string]any {
	p.t.Helper()
	select {
	case v, ok := <-p.lines:
		if !ok {
			p.t.Fatalf("stdout ended; stderr:\n%s", p.stderr.String())
		}
		return v
	case <-time.After(15 * time.Second):
		p.t.Fatalf("no output; stderr:\n%s", p.stderr.String())
	}
	return nil
}

// reply skips events up to the next reply, appending them to evs.
func (p *proc) reply(evs *[]map[string]any) map[string]any {
	p.t.Helper()
	for {
		v := p.next()
		if v["type"] == "reply" {
			return v
		}
		if evs != nil {
			*evs = append(*evs, v)
		}
	}
}

// until reads events until one of kind for task, appending them to evs.
func (p *proc) until(evs []map[string]any, task, kind string) []map[string]any {
	p.t.Helper()
	for len(evs) == 0 || evs[len(evs)-1]["task_id"] != task || evs[len(evs)-1]["kind"] != kind {
		v := p.next()
		if v["type"] != "event" {
			p.t.Fatalf("unexpected reply %v", v)
		}
		evs = append(evs, v)
	}
	return evs
}

func env(f *fakeOpenAI, extra ...string) []string {
	return append([]string{
		"NEUMANN_PROVIDER=openai",
		"NEUMANN_BASE_URL=" + f.URL + "/v1",
		"NEUMANN_MODEL=fake-1",
		"NEUMANN_API_KEY=" + secret,
	}, extra...)
}

func TestEndToEnd(t *testing.T) {
	f := newFake(t)
	store := filepath.Join(t.TempDir(), "state.db")
	system := filepath.Join(t.TempDir(), "system.txt")
	os.WriteFile(system, []byte("You echo."), 0o644)
	p := launch(t, env(f, "NEUMANN_STORE="+store), "-echo", "-cores", "2", "-system-file", system)

	p.send(`{"id":"s","kind":"subscribe","from_seq":0}`)
	if r := p.reply(nil); r["ok"] != true || r["id"] != "s" {
		t.Fatalf("subscribe: %v", r)
	}
	var evs []map[string]any
	p.send(`{"id":1,"kind":"submit","task_id":"e2e-1","input":"ping"}`)
	r := p.reply(&evs)
	if r["ok"] != true || r["task_id"] != "e2e-1" || r["id"] != float64(1) {
		t.Fatalf("submit: %v", r)
	}
	evs = p.until(evs, "e2e-1", "done")
	var answer string
	var last float64
	for _, e := range evs {
		if seq := e["seq"].(float64); seq <= last {
			t.Fatalf("events out of order: %v", evs)
		} else {
			last = seq
		}
		if e["kind"] == "answer" {
			answer = fmt.Sprint(e["data"])
		}
	}
	if !strings.Contains(answer, "echoed: ping") {
		t.Fatalf("answer %q; events %v", answer, evs)
	}

	p.send(`not json`)
	if r := p.reply(nil); r["ok"] != false || r["error"].(map[string]any)["code"] != "bad_request" {
		t.Fatalf("malformed line: %v", r)
	}
	p.send(`{"id":"x","kind":"steer","task_id":"e2e-1","input":"more"}`)
	if r := p.reply(nil); r["error"].(map[string]any)["code"] != "finished" || r["id"] != "x" {
		t.Fatalf("steer of a finished task: %v", r)
	}
	p.send(`{"kind":"group_reset","class":"default"}`)
	if r := p.reply(nil); r["ok"] != true {
		t.Fatalf("group_reset: %v", r)
	}
	p.send(`{"kind":"suspend","task_id":"e2e-1"}`)
	if r := p.reply(nil); r["error"].(map[string]any)["code"] != "finished" {
		t.Fatalf("suspend of a finished task: %v", r)
	}
	p.send(`{"kind":"status"}`)
	if r := p.reply(nil); r["status"].(map[string]any)["groups"].([]any)[0].(map[string]any)["target"] != float64(2) {
		t.Fatalf("status: %v", r)
	}

	p.stdin.Close() // EOF: graceful stop, exit 0
	if err := p.wait(); err != nil {
		t.Fatalf("exit after EOF: %v; stderr:\n%s", err, p.stderr.String())
	}
	f.mu.Lock()
	for _, a := range f.auth {
		if a != "Bearer "+secret {
			t.Errorf("Authorization %q", a)
		}
	}
	calls := f.calls
	f.mu.Unlock()
	if calls != 2 {
		t.Fatalf("%d model calls, want 2", calls)
	}

	// The state is durable: a new process replays the same events from the
	// store, without calling the model again.
	q := launch(t, env(f, "NEUMANN_STORE="+store), "-echo")
	q.send(`{"kind":"subscribe"}`)
	q.reply(nil)
	replay := q.until(nil, "e2e-1", "done")
	if len(replay) != len(evs) {
		t.Fatalf("replayed %d events, saw %d live", len(replay), len(evs))
	}
	for i := range evs {
		a, _ := json.Marshal(evs[i])
		b, _ := json.Marshal(replay[i])
		if !bytes.Equal(a, b) {
			t.Fatalf("event %d: live %s, replayed %s", i, a, b)
		}
	}
	q.stdin.Close()
	if err := q.wait(); err != nil {
		t.Fatalf("second exit: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 2 {
		t.Fatalf("the replay called the model")
	}
}

func TestSignalStopsGracefully(t *testing.T) {
	f := newFake(t)
	p := launch(t, env(f), "-echo")
	p.send(`{"id":1,"kind":"status"}`)
	p.reply(nil)
	if !strings.Contains(p.stderr.String(), "nothing is durable") {
		t.Fatalf("no warning about the in-memory store:\n%s", p.stderr.String())
	}
	p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.wait(); err != nil {
		t.Fatalf("exit after SIGTERM: %v; stderr:\n%s", err, p.stderr.String())
	}
}

func TestStartupFailuresExitNonZero(t *testing.T) {
	f := newFake(t)
	for name, c := range map[string]struct {
		env  []string
		args []string
		code int
	}{
		"no model":        {env: []string{"NEUMANN_BASE_URL=" + f.URL + "/v1"}, code: 1},
		"bad provider":    {env: env(f), args: []string{"-provider", "bard"}, code: 1},
		"bad store":       {env: env(f), args: []string{"-store", filepath.Join(t.TempDir(), "no", "such", "dir", "x.db")}, code: 1},
		"bad system file": {env: env(f), args: []string{"-system-file", filepath.Join(t.TempDir(), "missing")}, code: 1},
		"bad flag":        {env: env(f), args: []string{"-no-such-flag"}, code: 2},
		"metrics alone":   {env: env(f), args: []string{"-metrics"}, code: 1},
		"bad listen addr": {env: env(f), args: []string{"-observe-addr", "no-such-host.invalid:1"}, code: 1},
	} {
		t.Run(name, func(t *testing.T) {
			p := launch(t, c.env, c.args...)
			err := p.wait()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != c.code {
				t.Fatalf("exit %v, want code %d; stderr:\n%s", err, c.code, p.stderr.String())
			}
		})
	}
}
