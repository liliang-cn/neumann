package decide_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/decide"
)

var bg = context.Background()

// realReply is a reply Ollama 0.35 sent for tev1:4b, as it came.
const realReply = `{"model":"tev1:4b","answers":{
 "risky":{"type":"noul","noul":0.4691877290428777},
 "route":{"type":"choice","choice":"fast","probabilities":{"fast":0.7511,"strong":0.2489},"confidence":0.1905},
 "effort":{"type":"score","score":0.2102,"legend":{"0":"一步","1":"几步","2":"很多步"},"probabilities":{"0":0.8226,"1":0.1445,"2":0.0328},"confidence":0.4973}},
 "usage":{"input_tokens":749,"output_tokens":4}}`

// questions are the three questions realReply answers.
func questions() []decide.Ask {
	return []decide.Ask{
		{Name: "risky", Question: decide.Noul("Is this risky?")},
		{Name: "route", Question: decide.Choice("Which model?", "fast", "strong")},
		{Name: "effort", Question: decide.Score("How much work is it?", "一步", "几步", "很多步")},
	}
}

func one(name string, q decide.Question) []decide.Ask { return []decide.Ask{{Name: name, Question: q}} }

// server records each request and answers with reply and status, or with
// the next of script while it lasts.
type server struct {
	mu     sync.Mutex
	reqs   []*http.Request
	bodies []string
	status int
	reply  string
	delay  time.Duration
	script []scripted
}

type scripted struct {
	status int
	reply  string
}

func (s *server) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, r)
		s.bodies = append(s.bodies, string(b))
		status, reply, delay := s.status, s.reply, s.delay
		if len(s.script) > 0 {
			status, reply, s.script = s.script[0].status, s.script[0].reply, s.script[1:]
		}
		s.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (s *server) last(t *testing.T) (*http.Request, string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reqs) == 0 {
		t.Fatal("no request reached the server")
	}
	return s.reqs[len(s.reqs)-1], s.bodies[len(s.bodies)-1]
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestSystemOneEncodesQuestions(t *testing.T) {
	s := &server{reply: realReply}
	srv := s.start(t)
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL + "/v1/", APIKey: "k-secret", KeepAlive: 5 * time.Minute})
	// Questions and options out of alphabetical order, one option described
	// and one not: both orders must survive and the undescribed option must
	// be null.
	qs := []decide.Ask{
		{Name: "route", Question: decide.Choice("Which model?", "strong", "fast").Describe("strong", "hard requests")},
		{Name: "risky", Question: decide.Noul("Is this risky?").Describe("yes", "it destroys data").Describe("no", "it reads")},
		{Name: "effort", Question: decide.Score("How much work is it?", "一步", "几步", "很多步")},
	}
	if _, err := e.Decide(bg, "delete everything", qs); err != nil {
		t.Fatal(err)
	}
	req, body := s.last(t)
	if req.Method != http.MethodPost || req.URL.Path != "/v1/systemone" {
		t.Fatalf("%s %s", req.Method, req.URL.Path)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer k-secret" {
		t.Fatalf("authorization %q", got)
	}
	if ct := req.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q", ct)
	}
	for _, want := range []string{
		`"criteria":{"strong":"hard requests","fast":null}`,
		`"criteria":{"false":"it reads","true":"it destroys data"}`,
		`"criteria":["一步","几步","很多步"]`,
		`"model":"tev1:4b"`,
		`"state":"delete everything"`,
		`"keep_alive":"5m0s"`,
		`"type":"score"`,
		`"instructions":"How much work is it?"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the body lacks %s:\n%s", want, body)
		}
	}
	// The questions are sent in the caller's order.
	route, risky, effort := strings.Index(body, `"route":{`), strings.Index(body, `"risky":{`), strings.Index(body, `"effort":{`)
	if route < 0 || !(route < risky && risky < effort) {
		t.Errorf("questions out of order (route %d, risky %d, effort %d):\n%s", route, risky, effort, body)
	}

	// No descriptions, no keep-alive, no key: none of them is sent.
	e = decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL})
	if _, err := e.Decide(bg, "x", questions()); err != nil {
		t.Fatal(err)
	}
	req, body = s.last(t)
	var raw struct {
		Questions map[string]map[string]json.RawMessage `json:"questions"`
	}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw.Questions["risky"]["criteria"]; ok {
		t.Errorf("an undescribed noul sent criteria: %s", body)
	}
	if got := string(raw.Questions["route"]["criteria"]); got != `{"fast":null,"strong":null}` {
		t.Errorf("route criteria %s", got)
	}
	if strings.Contains(body, "keep_alive") || req.Header.Get("Authorization") != "" {
		t.Errorf("keep_alive or a key sent unasked: %s %v", body, req.Header)
	}
}

func TestSystemOneDecodesARealReply(t *testing.T) {
	s := &server{reply: realReply}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	got, usage, err := e.DecideUsage(bg, "a request", questions())
	if err != nil {
		t.Fatal(err)
	}
	if usage != (decide.Usage{InputTokens: 749, OutputTokens: 4}) {
		t.Errorf("usage %+v", usage)
	}
	r := got["risky"]
	if r.Type != decide.TypeNoul || r.Yes || r.Label != "no" || !near(r.Confidence, math.Abs(2*0.4691877290428777-1)) ||
		!near(r.P("yes"), 0.4691877290428777) || !near(r.P("no"), 1-0.4691877290428777) {
		t.Errorf("risky %+v", r)
	}
	c := got["route"]
	if c.Type != decide.TypeChoice || c.Label != "fast" || c.Confidence != 0.1905 || c.P("fast") != 0.7511 || c.P("strong") != 0.2489 || c.Yes {
		t.Errorf("route %+v", c)
	}
	f := got["effort"]
	if f.Type != decide.TypeScore || f.Label != "一步" || f.Score != 0.2102 || f.Confidence != 0.4973 ||
		f.P("一步") != 0.8226 || f.P("几步") != 0.1445 || f.P("很多步") != 0.0328 {
		t.Errorf("effort %+v", f)
	}
}

func TestSystemOneNoulYesAtOneHalf(t *testing.T) {
	s := &server{reply: `{"answers":{"q":{"type":"noul","noul":0.5}}}`}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	got, err := e.Decide(bg, "x", one("q", decide.Noul("?")))
	if err != nil {
		t.Fatal(err)
	}
	if a := got["q"]; !a.Yes || a.Label != "yes" || a.Confidence != 0 {
		t.Fatalf("%+v", a)
	}
}

func TestSystemOneRejectsAnswersThatDoNotFit(t *testing.T) {
	for name, reply := range map[string]string{
		"missing":       `{"answers":{"risky":{"type":"noul","noul":0.1}}}`,
		"wrong type":    `{"answers":{"q":{"type":"noul","noul":0.1}}}`,
		"unknown label": `{"answers":{"q":{"type":"choice","choice":"medium","confidence":0.9}}}`,
		"not json":      `<html>`,
	} {
		t.Run(name, func(t *testing.T) {
			s := &server{reply: reply}
			e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
			_, err := e.Decide(bg, "x", one("q", decide.Choice("?", "fast", "strong")))
			if err == nil || !strings.HasPrefix(err.Error(), "decide: ") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestSystemOneScoreLevelOutOfRange(t *testing.T) {
	s := &server{reply: `{"answers":{"q":{"type":"score","score":1,"probabilities":{"0":0.2,"5":0.8}}}}`}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	if _, err := e.Decide(bg, "x", one("q", decide.Score("?", "low", "high"))); err == nil {
		t.Fatal("a probability for a level that does not exist was accepted")
	}
}

func TestSystemOneErrorBodies(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"ollama", 400, `{"error":"question \"a\": criteria must contain 2–26 candidates"}`, `400 Bad Request: question "a": criteria must contain 2–26 candidates`},
		{"relay", 401, `{"error":{"message":"invalid api key","type":"auth"}}`, "401 Unauthorized: invalid api key"},
		{"plain", 502, "upstream went away", "502 Bad Gateway: upstream went away"},
		{"empty", 500, "", "500 Internal Server Error: no message"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := &server{status: c.status, reply: c.body}
			e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
			_, err := e.Decide(bg, "x", questions())
			if err == nil || err.Error() != "decide: systemone:tev1:4b: "+c.want {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestSystemOneRefusesBeforeSending(t *testing.T) {
	s := &server{reply: realReply}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	var many []decide.Ask
	for i := range decide.MaxQuestions + 1 {
		many = append(many, decide.Ask{Name: fmt.Sprint("q", i), Question: decide.Noul("?")})
	}
	cases := map[string]struct {
		state string
		qs    []decide.Ask
	}{
		"empty state":    {" ", questions()},
		"body too large": {strings.Repeat("x", decide.MaxBody-100), questions()},
		"no questions":   {"x", nil},
		"65 questions":   {"x", many},
		"bad question":   {"x", one("q", decide.Choice("?", "only"))},
	}
	for name, c := range cases {
		if _, err := e.Decide(bg, c.state, c.qs); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if len(s.reqs) != 0 {
		t.Fatalf("%d requests were sent", len(s.reqs))
	}
}

func TestSystemOneTimeout(t *testing.T) {
	s := &server{reply: realReply, delay: 5 * time.Second}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL, Timeout: 50 * time.Millisecond})
	start := time.Now()
	_, err := e.Decide(bg, "x", questions())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the request took %s", d)
	}
}

func TestSystemOneDefaultsAndURL(t *testing.T) {
	for in, want := range map[string]string{
		"":                             decide.DefaultURL,
		"  ":                           decide.DefaultURL,
		"http://h:11434":               "http://h:11434",
		"http://h:11434/":              "http://h:11434",
		"http://h:11434/v1":            "http://h:11434",
		"http://h:11434/v1/":           "http://h:11434",
		" https://relay.example/v1// ": "https://relay.example",
		"https://relay.example/api/v1": "https://relay.example/api",
	} {
		if got := decide.NewSystemOne(decide.SystemOneConfig{URL: in}).URL(); got != want {
			t.Errorf("URL %q -> %q, want %q", in, got, want)
		}
	}
	if n := decide.NewSystemOne(decide.SystemOneConfig{}).Name(); n != "systemone:tev1:4b" {
		t.Errorf("name %q", n)
	}
	if n := decide.NewSystemOne(decide.SystemOneConfig{Model: "clef-flash:9b"}).Name(); n != "systemone:clef-flash:9b" {
		t.Errorf("name %q", n)
	}
}

func TestSystemOneReady(t *testing.T) {
	s := &server{reply: `{"object":"list","data":[{"id":"clef-flash:9b"},{"id":"tev1:4b"},{"id":"nimble:latest"}]}`}
	srv := s.start(t)
	if err := decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL, APIKey: "k"}).Ready(bg); err != nil {
		t.Fatal(err)
	}
	req, _ := s.last(t)
	if req.Method != http.MethodGet || req.URL.Path != "/v1/models" || req.Header.Get("Authorization") != "Bearer k" {
		t.Fatalf("%s %s %v", req.Method, req.URL.Path, req.Header)
	}
	if err := decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL, Model: "nimble"}).Ready(bg); err != nil {
		t.Fatalf("an untagged model is :latest: %v", err)
	}
	if err := decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL, Model: "nope:1b"}).Ready(bg); err == nil || !strings.Contains(err.Error(), "does not serve nope:1b") {
		t.Fatalf("err = %v", err)
	}
	bad := &server{status: 401, reply: `{"error":{"message":"invalid api key"}}`}
	if err := decide.NewSystemOne(decide.SystemOneConfig{URL: bad.start(t).URL}).Ready(bg); err == nil || !strings.Contains(err.Error(), "invalid api key") {
		t.Fatalf("err = %v", err)
	}
	srv.Close()
	if err := decide.NewSystemOne(decide.SystemOneConfig{URL: srv.URL}).Ready(bg); err == nil {
		t.Fatal("a closed server is ready")
	}
}

// overContext is how Ollama refuses a state longer than the model's context.
const overContext = `{"error":"prompt 0 has 4000 tokens; expected 1–2050 (input is never truncated)"}`

func TestSystemOneRetriesAStateTooLong(t *testing.T) {
	s := &server{reply: realReply, script: []scripted{{400, overContext}}}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	state := "HEAD " + strings.Repeat("é", 10000) + " TAIL"
	_, usage, err := e.DecideUsage(bg, state, questions())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.bodies) != 2 {
		t.Fatalf("%d requests", len(s.bodies))
	}
	var second struct{ State string }
	if err := json.Unmarshal([]byte(s.bodies[1]), &second); err != nil {
		t.Fatal(err)
	}
	// The questions' share comes off the limit first, so the state is cut
	// further than by the bare ratio of the limit to the count.
	naive := int(float64(len(state)) * 2050 / 4000 * 0.9)
	if len(second.State) > naive || len(second.State) < naive*3/4 {
		t.Errorf("the second state is %d bytes; the bare ratio gives %d", len(second.State), naive)
	}
	if usage.StateCut != len(state)-len(second.State) {
		t.Errorf("StateCut %d, the state went from %d to %d bytes", usage.StateCut, len(state), len(second.State))
	}
	if !strings.HasPrefix(second.State, "HEAD ") || !strings.HasSuffix(second.State, " TAIL") || !strings.Contains(second.State, "bytes cut …]") {
		t.Errorf("the second state keeps neither end, or has no marker: %.40q … %.40q", second.State, second.State[len(second.State)-40:])
	}

	// Refused twice: the second refusal is the error, and there is no third
	// request.
	s = &server{status: 400, reply: overContext}
	e = decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	_, err = e.Decide(bg, state, questions())
	if err == nil || err.Error() != "decide: systemone:tev1:4b: 400 Bad Request: prompt 0 has 4000 tokens; expected 1–2050 (input is never truncated)" {
		t.Fatalf("err = %v", err)
	}
	if len(s.bodies) != 2 {
		t.Fatalf("%d requests", len(s.bodies))
	}

	// Questions that alone take most of the limit: no cut of the state
	// would help, and there is no retry.
	var big []decide.Ask
	for i := range 20 {
		big = append(big, decide.Ask{Name: fmt.Sprint("q", i), Question: decide.Noul(strings.Repeat("Is it so? ", 50))})
	}
	s = &server{status: 400, reply: overContext}
	e = decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	if _, err := e.Decide(bg, state, big); err == nil || len(s.bodies) != 1 {
		t.Fatalf("err = %v after %d requests", err, len(s.bodies))
	}

	// Any other 400 is not retried.
	s = &server{status: 400, reply: `{"error":"questions must contain 1–64 fields"}`}
	e = decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL})
	if _, err := e.Decide(bg, state, questions()); err == nil || len(s.bodies) != 1 {
		t.Fatalf("err = %v after %d requests", err, len(s.bodies))
	}
}

func TestSystemOneWarm(t *testing.T) {
	s := &server{reply: `{"answers":{"ready":{"type":"noul","noul":0.9}}}`, delay: 200 * time.Millisecond}
	e := decide.NewSystemOne(decide.SystemOneConfig{URL: s.start(t).URL, Timeout: 50 * time.Millisecond})
	if err := e.Warm(bg); err != nil {
		t.Fatalf("Warm is cut off by the request timeout: %v", err)
	}
	if _, body := s.last(t); !strings.Contains(body, `"questions":{"ready":{"type":"noul"`) {
		t.Fatalf("body %s", body)
	}
	if _, err := e.Decide(bg, "x", one("ready", decide.Noul("?"))); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Decide keeps its timeout: %v", err)
	}
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if err := e.Warm(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Warm keeps ctx: %v", err)
	}
}
