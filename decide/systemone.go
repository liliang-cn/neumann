package decide

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SystemOne defaults: tev1:4b on the local Ollama.
const (
	DefaultURL     = "http://127.0.0.1:11434"
	DefaultModel   = "tev1:4b"
	DefaultTimeout = 5 * time.Second
)

// SystemOneConfig configures a SystemOne client. The zero value asks tev1:4b
// on the local Ollama.
type SystemOneConfig struct {
	// URL is the Ollama server, or a relay that forwards /v1/systemone
	// (default DefaultURL). A trailing "/v1" is dropped, so the base URL an
	// OpenAI-compatible client uses works as it is.
	URL string
	// Model is the decision model (default DefaultModel).
	Model string
	// APIKey, if set, is sent as a bearer token, for a relay that wants one;
	// a local Ollama ignores it.
	APIKey string
	// Timeout bounds each request (default DefaultTimeout; negative: none
	// beyond the caller's context).
	Timeout time.Duration
	// Client sends the requests (default: a new http.Client). Its own
	// Timeout, if any, applies as well.
	Client *http.Client
	// KeepAlive asks Ollama to keep the model loaded this long after each
	// request; negative keeps it loaded. Zero leaves Ollama's default (five
	// minutes). A model that has been unloaded takes seconds to load again,
	// longer than a tool policy waits.
	KeepAlive time.Duration
}

// SystemOne is an Engine on Ollama's System One API (POST /v1/systemone),
// which serves decision models such as tev1:4b from Ollama 0.35 on. Anything
// that forwards that path, a relay for one, works the same way. It is safe
// for concurrent use.
type SystemOne struct {
	url, model, key string
	timeout         time.Duration
	keepAlive       time.Duration
	client          *http.Client
}

// NewSystemOne builds a client; it does not contact the server (Ready does).
func NewSystemOne(cfg SystemOneConfig) *SystemOne {
	s := &SystemOne{
		url:       normalizeURL(cfg.URL),
		model:     strings.TrimSpace(cfg.Model),
		key:       strings.TrimSpace(cfg.APIKey),
		timeout:   cfg.Timeout,
		keepAlive: cfg.KeepAlive,
		client:    cfg.Client,
	}
	if s.model == "" {
		s.model = DefaultModel
	}
	if s.timeout == 0 {
		s.timeout = DefaultTimeout
	}
	if s.client == nil {
		s.client = &http.Client{}
	}
	return s
}

// normalizeURL trims spaces, trailing slashes and one trailing "/v1".
func normalizeURL(u string) string {
	u = strings.TrimRight(strings.TrimSpace(u), "/")
	u = strings.TrimRight(strings.TrimSuffix(u, "/v1"), "/")
	if u == "" {
		return DefaultURL
	}
	return u
}

// Name is "systemone:" and the model, e.g. "systemone:tev1:4b".
func (s *SystemOne) Name() string { return "systemone:" + s.model }

// URL is the server's base URL, normalised.
func (s *SystemOne) URL() string { return s.url }

// Ready reports whether the server is up and lists the model (GET
// /v1/models). A server without the model is not ready: every Decide would
// fail. Use it as a start-up probe; Decide never calls it.
func (s *SystemOne) Ready(ctx context.Context) error {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url+"/v1/models", nil)
	if err != nil {
		return fmt.Errorf("decide: %s: %w", s.Name(), err)
	}
	s.authorize(req)
	res, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("decide: %s: %w", s.Name(), err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("decide: %s: list models: %s: %s", s.Name(), res.Status, errorText(res.Body))
	}
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&list); err != nil {
		return fmt.Errorf("decide: %s: list models: %w", s.Name(), err)
	}
	for _, m := range list.Data {
		if m.ID == s.model || m.ID == s.model+":latest" {
			return nil
		}
	}
	return fmt.Errorf("decide: %s does not serve %s", s.url, s.model)
}

// Usage is what one request cost, as the server counts it.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Decide asks every question about state in one request.
func (s *SystemOne) Decide(ctx context.Context, state string, qs map[string]Question) (map[string]Answer, error) {
	a, _, err := s.DecideUsage(ctx, state, qs)
	return a, err
}

// DecideUsage is Decide that also returns the request's usage.
func (s *SystemOne) DecideUsage(ctx context.Context, state string, qs map[string]Question) (map[string]Answer, Usage, error) {
	if err := Validate(qs); err != nil {
		return nil, Usage{}, err
	}
	if strings.TrimSpace(state) == "" {
		return nil, Usage{}, fmt.Errorf("decide: empty state")
	}
	if len(state) > MaxState {
		return nil, Usage{}, fmt.Errorf("decide: the state is %d bytes, over the %d System One takes", len(state), MaxState)
	}
	body, err := s.encode(state, qs)
	if err != nil {
		return nil, Usage{}, err
	}
	ctx, cancel := s.bound(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/v1/systemone", bytes.NewReader(body))
	if err != nil {
		return nil, Usage{}, fmt.Errorf("decide: %s: %w", s.Name(), err)
	}
	req.Header.Set("Content-Type", "application/json")
	s.authorize(req)
	res, err := s.client.Do(req)
	if err != nil {
		return nil, Usage{}, fmt.Errorf("decide: %s: %w", s.Name(), err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// Ollama says exactly what it refused (too many options, too long a
		// state, an unknown model); a bare status would leave it to guessing.
		return nil, Usage{}, fmt.Errorf("decide: %s: %s: %s", s.Name(), res.Status, errorText(res.Body))
	}
	var reply struct {
		Answers map[string]wireAnswer `json:"answers"`
		Usage   Usage                 `json:"usage"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&reply); err != nil {
		return nil, Usage{}, fmt.Errorf("decide: %s: reading the answer: %w", s.Name(), err)
	}
	out := make(map[string]Answer, len(qs))
	for name, q := range qs {
		w, ok := reply.Answers[name]
		if !ok {
			return nil, Usage{}, fmt.Errorf("decide: %s did not answer %q", s.Name(), name)
		}
		a, err := w.answer(q)
		if err != nil {
			return nil, Usage{}, fmt.Errorf("decide: %s: question %q: %w", s.Name(), name, err)
		}
		out[name] = a
	}
	return out, reply.Usage, nil
}

func (s *SystemOne) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout > 0 {
		return context.WithTimeout(ctx, s.timeout)
	}
	return ctx, func() {}
}

func (s *SystemOne) authorize(req *http.Request) {
	if s.key != "" {
		req.Header.Set("Authorization", "Bearer "+s.key)
	}
}

type wireQuestion struct {
	Type         Type   `json:"type"`
	Instructions string `json:"instructions"`
	// Criteria: an ordered object for a choice, a list for a score, an
	// optional {"true", "false"} object for a noul.
	Criteria json.RawMessage `json:"criteria,omitempty"`
}

type wireRequest struct {
	Model     string                  `json:"model"`
	State     string                  `json:"state"`
	Questions map[string]wireQuestion `json:"questions"`
	KeepAlive string                  `json:"keep_alive,omitempty"`
}

// encode is the request body for qs, which Validate has passed.
func (s *SystemOne) encode(state string, qs map[string]Question) ([]byte, error) {
	r := wireRequest{Model: s.model, State: state, Questions: make(map[string]wireQuestion, len(qs))}
	if s.keepAlive != 0 {
		r.KeepAlive = s.keepAlive.String() // Ollama parses Go durations; negative keeps the model loaded
	}
	for name, q := range qs {
		c, err := criteria(q)
		if err != nil {
			return nil, fmt.Errorf("decide: question %q: %w", name, err)
		}
		r.Questions[name] = wireQuestion{Type: q.Type, Instructions: q.Instructions, Criteria: c}
	}
	return json.Marshal(r)
}

// criteria renders a question's options in System One's shape.
func criteria(q Question) (json.RawMessage, error) {
	switch q.Type {
	case TypeScore:
		return json.Marshal(q.Options)
	case TypeNoul:
		// Ollama's keys are "true" and "false"; a side left undescribed
		// keeps the model's own reading.
		side := map[string]string{}
		if d := strings.TrimSpace(q.Descriptions["yes"]); d != "" {
			side["true"] = d
		}
		if d := strings.TrimSpace(q.Descriptions["no"]); d != "" {
			side["false"] = d
		}
		if len(side) == 0 {
			return nil, nil
		}
		return json.Marshal(side)
	default:
		// An object in option order, written by hand: a Go map would sort
		// it, and Ollama breaks ties by this order. An undescribed option is
		// null, which Ollama reads as a label that describes itself.
		var b bytes.Buffer
		b.WriteByte('{')
		for i, o := range q.Options {
			if i > 0 {
				b.WriteByte(',')
			}
			k, err := json.Marshal(o)
			if err != nil {
				return nil, err
			}
			b.Write(k)
			b.WriteByte(':')
			if d := strings.TrimSpace(q.Descriptions[o]); d != "" {
				v, err := json.Marshal(d)
				if err != nil {
					return nil, err
				}
				b.Write(v)
			} else {
				b.WriteString("null")
			}
		}
		b.WriteByte('}')
		return b.Bytes(), nil
	}
}

// wireAnswer is one answer as System One sends it. A noul has only its
// probability of yes, and no confidence.
type wireAnswer struct {
	Type          Type               `json:"type"`
	Choice        string             `json:"choice"`
	Noul          *float64           `json:"noul"`
	Score         *float64           `json:"score"`
	Probabilities map[string]float64 `json:"probabilities"`
	Confidence    float64            `json:"confidence"`
}

// answer converts w, the answer to q, and checks it fits q.
func (w wireAnswer) answer(q Question) (Answer, error) {
	if w.Type != q.Type {
		return Answer{}, fmt.Errorf("answered as a %q, asked as a %q", w.Type, q.Type)
	}
	a := Answer{Type: q.Type, Confidence: w.Confidence}
	switch q.Type {
	case TypeNoul:
		if w.Noul == nil {
			return Answer{}, fmt.Errorf("a noul answer without its probability")
		}
		p := *w.Noul
		a.Yes, a.Label = p >= 0.5, "no"
		if a.Yes {
			a.Label = "yes"
		}
		a.Confidence = math.Abs(2*p - 1)
		a.Probabilities = map[string]float64{"yes": p, "no": 1 - p}
	case TypeChoice:
		known := false
		for _, o := range q.Options {
			known = known || o == w.Choice
		}
		if !known {
			return Answer{}, fmt.Errorf("chose %q, which is not an option", w.Choice)
		}
		a.Label, a.Probabilities = w.Choice, w.Probabilities
	case TypeScore:
		if w.Score == nil {
			return Answer{}, fmt.Errorf("a score answer without its score")
		}
		// Probabilities come keyed by level index; key them by level, and
		// take the most likely level as the label (the lower one on a tie).
		a.Score, a.Probabilities = *w.Score, make(map[string]float64, len(q.Options))
		best := -1.0
		for k, p := range w.Probabilities {
			i, err := strconv.Atoi(k)
			if err != nil || i < 0 || i >= len(q.Options) {
				return Answer{}, fmt.Errorf("a probability for level %q, of %d levels", k, len(q.Options))
			}
			a.Probabilities[q.Options[i]] = p
		}
		for _, o := range q.Options {
			if p, ok := a.Probabilities[o]; ok && p > best {
				a.Label, best = o, p
			}
		}
		if a.Label == "" {
			a.Label = q.Options[min(max(int(math.Round(a.Score)), 0), len(q.Options)-1)]
		}
	}
	return a, nil
}

// errorText is the message of an error body: Ollama sends {"error":"..."},
// an OpenAI-style relay {"error":{"message":"..."}}; anything else is the
// body itself, cut short.
func errorText(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, 4096))
	var e struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &e) == nil && len(e.Error) > 0 {
		var msg string
		if json.Unmarshal(e.Error, &msg) == nil && msg != "" {
			return msg
		}
		var obj struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(e.Error, &obj) == nil && obj.Message != "" {
			return obj.Message
		}
	}
	text := strings.TrimSpace(string(raw))
	if len(text) > 300 {
		text = strings.ToValidUTF8(text[:300], "")
	}
	if text == "" {
		return "no message"
	}
	return text
}
