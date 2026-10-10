package observe

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/proto"
)

// HandlerOptions configure the HTTP handler.
type HandlerOptions struct {
	// Metrics is served at /metrics; nil: not served. PrometheusReader
	// builds one.
	Metrics http.Handler
	// Pprof serves the runtime profiles at /debug/pprof/. Off by default:
	// a CPU profile or trace costs the process while it runs.
	Pprof bool
	// Token, when set, is required on every request as
	// "Authorization: Bearer <Token>".
	Token string
	// KeepAlive is how often an idle /events stream gets a comment line, so
	// proxies do not close it; 0 = 15s.
	KeepAlive time.Duration
}

// NewHandler returns a read-only HTTP view of a machine, for a host to
// mount:
//
//	GET /status            Status, as JSON
//	GET /queue             Queue, as JSON
//	GET /tasks/{id}        {"task": the committed TCB, "calls": its call records}
//	GET /events?from_seq=N committed events with Seq > N as server-sent events
//	                       (id: seq, event: kind, data: the event as JSON),
//	                       the log first, then live; Last-Event-ID resumes
//	                       after that seq; &task=ID keeps one task's events
//	GET /metrics           HandlerOptions.Metrics, if set
//	GET /debug/pprof/...   runtime profiles, if HandlerOptions.Pprof
//
// There is no endpoint that changes anything; other methods get 405.
//
// /tasks and /events expose task content: prompts, answers, tool arguments
// and results. Do not serve the handler where anyone but the operator can
// reach it, unless HandlerOptions.Token is set (and the connection is
// encrypted).
func NewHandler(src Source, o HandlerOptions) http.Handler {
	if o.KeepAlive <= 0 {
		o.KeepAlive = 15 * time.Second
	}
	h := &handler{src: src, o: o}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", h.json(func(*http.Request) (any, error) { return src.Status(), nil }))
	mux.HandleFunc("/queue", h.json(func(*http.Request) (any, error) { return src.Queue(), nil }))
	mux.HandleFunc("/tasks/{id}", h.json(h.task))
	mux.HandleFunc("/events", h.events)
	if o.Metrics != nil {
		mux.Handle("/metrics", o.Metrics)
	}
	if o.Pprof {
		mux.HandleFunc("/debug/pprof/", pprofHandler)
	}
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "neumann: /status /queue /tasks/{id} /events?from_seq=N")
	})
	return h.guard(mux)
}

type handler struct {
	src Source
	o   HandlerOptions
}

// guard enforces the method and the token before anything else runs.
func (h *handler) guard(next http.Handler) http.Handler {
	want := []byte("Bearer " + h.o.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.o.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="neumann"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func (h *handler) json(get func(*http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v, err := get(r)
		if err != nil {
			code := http.StatusInternalServerError
			var he *httpError
			if errors.As(err, &he) {
				code = he.code
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(v)
	}
}

type taskView struct {
	Task  any              `json:"task"`
	Calls []mem.CallRecord `json:"calls"`
}

func (h *handler) task(r *http.Request) (any, error) {
	id := r.PathValue("id")
	t, err := h.src.Load(r.Context(), id)
	if errors.Is(err, mem.ErrNotFound) {
		return nil, &httpError{http.StatusNotFound, "no task " + strconv.Quote(id)}
	}
	if err != nil {
		return nil, err
	}
	calls, err := h.src.Calls(r.Context(), id)
	if err != nil {
		return nil, err
	}
	return taskView{Task: t, Calls: calls}, nil
}

// events streams the log as server-sent events. It reads at the client's
// pace from the durable log (Machine.Events), so a slow client never holds
// the machine up, and a client that reconnects with Last-Event-ID misses
// nothing.
func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	from := int64(0)
	q := r.URL.Query()
	if s := q.Get("from_seq"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "from_seq: want a seq", http.StatusBadRequest)
			return
		}
		from = n
	}
	if s := r.Header.Get("Last-Event-ID"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 0 {
			from = n
		}
	}
	only := q.Get("task")
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	fl.Flush()
	ctx := r.Context()
	evs := h.src.Events(ctx, from)
	tick := time.NewTicker(h.o.KeepAlive)
	defer tick.Stop()
	unflushed := false
	for {
		select {
		case ev, ok := <-evs:
			if !ok {
				return
			}
			if only == "" || ev.TaskID == only {
				if err := writeEvent(w, ev); err != nil {
					return
				}
				unflushed = true
			}
			if unflushed && len(evs) == 0 {
				fl.Flush() // caught up: send what is written
				unflushed = false
			}
		case <-tick.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			fl.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func writeEvent(w http.ResponseWriter, ev proto.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// Kinds are plain words; a kind with a line break would end the field.
	kind := strings.NewReplacer("\n", "", "\r", "").Replace(ev.Kind)
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, kind, b)
	return err
}

// PrometheusReader returns an OpenTelemetry metric reader that exposes what
// it reads on a registry of its own, and the handler that serves that
// registry in the Prometheus text format: give the reader to the meter
// provider Metrics uses, and the handler to HandlerOptions.Metrics. OTel
// names become Prometheus names with dots as underscores, the unit as a
// suffix (_seconds, _bytes) and _total on counters.
func PrometheusReader() (sdkmetric.Reader, http.Handler, error) {
	reg := prometheus.NewRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, nil, err
	}
	return exp, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}
