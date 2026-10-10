// Command observe runs a machine on a simulated model, offline, with the
// observe package attached: the HTTP view with Prometheus metrics and
// pprof, the activity log on stderr, and traces kept in memory and printed
// as a tree at the end. It runs a few tasks, prints the URLs to look at, and
// exits once the tasks are done unless -serve keeps it up.
//
//	go run ./examples/observe -serve
//	curl -s http://127.0.0.1:43517/status
//	curl -N 'http://127.0.0.1:43517/events?from_seq=0'
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/observe"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:43517", "where to serve the observe view")
	serve := flag.Bool("serve", false, "keep serving after the tasks are done, until interrupted")
	tasks := flag.Int("tasks", 4, "tasks to run")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The model thinks for 300ms, asks for the clock once, then answers. The
	// tool takes 100ms. Every number below comes from this script.
	clock := builtin.New("util").Add(bus.ToolDef{Name: "now", Description: "The time.", Effect: bus.ReadOnly},
		func(context.Context, bus.DeviceCall) (string, error) {
			time.Sleep(100 * time.Millisecond)
			return time.Now().UTC().Format(time.RFC3339), nil
		})
	m, err := neumann.New(neumann.Config{
		Model: &sim.Model{Name: "sim:think-300ms", Fn: func(req model.Request) (model.Response, error) {
			time.Sleep(300 * time.Millisecond)
			if sim.ToolRounds(req) == 0 {
				return sim.CallTools("util_now"), nil
			}
			return sim.Reply("It is time for " + sim.FirstUser(req) + "."), nil
		}},
		Devices: []bus.Device{clock},
		Cores:   2,
		Logf:    log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Metrics on a Prometheus registry, traces in memory, the activity log
	// on stderr: all fed from the event log by one Follow.
	reader, prom, err := observe.PrometheusReader()
	if err != nil {
		log.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := observe.NewMetrics(m, mp, observe.MetricsOptions{})
	if err != nil {
		log.Fatal(err)
	}
	spans := tracetest.NewInMemoryExporter()
	tracer := observe.NewTracer(spans, observe.TracerOptions{})
	activity := observe.NewLog(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	followed := make(chan struct{})
	go func() {
		defer close(followed)
		observe.Follow(context.Background(), m, 0, metrics, tracer, activity)
	}()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Handler: observe.NewHandler(m, observe.HandlerOptions{Metrics: prom, Pprof: true}), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)
	base := "http://" + ln.Addr().String()
	fmt.Printf("observe view on %s:\n", base)
	for _, p := range []string{"/status", "/queue", "/tasks/task-1", "/events?from_seq=0", "/metrics", "/debug/pprof/"} {
		fmt.Printf("  %s%s\n", base, p)
	}

	events := m.Events(ctx, 0)
	for i := 1; i <= *tasks; i++ {
		if _, err := m.Submit(ctx, proto.Instruction{TaskID: fmt.Sprintf("task-%d", i), Input: fmt.Sprintf("task %d", i)}); err != nil {
			log.Fatal(err)
		}
	}
	for done := 0; done < *tasks; {
		ev, ok := <-events
		if !ok {
			log.Fatal("event stream ended early")
		}
		if ev.Terminal() {
			done++
		}
	}
	st, _ := json.Marshal(m.Status().Groups[0])
	fmt.Printf("\nall %d tasks done; group: %s\n", *tasks, st)
	if *serve {
		fmt.Println("serving until interrupted (-serve)")
		<-ctx.Done()
	}

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.Stop(sctx)
	<-followed // the log is read to its end once the machine stops
	srv.Shutdown(sctx)
	mp.Shutdown(sctx)
	printTraces(spans.GetSpans())
}

// printTraces prints each task's trace as an indented tree with offsets
// from the task's start.
func printTraces(stubs tracetest.SpanStubs) {
	children := map[trace.SpanID][]tracetest.SpanStub{}
	var roots []tracetest.SpanStub
	for _, s := range stubs {
		if s.Parent.IsValid() {
			children[s.Parent.SpanID()] = append(children[s.Parent.SpanID()], s)
		} else {
			roots = append(roots, s)
		}
	}
	slices.SortFunc(roots, func(a, b tracetest.SpanStub) int { return a.StartTime.Compare(b.StartTime) })
	fmt.Println("\ntraces (offsets from each task's start):")
	var walk func(s tracetest.SpanStub, t0 time.Time, depth int)
	walk = func(s tracetest.SpanStub, t0 time.Time, depth int) {
		id := ""
		for _, a := range s.Attributes {
			if a.Key == "neumann.task.id" && depth == 0 {
				id = " " + a.Value.AsString()
			}
		}
		fmt.Printf("  %s%-*s %7.3fs +%7.3fs%s\n", strings.Repeat("  ", depth), 24-2*depth, s.Name+id,
			s.StartTime.Sub(t0).Seconds(), s.EndTime.Sub(s.StartTime).Seconds(), status(s))
		kids := children[s.SpanContext.SpanID()]
		slices.SortFunc(kids, func(a, b tracetest.SpanStub) int { return a.StartTime.Compare(b.StartTime) })
		for _, k := range kids {
			walk(k, t0, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, r.StartTime, 0)
	}
}

func status(s tracetest.SpanStub) string {
	if s.Status.Description != "" {
		return "  " + s.Status.Code.String() + ": " + s.Status.Description
	}
	return ""
}
