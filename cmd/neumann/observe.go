package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/observe"
)

// observing is what -observe-addr and the OTLP environment attach to the
// machine: the HTTP view, metrics, traces, and the goroutine feeding them
// the event log.
type observing struct {
	logf     func(string, ...any)
	srv      *http.Server
	follow   context.CancelFunc
	followed chan struct{}
	shutdown []func(context.Context) error
}

// otlpWanted reports whether the standard OpenTelemetry environment asks
// for traces and metrics over OTLP.
func otlpWanted(getenv func(string) string) (traces, metrics bool) {
	all := getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
	return all || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "", all || getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

// startObserving attaches what c and the environment ask for; with neither
// it attaches nothing and costs nothing. Observers start at the end of the
// log: they count what happens from now on.
func startObserving(ctx context.Context, c config, getenv func(string) string, m *neumann.Machine, logf func(string, ...any)) (*observing, error) {
	o := &observing{logf: logf}
	traces, otlpMetrics := otlpWanted(getenv)
	if c.observeAddr == "" && !traces && !otlpMetrics {
		return o, nil
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "neumann")),
		resource.WithFromEnv(), // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES win
		resource.WithTelemetrySDK())
	if err != nil {
		return nil, err
	}
	var observers []observe.Observer
	var promHandler http.Handler
	var readers []sdkmetric.Option
	if c.metrics {
		reader, h, err := observe.PrometheusReader()
		if err != nil {
			return nil, err
		}
		readers, promHandler = append(readers, sdkmetric.WithReader(reader)), h
	}
	if otlpMetrics {
		exp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, err
		}
		readers = append(readers, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	if len(readers) > 0 {
		mp := sdkmetric.NewMeterProvider(append(readers, sdkmetric.WithResource(res))...)
		o.shutdown = append(o.shutdown, mp.Shutdown)
		mt, err := observe.NewMetrics(m, mp, observe.MetricsOptions{})
		if err != nil {
			return nil, err
		}
		observers = append(observers, mt)
	}
	if traces {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		tr := observe.NewTracer(exp, observe.TracerOptions{Resource: res})
		o.shutdown = append(o.shutdown, tr.Shutdown)
		observers = append(observers, tr)
	}
	var ln net.Listener
	if c.observeAddr != "" {
		if ln, err = net.Listen("tcp", c.observeAddr); err != nil {
			o.stop()
			return nil, err
		}
	}
	if len(observers) > 0 {
		from, err := observe.Head(ctx, m)
		if err != nil {
			if ln != nil {
				ln.Close()
			}
			o.stop()
			return nil, err
		}
		fctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		o.follow, o.followed = cancel, make(chan struct{})
		go func() {
			defer close(o.followed)
			observe.Follow(fctx, m, from, observers...)
		}()
	}
	if ln != nil {
		h := observe.NewHandler(m, observe.HandlerOptions{Metrics: promHandler, Pprof: c.pprof, Token: c.observeToken})
		o.srv = &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := o.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logf("neumann: observe: %v", err)
			}
		}()
		logf("neumann: observe: serving http://%s/status", ln.Addr())
		if c.observeToken == "" {
			if host, _, _ := net.SplitHostPort(ln.Addr().String()); !net.ParseIP(host).IsLoopback() {
				logf("neumann: WARNING: observe serves task content (/tasks, /events) on %s without NEUMANN_OBSERVE_TOKEN", ln.Addr())
			}
		}
	}
	return o, nil
}

// stop runs once the machine has stopped: the observers read the log to its
// end, the exporters flush, and the HTTP server closes. Each step is
// bounded, so a dead collector cannot hold up the exit.
func (o *observing) stop() {
	if o.followed != nil {
		select {
		case <-o.followed:
		case <-time.After(5 * time.Second):
			o.follow()
			<-o.followed
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, f := range o.shutdown {
		if err := f(ctx); err != nil {
			o.logf("neumann: observe: shutdown: %v", err)
		}
	}
	if o.srv != nil {
		if err := o.srv.Shutdown(ctx); err != nil {
			o.srv.Close()
		}
	}
}
