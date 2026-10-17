package app

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

// observing is -observe-addr (the read-only HTTP view with Prometheus
// metrics) and -otlp (traces and metrics over OTLP/HTTP, configured by the
// standard OTEL_ variables), fed from the event log by one goroutine.
type observing struct {
	logf     func(string, ...any)
	srv      *http.Server
	follow   context.CancelFunc
	followed chan struct{}
	shutdown []func(context.Context) error
}

func startObserving(ctx context.Context, c Config, m *neumann.Machine, logf func(string, ...any)) (*observing, error) {
	o := &observing{logf: logf}
	if c.ObserveAddr == "" && !c.OTLP {
		return o, nil
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "nm")),
		resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, err
	}
	var observers []observe.Observer
	var readers []sdkmetric.Option
	var prom http.Handler
	if c.ObserveAddr != "" {
		reader, h, err := observe.PrometheusReader()
		if err != nil {
			return nil, err
		}
		readers, prom = append(readers, sdkmetric.WithReader(reader)), h
	}
	if c.OTLP {
		exp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, err
		}
		readers = append(readers, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
		texp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		tr := observe.NewTracer(texp, observe.TracerOptions{Resource: res})
		o.shutdown = append(o.shutdown, tr.Shutdown)
		observers = append(observers, tr)
	}
	mp := sdkmetric.NewMeterProvider(append(readers, sdkmetric.WithResource(res))...)
	o.shutdown = append(o.shutdown, mp.Shutdown)
	mt, err := observe.NewMetrics(m, mp, observe.MetricsOptions{})
	if err != nil {
		return nil, err
	}
	observers = append(observers, mt)
	from, err := observe.Head(ctx, m)
	if err != nil {
		return nil, err
	}
	fctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	o.follow, o.followed = cancel, make(chan struct{})
	go func() {
		defer close(o.followed)
		observe.Follow(fctx, m, from, observers...)
	}()
	if c.ObserveAddr != "" {
		ln, err := net.Listen("tcp", c.ObserveAddr)
		if err != nil {
			o.stop()
			return nil, err
		}
		o.srv = &http.Server{Handler: observe.NewHandler(m, observe.HandlerOptions{Metrics: prom}), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := o.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logf("observe: %v", err)
			}
		}()
		logf("observe: http://%s/status (also /queue, /tasks/{id}, /events, /metrics)", ln.Addr())
	}
	return o, nil
}

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
			o.logf("observe: shutdown: %v", err)
		}
	}
	if o.srv != nil {
		if err := o.srv.Shutdown(ctx); err != nil {
			o.srv.Close()
		}
	}
}
