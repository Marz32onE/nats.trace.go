package natstrace

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	tracerInitialized bool
	globalShutdown    func()
)

// InitTracer sets the global TracerProvider and TextMapPropagator for the process.
// Call once at startup (e.g. in main) with OTLP endpoint and resource attributes.
// Propagator is fixed to TraceContext + Baggage. If the global provider is already
// an SDK TracerProvider (e.g. set by another package), InitTracer is a no-op.
// Empty endpoint uses OTEL_EXPORTER_OTLP_ENDPOINT env or "localhost:4317".
func InitTracer(endpoint string, attrs ...attribute.KeyValue) error {
	if g := otel.GetTracerProvider(); g != nil {
		if _, ok := g.(*sdktrace.TracerProvider); ok {
			tracerInitialized = true
			return nil
		}
	}
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	useHTTP := useHTTPEndpoint(endpoint)
	ctx := context.Background()

	var exp sdktrace.SpanExporter
	var err error
	if useHTTP {
		exp, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithInsecure(),
		)
	} else {
		exp, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		)
	}
	if err != nil {
		return err
	}

	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		return err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	globalShutdown = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}
	tracerInitialized = true
	return nil
}

// ShutdownTracer shuts down the TracerProvider set by InitTracer. Call before process exit (e.g. defer).
func ShutdownTracer() {
	if globalShutdown != nil {
		globalShutdown()
		globalShutdown = nil
	}
}

func useHTTPEndpoint(endpoint string) bool {
	s := strings.TrimSpace(endpoint)
	if s == "" {
		return false
	}
	if u, err := url.Parse(s); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		return true
	}
	_, port, err := splitHostPort(s)
	if err != nil {
		return false
	}
	p, _ := strconv.Atoi(port)
	return p == 4318
}

func splitHostPort(hostport string) (host, port string, err error) {
	u, err := url.Parse("//" + hostport)
	if err != nil {
		return "", "", err
	}
	return u.Hostname(), u.Port(), nil
}

func isTracerInitialized() bool {
	return tracerInitialized
}
