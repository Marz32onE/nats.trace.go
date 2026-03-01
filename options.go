// Package natstracing provides OpenTelemetry tracing instrumentation for
// the NATS messaging system, wrapping the nats.io/nats.go client library.
// It propagates trace context (W3C TraceContext) via NATS message headers so
// that every published message carries the active span's traceID/spanID, and
// every subscriber creates a new consumer span linked to the producer's context.
package natstracing

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	// instrumentationName is the OpenTelemetry instrumentation scope name used
	// when creating spans. It follows the reverse-DNS convention.
	instrumentationName    = "github.com/Marz32onE/nats.trace.go"
	instrumentationVersion = "0.1.0"
)

// options holds the configuration for the tracing wrapper.
type options struct {
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
}

// Option is a functional option that configures the tracing wrapper.
type Option func(*options)

// WithTracerProvider configures the TracerProvider used to create spans.
// Defaults to the global TracerProvider (otel.GetTracerProvider()).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) {
		o.tracerProvider = tp
	}
}

// WithPropagator configures the TextMapPropagator used to inject/extract
// trace context into/from NATS message headers.
// Defaults to the global propagator (otel.GetTextMapPropagator()).
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) {
		o.propagator = p
	}
}

// applyOptions applies the given options and fills in defaults.
func applyOptions(opts []Option) options {
	o := options{
		tracerProvider: otel.GetTracerProvider(),
		propagator:     otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
