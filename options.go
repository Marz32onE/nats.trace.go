package natstrace

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/Marz32onE/nats.trace.go"
	instrumentationVersion = "0.2.0"
)

type options struct {
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
}

// Option configures the tracing wrapper.
type Option func(*options)

// WithTracerProvider sets the TracerProvider used to create spans.
// Defaults to otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) {
		o.tracerProvider = tp
	}
}

// WithPropagator sets the TextMapPropagator used to inject/extract trace
// context into/from NATS message headers. Defaults to otel.GetTextMapPropagator().
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) {
		o.propagator = p
	}
}

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
