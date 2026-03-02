package natstrace

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/Marz32onE/natstrace/natstrace"
	instrumentationVersion = "0.3.0"
)

type options struct {
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
}

// Option configures the tracing wrapper.
type Option func(*options)

// WithTracerProvider sets the TracerProvider. Defaults to otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) {
		o.tracerProvider = tp
	}
}

// WithPropagator sets the TextMapPropagator for inject/extract. Defaults to otel.GetTextMapPropagator().
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
