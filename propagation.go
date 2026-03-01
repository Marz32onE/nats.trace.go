package natstrace

import (
	"go.opentelemetry.io/otel/propagation"

	nats "github.com/nats-io/nats.go"
)

// headerCarrier adapts nats.Header to propagation.TextMapCarrier for
// W3C TraceContext and Baggage inject/extract.
type headerCarrier struct {
	h nats.Header
}

func (c headerCarrier) Get(key string) string {
	if c.h == nil {
		return ""
	}
	return c.h.Get(key)
}

func (c headerCarrier) Set(key, value string) {
	if c.h == nil {
		return
	}
	c.h.Set(key, value)
}

func (c headerCarrier) Keys() []string {
	if c.h == nil {
		return nil
	}
	keys := make([]string, 0, len(c.h))
	for k := range c.h {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = headerCarrier{}
