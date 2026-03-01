package natstracing

import (
	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier adapts nats.Header to the propagation.TextMapCarrier
// interface so that the W3C TraceContext and Baggage propagators can
// read from and write to NATS message headers.
type headerCarrier struct {
	h nats.Header
}

// Get returns the value for the given key, following the case-insensitive
// canonical header key look-up that net/http uses.
func (c headerCarrier) Get(key string) string {
	return c.h.Get(key)
}

// Set stores the value under the given key, creating the header map lazily.
func (c headerCarrier) Set(key, value string) {
	c.h.Set(key, value)
}

// Keys returns all header keys currently stored in the carrier.
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.h))
	for k := range c.h {
		keys = append(keys, k)
	}
	return keys
}

// Compile-time assertion: headerCarrier must satisfy propagation.TextMapCarrier.
var _ propagation.TextMapCarrier = headerCarrier{}
