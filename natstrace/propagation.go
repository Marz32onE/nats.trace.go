package natstrace

import (
	"go.opentelemetry.io/otel/propagation"

	nats "github.com/nats-io/nats.go"
)

// HeaderCarrier adapts nats.Header to propagation.TextMapCarrier for W3C TraceContext inject/extract.
// Used by jetstreamtrace and by Conn internally.
type HeaderCarrier struct {
	H nats.Header
}

func (c HeaderCarrier) Get(key string) string {
	if c.H == nil {
		return ""
	}
	return c.H.Get(key)
}

func (c HeaderCarrier) Set(key, value string) {
	if c.H == nil {
		return
	}
	c.H.Set(key, value)
}

func (c HeaderCarrier) Keys() []string {
	if c.H == nil {
		return nil
	}
	keys := make([]string, 0, len(c.H))
	for k := range c.H {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = (*HeaderCarrier)(nil)
