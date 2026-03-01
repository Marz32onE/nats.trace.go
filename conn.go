package natstracing

import (
	"context"
	"time"

	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// messagingSystem is the OpenTelemetry semantic-convention value for NATS.
// NATS does not yet have a dedicated constant in the official semconv package,
// so we define it here following the specification.
const messagingSystem = "nats"

// MsgHandler is the callback signature for traced subscriptions. The context
// carries the extracted trace context (traceID/spanID) from the incoming
// NATS message headers, enabling callers to create child spans.
type MsgHandler func(ctx context.Context, msg *nats.Msg)

// Conn is a tracing-aware wrapper around *nats.Conn. Every Publish* and
// Request call creates an OTLP producer span and injects the W3C TraceContext
// into the NATS message headers. Every Subscribe* callback receives a context
// derived from the consumer span that was started by extracting the trace
// context from the incoming message headers.
type Conn struct {
	nc         *nats.Conn
	tracer     trace.Tracer
	opts       options
}

// newConn wraps an existing *nats.Conn with tracing instrumentation.
func newConn(nc *nats.Conn, tracingOpts ...Option) *Conn {
	o := applyOptions(tracingOpts)
	return &Conn{
		nc:   nc,
		tracer: o.tracerProvider.Tracer(
			instrumentationName,
			trace.WithInstrumentationVersion(instrumentationVersion),
			trace.WithSchemaURL(semconv.SchemaURL),
		),
		opts: o,
	}
}

// NatsConn returns the underlying *nats.Conn for advanced use-cases that
// require access to operations not covered by this wrapper.
func (c *Conn) NatsConn() *nats.Conn {
	return c.nc
}

// Drain drains the connection (flush + close), waiting for all pending work
// to complete. It delegates directly to *nats.Conn.Drain.
func (c *Conn) Drain() error {
	return c.nc.Drain()
}

// Close closes the underlying NATS connection immediately.
func (c *Conn) Close() {
	c.nc.Close()
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

// Publish creates a producer span, injects the W3C TraceContext into the
// NATS message headers, and publishes data to subject.
func (c *Conn) Publish(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	return c.PublishMsg(ctx, msg)
}

// PublishMsg creates a producer span, injects the W3C TraceContext into msg's
// headers, and publishes the message. If msg.Header is nil it is initialised
// automatically.
func (c *Conn) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}

	spanName := msg.Subject + " publish"
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(publishAttrs(msg)...),
	)
	defer span.End()

	// Inject trace context into the message headers.
	c.opts.propagator.Inject(ctx, headerCarrier{msg.Header})

	if err := c.nc.PublishMsg(msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// Request creates a producer span, injects the W3C TraceContext into request
// headers, sends the request, and waits up to timeout for a reply.
func (c *Conn) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}

	spanName := subject + " publish"
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(messagingSystem),
			semconv.MessagingDestinationNameKey.String(subject),
			semconv.MessagingOperationTypePublish,
			semconv.MessagingOperationNameKey.String("publish"),
		),
	)
	defer span.End()

	c.opts.propagator.Inject(ctx, headerCarrier{msg.Header})

	reply, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.SetAttributes(attribute.Int(string(semconv.MessagingMessageBodySizeKey), len(reply.Data)))
	return reply, nil
}

// ---------------------------------------------------------------------------
// Subscribing
// ---------------------------------------------------------------------------

// Subscribe creates a NATS subscription on subject. For every received
// message the wrapper:
//  1. Extracts the W3C TraceContext from the message headers.
//  2. Starts a consumer span (SpanKindConsumer) linked to the producer span.
//  3. Calls handler with the enriched context and the original message.
//  4. Ends the span after handler returns.
func (c *Conn) Subscribe(subject string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subject, c.wrapHandler(subject, "", handler))
}

// QueueSubscribe is the queue-group variant of Subscribe. Multiple instances
// sharing the same queue will load-balance incoming messages; tracing
// semantics are identical to Subscribe.
func (c *Conn) QueueSubscribe(subject, queue string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.QueueSubscribe(subject, queue, c.wrapHandler(subject, queue, handler))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// wrapHandler wraps a MsgHandler with tracing logic (context extraction and
// consumer span creation).
func (c *Conn) wrapHandler(subject, queue string, handler MsgHandler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		// Extract the remote trace context from message headers.
		carrier := headerCarrier{msg.Header}
		parentCtx := c.opts.propagator.Extract(context.Background(), carrier)

		spanName := subject + " receive"
		attrs := receiveAttrs(msg, queue)

		ctx, span := c.tracer.Start(parentCtx, spanName,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()

		handler(ctx, msg)
	}
}

// publishAttrs returns the standard OTLP messaging attributes for a publish
// operation.
func publishAttrs(msg *nats.Msg) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject),
		semconv.MessagingOperationTypePublish,
		semconv.MessagingOperationNameKey.String("publish"),
	}
	if len(msg.Data) > 0 {
		attrs = append(attrs, semconv.MessagingMessageBodySize(len(msg.Data)))
	}
	if msg.Reply != "" {
		attrs = append(attrs, semconv.MessagingMessageConversationID(msg.Reply))
	}
	return attrs
}

// receiveAttrs returns the standard OTLP messaging attributes for a receive
// (subscribe) operation.
func receiveAttrs(msg *nats.Msg, queue string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject),
		semconv.MessagingOperationTypeReceive,
		semconv.MessagingOperationNameKey.String("receive"),
	}
	if len(msg.Data) > 0 {
		attrs = append(attrs, semconv.MessagingMessageBodySize(len(msg.Data)))
	}
	if queue != "" {
		attrs = append(attrs, semconv.MessagingConsumerGroupName(queue))
	}
	return attrs
}
