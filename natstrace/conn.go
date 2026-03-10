package natstrace

import (
	"context"
	"time"

	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/Marz32onE/natstrace/natstrace"
	instrumentationVersion = "0.1.9"
	messagingSystem        = "nats"
)

// MsgHandler is the callback for subscriptions. Same as nats.MsgHandler but with context
// that carries the trace extracted from the message headers.
type MsgHandler func(ctx context.Context, msg *nats.Msg)

// Conn is a tracing-aware wrapper around *nats.Conn. API mirrors nats.Conn; the only
// difference is Publish/PublishMsg take context.Context and handlers receive (ctx, msg).
type Conn struct {
	nc         *nats.Conn
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

func newConn(nc *nats.Conn) *Conn {
	tp := otel.GetTracerProvider()
	prop := otel.GetTextMapPropagator()
	return &Conn{
		nc:         nc,
		tracer:     tp.Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion), trace.WithSchemaURL(semconv.SchemaURL)),
		propagator: prop,
	}
}

// TraceContext returns the tracer and propagator used by this Conn. Used by jetstreamtrace.
func (c *Conn) TraceContext() (trace.Tracer, propagation.TextMapPropagator) {
	return c.tracer, c.propagator
}

// NatsConn returns the underlying *nats.Conn (same as nats package).
func (c *Conn) NatsConn() *nats.Conn {
	return c.nc
}

// Close closes the connection (same as nats.Conn.Close).
func (c *Conn) Close() {
	c.nc.Close()
}

// Drain flushes and closes (same as nats.Conn.Drain).
func (c *Conn) Drain() error {
	return c.nc.Drain()
}

// Publish publishes data to subject. Same as nats.Conn.Publish but accepts context for trace.
func (c *Conn) Publish(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	return c.PublishMsg(ctx, msg)
}

// PublishMsg publishes the message. Same as nats.Conn.PublishMsg but accepts context for trace.
// Per OTel messaging semconv: "Send" span with creation context injected into message; consumer
// spans link to this context. Span name is "{operation.name} {destination}".
func (c *Conn) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	spanName := "send " + msg.Subject
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(publishAttrs(msg)...),
	)
	defer span.End()
	c.propagator.Inject(ctx, &HeaderCarrier{H: msg.Header})
	if err := c.nc.PublishMsg(msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// Request sends a request and waits for reply. Same as nats.Conn.Request but accepts context.
// The timeout parameter is applied to the request; the call returns when the reply is received or timeout is reached.
func (c *Conn) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	spanName := "send " + subject
	reqCtx, span := c.tracer.Start(reqCtx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(messagingSystem),
			semconv.MessagingDestinationNameKey.String(subject),
			attribute.String(string(semconv.MessagingOperationTypeKey), "send"),
			semconv.MessagingOperationNameKey.String("publish"),
		),
	)
	defer span.End()
	c.propagator.Inject(reqCtx, &HeaderCarrier{H: msg.Header})
	reply, err := c.nc.RequestMsgWithContext(reqCtx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Int(string(semconv.MessagingMessageBodySizeKey), len(reply.Data)))
	return reply, nil
}

// Subscribe subscribes to subject. Handler receives (ctx, msg) with ctx carrying extracted trace.
func (c *Conn) Subscribe(subject string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subject, c.wrapHandler(subject, "", handler))
}

// QueueSubscribe is the queue-group variant. Handler receives (ctx, msg).
func (c *Conn) QueueSubscribe(subject, queue string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.QueueSubscribe(subject, queue, c.wrapHandler(subject, queue, handler))
}

func (c *Conn) wrapHandler(subject, queue string, handler MsgHandler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		msgCtx := c.propagator.Extract(context.Background(), &HeaderCarrier{H: msg.Header})
		// Per OTel messaging semconv: correlate producer and consumer only via span link (no parent-child).
		spanName := "process " + subject
		opts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(receiveAttrs(msg, queue, "process")...),
		}
		if sc := trace.SpanContextFromContext(msgCtx); sc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.LinkFromContext(msgCtx)))
		}
		ctx, span := c.tracer.Start(context.Background(), spanName, opts...)
		defer span.End()
		handler(ctx, msg)
	}
}

func publishAttrs(msg *nats.Msg) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject),
		attribute.String(string(semconv.MessagingOperationTypeKey), "send"),
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

// receiveAttrs builds consumer span attributes. opType is "process" (push) or "receive" (pull).
func receiveAttrs(msg *nats.Msg, queue string, opType string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject),
		attribute.String(string(semconv.MessagingOperationTypeKey), opType),
		semconv.MessagingOperationNameKey.String(opType),
	}
	if len(msg.Data) > 0 {
		attrs = append(attrs, semconv.MessagingMessageBodySize(len(msg.Data)))
	}
	if queue != "" {
		attrs = append(attrs, semconv.MessagingConsumerGroupName(queue))
	}
	return attrs
}
