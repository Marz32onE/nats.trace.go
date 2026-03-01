package natstrace

import (
	"context"
	"time"

	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const messagingSystem = "nats"

// MsgHandler is the callback for traced subscriptions (like nats.MsgHandler but with context).
// The context carries the extracted trace from the incoming message headers.
type MsgHandler func(ctx context.Context, msg *nats.Msg)

// Conn is a tracing-aware wrapper around *nats.Conn. API mirrors nats.Conn: Publish, PublishMsg,
// Subscribe, QueueSubscribe, Close, Drain, NatsConn. All publish/subscribe use W3C trace propagation.
type Conn struct {
	nc     *nats.Conn
	tracer trace.Tracer
	opts   options
}

func newConn(nc *nats.Conn, opts ...Option) *Conn {
	o := applyOptions(opts)
	return &Conn{
		nc:     nc,
		tracer: o.tracerProvider.Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion), trace.WithSchemaURL(semconv.SchemaURL)),
		opts:   o,
	}
}

// NatsConn returns the underlying *nats.Conn (same as using the original nats package).
func (c *Conn) NatsConn() *nats.Conn {
	return c.nc
}

// Drain flushes and closes the connection, waiting for pending work.
func (c *Conn) Drain() error {
	return c.nc.Drain()
}

// Close closes the underlying connection.
func (c *Conn) Close() {
	c.nc.Close()
}

// Publish publishes data to subject with trace context in headers (like nats.Conn.Publish + propagation).
func (c *Conn) Publish(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	return c.PublishMsg(ctx, msg)
}

// PublishMsg publishes the message with trace context in msg.Header (like nats.Conn.PublishMsg + propagation).
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
	c.opts.propagator.Inject(ctx, headerCarrier{msg.Header})
	if err := c.nc.PublishMsg(msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// PublishJetStream publishes to JetStream with trace context in headers.
func (c *Conn) PublishJetStream(ctx context.Context, subject string, data []byte) error {
	js, err := c.nc.JetStream()
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	ctx, span := c.tracer.Start(ctx, subject+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(publishAttrs(msg)...),
	)
	defer span.End()
	c.opts.propagator.Inject(ctx, headerCarrier{msg.Header})
	_, err = js.PublishMsg(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// Request sends a request with trace context and waits for a reply (like nats.Conn.Request + propagation).
func (c *Conn) Request(ctx context.Context, subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	ctx, span := c.tracer.Start(ctx, subject+" publish",
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

// Subscribe subscribes to subject (like nats.Conn.Subscribe); handler receives context with extracted trace.
func (c *Conn) Subscribe(subject string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subject, c.wrapHandler(subject, "", handler))
}

// QueueSubscribe is the queue-group variant (like nats.Conn.QueueSubscribe).
func (c *Conn) QueueSubscribe(subject, queue string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.QueueSubscribe(subject, queue, c.wrapHandler(subject, queue, handler))
}

// SubscribeJetStream subscribes to a JetStream subject with DeliverNew(); handler may call msg.Ack().
func (c *Conn) SubscribeJetStream(subject string, handler MsgHandler) (*nats.Subscription, error) {
	js, err := c.nc.JetStream()
	if err != nil {
		return nil, err
	}
	return js.Subscribe(subject, c.wrapHandler(subject, "", handler), nats.DeliverNew())
}

func (c *Conn) wrapHandler(subject, queue string, handler MsgHandler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		msgCtx := c.opts.propagator.Extract(context.Background(), headerCarrier{msg.Header})
		spanName := subject + " receive"
		opts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(receiveAttrs(msg, queue)...),
		}
		// Fan-out: use link only (no parent from message) so multiple consumers do not
		// become N children of one producer in the same trace. Per messaging spec.
		if sc := trace.SpanFromContext(msgCtx).SpanContext(); sc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
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
