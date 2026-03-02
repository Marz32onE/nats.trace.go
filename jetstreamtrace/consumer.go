package jetstreamtrace

import (
	"context"

	"github.com/Marz32onE/natstrace/natstrace"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// MessageHandler is the callback for Consume. Same as jetstream MessageHandler but receives
// context with trace extracted from message headers.
type MessageHandler func(ctx context.Context, msg Msg)

// ConsumeContext is returned by Consume. Same as jetstream.ConsumeContext; call Stop() when done.
type ConsumeContext interface {
	Stop()
}

// MessagesContext is the iterator from Messages(). Same as jetstream.MessagesContext but
// Next() returns (ctx, msg, error) with ctx carrying extracted trace.
type MessagesContext interface {
	Next(opts ...jetstream.NextOpt) (context.Context, Msg, error)
	Stop()
	Drain()
}

// ConsumerInfo mirrors jetstream.ConsumerInfo.
type ConsumerInfo = jetstream.ConsumerInfo

// Consumer mirrors jetstream.Consumer. Consume(handler) passes (ctx, msg) to handler; Messages().Next() returns (ctx, msg, error); Next() returns (ctx, msg, error).
// Fetch/FetchBytes/FetchNoWait are not provided (batch pull trace wrapping skipped).
type Consumer interface {
	Consume(handler MessageHandler, opts ...jetstream.PullConsumeOpt) (ConsumeContext, error)
	Messages(opts ...jetstream.PullMessagesOpt) (MessagesContext, error)
	Next(ctx context.Context, opts ...jetstream.FetchOpt) (context.Context, Msg, error)
	Info(ctx context.Context) (*ConsumerInfo, error)
	CachedInfo() *ConsumerInfo
}

// Attribute for distinguishing which consumer handled the message (durable/consumer name).
const attrConsumerName = "messaging.consumer.name"

type consumerImpl struct {
	conn         *natstrace.Conn
	streamName   string
	consumerName string
	c            jetstream.Consumer
}

func (c *consumerImpl) Consume(handler MessageHandler, opts ...jetstream.PullConsumeOpt) (ConsumeContext, error) {
	tracer, prop := c.conn.TraceContext()
	wrapped := func(msg jetstream.Msg) {
		h := msg.Headers()
		if h == nil {
			h = make(nats.Header)
		}
		msgCtx := prop.Extract(context.Background(), &natstrace.HeaderCarrier{H: h})
		spanName := msg.Subject() + " receive"
		attrs := append(receiveAttrs(msg), attribute.String(attrConsumerName, c.consumerName))
		ctx, span := tracer.Start(msgCtx, spanName,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()
		handler(ctx, msg)
	}
	cc, err := c.c.Consume(wrapped, opts...)
	if err != nil {
		return nil, err
	}
	return &consumeContextImpl{cc: cc}, nil
}

func (c *consumerImpl) Messages(opts ...jetstream.PullMessagesOpt) (MessagesContext, error) {
	iter, err := c.c.Messages(opts...)
	if err != nil {
		return nil, err
	}
	return &messagesContextImpl{conn: c.conn, consumerName: c.consumerName, iter: iter}, nil
}

func (c *consumerImpl) Next(ctx context.Context, opts ...jetstream.FetchOpt) (context.Context, Msg, error) {
	msg, err := c.c.Next(opts...)
	if err != nil {
		return nil, nil, err
	}
	h := msg.Headers()
	if h == nil {
		h = make(nats.Header)
	}
	_, prop := c.conn.TraceContext()
	msgCtx := prop.Extract(context.Background(), &natstrace.HeaderCarrier{H: h})
	return msgCtx, msg, nil
}

func (c *consumerImpl) Info(ctx context.Context) (*ConsumerInfo, error) {
	return c.c.Info(ctx)
}

func (c *consumerImpl) CachedInfo() *ConsumerInfo {
	return c.c.CachedInfo()
}

func receiveAttrs(msg jetstream.Msg) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject()),
		semconv.MessagingOperationTypeReceive,
		semconv.MessagingOperationNameKey.String("receive"),
	}
	if d := msg.Data(); len(d) > 0 {
		attrs = append(attrs, semconv.MessagingMessageBodySize(len(d)))
	}
	return attrs
}

type consumeContextImpl struct {
	cc jetstream.ConsumeContext
}

func (c *consumeContextImpl) Stop() {
	if c.cc != nil {
		c.cc.Stop()
	}
}

type messagesContextImpl struct {
	conn         *natstrace.Conn
	consumerName string
	iter         jetstream.MessagesContext
	lastSpan     trace.Span
}

func (m *messagesContextImpl) Next(opts ...jetstream.NextOpt) (context.Context, Msg, error) {
	if m.lastSpan != nil {
		m.lastSpan.End()
		m.lastSpan = nil
	}
	msg, err := m.iter.Next(opts...)
	if err != nil {
		return nil, nil, err
	}
	h := msg.Headers()
	if h == nil {
		h = make(nats.Header)
	}
	tracer, prop := m.conn.TraceContext()
	msgCtx := prop.Extract(context.Background(), &natstrace.HeaderCarrier{H: h})
	spanName := msg.Subject() + " receive"
	attrs := append(receiveAttrs(msg), attribute.String(attrConsumerName, m.consumerName))
	ctx, span := tracer.Start(msgCtx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
	m.lastSpan = span
	return ctx, msg, nil
}

func (m *messagesContextImpl) Stop() {
	if m.lastSpan != nil {
		m.lastSpan.End()
		m.lastSpan = nil
	}
	m.iter.Stop()
}

func (m *messagesContextImpl) Drain() {
	if m.lastSpan != nil {
		m.lastSpan.End()
		m.lastSpan = nil
	}
	m.iter.Drain()
}
