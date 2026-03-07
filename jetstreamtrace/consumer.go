package jetstreamtrace

import (
	"context"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Marz32onE/natstrace/natstrace"
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

// MsgWithContext carries a message and the context with extracted trace (for Fetch batch iteration).
type MsgWithContext struct {
	Ctx context.Context
	Msg Msg
}

// MessageBatch is the result of Fetch/FetchBytes/FetchNoWait. Iterate over MessagesWithContext()
// to get (ctx, msg) with trace context; call Error() after the channel is closed.
type MessageBatch interface {
	MessagesWithContext() <-chan MsgWithContext
	Error() error
}

// ConsumerInfo mirrors jetstream.ConsumerInfo.
type ConsumerInfo = jetstream.ConsumerInfo

// Consumer mirrors jetstream.Consumer. Consume, Messages, Next; Fetch/FetchBytes/FetchNoWait
// return MessageBatch with MessagesWithContext() for trace context per message.
type Consumer interface {
	Consume(handler MessageHandler, opts ...jetstream.PullConsumeOpt) (ConsumeContext, error)
	Messages(opts ...jetstream.PullMessagesOpt) (MessagesContext, error)
	Next(ctx context.Context, opts ...jetstream.FetchOpt) (context.Context, Msg, error)
	Fetch(batch int, opts ...jetstream.FetchOpt) (MessageBatch, error)
	FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (MessageBatch, error)
	FetchNoWait(batch int) (MessageBatch, error)
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
		spanName := "process " + msg.Subject()
		attrs := append(receiveAttrs(msg, "process"), attribute.String(attrConsumerName, c.consumerName))
		startOpts := []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attrs...),
		}
		if sc := trace.SpanContextFromContext(msgCtx); sc.IsValid() {
			startOpts = append(startOpts, trace.WithLinks(trace.LinkFromContext(msgCtx)))
		}
		ctx, span := tracer.Start(context.Background(), spanName, startOpts...)
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
	if ctx != nil {
		opts = append([]jetstream.FetchOpt{jetstream.FetchContext(ctx)}, opts...)
	}
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

func (c *consumerImpl) Fetch(batch int, opts ...jetstream.FetchOpt) (MessageBatch, error) {
	raw, err := c.c.Fetch(batch, opts...)
	if err != nil {
		return nil, err
	}
	return wrapMessageBatch(c.conn, c.consumerName, raw), nil
}

func (c *consumerImpl) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (MessageBatch, error) {
	raw, err := c.c.FetchBytes(maxBytes, opts...)
	if err != nil {
		return nil, err
	}
	return wrapMessageBatch(c.conn, c.consumerName, raw), nil
}

func (c *consumerImpl) FetchNoWait(batch int) (MessageBatch, error) {
	raw, err := c.c.FetchNoWait(batch)
	if err != nil {
		return nil, err
	}
	return wrapMessageBatch(c.conn, c.consumerName, raw), nil
}

func (c *consumerImpl) Info(ctx context.Context) (*ConsumerInfo, error) {
	return c.c.Info(ctx)
}

func (c *consumerImpl) CachedInfo() *ConsumerInfo {
	return c.c.CachedInfo()
}

// receiveAttrs builds consumer span attributes. opType is "process" (push) or "receive" (pull).
func receiveAttrs(msg jetstream.Msg, opType string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(messagingSystem),
		semconv.MessagingDestinationNameKey.String(msg.Subject()),
		attribute.String(string(semconv.MessagingOperationTypeKey), opType),
		semconv.MessagingOperationNameKey.String(opType),
	}
	if d := msg.Data(); len(d) > 0 {
		attrs = append(attrs, semconv.MessagingMessageBodySize(len(d)))
	}
	return attrs
}

type messageBatchTrace struct {
	ch  chan MsgWithContext
	raw jetstream.MessageBatch
}

func (m *messageBatchTrace) MessagesWithContext() <-chan MsgWithContext {
	return m.ch
}

func (m *messageBatchTrace) Error() error {
	return m.raw.Error()
}

func wrapMessageBatch(conn *natstrace.Conn, consumerName string, raw jetstream.MessageBatch) MessageBatch {
	ch := make(chan MsgWithContext)
	go func() {
		defer close(ch)
		tracer, prop := conn.TraceContext()
		var lastSpan trace.Span
		for msg := range raw.Messages() {
			if lastSpan != nil {
				lastSpan.End()
				lastSpan = nil
			}
			h := msg.Headers()
			if h == nil {
				h = make(nats.Header)
			}
			msgCtx := prop.Extract(context.Background(), &natstrace.HeaderCarrier{H: h})
			spanName := "receive " + msg.Subject()
			attrs := append(receiveAttrs(msg, "receive"), attribute.String(attrConsumerName, consumerName))
			opts := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(attrs...),
			}
			if sc := trace.SpanContextFromContext(msgCtx); sc.IsValid() {
				opts = append(opts, trace.WithLinks(trace.LinkFromContext(msgCtx)))
			}
			ctx, span := tracer.Start(context.Background(), spanName, opts...)
			lastSpan = span
			ch <- MsgWithContext{Ctx: ctx, Msg: msg}
		}
		if lastSpan != nil {
			lastSpan.End()
		}
	}()
	return &messageBatchTrace{ch: ch, raw: raw}
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
	spanName := "receive " + msg.Subject()
	attrs := append(receiveAttrs(msg, "receive"), attribute.String(attrConsumerName, m.consumerName))
	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if sc := trace.SpanContextFromContext(msgCtx); sc.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trace.LinkFromContext(msgCtx)))
	}
	ctx, span := tracer.Start(context.Background(), spanName, startOpts...)
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
