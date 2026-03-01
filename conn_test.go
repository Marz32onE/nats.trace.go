package natstracing_test

import (
	"context"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	natssrv "github.com/nats-io/nats-server/v2/server"
	natstracing "github.com/Marz32onE/nats.trace.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// test helpers
// ---------------------------------------------------------------------------

// newTestProvider creates an in-memory tracer provider and span recorder.
func newTestProvider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp, sr
}

// startServer starts an in-process NATS server on a random port and returns
// its client URL. The server is shut down via t.Cleanup.
func startServer(t *testing.T) string {
	t.Helper()
	opts := &natssrv.Options{
		Host: "127.0.0.1",
		Port: -1, // pick a random available port
	}
	s, err := natssrv.NewServer(opts)
	if err != nil {
		t.Fatalf("nats-server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not become ready in time")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

// findSpanByKind returns the first span with the given SpanKind or nil.
func findSpanByKind(spans []sdktrace.ReadOnlySpan, kind trace.SpanKind) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.SpanKind() == kind {
			return s
		}
	}
	return nil
}

// assertAttr fails the test if the given attribute key does not have the
// expected string value.
func assertAttr(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, kv := range attrs {
		if string(kv.Key) == key {
			if got := kv.Value.AsString(); got != want {
				t.Errorf("attribute %q: got %q, want %q", key, got, want)
			}
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}

// ---------------------------------------------------------------------------
// Propagation smoke-test (no server needed)
// ---------------------------------------------------------------------------

// TestW3CPropagationRoundtrip verifies that trace context injected into a
// NATS header by Publish can be extracted by the W3C TraceContext propagator,
// mirroring what the subscriber wrapper does.
func TestW3CPropagationRoundtrip(t *testing.T) {
	url := startServer(t)

	tp, _ := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstracing.Connect(url, nil,
		natstracing.WithTracerProvider(tp),
		natstracing.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	// Start a parent span so there is a non-zero traceID to propagate.
	tracer := tp.Tracer("roundtrip-test")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	defer parentSpan.End()
	wantTraceID := parentSpan.SpanContext().TraceID()

	subject := "rt.test"

	// The subscriber captures the header map via a raw nats.Conn subscription.
	rawConn := conn.NatsConn()
	headerCh := make(chan nats.Header, 1)
	_, err = rawConn.Subscribe(subject, func(msg *nats.Msg) {
		headerCh <- msg.Header
	})
	if err != nil {
		t.Fatalf("raw Subscribe: %v", err)
	}

	if err := conn.Publish(parentCtx, subject, []byte("ping")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var h nats.Header
	select {
	case h = <-headerCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Extract the trace context from the received header.
	extracted := prop.Extract(context.Background(), natsHeaderCarrier(h))
	gotTraceID := trace.SpanFromContext(extracted).SpanContext().TraceID()

	if gotTraceID != wantTraceID {
		t.Errorf("traceID mismatch: header contained %s, expected %s", gotTraceID, wantTraceID)
	}
}

// natsHeaderCarrier is a minimal TextMapCarrier backed by nats.Header,
// used only in tests to verify propagation without depending on the
// unexported headerCarrier in the natstracing package.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string      { return nats.Header(c).Get(key) }
func (c natsHeaderCarrier) Set(key, val string)        { nats.Header(c).Set(key, val) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// ---------------------------------------------------------------------------
// Publish tests
// ---------------------------------------------------------------------------

// TestPublishCreatesProducerSpan verifies that Publish starts a producer span
// with the correct span name and OTLP messaging attributes.
func TestPublishCreatesProducerSpan(t *testing.T) {
	url := startServer(t)

	tp, sr := newTestProvider()
	conn, err := natstracing.Connect(url, nil, natstracing.WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.publish"
	if err := conn.Publish(context.Background(), subject, []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]

	if want := subject + " publish"; s.Name() != want {
		t.Errorf("span name: got %q, want %q", s.Name(), want)
	}
	if s.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind: got %v, want Producer", s.SpanKind())
	}
	assertAttr(t, s.Attributes(), "messaging.system", "nats")
	assertAttr(t, s.Attributes(), "messaging.destination.name", subject)
	assertAttr(t, s.Attributes(), "messaging.operation.type", "publish")
	assertAttr(t, s.Attributes(), "messaging.operation.name", "publish")
}

// TestPublishMsgCreatesProducerSpan verifies that PublishMsg propagates the
// trace context into the message and creates a producer span.
func TestPublishMsgCreatesProducerSpan(t *testing.T) {
	url := startServer(t)

	tp, sr := newTestProvider()
	conn, err := natstracing.Connect(url, nil, natstracing.WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.publishmsg"
	msg := &nats.Msg{
		Subject: subject,
		Data:    []byte("hello msg"),
	}
	if err := conn.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind: got %v, want Producer", spans[0].SpanKind())
	}
}

// ---------------------------------------------------------------------------
// Subscribe tests
// ---------------------------------------------------------------------------

// TestSubscribeExtractsTraceContext verifies that the subscriber callback
// receives a context whose traceID matches the one injected by the publisher.
func TestSubscribeExtractsTraceContext(t *testing.T) {
	url := startServer(t)

	tp, sr := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstracing.Connect(url, nil,
		natstracing.WithTracerProvider(tp),
		natstracing.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.subscribe"
	done := make(chan trace.TraceID, 1)

	_, err = conn.Subscribe(subject, func(ctx context.Context, _ *nats.Msg) {
		done <- trace.SpanFromContext(ctx).SpanContext().TraceID()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish with an active parent span.
	tracer := tp.Tracer("publisher")
	pubCtx, pubSpan := tracer.Start(context.Background(), "pub-parent")
	wantTraceID := pubSpan.SpanContext().TraceID()

	if err := conn.Publish(pubCtx, subject, []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pubSpan.End()

	select {
	case gotTraceID := <-done:
		if gotTraceID != wantTraceID {
			t.Errorf("traceID: subscriber got %s, expected %s", gotTraceID, wantTraceID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for subscribed message")
	}

	// Verify consumer span.
	consumerSpan := findSpanByKind(sr.Ended(), trace.SpanKindConsumer)
	if consumerSpan == nil {
		t.Fatal("no consumer span found")
	}
	if want := subject + " receive"; consumerSpan.Name() != want {
		t.Errorf("consumer span name: got %q, want %q", consumerSpan.Name(), want)
	}
	assertAttr(t, consumerSpan.Attributes(), "messaging.system", "nats")
	assertAttr(t, consumerSpan.Attributes(), "messaging.operation.type", "receive")
	assertAttr(t, consumerSpan.Attributes(), "messaging.operation.name", "receive")
}

// TestQueueSubscribeRecordsQueueName verifies that QueueSubscribe records the
// queue group name in the consumer span attributes.
func TestQueueSubscribeRecordsQueueName(t *testing.T) {
	url := startServer(t)

	tp, sr := newTestProvider()
	conn, err := natstracing.Connect(url, nil, natstracing.WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.queue"
	queue := "workers"
	done := make(chan struct{}, 1)

	_, err = conn.QueueSubscribe(subject, queue, func(ctx context.Context, _ *nats.Msg) {
		done <- struct{}{}
	})
	if err != nil {
		t.Fatalf("QueueSubscribe: %v", err)
	}

	if err := conn.Publish(context.Background(), subject, []byte("work")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for queue message")
	}

	consumerSpan := findSpanByKind(sr.Ended(), trace.SpanKindConsumer)
	if consumerSpan == nil {
		t.Fatal("no consumer span found")
	}
	assertAttr(t, consumerSpan.Attributes(), "messaging.consumer.group.name", queue)
}

// TestSubscribeConsumerSpanLinkedToProducer verifies that the consumer span's
// parent is the producer's span (i.e. the traceID is the same and the
// consumer's parentSpanID equals the producer's spanID).
func TestSubscribeConsumerSpanLinkedToProducer(t *testing.T) {
	url := startServer(t)

	tp, sr := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstracing.Connect(url, nil,
		natstracing.WithTracerProvider(tp),
		natstracing.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.linkage"
	done := make(chan struct{}, 1)

	_, err = conn.Subscribe(subject, func(ctx context.Context, _ *nats.Msg) {
		done <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := conn.Publish(context.Background(), subject, []byte("link-test")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	spans := sr.Ended()
	producer := findSpanByKind(spans, trace.SpanKindProducer)
	consumer := findSpanByKind(spans, trace.SpanKindConsumer)

	if producer == nil || consumer == nil {
		t.Fatalf("missing spans: producer=%v consumer=%v", producer, consumer)
	}

	if producer.SpanContext().TraceID() != consumer.SpanContext().TraceID() {
		t.Errorf("traceID mismatch: producer %s, consumer %s",
			producer.SpanContext().TraceID(), consumer.SpanContext().TraceID())
	}
	if consumer.Parent().SpanID() != producer.SpanContext().SpanID() {
		t.Errorf("consumer parent spanID %s != producer spanID %s",
			consumer.Parent().SpanID(), producer.SpanContext().SpanID())
	}
}
