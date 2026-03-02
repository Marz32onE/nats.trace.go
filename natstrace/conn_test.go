package natstrace_test

import (
	"context"
	"testing"
	"time"

	natstrace "github.com/Marz32onE/nats.trace.go/natstrace"
	nats "github.com/nats-io/nats.go"
	natssrv "github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func newTestProvider() (*trace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	return tp, sr
}

func startServer(t *testing.T) string {
	t.Helper()
	opts := &natssrv.Options{Host: "127.0.0.1", Port: -1}
	s, err := natssrv.NewServer(opts)
	if err != nil {
		t.Fatalf("nats-server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func findSpanByKind(spans []trace.ReadOnlySpan, kind oteltrace.SpanKind) trace.ReadOnlySpan {
	for _, s := range spans {
		if s.SpanKind() == kind {
			return s
		}
	}
	return nil
}

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
	t.Errorf("attribute %q not found", key)
}

func TestW3CPropagationRoundtrip(t *testing.T) {
	url := startServer(t)
	tp, _ := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp), natstrace.WithPropagator(prop))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	tracer := tp.Tracer("roundtrip-test")
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	defer parentSpan.End()
	wantTraceID := parentSpan.SpanContext().TraceID()

	subject := "rt.test"
	headerCh := make(chan nats.Header, 1)
	_, err = conn.NatsConn().Subscribe(subject, func(msg *nats.Msg) {
		headerCh <- msg.Header
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := conn.Publish(parentCtx, subject, []byte("ping")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var h nats.Header
	select {
	case h = <-headerCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	carrier := natstrace.HeaderCarrier{H: h}
	extracted := prop.Extract(context.Background(), carrier)
	gotTraceID := oteltrace.SpanFromContext(extracted).SpanContext().TraceID()
	if gotTraceID != wantTraceID {
		t.Errorf("traceID: got %s, want %s", gotTraceID, wantTraceID)
	}
}

func TestPublishCreatesProducerSpan(t *testing.T) {
	url := startServer(t)
	tp, sr := newTestProvider()
	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp))
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
	if s.SpanKind() != oteltrace.SpanKindProducer {
		t.Errorf("span kind: got %v", s.SpanKind())
	}
	assertAttr(t, s.Attributes(), "messaging.system", "nats")
	assertAttr(t, s.Attributes(), "messaging.destination.name", subject)
}

func TestPublishMsgCreatesProducerSpan(t *testing.T) {
	url := startServer(t)
	tp, sr := newTestProvider()
	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.publishmsg"
	msg := &nats.Msg{Subject: subject, Data: []byte("hello msg")}
	if err := conn.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 || spans[0].SpanKind() != oteltrace.SpanKindProducer {
		t.Errorf("expected 1 producer span, got %d", len(spans))
	}
}

func TestSubscribeExtractsTraceContext(t *testing.T) {
	url := startServer(t)
	tp, sr := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp), natstrace.WithPropagator(prop))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject := "test.subscribe"
	done := make(chan struct{}, 1)
	_, err = conn.Subscribe(subject, func(ctx context.Context, _ *nats.Msg) {
		_ = oteltrace.SpanFromContext(ctx).SpanContext().TraceID()
		done <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tracer := tp.Tracer("publisher")
	pubCtx, pubSpan := tracer.Start(context.Background(), "pub-parent")
	if err := conn.Publish(pubCtx, subject, []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pubSpan.End()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	spans := sr.Ended()
	producer := findSpanByKind(spans, oteltrace.SpanKindProducer)
	consumerSpan := findSpanByKind(spans, oteltrace.SpanKindConsumer)
	if consumerSpan == nil {
		t.Fatal("no consumer span")
	}
	if want := subject + " receive"; consumerSpan.Name() != want {
		t.Errorf("consumer span name: got %q, want %q", consumerSpan.Name(), want)
	}
	if producer != nil {
		if consumerSpan.SpanContext().TraceID() != producer.SpanContext().TraceID() {
			t.Errorf("consumer should be in same trace as producer")
		}
		if consumerSpan.Parent().SpanID() != producer.SpanContext().SpanID() {
			t.Errorf("consumer should be child of producer span")
		}
	}
}

func TestQueueSubscribeRecordsQueueName(t *testing.T) {
	url := startServer(t)
	tp, sr := newTestProvider()
	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	subject, queue := "test.queue", "workers"
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
		t.Fatal("timeout")
	}
	consumerSpan := findSpanByKind(sr.Ended(), oteltrace.SpanKindConsumer)
	if consumerSpan == nil {
		t.Fatal("no consumer span")
	}
	assertAttr(t, consumerSpan.Attributes(), "messaging.consumer.group.name", queue)
}

func TestSubscribeConsumerSpanLinkedToProducer(t *testing.T) {
	url := startServer(t)
	tp, sr := newTestProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil, natstrace.WithTracerProvider(tp), natstrace.WithPropagator(prop))
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
	producer := findSpanByKind(spans, oteltrace.SpanKindProducer)
	consumer := findSpanByKind(spans, oteltrace.SpanKindConsumer)
	if producer == nil || consumer == nil {
		t.Fatalf("missing spans: producer=%v consumer=%v", producer, consumer)
	}
	if consumer.SpanContext().TraceID() != producer.SpanContext().TraceID() {
		t.Errorf("consumer should be in same trace as producer")
	}
	if consumer.Parent().SpanID() != producer.SpanContext().SpanID() {
		t.Errorf("consumer should be child of producer span")
	}
}
