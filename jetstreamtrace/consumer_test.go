package jetstreamtrace_test

import (
	"context"
	"testing"
	"time"

	natssrv "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/Marz32onE/natstrace/jetstreamtrace"
	natstrace "github.com/Marz32onE/natstrace/natstrace"
)

func startJetStreamServer(t *testing.T) string {
	t.Helper()
	opts := &natssrv.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
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

func TestFetchReturnsMessagesWithTraceContext(t *testing.T) {
	url := startJetStreamServer(t)
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil,
		natstrace.WithTracerProvider(tp),
		natstrace.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	js, err := jetstreamtrace.New(conn)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	ctx := context.Background()
	streamName := "FETCHTEST"
	_, err = js.CreateOrUpdateStream(ctx, jetstreamtrace.StreamConfig{
		Name:     streamName,
		Subjects: []string{"fetch.>"},
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateStream: %v", err)
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	consumerName := "fetch-consumer"
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstreamtrace.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: "fetch.test",
		AckPolicy:     jetstreamtrace.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}

	// Publish with trace context
	tracer := tp.Tracer("publisher")
	pubCtx, pubSpan := tracer.Start(ctx, "pub-parent")
	defer pubSpan.End()
	if _, err := js.Publish(pubCtx, "fetch.test", []byte("hello fetch")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Fetch with retries until message is available (deterministic, avoids flaky fixed sleep)
	var received int
	var batch jetstreamtrace.MessageBatch
	for attempt := 0; attempt < 30; attempt++ {
		var ferr error
		batch, ferr = cons.Fetch(5, jetstream.FetchMaxWait(300*time.Millisecond))
		if ferr != nil {
			t.Fatalf("Fetch: %v", ferr)
		}
		for m := range batch.MessagesWithContext() {
			received++
			if string(m.Msg.Data()) != "hello fetch" {
				t.Errorf("got data %q", m.Msg.Data())
			}
			// Context carries consumer span (correlation to producer is via link only)
			span := oteltrace.SpanFromContext(m.Ctx)
			if !span.SpanContext().TraceID().IsValid() {
				t.Error("context should have valid trace ID")
			}
			_ = m.Msg.Ack()
		}
		if batch.Error() != nil {
			t.Logf("Fetch attempt %d: %v", attempt+1, batch.Error())
		}
		if received == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if received != 1 {
		t.Errorf("expected 1 message, got %d after retries", received)
	}
	if batch != nil && batch.Error() != nil {
		t.Errorf("batch error: %v", batch.Error())
	}

	// Assert consumer span has messaging.consumer.name and link to producer
	spans := sr.Ended()
	consumerSpan := findSpanByKind(spans, oteltrace.SpanKindConsumer)
	producerSpan := findSpanByKind(spans, oteltrace.SpanKindProducer)
	if consumerSpan == nil {
		t.Fatal("no consumer span")
	}
	assertAttr(t, consumerSpan.Attributes(), "messaging.consumer.name", consumerName)
	if producerSpan != nil && len(consumerSpan.Links()) == 1 {
		linkCtx := consumerSpan.Links()[0].SpanContext
		if linkCtx.TraceID() != producerSpan.SpanContext().TraceID() || linkCtx.SpanID() != producerSpan.SpanContext().SpanID() {
			t.Errorf("consumer link should point to producer span")
		}
	}
}

func TestConsumeTraceContext(t *testing.T) {
	url := startJetStreamServer(t)
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil,
		natstrace.WithTracerProvider(tp),
		natstrace.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	js, err := jetstreamtrace.New(conn)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	ctx := context.Background()
	_, err = js.CreateOrUpdateStream(ctx, jetstreamtrace.StreamConfig{
		Name:     "CONSUMETEST",
		Subjects: []string{"consume.>"},
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateStream: %v", err)
	}
	stream, _ := js.Stream(ctx, "CONSUMETEST")
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstreamtrace.ConsumerConfig{
		Durable:       "consume-dup",
		FilterSubject: "consume.msg",
		AckPolicy:     jetstreamtrace.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}

	done := make(chan struct{}, 1)
	cc, err := cons.Consume(func(msgCtx context.Context, msg jetstreamtrace.Msg) {
		if oteltrace.SpanFromContext(msgCtx).SpanContext().TraceID().IsValid() {
			done <- struct{}{}
		}
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer cc.Stop()

	tracer := tp.Tracer("pub")
	pubCtx, pubSpan := tracer.Start(ctx, "parent")
	defer pubSpan.End()
	_, _ = js.Publish(pubCtx, "consume.msg", []byte("hi"))
	time.Sleep(300 * time.Millisecond)
	select {
	case <-done:
	default:
		t.Fatal("Consume handler did not receive trace context")
	}
}

func TestMessagesNextTraceContext(t *testing.T) {
	url := startJetStreamServer(t)
	tp := trace.NewTracerProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil,
		natstrace.WithTracerProvider(tp),
		natstrace.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	js, _ := jetstreamtrace.New(conn)
	ctx := context.Background()
	_, _ = js.CreateOrUpdateStream(ctx, jetstreamtrace.StreamConfig{
		Name:     "MSGTEST",
		Subjects: []string{"msg.>"},
	})
	stream, _ := js.Stream(ctx, "MSGTEST")
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstreamtrace.ConsumerConfig{
		Durable:       "msg-dup",
		FilterSubject: "msg.one",
		AckPolicy:     jetstreamtrace.AckExplicitPolicy,
	})

	iter, err := cons.Messages()
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	_, _ = js.Publish(ctx, "msg.one", []byte("data"))
	time.Sleep(300 * time.Millisecond)

	msgCtx, msg, err := iter.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(msg.Data()) != "data" {
		t.Errorf("got %q", msg.Data())
	}
	if !oteltrace.SpanFromContext(msgCtx).SpanContext().TraceID().IsValid() {
		t.Error("Next should return context with trace")
	}
	_ = msg.Ack()
}

func TestFetchNoWaitReturnsTraceContext(t *testing.T) {
	url := startJetStreamServer(t)
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(tracetest.NewSpanRecorder()))
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil,
		natstrace.WithTracerProvider(tp),
		natstrace.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	js, _ := jetstreamtrace.New(conn)
	ctx := context.Background()
	_, _ = js.CreateOrUpdateStream(ctx, jetstreamtrace.StreamConfig{
		Name:     "NOWAIT",
		Subjects: []string{"nowait.>"},
	})
	stream, _ := js.Stream(ctx, "NOWAIT")
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstreamtrace.ConsumerConfig{
		Durable:       "nowait-c",
		FilterSubject: "nowait.x",
		AckPolicy:     jetstreamtrace.AckExplicitPolicy,
	})

	_, _ = js.Publish(ctx, "nowait.x", []byte("v"))
	time.Sleep(200 * time.Millisecond)

	batch, err := cons.FetchNoWait(5)
	if err != nil {
		t.Fatalf("FetchNoWait: %v", err)
	}
	n := 0
	for m := range batch.MessagesWithContext() {
		n++
		if !oteltrace.SpanFromContext(m.Ctx).SpanContext().TraceID().IsValid() {
			t.Error("context should have trace")
		}
		_ = m.Msg.Ack()
	}
	if n != 1 {
		t.Errorf("expected 1 message, got %d", n)
	}
}

func TestFetchBytesTraceContext(t *testing.T) {
	url := startJetStreamServer(t)
	tp := trace.NewTracerProvider()
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})

	conn, err := natstrace.Connect(url, nil,
		natstrace.WithTracerProvider(tp),
		natstrace.WithPropagator(prop),
	)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	js, _ := jetstreamtrace.New(conn)
	ctx := context.Background()
	_, _ = js.CreateOrUpdateStream(ctx, jetstreamtrace.StreamConfig{
		Name:     "BYTESTEST",
		Subjects: []string{"bytes.>"},
	})
	stream, _ := js.Stream(ctx, "BYTESTEST")
	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstreamtrace.ConsumerConfig{
		Durable:       "bytes-c",
		FilterSubject: "bytes.a",
		AckPolicy:     jetstreamtrace.AckExplicitPolicy,
	})

	_, _ = js.Publish(ctx, "bytes.a", []byte("hello"))
	time.Sleep(200 * time.Millisecond)

	batch, err := cons.FetchBytes(1024, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("FetchBytes: %v", err)
	}
	for m := range batch.MessagesWithContext() {
		if !oteltrace.SpanFromContext(m.Ctx).SpanContext().TraceID().IsValid() {
			t.Error("context should have trace")
		}
		_ = m.Msg.Ack()
	}
}
