# natstrace

OpenTelemetry (OTLP) tracing for the [NATS](https://nats.io) client: **Core NATS** and **JetStream**.  
W3C TraceContext is injected into outbound messages and extracted on the consumer side, so messages carry a distributed trace that can be collected by any OTLP backend (Jaeger, Tempo, Honeycomb, etc.).

The API mirrors [nats.io/nats.go](https://github.com/nats-io/nats.go) and [nats.io/nats.go/jetstream](https://github.com/nats-io/nats.go/tree/main/jetstream): same function names and types, with `context.Context` added for tracing.

**Conventions**: Spans and attributes follow the [OpenTelemetry Semantic Conventions for Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) (trace structure, operation types, span names, and attributes).

---

## What this package does

- **Producer**: On each `Publish` / `PublishMsg`, a "Send" span is created and its context is injected into the message as the *message creation context*. Span name is `send {subject}`; `messaging.operation.type` is `send`.
- **Consumer**: On each received message (Subscribe, Consume, Messages, Fetch), a "Process" (push) or "Receive" (pull) span is created with **no parent** from the message; it **links** to the message creation context so producer and consumer are correlated only via the link (no parent-child). Span names are `process {subject}` or `receive {subject}`.
- **End-to-end**: Backends (e.g. Tempo, Grafana) correlate producer and consumer via the span link; the consumer span is the root of its own trace.

---

## Packages

| Package | Purpose |
|--------|---------|
| **natstrace** | Core NATS: `Connect`, `Conn`, `Publish`, `PublishMsg`, `Request`, `Subscribe`, `QueueSubscribe` |
| **jetstreamtrace** | JetStream: `New(conn)`, `JetStream` (Publish, Stream), `Stream` (Consumer), `Consumer` (Consume, Messages, Next) |

Use **natstrace** for plain NATS pub/sub and request-reply. Use **jetstreamtrace** for JetStream streams and consumers; it takes a `*natstrace.Conn` so trace is propagated end-to-end.

---

## Installation

```bash
go get github.com/Marz32onE/natstrace
```

---

## Quick Start

### 1. Core NATS — connect and publish

```go
import (
    "go.opentelemetry.io/otel"
    natstrace "github.com/Marz32onE/natstrace/natstrace"
    "go.opentelemetry.io/otel/propagation"
)

conn, err := natstrace.Connect("nats://localhost:4222", nil,
    natstrace.WithTracerProvider(otel.GetTracerProvider()),
    natstrace.WithPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    )),
)
// conn.Publish(ctx, subject, data) / PublishMsg(ctx, msg) / Request(ctx, subject, data, timeout)
```

### 2. Core NATS — subscribe (handler receives context with extracted trace)

```go
conn.Subscribe("orders.created", func(ctx context.Context, msg *nats.Msg) {
    _, span := tracer.Start(ctx, "process order")
    defer span.End()
    // handle msg
})

conn.QueueSubscribe("orders.created", "processors", func(ctx context.Context, msg *nats.Msg) {
    // ...
})
```

### 3. JetStream — create stream and publish

```go
import (
    natstrace "github.com/Marz32onE/natstrace/natstrace"
    "github.com/Marz32onE/natstrace/jetstreamtrace"
)

conn, _ := natstrace.Connect("nats://localhost:4222", nil /* opts */)
js, err := jetstreamtrace.New(conn)
// CreateOrUpdateStream, then:
ack, err := js.Publish(ctx, "orders.created", payload)
// or js.PublishMsg(ctx, msg, opts...)
```

### 4. JetStream — consume (Consume callback or Messages().Next())

```go
stream, _ := js.Stream(ctx, "ORDERS")
consumer, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
    Durable: "worker-1",
})
// Option A: Consume with handler(ctx, msg)
cc, _ := consumer.Consume(func(ctx context.Context, msg jetstreamtrace.Msg) {
    // ctx has extracted trace; create child spans as needed
})
defer cc.Stop()

// Option B: Messages() iterator — Next() returns (ctx, msg, error)
iter, _ := consumer.Messages()
ctx, msg, err := iter.Next()
// ... later iter.Stop() or iter.Drain()
```

---

## Features

| Feature | Details |
|--------|---------|
| **Context propagation** | W3C `traceparent` / `tracestate` in NATS message headers (message creation context) |
| **Producer spans** | `SpanKindProducer` per Publish / PublishMsg; creation context injected into message |
| **Consumer spans** | `SpanKindConsumer` per message; producer and consumer are correlated **only via span link** (no parent-child), per [OTel messaging semconv](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#trace-structure) |
| **JetStream consumer name** | Consumer spans include `messaging.consumer.name` (durable/consumer name) |
| **Connection modes** | Plain, TLS (`ConnectTLS`), credentials file (`ConnectWithCredentials`) |
| **OTLP semantics** | `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type` (send/process/receive), `messaging.message.body.size`, `messaging.consumer.group.name` (queue), `messaging.consumer.name` (JetStream) |
| **Configurable** | `WithTracerProvider`, `WithPropagator` |

---

## Trace structure (OTel messaging semconv)

The spec uses **span links as the default mechanism** to correlate producers and consumers (no parent-child). This package follows that: the consumer span is started with no parent and only adds a link to the message creation context (producer span).

- **Producer**: A "Send" span is created; its context is injected into the message as the *message creation context*. Span name: `send {destination}`.
- **Consumer**: A "Process" (push) or "Receive" (pull) span is created with `context.Background()` as parent and a **link** to the message creation context. Span names: `process {destination}` or `receive {destination}`.

**Reference**: [Semantic conventions for messaging spans — Trace structure](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#trace-structure) (OpenTelemetry).

## Span attributes (OTLP Messaging Semconv)

Attribute names and values follow the [messaging spans spec](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#messaging-attributes).

| Attribute | Value |
|-----------|--------|
| `messaging.system` | `nats` |
| `messaging.destination.name` | Subject |
| `messaging.operation.type` | `send` (producer) / `process` (push consumer) / `receive` (pull consumer) |
| `messaging.operation.name` | `publish` (producer) / `process` or `receive` (consumer) |
| `messaging.message.body.size` | Byte length of payload |
| `messaging.consumer.group.name` | Queue group (queue subscriptions only) |
| `messaging.consumer.name` | Consumer name (JetStream Consume / Messages only) |

---

## Options (natstrace)

| Option | Default | Description |
|--------|---------|-------------|
| `WithTracerProvider(tp)` | `otel.GetTracerProvider()` | TracerProvider for spans |
| `WithPropagator(p)` | `otel.GetTextMapPropagator()` | Inject/extract (e.g. TraceContext + Baggage) |

---

## API notes

- **Sync only**: Async publish (e.g. `PublishAsync`) is not wrapped.
- **Fetch / FetchBytes / FetchNoWait**: Single-fetch batch APIs return `MessageBatch`; iterate `MessagesWithContext()` for `(ctx, msg)` with trace and consumer span per message (same semantics as `Consume` / `Messages()`).
- **Types**: `jetstreamtrace` re-exports types such as `Msg`, `PubAck`, `StreamConfig`, `ConsumerConfig`, `ConsumerInfo`, `ConsumeContext`, `MessagesContext`, `MessageBatch`, `MsgWithContext` so callers need not import `jetstream` for common types.
- **HeaderCarrier**: `natstrace.HeaderCarrier` adapts `nats.Header` to `propagation.TextMapCarrier` for custom inject/extract (e.g. in WebSocket or HTTP bridges).

---

## Project layout

Packages live in top-level directories at the module root: `./natstrace` (Core NATS) and `./jetstreamtrace` (JetStream). The module path is `github.com/Marz32onE/natstrace`, so import paths are `github.com/Marz32onE/natstrace/natstrace` and `github.com/Marz32onE/natstrace/jetstreamtrace`.

---

## Development

- **Tests**: `go test ./...`
- **Lint**: [golangci-lint](https://golangci-lint.run/) — run `golangci-lint run ./...` locally (install via `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`). CI runs tests and lint on push/PR to `main` and `feat/package-nats-and-jetstream`.
- **Pre-commit** (optional): Add a [pre-commit](https://pre-commit.com/) hook that runs `go test ./...` and `golangci-lint run ./...` before commit; config is not included in repo, so set it up locally if desired.

---

## References

- [Semantic conventions for messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) — OpenTelemetry spec for trace structure, span names, operation types, and attributes used by this package.
