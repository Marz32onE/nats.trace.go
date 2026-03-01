# nats.trace.go

`natstracing` is a thin OpenTelemetry (OTLP) tracing wrapper around the
[nats.io/nats.go](https://github.com/nats-io/nats.go) client library.  
It injects W3C TraceContext into every outbound NATS message and extracts it
on the subscriber side, so every message flowing through NATS carries a
distributed trace that can be collected by any OTLP-compatible backend
(Jaeger, Tempo, Honeycomb, etc.).

---

## Features

| Feature | Details |
|---|---|
| **Context propagation** | W3C `traceparent` / `tracestate` injected into NATS message headers |
| **Producer spans** | `SpanKindProducer` span per `Publish` / `PublishMsg` / `Request` call |
| **Consumer spans** | `SpanKindConsumer` span per received message; child of the producer span |
| **3 connection modes** | Plain, mTLS, credentials file (NKey + JWT) |
| **OTLP semantic conventions** | `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type`, `messaging.message.body.size`, `messaging.consumer.group.name` (semconv v1.27.0) |
| **Configurable** | Custom `TracerProvider` and `TextMapPropagator` via functional options |

---

## Installation

```bash
go get github.com/Marz32onE/nats.trace.go
```

---

## Quick Start

### 1 — Plain connection

```go
import (
    natstracing "github.com/Marz32onE/nats.trace.go"
    "go.opentelemetry.io/otel/propagation"
)

conn, err := natstracing.Connect("nats://localhost:4222", nil,
    natstracing.WithTracerProvider(otel.GetTracerProvider()),
    natstracing.WithPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    )),
)
```

### 2 — mTLS connection

```go
conn, err := natstracing.ConnectTLS(
    "nats://localhost:4222",
    "client.crt", "client.key", "ca.crt",
    nil, // extra nats.Options
)
```

### 3 — Credentials file (NKey + JWT)

```go
conn, err := natstracing.ConnectWithCredentials(
    "nats://localhost:4222",
    "/path/to/user.creds",
    nil, // extra nats.Options
)
```

---

## Publishing

```go
ctx := context.Background() // or a context already carrying a span

// Inject trace context and publish
err := conn.Publish(ctx, "orders.created", payload)

// Publish a pre-built message
msg := &nats.Msg{Subject: "orders.created", Data: payload}
err = conn.PublishMsg(ctx, msg)

// Request-reply
reply, err := conn.Request(ctx, "inventory.check", payload, 5*time.Second)
```

## Subscribing

```go
// Subscribe – the callback receives a context with the extracted traceID.
conn.Subscribe("orders.created", func(ctx context.Context, msg *nats.Msg) {
    // ctx carries the traceID from the publisher; create child spans here.
    _, span := tracer.Start(ctx, "process order")
    defer span.End()
    // ... handle msg
})

// Queue subscribe (load-balanced consumer group)
conn.QueueSubscribe("orders.created", "processors",
    func(ctx context.Context, msg *nats.Msg) { /* ... */ })
```

---

## Span attributes (OTLP Messaging Semconv v1.27.0)

| Attribute | Value |
|---|---|
| `messaging.system` | `nats` |
| `messaging.destination.name` | NATS subject |
| `messaging.operation.type` | `publish` (producer) / `receive` (consumer) |
| `messaging.operation.name` | `publish` / `receive` |
| `messaging.message.body.size` | byte length of `msg.Data` |
| `messaging.message.conversation_id` | reply subject (if set) |
| `messaging.consumer.group.name` | queue group name (queue subscriptions only) |

---

## Options

| Option | Default | Description |
|---|---|---|
| `WithTracerProvider(tp)` | `otel.GetTracerProvider()` | Custom TracerProvider |
| `WithPropagator(p)` | `otel.GetTextMapPropagator()` | Custom TextMapPropagator |
