// Package natstrace provides OpenTelemetry tracing for the NATS messaging client.
// It mirrors the API of github.com/nats-io/nats.go: Connect, Conn, Publish, Subscribe, etc.
//
// The only differences from the official client:
//   - Publish and PublishMsg accept context.Context as the first argument (for trace propagation).
//   - Message handlers (Subscribe, QueueSubscribe) receive func(ctx context.Context, msg *nats.Msg);
//     the context carries the trace extracted from the message headers.
//
// Use Connect() to obtain a *Conn, then use it like *nats.Conn. For JetStream with tracing,
// use the jetstreamtrace package: jetstreamtrace.New(conn).
package natstrace
