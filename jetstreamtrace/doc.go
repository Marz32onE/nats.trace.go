// Package jetstreamtrace provides OpenTelemetry tracing for NATS JetStream.
// It mirrors the API of github.com/nats-io/nats.go/jetstream: New, JetStream, Stream, Consumer.
//
// Usage aligns with the official package:
//   - New(conn) takes a *natstrace.Conn so that trace is propagated.
//   - Publish and PublishMsg accept context.Context (same as official).
//   - Consume(handler): handler is func(ctx context.Context, msg Msg); ctx carries trace extracted from message headers.
//   - Messages(): Next() returns (ctx, msg, error) with ctx carrying extracted trace.
//   - Next(): returns (ctx, msg, error) for a single message.
//
// Async publish APIs (PublishAsync, PublishMsgAsync) are not provided.
// Batch pull (Fetch, FetchBytes, FetchNoWait) are not wrapped for per-message trace and are skipped.
package jetstreamtrace
