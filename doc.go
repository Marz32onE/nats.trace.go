// Package natstrace provides OpenTelemetry tracing instrumentation for the NATS
// messaging system. It wraps the nats.io/nats.go client and propagates W3C
// TraceContext via NATS message headers so that producer and consumer spans
// are linked across the message boundary.
//
// Use Connect (like nats.Connect) to get a *Conn. Conn mirrors nats.Conn with
// context-aware Publish, PublishMsg, Subscribe, QueueSubscribe; use NatsConn()
// for the underlying *nats.Conn when needed. PublishJetStream and SubscribeJetStream
// add JetStream support with the same tracing semantics.
package natstrace
