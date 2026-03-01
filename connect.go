package natstracing

import (
	nats "github.com/nats-io/nats.go"
)

// Connect establishes a NATS connection and wraps it with OpenTelemetry
// tracing instrumentation. It accepts the standard nats.Connect URL and any
// number of nats.Option values, plus the tracing-specific Option values
// (WithTracerProvider, WithPropagator).
//
// The url parameter supports the same formats as nats.Connect (e.g.
// "nats://localhost:4222", "nats://user:pass@host:4222", cluster seeds …).
func Connect(url string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}

// ConnectTLS establishes a NATS connection secured with mutual TLS and wraps
// it with OpenTelemetry tracing instrumentation.
//
// Parameters:
//   - url       – NATS server URL (e.g. "nats://localhost:4222").
//   - certFile  – path to the PEM-encoded client certificate.
//   - keyFile   – path to the PEM-encoded private key for the client certificate.
//   - caFile    – path to the PEM-encoded CA certificate; pass an empty string
//     to rely on the system certificate pool.
//   - natsOpts  – additional nats.Option values (applied before the TLS option).
//   - tracingOpts – functional options that configure the tracer (see Option).
func ConnectTLS(url, certFile, keyFile, caFile string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	tlsOpt := nats.ClientCert(certFile, keyFile)
	opts := append(natsOpts, tlsOpt)
	if caFile != "" {
		opts = append(opts, nats.RootCAs(caFile))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}

// ConnectWithCredentials establishes a NATS connection authenticated via a
// credentials file (NKey + JWT) and wraps it with OpenTelemetry tracing
// instrumentation.
//
// Parameters:
//   - url        – NATS server URL.
//   - credFile   – path to the .creds file produced by nsc.
//   - natsOpts   – additional nats.Option values (applied before the credentials option).
//   - tracingOpts – functional options that configure the tracer (see Option).
func ConnectWithCredentials(url, credFile string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	opts := append(natsOpts, nats.UserCredentials(credFile))
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}
