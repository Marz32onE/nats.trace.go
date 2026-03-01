package natstrace

import (
	nats "github.com/nats-io/nats.go"
)

// Connect establishes a NATS connection and wraps it with OpenTelemetry
// tracing. Signature matches nats.Connect(url, opts...); pass nats options
// as natsOpts and tracing options as tracingOpts.
func Connect(url string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}

// ConnectTLS establishes a TLS connection and wraps it with tracing.
func ConnectTLS(url, certFile, keyFile, caFile string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	opts := append(natsOpts, nats.ClientCert(certFile, keyFile))
	if caFile != "" {
		opts = append(opts, nats.RootCAs(caFile))
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}

// ConnectWithCredentials connects using a credentials file (NKey + JWT) and wraps with tracing.
func ConnectWithCredentials(url, credFile string, natsOpts []nats.Option, tracingOpts ...Option) (*Conn, error) {
	opts := append(natsOpts, nats.UserCredentials(credFile))
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc, tracingOpts...), nil
}
