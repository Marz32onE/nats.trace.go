package natstrace

import (
	"errors"

	nats "github.com/nats-io/nats.go"
)

// ErrInitTracerRequired is returned when Connect is called before InitTracer.
var ErrInitTracerRequired = errors.New("natstrace: InitTracer must be called before Connect")

// Connect establishes a NATS connection with tracing. Signature aligns with nats.Connect.
// InitTracer must be called first or Connect returns ErrInitTracerRequired.
func Connect(url string, natsOpts []nats.Option) (*Conn, error) {
	if !isTracerInitialized() {
		if err := InitTracer("", nil); err != nil {
			return nil, err
		}
	}
	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc), nil
}

// ConnectTLS establishes a TLS connection with tracing.
func ConnectTLS(url, certFile, keyFile, caFile string, natsOpts []nats.Option) (*Conn, error) {
	if !isTracerInitialized() {
		return nil, ErrInitTracerRequired
	}
	opts := append(natsOpts, nats.ClientCert(certFile, keyFile))
	if caFile != "" {
		opts = append(opts, nats.RootCAs(caFile))
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc), nil
}

// ConnectWithCredentials connects using a credentials file, with tracing.
func ConnectWithCredentials(url, credFile string, natsOpts []nats.Option) (*Conn, error) {
	if !isTracerInitialized() {
		return nil, ErrInitTracerRequired
	}
	opts := append(natsOpts, nats.UserCredentials(credFile))
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return newConn(nc), nil
}
