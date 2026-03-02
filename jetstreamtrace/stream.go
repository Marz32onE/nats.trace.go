package jetstreamtrace

import (
	"context"

	"github.com/Marz32onE/nats.trace.go/natstrace"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream mirrors jetstream.Stream for managing consumers with tracing.
type Stream interface {
	Consumer(ctx context.Context, name string) (Consumer, error)
	CreateOrUpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error)
	DeleteConsumer(ctx context.Context, name string) error
}

type streamImpl struct {
	conn       *natstrace.Conn
	streamName string
	s          jetstream.Stream
}

func (s *streamImpl) Consumer(ctx context.Context, name string) (Consumer, error) {
	cons, err := s.s.Consumer(ctx, name)
	if err != nil {
		return nil, err
	}
	return &consumerImpl{conn: s.conn, streamName: s.streamName, consumerName: name, c: cons}, nil
}

func (s *streamImpl) CreateOrUpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	cons, err := s.s.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	name := cfg.Durable
	if name == "" && cfg.Name != "" {
		name = cfg.Name
	}
	if name == "" {
		name = "consumer"
	}
	return &consumerImpl{conn: s.conn, streamName: s.streamName, consumerName: name, c: cons}, nil
}

func (s *streamImpl) DeleteConsumer(ctx context.Context, name string) error {
	return s.s.DeleteConsumer(ctx, name)
}
