package bus

import (
	"context"

	"github.com/nats-io/nats.go"
)


type Config struct {
	Address string `mapstructure:"address"`
}

type Bus interface {
	Publish(subject string, data any) error
	Subscribe(subject, stream, durable, queue string, handler func(ctx context.Context, data any) error) error
	EnsureStream(stream string, subjects []string) error
}



func New(conf Config) (*nats.Conn, error) {
	return nats.Connect(conf.Address)
}
