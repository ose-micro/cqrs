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
	Subscribe(subject string, handler func(ctx context.Context, data any) error) (*nats.Subscription, error)
	SubscribeWithQueue(subject, queue string, handler func(ctx context.Context, data any) error) (*nats.Subscription, error)
}



func New(conf Config) (*nats.Conn, error) {
	return nats.Connect(conf.Address)
}
