package bus

import (
	"context"

	"github.com/nats-io/nats.go"
)

type Bus[C any] interface {
	Publish(ctx context.Context, subject string, cmd C) error
	Subscribe(subject string, handler func(ctx context.Context, cmd C) error) (*nats.Subscription, error)
}
