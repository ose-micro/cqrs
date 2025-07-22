package bus

import (
	"context"
)

type Bus interface {
	Publish(subject string, data any) error
	Subscribe(subject, queue string, handler func(ctx context.Context, data any) error) error
	Close() error
}

