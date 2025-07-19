package bus

import (
	"context"
)

type Bus interface {
	Publish(subject string, data any) error
	Subscribe(subject, stream, durable, queue string, handler func(ctx context.Context, data any) error) error
}

