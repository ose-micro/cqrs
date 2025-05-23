package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/ose-micro/cqrs/bus"
)

type natsBus[C any] struct {
	nc       *nats.Conn
}

// Subscribe implements ose_cqrs.Bus.
func (n *natsBus[C]) Subscribe(subject string, handler func(ctx context.Context, cmd C) error)  (*nats.Subscription, error) {
    return n.nc.Subscribe(subject, func(msg *nats.Msg) {
		var cmd C
		if err := json.Unmarshal(msg.Data, &cmd); err != nil {
			fmt.Printf("unmarshal error: %v\n", err)
			return
		}

		_ = handler(context.Background(), cmd)
    })
}

// Publish implements ose_cqrs.Bus.
func (n *natsBus[C]) Publish(ctx context.Context, subject string, cmd C) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return n.nc.Publish(subject, data)
}

func NewNatsBus[C any](nc *nats.Conn) bus.Bus[C] {
	return &natsBus[C]{
		nc:       nc,
	}
}
