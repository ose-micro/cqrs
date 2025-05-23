package nats

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/cqrs"
	"github.com/ose-micro/cqrs/bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type natsBus[cmd any] struct {
	nc  *nats.Conn
	log logger.Logger
}

// Subscribe implements ose_cqrs.Bus.
func (n *natsBus[C]) Subscribe(topic string, handler func(ctx context.Context, cmd C) error) (*nats.Subscription, error) {

	return n.nc.Subscribe(topic, func(msg *nats.Msg) {
		_, span := otel.Tracer("ose-micro-cqrs").Start(context.Background(), "Bus.Subscribe")
		span.SetAttributes(attribute.String("topic", topic))
		defer span.End()

		var cmd C
		if err := json.Unmarshal(msg.Data, &cmd); err != nil {
			n.log.Fatal("unmarshal error: %v\n", err)
		}

		_ = handler(context.Background(), cmd)
	})
}

// Publish implements ose_cqrs.Bus.
func (n *natsBus[C]) Publish(ctx context.Context, topic string, cmd C) error {
	tracer := otel.Tracer("ose-micro-cqrs")
	_, span := tracer.Start(ctx, "Bus.Publish")
	span.SetAttributes(attribute.String("topic", topic))
	defer span.End()

	data, err := json.Marshal(cmd)
	if err != nil {
		n.log.Error("Failed to publish command", "cmd", topic, "error", err)
		span.RecordError(err)
		return err
	}
	return n.nc.Publish(topic, data)
}

func NewNatsBus[C cqrs.Command](nc *nats.Conn, log logger.Logger) bus.Bus[C] {
	return &natsBus[C]{
		nc: nc,
	}
}
