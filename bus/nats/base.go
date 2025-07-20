package nats

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/core/tracing"
	"github.com/ose-micro/cqrs/bus"
)

type natsBus struct {
	nc     *nats.Conn
	log    logger.Logger
	tracer tracing.Tracer
}

func New(conf Config, log logger.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		log.Error("Failed to connect to NATS", "error", err)
		return nil, err
	}

	log.Info("Connected to NATS", "url", conf.URL)

	return &natsBus{
		nc:     nc,
		log:    log,
		tracer: tracer,
	}, nil
}

func (n *natsBus) Publish(subject string, data any) error {
	_, span := n.tracer.Start(context.Background(), "nats.Publish")
	defer span.End()

	body, err := json.Marshal(data)
	if err != nil {
		n.log.Error("Failed to marshal message", "error", err)
		return err
	}

	err = n.nc.Publish(subject, body)
	if err != nil {
		n.log.Error("Failed to publish message", "subject", subject, "error", err)
	} else {
		n.log.Info("Published message", "subject", subject)
	}

	return err
}

func (n *natsBus) Subscribe(subject, queue string, handler func(ctx context.Context, data any) error) error {
	ready := make(chan struct{})

	_, err := n.nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		ctx := context.Background()
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			n.log.Error("Invalid message format", "raw", string(msg.Data))
			return
		}

		if err := handler(ctx, payload); err != nil {
			n.log.Error("Handler error", "error", err)
		}
	})
	if err != nil {
		n.log.Error("Failed to subscribe", "subject", subject, "queue", queue, "error", err)
		return err
	}

	n.log.Info("Subscribed to subject", "subject", subject, "queue", queue)
	close(ready)
	return nil
}

func (n *natsBus) Close() error {
	return n.nc.Drain()
}
