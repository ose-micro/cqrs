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
	ctx := context.Background()
	_, span := n.tracer.Start(ctx, "nats.Publish")
	defer span.End()

	body, err := json.Marshal(data)
	if err != nil {
		n.log.Error("Failed to marshal message", "error", err)
		return err
	}

	err = n.nc.Publish(subject, body)
	if err != nil {
		n.log.Error("Failed to publish message", "subject", subject, "error", err)
		return err
	}

	if flushErr := n.nc.Flush(); flushErr != nil {
		n.log.Error("Failed to flush NATS publish", "error", flushErr)
		return flushErr
	}

	if lastErr := n.nc.LastError(); lastErr != nil {
		n.log.Error("NATS connection error", "error", lastErr)
		return lastErr
	}

	n.log.Info("Published message", "subject", subject)
	return nil
}

func (n *natsBus) Subscribe(subject, queue string, handler func(ctx context.Context, data any) error) error {
	sub, err := n.nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		// Run each handler in its own goroutine to avoid blocking NATS
		go func() {
			ctx := context.Background()
			var payload map[string]interface{}

			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				n.log.Error("Invalid message format", "raw", string(msg.Data))
				return
			}

			if err := handler(ctx, payload); err != nil {
				n.log.Error("Handler error", "subject", subject, "error", err)
			}
		}()
	})
	if err != nil {
		n.log.Error("Failed to subscribe", "subject", subject, "queue", queue, "error", err)
		return err
	}

	// Ensure subscription is flushed to server
	if err := n.nc.Flush(); err != nil {
		n.log.Error("NATS flush failed", "error", err)
		return err
	}

	if lastErr := n.nc.LastError(); lastErr != nil {
		n.log.Error("NATS connection error after subscribe", "error", lastErr)
		return lastErr
	}

	n.log.Info("Subscribed to subject", "subject", subject, "queue", queue)

	// Prevent garbage collection of the subscription
	_ = sub

	return nil
}

func (n *natsBus) Close() error {
	n.log.Info("Draining NATS connection...")
	if err := n.nc.Drain(); err != nil {
		n.log.Error("Failed to drain NATS", "error", err)
		n.nc.Close()
		return err
	}
	n.log.Info("NATS connection drained successfully")
	n.nc.Close()
	return nil
}
