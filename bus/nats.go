package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/ose-micro/core/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type natsBus struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	log logger.Logger
}

// Publish implements Bus.
func (n *natsBus) Publish(subject string, data any) error {
	ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "ose-cqrs.nats.publish",
		trace.WithAttributes(
			attribute.String("operation", "PUBLISH"),
			attribute.String("subject", subject),
		),
	)
	defer span.End()

	traceID := trace.SpanContextFromContext(ctx).TraceID().String()

	payload, err := json.Marshal(data)
	if err != nil {
		span.RecordError(err)
		n.log.Error("failed to marshal event",
			zap.String("trace_id", traceID),
			zap.String("subject", subject),
			zap.Error(err),
		)
		return err
	}

	_, err = n.js.Publish(subject, payload)
	if err != nil {
		span.RecordError(err)
		n.log.Error("failed to publish event",
			zap.String("trace_id", traceID),
			zap.String("subject", subject),
			zap.Error(err),
		)
		return err
	}

	return nil
}

// Subscribe implements Bus.
func (n *natsBus) Subscribe(subject string, stream string, durable string, queue string, handler func(ctx context.Context, data any) error) error {
	if err := n.EnsureStream(stream, []string{subject}); err != nil {
		return err
	}

	_, err := n.js.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "ose-cqrs.nats.jetstream_subscribe")
		defer span.End()

		span.SetAttributes(
			attribute.String("subject", subject),
			attribute.String("stream", stream),
			attribute.String("durable", durable),
			attribute.String("queue", queue),
		)

		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			span.RecordError(err)
			n.log.Error("failed to unmarshal JetStream message", zap.Error(err))
			msg.Nak()
			return
		}

		if err := handler(ctx, payload); err != nil {
			span.RecordError(err)
			n.log.Error("handler error in JetStream subscription", zap.Error(err))
			msg.Nak()
			return
		}

		msg.Ack()
	},
		nats.Durable(durable),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverAll(),
	)
	return err
}

// EnsureStream creates a JetStream stream if it doesn't exist.
func (n *natsBus) EnsureStream(stream string, subjects []string) error {
	_, err := n.js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  subjects,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		n.log.Error("failed to create JetStream stream", zap.Error(err))
		return err
	}
	return nil
}

// NewNats creates a new NATS bus implementation.
func NewNats(nc *nats.Conn, log logger.Logger) (Bus, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}
	
	return &natsBus{
		nc:  nc,
		js:  js,
		log: log,
	}, nil
}
