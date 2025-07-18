package bus

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/ose-micro/core/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type natsBus struct {
	nc  *nats.Conn
	log logger.Logger
}

// Publish sends a message to the given subject.
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

	if err := n.nc.Publish(subject, payload); err != nil {
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

// SubscribeWithQueue ensures only one consumer in a group receives a message.
func (n *natsBus) SubscribeWithQueue(subject, queue string, handler func(ctx context.Context, data any) error) (*nats.Subscription, error) {
	return n.nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "ose-cqrs.nats.queue_subscribe")
		defer span.End()

		span.SetAttributes(attribute.String("subject", subject), attribute.String("queue_group", queue))

		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			span.RecordError(err)
			n.log.Error("failed to unmarshal NATS message",
				zap.String("subject", subject),
				zap.Error(err),
			)
			return
		}

		if err := handler(ctx, payload); err != nil {
			span.RecordError(err)
			n.log.Error("handler error in queue subscription",
				zap.String("subject", subject),
				zap.Error(err),
			)
		}
	})
}

// SubscribeSimple broadcasts to all subscribers (no queue group).
func (n *natsBus) Subscribe(subject string, handler func(ctx context.Context, data any) error) (*nats.Subscription, error) {
	return n.nc.Subscribe(subject, func(msg *nats.Msg) {
		ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "ose-cqrs.nats.broadcast_subscribe")
		defer span.End()

		span.SetAttributes(attribute.String("subject", subject))

		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			span.RecordError(err)
			n.log.Error("failed to unmarshal NATS message",
				zap.String("subject", subject),
				zap.Error(err),
			)
			return
		}

		if err := handler(ctx, payload); err != nil {
			span.RecordError(err)
			n.log.Error("handler error in broadcast subscription",
				zap.String("subject", subject),
				zap.Error(err),
			)
		}
	})
}

// NewNats creates a new NATS bus implementation.
func NewNats(nc *nats.Conn, log logger.Logger) Bus {
	return &natsBus{
		nc:  nc,
		log: log,
	}
}
