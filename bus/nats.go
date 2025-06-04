package bus

import (
	"context"
	"encoding/json"
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
	log logger.Logger
}

// Publish implements bus.Bus.
func (n *natsBus) Publish(subject string, data any) error {
	ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "ose-cqrs.nats.publish", trace.WithAttributes(
		attribute.String("operation", "PUBlISH"),
		attribute.String("payload", fmt.Sprintf("%v", data)),
	))
	defer span.End()

	traceId := trace.SpanContextFromContext(ctx).TraceID().String()
	span.SetAttributes(attribute.String("subject", subject))

	payload, err := json.Marshal(data)
	if err != nil {
		span.RecordError(err)
		return err
	}

	err = n.nc.Publish(subject, payload)
	if err != nil {
		span.RecordError(err)
		n.log.Error("failed to publish event",
			zap.String("trace_id", traceId),
			zap.String("operation", "PUBlISH"),
			zap.Error(err),
		)
	}
	return err
}

// Subscribe implements bus.Bus.
func (n *natsBus) Subscribe(subject string, handler func(ctx context.Context, data any) error) (*nats.Subscription, error) {
	return n.nc.Subscribe(subject, func(msg *nats.Msg) {
		ctx, span := otel.Tracer("ose-cqrs").Start(context.Background(), "Bus.Subscribe")
		defer span.End()
		span.SetAttributes(attribute.String("subject", subject))

		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			span.RecordError(err)
			return
		}

		if err := handler(ctx, payload); err != nil {
			span.RecordError(err)
		}
	})
}

func NewNats(nc *nats.Conn, log logger.Logger) Bus {
	return &natsBus{
		nc:  nc,
		log: log,
	}
}
