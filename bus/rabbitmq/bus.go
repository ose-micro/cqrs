package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/core/tracing"
	"github.com/ose-micro/cqrs/bus"
	"github.com/streadway/amqp"
)

type rabbitBus struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	log     logger.Logger
	tracer  tracing.Tracer
}

func New(url string, log logger.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Error("failed to dial RabbitMQ", "error", err)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Error("failed to open channel", "error", err)
		conn.Close()
		return nil, err
	}

	return &rabbitBus{
		conn:    conn,
		channel: ch,
		log:     log,
		tracer:  tracer,
	}, nil
}

func (r *rabbitBus) Publish(subject string, data any) error {
	ctx := context.Background()
	_, span := r.tracer.Start(ctx, "rabbitmq.Publish")
	defer span.End()

	body, err := json.Marshal(data)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal: %w", err)
	}

	if err := r.channel.ExchangeDeclare(subject, "fanout", true, false, false, false, nil); err != nil {
		span.RecordError(err)
		return fmt.Errorf("declare exchange: %w", err)
	}

	err = r.channel.Publish(
		subject, "", false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("publish: %w", err)
	}

	r.log.Debug("Published message", "subject", subject)
	return nil
}

func (r *rabbitBus) Subscribe(subject, queue string, handler func(ctx context.Context, data any) error) error {
	if queue == "" {
		queue = fmt.Sprintf("%s.queue", subject)
	}
	consumerTag := fmt.Sprintf("%s-%d", queue, time.Now().UnixNano())

	if err := r.channel.ExchangeDeclare(subject, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	q, err := r.channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if err := r.channel.QueueBind(q.Name, "", subject, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	msgs, err := r.channel.Consume(q.Name, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	go func() {
		for d := range msgs {
			ctx := context.Background()
			ctx, span := r.tracer.Start(ctx, "rabbitmq.Subscribe")

			var payload any
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				r.log.Error("unmarshal error", "err", err)
				span.RecordError(err)
				d.Nack(false, false)
				span.End()
				continue
			}

			if err := handler(ctx, payload); err != nil {
				r.log.Error("handler error", "err", err)
				span.RecordError(err)
				d.Nack(false, true)
				span.End()
				continue
			}

			d.Ack(false)
			span.End()
		}

		r.log.Warn("consumer closed", "queue", queue)
	}()

	r.log.Info("subscription started", "subject", subject, "queue", queue, "tag", consumerTag)
	return nil
}

func (r *rabbitBus) Close() error {
	if err := r.channel.Close(); err != nil {
		r.log.Warn("failed to close channel", "error", err)
	}
	if err := r.conn.Close(); err != nil {
		r.log.Warn("failed to close connection", "error", err)
	}
	return nil
}
