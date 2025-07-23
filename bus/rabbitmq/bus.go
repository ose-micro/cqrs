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

type Config struct {
	URL string `mapstructure:"url"`
}

type rabbitBus struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	log     logger.Logger
	tracer  tracing.Tracer
}

func New(conf Config, log logger.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	conn, err := amqp.Dial(conf.URL)
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
    // Let RabbitMQ generate a unique queue name if empty
    if queue == "" {
        queue = "" // empty means auto-generated queue name
    }
    consumerTag := fmt.Sprintf("consumer-%d", time.Now().UnixNano())

    // Declare the fanout exchange (broadcast)
    if err := r.channel.ExchangeDeclare(subject, "fanout", true, false, false, false, nil); err != nil {
        return fmt.Errorf("declare exchange: %w", err)
    }

    // Declare the queue (empty queue name means server generates a unique name)
    q, err := r.channel.QueueDeclare(
        queue,
        true,  // durable (set to false if you want auto-delete when subscriber disconnects)
        false, // autoDelete (true if queue should be deleted when no longer used)
        false, // exclusive (true if only this connection can use the queue)
        false, // noWait
        nil,   // args
    )
    if err != nil {
        return fmt.Errorf("declare queue: %w", err)
    }

    // Bind queue to exchange (routing key is ignored for fanout)
    if err := r.channel.QueueBind(q.Name, "", subject, false, nil); err != nil {
        return fmt.Errorf("bind queue: %w", err)
    }

    msgs, err := r.channel.Consume(
        q.Name,
        consumerTag,
        false, // manual ack
        false, // exclusive
        false, // noLocal (not supported by RabbitMQ)
        false, // noWait
        nil,   // args
    )
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
                _ = d.Nack(false, false) // discard message
                span.End()
                continue
            }

            if err := handler(ctx, payload); err != nil {
                r.log.Error("handler error", "err", err)
                span.RecordError(err)
                _ = d.Nack(false, true) // requeue for retry
                span.End()
                continue
            }

            _ = d.Ack(false)
            span.End()
        }

        r.log.Warn("consumer closed", "queue", q.Name)
    }()

    r.log.Info("subscription started", "subject", subject, "queue", q.Name, "tag", consumerTag)
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
