package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ose-micro/core/tracing"
	"github.com/ose-micro/cqrs/bus"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Config struct {
	Url      string `mapstructure:"url"`
	Exchange string `mapstructure:"exchange"`
}

type rabbitBus struct {
	conn    *amqp091.Connection
	channel *amqp091.Channel
	log     *zap.Logger
	tracer  tracing.Tracer
}

func NewBus(conf Config, log *zap.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	conn, err := amqp091.Dial(conf.Url)
	if err != nil {
		log.Error("Failed to dial RabbitMQ", zap.String("url", conf.Url), zap.Error(err))
		return nil, fmt.Errorf("dial error: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Error("Failed to create RabbitMQ channel", zap.Error(err))
		return nil, fmt.Errorf("channel error: %w", err)
	}

	log.Info("Connected to RabbitMQ", zap.String("url", conf.Url))

	return &rabbitBus{
		conn:    conn,
		channel: ch,
		log:     log,
		tracer:  tracer,
	}, nil
}

func (r *rabbitBus) Publish(subject string, data any) error {
	ctx, span := r.tracer.Start(context.Background(), "rabbitmq.publish")
	defer span.End()

	body, err := json.Marshal(data)
	if err != nil {
		r.log.Error("Failed to marshal message", zap.String("subject", subject), zap.Error(err))
		return fmt.Errorf("marshal error: %w", err)
	}

	_, err = r.channel.QueueDeclare(
		subject,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to declare queue", zap.String("subject", subject), zap.Error(err))
		return fmt.Errorf("queue declare error: %w", err)
	}

	err = r.channel.PublishWithContext(ctx,
		"",
		subject,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
			DeliveryMode: amqp091.Persistent,
		},
	)
	if err != nil {
		r.log.Error("Failed to publish message", zap.String("subject", subject), zap.Error(err))
		return fmt.Errorf("publish error: %w", err)
	}

	r.log.Info("Published message", zap.String("subject", subject))
	return nil
}

func (r *rabbitBus) Subscribe(subject, queue string, handler func(ctx context.Context, data any) error) error {
	_, err := r.channel.QueueDeclare(
		subject,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to declare queue", zap.String("subject", subject), zap.Error(err))
		return fmt.Errorf("queue declare error: %w", err)
	}

	msgs, err := r.channel.Consume(
		subject,
		queue,
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to start consumer", zap.String("subject", subject), zap.String("queue", queue), zap.Error(err))
		return fmt.Errorf("consume error: %w", err)
	}

	go func() {
		r.log.Info("Started RabbitMQ consumer", zap.String("subject", subject), zap.String("queue", queue))

		for d := range msgs {
			ctx, span := r.tracer.Start(context.Background(), "rabbitmq.handle")

			var payload map[string]any
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				r.log.Error("Failed to unmarshal message", zap.String("subject", subject), zap.Error(err))
				_ = d.Nack(false, false) // drop
				span.End()
				continue
			}

			if err := handler(ctx, payload); err != nil {
				r.log.Error("Handler error", zap.String("subject", subject), zap.Error(err))
				_ = d.Nack(false, true) // retry
				span.End()
				continue
			}

			_ = d.Ack(false)
			r.log.Info("Handled and acked message", zap.String("subject", subject))
			span.End()
		}
	}()

	return nil
}

func (r *rabbitBus) Close() error {
	if r.channel != nil {
		if err := r.channel.Close(); err == nil {
			r.log.Info("Closed RabbitMQ channel")
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err == nil {
			r.log.Info("Closed RabbitMQ connection")
		}
	}
	return nil
}
