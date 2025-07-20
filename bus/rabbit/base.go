package rabbit

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/core/tracing"
	"github.com/ose-micro/cqrs/bus"
	"github.com/streadway/amqp"
)

type rabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
	log     logger.Logger
	tracer  tracing.Tracer
}

// Publish implements bus.Bus.
func (r *rabbitMQ) Publish(subject string, data any) error {
	_, span := r.tracer.Start(context.Background(), "rabbitmq.Publish")
	defer span.End()

	r.log.Debug("Publishing message",
		"subject", subject,
		"exchange", r.config.Exchange,
	)

	body, err := json.Marshal(data)
	if err != nil {
		r.log.Error("Failed to marshal publish payload", "error", err)
		return err
	}

	err = r.channel.Publish(
		r.config.Exchange,
		subject,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Timestamp:   time.Now(),
			Body:        body,
		},
	)

	if err != nil {
		r.log.Error("Failed to publish message", "subject", subject, "error", err)
	} else {
		r.log.Info("Published message", "subject", subject)
	}

	return err
}

// Subscribe implements bus.Bus with ready signal and reliability improvements.
func (r *rabbitMQ) Subscribe(subject, queue string, handler func(ctx context.Context, data any) error)  error {
	ready := make(chan struct{})

	q, err := r.channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to declare queue", "error", err)
		return err
	}

	err = r.channel.QueueBind(
		q.Name,
		subject,
		r.config.Exchange,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to bind queue", "error", err)
		return err
	}

	msgs, err := r.channel.Consume(
		q.Name,
		"",   // consumer tag
		true, // auto-ack for now
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to consume messages", "error", err)
		return err
	}

	go func() {
		r.log.Info("Consumer ready", "queue", q.Name, "routingKey", subject)
		close(ready)

		for d := range msgs {
			ctx := context.Background()

			var payload map[string]interface{}
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				r.log.Error("Handler failed", "error", "invalid message format", "raw", string(d.Body))
				continue
			}

			if err := handler(ctx, payload); err != nil {
				r.log.Error("Handler error", "error", err)
			}
		}
	}()

	return nil
}

// New creates a RabbitMQ-backed bus.Bus.
func New(cfg *Config, log logger.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	log.Info("Connecting to RabbitMQ", "url", cfg.URL)

	var conn *amqp.Connection
	var err error
	for {
		conn, err = amqp.Dial(cfg.URL)
		if err == nil {
			break
		}
		log.Warn("Retrying RabbitMQ connection", "backoff", cfg.ReconnectBackoff, "error", err)
		time.Sleep(time.Duration(cfg.ReconnectBackoff) * time.Second)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Error("Failed to open channel", "error", err)
		return nil, err
	}

	err = ch.ExchangeDeclare(
		cfg.Exchange,
		cfg.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error("Failed to declare exchange", "exchange", cfg.Exchange, "error", err)
		return nil, err
	}

	log.Info("RabbitMQ ready", "exchange", cfg.Exchange)

	return &rabbitMQ{
		conn:    conn,
		channel: ch,
		config:  cfg,
		log:     log,
		tracer:  tracer,
	}, nil
}
