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
		r.config.Exchange, // exchange
		subject,           // routing key
		false,             // mandatory
		false,             // immediate
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

// Subscribe implements bus.Bus.
func (r *rabbitMQ) Subscribe(subject, stream, durable, queue string, handler func(ctx context.Context, data any) error) error {
	qName := queue
	if qName == "" {
		qName = durable
	}
	r.log.Info("Setting up consumer",
		"subject", subject,
		"queue", qName,
		"exchange", r.config.Exchange,
	)

	_, err := r.channel.QueueDeclare(
		qName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		r.log.Error("Failed to declare queue", "queue", qName, "error", err)
		return err
	}

	err = r.channel.QueueBind(
		qName,
		subject,
		r.config.Exchange,
		false,
		nil,
	)
	if err != nil {
		r.log.Error("Failed to bind queue", "queue", qName, "subject", subject, "error", err)
		return err
	}

	deliveries, err := r.channel.Consume(
		qName,
		"",    // consumer tag
		false, // autoAck
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		r.log.Error("Failed to consume queue", "queue", qName, "error", err)
		return err
	}

	go func() {
		for msg := range deliveries {
			ctx, span := r.tracer.Start(context.Background(), "rabbitmq.Consume")
			r.log.Debug("Received message", "subject", subject, "queue", qName)

			err := handler(ctx, msg.Body)
			if err != nil {
				r.log.Error("Handler failed", "error", err)
				_ = msg.Nack(false, true)
				span.End()
				continue
			}

			_ = msg.Ack(false)
			span.End()
		}
		r.log.Warn("Delivery channel closed", "queue", qName)
	}()

	r.log.Info("Consumer registered", "subject", subject, "queue", qName)
	return nil
}

// New creates a RabbitMQ-backed bus.Bus.
func New(cfg *Config, log logger.Logger, tracer tracing.Tracer) (bus.Bus, error) {
	var conn *amqp.Connection
	var err error

	log.Info("Connecting to RabbitMQ", "url", cfg.URL)

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
