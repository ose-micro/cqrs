package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/core/tracing"
	"github.com/ose-micro/cqrs/bus/rabbit"
)

func main() {
	// Logger setup
	log, _ := logger.NewZap(logger.Config{})

	// Tracer setup
	tracer, _ := tracing.NewOtel(tracing.Config{
		Endpoint:    "localhost:4317",
		ServiceName: "redis",
		SampleRatio: 1.0,
	}, log)

	// RabbitMQ config
	cfg := &rabbit.Config{
		URL:              "amqp://cYM0obPvrdpPyP0Z:Jhnpr6PoovjOBktcIIkcgJFAbkKIGU2Q@maglev.proxy.rlwy.net:13140", // e.g. amqp://guest:guest@localhost:5672/
		Exchange:         "ose.exchange",
		ExchangeType:     "topic",
		ReconnectBackoff: 3,
	}

	// Init bus
	rmqBus, err := rabbit.New(cfg, log, tracer)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ", "error", err)
	}

	// Subscribe to event
	err = rmqBus.Subscribe("event.user.created", "", "user-service", "", func(ctx context.Context, data any) error {
		log.Info("📥 Received user.created event", "data", string(data.([]byte)))
		return nil
	})
	if err != nil {
		log.Fatal("Subscribe failed", "error", err)
	}

	// Publish a test message
	go func() {
		time.Sleep(2 * time.Second) // wait for subscriber to be ready
		err := rmqBus.Publish("event.user.created", map[string]string{
			"id":   "user-123",
			"name": "Dev Isho",
		})
		if err != nil {
			log.Error("Publish failed", "error", err)
		} else {
			log.Info("📤 Published event.user.created")
		}
	}()

	// Wait until CTRL+C
	log.Info("RabbitMQ example running. Press Ctrl+C to exit...")
	waitForShutdown()
}

func waitForShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
