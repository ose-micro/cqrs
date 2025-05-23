package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ose-micro/core/logger"
	"github.com/ose-micro/cqrs"
	"github.com/stretchr/testify/assert"
)

type SendEmailCommand struct {
	To   string `mapstructure:"to"`
	Body string `mapstructure:"body"`
}

// CommandName implements cqrs.Command.
func (s SendEmailCommand) CommandName() string {
	return "SendEmailCommand"
}

// Validate implements cqrs.Command.
func (s SendEmailCommand) Validate() error {
	return nil
}

var _ cqrs.Command = SendEmailCommand{}

const SUBJECT string = "fundme.account.created"

func TestNatsCreateBuss(t *testing.T) {
	nt, err := New(Config{
		Address: "nats://localhost:4222",
	})
	assert.Nil(t, err)
	log, err := logger.NewZap(logger.Config{
		Environment: "",
		Level:       "info",
	})

	assert.Nil(t, err)

	cmd := NewNatsBus[SendEmailCommand](nt, log)

	var wg sync.WaitGroup
	wg.Add(1)

	cmd.Subscribe(SUBJECT, func(ctx context.Context, cmd SendEmailCommand) error {
		log.Info(cmd.Body)
		wg.Done()
		return nil
	})
	
	// Give a tiny moment for the subscriber to be set up
	time.Sleep(300 * time.Millisecond)

	err = cmd.Publish(context.Background(), SUBJECT, SendEmailCommand{
		To:   "hello@gudtok.com",
		Body: "Thanks for subscribing!",
	})
	assert.Nil(t, err)

	// Wait until the handler has processed the command
	wg.Wait()
}
