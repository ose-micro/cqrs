package nats

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)


type SendEmailCommand struct {
    To   string
    Body string
}

const SUBJECT string = "fundme.account.created";

func TestNatsCreateBuss(t *testing.T) {
	nt, err := New(Config{
		Address: "nats://localhost:4222",
	})
	assert.Nil(t, err)

	cmd := NewNatsBus[SendEmailCommand](nt)

	var wg sync.WaitGroup
	wg.Add(1)

	cmd.Subscribe(SUBJECT, func(ctx context.Context, cmd SendEmailCommand) error {
		log.Printf("Sending mail to: %s\n", cmd.To)
		wg.Done()
		return nil
	})
	cmd.Subscribe(SUBJECT, func(ctx context.Context, cmd SendEmailCommand) error {
		log.Printf("Sending read to: %s\n", cmd.To)
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