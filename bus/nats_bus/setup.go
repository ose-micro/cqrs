package nats_bus

import (
	"github.com/nats-io/nats.go"
)

type Config struct {
	Address string `mapstructure:"address"`
}

func New(conf Config) (*nats.Conn, error) {
	return nats.Connect(conf.Address)
}
