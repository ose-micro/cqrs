package rabbit

type Config struct {
	URL              string `env:"RABBITMQ_URL,required"`
	Exchange         string `env:"RABBITMQ_EXCHANGE" default:"ose.exchange"`
	ExchangeType     string `env:"RABBITMQ_EXCHANGE_TYPE" default:"topic"`
	ReconnectBackoff int    `env:"RABBITMQ_RECONNECT_BACKOFF" default:"5"`
}