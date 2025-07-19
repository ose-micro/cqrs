package rabbit

type Config struct {
	URL              string `mapstructure:"url"`
	Exchange         string `mapstructure:"exchange" default:"ose.exchange"`
	ExchangeType     string `mapstructure:"exchange_type" default:"topic"`
	ReconnectBackoff int    `mapstructure:"reconnect_backoff" default:"5"`
}