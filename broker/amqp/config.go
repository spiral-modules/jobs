package amqp

import (
	"github.com/spiral/roadrunner/service"
	"github.com/streadway/amqp"
)

// Config defined RabbitMQ connection options.
type Config struct {
	// AMPQ connect url
	Url string
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// Conn creates new rpc socket Listener.
func (c *Config) Conn() (*amqp.Connection, error) {
	return amqp.Dial(c.Url)
}
