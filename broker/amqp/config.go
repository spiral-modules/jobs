package amqp

import (
	"errors"
	"github.com/spiral/roadrunner/service"
	"github.com/streadway/amqp"
)

type Config struct {
	URL string

	Exchanges []Exchange
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  amqp.Table
}

func (c *Config) AMQP() (*amqp.Connection, error) {
	return amqp.Dial(c.URL)
}

func (c *Config) initExchanges(conn *amqp.Connection) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	for _, e := range c.Exchanges {
		if err := channel.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Arguments); err != nil {
			return err
		}
	}

	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(c); err != nil {
		return err
	}

	if c.URL == "" {
		return errors.New("URL is missing")
	}

	if _, err := amqp.ParseURI(c.URL); err != nil {
		return err
	}

	for _, e := range c.Exchanges {
		if e.Name == "" {
			return errors.New("name is required for each exchange")
		}
	}

	return nil
}
