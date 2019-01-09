package amqp

import (
	"github.com/spiral/roadrunner/service"
	"time"
)

// Config defines sqs broker configuration.
type Config struct {
	// Addr of AMQP server (example: amqp://guest:guest@localhost:5672/).
	Addr string

	// Exchange exchange to be used.
	Exchange string

	// Size defines number of open connections to beanstalk server. Default 5.
	NumConn int

	// Timeout to allocate the conn. Default 5.
	Timeout int
}

// InitDefaults sets missing values to their default values.
func (c *Config) InitDefaults() error {
	c.NumConn = 2
	c.Timeout = 5

	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(c); err != nil {
		return err
	}

	return nil
}

// TimeoutDuration returns number of seconds allowed to allocate the conn.
func (c *Config) TimeoutDuration() time.Duration {
	return time.Duration(c.Timeout) * time.Second
}
