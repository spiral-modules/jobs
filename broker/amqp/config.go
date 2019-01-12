package amqp

import (
	"github.com/spiral/roadrunner/service"
	"time"
)

// Config defines sqs broker configuration.
type Config struct {
	// Addr of AMQP server (example: amqp://guest:guest@localhost:5672/).
	Addr string

	// Timeout to allocate the conn. Default 10 seconds.
	Timeout int
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
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 10
	}

	return time.Duration(timeout) * time.Second
}
