package jobs

import (
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner"
	"errors"
	"time"
)

type Config struct {
	Enabled   bool
	Pipelines map[string]string
	Handlers struct {
		Local *roadrunner.ServerConfig
	}
}

func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	if c.Handlers.Local.Relay == "" {
		c.Handlers.Local.Relay = "pipes"
	}

	if c.Handlers.Local.RelayTimeout < time.Microsecond {
		c.Handlers.Local.RelayTimeout = time.Second * time.Duration(c.Handlers.Local.RelayTimeout.Nanoseconds())
	}

	if c.Handlers.Local.Pool.AllocateTimeout < time.Microsecond {
		c.Handlers.Local.Pool.AllocateTimeout = time.Second * time.Duration(c.Handlers.Local.Pool.AllocateTimeout.Nanoseconds())
	}

	if c.Handlers.Local.Pool.DestroyTimeout < time.Microsecond {
		c.Handlers.Local.Pool.DestroyTimeout = time.Second * time.Duration(c.Handlers.Local.Pool.DestroyTimeout.Nanoseconds())
	}

	return nil
}

// Valid validates the configuration.
func (c *Config) Valid() error {
	if c.Handlers.Local == nil {
		return errors.New("mailformed local config")
	}

	if err := c.Handlers.Local.Pool.Valid(); err != nil {
		return err
	}

	return nil
}
