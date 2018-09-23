package local

import (
	"fmt"
	"github.com/spiral/roadrunner/service"
)

// Config defines local broker configuration.
type Config struct {
	// Threads define how much treads local broker should run at the same time.
	Threads int
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// Valid validates local config.
func (c *Config) Valid() error {
	if c.Threads < 1 {
		return fmt.Errorf("invalid `threads` value, must be 1 or greater")
	}

	return nil
}

// InitDefaults init default config values.
func (c *Config) InitDefaults() error {
	c.Threads = 1
	return nil
}
