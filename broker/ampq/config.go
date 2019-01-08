package ampq

import (
	"github.com/spiral/roadrunner/service"
)

// Config defines sqs broker configuration.
type Config struct {
}

// InitDefaults sets missing values to their default values.
func (c *Config) InitDefaults() error {

	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(c); err != nil {
		return err
	}

	return nil
}
