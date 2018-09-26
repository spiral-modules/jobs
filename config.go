package jobs

import (
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
)

const (
	// BrokerConfig defines config section related to Brokers configuration.
	BrokerConfig = "brokers"
)

// Config defines settings for job broker, workers and routing PipelineOptions.
type Config struct {
	// Workers configures roadrunner server and worker busy.
	Workers *roadrunner.ServerConfig

	// Pipelines defines mapping between PHP job pipeline and associated job broker.
	Pipelines []*Pipeline
}

// Hydrate populates config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	if c.Workers == nil {
		c.Workers = &roadrunner.ServerConfig{}
	}

	c.Workers.InitDefaults()

	return c.Workers.Pool.Valid()
}

type emptyConfig struct{}

// Get is doing nothing.
func (e *emptyConfig) Get(service string) service.Config {
	return nil
}

// Unmarshal is doing nothing.
func (e *emptyConfig) Unmarshal(out interface{}) error {
	return nil
}
