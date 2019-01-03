package jobs

import (
	"fmt"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
)

// Pipeline defines settings for job broker, workers and routing PipelineOptions.
type Config struct {
	// Workers configures roadrunner server and worker busy.
	Workers *roadrunner.ServerConfig

	// Pipelines defines mapping between PHP job pipeline and associated job broker.
	Pipelines map[string]*Pipeline

	// Mapping defines how to map jobs to their target pipelines.
	Mapping Mapper

	// Consume specifies names of pipelines to be consumed on service start.
	Consume []string

	// brokers config for broken options.
	brokers service.Config
}

// Hydrate populates config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	c.brokers = cfg.Get("brokers")

	if c.Workers == nil {
		c.Workers = &roadrunner.ServerConfig{}
	}

	if err := c.Workers.InitDefaults(); err != nil {
		return err
	}

	return c.Workers.Pool.Valid()
}

// FindTarget locates the broker name and associated pipeline.
func (c *Config) FindPipeline(j *Job) (*Pipeline, error) {
	p := j.Options.Pipeline
	if p == "" {
		p = c.Mapping.find(j.Job)
	}

	if p == "" {
		return nil, fmt.Errorf("unable to locate pipeline for `%s`", j.Job)
	}

	pipe, ok := c.Pipelines[p]
	if !ok {
		return nil, fmt.Errorf("undefined pipeline `%s`", p)
	}

	return pipe, nil
}

// BrokerPipelines returns all pipelines broker accept jobs into.
func (c *Config) BrokerPipelines(broker string) []*Pipeline {
	accept := make([]*Pipeline, 0)
	for _, p := range c.Pipelines {
		if p.Broker() == broker {
			accept = append(accept, p)
		}
	}

	return accept
}

// BrokerPipelines returns all pipelines broker consumes from.
func (c *Config) ConsumedPipelines(broker string) []*Pipeline {
	consume := make([]*Pipeline, 0)
	for _, pipe := range c.Consume {
		p, ok := c.Pipelines[pipe]
		if !ok || p.Broker() != broker {
			// undefined reference
			continue
		}

		consume = append(consume, p)
	}

	return consume
}

// Get underlying broker config.
func (c *Config) Get(service string) service.Config {
	if c.brokers == nil {
		return nil
	}

	return c.brokers.Get(service)
}

// Unmarshal is doing nothing.
func (c *Config) Unmarshal(out interface{}) error {
	return nil
}
