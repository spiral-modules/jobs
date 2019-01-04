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
	Pipelines Pipelines

	// Mapping defines how to map jobs to their target pipelines.
	Mapping Mapper

	// Consuming specifies names of pipelines to be consumed on service start.
	Consume []string

	// parent config for broken options.
	parent service.Config
}

// Hydrate populates config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	c.parent = cfg
	if c.Workers == nil {
		c.Workers = &roadrunner.ServerConfig{}
	}

	if err := c.Workers.InitDefaults(); err != nil {
		return err
	}

	if err := c.Pipelines.Valid(); err != nil {
		return err
	}

	return c.Workers.Pool.Valid()
}

// FindTarget locates the broker name and associated pipeline.
func (c *Config) MapPipeline(j *Job) (*Pipeline, error) {
	p := j.Options.Pipeline
	if p == "" {
		p = c.Mapping.find(j.Job)
	}

	if p == "" {
		return nil, fmt.Errorf("unable to locate pipeline for `%s`", j.Job)
	}

	if p := c.Pipelines.Get(p); p != nil {
		return p, nil
	}

	return nil, fmt.Errorf("undefined pipeline `%s`", p)
}

// Get underlying broker config.
func (c *Config) Get(service string) service.Config {
	if c.parent == nil {
		return nil
	}

	return c.parent.Get(service)
}

// Unmarshal is doing nothing.
func (c *Config) Unmarshal(out interface{}) error {
	return nil
}
