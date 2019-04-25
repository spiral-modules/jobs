package jobs

import (
	"fmt"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
)

// Config defines settings for job broker, workers and job-pipeline mapping.
type Config struct {
	// Workers configures roadrunner server and worker busy.
	Workers *roadrunner.ServerConfig

	// Dispatch defines where and how to match jobs.
	Dispatch Dispatcher

	// Pipelines defines mapping between PHP job pipeline and associated job broker.
	Pipelines map[string]*Pipeline

	// Consuming specifies names of pipelines to be consumed on service start.
	Consume []string

	// parent config for broken options.
	parent    service.Config
	pipelines Pipelines
}

// Hydrate populates config values.
func (c *Config) Hydrate(cfg service.Config) (err error) {
	c.parent = cfg

	if c.Workers == nil {
		c.Workers = &roadrunner.ServerConfig{}
	}
	c.Workers.InitDefaults()

	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	c.pipelines, err = initPipelines(c.Pipelines)
	if err != nil {
		return err
	}

	if c.Workers != nil {
		return c.Workers.Pool.Valid()
	}

	return nil
}

// MatchPipeline locates the pipeline associated with the job.
func (c *Config) MatchPipeline(job *Job) (*Pipeline, *Options, error) {
	opt := c.Dispatch.match(job)

	pipe := ""
	if job.Options != nil {
		pipe = job.Options.Pipeline
	}

	if pipe == "" && opt != nil {
		pipe = opt.Pipeline
	}

	if pipe == "" {
		return nil, nil, fmt.Errorf("unable to locate pipeline for `%s`", job.Job)
	}

	if p := c.pipelines.Get(pipe); p != nil {
		return p, opt, nil
	}

	return nil, nil, fmt.Errorf("undefined pipeline `%s`", pipe)
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
