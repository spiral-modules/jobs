package jobs

import (
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
	"time"
)

// Config defines settings for job broker, workers and routing PipelineOptions.
type Config struct {
	// Enable enables jobs service.
	Enable bool

	// Workers configures roadrunner server and worker pool.
	Workers *roadrunner.ServerConfig

	// Pipelines defines mapping between PHP job pipeline and associated job broker.
	Pipelines map[string]*Pipeline
}

// Pipeline describes broker specific pipeline.
type Pipeline struct {
	// Broker defines name of associated broker.
	Broker string

	// Retry defined number of job retries in case of error. Default none.
	Retry int

	// RetryDelay defines for how long wait till job retry.
	RetryDelay int

	// Listen tells the service that this pipeline must be consumed by the service.
	Listen bool

	// Options are broker specific PipelineOptions.
	Options PipelineOptions
}

type PipelineOptions map[string]interface{}

// String must return option value as string or return default value.
func (o PipelineOptions) String(name string, d string) string {
	if value, ok := o[name]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}

	return d
}

// Int must return option value as string or return default value.
func (o PipelineOptions) Integer(name string, d int) int {
	if value, ok := o[name]; ok {
		if str, ok := value.(int); ok {
			return str
		}
	}

	return d
}

// Hydrate populates config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(&c); err != nil {
		return err
	}

	if !c.Enable {
		return nil
	}

	if c.Workers.Relay == "" {
		c.Workers.Relay = "pipes"
	}

	if c.Workers.RelayTimeout < time.Microsecond {
		c.Workers.RelayTimeout = time.Second * time.Duration(c.Workers.RelayTimeout.Nanoseconds())
	}

	if c.Workers.Pool.AllocateTimeout < time.Microsecond {
		if c.Workers.Pool.AllocateTimeout == 0 {
			c.Workers.Pool.AllocateTimeout = time.Second * 60
		} else {
			c.Workers.Pool.AllocateTimeout = time.Second * time.Duration(c.Workers.Pool.AllocateTimeout.Nanoseconds())
		}
	}

	if c.Workers.Pool.DestroyTimeout < time.Microsecond {
		if c.Workers.Pool.DestroyTimeout == 0 {
			c.Workers.Pool.DestroyTimeout = time.Second * 30
		} else {
			c.Workers.Pool.DestroyTimeout = time.Second * time.Duration(c.Workers.Pool.DestroyTimeout.Nanoseconds())
		}
	}

	return nil
}
