package jobs

import (
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner"
	"time"
)

// Config defines settings for job endpoint, workers and routing options.
type Config struct {
	// Enable enables jobs service.
	Enable bool

	// Workers configures roadrunner server and worker pool.
	Workers *roadrunner.ServerConfig

	// Pipelines defines mapping between PHP job pipeline and associated job endpoint.
	Pipelines map[string]struct {
		// Endpoint defines associated endpoint.
		Endpoint string
	}
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
