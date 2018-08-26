package jobs

import (
	"github.com/spiral/roadrunner/service"
)

type Config struct {
	Enabled bool
	Pipelines map[string]string
}

func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}
