package broker

import "github.com/spiral/roadrunner/service"

// RedisConfig defines connection options to Redis server.
type BeanstalkConfig struct {
	Enable  bool
	Address string
}

// Hydrate populates config with values.
func (c *BeanstalkConfig) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}
