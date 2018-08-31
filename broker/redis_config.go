package broker

import "github.com/spiral/roadrunner/service"

// RedisConfig defines connection options to Redis server.
type RedisConfig struct {
	Enable   bool
	Address  string
	Password string
	DB       int
}

// Hydrate populates config with values.
func (c *RedisConfig) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}
